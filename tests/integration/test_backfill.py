"""Integration tests for `recon backfill` (recon/service/backfill.py) on a real temp SQLite DB."""

import shutil
from pathlib import Path

import pytest

from recon.presentation.cli import format_summary, main
from recon.repository import run_repo
from recon.repository.db import connect
from recon.repository.schema import TABLES, table_row_counts
from recon.service import backfill, pipeline
from recon.service.backfill import BackfillRequest, ManifestError, parse_manifest, run_backfill
from tests.silver_helpers import REF_HEADER, SRC_HEADER, TGT_HEADER, order_row

ROOT = Path(__file__).resolve().parents[2]
SAMPLE = ROOT / "data" / "sample"
CONFIG = ROOT / "config" / "orders_recon.json"
HEADER = "batch_id,source_path,target_path,reference_path"


@pytest.fixture(autouse=True)
def _no_fault_injection(monkeypatch):
    monkeypatch.delenv(pipeline.FAIL_AT_ENV, raising=False)


@pytest.fixture
def work(tmp_path) -> Path:
    """tmp/fail/ = the committed sample (recon FAIL, exit 1); tmp/pass/ = a clean pair (recon PASS, exit 0)."""
    (tmp_path / "fail").mkdir()
    for name in ("source.csv", "target.csv", "customers.csv"):
        shutil.copyfile(SAMPLE / name, tmp_path / "fail" / name)
    (tmp_path / "pass").mkdir()
    rows = [order_row("O1"), order_row("O2", amount="20.00")]
    for name, header in (("source.csv", SRC_HEADER), ("target.csv", TGT_HEADER)):
        (tmp_path / "pass" / name).write_bytes(("\n".join([header, *rows]) + "\n").encode("utf-8"))
    (tmp_path / "pass" / "customers.csv").write_bytes(f"{REF_HEADER}\nC1,Alice\n".encode("utf-8"))
    return tmp_path


def manifest(work: Path, *lines: str, header: str = HEADER, name: str = "manifest.csv") -> Path:
    path = work / name
    path.write_bytes(("\n".join([header, *lines]) + "\n").encode("utf-8"))
    return path


def pair(folder: str, batch_id: str) -> str:
    return f"{batch_id},{folder}/source.csv,{folder}/target.csv,{folder}/customers.csv"


def request(work: Path, manifest_path: Path) -> BackfillRequest:
    return BackfillRequest(manifest=manifest_path, config_path=CONFIG, db_path=work / "out" / "recon.db",
                           out_dir=work / "out")


def counts(work: Path) -> dict[str, int]:
    conn = connect(work / "out" / "recon.db")
    try:
        return table_row_counts(conn)
    finally:
        conn.close()


def cli(work: Path, manifest_path: Path) -> list[str]:
    return ["backfill", "--manifest", str(manifest_path), "--config", str(CONFIG),
            "--db", str(work / "out" / "recon.db"), "--out", str(work / "out")]


# --------------------------------------------------------------------------- runs


def test_lines_run_in_file_order_and_summary_is_sorted_by_batch_id(work, monkeypatch):
    order = []
    real = pipeline.run_pipeline

    def spy(req):
        order.append(req.source.parent.name)
        return real(req)

    monkeypatch.setattr(pipeline, "run_pipeline", spy)
    m = manifest(work, pair("fail", "b3"), pair("pass", "b1"), "b2,missing/source.csv,pass/target.csv,pass/customers.csv")

    result = run_backfill(request(work, m))

    assert order == ["fail", "pass", "missing"]  # manifest order
    assert [line.batch_id for line in result.lines] == ["b3", "b1", "b2"]
    assert [line.batch_id for line in result.summary] == ["b1", "b2", "b3"]
    by_batch = {line.batch_id: line for line in result.lines}
    assert (by_batch["b1"].status, by_batch["b1"].recon_status, by_batch["b1"].exit_code) == ("SUCCEEDED", "PASS", 0)
    assert (by_batch["b3"].status, by_batch["b3"].recon_status, by_batch["b3"].exit_code) == ("SUCCEEDED", "FAIL", 1)


def test_failing_line_does_not_stop_the_others_and_exit_is_max(work):
    m = manifest(work, pair("pass", "a"), "b,missing/source.csv,pass/target.csv,pass/customers.csv", pair("fail", "c"))

    result = run_backfill(request(work, m))

    statuses = {line.batch_id: (line.status, line.exit_code) for line in result.lines}
    assert statuses == {"a": ("SUCCEEDED", 0), "b": ("REJECTED", 2), "c": ("SUCCEEDED", 1)}
    assert result.exit_code == 2
    conn = connect(work / "out" / "recon.db")
    try:
        row = run_repo.get_run(conn, result.lines[1].run_id)
    finally:
        conn.close()
    assert (row["status"], row["failure_reason"]) == ("REJECTED", "FILE_NOT_FOUND")


@pytest.mark.parametrize("folders, expected", [
    (["pass"], 0),
    (["pass", "fail"], 1),
    (["fail", "pass"], 1),
])
def test_exit_code_is_max_over_lines(work, folders, expected):
    m = manifest(work, *(pair(folder, f"b{i}") for i, folder in enumerate(folders)))
    assert run_backfill(request(work, m)).exit_code == expected


def test_unexpected_exception_in_one_line_is_reported_and_others_continue(work, monkeypatch):
    real = pipeline.run_pipeline

    def flaky(req):
        if req.source.parent.name == "fail":
            raise OSError("disk on fire")
        return real(req)

    monkeypatch.setattr(pipeline, "run_pipeline", flaky)
    result = run_backfill(request(work, manifest(work, pair("fail", "x"), pair("pass", "y"))))

    x, y = result.lines
    assert (x.status, x.exit_code, x.run_id, x.error) == ("ERROR", 2, None, "OSError: disk on fire")
    assert (y.status, y.exit_code) == ("SUCCEEDED", 0)
    assert result.exit_code == 2


def test_rerun_skips_succeeded_pairs(work):
    m = manifest(work, pair("pass", "a"), "b,missing/source.csv,pass/target.csv,pass/customers.csv", pair("fail", "c"))
    first = {line.batch_id: line for line in run_backfill(request(work, m)).lines}
    before = counts(work)

    second = {line.batch_id: line for line in run_backfill(request(work, m)).lines}

    for batch_id in ("a", "c"):
        assert second[batch_id].status == "SKIPPED"
        assert second[batch_id].run_id == first[batch_id].run_id  # the reused SUCCEEDED run
        assert second[batch_id].exit_code == first[batch_id].exit_code
    assert second["b"].status == "REJECTED"  # a REJECTED pair is retried as the next attempt
    assert second["b"].run_id != first["b"].run_id and second["b"].run_id.endswith("-a2")
    after = counts(work)
    assert {t: after[t] for t in TABLES if t != "run"} == {t: before[t] for t in TABLES if t != "run"}
    assert after["run"] == before["run"] + 3  # two SKIPPED rows + one new REJECTED attempt


def test_same_pair_twice_in_one_manifest_is_skipped_the_second_time(work):
    result = run_backfill(request(work, manifest(work, pair("pass", "a"), pair("pass", "b"))))
    a, b = result.lines
    assert (a.status, b.status) == ("SUCCEEDED", "SKIPPED")
    assert b.run_id == a.run_id


def test_line_blocked_by_a_fresh_running_attempt_is_in_progress(work):
    from recon.repository.db import transaction
    from recon.repository.schema import create_schema
    from recon.service.metrics import utc_iso
    from recon.service.pipeline import RunRequest

    probe = pipeline.run_pipeline(RunRequest(  # learn the pair's run_key in a throwaway DB
        source=work / "pass" / "source.csv", target=work / "pass" / "target.csv",
        reference=work / "pass" / "customers.csv", config_path=CONFIG,
        db_path=work / "probe" / "recon.db", out_dir=work / "probe"))
    running_id = probe.run_key[:16] + "-a1"
    (work / "out").mkdir()
    conn = connect(work / "out" / "recon.db")
    try:
        create_schema(conn)
        with transaction(conn):  # a RUNNING attempt left by another process that is still working
            run_repo.create_run(conn, run_id=running_id, run_key=probe.run_key, attempt=1, status="RUNNING",
                                source_file="s", target_file="t", reference_file="r", config_sha256="c",
                                started_at=utc_iso())
    finally:
        conn.close()

    result = run_backfill(request(work, manifest(work, pair("pass", "a"), pair("fail", "b"))))

    a, b = result.lines
    assert (a.status, a.exit_code, a.run_id) == ("IN_PROGRESS", 2, running_id)
    assert (b.status, b.exit_code) == ("SUCCEEDED", 1)  # the blocked line does not stop the others
    assert result.exit_code == 2
    conn = connect(work / "out" / "recon.db")
    try:
        assert [r["run_id"] for r in run_repo.find_by_run_key(conn, probe.run_key)] == [running_id]  # no new row
    finally:
        conn.close()


def test_relative_paths_resolve_against_the_manifest_directory(work, monkeypatch):
    nested = work / "manifests"
    nested.mkdir()
    m = manifest(nested, "a,../pass/source.csv,../pass/target.csv,../pass/customers.csv")
    monkeypatch.chdir(work.parent)  # the CWD must not matter
    assert run_backfill(request(work, m)).lines[0].status == "SUCCEEDED"


# --------------------------------------------------------------------------- malformed manifest


MALFORMED = [
    ("empty file", ""),
    ("blank lines only", "\n\n"),
    ("header only", HEADER + "\n"),
    ("missing column", "batch_id,source_path,target_path\na,s,t\n"),
    ("unknown column", HEADER + ",note\na,s,t,r,x\n"),
    ("duplicate column", HEADER + ",batch_id\na,s,t,r,b\n"),
    ("too few fields", HEADER + "\na,s,t\n"),
    ("too many fields", HEADER + "\na,s,t,r,x\n"),
    ("empty batch_id", HEADER + "\n ,s,t,r\n"),
    ("empty path", HEADER + "\na,s,,r\n"),
    ("duplicate batch_id", HEADER + "\na,s,t,r\na,s2,t2,r2\n"),
    ("bad quoting", HEADER + '\na,"s"x,t,r\n'),
]


@pytest.mark.parametrize("text", [t for _, t in MALFORMED], ids=[n for n, _ in MALFORMED])
def test_malformed_manifest_exits_2_before_any_run(work, capsys, text):
    m = work / "manifest.csv"
    m.write_bytes(text.encode("utf-8"))
    with pytest.raises(ManifestError):
        parse_manifest(m)
    assert main(cli(work, m)) == 2
    assert "error: manifest" in capsys.readouterr().err
    assert not (work / "out").exists()  # no DB, no run, no output


def test_missing_manifest_exits_2(work, capsys):
    assert main(cli(work, work / "nope.csv")) == 2
    assert "manifest not found" in capsys.readouterr().err
    assert not (work / "out").exists()


def test_bad_config_exits_2_before_any_run(work, capsys):
    bad = work / "bad.json"
    bad.write_text("{}", encoding="utf-8")
    m = manifest(work, pair("pass", "a"))
    args = cli(work, m)
    args[args.index("--config") + 1] = str(bad)
    assert main(args) == 2
    assert "error:" in capsys.readouterr().err
    assert not (work / "out").exists()


def test_manifest_header_is_trimmed_lower_cased_and_order_free(work):
    m = manifest(work, "pass/target.csv,a,pass/customers.csv,pass/source.csv",
                 header=" Target_Path ,BATCH_ID,reference_path,source_path")
    (line,) = parse_manifest(m)
    assert (line.batch_id, line.source.name, line.target.name, line.reference.name) == \
        ("a", "source.csv", "target.csv", "customers.csv")
    assert line.line_no == 2


# --------------------------------------------------------------------------- CLI


def test_cli_prints_sorted_summary_and_writes_metrics(work, capsys):
    m = manifest(work, pair("fail", "b2"), pair("pass", "b1"))
    assert main(cli(work, m)) == 1
    out = capsys.readouterr().out.splitlines()
    assert out[0].split() == ["batch_id", "run_id", "status", "recon_status", "exit_code"]
    rows = [line.split() for line in out[1:]]
    assert [r[0] for r in rows] == ["b1", "b2"]
    assert [(r[2], r[3], r[4]) for r in rows] == [("SUCCEEDED", "PASS", "0"), ("SUCCEEDED", "FAIL", "1")]
    for r in rows:
        assert (work / "out" / r[1] / "metrics.json").is_file()


def test_format_summary_alignment_and_placeholders():
    lines = [backfill.BackfillLine("b1", 2, "ERROR", 2, error="x"),
             backfill.BackfillLine("batch-10", 3, "SUCCEEDED", 0, run_id="abc-a1", recon_status="PASS")]
    assert format_summary(lines).splitlines() == [
        "batch_id  run_id  status     recon_status  exit_code",
        "b1        -       ERROR      -             2",
        "batch-10  abc-a1  SUCCEEDED  PASS          0",
    ]
