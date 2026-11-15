"""Integration tests for the run outputs (recon/service/report.py, recon/presentation/report_writer.py)."""

import csv
import io
import json
import re
import shutil
from html.parser import HTMLParser
from pathlib import Path

import pytest

from recon.presentation import report_writer
from recon.presentation.cli import main
from recon.presentation.report_writer import (FIELD_DIFFS_CSV, MISMATCHES_CSV, QUARANTINE_CSV, REPORT_FILES,
                                              REPORT_JSON, SUMMARY_HTML, SUMMARY_MD, md_cell, recover_run_dir,
                                              render_summary, render_summary_html)
from recon.repository import gold_repo, silver_repo
from recon.repository.db import connect
from recon.service import pipeline, report
from recon.service.normalize import format_minor
from tests.silver_helpers import REF_HEADER, SRC_HEADER, TGT_HEADER, order_row

ROOT = Path(__file__).resolve().parents[2]
SAMPLE = ROOT / "data" / "sample"
CONFIG = ROOT / "config" / "orders_recon.json"
EXPECTED = json.loads((ROOT / "data" / "expected" / "sample.json").read_text(encoding="utf-8"))
TIMESTAMP = re.compile(r'((?:started_at|finished_at)\W+)\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ')


@pytest.fixture(autouse=True)
def _no_fault_injection(monkeypatch):
    monkeypatch.delenv(pipeline.FAIL_AT_ENV, raising=False)


@pytest.fixture
def inputs(tmp_path) -> Path:
    folder = tmp_path / "in"
    folder.mkdir()
    for name in ("source.csv", "target.csv", "customers.csv"):
        shutil.copyfile(SAMPLE / name, folder / name)
    return folder


def run_args(inputs: Path, out: Path, *extra: str) -> list[str]:
    return ["run", "--source", str(inputs / "source.csv"), "--target", str(inputs / "target.csv"),
            "--reference", str(inputs / "customers.csv"), "--config", str(CONFIG),
            "--db", str(out / "recon.db"), "--out", str(out), *extra]


def run_cli(args: list[str], capsys) -> tuple[int, dict[str, str], str]:
    code = main(args)
    out, err = capsys.readouterr()
    fields = dict(part.split("=", 1) for part in out.split()) if out.strip() else {}
    return code, fields, err


def report_cli(out: Path, run_id: str) -> list[str]:
    return ["report", "--run-id", run_id, "--db", str(out / "recon.db"), "--out", str(out)]


def files_bytes(run_dir: Path) -> dict[str, bytes]:
    return {p.name: p.read_bytes() for p in sorted(run_dir.iterdir())}


def read_csv(path: Path) -> list[dict[str, str]]:
    return list(csv.DictReader(io.StringIO(path.read_bytes().decode("utf-8"), newline="")))


def mask_timestamps(data: bytes) -> bytes:
    return TIMESTAMP.sub(r"\1<ts>", data.decode("utf-8")).encode("utf-8")


@pytest.fixture
def sample_run(inputs, tmp_path, capsys) -> tuple[Path, str]:
    """`recon run` on the committed sample; returns (out dir, run_id)."""
    out = tmp_path / "out"
    code, fields, _ = run_cli(run_args(inputs, out), capsys)
    assert code == 1 and fields["status"] == "SUCCEEDED"
    return out, fields["run_id"]


def db_rows(out: Path, fn, *args):
    conn = connect(out / "recon.db")
    try:
        return fn(conn, *args)
    finally:
        conn.close()


# --------------------------------------------------------------------------- report.json


def test_run_writes_every_output_file(sample_run):
    out, run_id = sample_run
    names = {p.name for p in (out / run_id).iterdir()}
    assert names == set(REPORT_FILES) | {"run.log", "metrics.json"}
    assert not [p for p in out.iterdir() if p.name.startswith(".")]  # no staging / backup dir left


def test_report_json_parses_and_has_every_top_level_key(sample_run):
    out, run_id = sample_run
    raw = (out / run_id / REPORT_JSON).read_bytes()
    doc = json.loads(raw.decode("utf-8"))

    assert tuple(sorted(doc)) == report.TOP_LEVEL_KEYS
    assert doc["report_version"] == 1
    assert raw.endswith(b"\n") and b"\r" not in raw
    assert set(doc["run"]) == {"run_id", "run_key", "attempt", "forced", "status", "recon_status", "exit_code",
                               "started_at", "finished_at", "config_sha256", "dataset", "business_key"}
    assert (doc["run"]["run_id"], doc["run"]["status"], doc["run"]["recon_status"]) == (run_id, "SUCCEEDED", "FAIL")
    assert list(doc["inputs"]) == ["REFERENCE", "SOURCE", "TARGET"]  # sorted keys
    assert {i["data_records"] for i in doc["inputs"].values()} == {63, 61, 5}
    assert doc["key_status_counts"] == EXPECTED["key_status_counts"]
    assert doc["totals"]["keys"] == EXPECTED["total_keys"]
    assert doc["totals"]["unattributable_rejects"] == EXPECTED["unattributable_rejects"]
    for side in ("SOURCE", "TARGET"):
        exp, got = EXPECTED["sides"][side], doc["sides"][side]
        for key in ("rows_read", "accepted", "duplicate_exact", "superseded", "conflicting_rows",
                    "rejected_by_reason"):
            assert got[key] == exp[key], (side, key)
    flat = [{"field_name": f, "diff_type": t, "count": n}
            for f, by_type in sorted(doc["field_diff_counts"].items()) for t, n in sorted(by_type.items()) if n]
    assert flat == EXPECTED["field_diff_counts"]
    assert [c["check_name"] for c in doc["checks"]] == sorted(c["check_name"] for c in doc["checks"])
    assert len(doc["checks"]) == 9 and all(isinstance(c["detail"], (dict, list)) for c in doc["checks"])
    assert doc["warnings"] == []
    # metrics never enter the report (DESIGN 7.1 #13)
    assert b"duration" not in raw and b"peak_memory" not in raw


def test_aggregate_explanation_adds_up(sample_run):
    out, run_id = sample_run
    doc = json.loads((out / run_id / REPORT_JSON).read_text(encoding="utf-8"))
    overall = {a["metric"]: a for a in doc["aggregates"] if a["group_by"] == "ALL"}
    assert [e["metric"] for e in doc["aggregate_explanation"]] == ["ROW_COUNT", "SUM_AMOUNT", "SUM_DISCOUNT"]
    for e in doc["aggregate_explanation"]:
        assert e["delta"] == overall[e["metric"]]["delta"] == e["explained_delta"]
        assert e["delta"] == e["source_only"] - e["target_only"] + e["both_sides"]
        assert sum(b["delta"] for b in e["by_key_status"].values()) == e["delta"]
        assert set(e["by_key_status"]) == set(doc["key_status_counts"])
    row_count = next(e for e in doc["aggregate_explanation"] if e["metric"] == "ROW_COUNT")
    assert row_count["by_key_status"]["MISSING_IN_TARGET"] == {"keys": 1, "source_value": 1, "target_value": 0,
                                                               "delta": 1}


def test_mismatch_samples_are_the_first_non_matched_keys(sample_run):
    out, run_id = sample_run
    doc = json.loads((out / run_id / REPORT_JSON).read_text(encoding="utf-8"))
    samples = doc["mismatch_samples"]
    assert samples["limit"] == report.SAMPLE_LIMIT
    keys = [s["record_key"] for s in samples["keys"]]
    csv_keys = [r["record_key"] for r in read_csv(out / run_id / MISMATCHES_CSV)]
    assert keys == csv_keys[:report.SAMPLE_LIMIT]
    assert all(s["key_status"] != "MATCHED" for s in samples["keys"])


# --------------------------------------------------------------------------- CSV extracts


def test_mismatched_records_csv_has_one_row_per_non_matched_key(sample_run):
    out, run_id = sample_run
    rows = read_csv(out / run_id / MISMATCHES_CSV)
    key_results = db_rows(out, gold_repo.fetch_key_results, run_id)
    non_matched = [k for k in key_results if k["key_status"] != "MATCHED"]

    assert len(rows) == len(non_matched) == EXPECTED["total_keys"] - EXPECTED["key_status_counts"]["MATCHED"]
    assert [r["record_key"] for r in rows] == [k["order_id"] for k in non_matched]  # sorted by key
    assert [r["key_status"] for r in rows] == [k["key_status"] for k in non_matched]
    header = list(rows[0])
    lead = len(report.MISMATCH_LEAD_COLUMNS)
    assert header[:lead] == list(report.MISMATCH_LEAD_COLUMNS) and header[0] == "run_id"
    assert {r["run_id"] for r in rows} == {run_id}  # every exported row is traceable on its own
    assert header[lead:lead + 4] == ["source_customer_id", "target_customer_id", "source_order_ts", "target_order_ts"]

    diffs: dict[str, list[str]] = {}
    for d in db_rows(out, gold_repo.fetch_field_diffs, run_id):
        diffs.setdefault(d["order_id"], []).append(d["field_name"])
    for r in rows:
        assert r["differing_columns"] == ";".join(diffs.get(r["record_key"], []))
        for side in ("source", "target"):
            values = [v for k, v in r.items() if k.startswith(f"{side}_") and k not in report.MISMATCH_LEAD_COLUMNS]
            if r[f"{side}_state"] != "CLEAN":
                assert values == [""] * len(values)  # no canonical values without a clean row
    by_key = {r["record_key"]: r for r in rows}
    assert by_key["ORD-000016"]["differing_columns"] == "amount"
    assert (by_key["ORD-000016"]["source_amount"], by_key["ORD-000016"]["target_amount"]) == ("736.79", "775.49")
    assert by_key["ORD-000022"]["source_state"] == "MISSING_REQUIRED"
    assert by_key["ORD-000060"]["target_state"] == "ABSENT"
    assert by_key["ORD-000054"]["source_state"] == "CONFLICTING_DUPLICATE"


def test_field_diffs_csv_matches_field_diff_table(sample_run):
    out, run_id = sample_run
    rows = read_csv(out / run_id / FIELD_DIFFS_CSV)
    table = db_rows(out, gold_repo.fetch_field_diffs, run_id)
    assert [list(r.values()) for r in rows] == [
        [run_id, d["order_id"], d["field_name"], d["source_value"] or "", d["target_value"] or "", d["diff_type"]]
        for d in table]
    assert list(rows[0]) == list(report.FIELD_DIFF_CSV_COLUMNS)


def test_quarantine_csv_count_equals_quarantine_table(sample_run):
    out, run_id = sample_run
    rows = read_csv(out / run_id / QUARANTINE_CSV)
    table = db_rows(out, silver_repo.fetch_quarantine, run_id)

    assert len(rows) == len(table) == 10
    assert list(rows[0]) == list(report.QUARANTINE_CSV_COLUMNS) and list(rows[0])[0] == "run_id"
    assert {r["run_id"] for r in rows} == {run_id}
    order = {"SOURCE": 0, "TARGET": 1, "REFERENCE": 2}
    assert [(r["side"], int(r["line_no"])) for r in rows] == sorted(
        ((q["side"], q["line_no"]) for q in table), key=lambda k: (order[k[0]], k[1]))
    by_line = {(q["side"], q["line_no"]): q for q in table}
    for r in rows:  # raw text survives the CSV round trip exactly
        assert r["raw_text"] == by_line[(r["side"], int(r["line_no"]))]["raw_text"]


# --------------------------------------------------------------------------- determinism


def test_regenerating_twice_gives_byte_identical_files(sample_run, capsys):
    out, run_id = sample_run
    run_dir = out / run_id
    original = files_bytes(run_dir)

    for _ in range(2):
        code, fields, _ = run_cli(report_cli(out, run_id), capsys)
        assert code == 0 and fields["report"] == (run_dir / REPORT_JSON).as_posix()
        assert files_bytes(run_dir) == original  # report files identical; run.log and metrics.json kept
    assert not [p for p in out.iterdir() if p.name.startswith(".")]


def test_fresh_dbs_give_identical_reports_apart_from_run_timestamps(inputs, tmp_path, capsys):
    reports = []
    for name in ("a", "b"):
        out = tmp_path / name
        code, fields, _ = run_cli(run_args(inputs, out), capsys)
        assert code == 1
        reports.append({n: b for n, b in files_bytes(out / fields["run_id"]).items() if n in REPORT_FILES})
    a, b = reports
    for name in REPORT_FILES:
        assert mask_timestamps(a[name]) == mask_timestamps(b[name]), name
    for name in (MISMATCHES_CSV, FIELD_DIFFS_CSV, QUARANTINE_CSV):
        assert a[name] == b[name]  # no timestamps in the extracts at all


def test_forced_attempt_differs_only_in_run_identity(sample_run, inputs, capsys):
    out, first_id = sample_run
    code, fields, _ = run_cli(run_args(inputs, out, "--force"), capsys)
    assert code == 1 and fields["run_id"] == first_id[:-1] + "2"
    first, forced = files_bytes(out / first_id), files_bytes(out / fields["run_id"])
    for name in (MISMATCHES_CSV, FIELD_DIFFS_CSV, QUARANTINE_CSV):  # identical apart from the run_id column
        assert first[name].replace(first_id.encode(), b"<run_id>") ==             forced[name].replace(fields["run_id"].encode(), b"<run_id>")
    a, b = (json.loads(x[REPORT_JSON]) for x in (first, forced))
    for doc in (a, b):
        for key in ("run_id", "attempt", "forced", "started_at", "finished_at"):
            doc["run"].pop(key)
    assert a == b


# --------------------------------------------------------------------------- SKIPPED and other statuses


def test_skipped_prints_existing_report_path_without_rewriting(sample_run, inputs, capsys):
    out, run_id = sample_run
    run_dir = out / run_id
    marker = run_dir / REPORT_JSON
    marker.write_bytes(marker.read_bytes() + b"tampered")  # a rewrite would remove this
    before = {p.name: (p.stat().st_mtime_ns, p.read_bytes()) for p in run_dir.iterdir()}

    code, fields, err = run_cli(run_args(inputs, out), capsys)

    assert code == 1 and fields["status"] == "SKIPPED" and fields["run_id"] == run_id
    assert fields["report"] == marker.as_posix()
    assert {p.name: (p.stat().st_mtime_ns, p.read_bytes()) for p in run_dir.iterdir()} == before
    assert "warning" not in err


def test_skipped_with_missing_report_warns_and_writes_nothing(sample_run, inputs, capsys):
    out, run_id = sample_run
    shutil.rmtree(out / run_id)
    code, fields, err = run_cli(run_args(inputs, out), capsys)
    assert code == 1 and fields["status"] == "SKIPPED"
    assert f"recon report --run-id {run_id}" in err
    assert not (out / run_id).exists()


def test_failed_and_rejected_runs_write_no_report(inputs, tmp_path, monkeypatch, capsys):
    out = tmp_path / "out"
    monkeypatch.setenv(pipeline.FAIL_AT_ENV, "after_gold")
    code, fields, _ = run_cli(run_args(inputs, out), capsys)
    assert code == 2 and fields["status"] == "FAILED" and "report" not in fields
    assert {p.name for p in (out / fields["run_id"]).iterdir()} == {"run.log", "metrics.json"}

    code, _, err = run_cli(report_cli(out, fields["run_id"]), capsys)
    assert code == 2 and "status FAILED" in err


def test_report_command_refuses_a_skipped_run_id(sample_run, inputs, capsys):
    out, run_id = sample_run
    code, fields, _ = run_cli(run_args(inputs, out), capsys)
    assert fields["status"] == "SKIPPED"
    code, _, err = run_cli(report_cli(out, fields["skip_run_id"]), capsys)
    assert code == 2 and "status SKIPPED" in err
    assert not (out / fields["skip_run_id"]).exists()


def test_batch_warnings_reach_report_json_and_never_change_status(tmp_path, capsys):
    folder = tmp_path / "in"
    folder.mkdir()
    rows = [order_row("O1"), order_row("O2", amount="20.00")]
    (folder / "source.csv").write_bytes(("\n".join([f"{SRC_HEADER},Note", *(r + ",x" for r in rows)]) + "\n").encode())
    (folder / "target.csv").write_bytes(("\n".join([f"{TGT_HEADER},load_batch", *(r + ",7" for r in rows)]) + "\n")
                                        .encode())
    (folder / "customers.csv").write_bytes(b"customer_id\nC1\n")  # customer_name is optional
    out = tmp_path / "out"

    code, fields, _ = run_cli(run_args(folder, out), capsys)

    assert (code, fields["status"], fields["recon_status"]) == (0, "SUCCEEDED", "PASS")
    doc = json.loads((out / fields["run_id"] / REPORT_JSON).read_text(encoding="utf-8"))
    assert doc["warnings"] == [  # sorted by code, then side
        {"code": "MISSING_OPTIONAL_COLUMN", "side": "REFERENCE", "columns": ["customer_name"]},
        {"code": "SCHEMA_EXTRA_COLUMN", "side": "SOURCE", "columns": ["note"]},
        {"code": "SCHEMA_EXTRA_COLUMN", "side": "TARGET", "columns": ["load_batch"]}]
    checks = {c["check_name"]: c for c in doc["checks"]}
    assert (checks["SCHEMA_DRIFT"]["status"], checks["MISSING_OPTIONAL_COLUMN"]["status"]) == ("WARN", "WARN")
    assert checks["EMPTY_BATCH"]["status"] == "PASS"
    assert doc["key_status_counts"]["MATCHED"] == 2  # extra columns are ignored, never compared


def test_report_command_errors_exit_2(sample_run, tmp_path, capsys):
    out, _ = sample_run
    code, _, err = run_cli(report_cli(out, "nope-a1"), capsys)
    assert code == 2 and "not found" in err
    code, _, err = run_cli(["report", "--run-id", "x", "--db", str(tmp_path / "missing.db")], capsys)
    assert code == 2 and "database not found" in err
    assert not (tmp_path / "missing.db").exists()


def test_report_refuses_config_that_does_not_match_hash(sample_run, capsys):
    out, run_id = sample_run
    conn = connect(out / "recon.db")
    try:
        conn.execute("UPDATE run SET config_json = replace(config_json, 'orders', 'other') WHERE run_id = ?",
                     (run_id,))
    finally:
        conn.close()
    code, _, err = run_cli(report_cli(out, run_id), capsys)
    assert code == 2 and "config_sha256" in err


def test_backfill_writes_a_report_per_succeeded_line(inputs, tmp_path, capsys):
    manifest = tmp_path / "manifest.csv"
    manifest.write_text("batch_id,source_path,target_path,reference_path\n"
                        "b1,in/source.csv,in/target.csv,in/customers.csv\n", encoding="utf-8")
    out = tmp_path / "out"
    code = main(["backfill", "--manifest", str(manifest), "--config", str(CONFIG), "--db", str(out / "recon.db"),
                 "--out", str(out)])
    assert code == 1
    run_dirs = [p for p in out.iterdir() if p.is_dir()]
    assert len(run_dirs) == 1 and (run_dirs[0] / REPORT_JSON).is_file()


# --------------------------------------------------------------------------- atomic publish


def test_crash_while_regenerating_keeps_previous_report(sample_run, monkeypatch, capsys):
    out, run_id = sample_run
    before = files_bytes(out / run_id)

    def boom(_report):
        raise RuntimeError("disk full")
    monkeypatch.setattr(report_writer, "render_summary", boom)  # summary.md is written last
    code, _, err = run_cli(report_cli(out, run_id), capsys)

    assert code == 2 and "disk full" in err
    assert files_bytes(out / run_id) == before
    assert not [p for p in out.iterdir() if p.name.startswith(".")]


def test_crash_on_first_write_leaves_no_partial_report(inputs, tmp_path, monkeypatch, capsys):
    out = tmp_path / "out"

    def boom(_report):
        raise RuntimeError("disk full")
    monkeypatch.setattr(report_writer, "render_summary", boom)
    code, fields, err = run_cli(run_args(inputs, out), capsys)

    assert code == 2 and fields["status"] == "SUCCEEDED"  # the run succeeded; its outputs did not
    assert f"recon report --run-id {fields['run_id']}" in err
    assert {p.name for p in (out / fields["run_id"]).iterdir()} == {"run.log", "metrics.json"}

    monkeypatch.undo()
    code, _, _ = run_cli(report_cli(out, fields["run_id"]), capsys)
    assert code == 0 and (out / fields["run_id"] / SUMMARY_MD).is_file()


def test_stdout_shows_the_real_exit_code_and_no_report_path_when_publishing_fails(inputs, tmp_path, monkeypatch,
                                                                                    capsys):
    def boom(*_args):
        raise OSError("disk full")
    monkeypatch.setattr("recon.presentation.cli.write_report", boom)
    code, fields, _ = run_cli(run_args(inputs, tmp_path / "out"), capsys)

    assert (code, fields["status"], fields["recon_status"]) == (2, "SUCCEEDED", "FAIL")
    assert fields["exit_code"] == "2"  # the process exit code, not the run's recon exit code 1
    assert "report" not in fields


def test_recover_run_dir_finishes_an_interrupted_swap(tmp_path):
    run_dir = tmp_path / "r-a1"
    backup, staging = tmp_path / ".r-a1.old", tmp_path / ".r-a1.staging"

    backup.mkdir()
    (backup / REPORT_JSON).write_text("old", encoding="utf-8")
    staging.mkdir()
    recover_run_dir(run_dir)  # crash between the two renames: the previous dir comes back
    assert (run_dir / REPORT_JSON).read_text(encoding="utf-8") == "old"
    assert not backup.exists() and not staging.exists()

    backup.mkdir()
    recover_run_dir(run_dir)  # crash after the swap: the leftover backup goes
    assert not backup.exists() and run_dir.is_dir()


# --------------------------------------------------------------------------- summary.md


def test_summary_md_puts_the_verdict_first(sample_run):
    out, run_id = sample_run
    text = (out / run_id / SUMMARY_MD).read_text(encoding="utf-8")
    assert text.splitlines()[0] == "# Reconciliation FAIL"
    assert "11 of 61 keys not matched" in text and "ORD-000016" in text


def test_summary_md_pass(tmp_path, capsys):
    folder = tmp_path / "in"
    folder.mkdir()
    rows = [order_row("O1"), order_row("O2", amount="20.00")]
    for name, header in (("source.csv", SRC_HEADER), ("target.csv", TGT_HEADER)):
        (folder / name).write_bytes(("\n".join([header, *rows]) + "\n").encode("utf-8"))
    (folder / "customers.csv").write_bytes(f"{REF_HEADER}\nC1,Alice\n".encode("utf-8"))
    out = tmp_path / "out"
    code, fields, _ = run_cli(run_args(folder, out), capsys)
    assert code == 0
    run_dir = out / fields["run_id"]
    assert (run_dir / SUMMARY_MD).read_text(encoding="utf-8").startswith("# Reconciliation PASS\n")
    assert read_csv(run_dir / MISMATCHES_CSV) == [] and read_csv(run_dir / QUARANTINE_CSV) == []
    assert json.loads((run_dir / REPORT_JSON).read_text(encoding="utf-8"))["mismatch_samples"]["keys"] == []


def test_md_cell_escapes_table_syntax():
    assert md_cell(None) == "-"
    assert md_cell("A\\|B") == "A\\\\\\|B"  # a composite key cannot break the table
    assert md_cell("line1\nline2") == "line1 line2"
    assert md_cell(True) == "yes"


def test_render_summary_is_pure(sample_run):
    out, run_id = sample_run
    doc = json.loads((out / run_id / REPORT_JSON).read_text(encoding="utf-8"))
    assert render_summary(doc) == (out / run_id / SUMMARY_MD).read_text(encoding="utf-8")


# --------------------------------------------------------------------------- summary.html


class _Page(HTMLParser):
    """Collects tags, attributes, tile label -> value, bar-row texts, the h1 and table rows (cell texts)."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.tags, self.attrs, self.tiles, self.bars, self.tables = [], [], {}, [], []
        self.h1, self._label = "", ""
        self._classes, self._text, self._row, self._cell = [], [], None, None

    def handle_starttag(self, tag, attrs):
        self.tags.append(tag)
        self.attrs += attrs
        cls = dict(attrs).get("class") or ""
        self._classes.append(cls)
        if cls in ("label", "value", "bar-row") or tag == "h1":
            self._text = []
        if tag == "table":
            self.tables.append([])
        elif tag == "tr":
            self._row = []
        elif tag in ("td", "th"):
            self._cell = []

    def handle_endtag(self, tag):
        cls = self._classes.pop() if self._classes else ""
        text = "".join(self._text)
        if cls == "label":
            self._label = text
        elif cls == "value":
            self.tiles[self._label] = int(text)
        elif cls == "bar-row":
            self.bars.append(" ".join(" ".join(self._text).split()))
        elif tag == "h1":
            self.h1 = text
        if tag in ("td", "th"):
            self._row.append("".join(self._cell))
            self._cell = None
        elif tag == "tr":
            self.tables[-1].append(self._row)

    def handle_data(self, data):
        self._text.append(data)
        if self._cell is not None:
            self._cell.append(data)


def parse_html(text: str) -> _Page:
    page = _Page()
    page.feed(text)
    page.close()
    return page


def test_summary_html_is_written_self_contained_and_parses(sample_run):
    out, run_id = sample_run
    raw = (out / run_id / SUMMARY_HTML).read_bytes()
    text = raw.decode("utf-8")
    assert b"\r" not in raw and text.startswith("<!DOCTYPE html>\n") and text.endswith("</html>\n")
    page = parse_html(text)
    assert page.h1 == "Reconciliation FAIL"
    assert "style" in page.tags and "script" not in page.tags and "link" not in page.tags
    assert not [v for k, v in page.attrs if k in ("src", "href")]  # no external resources at all
    assert "http" not in text and "@import" not in text and "url(" not in text
    assert f"<code>{run_id}</code>" in text and "run status SUCCEEDED" in text and "exit code 1" in text


def test_summary_html_numbers_match_report_json(sample_run):
    out, run_id = sample_run
    doc = json.loads((out / run_id / REPORT_JSON).read_text(encoding="utf-8"))
    page = parse_html((out / run_id / SUMMARY_HTML).read_text(encoding="utf-8"))
    totals, counts = doc["totals"], doc["key_status_counts"]

    assert page.tiles == {"total keys": totals["keys"], "MATCHED": counts["MATCHED"],
                          "non-matched": totals["non_matched_keys"], "field diffs": totals["field_diffs"],
                          "quarantined rows": totals["quarantine_rows"],
                          "unattributable rejects": totals["unattributable_rejects"]}
    bars = {b.split()[0]: int(b.split()[1]) for b in page.bars if b.split()[0] != "total"}
    assert bars == counts and f"total {totals['keys']}" in page.bars
    assert "MATCHED 50 (81.97%)" in page.bars  # width = share of keys: 50 / 61

    explanation_rows = sorted(r for t in page.tables if t[0][0] == "key_status" for r in t[1:])
    expected = sorted([s, str(b["keys"]), *(format_minor(b[k], e["scale"]) if e["unit"] == "MINOR_UNITS"
                                            else str(b[k]) for k in ("source_value", "target_value", "delta"))]
                      for e in doc["aggregate_explanation"] for s, b in e["by_key_status"].items() if b["keys"])
    assert explanation_rows == expected
    [diffs] = [t for t in page.tables if t[0][0] == "column"]
    assert sorted((r[0], r[1], int(r[2])) for r in diffs[1:]) == sorted(
        (col, t, n) for col, by_type in doc["field_diff_counts"].items() for t, n in by_type.items() if n)
    [samples] = [t for t in page.tables if t[0][0] == "record_key"]
    assert [r[:2] for r in samples[1:]] == [[m["record_key"], m["key_status"]]
                                           for m in doc["mismatch_samples"]["keys"]]
    [checks] = [t for t in page.tables if t[0][0] == "check"]
    assert [r[0] for r in checks[1:]] == [c["check_name"] for c in doc["checks"]]


def test_summary_html_tables_show_the_same_rows_as_summary_md(sample_run):
    out, run_id = sample_run
    md = (out / run_id / SUMMARY_MD).read_text(encoding="utf-8")
    md_rows = {tuple(c.strip() for c in line.strip().strip("|").split(" | ")) for line in md.splitlines()
               if line.startswith("| ")}
    page = parse_html((out / run_id / SUMMARY_HTML).read_text(encoding="utf-8"))
    html_rows = [tuple(r) for t in page.tables for r in t[1:]]
    assert len(html_rows) > 20 and [r for r in html_rows if r not in md_rows] == []


def test_summary_html_escapes_every_value_from_the_data(sample_run):
    out, run_id = sample_run
    doc = json.loads((out / run_id / REPORT_JSON).read_text(encoding="utf-8"))
    evil = '<script>alert("x")</script>&'
    doc["run"]["dataset"] = evil
    doc["mismatch_samples"]["keys"][0]["record_key"] = evil
    doc["field_diff_counts"][evil] = {"VALUE_MISMATCH": 1}
    doc["warnings"].append({"code": "SCHEMA_EXTRA_COLUMN", "side": "TARGET", "columns": [evil]})
    doc["checks"][0]["status"] = evil
    text = render_summary_html(doc)

    assert "<script>" not in text and "</script>" not in text
    assert "&lt;script&gt;alert(&quot;x&quot;)&lt;/script&gt;&amp;" in text
    page = parse_html(text)
    assert "script" not in page.tags
    assert evil in {c for t in page.tables for r in t for c in r}  # round-trips as text, not markup


def test_summary_html_regeneration_is_byte_identical(sample_run, capsys):
    out, run_id = sample_run
    before = (out / run_id / SUMMARY_HTML).read_bytes()
    code, _, _ = run_cli(report_cli(out, run_id), capsys)
    assert code == 0 and (out / run_id / SUMMARY_HTML).read_bytes() == before
    doc = json.loads((out / run_id / REPORT_JSON).read_text(encoding="utf-8"))
    assert render_summary_html(doc).encode("utf-8") == before  # pure: same bytes from the parsed report.json


def test_summary_html_pass(tmp_path, capsys):
    folder = tmp_path / "in"
    folder.mkdir()
    for name, header in (("source.csv", SRC_HEADER), ("target.csv", TGT_HEADER)):
        (folder / name).write_bytes(f"{header}\n{order_row('O1')}\n".encode("utf-8"))
    (folder / "customers.csv").write_bytes(f"{REF_HEADER}\nC1,Alice\n".encode("utf-8"))
    out = tmp_path / "out"
    code, fields, _ = run_cli(run_args(folder, out), capsys)
    assert code == 0
    text = (out / fields["run_id"] / SUMMARY_HTML).read_text(encoding="utf-8")
    page = parse_html(text)
    assert page.h1 == "Reconciliation PASS" and 'class="banner pass"' in text
    assert page.tiles["MATCHED"] == 1 and page.tiles["non-matched"] == 0
    assert "MATCHED 1 (100.00%)" in page.bars and "CONFLICT 0 (0.00%)" in page.bars
