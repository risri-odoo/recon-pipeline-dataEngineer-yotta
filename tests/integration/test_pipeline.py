"""Integration tests for `recon run` (recon/service/pipeline.py) on a real temp SQLite DB."""

import json
import shutil
from dataclasses import replace
from pathlib import Path

import pytest

from recon.presentation.cli import main
from recon.repository import bronze_repo, gold_repo, run_repo, silver_repo
from recon.repository.db import connect, transaction
from recon.repository.schema import TABLES, create_schema, table_row_counts
from recon.service import pipeline
from recon.service.metrics import utc_iso
from recon.service.normalize import format_minor
from recon.service.pipeline import RunRequest, run_pipeline
from tests.gold_helpers import gold_snapshot

ROOT = Path(__file__).resolve().parents[2]
SAMPLE = ROOT / "data" / "sample"
EXPECTED = json.loads((ROOT / "data" / "expected" / "sample.json").read_text(encoding="utf-8"))
CONFIG = ROOT / "config" / "orders_recon.json"

DATA_TABLES = tuple(t for t in TABLES if t != "run")  # every bronze/silver/gold table except run
KEY_STATUSES = ("CONFLICT", "MATCHED", "MISMATCHED", "SOURCE_REJECTED", "TARGET_REJECTED",
                "MISSING_IN_TARGET", "EXTRA_IN_TARGET")
ROW_REJECT_CODES = ("MALFORMED_ROW", "MISSING_REQUIRED", "BAD_TYPE", "BAD_FORMAT", "BAD_ENUM", "OUT_OF_RANGE",
                    "INVALID_REFERENCE")
GENERATOR_ONLY = {"bucket_sizes", "generator_version", "n", "scenario_keys", "seed"}


@pytest.fixture
def request_(tmp_path) -> RunRequest:
    """The committed sample + shipped config copied into tmp, with a fresh DB and out dir there."""
    inputs = tmp_path / "in"
    inputs.mkdir()
    for name in ("source.csv", "target.csv", "customers.csv"):
        shutil.copyfile(SAMPLE / name, inputs / name)
    shutil.copyfile(CONFIG, inputs / "config.json")
    return RunRequest(source=inputs / "source.csv", target=inputs / "target.csv",
                      reference=inputs / "customers.csv", config_path=inputs / "config.json",
                      db_path=tmp_path / "out" / "recon.db", out_dir=tmp_path / "out")


@pytest.fixture(autouse=True)
def _no_fault_injection(monkeypatch):
    monkeypatch.delenv(pipeline.FAIL_AT_ENV, raising=False)


def db(request_: RunRequest):
    return connect(request_.db_path)


def run_row(request_: RunRequest, run_id: str) -> dict:
    conn = db(request_)
    try:
        return run_repo.get_run(conn, run_id)
    finally:
        conn.close()


def counts(request_: RunRequest, run_id: str | None = None) -> dict[str, int]:
    conn = db(request_)
    try:
        return table_row_counts(conn, run_id)
    finally:
        conn.close()


def summarize(conn, run_id: str) -> dict:
    """The DB results of a run, in the shape of data/expected/<name>.json (without generator metadata)."""
    key_counts = gold_repo.count_key_statuses(conn, run_id)
    by_reason = silver_repo.count_quarantine_by_reason(conn, run_id)
    aggregates = {(a["metric"], a["group_by"]): a for a in gold_repo.fetch_aggregates(conn, run_id)
                  if a["group_by"] == "ALL"}
    sides = {}
    for side, value in (("SOURCE", "source_value"), ("TARGET", "target_value")):
        sides[side] = {
            "rows_read": bronze_repo.count_raw_records(conn, run_id, side, "DATA"),
            "accepted": silver_repo.count_clean_records(conn, run_id, side),
            "duplicate_exact": by_reason.get((side, "DUPLICATE_EXACT"), 0),
            "superseded": by_reason.get((side, "SUPERSEDED"), 0),
            "conflicting_rows": by_reason.get((side, "CONFLICTING_DUPLICATE"), 0),
            "rejected_by_reason": {code: by_reason.get((side, code), 0) for code in ROW_REJECT_CODES},
            "sum_amount": format_minor(aggregates[("SUM_AMOUNT", "ALL")][value]),
            "sum_discount": format_minor(aggregates[("SUM_DISCOUNT", "ALL")][value]),
        }
    sides["REFERENCE"] = {"rows_read": bronze_repo.count_raw_records(conn, run_id, "REFERENCE", "DATA"),
                          "accepted": len(silver_repo.fetch_ref_customers(conn, run_id))}
    return {
        "key_status_counts": {s: key_counts.get(s, 0) for s in KEY_STATUSES},
        "field_diff_counts": gold_repo.count_field_diffs(conn, run_id),
        "recon_status": run_repo.get_run(conn, run_id)["recon_status"],
        "total_keys": sum(key_counts.values()),
        "unattributable_rejects": silver_repo.count_unattributable_rejects(conn, run_id),
        "sides": sides,
    }


def snapshot(request_: RunRequest, run_id: str) -> dict:
    conn = db(request_)
    try:
        return gold_snapshot(conn, run_id)
    finally:
        conn.close()


# --------------------------------------------------------------------------- happy path


def test_happy_path_matches_expected_sample(request_):
    outcome = run_pipeline(request_)

    assert outcome.status == "SUCCEEDED"
    assert outcome.attempt == 1 and outcome.run_id == outcome.run_key[:16] + "-a1"
    assert (outcome.recon_status, outcome.exit_code) == ("FAIL", 1)  # the sample has injected defects
    conn = db(request_)
    try:
        assert summarize(conn, outcome.run_id) == {k: v for k, v in EXPECTED.items() if k not in GENERATOR_ONLY}
        assert all(c["status"] == "PASS" for c in gold_repo.fetch_check_results(conn, outcome.run_id))
        row = run_repo.get_run(conn, outcome.run_id)
    finally:
        conn.close()
    assert row["status"] == "SUCCEEDED" and row["exit_code"] == 1 and row["forced"] == 0
    assert row["finished_at"] >= row["started_at"]
    assert row["failure_reason"] is None and row["failure_detail"] is None
    assert row["source_sha256"] and row["target_sha256"] and row["reference_sha256"]


def test_happy_path_writes_run_log_and_metrics(request_):
    outcome = run_pipeline(request_)
    log = (outcome.run_dir / pipeline.RUN_LOG).read_bytes().decode("utf-8")
    assert "\r" not in log
    lines = log.splitlines()
    assert lines and all(line.startswith("ts=") and f" run_id={outcome.run_id} " in line for line in lines)
    assert any("stage=bronze event=side_rows side=SOURCE rows_in=63 rows_out=63" in line for line in lines)
    assert any("stage=silver event=side_rows side=SOURCE rows_in=63 rows_out=55 rows_rejected=4" in line
               for line in lines)
    assert "event=run_finished status=SUCCEEDED recon_status=FAIL exit_code=1" in lines[-1]

    m = outcome.metrics
    assert [s["stage"] for s in m["stages"]] == ["batch_checks", "bronze", "silver", "gold"]
    assert all(s["duration_ms"] is not None and s["duration_ms"] >= 0 for s in m["stages"])
    assert m["peak_memory_bytes"] is None  # memory profiling is off by default
    silver = next(s for s in m["stages"] if s["stage"] == "silver")
    assert silver["sides"]["TARGET"] == {"rows_in": 61, "rows_out": 59, "rows_rejected": 1, "duplicate_exact": 1,
                                         "superseded": 0, "conflicting_rows": 0}


def test_profile_memory_records_peaks(request_):
    m = run_pipeline(replace(request_, profile_memory=True)).metrics
    assert m["profile_memory"] is True
    assert all(s["peak_memory_bytes"] > 0 for s in m["stages"])
    assert m["peak_memory_bytes"] == max(s["peak_memory_bytes"] for s in m["stages"])


# --------------------------------------------------------------------------- idempotency


def test_rerun_is_skipped_and_writes_nothing_else(request_):
    first = run_pipeline(request_)
    before = counts(request_)
    dirs_before = sorted(p.name for p in request_.out_dir.iterdir())

    second = run_pipeline(request_)

    assert second.status == "SKIPPED"
    assert second.skipped_of_run_id == first.run_id and second.result_run_id == first.run_id
    assert (second.recon_status, second.exit_code) == ("FAIL", 1)
    assert second.metrics is None and second.run_dir is None
    after = counts(request_)
    assert {t: after[t] for t in DATA_TABLES} == {t: before[t] for t in DATA_TABLES}
    assert after["run"] == before["run"] + 1  # only the SKIPPED run row (DESIGN 6.2)
    assert counts(request_, second.run_id) == {t: (1 if t == "run" else 0) for t in TABLES}
    skipped = run_row(request_, second.run_id)
    assert (skipped["status"], skipped["skipped_of_run_id"], skipped["attempt"]) == ("SKIPPED", first.run_id, 2)
    assert (skipped["recon_status"], skipped["exit_code"]) == ("FAIL", 1)
    assert sorted(p.name for p in request_.out_dir.iterdir()) == dirs_before  # no new output dir


def test_force_runs_attempt_2_with_identical_gold(request_):
    first = run_pipeline(request_)
    forced = run_pipeline(replace(request_, force=True))

    assert forced.status == "SUCCEEDED" and forced.attempt == 2
    assert forced.run_id == first.run_key[:16] + "-a2" and forced.run_key == first.run_key
    assert run_row(request_, forced.run_id)["forced"] == 1
    assert snapshot(request_, forced.run_id) == snapshot(request_, first.run_id)
    assert run_row(request_, first.run_id)["status"] == "SUCCEEDED"  # earlier attempt untouched


def test_config_change_gives_new_run_key(request_):
    first = run_pipeline(request_)
    raw = json.loads(request_.config_path.read_text(encoding="utf-8"))
    raw["aggregates"]["group_by"] = list(reversed(raw["aggregates"]["group_by"]))
    changed = request_.config_path.with_name("config2.json")
    changed.write_text(json.dumps(raw), encoding="utf-8")

    second = run_pipeline(replace(request_, config_path=changed))

    assert second.run_key != first.run_key
    assert (second.status, second.attempt) == ("SUCCEEDED", 1)


# --------------------------------------------------------------------------- failure and retry


@pytest.mark.parametrize("point", pipeline.FAIL_POINTS)
def test_injected_failure_rolls_back_everything_then_retry_succeeds(request_, monkeypatch, point):
    monkeypatch.setenv(pipeline.FAIL_AT_ENV, point)
    failed = run_pipeline(request_)

    assert (failed.status, failed.exit_code, failed.recon_status) == ("FAILED", 2, None)
    row = run_row(request_, failed.run_id)
    assert row["status"] == "FAILED" and row["exit_code"] == 2 and row["finished_at"]
    assert row["failure_reason"] == "UNEXPECTED_ERROR"
    assert row["failure_detail"] == f"RuntimeError: injected failure at {point} (RECON_FAIL_AT)"
    assert counts(request_, failed.run_id) == {t: (1 if t == "run" else 0) for t in TABLES}
    log = (failed.run_dir / pipeline.RUN_LOG).read_text(encoding="utf-8")
    assert "level=ERROR" in log and "status=FAILED failure_reason=UNEXPECTED_ERROR" in log

    monkeypatch.delenv(pipeline.FAIL_AT_ENV)
    retry = run_pipeline(request_)
    assert (retry.status, retry.attempt, retry.exit_code) == ("SUCCEEDED", 2, 1)
    assert retry.run_key == failed.run_key
    assert counts(request_, retry.run_id)["key_result"] == EXPECTED["total_keys"]


def test_unknown_fail_point_is_an_error_before_any_row(request_, monkeypatch):
    monkeypatch.setenv(pipeline.FAIL_AT_ENV, "after_everything")
    with pytest.raises(ValueError, match="RECON_FAIL_AT"):
        run_pipeline(request_)
    assert not request_.db_path.exists()


def test_batch_reject_leaves_no_rows(request_):
    request_.target.write_text("order_id,cust_id\nORD-1,C1\n", encoding="utf-8")  # required columns missing
    missing_ref = request_.reference.with_name("nope.csv")

    outcome = run_pipeline(replace(request_, reference=missing_ref))

    assert (outcome.status, outcome.exit_code) == ("REJECTED", 2)
    assert counts(request_, outcome.run_id) == {t: (1 if t == "run" else 0) for t in TABLES}
    row = run_row(request_, outcome.run_id)
    assert row["status"] == "REJECTED" and row["recon_status"] is None
    assert row["failure_reason"] == "SCHEMA_MISSING_COLUMN"  # first in SOURCE -> TARGET -> REFERENCE order
    detail = json.loads(row["failure_detail"])
    assert [(d["side"], d["code"]) for d in detail] == [("TARGET", "SCHEMA_MISSING_COLUMN"),
                                                         ("REFERENCE", "FILE_NOT_FOUND")]
    assert row["reference_sha256"] is None  # a missing file has no hash (run_key uses MISSING)


# --------------------------------------------------------------------------- stale RUNNING


def _insert_running(request_: RunRequest, started_at: str, attempt: int = 1) -> str:
    """A RUNNING attempt for the request's run_key, as left behind by a killed process."""
    probe = run_pipeline(replace(request_, db_path=request_.db_path.with_name("probe.db"),
                                 out_dir=request_.out_dir / "probe", force=True))
    run_id = f"{probe.run_key[:16]}-a{attempt}"
    request_.db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = db(request_)
    try:
        create_schema(conn)
        with transaction(conn):
            run_repo.create_run(conn, run_id=run_id, run_key=probe.run_key, attempt=attempt,
                                status="RUNNING", source_file="s", target_file="t", reference_file="r",
                                config_sha256="c", started_at=started_at)
    finally:
        conn.close()
    return run_id


def test_stale_running_becomes_abandoned(request_):
    stale_id = _insert_running(request_, "2000-01-01T00:00:00Z")

    outcome = run_pipeline(request_)

    assert (outcome.status, outcome.attempt) == ("SUCCEEDED", 2)
    assert outcome.abandoned_run_ids == [stale_id]
    stale = run_row(request_, stale_id)
    assert (stale["status"], stale["failure_reason"]) == ("ABANDONED", "STALE_RUNNING")
    assert stale["finished_at"] is not None
    assert "event=abandoned" in (outcome.run_dir / pipeline.RUN_LOG).read_text(encoding="utf-8")


def test_fresh_running_is_run_in_progress_and_writes_nothing(request_):
    running_id = _insert_running(request_, utc_iso())
    before = counts(request_)

    with pytest.raises(pipeline.RunInProgress) as info:
        run_pipeline(request_)

    assert info.value.run_id == running_id
    assert counts(request_) == before
    assert run_row(request_, running_id)["status"] == "RUNNING"


def test_stale_and_fresh_running_abandons_stale_then_reports_in_progress(request_):
    stale_id = _insert_running(request_, "2000-01-01T00:00:00Z", attempt=1)
    fresh_id = _insert_running(request_, utc_iso(), attempt=2)

    with pytest.raises(pipeline.RunInProgress) as info:
        run_pipeline(request_)

    assert info.value.run_id == fresh_id
    assert info.value.abandoned_run_ids == [stale_id]
    stale = run_row(request_, stale_id)
    assert (stale["status"], stale["failure_reason"]) == ("ABANDONED", "STALE_RUNNING")  # committed
    assert run_row(request_, fresh_id)["status"] == "RUNNING"
    assert counts(request_)["run"] == 2  # no new attempt row
    assert counts(request_, stale_id) == {t: (1 if t == "run" else 0) for t in TABLES}


# --------------------------------------------------------------------------- CLI


def cli_args(request_: RunRequest, *extra: str) -> list[str]:
    return ["run", "--source", str(request_.source), "--target", str(request_.target),
            "--reference", str(request_.reference), "--config", str(request_.config_path),
            "--db", str(request_.db_path), "--out", str(request_.out_dir), *extra]


def test_cli_run_then_skip(request_, capsys):
    assert main(cli_args(request_)) == 1
    out, err = capsys.readouterr()
    fields = dict(part.split("=", 1) for part in out.split())
    run_id = fields["run_id"]
    assert (fields["status"], fields["recon_status"], fields["exit_code"]) == ("SUCCEEDED", "FAIL", "1")
    assert "event=run_finished" in err  # key=value log lines go to stderr
    metrics = json.loads((request_.out_dir / run_id / "metrics.json").read_text(encoding="utf-8"))
    assert metrics["run_id"] == run_id and metrics["status"] == "SUCCEEDED"
    assert (request_.out_dir / run_id / "run.log").is_file()

    assert main(cli_args(request_)) == 1  # SKIPPED returns the reused run's exit code
    out, err = capsys.readouterr()
    fields = dict(part.split("=", 1) for part in out.split())
    assert (fields["status"], fields["run_id"]) == ("SKIPPED", run_id)  # prints the existing run_id
    assert fields["skip_run_id"] == run_id[:-1] + "2"
    assert "event=skipped" in err


def test_cli_rejected_exits_2(request_, capsys):
    request_.source.write_bytes(b"")
    assert main(cli_args(request_)) == 2
    fields = dict(part.split("=", 1) for part in capsys.readouterr().out.split())
    assert (fields["status"], fields["failure_reason"]) == ("REJECTED", "EMPTY_FILE")


def test_cli_bad_config_exits_2_without_run_row(request_, capsys):
    request_.config_path.write_text("{", encoding="utf-8")
    assert main(cli_args(request_)) == 2
    assert "not valid JSON" in capsys.readouterr().err
    assert not request_.db_path.exists()


def test_cli_run_in_progress_exits_2(request_, capsys):
    running_id = _insert_running(request_, utc_iso())
    capsys.readouterr()
    assert main(cli_args(request_)) == 2
    assert f"run in progress: run_id={running_id}" in capsys.readouterr().err
