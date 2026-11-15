"""Integration tests for `recon run` (recon/service/pipeline.py) on a real temp SQLite DB."""

import json
import shutil
import sqlite3
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
from tests.silver_helpers import REF_HEADER, SRC_HEADER, TGT_HEADER, order_row

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


INVARIANTS = ("SRC_ROW_ACCOUNTING", "TGT_ROW_ACCOUNTING", "KEY_UNIQUENESS", "REFERENTIAL_INTEGRITY",
              "KEY_COVERAGE", "AGGREGATE_EXPLAINED")


def _simulate_engine_bug(m: pytest.MonkeyPatch, check: str) -> None:
    """Make one repository count lie so that only `check` fails (DESIGN 4.7). KEY_UNIQUENESS cannot be
    broken through the DB (primary keys forbid it), so every bug is simulated at the repository boundary."""
    if check in ("SRC_ROW_ACCOUNTING", "TGT_ROW_ACCOUNTING"):
        side, real = ("SOURCE" if check.startswith("SRC") else "TARGET"), silver_repo.count_clean_records
        m.setattr(silver_repo, "count_clean_records", lambda c, r, s: real(c, r, s) - (s == side))
    elif check == "KEY_UNIQUENESS":
        m.setattr(gold_repo, "count_duplicate_keys", lambda c, r: (1, 0))
    elif check == "REFERENTIAL_INTEGRITY":
        m.setattr(gold_repo, "count_unknown_references", lambda c, r, columns: 1)
    elif check == "KEY_COVERAGE":
        real = gold_repo.key_coverage
        m.setattr(gold_repo, "key_coverage", lambda c, r: real(c, r) | {"missing": 1})
    else:
        m.setattr(gold_repo, "count_unexplained_aggregates", lambda c, r: 1)


@pytest.mark.parametrize("check", INVARIANTS)
def test_invariant_violation_fails_the_run_then_retry_succeeds(request_, monkeypatch, check):
    with monkeypatch.context() as m:
        _simulate_engine_bug(m, check)
        failed = run_pipeline(request_)

    assert (failed.status, failed.exit_code, failed.recon_status) == ("FAILED", 2, None)
    row = run_row(request_, failed.run_id)
    assert (row["status"], row["exit_code"]) == ("FAILED", 2)
    assert row["failure_reason"] == f"INVARIANT_VIOLATION:{check}"
    assert isinstance(json.loads(row["failure_detail"]), dict)  # the counts, as JSON
    assert counts(request_, failed.run_id) == {t: (1 if t == "run" else 0) for t in TABLES}  # full rollback

    retry = run_pipeline(request_)  # the bug is gone: same run_key, next attempt
    assert (retry.status, retry.attempt, retry.run_key) == ("SUCCEEDED", 2, failed.run_key)


def test_input_changed_mid_run_fails_then_the_new_bytes_are_a_new_run_key(request_, monkeypatch):
    real_check_inputs = pipeline.ingest.check_inputs

    def check_then_append(paths, config):  # the file changes after hashing, before landing
        checks = real_check_inputs(paths, config)
        data = request_.target.read_bytes()
        request_.target.write_bytes(data + data.splitlines(keepends=True)[-1])
        return checks

    with monkeypatch.context() as m:
        m.setattr(pipeline.ingest, "check_inputs", check_then_append)
        failed = run_pipeline(request_)

    assert (failed.status, failed.exit_code, failed.failure_reason) == ("FAILED", 2, "INPUT_CHANGED")
    row = run_row(request_, failed.run_id)
    assert (row["status"], row["failure_reason"]) == ("FAILED", "INPUT_CHANGED")
    detail = json.loads(row["failure_detail"])
    assert detail["side"] == "TARGET" and detail["expected_sha256"] == row["target_sha256"]
    assert detail["actual_sha256"] != detail["expected_sha256"]
    assert counts(request_, failed.run_id) == {t: (1 if t == "run" else 0) for t in TABLES}

    rerun = run_pipeline(request_)  # the changed file hashes differently: a new run_key, attempt 1
    assert rerun.run_key != failed.run_key
    assert (rerun.status, rerun.attempt) == ("SUCCEEDED", 1)


def test_skipped_reuses_the_latest_succeeded_attempt(request_, monkeypatch):
    first = run_pipeline(request_)                                 # a1 SUCCEEDED
    monkeypatch.setenv(pipeline.FAIL_AT_ENV, "after_silver")
    failed = run_pipeline(replace(request_, force=True))           # a2 FAILED
    monkeypatch.delenv(pipeline.FAIL_AT_ENV)
    assert (failed.status, failed.attempt) == ("FAILED", 2)

    skipped = run_pipeline(request_)                               # a3 SKIPPED: a later FAILED attempt is ignored
    assert (skipped.status, skipped.attempt, skipped.skipped_of_run_id) == ("SKIPPED", 3, first.run_id)

    forced = run_pipeline(replace(request_, force=True))           # a4 SUCCEEDED
    again = run_pipeline(request_)                                 # a5 SKIPPED, reusing a4 (the latest)
    assert (again.status, again.attempt, again.skipped_of_run_id) == ("SKIPPED", 5, forced.run_id)


def test_stale_after_minutes_is_not_part_of_the_run_key(request_):
    first = run_pipeline(request_)
    second = run_pipeline(replace(request_, stale_after_minutes=5))
    assert second.run_key == first.run_key
    assert (second.status, second.skipped_of_run_id) == ("SKIPPED", first.run_id)


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


# --------------------------------------------------------------------------- step 9: QA-pass edge cases (P1-P16)


def _write_inputs(request_: RunRequest, source, target, reference=("C1,Alice",)) -> None:
    for path, header, lines in ((request_.source, SRC_HEADER, source), (request_.target, TGT_HEADER, target),
                                (request_.reference, REF_HEADER, reference)):
        path.write_bytes(("\n".join([header, *lines]) + "\n").encode("utf-8"))


def _key_statuses(request_: RunRequest, run_id: str) -> dict[str, str]:
    conn = db(request_)
    try:
        return {k["order_id"]: k["key_status"] for k in gold_repo.fetch_key_results(conn, run_id)}
    finally:
        conn.close()


def test_amount_above_max_is_a_row_reject_and_the_run_succeeds(request_):
    _write_inputs(request_, source=[order_row("O1", amount="99999999999999999.99"),
                                    order_row("O2", amount="9999999999.99"), order_row("O3", amount="10000000000.00")],
                  target=[order_row("O1"), order_row("O2", amount="9999999999.99"), order_row("O3")])

    outcome = run_pipeline(request_)

    assert (outcome.status, outcome.recon_status, outcome.exit_code) == ("SUCCEEDED", "FAIL", 1)
    conn = db(request_)
    try:
        q = silver_repo.fetch_quarantine(conn, outcome.run_id, "SOURCE")
    finally:
        conn.close()
    assert [(r["order_id"], r["reason_code"], r["field_name"]) for r in q] == [
        ("O1", "OUT_OF_RANGE", "amount"), ("O3", "OUT_OF_RANGE", "amount")]
    assert "above maximum 9999999999.99" in q[0]["detail"]
    assert _key_statuses(request_, outcome.run_id) == {"O1": "SOURCE_REJECTED", "O2": "MATCHED", "O3": "SOURCE_REJECTED"}


@pytest.mark.parametrize("step", ["aggregates", "checks"])
def test_aggregate_overflow_fails_the_run(request_, monkeypatch, step):
    from recon.service import config as config_module
    if step == "aggregates":
        # Lift the money limit and the configured max so that two values fit int64 but their SUM does not.
        monkeypatch.setattr(config_module, "MONEY_LIMIT_MINOR", config_module.INT64_MAX)
        raw = json.loads(request_.config_path.read_text(encoding="utf-8"))
        for f in raw["fields"]:
            f.pop("max", None)
        request_.config_path.write_text(json.dumps(raw), encoding="utf-8")
        big = "50000000000000000.00"  # 5 * 10**18 minor units
        _write_inputs(request_, source=[order_row("O1", amount=big), order_row("O2", amount=big)],
                      target=[order_row("O1", amount=big)])
    else:  # the AGGREGATE_EXPLAINED breakdown sums the values a second time
        def overflow(*_args):
            raise sqlite3.OperationalError("integer overflow")
        monkeypatch.setattr(gold_repo, "aggregate_breakdown", overflow)

    failed = run_pipeline(request_)

    assert (failed.status, failed.exit_code, failed.failure_reason) == ("FAILED", 2, "AGGREGATE_OVERFLOW")
    row = run_row(request_, failed.run_id)
    assert (row["status"], row["failure_reason"], row["recon_status"]) == ("FAILED", "AGGREGATE_OVERFLOW", None)
    assert json.loads(row["failure_detail"]) == {"error": "integer overflow", "step": step}
    assert counts(request_, failed.run_id) == {t: (1 if t == "run" else 0) for t in TABLES}


def test_newer_row_with_unparseable_updated_at_shadows_the_key_instead_of_a_false_pass(request_):
    # Final-review repro: the older valid source row used to win and MATCH the target (recon PASS).
    _write_inputs(request_, source=[order_row("O1", updated_at="2026-01-01T00:00:00Z"),
                                    order_row("O1", status="CANCELLED", amount="99.00", updated_at="garbage")],
                  target=[order_row("O1", updated_at="2026-01-01T00:00:00Z")])

    outcome = run_pipeline(request_)

    assert (outcome.status, outcome.recon_status, outcome.exit_code) == ("SUCCEEDED", "FAIL", 1)
    conn = db(request_)
    try:
        [key] = gold_repo.fetch_key_results(conn, outcome.run_id)
        q = {r["line_no"]: r for r in silver_repo.fetch_quarantine(conn, outcome.run_id, "SOURCE")}
        clean_source = silver_repo.count_clean_records(conn, outcome.run_id, "SOURCE")
    finally:
        conn.close()
    assert (key["key_status"], key["source_line_no"], key["target_line_no"]) == ("SOURCE_REJECTED", 3, 2)
    assert clean_source == 0
    assert (q[2]["reason_code"], q[2]["detail"]) == ("SUPERSEDED", "shadowed by rejected line 3 (unparseable updated_at)")
    assert (q[3]["reason_code"], q[3]["field_name"], q[3]["updated_at"]) == ("BAD_TYPE", "updated_at", None)


def test_both_order_sides_empty_is_pass_flagged_by_empty_batch(request_):
    # DESIGN 4.8: a quiet day with nothing to reconcile is PASS; EMPTY_BATCH is the signal to alert on.
    _write_inputs(request_, source=[], target=[])

    outcome = run_pipeline(request_)

    assert (outcome.status, outcome.recon_status, outcome.exit_code) == ("SUCCEEDED", "PASS", 0)
    conn = db(request_)
    try:
        keys = gold_repo.fetch_key_results(conn, outcome.run_id)
        checks = {c["check_name"]: c for c in gold_repo.fetch_check_results(conn, outcome.run_id)}
    finally:
        conn.close()
    assert keys == []
    assert (checks["EMPTY_BATCH"]["status"], checks["EMPTY_BATCH"]["actual"]) == ("WARN", "2")
    assert json.loads(checks["EMPTY_BATCH"]["detail"]) == [{"columns": [], "side": "SOURCE"},
                                                          {"columns": [], "side": "TARGET"}]


def test_interrupt_marks_the_run_failed_then_reraises(request_, monkeypatch):
    def interrupt(*_args, **_kwargs):
        raise KeyboardInterrupt
    monkeypatch.setattr(pipeline.compare, "reconcile", interrupt)  # inside the data transaction

    with pytest.raises(KeyboardInterrupt):
        run_pipeline(request_)

    conn = db(request_)
    try:
        [row] = [dict(r) for r in conn.execute("SELECT * FROM run")]
    finally:
        conn.close()
    assert (row["status"], row["exit_code"], row["failure_reason"]) == ("FAILED", 2, "INTERRUPTED")
    assert row["failure_detail"] == "KeyboardInterrupt" and row["finished_at"]
    assert counts(request_, row["run_id"]) == {t: (1 if t == "run" else 0) for t in TABLES}

    monkeypatch.undo()
    retry = run_pipeline(request_)  # no stale wait: the interrupted attempt is not RUNNING
    assert (retry.status, retry.attempt) == ("SUCCEEDED", 2)


def test_header_only_reference_rejects_every_order(request_):
    _write_inputs(request_, source=[order_row("O1")], target=[order_row("O1")], reference=())

    outcome = run_pipeline(request_)

    assert (outcome.status, outcome.recon_status, outcome.exit_code) == ("SUCCEEDED", "FAIL", 1)
    conn = db(request_)
    try:
        [key] = gold_repo.fetch_key_results(conn, outcome.run_id)
        checks = {c["check_name"]: c for c in gold_repo.fetch_check_results(conn, outcome.run_id)}
        reasons = {(q["side"], q["reason_code"]) for q in silver_repo.fetch_quarantine(conn, outcome.run_id)}
    finally:
        conn.close()
    assert (key["key_status"], key["note"]) == ("SOURCE_REJECTED", "rejected on both sides")
    assert reasons == {("SOURCE", "INVALID_REFERENCE"), ("TARGET", "INVALID_REFERENCE")}
    assert checks["EMPTY_BATCH"]["status"] == "WARN"
    assert json.loads(checks["EMPTY_BATCH"]["detail"]) == [{"columns": [], "side": "REFERENCE"}]


def test_zero_byte_reference_is_rejected(request_):
    request_.reference.write_bytes(b"")
    outcome = run_pipeline(request_)
    assert (outcome.status, outcome.exit_code, outcome.failure_reason) == ("REJECTED", 2, "EMPTY_FILE")
    assert [(d["side"], d["code"]) for d in json.loads(outcome.failure_detail)] == [("REFERENCE", "EMPTY_FILE")]


def test_same_file_as_source_and_target(request_):
    # Shipped config: the target map expects warehouse column names, so the source file fails as TARGET.
    rejected = run_pipeline(replace(request_, target=request_.source))
    assert (rejected.status, rejected.failure_reason) == ("REJECTED", "SCHEMA_MISSING_COLUMN")
    assert [(d["side"], d["code"]) for d in json.loads(rejected.failure_detail)] == [("TARGET", "SCHEMA_MISSING_COLUMN")]

    # Identity mapping: the file reconciles against itself.
    raw = json.loads(request_.config_path.read_text(encoding="utf-8"))
    raw["columns"]["target"] = dict(raw["columns"]["source"])
    identity = request_.config_path.with_name("identity.json")
    identity.write_text(json.dumps(raw), encoding="utf-8")
    _write_inputs(request_, source=[order_row("O1"), order_row("O2", amount="20.00")], target=[])
    outcome = run_pipeline(replace(request_, target=request_.source, config_path=identity))
    assert (outcome.status, outcome.recon_status, outcome.exit_code) == ("SUCCEEDED", "PASS", 0)
    assert _key_statuses(request_, outcome.run_id) == {"O1": "MATCHED", "O2": "MATCHED"}
    row = run_row(request_, outcome.run_id)
    assert row["source_sha256"] == row["target_sha256"]


@pytest.mark.parametrize("started_at, expected", [
    ("2026-11-15T12:00:00Z", "in progress"),   # same second as now: not older than 0 minutes
    ("2026-11-15T11:59:59Z", "abandoned"),     # one second older: stale
])
def test_stale_after_zero_minutes_boundary(request_, monkeypatch, started_at, expected):
    from datetime import datetime, timezone

    class FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 11, 15, 12, 0, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(pipeline, "datetime", FrozenDatetime)
    running_id = _insert_running(request_, started_at)
    zero = replace(request_, stale_after_minutes=0)

    if expected == "in progress":
        with pytest.raises(pipeline.RunInProgress) as info:
            run_pipeline(zero)
        assert info.value.run_id == running_id
        assert run_row(request_, running_id)["status"] == "RUNNING"
    else:
        outcome = run_pipeline(zero)
        assert (outcome.status, outcome.attempt, outcome.abandoned_run_ids) == ("SUCCEEDED", 2, [running_id])
        assert run_row(request_, running_id)["status"] == "ABANDONED"


def test_locked_database_exits_2_and_writes_no_run_row(request_, monkeypatch, capsys):
    from recon.repository import db as db_module
    monkeypatch.setattr(db_module, "BUSY_TIMEOUT_MS", 50)  # do not wait the default 5 s
    request_.db_path.parent.mkdir(parents=True, exist_ok=True)
    blocker = connect(request_.db_path)
    try:
        create_schema(blocker)
        with transaction(blocker):  # another process holds the write lock for the whole invocation
            assert main(cli_args(request_)) == 2
    finally:
        blocker.close()
    assert "database is locked" in capsys.readouterr().err
    assert counts(request_)["run"] == 0  # blocked at admission: no run row, nothing half-written

    outcome = run_pipeline(request_)  # once the lock is gone, the same inputs run normally
    assert (outcome.status, outcome.attempt) == ("SUCCEEDED", 1)


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
