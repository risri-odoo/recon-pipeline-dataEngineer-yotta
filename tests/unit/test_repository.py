import sqlite3

import pytest

from recon.repository import bronze_repo, gold_repo, run_repo, silver_repo
from recon.repository.db import connect, transaction
from recon.repository.schema import TABLES, create_schema, list_tables

BASE_RUN = dict(run_key="key1", status="RUNNING", source_file="s", target_file="t", reference_file="r",
                config_sha256="c")


@pytest.fixture
def conn(tmp_path):
    c = connect(tmp_path / "recon.db")
    create_schema(c)
    yield c
    c.close()


def add_run(conn, run_id, attempt=1, started_at="2026-11-15T10:00:00Z", **extra):
    with transaction(conn):
        run_repo.create_run(conn, run_id=run_id, attempt=attempt, started_at=started_at, **(BASE_RUN | extra))


def test_schema_creates_every_table_and_is_idempotent(conn):
    create_schema(conn)
    assert set(TABLES) <= set(list_tables(conn))


def test_connection_pragmas(conn):
    assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
    assert conn.execute("PRAGMA busy_timeout").fetchone()[0] > 0


def test_transaction_rolls_back_on_exception(conn):
    add_run(conn, "r1")
    with pytest.raises(RuntimeError):
        with transaction(conn):
            bronze_repo.insert_raw_records(conn, [("r1", "SOURCE", 1, "s", "HEADER", "h")])
            raise RuntimeError("boom")
    assert bronze_repo.count_raw_records(conn, "r1") == 0
    assert not conn.in_transaction


def test_transaction_commits(conn):
    add_run(conn, "r1")
    with transaction(conn):
        bronze_repo.insert_raw_records(conn, [("r1", "SOURCE", 1, "s", "HEADER", "h")])
    assert bronze_repo.count_raw_records(conn, "r1") == 1


def test_transaction_does_not_nest(conn):
    with transaction(conn):
        with pytest.raises(RuntimeError):
            with transaction(conn):
                pass


def test_foreign_key_to_run_is_enforced(conn):
    with pytest.raises(sqlite3.IntegrityError):
        with transaction(conn):
            bronze_repo.insert_raw_records(conn, [("no-such-run", "SOURCE", 1, "s", "HEADER", "h")])


def test_check_constraint_rejects_unknown_side(conn):
    add_run(conn, "r1")
    with pytest.raises(sqlite3.IntegrityError):
        with transaction(conn):
            bronze_repo.insert_raw_records(conn, [("r1", "LEFT", 1, "s", "HEADER", "h")])


def test_create_find_and_finish_run(conn):
    add_run(conn, "key1-a2", attempt=2)
    add_run(conn, "key1-a1", attempt=1, forced=0)
    assert [r["attempt"] for r in run_repo.find_by_run_key(conn, "key1")] == [1, 2]
    with transaction(conn):
        run_repo.finish_run(conn, "key1-a1", status="SUCCEEDED", finished_at="2026-11-15T10:01:00Z",
                            recon_status="FAIL", exit_code=1)
    row = run_repo.get_run(conn, "key1-a1")
    assert (row["status"], row["recon_status"], row["exit_code"], row["finished_at"]) == (
        "SUCCEEDED", "FAIL", 1, "2026-11-15T10:01:00Z")
    assert run_repo.find_by_run_key(conn, "other") == []


def test_finish_unknown_run_raises(conn):
    with pytest.raises(LookupError):
        run_repo.finish_run(conn, "nope", status="FAILED", finished_at="2026-11-15T10:00:00Z")


def test_create_run_rejects_unknown_column(conn):
    with pytest.raises(ValueError):
        run_repo.create_run(conn, run_id="x", bogus=1)


def test_run_key_attempt_is_unique(conn):
    add_run(conn, "a")
    with pytest.raises(sqlite3.IntegrityError):
        add_run(conn, "b", attempt=1)


def test_mark_stale_running(conn):
    add_run(conn, "old", attempt=1, started_at="2026-11-15T08:00:00Z")
    add_run(conn, "new", attempt=2, started_at="2026-11-15T09:30:00Z")
    add_run(conn, "done", attempt=3, started_at="2026-11-15T07:00:00Z", status="SUCCEEDED")
    with transaction(conn):
        abandoned = run_repo.mark_stale_running(conn, "key1", cutoff="2026-11-15T09:00:00Z",
                                                finished_at="2026-11-15T10:00:00Z")
    assert abandoned == ["old"]
    old = run_repo.get_run(conn, "old")
    assert (old["status"], old["failure_reason"], old["finished_at"]) == (
        "ABANDONED", "STALE_RUNNING", "2026-11-15T10:00:00Z")
    assert run_repo.get_run(conn, "new")["status"] == "RUNNING"
    assert run_repo.get_run(conn, "done")["status"] == "SUCCEEDED"


def _clean_row(run_id, side, line_no, order_id, discount_minor):
    return {"run_id": run_id, "side": side, "order_id": order_id, "customer_id": "C1",
            "order_ts": "2026-01-01T00:00:00Z", "status": "PLACED", "amount_minor": 1000, "currency": "USD",
            "discount_minor": discount_minor, "updated_at": "2026-01-01T01:00:00Z", "payload_sha256": "p",
            "source_file": side.lower(), "line_no": line_no}


def test_aggregate_null_group_value_and_config_derived_group_by(conn):
    # group_by 'DISCOUNT' is not one of the orders-config groupings: the DDL no longer hardcodes them.
    add_run(conn, "r1")
    rows = [("SOURCE", 2, "O1", None), ("SOURCE", 3, "O2", 150), ("TARGET", 2, "O1", None)]
    with transaction(conn):
        bronze_repo.insert_raw_records(conn, [("r1", side, line, side.lower(), "DATA", "x")
                                              for side, line, _, _ in rows])
        silver_repo.insert_clean_records(conn, [_clean_row("r1", *r) for r in rows])
        gold_repo.insert_key_results(conn, "r1")
        gold_repo.insert_aggregates(conn, "r1", [gold_repo.Measure("ROW_COUNT", None)],
                                    [gold_repo.Grouping("DISCOUNT", "discount_minor")])
    got = [(a["metric"], a["group_by"], a["group_value"], a["source_value"], a["target_value"], a["delta"],
            a["explained_delta"], a["unexplained_delta"]) for a in gold_repo.fetch_aggregates(conn, "r1")]
    assert got == [("ROW_COUNT", "DISCOUNT", "150", 1, 0, 1, 1, 0),
                   ("ROW_COUNT", "DISCOUNT", "NULL", 1, 1, 0, 0, 0)]
