"""DDL for BRONZE, SILVER and GOLD tables (docs/DESIGN.md section 3). Idempotent: CREATE ... IF NOT EXISTS."""

from __future__ import annotations

import sqlite3

from recon.repository.db import transaction

_SIDES = "('SOURCE', 'TARGET', 'REFERENCE')"
_ORDER_SIDES = "('SOURCE', 'TARGET')"

DDL: tuple[str, ...] = (
    # ------------------------------------------------------------------ BRONZE
    """
    CREATE TABLE IF NOT EXISTS run (
        run_id            TEXT PRIMARY KEY,
        run_key           TEXT NOT NULL,
        attempt           INTEGER NOT NULL CHECK (attempt >= 1),
        status            TEXT NOT NULL CHECK (status IN
                              ('RUNNING', 'SUCCEEDED', 'FAILED', 'REJECTED', 'SKIPPED', 'ABANDONED')),
        recon_status      TEXT CHECK (recon_status IN ('PASS', 'FAIL')),
        exit_code         INTEGER CHECK (exit_code IN (0, 1, 2)),
        forced            INTEGER NOT NULL DEFAULT 0 CHECK (forced IN (0, 1)),
        skipped_of_run_id TEXT REFERENCES run (run_id),
        source_file       TEXT NOT NULL,
        target_file       TEXT NOT NULL,
        reference_file    TEXT NOT NULL,
        source_sha256     TEXT,
        target_sha256     TEXT,
        reference_sha256  TEXT,
        config_sha256     TEXT NOT NULL,
        config_json       TEXT,
        started_at        TEXT NOT NULL,
        finished_at       TEXT,
        failure_reason    TEXT,
        failure_detail    TEXT,
        UNIQUE (run_key, attempt)
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS raw_record (
        run_id      TEXT NOT NULL REFERENCES run (run_id),
        side        TEXT NOT NULL CHECK (side IN {_SIDES}),
        line_no     INTEGER NOT NULL CHECK (line_no >= 1),
        source_file TEXT NOT NULL,
        record_type TEXT NOT NULL CHECK (record_type IN ('HEADER', 'DATA')),
        raw_text    TEXT NOT NULL,
        PRIMARY KEY (run_id, side, line_no)
    )
    """,
    # ------------------------------------------------------------------ SILVER
    f"""
    CREATE TABLE IF NOT EXISTS clean_record (
        run_id         TEXT NOT NULL REFERENCES run (run_id),
        side           TEXT NOT NULL CHECK (side IN {_ORDER_SIDES}),
        order_id       TEXT NOT NULL,
        customer_id    TEXT NOT NULL,
        order_ts       TEXT NOT NULL,
        status         TEXT NOT NULL,
        amount_minor   INTEGER NOT NULL,
        currency       TEXT NOT NULL,
        discount_minor INTEGER,
        updated_at     TEXT NOT NULL,
        payload_sha256 TEXT NOT NULL,
        source_file    TEXT NOT NULL,
        line_no        INTEGER NOT NULL,
        PRIMARY KEY (run_id, side, order_id),
        FOREIGN KEY (run_id, side, line_no) REFERENCES raw_record (run_id, side, line_no)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ref_customer (
        run_id        TEXT NOT NULL REFERENCES run (run_id),
        customer_id   TEXT NOT NULL,
        customer_name TEXT,
        PRIMARY KEY (run_id, customer_id)
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS quarantine (
        run_id      TEXT NOT NULL REFERENCES run (run_id),
        side        TEXT NOT NULL CHECK (side IN {_SIDES}),
        line_no     INTEGER NOT NULL CHECK (line_no >= 2),
        source_file TEXT NOT NULL,
        raw_text    TEXT NOT NULL,
        category    TEXT NOT NULL CHECK (category IN ('ROW_REJECT', 'DUPLICATE')),
        reason_code TEXT NOT NULL,
        field_name  TEXT,
        order_id    TEXT,
        updated_at  TEXT,
        detail      TEXT NOT NULL,
        PRIMARY KEY (run_id, side, line_no),
        FOREIGN KEY (run_id, side, line_no) REFERENCES raw_record (run_id, side, line_no)
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_quarantine_key ON quarantine (run_id, side, order_id)",
    # ------------------------------------------------------------------ GOLD
    """
    CREATE TABLE IF NOT EXISTS key_result (
        run_id            TEXT NOT NULL REFERENCES run (run_id),
        order_id          TEXT NOT NULL,
        key_status        TEXT NOT NULL CHECK (key_status IN
                              ('CONFLICT', 'MATCHED', 'MISMATCHED', 'SOURCE_REJECTED', 'TARGET_REJECTED',
                               'MISSING_IN_TARGET', 'EXTRA_IN_TARGET')),
        source_line_no    INTEGER,
        target_line_no    INTEGER,
        source_updated_at TEXT,
        target_updated_at TEXT,
        diff_count        INTEGER NOT NULL CHECK (diff_count >= 0),
        note              TEXT,
        PRIMARY KEY (run_id, order_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_key_result_status ON key_result (run_id, key_status)",
    """
    CREATE TABLE IF NOT EXISTS field_diff (
        run_id       TEXT NOT NULL,
        order_id     TEXT NOT NULL,
        field_name   TEXT NOT NULL,
        source_value TEXT,
        target_value TEXT,
        delta_minor  INTEGER,
        diff_type    TEXT NOT NULL CHECK (diff_type IN
                         ('NULL_MISMATCH', 'TARGET_STALE', 'TARGET_AHEAD', 'VALUE_MISMATCH')),
        PRIMARY KEY (run_id, order_id, field_name),
        FOREIGN KEY (run_id, order_id) REFERENCES key_result (run_id, order_id)
    )
    """,
    # metric and group_by come from the config (ROW_COUNT / SUM_<field>, ALL / <field>) and are validated
    # in config.py, so they carry no CHECK; only fixed catalogues are enforced in the DDL.
    """
    CREATE TABLE IF NOT EXISTS aggregate_result (
        run_id            TEXT NOT NULL REFERENCES run (run_id),
        metric            TEXT NOT NULL,
        group_by          TEXT NOT NULL,
        group_value       TEXT NOT NULL,
        source_value      INTEGER NOT NULL,
        target_value      INTEGER NOT NULL,
        delta             INTEGER NOT NULL,
        explained_delta   INTEGER NOT NULL,
        unexplained_delta INTEGER NOT NULL,
        PRIMARY KEY (run_id, metric, group_by, group_value)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS check_result (
        run_id     TEXT NOT NULL REFERENCES run (run_id),
        check_name TEXT NOT NULL,
        severity   TEXT NOT NULL CHECK (severity IN ('INVARIANT', 'WARN')),
        status     TEXT NOT NULL CHECK (status IN ('PASS', 'WARN')),
        expected   TEXT,
        actual     TEXT,
        detail     TEXT NOT NULL,
        PRIMARY KEY (run_id, check_name)
    )
    """,
)

TABLES = ("run", "raw_record", "clean_record", "ref_customer", "quarantine",
          "key_result", "field_diff", "aggregate_result", "check_result")


def create_schema(conn: sqlite3.Connection) -> None:
    """Create every table and index if missing, in one transaction."""
    with transaction(conn):
        for statement in DDL:
            conn.execute(statement)


def table_row_counts(conn: sqlite3.Connection, run_id: str | None = None) -> dict[str, int]:
    """Row count of every table in TABLES, optionally only the rows of one run_id."""
    where, params = (" WHERE run_id = ?", (run_id,)) if run_id is not None else ("", ())
    return {table: conn.execute(f"SELECT COUNT(*) FROM {table}{where}", params).fetchone()[0]
            for table in TABLES}


def list_tables(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name").fetchall()
    return [row[0] for row in rows]
