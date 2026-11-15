"""SQL for BRONZE tables: raw_record."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from typing import Any

RawRow = tuple[str, str, int, str, str, str]  # run_id, side, line_no, source_file, record_type, raw_text


def insert_raw_records(conn: sqlite3.Connection, rows: Iterable[RawRow]) -> None:
    """Insert one batch of raw_record rows (the caller chunks and owns the transaction)."""
    conn.executemany(
        "INSERT INTO raw_record (run_id, side, line_no, source_file, record_type, raw_text)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )


def fetch_raw_records(conn: sqlite3.Connection, run_id: str, side: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT run_id, side, line_no, source_file, record_type, raw_text FROM raw_record"
        " WHERE run_id = ? AND side = ? ORDER BY line_no",
        (run_id, side),
    ).fetchall()
    return [dict(r) for r in rows]


def count_raw_records(conn: sqlite3.Connection, run_id: str, side: str | None = None,
                      record_type: str | None = None) -> int:
    sql = "SELECT COUNT(*) FROM raw_record WHERE run_id = ?"
    params: list[Any] = [run_id]
    if side is not None:
        sql += " AND side = ?"
        params.append(side)
    if record_type is not None:
        sql += " AND record_type = ?"
        params.append(record_type)
    return conn.execute(sql, params).fetchone()[0]
