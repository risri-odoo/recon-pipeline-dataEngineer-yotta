"""SQL for the run table.

These functions issue statements only; the caller owns the transaction (DESIGN 6.1: the RUNNING row and
failure updates are committed on their own, the SUCCEEDED update commits with the data transaction).
"""

from __future__ import annotations

import sqlite3
from typing import Any

RUN_COLUMNS = ("run_id", "run_key", "attempt", "status", "recon_status", "exit_code", "forced",
               "skipped_of_run_id", "source_file", "target_file", "reference_file",
               "source_sha256", "target_sha256", "reference_sha256", "config_sha256", "config_json",
               "started_at", "finished_at", "failure_reason", "failure_detail")


def create_run(conn: sqlite3.Connection, **values: Any) -> None:
    """Insert one run row. Unknown column names are an error; missing ones default to NULL / DDL default."""
    unknown = sorted(set(values) - set(RUN_COLUMNS))
    if unknown:
        raise ValueError(f"unknown run column(s): {', '.join(unknown)}")
    columns = [c for c in RUN_COLUMNS if c in values]
    placeholders = ", ".join("?" for _ in columns)
    conn.execute(f"INSERT INTO run ({', '.join(columns)}) VALUES ({placeholders})",
                 [values[c] for c in columns])


def finish_run(conn: sqlite3.Connection, run_id: str, *, status: str, finished_at: str,
               recon_status: str | None = None, exit_code: int | None = None,
               failure_reason: str | None = None, failure_detail: str | None = None) -> None:
    cur = conn.execute(
        "UPDATE run SET status = ?, finished_at = ?, recon_status = ?, exit_code = ?,"
        " failure_reason = ?, failure_detail = ? WHERE run_id = ?",
        (status, finished_at, recon_status, exit_code, failure_reason, failure_detail, run_id),
    )
    if cur.rowcount != 1:
        raise LookupError(f"run {run_id!r} not found")


def get_run(conn: sqlite3.Connection, run_id: str) -> dict[str, Any] | None:
    row = conn.execute("SELECT * FROM run WHERE run_id = ?", (run_id,)).fetchone()
    return dict(row) if row is not None else None


def find_by_run_key(conn: sqlite3.Connection, run_key: str) -> list[dict[str, Any]]:
    """Every attempt for run_key, ordered by attempt."""
    rows = conn.execute("SELECT * FROM run WHERE run_key = ? ORDER BY attempt", (run_key,)).fetchall()
    return [dict(r) for r in rows]


def mark_stale_running(conn: sqlite3.Connection, run_key: str, cutoff: str, finished_at: str) -> list[str]:
    """RUNNING attempts of run_key with started_at < cutoff become ABANDONED / STALE_RUNNING.

    Timestamps are canonical 'YYYY-MM-DDTHH:MM:SSZ', so text comparison is chronological.
    Returns the abandoned run_ids, sorted.
    """
    rows = conn.execute(
        "SELECT run_id FROM run WHERE run_key = ? AND status = 'RUNNING' AND started_at < ? ORDER BY run_id",
        (run_key, cutoff),
    ).fetchall()
    run_ids = [r[0] for r in rows]
    if run_ids:
        conn.executemany(
            "UPDATE run SET status = 'ABANDONED', failure_reason = 'STALE_RUNNING', finished_at = ?"
            " WHERE run_id = ?",
            [(finished_at, rid) for rid in run_ids],
        )
    return run_ids
