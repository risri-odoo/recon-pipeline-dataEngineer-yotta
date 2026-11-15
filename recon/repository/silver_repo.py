"""SQL for SILVER tables: clean_record, ref_customer, quarantine (docs/DESIGN.md section 3).

Writers take dicts keyed by column name and insert them with executemany; the caller owns the
transaction. Readers return rows in a deterministic order (business key, or line_no).
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable, Mapping
from typing import Any

# clean_record.order_id holds the business key (joined with '|' when the config key is composite).
KEY_COLUMN = "order_id"

CLEAN_COLUMNS = ("run_id", "side", "order_id", "customer_id", "order_ts", "status", "amount_minor",
                 "currency", "discount_minor", "updated_at", "payload_sha256", "source_file", "line_no")
QUARANTINE_COLUMNS = ("run_id", "side", "line_no", "source_file", "raw_text", "category", "reason_code",
                      "field_name", "order_id", "updated_at", "detail")
REF_CUSTOMER_COLUMNS = ("run_id", "customer_id", "customer_name")


def _insert_sql(table: str, columns: tuple[str, ...]) -> str:
    return (f"INSERT INTO {table} ({', '.join(columns)})"
            f" VALUES ({', '.join(':' + c for c in columns)})")


_INSERT_CLEAN = _insert_sql("clean_record", CLEAN_COLUMNS)
_INSERT_QUARANTINE = _insert_sql("quarantine", QUARANTINE_COLUMNS)
_INSERT_REF_CUSTOMER = _insert_sql("ref_customer", REF_CUSTOMER_COLUMNS)


def insert_clean_records(conn: sqlite3.Connection, rows: Iterable[Mapping[str, Any]]) -> None:
    """Insert clean_record rows. Each row must provide every column in CLEAN_COLUMNS (extra keys ignored)."""
    conn.executemany(_INSERT_CLEAN, rows)


def insert_quarantine(conn: sqlite3.Connection, rows: Iterable[Mapping[str, Any]]) -> None:
    """Insert quarantine rows. Each row must provide every column in QUARANTINE_COLUMNS."""
    conn.executemany(_INSERT_QUARANTINE, rows)


def insert_ref_customers(conn: sqlite3.Connection, rows: Iterable[Mapping[str, Any]]) -> None:
    conn.executemany(_INSERT_REF_CUSTOMER, rows)


def fetch_clean_records(conn: sqlite3.Connection, run_id: str, side: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        f"SELECT {', '.join(CLEAN_COLUMNS)} FROM clean_record WHERE run_id = ? AND side = ? ORDER BY order_id",
        (run_id, side),
    ).fetchall()
    return [dict(r) for r in rows]


def fetch_quarantine(conn: sqlite3.Connection, run_id: str, side: str | None = None) -> list[dict[str, Any]]:
    """Quarantine rows of a run (optionally one side), ordered by side then line_no."""
    sql = f"SELECT {', '.join(QUARANTINE_COLUMNS)} FROM quarantine WHERE run_id = ?"
    params: list[Any] = [run_id]
    if side is not None:
        sql += " AND side = ?"
        params.append(side)
    rows = conn.execute(sql + " ORDER BY side, line_no", params).fetchall()
    return [dict(r) for r in rows]


def fetch_ref_customers(conn: sqlite3.Connection, run_id: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT run_id, customer_id, customer_name FROM ref_customer WHERE run_id = ? ORDER BY customer_id",
        (run_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def count_clean_records(conn: sqlite3.Connection, run_id: str, side: str) -> int:
    return conn.execute("SELECT COUNT(*) FROM clean_record WHERE run_id = ? AND side = ?",
                        (run_id, side)).fetchone()[0]


def count_quarantine(conn: sqlite3.Connection, run_id: str, side: str, category: str | None = None) -> int:
    sql = "SELECT COUNT(*) FROM quarantine WHERE run_id = ? AND side = ?"
    params: list[Any] = [run_id, side]
    if category is not None:
        sql += " AND category = ?"
        params.append(category)
    return conn.execute(sql, params).fetchone()[0]
