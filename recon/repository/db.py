"""SQLite connection management."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

BUSY_TIMEOUT_MS = 5000


def connect(path: str | Path) -> sqlite3.Connection:
    """Open the recon DB in autocommit mode; transactions are explicit via `transaction()`."""
    conn = sqlite3.connect(str(path), isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
    return conn


def explain_query_plan(conn: sqlite3.Connection, sql: str) -> list[str]:
    """The EXPLAIN QUERY PLAN detail lines of `sql`, in plan order (dev tooling: tools/explain_gold.py)."""
    return [row[3] for row in conn.execute("EXPLAIN QUERY PLAN " + sql).fetchall()]


def is_integer_overflow(exc: BaseException) -> bool:
    """True for SQLite's 'integer overflow' (raised by SUM() when a 64-bit integer total overflows)."""
    return isinstance(exc, sqlite3.OperationalError) and "integer overflow" in str(exc)


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """BEGIN IMMEDIATE ... COMMIT; ROLLBACK and re-raise on any exception (including BaseException)."""
    if conn.in_transaction:
        raise RuntimeError("transaction() does not nest: a transaction is already open")
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    conn.execute("COMMIT")
