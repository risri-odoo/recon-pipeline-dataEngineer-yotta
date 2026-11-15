"""SQL for GOLD tables: key_result, field_diff, aggregate_result, check_result (docs/DESIGN.md 3, 4.5-4.7).

Every comparison is one set-based statement over clean_record / quarantine for a single run_id; nothing
here loops over rows in Python. Statements that depend on the config (compared fields, aggregate measures
and groupings) are generated from small spec objects whose identifiers are checked before they are
interpolated. Values are always bound parameters. The caller owns the transaction.

Write order inside one run (field_diff has a foreign key to key_result):
    insert_key_results -> insert_field_diffs -> apply_diff_counts -> insert_aggregates -> insert_check_results
"""

from __future__ import annotations

import re
import sqlite3
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

FORMAT_MINOR_SQL = "recon_format_minor"  # SQLite function: (minor_units INTEGER, scale INTEGER) -> TEXT

KEY_RESULT_COLUMNS = ("run_id", "order_id", "key_status", "source_line_no", "target_line_no",
                      "source_updated_at", "target_updated_at", "diff_count", "note")
FIELD_DIFF_COLUMNS = ("run_id", "order_id", "field_name", "source_value", "target_value", "delta_minor",
                      "diff_type")
AGGREGATE_COLUMNS = ("run_id", "metric", "group_by", "group_value", "source_value", "target_value", "delta",
                     "explained_delta", "unexplained_delta")
CHECK_COLUMNS = ("run_id", "check_name", "severity", "status", "expected", "actual", "detail")

ALL_GROUP = "ALL"
ALL_GROUP_VALUE = "*"
NULL_GROUP_VALUE = "NULL"
BOTH_REJECTED_NOTE = "rejected on both sides"

_IDENT = re.compile(r"^[a-z][a-z0-9_]*$")
_LABEL = re.compile(r"^[A-Z][A-Z0-9_]*$")


def _ident(name: str) -> str:
    """A column name that is about to be interpolated into SQL (config-derived, so checked here too)."""
    if not _IDENT.match(name):
        raise ValueError(f"not a safe SQL identifier: {name!r}")
    return name


def _label(name: str) -> str:
    if not _LABEL.match(name):
        raise ValueError(f"not an UPPER_SNAKE label: {name!r}")
    return name


# --------------------------------------------------------------------------- specs


@dataclass(frozen=True)
class CompareField:
    """One compared logical field. `money` fields compare minor units within `tolerance_minor` (inclusive)."""

    name: str  # logical name, stored in field_diff.field_name
    column: str  # clean_record column (money columns carry the _minor suffix)
    money: bool = False
    tolerance_minor: int = 0
    scale: int = 0


@dataclass(frozen=True)
class Measure:
    """An aggregate metric. column None = ROW_COUNT; otherwise SUM over that column, NULLs ignored."""

    metric: str
    column: str | None


@dataclass(frozen=True)
class Grouping:
    """An aggregate grouping. column None = the single ALL group ('*')."""

    group_by: str
    column: str | None


# --------------------------------------------------------------------------- functions


def register_functions(conn: sqlite3.Connection, format_minor: Callable[[int, int], str]) -> None:
    """Register recon_format_minor(units, scale) on this connection. NULL in -> NULL out; no float involved."""

    def _fmt(units: Any, scale: Any) -> str | None:
        return None if units is None else format_minor(int(units), int(scale))

    conn.create_function(FORMAT_MINOR_SQL, 2, _fmt, deterministic=True)


# --------------------------------------------------------------------------- key_result

_INSERT_KEY_RESULTS = f"""
INSERT INTO key_result ({', '.join(KEY_RESULT_COLUMNS)})
WITH candidate AS (
    -- every row that can decide a side of a key: the clean winner, conflicts, row rejects
    SELECT side, order_id, line_no, updated_at, 'CLEAN' AS kind, 0 AS priority
    FROM clean_record
    WHERE run_id = :run_id
    UNION ALL
    SELECT side, order_id, line_no, updated_at,
           CASE WHEN reason_code = 'CONFLICTING_DUPLICATE' THEN 'CONFLICT'
                WHEN category = 'ROW_REJECT' THEN 'REJECT'
                ELSE 'OTHER' END,
           CASE WHEN reason_code = 'CONFLICTING_DUPLICATE' THEN 1
                WHEN category = 'ROW_REJECT' THEN 2
                ELSE 3 END
    FROM quarantine
    WHERE run_id = :run_id AND side IN ('SOURCE', 'TARGET') AND order_id IS NOT NULL
),
flags AS (
    SELECT order_id,
           MAX(side = 'SOURCE' AND kind = 'CLEAN')  AS s_clean,
           MAX(side = 'TARGET' AND kind = 'CLEAN')  AS t_clean,
           MAX(side = 'SOURCE' AND kind = 'REJECT') AS s_reject,
           MAX(side = 'TARGET' AND kind = 'REJECT') AS t_reject,
           MAX(kind = 'CONFLICT')                   AS conflict
    FROM candidate
    GROUP BY order_id
),
deciding AS (
    -- per (side, key): clean row, else the lowest conflicting line, else the newest reject (lowest line on a tie)
    SELECT side, order_id, line_no, updated_at
    FROM (SELECT c.*, ROW_NUMBER() OVER (PARTITION BY side, order_id
                                         ORDER BY priority, updated_at DESC, line_no) AS rn
          FROM candidate AS c
          WHERE kind <> 'OTHER')
    WHERE rn = 1
)
SELECT :run_id, f.order_id,
       CASE WHEN f.conflict                     THEN 'CONFLICT'
            WHEN f.s_clean AND f.t_clean        THEN 'MATCHED'  -- MISMATCHED is set by apply_diff_counts
            WHEN NOT f.s_clean AND f.s_reject   THEN 'SOURCE_REJECTED'
            WHEN NOT f.t_clean AND f.t_reject   THEN 'TARGET_REJECTED'
            WHEN f.s_clean                      THEN 'MISSING_IN_TARGET'
            WHEN f.t_clean                      THEN 'EXTRA_IN_TARGET'
       END,
       s.line_no, t.line_no, s.updated_at, t.updated_at, 0,
       CASE WHEN NOT f.conflict AND NOT f.s_clean AND f.s_reject AND NOT f.t_clean AND f.t_reject
            THEN '{BOTH_REJECTED_NOTE}' END
FROM flags AS f
LEFT JOIN deciding AS s ON s.side = 'SOURCE' AND s.order_id = f.order_id
LEFT JOIN deciding AS t ON t.side = 'TARGET' AND t.order_id = f.order_id
"""


def insert_key_results(conn: sqlite3.Connection, run_id: str) -> int:
    """One key_result per key in clean_record or keyed SOURCE/TARGET quarantine (DESIGN 4.5). Returns rows."""
    return conn.execute(_INSERT_KEY_RESULTS, {"run_id": run_id}).rowcount


_APPLY_DIFF_COUNT = """
UPDATE key_result
SET diff_count = (SELECT COUNT(*) FROM field_diff AS d
                  WHERE d.run_id = key_result.run_id AND d.order_id = key_result.order_id)
WHERE run_id = :run_id AND key_status = 'MATCHED'
"""
_APPLY_MISMATCHED = """
UPDATE key_result SET key_status = 'MISMATCHED'
WHERE run_id = :run_id AND key_status = 'MATCHED' AND diff_count > 0
"""


def apply_diff_counts(conn: sqlite3.Connection, run_id: str) -> None:
    """Set diff_count on both-sides keys and turn the ones with >= 1 field_diff into MISMATCHED."""
    conn.execute(_APPLY_DIFF_COUNT, {"run_id": run_id})
    conn.execute(_APPLY_MISMATCHED, {"run_id": run_id})


# --------------------------------------------------------------------------- field_diff

_DIFF_TYPE = """CASE WHEN ({s} IS NULL) <> ({t} IS NULL)  THEN 'NULL_MISMATCH'
                WHEN t.updated_at < s.updated_at      THEN 'TARGET_STALE'
                WHEN t.updated_at > s.updated_at      THEN 'TARGET_AHEAD'
                ELSE 'VALUE_MISMATCH' END"""


def _field_diff_branch(field: CompareField) -> str:
    col = _ident(field.column)
    name = _ident(field.name)
    s, t = f"s.{col}", f"t.{col}"
    if field.money:
        values = (f"{FORMAT_MINOR_SQL}({s}, :scale_{name}), {FORMAT_MINOR_SQL}({t}, :scale_{name}),"
                  f" {s} - {t}")
        differs = f"(({s} IS NULL) <> ({t} IS NULL) OR ABS({s} - {t}) > :tolerance_{name})"
    else:
        values = f"{s}, {t}, NULL"
        differs = f"{s} IS NOT {t}"
    return f"""SELECT s.run_id, s.order_id, '{name}', {values},
       {_DIFF_TYPE.format(s=s, t=t)}
FROM clean_record AS s
JOIN clean_record AS t ON t.run_id = s.run_id AND t.side = 'TARGET' AND t.order_id = s.order_id
WHERE s.run_id = :run_id AND s.side = 'SOURCE'
  AND {differs}"""


def field_diff_sql(fields: Sequence[CompareField]) -> str:
    """The single INSERT ... SELECT for field_diff: one UNION ALL branch per compared field."""
    if not fields:
        raise ValueError("at least one compare field is required")
    branches = "\nUNION ALL\n".join(_field_diff_branch(f) for f in fields)
    return f"INSERT INTO field_diff ({', '.join(FIELD_DIFF_COLUMNS)})\n{branches}\n"


def field_diff_params(run_id: str, fields: Sequence[CompareField]) -> dict[str, Any]:
    params: dict[str, Any] = {"run_id": run_id}
    for f in fields:
        if f.money:
            params[f"scale_{f.name}"] = f.scale
            params[f"tolerance_{f.name}"] = f.tolerance_minor
    return params


def insert_field_diffs(conn: sqlite3.Connection, run_id: str, fields: Sequence[CompareField]) -> int:
    """Field-level diffs for keys clean on both sides (DESIGN 4.6). Requires register_functions()."""
    return conn.execute(field_diff_sql(fields), field_diff_params(run_id, fields)).rowcount


# --------------------------------------------------------------------------- aggregate_result


def _measure_expr(measure: Measure, alias: str) -> str:
    return "1" if measure.column is None else f"{alias}.{_ident(measure.column)}"


def _group_expr(grouping: Grouping, alias: str) -> str:
    if grouping.column is None:
        return f"'{ALL_GROUP_VALUE}'"
    return f"COALESCE(CAST({alias}.{_ident(grouping.column)} AS TEXT), '{NULL_GROUP_VALUE}')"


def aggregate_sql(measure: Measure, grouping: Grouping) -> str:
    """One aggregate_result statement: SQL SUM per side, and the delta rebuilt by walking key_result."""
    m_c, m_s, m_t = (_measure_expr(measure, a) for a in ("c", "s", "t"))
    g_c, g_s, g_t = (_group_expr(grouping, a) for a in ("c", "s", "t"))
    always_all = f"\n    UNION SELECT '{ALL_GROUP_VALUE}'" if grouping.column is None else ""
    return f"""
INSERT INTO aggregate_result ({', '.join(AGGREGATE_COLUMNS)})
WITH actual AS (
    SELECT {g_c} AS group_value,
           COALESCE(SUM(CASE WHEN c.side = 'SOURCE' THEN {m_c} END), 0) AS source_value,
           COALESCE(SUM(CASE WHEN c.side = 'TARGET' THEN {m_c} END), 0) AS target_value
    FROM clean_record AS c
    WHERE c.run_id = :run_id
    GROUP BY 1
),
walk AS (
    SELECT {g_s} AS group_value, COALESCE({m_s}, 0) AS contribution
    FROM key_result AS k
    JOIN clean_record AS s ON s.run_id = k.run_id AND s.side = 'SOURCE' AND s.order_id = k.order_id
    WHERE k.run_id = :run_id
    UNION ALL
    SELECT {g_t}, -COALESCE({m_t}, 0)
    FROM key_result AS k
    JOIN clean_record AS t ON t.run_id = k.run_id AND t.side = 'TARGET' AND t.order_id = k.order_id
    WHERE k.run_id = :run_id
),
explained AS (
    SELECT group_value, SUM(contribution) AS explained_delta FROM walk GROUP BY group_value
),
groups AS (
    SELECT group_value FROM actual
    UNION SELECT group_value FROM explained{always_all}
)
SELECT :run_id, :metric, :group_by, g.group_value,
       COALESCE(a.source_value, 0), COALESCE(a.target_value, 0),
       COALESCE(a.source_value, 0) - COALESCE(a.target_value, 0),
       COALESCE(e.explained_delta, 0),
       COALESCE(a.source_value, 0) - COALESCE(a.target_value, 0) - COALESCE(e.explained_delta, 0)
FROM groups AS g
LEFT JOIN actual AS a ON a.group_value = g.group_value
LEFT JOIN explained AS e ON e.group_value = g.group_value
"""


def insert_aggregates(conn: sqlite3.Connection, run_id: str, measures: Sequence[Measure],
                      groupings: Sequence[Grouping]) -> None:
    """One set-based statement per (measure, grouping)."""
    for measure in measures:
        for grouping in groupings:
            conn.execute(aggregate_sql(measure, grouping),
                         {"run_id": run_id, "metric": _label(measure.metric),
                          "group_by": _label(grouping.group_by)})


def aggregate_breakdown(conn: sqlite3.Connection, run_id: str, measure: Measure) -> dict[str, int]:
    """Overall delta of one measure split by where each key has clean rows (walks key_result).

    delta = source_only - target_only + both_sides, where both_sides = sum(src - tgt) over keys clean on
    both sides. NULL measure values count as 0, matching SUM ignoring NULLs.
    """
    m_s, m_t = _measure_expr(measure, "s"), _measure_expr(measure, "t")
    row = conn.execute(f"""
        SELECT COALESCE(SUM(CASE WHEN s.order_id IS NOT NULL AND t.order_id IS NULL
                                 THEN COALESCE({m_s}, 0) END), 0),
               COALESCE(SUM(CASE WHEN s.order_id IS NULL AND t.order_id IS NOT NULL
                                 THEN COALESCE({m_t}, 0) END), 0),
               COALESCE(SUM(CASE WHEN s.order_id IS NOT NULL AND t.order_id IS NOT NULL
                                 THEN COALESCE({m_s}, 0) - COALESCE({m_t}, 0) END), 0)
        FROM key_result AS k
        LEFT JOIN clean_record AS s ON s.run_id = k.run_id AND s.side = 'SOURCE' AND s.order_id = k.order_id
        LEFT JOIN clean_record AS t ON t.run_id = k.run_id AND t.side = 'TARGET' AND t.order_id = k.order_id
        WHERE k.run_id = :run_id
        """, {"run_id": run_id}).fetchone()
    return {"source_only": row[0], "target_only": row[1], "both_sides": row[2]}


# --------------------------------------------------------------------------- invariant inputs


def count_duplicate_keys(conn: sqlite3.Connection, run_id: str) -> tuple[int, int]:
    """(keys with > 1 clean row on a side, keys with > 1 key_result row)."""
    clean = conn.execute(
        "SELECT COUNT(*) FROM (SELECT 1 FROM clean_record WHERE run_id = ?"
        " GROUP BY side, order_id HAVING COUNT(*) > 1)", (run_id,)).fetchone()[0]
    results = conn.execute(
        "SELECT COUNT(*) FROM (SELECT 1 FROM key_result WHERE run_id = ?"
        " GROUP BY order_id HAVING COUNT(*) > 1)", (run_id,)).fetchone()[0]
    return clean, results


def count_unknown_references(conn: sqlite3.Connection, run_id: str, columns: Sequence[str]) -> int:
    """clean_record rows whose value in any of `columns` is not in ref_customer for the run."""
    if not columns:
        return 0
    misses = " OR ".join(
        f"NOT EXISTS (SELECT 1 FROM ref_customer AS r WHERE r.run_id = c.run_id AND r.customer_id = c.{_ident(col)})"
        for col in columns)
    return conn.execute(f"SELECT COUNT(*) FROM clean_record AS c WHERE c.run_id = ? AND ({misses})",
                        (run_id,)).fetchone()[0]


_KEY_COVERAGE = """
WITH expected AS (
    SELECT order_id FROM clean_record WHERE run_id = :run_id
    UNION
    SELECT order_id FROM quarantine
    WHERE run_id = :run_id AND side IN ('SOURCE', 'TARGET') AND order_id IS NOT NULL
),
actual AS (SELECT order_id FROM key_result WHERE run_id = :run_id)
SELECT (SELECT COUNT(*) FROM expected),
       (SELECT COUNT(*) FROM actual),
       (SELECT COUNT(*) FROM expected WHERE order_id NOT IN (SELECT order_id FROM actual)),
       (SELECT COUNT(*) FROM actual WHERE order_id NOT IN (SELECT order_id FROM expected))
"""


def key_coverage(conn: sqlite3.Connection, run_id: str) -> dict[str, int]:
    row = conn.execute(_KEY_COVERAGE, {"run_id": run_id}).fetchone()
    return {"expected_keys": row[0], "key_results": row[1], "missing": row[2], "extra": row[3]}


def count_unexplained_aggregates(conn: sqlite3.Connection, run_id: str) -> int:
    return conn.execute("SELECT COUNT(*) FROM aggregate_result WHERE run_id = ? AND unexplained_delta <> 0",
                        (run_id,)).fetchone()[0]


# --------------------------------------------------------------------------- check_result


def insert_check_results(conn: sqlite3.Connection, rows: Iterable[Mapping[str, Any]]) -> None:
    conn.executemany(
        f"INSERT INTO check_result ({', '.join(CHECK_COLUMNS)})"
        f" VALUES ({', '.join(':' + c for c in CHECK_COLUMNS)})", rows)


# --------------------------------------------------------------------------- readers (sorted by business key)


def fetch_key_results(conn: sqlite3.Connection, run_id: str) -> list[dict[str, Any]]:
    rows = conn.execute(f"SELECT {', '.join(KEY_RESULT_COLUMNS)} FROM key_result WHERE run_id = ?"
                        " ORDER BY order_id", (run_id,)).fetchall()
    return [dict(r) for r in rows]


def fetch_field_diffs(conn: sqlite3.Connection, run_id: str) -> list[dict[str, Any]]:
    rows = conn.execute(f"SELECT {', '.join(FIELD_DIFF_COLUMNS)} FROM field_diff WHERE run_id = ?"
                        " ORDER BY order_id, field_name", (run_id,)).fetchall()
    return [dict(r) for r in rows]


def fetch_aggregates(conn: sqlite3.Connection, run_id: str) -> list[dict[str, Any]]:
    rows = conn.execute(f"SELECT {', '.join(AGGREGATE_COLUMNS)} FROM aggregate_result WHERE run_id = ?"
                        " ORDER BY metric, group_by, group_value", (run_id,)).fetchall()
    return [dict(r) for r in rows]


def fetch_check_results(conn: sqlite3.Connection, run_id: str) -> list[dict[str, Any]]:
    rows = conn.execute(f"SELECT {', '.join(CHECK_COLUMNS)} FROM check_result WHERE run_id = ?"
                        " ORDER BY check_name", (run_id,)).fetchall()
    return [dict(r) for r in rows]


def count_key_statuses(conn: sqlite3.Connection, run_id: str) -> dict[str, int]:
    rows = conn.execute("SELECT key_status, COUNT(*) FROM key_result WHERE run_id = ?"
                        " GROUP BY key_status ORDER BY key_status", (run_id,)).fetchall()
    return {r[0]: r[1] for r in rows}


def count_field_diffs(conn: sqlite3.Connection, run_id: str) -> list[dict[str, Any]]:
    rows = conn.execute("SELECT field_name, diff_type, COUNT(*) AS count FROM field_diff WHERE run_id = ?"
                        " GROUP BY field_name, diff_type ORDER BY field_name, diff_type", (run_id,)).fetchall()
    return [dict(r) for r in rows]
