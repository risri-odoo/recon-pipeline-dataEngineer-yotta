"""EXPLAIN QUERY PLAN for every GOLD query of one reconciliation (dev tool; see docs/PERFORMANCE.md).

Generates a synthetic dataset and reconciles it into a fresh SQLite DB with a trace callback recording
every statement the GOLD stage executes (compare.reconcile) and every statement the report writer runs
against the gold tables. Then each distinct statement's query plan is printed, with:
- `<-- FULL SCAN OF BASE TABLE` on a SCAN of a recon table (raw_record, clean_record, key_result, ...);
- `(scan of a transient result)` on a SCAN of something the statement builds itself (a materialized CTE or
  subquery), which is inherent to the query and not a missing index;
- the re-run time of read-only statements (writes are not re-run).
The last line counts base-table full scans.

Run from the repository root:
    python tools/explain_gold.py --n 100000 --seed 1
"""

from __future__ import annotations

import argparse
import re
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from recon.generator.generate import generate, write_dataset  # noqa: E402
from recon.presentation.report_writer import write_report  # noqa: E402
from recon.repository import gold_repo  # noqa: E402
from recon.repository.db import connect, explain_query_plan  # noqa: E402
from recon.repository.schema import TABLES  # noqa: E402
from recon.service import compare, pipeline, report  # noqa: E402
from recon.service.normalize import format_minor  # noqa: E402

_SCAN = re.compile(r"^SCAN (\w+)")
_READ_ONLY = re.compile(r"^\s*(SELECT|WITH)\b", re.IGNORECASE)
_WRITE = re.compile(r"\b(INSERT|UPDATE|DELETE)\b", re.IGNORECASE)
_EXPLAINABLE = re.compile(r"^\s*(SELECT|WITH|INSERT|UPDATE|DELETE)\b", re.IGNORECASE)


def base_table_names(sql: str) -> set[str]:
    """Names a plan line can use for a recon table in this statement: the table itself and its aliases
    (`clean_record AS c` shows up as `SCAN c`)."""
    names = set(TABLES)
    for table in TABLES:
        names.update(re.findall(rf"\b{table}\s+(?:AS\s+)?(\w+)", sql, re.IGNORECASE))
    keywords = {"where", "join", "left", "on", "group", "order", "union", "set", "values", "inner", "cross", "using"}
    return {n for n in names if n.lower() not in keywords}


def capture(data_dir: Path, out: Path, config: Path) -> tuple[Path, list[str], list[str]]:
    """Run the pipeline and the report writer once; return (db path, gold statements, report statements)."""
    gold_sql: list[str] = []
    report_sql: list[str] = []
    real_reconcile, real_build_bundle = compare.reconcile, report.build_bundle

    def traced_reconcile(conn: sqlite3.Connection, *args, **kwargs):
        conn.set_trace_callback(gold_sql.append)
        try:
            return real_reconcile(conn, *args, **kwargs)
        finally:
            conn.set_trace_callback(None)

    def traced_build_bundle(conn: sqlite3.Connection, run_id: str):
        conn.set_trace_callback(report_sql.append)  # stays on while the report files stream from cursors
        return real_build_bundle(conn, run_id)

    compare.reconcile, report.build_bundle = traced_reconcile, traced_build_bundle
    try:
        request = pipeline.RunRequest(
            source=data_dir / "source.csv", target=data_dir / "target.csv", reference=data_dir / "customers.csv",
            config_path=config, db_path=out / "explain.db", out_dir=out)
        outcome = pipeline.run_pipeline(request)
        if outcome.status != pipeline.SUCCEEDED:
            raise SystemExit(f"run ended {outcome.status}: {outcome.failure_reason}")
        with report.open_report(request.db_path, outcome.run_id) as bundle:
            write_report(out / outcome.run_id, bundle)
    finally:
        compare.reconcile, report.build_bundle = real_reconcile, real_build_bundle
    return request.db_path, gold_sql, report_sql


def explain(conn: sqlite3.Connection, label: str, statements: list[str], width: int) -> int:
    """Print the plan of each distinct explainable statement; return the number of base-table full scans."""
    print(f"===== {label}")
    full_scans, seen = 0, set()
    for sql in statements:
        if not _EXPLAINABLE.match(sql) or sql in seen:
            continue
        seen.add(sql)
        timing = "write, not re-run"
        if _READ_ONLY.match(sql) and not _WRITE.search(sql):
            t0 = time.perf_counter()
            conn.execute(sql).fetchall()
            timing = f"re-run {time.perf_counter() - t0:.3f} s"
        print(f"--- {' '.join(sql.split())[:width]}\n    [{timing}]")
        base_names = base_table_names(sql)
        for detail in explain_query_plan(conn, sql):
            scan = _SCAN.match(detail)
            note = ""
            if scan and scan.group(1) in base_names:
                note, full_scans = "  <-- FULL SCAN OF BASE TABLE", full_scans + 1
            elif scan and detail != "SCAN CONSTANT ROW":
                note = "  (scan of a transient result)"
            print(f"    {detail}{note}")
    print()
    return full_scans


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--n", type=int, default=100_000, help="base orders to generate (default 100000)")
    parser.add_argument("--seed", type=int, default=1, help="generator seed (default 1)")
    parser.add_argument("--config", type=Path, default=ROOT / "config" / "orders_recon.json")
    parser.add_argument("--width", type=int, default=160, help="characters of each statement to show")
    parser.add_argument("--work", type=Path, help="keep data and DB here (default: a temp dir, removed)")
    args = parser.parse_args(argv)

    with tempfile.TemporaryDirectory(prefix="recon-explain-") as tmp:
        work = args.work or Path(tmp)
        data_dir, _ = write_dataset(generate(args.n, args.seed), "explain", work / "generated", work / "expected")
        db_path, gold_sql, report_sql = capture(data_dir, work / "out", args.config)
        conn = connect(db_path)
        try:
            gold_repo.register_functions(conn, format_minor)  # field_diff SQL calls recon_format_minor
            print(f"# n={args.n} seed={args.seed} sqlite={sqlite3.sqlite_version}\n")
            full_scans = explain(conn, "GOLD stage (compare.reconcile)", gold_sql, args.width)
            full_scans += explain(conn, "report reads of the gold tables", report_sql, args.width)
        finally:
            conn.close()
    print(f"base-table full scans: {full_scans}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
