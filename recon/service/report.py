"""The report of one SUCCEEDED run, rebuilt from the DB alone (docs/REPORT_SCHEMA.md).

`open_report(db_path, run_id)` yields a ReportBundle: the report.json document as a dict plus row
iterators for the CSV extracts (mismatches, field diffs, quarantine). The iterators stream from SQLite
cursors, so they must be consumed while the context is open. The config comes from run.config_json and
must hash to run.config_sha256, so a report never mixes one run's data with another config.

Everything here is deterministic: no timings, no memory figures (those live in metrics.json only,
DESIGN 7.1 #13); the only run-dependent values are run_id, attempt, forced, started_at and finished_at.
Every list is sorted by business key (then column), every map has a fixed key set.
"""

from __future__ import annotations

import itertools
import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from recon.repository import bronze_repo, gold_repo, run_repo, silver_repo
from recon.repository.db import connect
from recon.service import compare, normalize
from recon.service.config import Config, ConfigError, FieldSpec, load_stored_config
from recon.service.dedupe import CONFLICTING_DUPLICATE, DUPLICATE_EXACT, SUPERSEDED
from recon.service.ingest import REFERENCE, SIDE_ORDER, SOURCE, TARGET
from recon.service.validate import MALFORMED_ROW, ROW_REJECT, clean_column

REPORT_VERSION = 1
SAMPLE_LIMIT = 20
SUCCEEDED = "SUCCEEDED"
CLEAN, ABSENT = "CLEAN", "ABSENT"
ROWS, MINOR_UNITS = "ROWS", "MINOR_UNITS"

ROW_REJECT_CODES = (MALFORMED_ROW, normalize.MISSING_REQUIRED, normalize.BAD_TYPE, normalize.BAD_FORMAT,
                    normalize.BAD_ENUM, normalize.OUT_OF_RANGE, normalize.INVALID_REFERENCE)  # DESIGN 4.3 order

# Every CSV export starts with run_id, so each row is traceable on its own (CLAUDE.md rule 6).
MISMATCH_LEAD_COLUMNS = ("run_id", "record_key", "key_status", "differing_columns", "note", "source_state",
                         "target_state", "source_line_no", "target_line_no")
FIELD_DIFF_CSV_COLUMNS = ("run_id", "record_key", "column_name", "source_value", "target_value", "diff_type")
QUARANTINE_CSV_COLUMNS = ("run_id", "side", "source_file", "line_no", "reason_code", "detail", "raw_text")
DIFF_SEPARATOR = ";"

TOP_LEVEL_KEYS = ("aggregate_explanation", "aggregates", "checks", "field_diff_counts", "inputs",
                  "key_status_counts", "mismatch_samples", "report_version", "run", "sides", "totals", "warnings")


class ReportError(RuntimeError):
    """The report cannot be built: no DB, unknown run, a run that did not succeed, or no usable config."""


# --------------------------------------------------------------------------- bundle


@dataclass
class ReportBundle:
    run_id: str
    report: dict[str, Any]
    config: Config
    _conn: sqlite3.Connection

    @property
    def display_fields(self) -> tuple[FieldSpec, ...]:
        return display_fields(self.config)

    @property
    def mismatch_columns(self) -> tuple[str, ...]:
        pairs = ((f"source_{f.name}", f"target_{f.name}") for f in self.display_fields)
        return MISMATCH_LEAD_COLUMNS + tuple(itertools.chain.from_iterable(pairs))

    def mismatches(self) -> Iterator[dict[str, Any]]:
        """One dict per non-MATCHED key, in key order (see iter_mismatches)."""
        return iter_mismatches(self._conn, self.run_id, self.config)

    def mismatch_rows(self) -> Iterator[list[Any]]:
        """mismatches.csv rows (columns = mismatch_columns); NULL -> None."""
        for m in self.mismatches():
            row: list[Any] = [self.run_id, m["record_key"], m["key_status"], DIFF_SEPARATOR.join(m["differing_columns"]),
                              m["note"], m["source_state"], m["target_state"], m["source_line_no"],
                              m["target_line_no"]]
            for f in self.display_fields:
                row += [m["source"][f.name], m["target"][f.name]]
            yield row

    def field_diff_rows(self) -> Iterator[list[Any]]:
        for r in gold_repo.iter_field_diffs(self._conn, self.run_id):
            yield [self.run_id, r["order_id"], r["field_name"], r["source_value"], r["target_value"], r["diff_type"]]

    def quarantine_rows(self) -> Iterator[list[Any]]:
        for r in silver_repo.iter_quarantine(self._conn, self.run_id):
            yield [r[c] for c in QUARANTINE_CSV_COLUMNS]


@contextmanager
def open_report(db_path: str | Path, run_id: str) -> Iterator[ReportBundle]:
    """Open the DB read-only in intent and yield the report of a SUCCEEDED run."""
    db_path = Path(db_path)
    if not db_path.is_file():
        raise ReportError(f"database not found: {db_path}")
    conn = connect(db_path)
    try:
        yield build_bundle(conn, run_id)
    finally:
        conn.close()


def build_bundle(conn: sqlite3.Connection, run_id: str) -> ReportBundle:
    try:
        run = run_repo.get_run(conn, run_id)
    except sqlite3.OperationalError as exc:  # not a recon DB
        raise ReportError(f"cannot read run table: {exc}") from None
    if run is None:
        raise ReportError(f"run {run_id!r} not found")
    if run["status"] != SUCCEEDED:
        hint = f"; its results are in run {run['skipped_of_run_id']}" if run["skipped_of_run_id"] else ""
        raise ReportError(f"run {run_id} has status {run['status']}, only SUCCEEDED runs have a report{hint}")
    if not run["config_json"]:
        raise ReportError(f"run {run_id} has no stored config_json")
    try:
        config = load_stored_config(run["config_json"], run["config_sha256"])
    except ConfigError as exc:
        raise ReportError(f"run {run_id}: {exc}") from None
    return ReportBundle(run_id=run_id, report=build_report(conn, run, config), config=config, _conn=conn)


# --------------------------------------------------------------------------- report.json


def build_report(conn: sqlite3.Connection, run: dict[str, Any], config: Config) -> dict[str, Any]:
    run_id = run["run_id"]
    key_counts = gold_repo.count_key_statuses(conn, run_id)
    key_status_counts = {s: key_counts.get(s, 0) for s in compare.KEY_STATUSES}
    field_diff_counts = _field_diff_counts(conn, run_id, config)
    quarantine_total = sum(silver_repo.count_quarantine(conn, run_id, side) for side in SIDE_ORDER)
    report = {
        "report_version": REPORT_VERSION,
        "run": {
            "run_id": run_id, "run_key": run["run_key"], "attempt": run["attempt"], "forced": bool(run["forced"]),
            "status": run["status"], "recon_status": run["recon_status"], "exit_code": run["exit_code"],
            "started_at": run["started_at"], "finished_at": run["finished_at"],
            "config_sha256": run["config_sha256"], "dataset": config.dataset,
            "business_key": list(config.business_key),
        },
        "inputs": {side: {"file": run[f"{side.lower()}_file"], "sha256": run[f"{side.lower()}_sha256"],
                          "data_records": bronze_repo.count_raw_records(conn, run_id, side, "DATA")}
                   for side in SIDE_ORDER},
        "sides": _sides(conn, run_id),
        "key_status_counts": key_status_counts,
        "field_diff_counts": field_diff_counts,
        "totals": {
            "keys": sum(key_status_counts.values()),
            "non_matched_keys": sum(n for s, n in key_status_counts.items() if s != compare.MATCHED),
            "field_diffs": sum(n for by_type in field_diff_counts.values() for n in by_type.values()),
            "quarantine_rows": quarantine_total,
            "unattributable_rejects": silver_repo.count_unattributable_rejects(conn, run_id),
        },
        "aggregates": [_aggregate(a, config) for a in gold_repo.fetch_aggregates(conn, run_id)],
        "aggregate_explanation": _aggregate_explanation(conn, run_id, config),
        "checks": [{**c, "detail": json.loads(c["detail"])} for c in _without_run_id(
            gold_repo.fetch_check_results(conn, run_id))],
        "warnings": warnings_from_checks(gold_repo.fetch_check_results(conn, run_id)),
        "mismatch_samples": {"limit": SAMPLE_LIMIT,
                             "keys": list(itertools.islice(iter_mismatches(conn, run_id, config), SAMPLE_LIMIT))},
    }
    return report


def _without_run_id(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{k: v for k, v in r.items() if k != "run_id"} for r in rows]


def _sides(conn: sqlite3.Connection, run_id: str) -> dict[str, dict[str, Any]]:
    by_reason = silver_repo.count_quarantine_by_reason(conn, run_id)
    sides: dict[str, dict[str, Any]] = {}
    for side in SIDE_ORDER:
        rows_read = bronze_repo.count_raw_records(conn, run_id, side, "DATA")
        rejected = silver_repo.count_quarantine(conn, run_id, side, ROW_REJECT)
        rejected_by_reason = {code: by_reason.get((side, code), 0) for code in ROW_REJECT_CODES}
        if side == REFERENCE:
            accepted = silver_repo.count_ref_customers(conn, run_id)
            sides[side] = {"rows_read": rows_read, "accepted": accepted, "rejected": rejected,
                           "rejected_by_reason": rejected_by_reason,
                           "duplicates_ignored": rows_read - accepted - rejected}
        else:
            sides[side] = {"rows_read": rows_read, "accepted": silver_repo.count_clean_records(conn, run_id, side),
                           "rejected": rejected, "rejected_by_reason": rejected_by_reason,
                           "duplicate_exact": by_reason.get((side, DUPLICATE_EXACT), 0),
                           "superseded": by_reason.get((side, SUPERSEDED), 0),
                           "conflicting_rows": by_reason.get((side, CONFLICTING_DUPLICATE), 0)}
    return sides


def _field_diff_counts(conn: sqlite3.Connection, run_id: str, config: Config) -> dict[str, dict[str, int]]:
    """compare field -> diff type -> count, every compare field and diff type present (zeros included)."""
    counts = {f: {t: 0 for t in compare.DIFF_TYPES} for f in config.compare_fields}
    for row in gold_repo.count_field_diffs(conn, run_id):
        counts.setdefault(row["field_name"], {t: 0 for t in compare.DIFF_TYPES})[row["diff_type"]] = row["count"]
    return counts


def _measure_units(config: Config) -> dict[str, tuple[str, int]]:
    """metric -> (unit, scale): ROW_COUNT counts rows; SUM_<FIELD> is integer minor units at the field's scale."""
    units = {compare.ROW_COUNT: (ROWS, 0)}
    for name in config.aggregates.sum_fields:
        units[f"SUM_{name.upper()}"] = (MINOR_UNITS, config.field(name).scale)
    return units


def _aggregate(row: dict[str, Any], config: Config) -> dict[str, Any]:
    unit, scale = _measure_units(config)[row["metric"]]
    return {**{k: v for k, v in row.items() if k != "run_id"}, "unit": unit, "scale": scale}


def _aggregate_explanation(conn: sqlite3.Connection, run_id: str, config: Config) -> list[dict[str, Any]]:
    """Per metric (group ALL): the delta split by where keys have clean rows and by key status."""
    units = _measure_units(config)
    overall = {a["metric"]: a for a in gold_repo.fetch_aggregates(conn, run_id)
               if a["group_by"] == gold_repo.ALL_GROUP}
    explanation = []
    for measure in sorted(compare.measures(config), key=lambda m: m.metric):
        by_status = {s: {"keys": 0, "source_value": 0, "target_value": 0, "delta": 0} for s in compare.KEY_STATUSES}
        for r in gold_repo.aggregate_breakdown_by_status(conn, run_id, measure):
            by_status[r["key_status"]] = {"keys": r["keys"], "source_value": r["source_value"],
                                          "target_value": r["target_value"],
                                          "delta": r["source_value"] - r["target_value"]}
        unit, scale = units[measure.metric]
        agg = overall[measure.metric]
        explanation.append({"metric": measure.metric, "group_by": gold_repo.ALL_GROUP, "unit": unit, "scale": scale,
                            "delta": agg["delta"], "explained_delta": agg["explained_delta"],
                            **gold_repo.aggregate_breakdown(conn, run_id, measure), "by_key_status": by_status})
    return explanation


def warnings_from_checks(checks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The report's warnings array [{code, side, columns}] from the WARN check rows (DESIGN 4.2),
    sorted by code, then side in SOURCE -> TARGET -> REFERENCE order."""
    found = []
    for check in checks:
        if check["severity"] == compare.WARN and check["status"] == compare.WARN:
            code = compare.WARN_CHECKS[check["check_name"]]
            found += [{"code": code, "side": d["side"], "columns": d["columns"]} for d in json.loads(check["detail"])]
    return sorted(found, key=lambda w: (w["code"], SIDE_ORDER.index(w["side"])))


# --------------------------------------------------------------------------- mismatches


def display_fields(config: Config) -> tuple[FieldSpec, ...]:
    """Logical fields shown side by side: every field except the business-key fields (record_key carries
    them) and whatever is stored in the key column."""
    return tuple(f for f in config.fields
                 if f.name not in config.business_key and clean_column(f) != silver_repo.KEY_COLUMN)


def _render(spec: FieldSpec, value: Any) -> Any:
    if value is None:
        return None
    return normalize.format_minor(int(value), spec.scale) if spec.type == "decimal" else value


def iter_mismatches(conn: sqlite3.Connection, run_id: str, config: Config) -> Iterator[dict[str, Any]]:
    """One dict per non-MATCHED key, in key order: status, the sorted differing columns, both sides'
    state (CLEAN, the deciding quarantine reason_code, or ABSENT), line numbers and canonical values
    (money as fixed-point text, NULL as None; values are None whenever a side has no clean row)."""
    fields = display_fields(config)
    columns = [clean_column(f) for f in fields]
    cursor = gold_repo.iter_non_matched(conn, run_id, columns)
    for key, group in itertools.groupby(cursor, key=lambda r: r["order_id"]):
        rows = list(group)
        first = rows[0]
        yield {
            "record_key": key,
            "key_status": first["key_status"],
            "differing_columns": [r["field_name"] for r in rows if r["field_name"] is not None],
            "note": first["note"],
            "source_state": first["source_state"],
            "target_state": first["target_state"],
            "source_line_no": first["source_line_no"],
            "target_line_no": first["target_line_no"],
            "source": {f.name: _render(f, first[f"s_{c}"]) for f, c in zip(fields, columns)},
            "target": {f.name: _render(f, first[f"t_{c}"]) for f, c in zip(fields, columns)},
        }
