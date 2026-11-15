"""GOLD stage: key results, field diffs, aggregates and checks for one run (docs/DESIGN.md 4.5-4.8).

All comparison runs as set-based SQL in recon.repository.gold_repo; this module turns the config into
the specs that SQL needs, sequences the statements, evaluates the checks and derives recon_status.

An INVARIANT check failure is an engine bug: `reconcile` raises InvariantViolation before writing any
check_result row, and the caller's data transaction rolls back (DESIGN 4.7). The caller turns it into a
FAILED run with failure_reason INVARIANT_VIOLATION:<check>.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from recon.repository import bronze_repo, gold_repo, silver_repo
from recon.service.config import Config
from recon.service.ingest import (EMPTY_BATCH, MISSING_OPTIONAL_COLUMN, SCHEMA_EXTRA_COLUMN, SIDE_ORDER,
                                  BatchIssue)
from recon.service.normalize import format_minor
from recon.service.validate import SOURCE, TARGET, clean_column

INVARIANT, WARN = "INVARIANT", "WARN"
PASS, FAIL = "PASS", "FAIL"
MATCHED = "MATCHED"
KEY_STATUSES = ("CONFLICT", MATCHED, "MISMATCHED", "SOURCE_REJECTED", "TARGET_REJECTED", "MISSING_IN_TARGET",
                "EXTRA_IN_TARGET")  # DESIGN 4.5, in evaluation order
DIFF_TYPES = ("NULL_MISMATCH", "TARGET_STALE", "TARGET_AHEAD", "VALUE_MISMATCH")  # DESIGN 4.6, in precedence order

SRC_ROW_ACCOUNTING = "SRC_ROW_ACCOUNTING"
TGT_ROW_ACCOUNTING = "TGT_ROW_ACCOUNTING"
KEY_UNIQUENESS = "KEY_UNIQUENESS"
REFERENTIAL_INTEGRITY = "REFERENTIAL_INTEGRITY"
KEY_COVERAGE = "KEY_COVERAGE"
AGGREGATE_EXPLAINED = "AGGREGATE_EXPLAINED"
SCHEMA_DRIFT = "SCHEMA_DRIFT"
# MISSING_OPTIONAL_COLUMN and EMPTY_BATCH: the check name equals the warning code (imported from ingest).

INVARIANT_CHECKS = (SRC_ROW_ACCOUNTING, TGT_ROW_ACCOUNTING, KEY_UNIQUENESS, REFERENTIAL_INTEGRITY,
                    KEY_COVERAGE, AGGREGATE_EXPLAINED)
WARN_CHECKS = {SCHEMA_DRIFT: SCHEMA_EXTRA_COLUMN, MISSING_OPTIONAL_COLUMN: MISSING_OPTIONAL_COLUMN,
               EMPTY_BATCH: EMPTY_BATCH}  # check name -> batch warning code
CHECK_ORDER = INVARIANT_CHECKS + tuple(WARN_CHECKS)

ROW_COUNT = "ROW_COUNT"


class InvariantViolation(RuntimeError):
    """An INVARIANT check failed: the engine is wrong, not the data."""

    def __init__(self, check_name: str, counts: dict[str, Any]):
        super().__init__(f"invariant {check_name} failed: {canonical_json(counts)}")
        self.check_name = check_name
        self.counts = counts

    @property
    def failure_reason(self) -> str:
        return f"INVARIANT_VIOLATION:{self.check_name}"

    @property
    def failure_detail(self) -> str:
        return canonical_json(self.counts)


@dataclass(frozen=True)
class CheckOutcome:
    check_name: str
    severity: str
    passed: bool
    expected: str
    actual: str
    detail: Any  # JSON-serializable

    @property
    def status(self) -> str:
        if self.passed:
            return PASS
        return WARN if self.severity == WARN else FAIL

    def as_row(self, run_id: str) -> dict[str, Any]:
        return {"run_id": run_id, "check_name": self.check_name, "severity": self.severity,
                "status": self.status, "expected": self.expected, "actual": self.actual,
                "detail": canonical_json(self.detail)}


@dataclass
class CompareResult:
    key_status_counts: dict[str, int]
    field_diff_counts: list[dict[str, Any]]
    checks: list[CheckOutcome]
    unattributable_rejects: int
    recon_status: str
    total_keys: int = field(init=False)

    def __post_init__(self) -> None:
        self.total_keys = sum(self.key_status_counts.values())


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


# --------------------------------------------------------------------------- specs from config


def compare_fields(config: Config) -> list[gold_repo.CompareField]:
    specs = []
    for name in config.compare_fields:
        spec = config.field(name)
        money = spec.type == "decimal"
        specs.append(gold_repo.CompareField(name=name, column=clean_column(spec), money=money,
                                            tolerance_minor=spec.tolerance_minor if money else 0,
                                            scale=spec.scale if money else 0))
    return specs


def measures(config: Config) -> list[gold_repo.Measure]:
    return [gold_repo.Measure(ROW_COUNT, None)] + [
        gold_repo.Measure(f"SUM_{name.upper()}", clean_column(config.field(name)))
        for name in config.aggregates.sum_fields]


def groupings(config: Config) -> list[gold_repo.Grouping]:
    return [gold_repo.Grouping(gold_repo.ALL_GROUP, None)] + [
        gold_repo.Grouping(name.upper(), clean_column(config.field(name))) for name in config.aggregates.group_by]


# --------------------------------------------------------------------------- stage


def reconcile(conn: sqlite3.Connection, run_id: str, config: Config,
              warnings: Iterable[BatchIssue] = ()) -> CompareResult:
    """Build GOLD for run_id inside the caller's transaction. Raises InvariantViolation on an engine bug."""
    gold_repo.register_functions(conn, format_minor)
    gold_repo.insert_key_results(conn, run_id)
    gold_repo.insert_field_diffs(conn, run_id, compare_fields(config))
    gold_repo.apply_diff_counts(conn, run_id)
    gold_repo.insert_aggregates(conn, run_id, measures(config), groupings(config))

    checks = evaluate_checks(conn, run_id, config, list(warnings))
    for check in checks:
        if check.severity == INVARIANT and not check.passed:
            raise InvariantViolation(check.check_name, check.detail)
    gold_repo.insert_check_results(conn, (c.as_row(run_id) for c in checks))

    status_counts = gold_repo.count_key_statuses(conn, run_id)
    unattributable = silver_repo.count_unattributable_rejects(conn, run_id)
    all_matched = all(status == MATCHED for status in status_counts)
    return CompareResult(
        key_status_counts=status_counts,
        field_diff_counts=gold_repo.count_field_diffs(conn, run_id),
        checks=checks,
        unattributable_rejects=unattributable,
        recon_status=PASS if all_matched and unattributable == 0 else FAIL,
    )


# --------------------------------------------------------------------------- checks


def evaluate_checks(conn: sqlite3.Connection, run_id: str, config: Config,
                    warnings: list[BatchIssue]) -> list[CheckOutcome]:
    """All nine checks in catalogue order (DESIGN 4.7). Nothing is written here."""
    checks = [_row_accounting(conn, run_id, SOURCE, SRC_ROW_ACCOUNTING),
              _row_accounting(conn, run_id, TARGET, TGT_ROW_ACCOUNTING),
              _key_uniqueness(conn, run_id),
              _referential_integrity(conn, run_id, config),
              _key_coverage(conn, run_id),
              _aggregate_explained(conn, run_id, config)]
    checks += [_warning_check(name, code, warnings) for name, code in WARN_CHECKS.items()]
    return checks


def _row_accounting(conn: sqlite3.Connection, run_id: str, side: str, name: str) -> CheckOutcome:
    landed = bronze_repo.count_raw_records(conn, run_id, side, "DATA")
    clean = silver_repo.count_clean_records(conn, run_id, side)
    quarantined = silver_repo.count_quarantine(conn, run_id, side)
    return CheckOutcome(name, INVARIANT, landed == clean + quarantined, str(landed), str(clean + quarantined),
                        {"clean": clean, "landed": landed, "quarantine": quarantined, "side": side})


def _key_uniqueness(conn: sqlite3.Connection, run_id: str) -> CheckOutcome:
    clean_dups, result_dups = gold_repo.count_duplicate_keys(conn, run_id)
    total = clean_dups + result_dups
    return CheckOutcome(KEY_UNIQUENESS, INVARIANT, total == 0, "0", str(total),
                        {"duplicate_clean_keys": clean_dups, "duplicate_key_results": result_dups})


def _referential_integrity(conn: sqlite3.Connection, run_id: str, config: Config) -> CheckOutcome:
    ref_fields = [spec for spec in config.fields if spec.reference] if config.reference is not None else []
    unknown = gold_repo.count_unknown_references(conn, run_id, [clean_column(s) for s in ref_fields])
    return CheckOutcome(REFERENTIAL_INTEGRITY, INVARIANT, unknown == 0, "0", str(unknown),
                        {"fields": [s.name for s in ref_fields], "unknown_references": unknown})


def _key_coverage(conn: sqlite3.Connection, run_id: str) -> CheckOutcome:
    cov = gold_repo.key_coverage(conn, run_id)
    ok = cov["missing"] == 0 and cov["extra"] == 0
    return CheckOutcome(KEY_COVERAGE, INVARIANT, ok, str(cov["expected_keys"]), str(cov["key_results"]), cov)


def _aggregate_explained(conn: sqlite3.Connection, run_id: str, config: Config) -> CheckOutcome:
    """unexplained_delta = 0 on every aggregate row, and each overall delta splits exactly into
    source-only - target-only + both-sides contributions (the breakdown is stored in detail)."""
    unexplained_rows = gold_repo.count_unexplained_aggregates(conn, run_id)
    overall = {(a["metric"]): a for a in gold_repo.fetch_aggregates(conn, run_id)
               if a["group_by"] == gold_repo.ALL_GROUP}
    breakdown, unbalanced = [], 0
    for measure in measures(config):
        parts = gold_repo.aggregate_breakdown(conn, run_id, measure)
        delta = overall[measure.metric]["delta"]
        rebuilt = parts["source_only"] - parts["target_only"] + parts["both_sides"]
        unbalanced += delta != rebuilt
        breakdown.append({"metric": measure.metric, "delta": delta, **parts})
    ok = unexplained_rows == 0 and unbalanced == 0
    return CheckOutcome(AGGREGATE_EXPLAINED, INVARIANT, ok, "0", str(unexplained_rows + unbalanced),
                        {"breakdown": breakdown, "unbalanced_metrics": unbalanced,
                         "unexplained_rows": unexplained_rows})


def _warning_check(name: str, code: str, warnings: list[BatchIssue]) -> CheckOutcome:
    found = sorted((w for w in warnings if w.code == code), key=lambda w: SIDE_ORDER.index(w.side))
    detail = [{"side": w.side, "columns": list(w.columns)} for w in found]
    return CheckOutcome(name, WARN, not found, "0", str(len(found)), detail)
