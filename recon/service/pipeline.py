"""End-to-end orchestration of one run: batch checks -> bronze -> silver -> gold (docs/DESIGN.md 5, 6).

Flow of `run_pipeline`:
1. Load the config and run the batch-check pass over all three files (sha256 + header checks).
   run_key = sha256(source_sha256, target_sha256, reference_sha256, config_sha256); a missing file
   hashes as the literal MISSING.
2. Run-admin transaction (BEGIN IMMEDIATE, committed on its own):
   - a SUCCEEDED attempt exists and no --force -> insert a SKIPPED row (skipped_of_run_id) and stop;
     no stage runs and no other table is written;
   - RUNNING attempts older than stale_after_minutes -> ABANDONED / STALE_RUNNING;
   - a RUNNING attempt that is still fresh -> commit the ABANDONED updates, then RunInProgress (no new row);
   - otherwise insert the new RUNNING row, attempt = existing attempts + 1.
3. Data transaction (BEGIN IMMEDIATE): land BRONZE, build SILVER, build GOLD, then set the run row to
   SUCCEEDED in the same transaction, so the data and the SUCCEEDED status commit together.
4. A batch reject, invariant violation, changed input or any other exception rolls the data
   transaction back (no BRONZE/SILVER/GOLD row survives) and a separate short transaction marks the
   run REJECTED or FAILED.

Fault injection (tests only): RECON_FAIL_AT=after_bronze|after_silver|after_gold raises RuntimeError at
that point inside the data transaction.
"""

from __future__ import annotations

import hashlib
import logging
import os
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from recon.repository import run_repo, silver_repo
from recon.repository.db import connect, transaction
from recon.repository.schema import create_schema
from recon.service import compare, ingest
from recon.service.compare import AggregateOverflow, InvariantViolation
from recon.service.config import Config, load_config
from recon.service.dedupe import (CONFLICTING_DUPLICATE, DUPLICATE_EXACT, SUPERSEDED, SideCounts, SideSilver,
                                  SilverResult, dedupe)
from recon.service.ingest import REFERENCE, SOURCE, TARGET, BatchIssue, InputChangedError
from recon.service.metrics import RunMetrics, StageMetrics, log_event, run_log, utc_iso
from recon.service.validate import ReferenceValidation, validate_reference, validate_side

RUNNING, SUCCEEDED, FAILED = "RUNNING", "SUCCEEDED", "FAILED"
REJECTED, SKIPPED, ABANDONED = "REJECTED", "SKIPPED", "ABANDONED"
UNEXPECTED_ERROR = "UNEXPECTED_ERROR"
EXIT_PASS, EXIT_FAIL, EXIT_ERROR = 0, 1, 2

FAIL_AT_ENV = "RECON_FAIL_AT"
FAIL_POINTS = ("after_bronze", "after_silver", "after_gold")
DEFAULT_STALE_AFTER_MINUTES = 60

DEFAULT_DB = Path("out") / "recon.db"
DEFAULT_OUT = Path("out")
RUN_LOG = "run.log"


class RunInProgress(RuntimeError):
    """A RUNNING attempt for the same run_key is younger than the stale threshold (exit 2, no new row).

    Older RUNNING attempts found at the same time have already been committed as ABANDONED.
    """

    def __init__(self, run_id: str, started_at: str, abandoned_run_ids: list[str] | None = None):
        super().__init__(f"run in progress: {run_id} started at {started_at}")
        self.run_id = run_id
        self.started_at = started_at
        self.abandoned_run_ids = list(abandoned_run_ids or [])


class BatchRejected(RuntimeError):
    """At least one input file has a batch reject (DESIGN 4.1); raised inside the data transaction."""

    def __init__(self, rejects: list[BatchIssue]):
        super().__init__(f"batch rejected: {', '.join(f'{r.side}:{r.code}' for r in rejects)}")
        self.rejects = rejects

    @property
    def failure_reason(self) -> str:
        return self.rejects[0].code  # rejects are already in SOURCE -> TARGET -> REFERENCE order

    @property
    def failure_detail(self) -> str:
        return compare.canonical_json([{"side": r.side, "file": r.file, "code": r.code, "detail": r.detail}
                                       for r in self.rejects])


@dataclass(frozen=True)
class RunRequest:
    source: Path
    target: Path
    reference: Path
    config_path: Path
    db_path: Path = DEFAULT_DB
    out_dir: Path = DEFAULT_OUT
    force: bool = False
    stale_after_minutes: int = DEFAULT_STALE_AFTER_MINUTES
    profile_memory: bool = False


@dataclass
class RunOutcome:
    run_id: str  # the run row written by this invocation (for SKIPPED: the SKIPPED row)
    run_key: str
    attempt: int
    status: str
    exit_code: int
    recon_status: str | None = None
    skipped_of_run_id: str | None = None
    failure_reason: str | None = None
    failure_detail: str | None = None
    abandoned_run_ids: list[str] = field(default_factory=list)
    run_dir: Path | None = None  # out/<run_id>; None for SKIPPED (nothing is written)
    metrics: dict[str, Any] | None = None  # None for SKIPPED

    @property
    def result_run_id(self) -> str:
        """The run whose results apply: the reused SUCCEEDED run for SKIPPED, else this run."""
        return self.skipped_of_run_id or self.run_id


# --------------------------------------------------------------------------- keys


def make_run_id(run_key: str, attempt: int) -> str:
    return f"{run_key[:16]}-a{attempt}"


def compute_run_key(source_sha256: str | None, target_sha256: str | None, reference_sha256: str | None,
                    config_sha256: str) -> str:
    parts = [h if h is not None else ingest.MISSING_HASH for h in (source_sha256, target_sha256, reference_sha256)]
    return hashlib.sha256("\n".join([*parts, config_sha256]).encode("utf-8")).hexdigest()


def _file_sha256(path: str) -> str | None:
    p = Path(path)
    if not p.is_file():
        return None
    digest = hashlib.sha256()
    with open(p, "rb") as fh:
        while chunk := fh.read(ingest.HASH_CHUNK_BYTES):
            digest.update(chunk)
    return digest.hexdigest()


def input_hashes(paths: Mapping[str, str], checks: Mapping[str, ingest.FileCheck]) -> dict[str, str | None]:
    """Raw-bytes sha256 per side from the batch-check pass (a side the config does not check is hashed here)."""
    return {side: checks[side].sha256 if side in checks else _file_sha256(paths[side])
            for side in (SOURCE, TARGET, REFERENCE)}


# --------------------------------------------------------------------------- fault injection


def fail_point_from_env() -> str | None:
    value = os.environ.get(FAIL_AT_ENV, "").strip()
    if not value:
        return None
    if value not in FAIL_POINTS:
        raise ValueError(f"{FAIL_AT_ENV}={value!r} is not one of {', '.join(FAIL_POINTS)}")
    return value


def _maybe_fail(fail_at: str | None, point: str) -> None:
    if fail_at == point:
        raise RuntimeError(f"injected failure at {point} ({FAIL_AT_ENV})")


# --------------------------------------------------------------------------- SILVER stage


def build_silver(conn: sqlite3.Connection, run_id: str, config: Config) -> SilverResult:
    """Validate the reference and both order sides, dedupe, and write SILVER. The caller owns the transaction."""
    reference: ReferenceValidation | None = None
    reference_ids: frozenset[str] | None = None
    if config.reference is not None:
        reference = validate_reference(conn, run_id, config)
        reference_ids = reference.ids  # loaded once, shared by both sides
        silver_repo.insert_ref_customers(conn, ({"run_id": run_id, "customer_id": cid, "customer_name": name}
                                                for cid, name in sorted(reference.customers)))
        silver_repo.insert_quarantine(conn, (q.as_row(run_id) for q in reference.rejects))

    result = SilverResult(sides={}, reference=reference)
    for side in (SOURCE, TARGET):
        validated = validate_side(conn, run_id, side, config, reference_ids)
        deduped = dedupe(validated.valid, validated.rejects)
        quarantine = sorted(validated.rejects + deduped.dispositions, key=lambda q: q.line_no)
        silver_repo.insert_quarantine(conn, (q.as_row(run_id) for q in quarantine))
        silver_repo.insert_clean_records(conn, (w.as_clean_row(run_id, config) for w in deduped.winners))
        counts = SideCounts(
            side=side,
            rows_read=validated.rows_read,
            accepted=len(deduped.winners),
            rejected=len(validated.rejects),
            duplicate_exact=deduped.count(DUPLICATE_EXACT),
            superseded=deduped.count(SUPERSEDED),
            conflicting_rows=deduped.count(CONFLICTING_DUPLICATE),
        )
        result.sides[side] = SideSilver(counts, deduped.winners, quarantine, deduped.conflict_keys)
    return result


# --------------------------------------------------------------------------- run


def run_pipeline(request: RunRequest) -> RunOutcome:
    """Execute one `recon run`. Raises ConfigError / ValueError before any run row exists, RunInProgress
    when a fresh RUNNING attempt exists. Every other failure ends as a FAILED or REJECTED run row."""
    if request.stale_after_minutes < 0:
        raise ValueError("stale_after_minutes must be >= 0")
    fail_at = fail_point_from_env()
    config = load_config(request.config_path)
    paths = {SOURCE: str(request.source), TARGET: str(request.target), REFERENCE: str(request.reference)}

    metrics = RunMetrics(profile_memory=request.profile_memory)
    metrics.start()
    try:
        with metrics.stage("batch_checks") as stage:
            checks = ingest.check_inputs(paths, config)
            for side, check in checks.items():
                stage.counts[side] = {"rejects": len(check.rejects), "warnings": len(check.warnings)}
        hashes = input_hashes(paths, checks)
        run_key = compute_run_key(hashes[SOURCE], hashes[TARGET], hashes[REFERENCE], config.config_sha256)

        request.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = connect(request.db_path)
        try:
            create_schema(conn)
            outcome = _admit(conn, request, run_key, hashes, paths, config)
            if outcome.status == SKIPPED:
                return outcome
            outcome.run_dir = request.out_dir / outcome.run_id
            with run_log(outcome.run_dir / RUN_LOG):
                _log_admitted(outcome, metrics)
                _execute(conn, outcome, paths, config, checks, metrics, fail_at)
        finally:
            conn.close()
    finally:
        metrics.stop()
    outcome.metrics = {"run_id": outcome.run_id, "status": outcome.status, **metrics.as_dict()}
    return outcome


def _admit(conn: sqlite3.Connection, request: RunRequest, run_key: str, hashes: dict[str, str | None],
           paths: dict[str, str], config: Config) -> RunOutcome:
    """Run-admin transaction: SKIPPED, run-in-progress, stale -> ABANDONED, then the RUNNING row."""
    now = datetime.now(timezone.utc)
    started_at = utc_iso(now)
    row = dict(run_key=run_key, source_file=paths[SOURCE], target_file=paths[TARGET],
               reference_file=paths[REFERENCE], source_sha256=hashes[SOURCE], target_sha256=hashes[TARGET],
               reference_sha256=hashes[REFERENCE], config_sha256=config.config_sha256, config_json=config.config_json,
               started_at=started_at)
    in_progress: RunInProgress | None = None

    with transaction(conn):
        attempts = run_repo.find_by_run_key(conn, run_key)
        attempt = len(attempts) + 1
        run_id = make_run_id(run_key, attempt)
        succeeded = [a for a in attempts if a["status"] == SUCCEEDED]

        if succeeded and not request.force:
            reused = succeeded[-1]  # the latest SUCCEEDED attempt
            run_repo.create_run(conn, run_id=run_id, attempt=attempt, status=SKIPPED, forced=0,
                                recon_status=reused["recon_status"], exit_code=reused["exit_code"],
                                skipped_of_run_id=reused["run_id"], finished_at=started_at, **row)
            outcome = RunOutcome(run_id=run_id, run_key=run_key, attempt=attempt, status=SKIPPED,
                                 exit_code=reused["exit_code"], recon_status=reused["recon_status"],
                                 skipped_of_run_id=reused["run_id"])
        else:
            # Stale attempts are abandoned first (and committed) even when a fresh one blocks this run.
            cutoff = utc_iso(now - timedelta(minutes=request.stale_after_minutes))
            abandoned = run_repo.mark_stale_running(conn, run_key, cutoff, started_at)
            fresh = [a for a in attempts if a["status"] == RUNNING and a["started_at"] >= cutoff]
            if fresh:
                in_progress = RunInProgress(fresh[-1]["run_id"], fresh[-1]["started_at"], abandoned)
            else:
                run_repo.create_run(conn, run_id=run_id, attempt=attempt, status=RUNNING,
                                    forced=int(request.force), **row)
                outcome = RunOutcome(run_id=run_id, run_key=run_key, attempt=attempt, status=RUNNING,
                                     exit_code=EXIT_ERROR, abandoned_run_ids=abandoned)

    if in_progress is not None:  # raised after the commit, so the ABANDONED updates persist
        for stale in in_progress.abandoned_run_ids:
            log_event("abandoned", run_id=stale, level=logging.WARNING, failure_reason="STALE_RUNNING")
        raise in_progress
    if outcome.status == SKIPPED:
        log_event("skipped", run_id=outcome.run_id, skipped_of_run_id=outcome.skipped_of_run_id,
                  recon_status=outcome.recon_status, exit_code=outcome.exit_code)
    return outcome


def _log_admitted(outcome: RunOutcome, metrics: RunMetrics) -> None:
    for stale in outcome.abandoned_run_ids:
        log_event("abandoned", run_id=outcome.run_id, level=logging.WARNING, abandoned_run_id=stale,
                  failure_reason="STALE_RUNNING")
    log_event("run_started", run_id=outcome.run_id, run_key=outcome.run_key, attempt=outcome.attempt)
    checks = metrics.stage_named("batch_checks")
    if checks is not None:
        log_event("stage_end", run_id=outcome.run_id, stage=checks.name, duration_ms=checks.duration_ms)


def _execute(conn: sqlite3.Connection, outcome: RunOutcome, paths: dict[str, str], config: Config,
             checks: dict[str, ingest.FileCheck], metrics: RunMetrics, fail_at: str | None) -> None:
    """The data transaction plus the final run-row update. Mutates outcome.

    Landing re-hashes every file and compares with the batch-check pass, so the bytes that were hashed
    into run_key are exactly the bytes landed (otherwise INPUT_CHANGED).
    """
    run_id = outcome.run_id
    try:
        with transaction(conn):
            with metrics.stage("bronze") as stage:
                landed = ingest.ingest(conn, run_id, paths, config, checks=checks)
                if not landed.ok:
                    raise BatchRejected(landed.rejects)
                for side, s in landed.sides.items():
                    stage.record_side(side, rows_in=s.data_records + s.blank_lines, rows_out=s.data_records,
                                      rows_rejected=0, blank_lines=s.blank_lines)
                for warning in landed.warnings:
                    log_event("batch_warning", run_id=run_id, stage="bronze", level=logging.WARNING,
                              side=warning.side, code=warning.code, columns=",".join(warning.columns))
            _log_stage(run_id, stage)
            _maybe_fail(fail_at, "after_bronze")

            with metrics.stage("silver") as stage:
                silver = build_silver(conn, run_id, config)
                _record_silver(stage, silver)
            _log_stage(run_id, stage)
            _maybe_fail(fail_at, "after_silver")

            with metrics.stage("gold") as stage:
                result = compare.reconcile(conn, run_id, config, landed.warnings)
                for side in (SOURCE, TARGET):
                    accepted = silver.sides[side].counts.accepted
                    stage.record_side(side, rows_in=accepted, rows_out=accepted, rows_rejected=0)
                stage.counts.update(keys=result.total_keys,
                                    field_diffs=sum(d["count"] for d in result.field_diff_counts),
                                    unattributable_rejects=result.unattributable_rejects,
                                    key_status=dict(sorted(result.key_status_counts.items())))
            _log_stage(run_id, stage)
            _maybe_fail(fail_at, "after_gold")

            outcome.status = SUCCEEDED
            outcome.recon_status = result.recon_status
            outcome.exit_code = EXIT_PASS if result.recon_status == compare.PASS else EXIT_FAIL
            run_repo.finish_run(conn, run_id, status=SUCCEEDED, finished_at=utc_iso(),
                                recon_status=outcome.recon_status, exit_code=outcome.exit_code)
    except BatchRejected as exc:
        for issue in exc.rejects:
            log_event("batch_reject", run_id=run_id, stage="bronze", level=logging.ERROR, side=issue.side,
                      code=issue.code, file=issue.file, detail=issue.detail)
        _fail(conn, outcome, REJECTED, exc.failure_reason, exc.failure_detail)
    except (InvariantViolation, InputChangedError, AggregateOverflow) as exc:
        _fail(conn, outcome, FAILED, exc.failure_reason, exc.failure_detail)
    except Exception as exc:  # noqa: BLE001 - any other error is UNEXPECTED_ERROR; the data already rolled back
        _fail(conn, outcome, FAILED, UNEXPECTED_ERROR, f"{type(exc).__name__}: {exc}")
    else:
        log_event("run_finished", run_id=run_id, status=SUCCEEDED, recon_status=outcome.recon_status,
                  exit_code=outcome.exit_code)


def _fail(conn: sqlite3.Connection, outcome: RunOutcome, status: str, reason: str, detail: str) -> None:
    """The data transaction has rolled back; record the terminal status in its own short transaction."""
    outcome.status, outcome.exit_code = status, EXIT_ERROR
    outcome.recon_status = None
    outcome.failure_reason, outcome.failure_detail = reason, detail
    with transaction(conn):
        run_repo.finish_run(conn, outcome.run_id, status=status, finished_at=utc_iso(), exit_code=EXIT_ERROR,
                            failure_reason=reason, failure_detail=detail)
    log_event("run_finished", run_id=outcome.run_id, level=logging.ERROR, status=status,
              failure_reason=reason, failure_detail=detail, exit_code=EXIT_ERROR)


def _record_silver(stage: StageMetrics, silver: SilverResult) -> None:
    if silver.reference is not None:
        ref = silver.reference
        accepted, rejected = len(ref.customers), len(ref.rejects)
        stage.record_side(REFERENCE, rows_in=ref.rows_read, rows_out=accepted, rows_rejected=rejected,
                          duplicates_ignored=ref.rows_read - accepted - rejected)
    for side in (SOURCE, TARGET):
        c = silver.sides[side].counts
        stage.record_side(side, rows_in=c.rows_read, rows_out=c.accepted, rows_rejected=c.rejected,
                          duplicate_exact=c.duplicate_exact, superseded=c.superseded,
                          conflicting_rows=c.conflicting_rows)


def _log_stage(run_id: str, stage: StageMetrics) -> None:
    for side, rows in stage.sides.items():
        log_event("side_rows", run_id=run_id, stage=stage.name, side=side, **rows)
    scalar_counts = {k: v for k, v in stage.counts.items() if not isinstance(v, (dict, list))}
    log_event("stage_end", run_id=run_id, stage=stage.name, duration_ms=stage.duration_ms, **scalar_counts)
