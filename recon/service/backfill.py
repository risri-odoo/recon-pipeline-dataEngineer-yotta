"""Backfill: one independent `recon run` per manifest line (docs/DESIGN.md 6.6).

The manifest is a CSV with exactly the columns batch_id, source_path, target_path, reference_path
(header names trimmed + lower-cased, any order). It is validated completely before the first run; a
malformed manifest raises ManifestError and nothing runs. Relative paths resolve against the manifest's
directory.

Lines run in file order, each through pipeline.run_pipeline with all of its rules (SKIPPED, --force-free
retry, stale RUNNING). A failing line never stops the others. The summary is sorted by batch_id and the
process exit code is the maximum over all lines.
"""

from __future__ import annotations

import csv
import logging
from dataclasses import dataclass, replace
from pathlib import Path

from recon.service import pipeline
from recon.service.config import load_config
from recon.service.metrics import log_event
from recon.service.pipeline import RunInProgress, RunOutcome, RunRequest

MANIFEST_COLUMNS = ("batch_id", "source_path", "target_path", "reference_path")
IN_PROGRESS = "IN_PROGRESS"  # summary status for a line blocked by a fresh RUNNING attempt (no run row)
ERROR = "ERROR"  # summary status for a line whose run raised before a run row could record it


class ManifestError(ValueError):
    """The manifest is missing, unreadable or malformed. Raised before any run starts."""


@dataclass(frozen=True)
class ManifestLine:
    line_no: int
    batch_id: str
    source: Path
    target: Path
    reference: Path


@dataclass(frozen=True)
class BackfillRequest:
    manifest: Path
    config_path: Path
    db_path: Path = pipeline.DEFAULT_DB
    out_dir: Path = pipeline.DEFAULT_OUT
    stale_after_minutes: int = pipeline.DEFAULT_STALE_AFTER_MINUTES
    profile_memory: bool = False


@dataclass
class BackfillLine:
    batch_id: str
    line_no: int
    status: str  # a run status, or IN_PROGRESS / ERROR
    exit_code: int
    run_id: str | None = None  # the run whose results apply (for SKIPPED: the reused SUCCEEDED run)
    recon_status: str | None = None
    error: str | None = None
    outcome: RunOutcome | None = None


@dataclass
class BackfillResult:
    lines: list[BackfillLine]  # manifest (execution) order

    @property
    def summary(self) -> list[BackfillLine]:
        return sorted(self.lines, key=lambda line: line.batch_id)

    @property
    def exit_code(self) -> int:
        return max(line.exit_code for line in self.lines)


# --------------------------------------------------------------------------- manifest


def parse_manifest(path: str | Path) -> list[ManifestLine]:
    path = Path(path)
    if not path.is_file():
        raise ManifestError(f"manifest not found: {path}")
    try:
        text = path.read_bytes().decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ManifestError(f"manifest {path} is not valid UTF-8: {exc.reason}") from None

    reader = csv.reader(text.splitlines(), strict=True)
    rows: list[tuple[int, list[str]]] = []  # (line where the record starts, fields)
    try:
        start = 1
        for row in reader:
            rows.append((start, row))
            start = reader.line_num + 1
    except csv.Error as exc:
        raise ManifestError(f"manifest {path} line {reader.line_num}: {exc}") from None
    rows = [(n, row) for n, row in rows if any(cell.strip() for cell in row)]  # blank lines are ignored
    if not rows:
        raise ManifestError(f"manifest {path} is empty (no header)")

    header_no, header = rows[0]
    names = [h.strip().lower() for h in header]
    duplicates = sorted({n for n in names if names.count(n) > 1})
    if duplicates:
        raise ManifestError(f"manifest header line {header_no}: duplicate column(s) {', '.join(duplicates)}")
    missing = [c for c in MANIFEST_COLUMNS if c not in names]
    if missing:
        raise ManifestError(f"manifest header line {header_no}: missing column(s) {', '.join(missing)}")
    unknown = [n for n in names if n not in MANIFEST_COLUMNS]
    if unknown:
        raise ManifestError(f"manifest header line {header_no}: unknown column(s) {', '.join(unknown)}")
    if len(rows) == 1:
        raise ManifestError(f"manifest {path} has a header but no lines")

    index = {name: names.index(name) for name in MANIFEST_COLUMNS}
    base = path.parent
    lines: list[ManifestLine] = []
    seen: dict[str, int] = {}
    for line_no, row in rows[1:]:
        if len(row) != len(names):
            raise ManifestError(f"manifest line {line_no}: {len(row)} fields, header has {len(names)}")
        values = {name: row[i].strip() for name, i in index.items()}
        empty = [name for name, value in values.items() if not value]
        if empty:
            raise ManifestError(f"manifest line {line_no}: empty {', '.join(empty)}")
        batch_id = values["batch_id"]
        if batch_id in seen:
            raise ManifestError(f"manifest line {line_no}: duplicate batch_id {batch_id!r} (first on line {seen[batch_id]})")
        seen[batch_id] = line_no
        lines.append(ManifestLine(line_no, batch_id, *(base / values[c] for c in MANIFEST_COLUMNS[1:])))
    return lines


# --------------------------------------------------------------------------- run


def run_backfill(request: BackfillRequest) -> BackfillResult:
    """Validate everything shared by the lines first (manifest, config, settings), then run each line."""
    lines = parse_manifest(request.manifest)
    load_config(request.config_path)  # a bad config is a ConfigError before any run
    pipeline.fail_point_from_env()
    if request.stale_after_minutes < 0:
        raise ValueError("stale_after_minutes must be >= 0")

    template = RunRequest(source=Path(), target=Path(), reference=Path(), config_path=request.config_path,
                          db_path=request.db_path, out_dir=request.out_dir,
                          stale_after_minutes=request.stale_after_minutes, profile_memory=request.profile_memory)
    results = []
    for line in lines:
        run_request = replace(template, source=line.source, target=line.target, reference=line.reference)
        results.append(_run_line(line, run_request))
    return BackfillResult(results)


def _run_line(line: ManifestLine, run_request: RunRequest) -> BackfillLine:
    try:
        outcome = pipeline.run_pipeline(run_request)
    except RunInProgress as exc:
        result = BackfillLine(line.batch_id, line.line_no, IN_PROGRESS, pipeline.EXIT_ERROR, run_id=exc.run_id,
                              error=str(exc))
    except Exception as exc:  # noqa: BLE001 - one failing line never stops the others
        result = BackfillLine(line.batch_id, line.line_no, ERROR, pipeline.EXIT_ERROR,
                              error=f"{type(exc).__name__}: {exc}")
    else:
        result = BackfillLine(line.batch_id, line.line_no, outcome.status, outcome.exit_code,
                              run_id=outcome.result_run_id, recon_status=outcome.recon_status, outcome=outcome)
    log_event("backfill_line", run_id=result.run_id, level=logging.INFO if result.exit_code < 2 else logging.ERROR,
              batch_id=result.batch_id, manifest_line=result.line_no, status=result.status,
              recon_status=result.recon_status, exit_code=result.exit_code, error=result.error)
    return result
