"""Run metrics (stage timings, row counts, optional peak memory) and key=value structured logging.

Metrics are observability only: they are non-deterministic (timings, memory) and therefore never enter
the DB or report.json (DESIGN 7.1 #13). The pipeline returns them; the presentation tier writes
out/<run_id>/metrics.json.

Every log line is one `key=value` record:
    ts=2026-11-15T10:00:00Z level=INFO run_id=... stage=bronze event=stage_end duration_ms=1.234 ...
Values containing whitespace, '=' or '"' are JSON-quoted, so a line always parses back unambiguously.
"""

from __future__ import annotations

import json
import logging
import sys
import time
import tracemalloc
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

LOGGER_NAME = "recon"
logger = logging.getLogger(LOGGER_NAME)

TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def utc_iso(moment: datetime | None = None) -> str:
    """Canonical UTC timestamp `YYYY-MM-DDTHH:MM:SSZ` (now when no moment is given)."""
    moment = moment or datetime.now(timezone.utc)
    return moment.astimezone(timezone.utc).strftime(TS_FORMAT)


# --------------------------------------------------------------------------- metrics


@dataclass
class StageMetrics:
    name: str
    duration_ms: float | None = None
    peak_memory_bytes: int | None = None
    sides: dict[str, dict[str, int]] = field(default_factory=dict)  # side -> rows_in/rows_out/rows_rejected/...
    counts: dict[str, Any] = field(default_factory=dict)  # stage-level figures that have no side

    def record_side(self, side: str, *, rows_in: int, rows_out: int, rows_rejected: int, **extra: int) -> None:
        self.sides[side] = {"rows_in": rows_in, "rows_out": rows_out, "rows_rejected": rows_rejected, **extra}

    def as_dict(self) -> dict[str, Any]:
        return {"stage": self.name, "duration_ms": self.duration_ms, "peak_memory_bytes": self.peak_memory_bytes,
                "sides": self.sides, "counts": self.counts}


class RunMetrics:
    """Collects per-stage durations (perf_counter) and, with profile_memory, tracemalloc peaks per stage."""

    def __init__(self, profile_memory: bool = False, clock: Callable[[], float] = time.perf_counter):
        self.profile_memory = profile_memory
        self._clock = clock
        self.stages: list[StageMetrics] = []
        self._t0: float | None = None
        self.total_duration_ms: float | None = None
        self._we_started_tracing = False

    def start(self) -> None:
        if self.profile_memory and not tracemalloc.is_tracing():
            tracemalloc.start()
            self._we_started_tracing = True
        self._t0 = self._clock()

    def stop(self) -> None:
        if self._t0 is not None:
            self.total_duration_ms = _ms(self._clock() - self._t0)
        if self._we_started_tracing:
            tracemalloc.stop()
            self._we_started_tracing = False

    @contextmanager
    def stage(self, name: str) -> Iterator[StageMetrics]:
        """Time one stage. The duration is recorded even when the stage raises."""
        stage = StageMetrics(name)
        self.stages.append(stage)
        tracing = self.profile_memory and tracemalloc.is_tracing()
        if tracing:
            tracemalloc.reset_peak()
        t0 = self._clock()
        try:
            yield stage
        finally:
            stage.duration_ms = _ms(self._clock() - t0)
            if tracing:
                stage.peak_memory_bytes = tracemalloc.get_traced_memory()[1]

    def stage_named(self, name: str) -> StageMetrics | None:
        return next((s for s in self.stages if s.name == name), None)

    @property
    def peak_memory_bytes(self) -> int | None:
        peaks = [s.peak_memory_bytes for s in self.stages if s.peak_memory_bytes is not None]
        return max(peaks) if peaks else None

    def as_dict(self) -> dict[str, Any]:
        return {"profile_memory": self.profile_memory, "total_duration_ms": self.total_duration_ms,
                "peak_memory_bytes": self.peak_memory_bytes, "stages": [s.as_dict() for s in self.stages]}


def _ms(seconds: float) -> float:
    return round(seconds * 1000, 3)


# --------------------------------------------------------------------------- key=value logging


def _render(value: Any) -> str:
    text = "-" if value is None else str(value)
    if text == "" or any(c.isspace() or c in '="' for c in text):
        return json.dumps(text, ensure_ascii=False)
    return text


class KeyValueFormatter(logging.Formatter):
    """ts, level, run_id, stage, event first (always present, '-' when unknown), then the event's fields."""

    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.fromtimestamp(record.created, timezone.utc).strftime(TS_FORMAT)
        pairs: list[tuple[str, Any]] = [
            ("ts", ts), ("level", record.levelname),
            ("run_id", getattr(record, "run_id", None)), ("stage", getattr(record, "stage", None)),
            ("event", getattr(record, "event", None) or record.getMessage()),
        ]
        pairs += list(getattr(record, "fields", {}).items())
        return " ".join(f"{key}={_render(value)}" for key, value in pairs)


def log_event(event: str, *, run_id: str | None = None, stage: str | None = None,
              level: int = logging.INFO, **fields: Any) -> None:
    logger.log(level, event, extra={"run_id": run_id, "stage": stage, "event": event, "fields": fields})


class _StderrHandler(logging.StreamHandler):
    """Writes to whatever sys.stderr is at emit time (so redirected/captured stderr is honoured)."""

    def emit(self, record: logging.LogRecord) -> None:
        self.stream = sys.stderr
        super().emit(record)


class _RunLogHandler(logging.StreamHandler):
    """out/<run_id>/run.log, UTF-8 with LF line endings on every OS."""

    def __init__(self, path: Path):
        super().__init__(open(path, "w", encoding="utf-8", newline=""))

    def close(self) -> None:
        try:
            self.flush()
            self.stream.close()
        finally:
            super().close()


def configure_stderr_logging(level: int = logging.INFO) -> None:
    """Attach the key=value stderr handler to the recon logger once (idempotent)."""
    logger.setLevel(level)
    if not any(isinstance(h, _StderrHandler) for h in logger.handlers):
        handler = _StderrHandler()
        handler.setFormatter(KeyValueFormatter())
        logger.addHandler(handler)


@contextmanager
def run_log(path: Path) -> Iterator[None]:
    """Also write every recon log line to `path` for the duration of the block."""
    path.parent.mkdir(parents=True, exist_ok=True)
    handler = _RunLogHandler(path)
    handler.setFormatter(KeyValueFormatter())
    if logger.level == logging.NOTSET or logger.level > logging.INFO:
        logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    try:
        yield
    finally:
        logger.removeHandler(handler)
        handler.close()
