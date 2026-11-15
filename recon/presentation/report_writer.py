"""Writes machine-readable reconciliation reports and mismatch extracts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

METRICS_FILE = "metrics.json"


def write_json(path: Path, value: Any) -> Path:
    """Sorted keys, ensure_ascii=False, final newline, LF on every OS (DESIGN 7.1 #12)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as fh:
        fh.write(json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False) + "\n")
    return path


def write_metrics(run_dir: Path, metrics: dict[str, Any]) -> Path:
    """out/<run_id>/metrics.json: timings and memory only, never part of report.json (DESIGN 7.1 #13)."""
    return write_json(run_dir / METRICS_FILE, metrics)
