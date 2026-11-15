"""`recon bench`: generate a synthetic dataset, reconcile it into a fresh DB, and record the figures.

Runs per benchmark, each into its own fresh DB, on the same generated files:
- `--repeat K` **timed** runs without tracemalloc (tracing slows Python allocation down, so it would distort
  timings): per-stage seconds, report seconds and total wall time are the medians over the K runs (the total's
  min-max is shown too), the median CPU seconds of the process (time.process_time, which is far less
  sensitive to other load on the machine than wall time), plus records/sec and DB size. All K runs must give
  the same result digest;
- a **traced** run with tracemalloc (`--profile-memory`): peak Python memory only. tracemalloc does not see
  SQLite's own C allocations, so the figure is a lower bound of the process footprint.

Each benchmark appends one row to the results table in docs/PERFORMANCE.md (between the bench markers), so
before/after measurements stay in one table and the hand-written sections of the document are kept.
`result digest` = sha256 over the five report files with the run timestamps and the (temporary) input
directory masked: two rows with the same digest produced byte-identical reports for the same input paths.
"""

from __future__ import annotations

import hashlib
import json
import platform
import re
import statistics
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from recon.generator.generate import generate, write_dataset
from recon.presentation.report_writer import DIGEST_FILES, write_report
from recon.service import pipeline, report

STAGES = ("batch_checks", "bronze", "silver", "gold")
SIDES = ("SOURCE", "TARGET", "REFERENCE")
TABLE_START = "<!-- bench:results:start -->"
TABLE_END = "<!-- bench:results:end -->"
TABLE_HEADER = (
    "| label | date (UTC) | env | n | seed | data records S / T / R | runs | batch_checks s | bronze s "
    "| silver s | gold s | report s | total s | total min-max s | CPU s | records/s | peak MiB (tracemalloc) | DB MiB "
    "| keys | field diffs | result digest |\n"
    "|---|---|---|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|---|\n"
)
DEFAULT_DOC = f"""# Performance

Measured with `recon bench --n N --seed S --repeat K --label L`. Each row is one benchmark: a generated
dataset reconciled K times, each time into a fresh SQLite DB. Stage and total seconds are medians over the K
runs without tracemalloc; peak memory comes from one extra, traced run (Python allocations only). `total s` = pipeline + report files,
in-process wall time (no interpreter start-up, no data generation); `CPU s` = CPU time of the process over
the same span. `records/s` = data records of all three
inputs / `total s`. Rows with the same `result digest` produced byte-identical report files.

## Results

{TABLE_START}
{TABLE_HEADER}{TABLE_END}
"""
_TIMESTAMP = re.compile(rb'((?:started_at|finished_at)\W+)\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ')
MIB = 1024 * 1024


class BenchError(RuntimeError):
    """The benchmark run did not succeed (it must, for its figures to mean anything)."""


@dataclass(frozen=True)
class BenchResult:
    label: str
    date: str
    env: str
    n: int
    seed: int
    repeat: int
    rows: dict[str, int]  # landed data records per side
    stage_seconds: dict[str, float]
    report_seconds: float
    total_seconds: float
    total_min_seconds: float
    total_max_seconds: float
    cpu_seconds: float
    peak_memory_bytes: int
    db_bytes: int
    keys: int
    field_diffs: int
    result_digest: str

    @property
    def records(self) -> int:
        return sum(self.rows.values())

    @property
    def records_per_second(self) -> float:
        return self.records / self.total_seconds


@dataclass(frozen=True)
class _Run:
    stage_seconds: dict[str, float]
    report_seconds: float
    total_seconds: float
    cpu_seconds: float
    peak_memory_bytes: int | None
    rows: dict[str, int]
    db_bytes: int
    report: Path


def run_bench(n: int, seed: int, config_path: Path, label: str, repeat: int = 1) -> BenchResult:
    """Generate (untimed), then `repeat` timed runs and one traced run, each into a fresh DB in a temp dir."""
    if repeat < 1:
        raise ValueError("repeat must be >= 1")
    with tempfile.TemporaryDirectory(prefix="recon-bench-") as tmp:
        work = Path(tmp)
        data_dir, _ = write_dataset(generate(n, seed), "bench", work / "generated", work / "expected")
        timed = [_run(data_dir, config_path, work / f"timed{i}", profile_memory=False) for i in range(repeat)]
        digests = {report_digest(r.report.parent, data_dir) for r in timed}
        if len(digests) != 1:
            raise BenchError(f"repeated runs gave different report digests: {sorted(digests)}")
        traced = _run(data_dir, config_path, work / "traced", profile_memory=True)
        totals = [r.total_seconds for r in timed]
        doc = json.loads(timed[0].report.read_text(encoding="utf-8"))
        return BenchResult(
            label=label, date=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            env=f"{sys.platform} py{platform.python_version()}", n=n, seed=seed, repeat=repeat,
            rows=timed[0].rows,
            stage_seconds={name: statistics.median(r.stage_seconds[name] for r in timed) for name in STAGES},
            report_seconds=statistics.median(r.report_seconds for r in timed),
            total_seconds=statistics.median(totals), total_min_seconds=min(totals), total_max_seconds=max(totals),
            cpu_seconds=statistics.median(r.cpu_seconds for r in timed),
            peak_memory_bytes=traced.peak_memory_bytes or 0, db_bytes=timed[0].db_bytes,
            keys=doc["totals"]["keys"], field_diffs=doc["totals"]["field_diffs"], result_digest=digests.pop())


def _run(data_dir: Path, config_path: Path, out: Path, *, profile_memory: bool) -> _Run:
    request = pipeline.RunRequest(
        source=data_dir / "source.csv", target=data_dir / "target.csv", reference=data_dir / "customers.csv",
        config_path=config_path, db_path=out / "bench.db", out_dir=out, profile_memory=profile_memory)
    t0, cpu0 = time.perf_counter(), time.process_time()
    outcome = pipeline.run_pipeline(request)
    if outcome.status != pipeline.SUCCEEDED:
        raise BenchError(f"benchmark run ended {outcome.status}: {outcome.failure_reason}")
    t_report = time.perf_counter()
    with report.open_report(request.db_path, outcome.run_id) as bundle:
        report_path = write_report(out / outcome.run_id, bundle)
    t_end, cpu_end = time.perf_counter(), time.process_time()
    metrics = outcome.metrics
    stages = {s["stage"]: s for s in metrics["stages"]}
    bronze_sides = stages["bronze"]["sides"]
    return _Run(stage_seconds={name: stages[name]["duration_ms"] / 1000 for name in STAGES},
                report_seconds=t_end - t_report, total_seconds=t_end - t0, cpu_seconds=cpu_end - cpu0,
                peak_memory_bytes=metrics["peak_memory_bytes"],
                rows={side: bronze_sides[side]["rows_out"] for side in SIDES},
                db_bytes=request.db_path.stat().st_size, report=report_path)


def report_digest(run_dir: Path, input_dir: Path | None = None) -> str:
    """sha256 over the report files (fixed order) with run timestamps masked, and input_dir masked in every
    spelling it can take in the files (as given, posix, JSON-escaped)."""
    spellings: list[bytes] = []
    if input_dir is not None:
        raw = str(input_dir)
        spellings = sorted({s.encode("utf-8") for s in (raw, Path(raw).as_posix(), json.dumps(raw)[1:-1])},
                           key=len, reverse=True)
    h = hashlib.sha256()
    for name in DIGEST_FILES:
        data = _TIMESTAMP.sub(rb"\1<ts>", (run_dir / name).read_bytes())
        for spelling in spellings:
            data = data.replace(spelling, b"<input_dir>")
        h.update(name.encode("utf-8") + b"\0" + data + b"\0")
    return h.hexdigest()


# --------------------------------------------------------------------------- PERFORMANCE.md


def format_row(r: BenchResult) -> str:
    s = r.stage_seconds
    cells = [r.label, r.date, r.env, str(r.n), str(r.seed), " / ".join(str(r.rows[x]) for x in SIDES),
             str(r.repeat), *(f"{s[name]:.2f}" for name in STAGES), f"{r.report_seconds:.2f}",
             f"{r.total_seconds:.2f}", f"{r.total_min_seconds:.2f}-{r.total_max_seconds:.2f}", f"{r.cpu_seconds:.2f}",
             f"{r.records_per_second:,.0f}", f"{r.peak_memory_bytes / MIB:.1f}", f"{r.db_bytes / MIB:.1f}",
             str(r.keys), str(r.field_diffs), f"`{r.result_digest[:12]}`"]
    return "| " + " | ".join(cells) + " |\n"


def append_result(doc_path: Path, result: BenchResult) -> None:
    """Add one row to the results table; create the document (or the table) if missing."""
    text = doc_path.read_text(encoding="utf-8") if doc_path.exists() else DEFAULT_DOC
    if TABLE_START not in text or TABLE_END not in text:
        text = text.rstrip("\n") + f"\n\n## Results\n\n{TABLE_START}\n{TABLE_HEADER}{TABLE_END}\n"
    head, tail = text.split(TABLE_END, 1)
    doc_path.parent.mkdir(parents=True, exist_ok=True)
    with open(doc_path, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(head + format_row(result) + TABLE_END + tail)

