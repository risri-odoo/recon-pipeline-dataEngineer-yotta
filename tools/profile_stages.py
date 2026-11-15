"""Profile the SILVER and GOLD stages of one reconciliation (dev tool; see docs/PERFORMANCE.md).

Generates a synthetic dataset, then does two runs on it, each into a fresh SQLite DB:
1. an unprofiled run with time.perf_counter wrappers around the main functions of both stages
   ("Unprofiled split" in docs/PERFORMANCE.md);
2. a run with cProfile enabled around pipeline.build_silver and compare.reconcile, printing the top
   functions of each by cumulative time.

Run from the repository root:
    python tools/profile_stages.py --n 100000 --seed 1 --top 15
"""

from __future__ import annotations

import argparse
import cProfile
import io
import pstats
import re
import sys
import tempfile
import time
from collections import defaultdict
from collections.abc import Callable
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from recon.generator.generate import generate, write_dataset  # noqa: E402
from recon.repository import gold_repo, silver_repo  # noqa: E402
from recon.service import compare, pipeline, validate  # noqa: E402

# (module, function name, label) for the unprofiled split; module attributes are patched for one run.
SPLIT_TARGETS = (
    (pipeline, "validate_side", "silver: validate_side (parse + normalize + hash, both sides)"),
    (validate, "payload_sha256", "silver:   of which payload_sha256"),
    (validate, "parse_record", "silver:   of which parse_record (strict csv reader per row)"),
    (pipeline, "validate_reference", "silver: validate_reference"),
    (pipeline, "dedupe", "silver: dedupe"),
    (silver_repo, "insert_clean_records", "silver: insert_clean_records"),
    (silver_repo, "insert_quarantine", "silver: insert_quarantine"),
    *((gold_repo, name, f"gold: {name}") for name in (
        "insert_key_results", "insert_field_diffs", "apply_diff_counts", "insert_aggregates",
        "aggregate_breakdown", "key_coverage", "count_unknown_references", "count_duplicate_keys")),
)


def request_for(data_dir: Path, out: Path, config: Path) -> pipeline.RunRequest:
    return pipeline.RunRequest(source=data_dir / "source.csv", target=data_dir / "target.csv",
                               reference=data_dir / "customers.csv", config_path=config,
                               db_path=out / "profile.db", out_dir=out)


def run_checked(request: pipeline.RunRequest) -> pipeline.RunOutcome:
    outcome = pipeline.run_pipeline(request)
    if outcome.status != pipeline.SUCCEEDED:
        raise SystemExit(f"run ended {outcome.status}: {outcome.failure_reason}")
    return outcome


def patched(targets, wrap: Callable[[str, Callable], Callable]):
    """Replace each target with wrap(label, original); returns a function that restores them."""
    originals = [(module, name, getattr(module, name)) for module, name, _ in targets]
    for (module, name, label), (_, _, original) in zip(targets, originals):
        setattr(module, name, wrap(label, original))

    def restore() -> None:
        for module, name, original in originals:
            setattr(module, name, original)
    return restore


def unprofiled_split(data_dir: Path, work: Path, config: Path) -> str:
    totals: dict[str, float] = defaultdict(float)

    def timer(label: str, fn: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            t0 = time.perf_counter()
            try:
                return fn(*args, **kwargs)
            finally:
                totals[label] += time.perf_counter() - t0
        return wrapper

    restore = patched(SPLIT_TARGETS, timer)
    try:
        outcome = run_checked(request_for(data_dir, work / "split", config))
    finally:
        restore()
    lines = [f"{'stage ' + s['stage']:<64} {s['duration_ms'] / 1000:8.2f} s" for s in outcome.metrics["stages"]]
    lines += [f"{label:<64} {totals[label]:8.2f} s" for _, _, label in SPLIT_TARGETS]
    return "\n".join(lines)


def cprofile_stages(data_dir: Path, work: Path, config: Path, top: int) -> str:
    profiles: dict[str, tuple[cProfile.Profile, float]] = {}

    def profiler(label: str, fn: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            prof = cProfile.Profile()
            t0 = time.perf_counter()
            prof.enable()
            try:
                return fn(*args, **kwargs)
            finally:
                prof.disable()
                profiles[label] = (prof, time.perf_counter() - t0)
        return wrapper

    restore = patched(((pipeline, "build_silver", "silver"), (compare, "reconcile", "gold")), profiler)
    try:
        run_checked(request_for(data_dir, work / "cprofile", config))
    finally:
        restore()
    sections = []
    for label in ("silver", "gold"):
        prof, wall = profiles[label]
        buffer = io.StringIO()
        pstats.Stats(prof, stream=buffer).sort_stats("cumulative").print_stats(top)
        text = buffer.getvalue().replace(str(ROOT) + "\\", "").replace(str(ROOT) + "/", "")
        text = re.sub(r"\S*[\\/]Lib[\\/]", "", text)  # shorten standard-library paths
        sections.append(f"===== {label}: cProfile, top {top} by cumulative time "
                        f"(wall {wall:.2f} s, profiler overhead included)\n{text.strip()}")
    return "\n\n".join(sections)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--n", type=int, default=100_000, help="base orders to generate (default 100000)")
    parser.add_argument("--seed", type=int, default=1, help="generator seed (default 1)")
    parser.add_argument("--top", type=int, default=15, help="functions per stage (default 15)")
    parser.add_argument("--config", type=Path, default=ROOT / "config" / "orders_recon.json")
    parser.add_argument("--work", type=Path, help="keep data and DBs here (default: a temp dir, removed)")
    args = parser.parse_args(argv)

    with tempfile.TemporaryDirectory(prefix="recon-profile-") as tmp:
        work = args.work or Path(tmp)
        data_dir, _ = write_dataset(generate(args.n, args.seed), "profile", work / "generated", work / "expected")
        print(f"# n={args.n} seed={args.seed} python={sys.version.split()[0]}\n")
        print("===== unprofiled split (perf_counter wrappers, one run)")
        print(unprofiled_split(data_dir, work, args.config))
        print()
        print(cprofile_stages(data_dir, work, args.config, args.top))
    return 0


if __name__ == "__main__":
    sys.exit(main())
