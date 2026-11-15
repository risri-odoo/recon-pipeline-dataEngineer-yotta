"""Command-line interface (argparse) for running reconciliations."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from recon.generator.generate import generate, write_dataset
from recon.presentation.report_writer import write_metrics
from recon.service import backfill, pipeline
from recon.service.config import ConfigError
from recon.service.metrics import configure_stderr_logging


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="recon", description="Source-vs-target data reconciliation.")
    sub = parser.add_subparsers(dest="command", required=True)

    gen = sub.add_parser("generate", help="Generate a synthetic source/target/reference dataset with expected answers.")
    gen.add_argument("--n", type=int, required=True, help="number of base orders")
    gen.add_argument("--seed", type=int, required=True, help="random seed")
    gen.add_argument("--name", required=True, help="dataset name")
    gen.add_argument("--generated-dir", type=Path, default=Path("data/generated"),
                     help="data files go to <generated-dir>/<name>/ (default: data/generated)")
    gen.add_argument("--expected-dir", type=Path, default=Path("data/expected"),
                     help="expected answers go to <expected-dir>/<name>.json (default: data/expected)")
    gen.set_defaults(func=_cmd_generate)

    run = sub.add_parser("run", help="Reconcile a source file against a target file.")
    run.add_argument("--source", type=Path, required=True, help="SOURCE (OLTP export) CSV")
    run.add_argument("--target", type=Path, required=True, help="TARGET (warehouse load) CSV")
    run.add_argument("--reference", type=Path, required=True, help="REFERENCE (customer list) CSV")
    run.add_argument("--force", action="store_true", help="start a new attempt even if this run_key SUCCEEDED")
    _add_run_options(run)
    run.set_defaults(func=_cmd_run)

    back = sub.add_parser("backfill", help="Run one reconciliation per manifest line.")
    back.add_argument("--manifest", type=Path, required=True,
                          help="CSV with batch_id, source_path, target_path, reference_path "
                               "(relative paths resolve against the manifest's directory)")
    _add_run_options(back)
    back.set_defaults(func=_cmd_backfill)
    return parser


def _add_run_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", type=Path, required=True, help="reconciliation config JSON")
    parser.add_argument("--db", type=Path, default=pipeline.DEFAULT_DB, help="SQLite DB (default: out/recon.db)")
    parser.add_argument("--out", type=Path, default=pipeline.DEFAULT_OUT,
                        help="run outputs go to <out>/<run_id>/ (default: out)")
    parser.add_argument("--stale-after-minutes", type=int, default=pipeline.DEFAULT_STALE_AFTER_MINUTES,
                        help="a RUNNING attempt older than this becomes ABANDONED (default: 60)")
    parser.add_argument("--profile-memory", action="store_true",
                        help="record peak memory per stage (tracemalloc)")


def _cmd_generate(args: argparse.Namespace) -> int:
    try:
        dataset = generate(args.n, args.seed)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    data_dir, expected_path = write_dataset(dataset, args.name, args.generated_dir, args.expected_dir)
    print(f"wrote {data_dir}")
    print(f"wrote {expected_path}")
    return 0


def _cmd_run(args: argparse.Namespace) -> int:
    configure_stderr_logging()
    request = pipeline.RunRequest(
        source=args.source, target=args.target, reference=args.reference, config_path=args.config,
        db_path=args.db, out_dir=args.out, force=args.force, stale_after_minutes=args.stale_after_minutes,
        profile_memory=args.profile_memory)
    try:
        outcome = pipeline.run_pipeline(request)
    except pipeline.RunInProgress as exc:
        print(f"run in progress: run_id={exc.run_id} started_at={exc.started_at}", file=sys.stderr)
        return pipeline.EXIT_ERROR
    except (ConfigError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return pipeline.EXIT_ERROR
    except Exception as exc:  # noqa: BLE001 - never let a crash exit 1, which means "recon FAIL"
        print(f"error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return pipeline.EXIT_ERROR

    if outcome.run_dir is not None and outcome.metrics is not None:
        write_metrics(outcome.run_dir, outcome.metrics)
    fields = {"status": outcome.status, "run_id": outcome.result_run_id}
    if outcome.status == pipeline.SKIPPED:
        fields["skip_run_id"] = outcome.run_id
    fields.update(attempt=outcome.attempt, recon_status=outcome.recon_status or "-", exit_code=outcome.exit_code)
    if outcome.failure_reason:
        fields["failure_reason"] = outcome.failure_reason
    fields["out"] = (args.out / outcome.result_run_id).as_posix()
    print(" ".join(f"{k}={v}" for k, v in fields.items()))
    return outcome.exit_code


SUMMARY_COLUMNS = ("batch_id", "run_id", "status", "recon_status", "exit_code")


def _cmd_backfill(args: argparse.Namespace) -> int:
    configure_stderr_logging()
    request = backfill.BackfillRequest(
        manifest=args.manifest, config_path=args.config, db_path=args.db, out_dir=args.out,
        stale_after_minutes=args.stale_after_minutes, profile_memory=args.profile_memory)
    try:
        result = backfill.run_backfill(request)
    except (backfill.ManifestError, ConfigError, ValueError) as exc:  # nothing has run yet
        print(f"error: {exc}", file=sys.stderr)
        return pipeline.EXIT_ERROR

    for line in result.lines:
        outcome = line.outcome
        if outcome is not None and outcome.run_dir is not None and outcome.metrics is not None:
            write_metrics(outcome.run_dir, outcome.metrics)
    print(format_summary(result.summary))
    return result.exit_code


def format_summary(lines: list[backfill.BackfillLine]) -> str:
    """Fixed-width table, one row per manifest line, in the order given (callers pass it sorted by batch_id)."""
    rows = [SUMMARY_COLUMNS] + [(line.batch_id, line.run_id or "-", line.status, line.recon_status or "-",
                                 str(line.exit_code)) for line in lines]
    widths = [max(len(row[i]) for row in rows) for i in range(len(SUMMARY_COLUMNS))]
    return "\n".join("  ".join(cell.ljust(w) for cell, w in zip(row, widths)).rstrip() for row in rows)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
