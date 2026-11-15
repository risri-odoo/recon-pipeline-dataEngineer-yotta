"""Command-line interface (argparse) for running reconciliations."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from recon.generator.generate import generate, write_dataset


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
    return parser


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


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
