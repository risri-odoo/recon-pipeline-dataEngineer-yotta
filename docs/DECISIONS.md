# Design decisions

One dated line per decision.

- 2026-11-15: Console script `recon` points at `recon.presentation.cli:main`; `python -m recon` delegates to the same entry point.
- 2026-11-15: Dev tooling lives in a local `.venv` (gitignored); pytest declared as the `dev` optional dependency in pyproject.toml.
- 2026-11-15: `.gitattributes` forces LF for text (`* text=auto eol=lf`, `*.json text eol=lf`) and marks `*.csv -text` so git never rewrites CSV bytes; file hashes and byte-identical report tests must not depend on the OS.
