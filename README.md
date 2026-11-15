# recon

Source-vs-target data reconciliation pipeline. Compares two datasets using row counts, business
keys, aggregates and field-level checks, writes a machine-readable report, and isolates
mismatched records.

> Stub — usage, architecture and results will be filled in as the pipeline is built.

## Quick start

```
python -m venv .venv
.venv\Scripts\activate        # Windows  (source .venv/bin/activate on POSIX)
pip install -e .[dev]
python -m pytest -q
python -m recon --help
```
