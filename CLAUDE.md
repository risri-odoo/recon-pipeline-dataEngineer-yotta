# CLAUDE.md — Standing rules

Source-vs-target data reconciliation pipeline (hiring hackathon: Associate Data Forward Deployed Engineer).
Compares two datasets by row counts, business keys, aggregates and field-level checks; writes a
machine-readable report and isolates mismatched records.

## Rules

1. **Runtime: Python 3.11+, standard library only** (csv, sqlite3, decimal, json, hashlib, logging,
   argparse, time, tracemalloc). `pytest` is the only dev dependency. No pandas, no ORMs.
2. **3-tier data architecture (medallion) in one SQLite DB.** Every row carries `run_id`.
   - BRONZE = raw landed lines: `run`, `raw_record`
   - SILVER = validated / normalized / deduped: `clean_record`, `ref_customer`, `quarantine`
   - GOLD = reconciliation results: `key_result`, `field_diff`, `aggregate_result`, `check_result`
3. **3-tier code architecture:** presentation (`recon/presentation`: CLI, report writers) ->
   service (`recon/service`: orchestration and business rules) -> data access
   (`recon/repository`: DDL and ALL SQL).
   - No SQL strings outside `recon/repository`.
   - Presentation never imports repository directly.
4. **Money is never float:** parse with `Decimal`, store as integer minor units.
   Timestamps stored as ISO-8601 UTC `YYYY-MM-DDTHH:MM:SSZ`.
5. **Deterministic:** same inputs + same config = identical outputs except run timestamps.
   Every output sorted by business key.
6. **Traceability:** every rejected or excluded row keeps `run_id`, `side`, `source_file`,
   `line_no`, `raw_text`, `reason_code`, `detail`.
7. **Naming:** snake_case tables/columns, UPPER_SNAKE reason codes, stable deterministic IDs.
8. **Each step:** write code AND tests together, run `python -m pytest -q`, then report: files
   changed, test results, assumptions made, anything unsure about. Then STOP. Never start the
   next step on your own.
9. **Ambiguity:** if something is ambiguous, ask. Record each design decision as one dated line
   in `docs/DECISIONS.md`.
