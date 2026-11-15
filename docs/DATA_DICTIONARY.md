# Data dictionary

All tables live in one SQLite DB (default `out/recon.db`), created by `recon/repository/schema.py`.
Source of truth for behaviour: [DESIGN.md](DESIGN.md) sections 3 and 4.

Conventions:
- Every table carries `run_id`, and every primary key starts with it, so runs never overwrite each other.
- `side` is `SOURCE`, `TARGET` or `REFERENCE` (`clean_record` only SOURCE/TARGET).
- Timestamps are TEXT `YYYY-MM-DDTHH:MM:SSZ` (UTC, whole seconds).
- Money is INTEGER minor units in columns ending `_minor` (scale 2: `123.45` → `12345`). No floats anywhere.
- **Key**: PK = primary-key column, FK = foreign key, U = part of a UNIQUE constraint.
- Foreign keys are enforced (`foreign_keys=ON`). Indexes besides the PKs: `ix_quarantine_key (run_id, side, order_id)`,
  `ix_key_result_status (run_id, key_status)`.

## BRONZE

### run

One row per invocation of `recon run` / per backfill line, including SKIPPED. Written in its own short
transactions, separately from the data transaction, so FAILED/REJECTED/ABANDONED runs stay visible.

| column | type | null | key | description | derived from |
|---|---|---|---|---|---|
| run_id | TEXT | no | PK | run identifier, e.g. `c27d72e6f5d2f5f6-a1` | `run_key[:16] + "-a" + attempt` |
| run_key | TEXT | no | U | identifies the input set | sha256 of `source_sha256 \n target_sha256 \n reference_sha256 \n config_sha256` (missing file = `MISSING`) |
| attempt | INTEGER | no | U | 1-based; UNIQUE(run_key, attempt) | number of existing run rows for run_key + 1 (SKIPPED and ABANDONED included) |
| status | TEXT | no | | RUNNING, SUCCEEDED, FAILED, REJECTED, SKIPPED, ABANDONED | pipeline outcome; ABANDONED set by a later invocation (stale rule) |
| recon_status | TEXT | yes | | PASS or FAIL; only for SUCCEEDED (SKIPPED copies the reused run's) | PASS iff every key_result is MATCHED and no SOURCE/TARGET ROW_REJECT lacks an order_id |
| exit_code | INTEGER | yes | | 0 (PASS), 1 (FAIL), 2 (FAILED/REJECTED); NULL while RUNNING | status + recon_status |
| forced | INTEGER | no | | 1 if `--force` was given, else 0 | CLI |
| skipped_of_run_id | TEXT | yes | FK → run | for SKIPPED: the SUCCEEDED run that was reused | latest SUCCEEDED attempt of the run_key |
| source_file, target_file, reference_file | TEXT | no | | input paths as given (backfill: resolved against the manifest's directory) | CLI / manifest |
| source_sha256, target_sha256, reference_sha256 | TEXT | yes | | sha256 of the raw file bytes; NULL if the file is missing | batch-check pass over the raw bytes |
| config_sha256 | TEXT | no | | hash of the canonical config | sha256 of `json.dumps(config, sort_keys=True, separators=(",", ":"), ensure_ascii=False)` after defaults |
| config_json | TEXT | yes | | the canonical config JSON that config_sha256 hashes; used by `recon report` | config file + defaults |
| started_at | TEXT | no | | when the attempt started (non-deterministic) | clock |
| finished_at | TEXT | yes | | when it ended (non-deterministic) | clock |
| failure_reason | TEXT | yes | | REJECTED: first batch-reject code. FAILED: `INVARIANT_VIOLATION:<check>`, `INPUT_CHANGED`, `AGGREGATE_OVERFLOW`, `INTERRUPTED` (Ctrl-C / SystemExit) or `UNEXPECTED_ERROR`. ABANDONED: `STALE_RUNNING` | pipeline |
| failure_detail | TEXT | yes | | REJECTED: all batch rejects as JSON `[{side, file, code, detail}]`. FAILED: invariant counts, `{side, file, expected_sha256, actual_sha256}`, `{error, step}`, or exception type and message | pipeline |

### raw_record

Every record of every input file, landed verbatim. Nothing is parsed here. Blank lines are not landed.

| column | type | null | key | description | derived from |
|---|---|---|---|---|---|
| run_id | TEXT | no | PK, FK → run | | run |
| side | TEXT | no | PK | SOURCE, TARGET or REFERENCE | which CLI argument the file came from |
| line_no | INTEGER | no | PK | physical line where the record **starts** (header = its own line, usually 1); ≥ 1 | input file; skips over blank lines |
| source_file | TEXT | no | | input path | CLI / manifest |
| record_type | TEXT | no | | HEADER (first non-blank record) or DATA | position in file |
| raw_text | TEXT | no | | exact record text without its final line terminator; internal line breaks of quoted fields kept | input file bytes (UTF-8, BOM stripped) |

## SILVER

### clean_record

The surviving version of each business key per side: validated, normalized, deduplicated.

| column | type | null | key | description | derived from |
|---|---|---|---|---|---|
| run_id | TEXT | no | PK, FK → run, FK → raw_record | | run |
| side | TEXT | no | PK, FK → raw_record | SOURCE or TARGET | raw_record.side |
| order_id | TEXT | no | PK | business key, trimmed + ASCII upper-cased, never numerically coerced; composite keys joined with escaped `\|` | raw order_id column (`order_id` on both sides) |
| customer_id | TEXT | no | | trimmed + ASCII upper-cased; exists in ref_customer | SOURCE `customer_id`, TARGET `cust_id` |
| order_ts | TEXT | no | | UTC timestamp, fractional seconds truncated | SOURCE `order_ts`, TARGET `order_timestamp` |
| status | TEXT | no | | PLACED, SHIPPED, DELIVERED, CANCELLED or RETURNED | SOURCE `status`, TARGET `order_status` |
| amount_minor | INTEGER | no | | amount in minor units, 0 … 999999999999 | SOURCE `amount`, TARGET `total_amount`, parsed with Decimal |
| currency | TEXT | no | | 3 letters A–Z | SOURCE `currency`, TARGET `currency_code` |
| discount_minor | INTEGER | yes | | discount in minor units; NULL when empty or the column is absent (NULL ≠ 0) | SOURCE `discount`, TARGET `discount_amount` |
| updated_at | TEXT | no | | version timestamp (latest wins); not compared | SOURCE `updated_at`, TARGET `last_modified` |
| payload_sha256 | TEXT | no | | hash of all 8 normalized logical fields; equal hashes = exact duplicate | sha256 of the compact JSON array of the fields in config order, NULL as `\N`, money as minor-unit text |
| source_file | TEXT | no | | input path | raw_record.source_file |
| line_no | INTEGER | no | FK → raw_record | line of the kept version | raw_record.line_no |

### ref_customer

The validated reference customers.

| column | type | null | key | description | derived from |
|---|---|---|---|---|---|
| run_id | TEXT | no | PK, FK → run | | run |
| customer_id | TEXT | no | PK | trimmed + ASCII upper-cased; first occurrence kept, repeats ignored (not quarantined) | reference `customer_id` |
| customer_name | TEXT | yes | | informational only; trimmed, empty → NULL | reference `customer_name` |

### quarantine

Every excluded data record: row rejects and duplicate dispositions. Each landed DATA record of SOURCE/TARGET is
in exactly one of clean_record or quarantine. Batch rejects are not here (see `run.failure_detail`).

| column | type | null | key | description | derived from |
|---|---|---|---|---|---|
| run_id | TEXT | no | PK, FK → run, FK → raw_record | | run |
| side | TEXT | no | PK, FK → raw_record | SOURCE, TARGET or REFERENCE | raw_record.side |
| line_no | INTEGER | no | PK, FK → raw_record | data record line, ≥ 2 | raw_record.line_no |
| source_file | TEXT | no | | input path | raw_record.source_file |
| raw_text | TEXT | no | | the record exactly as landed | raw_record.raw_text |
| category | TEXT | no | | ROW_REJECT or DUPLICATE | validation / dedupe |
| reason_code | TEXT | no | | ROW_REJECT: MALFORMED_ROW, MISSING_REQUIRED, BAD_TYPE, BAD_FORMAT, BAD_ENUM, OUT_OF_RANGE, INVALID_REFERENCE (first failure). DUPLICATE: SUPERSEDED, DUPLICATE_EXACT, CONFLICTING_DUPLICATE | validation order / version ordering (DESIGN 4.3, 4.4) |
| field_name | TEXT | yes | | logical field of the first failure; NULL for MALFORMED_ROW and duplicates | validation |
| order_id | TEXT | yes | | normalized key if it parsed; NULL for MALFORMED_ROW | raw order_id column |
| updated_at | TEXT | yes | | canonical updated_at if it parsed, even on a rejected row (used for version ordering) | raw updated_at / last_modified |
| detail | TEXT | no | | human-readable; row rejects list every failure `;`-separated in field order; duplicates e.g. `superseded by line N (updated_at U)`, `shadowed by rejected line N` | validation / dedupe |

## GOLD

### key_result

One row per business key in the union of both sides (clean, or quarantined with a key).

| column | type | null | key | description | derived from |
|---|---|---|---|---|---|
| run_id | TEXT | no | PK, FK → run | | run |
| order_id | TEXT | no | PK | business key | clean_record ∪ keyed SOURCE/TARGET quarantine rows |
| key_status | TEXT | no | | first match of CONFLICT, MATCHED, MISMATCHED, SOURCE_REJECTED, TARGET_REJECTED, MISSING_IN_TARGET, EXTRA_IN_TARGET | DESIGN 4.5 over clean_record, quarantine and field_diff |
| source_line_no, target_line_no | INTEGER | yes | | the kept clean line, or the deciding quarantined line (lowest CONFLICTING_DUPLICATE, else newest ROW_REJECT); NULL if the side has nothing | clean_record.line_no / quarantine.line_no |
| source_updated_at, target_updated_at | TEXT | yes | | updated_at of that line | clean_record / quarantine |
| diff_count | INTEGER | no | | number of field_diff rows for the key, ≥ 0 | COUNT(field_diff) |
| note | TEXT | yes | | `rejected on both sides`, else NULL | quarantine |

### field_diff

One row per differing compared field of a key that is clean on both sides.

| column | type | null | key | description | derived from |
|---|---|---|---|---|---|
| run_id | TEXT | no | PK, FK → key_result | | run |
| order_id | TEXT | no | PK, FK → key_result | business key | clean_record |
| field_name | TEXT | no | PK | one of `compare_fields` | config |
| source_value, target_value | TEXT | yes | | normalized text; money as fixed-point (`123.45`); NULL only for discount | clean_record values |
| delta_minor | INTEGER | yes | | source − target in minor units, money fields only | amount_minor / discount_minor |
| diff_type | TEXT | no | | NULL_MISMATCH (one side NULL), else TARGET_STALE / TARGET_AHEAD / VALUE_MISMATCH by comparing updated_at | DESIGN 4.6; money equal when \|delta\| ≤ tolerance (1 minor unit) |

### aggregate_result

| column | type | null | key | description | derived from |
|---|---|---|---|---|---|
| run_id | TEXT | no | PK, FK → run | | run |
| metric | TEXT | no | PK | ROW_COUNT or SUM_<FIELD> (orders: SUM_AMOUNT, SUM_DISCOUNT) | config `aggregates.sum_fields` |
| group_by | TEXT | no | PK | ALL or <FIELD> (orders: STATUS, CURRENCY) | config `aggregates.group_by` |
| group_value | TEXT | no | PK | `*` for ALL; the group's value; `NULL` (string) for a NULL group | clean_record |
| source_value, target_value | INTEGER | no | | row count or sum in minor units; NULL discounts ignored; empty group = 0 | SQL COUNT / SUM over clean_record per side |
| delta | INTEGER | no | | source_value − target_value | |
| explained_delta | INTEGER | no | | the delta rebuilt bottom-up by walking key_result: a separate query path from the SUMs but over the same clean rows, so it catches aggregation-logic bugs, not missing rows (KEY_COVERAGE covers those) | key_result + clean_record (DESIGN 4.6) |
| unexplained_delta | INTEGER | no | | delta − explained_delta; must be 0 (AGGREGATE_EXPLAINED) | |

### check_result

All nine checks, written on every SUCCEEDED run.

| column | type | null | key | description | derived from |
|---|---|---|---|---|---|
| run_id | TEXT | no | PK, FK → run | | run |
| check_name | TEXT | no | PK | SRC_ROW_ACCOUNTING, TGT_ROW_ACCOUNTING, KEY_UNIQUENESS, REFERENTIAL_INTEGRITY, KEY_COVERAGE, AGGREGATE_EXPLAINED, SCHEMA_DRIFT, MISSING_OPTIONAL_COLUMN, EMPTY_BATCH | DESIGN 4.7 |
| severity | TEXT | no | | INVARIANT (engine check; a failure rolls back the run to FAILED, so it is never stored) or WARN | catalogue |
| status | TEXT | no | | PASS or WARN | check evaluation |
| expected, actual | TEXT | yes | | invariants: the compared counts; warnings: `0` and the number of sides affected | bronze/silver/gold counts |
| detail | TEXT | no | | canonical JSON (sorted keys); warnings: `[{"side", "columns"}]` | check evaluation |

## Input file layouts

All inputs: UTF-8 (a BOM is stripped), comma-delimited, `"` quoting, first non-blank record is the header.
Headers match after trim + lower-case, in any column order; extra columns are ignored (SCHEMA_EXTRA_COLUMN
warning). Every value is trimmed; empty after trim = NULL. One input row = one version of an order. File names
are arbitrary.

### SOURCE (OLTP export) — e.g. `data/sample/source.csv`

| column | logical field | required | accepted format |
|---|---|---|---|
| order_id | order_id | yes | any non-empty text; leading zeros kept (`0012` ≠ `12`) |
| customer_id | customer_id | yes | text; must exist in the reference file |
| order_ts | order_ts | yes | `YYYY-MM-DD[T\| ]HH:MM:SS[.fraction]` with optional `Z` or `±HH:MM`; naive = UTC |
| status | status | yes | PLACED, SHIPPED, DELIVERED, CANCELLED, RETURNED (any case) |
| amount | amount | yes | `^-?\d+(\.\d{1,2})?$`, 0 … 9999999999.99; no thousands separators, comma decimals, exponents or symbols |
| currency | currency | yes | 3 letters (any case) |
| discount | discount | no (column optional) | as amount, or empty |
| updated_at | updated_at | yes | as order_ts |

### TARGET (warehouse load) — e.g. `data/sample/target.csv`

Same logical fields and formats as SOURCE, under these headers:

| column | logical field |
|---|---|
| order_id | order_id |
| cust_id | customer_id |
| order_timestamp | order_ts |
| order_status | status |
| total_amount | amount |
| currency_code | currency |
| discount_amount | discount (column optional) |
| last_modified | updated_at |

### REFERENCE (customer list) — e.g. `data/sample/customers.csv`

| column | required | description |
|---|---|---|
| customer_id | yes | key checked by `customer_id` on orders; trimmed + ASCII upper-cased; an empty value is a MISSING_REQUIRED row reject |
| customer_name | no (column optional) | informational only |

### Backfill manifest

| column | description |
|---|---|
| batch_id | unique, non-empty label for the line |
| source_path, target_path, reference_path | input paths; relative paths resolve against the manifest's directory |
