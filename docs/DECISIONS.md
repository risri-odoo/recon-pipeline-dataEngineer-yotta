# Design decisions

One dated line per decision.

- 2026-11-15: Console script `recon` points at `recon.presentation.cli:main`; `python -m recon` delegates to the same entry point.
- 2026-11-15: Dev tooling lives in a local `.venv` (gitignored); pytest declared as the `dev` optional dependency in pyproject.toml.
- 2026-11-15: `.gitattributes` forces LF for text (`* text=auto eol=lf`, `*.json text eol=lf`) and marks `*.csv -text` so git never rewrites CSV bytes; file hashes and byte-identical report tests must not depend on the OS.
- 2026-11-15: All CSV writers open files with `newline=""` and use `lineterminator="\n"`, so output bytes are identical on Windows and Linux.
- 2026-11-15: SILVER gains `ref_customer(run_id, customer_id, customer_name)`, PK (run_id, customer_id), holding the validated reference customers (CLAUDE.md rule 2 updated).
- 2026-11-15: Batch warnings get no table: one check_result row per warning check (SCHEMA_DRIFT, MISSING_OPTIONAL_COLUMN, EMPTY_BATCH) with JSON detail `[{side, columns}]`, plus a `warnings` array in report.json.
- 2026-11-15: KEY_UNIQUENESS, REFERENTIAL_INTEGRITY and KEY_COVERAGE are engine invariants; a failure is a bug, so the run rolls back to FAILED with failure_reason `INVARIANT_VIOLATION:<check>` and counts in failure_detail.
- 2026-11-15: Data problems (rejects, conflicts, mismatches) affect only recon_status, never run status.
- 2026-11-15: Backfill = `recon backfill --manifest PATH`, manifest columns batch_id, source_path, target_path, reference_path; one independent run per line in order, continue past failures, print a summary table, skip SUCCEEDED pairs on re-run.
- 2026-11-15: Fractional seconds are accepted and truncated to whole seconds (warehouses commonly truncate ms; second precision avoids false mismatches).
- 2026-11-15: Batch rejects are not stored in quarantine: the data transaction rolls back and the separately committed run row gets status REJECTED, failure_reason = code, failure_detail = detail.
- 2026-11-15: A rejected newest version never falls back to an older valid one: the key becomes SOURCE_REJECTED / TARGET_REJECTED and older valid rows are SUPERSEDED; version ordering uses updated_at whenever it parsed, and rows with an unparsed updated_at cannot win.
- 2026-11-15: Stale RUNNING: started_at older than `--stale-after-minutes` (default 60) becomes ABANDONED; otherwise exit 2 "run in progress" and no row is created; no PID checks.
- 2026-11-15: SKIPPED keeps its run row (skipped_of_run_id), does not rewrite report files, prints the existing report path and returns that run's exit code; no new bronze data, silver or gold rows.
- 2026-11-15: Known limitations: all currencies treated as 2 decimals (JPY, KWD unsupported); currency codes not checked against ISO 4217.
- 2026-11-15: A key rejected on both sides keeps SOURCE_REJECTED and gets key_result.note = 'rejected on both sides'.
- 2026-11-15: run columns renamed for clarity: recon_result -> recon_status, error_detail -> failure_reason + failure_detail, skipped_of -> skipped_of_run_id.
- 2026-11-15: SRC_ROW_ACCOUNTING, TGT_ROW_ACCOUNTING and AGGREGATE_EXPLAINED are also INVARIANT checks (a failure means FAILED run with INVARIANT_VIOLATION:<check>).
- 2026-11-15: recon_status = PASS iff every key_result row is MATCHED and there is no SOURCE/TARGET ROW_REJECT without an order_id; otherwise FAIL.
- 2026-11-15: A SKIPPED invocation writes only its own run row; nothing else in bronze, silver or gold.
- 2026-11-15: A tie at max updated_at between a rejected and a valid row is treated as rejected (conservative): key *_REJECTED, valid rows SUPERSEDED with detail "shadowed by rejected line N".
- 2026-11-15: Blank lines (empty or whitespace-only) are not landed in raw_record and not quarantined; counted per side as metrics blank_lines; row accounting covers landed DATA records only.
- 2026-11-15: Config shape: `fields` list (order = validation order) with types str|enum|decimal|timestamp; top-level `business_key`, `version_field`, `compare_fields`, `columns.source/target`, `reference`, `aggregates.sum_fields/group_by`; unknown keys anywhere are a ConfigError.
- 2026-11-15: Decimal config values (`min`, `tolerance`) must be JSON strings, never JSON numbers, so no float ever enters the config; tolerance is stored as integer minor units (0.01 -> 1).
- 2026-11-15: config_sha256 hashes the raw config with per-type defaults filled in (required=false; str upper=false, pattern=null, reference=false; decimal scale=2, min=null, tolerance="0").
- 2026-11-15: Currency shape is expressed as str + `pattern: ^[A-Z]{3}$` (applied after upper-casing) rather than a dedicated type.
- 2026-11-15: BAD_FORMAT vs BAD_TYPE: money is BAD_FORMAT if, after removing currency-symbol characters, it looks numeric (digits with , . sign or exponent), else BAD_TYPE; a timestamp is BAD_FORMAT if it looks like a date (d-d-d, d/d/d, d.d.d) but not the accepted form, BAD_TYPE otherwise or when the calendar date/time/offset is impossible.
- 2026-11-15: Physical column names (side maps and reference.columns values) are not snake_case-checked: they are normalized with trim + lower-case (same as headers), must be non-empty after that, and must be unique per side after normalization. Logical field names stay strict snake_case.
- 2026-11-15: validate_row takes an optional reference-id set; INVALID_REFERENCE is added only when every other field passed.
- 2026-11-15: Generator (`recon/generator`) is a standalone package that never imports recon.service/recon.repository (enforced by an AST test); expected answers come only from its own per-line bookkeeping. The CLI may import it (it is not the repository tier).
- 2026-11-15: Generator bucket size = max(1, floor(N * rate)) with rates as integer per-mille; one `rng.sample` of base indices is sliced in a fixed scenario order, so buckets are disjoint by construction; N below the number of base-key scenarios (16) is an error.
- 2026-11-15: Generated files are `source.csv`, `target.csv`, `customers.csv`; source is written in business-key order, target shuffled with the same rng; ~N/50 customers = max(1, N // 50).
- 2026-11-15: src_malformed keys keep a valid target row, so their expected key status is EXTRA_IN_TARGET (a MALFORMED_ROW has no trusted order_id) and they count in `unattributable_rejects`.
- 2026-11-15: Injected bad values: BAD_TYPE = amount `abc` or order_ts `yesterday`; MISSING_REQUIRED blanks one of customer_id/order_ts/status/amount/currency (never order_id or updated_at, so the key and version stay attributable); INVALID_REFERENCE uses `CX<n>`; MALFORMED_ROW appends one extra field.
- 2026-11-15: expected/<name>.json always includes `scenario_keys` (all N, not only the sample), all key statuses and row-reject codes with zero counts for a stable shape, field diffs as a list of {field_name, diff_type, count}, and sums as 2-decimal strings.
