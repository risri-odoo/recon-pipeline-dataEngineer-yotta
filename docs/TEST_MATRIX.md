# Test matrix

One row per edge case. Cases come from DESIGN.md (every batch reject, warning, row reject, duplicate
disposition, key status, diff type, check, run status, and the idempotency / retry / backfill rules) plus the
file-format list from the step-9 prompt. Where the prompt and DESIGN differ, DESIGN wins (e.g. the report is
`report.json`, not `recon_report.json`).

- **level**: `unit` = one module against a temp DB, `integration` = `run_pipeline` / CLI / backfill on a temp DB,
  `e2e` = generator + CLI + report files.
- **status**: `covered` = test existed before step 9, `added` = written in step 9, `added (step 9, QA pass)` = written in
  step 9 for the QA-pass cases [P1–P16](#qa-pass-edge-cases-p1p16) at the end, `documented, not tested` = behaviour
  stated in DESIGN without a test. Every other case has a test.
- Test paths are relative to `tests/`. `[*]` = every parameter of a parametrized test.

## File and batch level (DESIGN 4.1, 4.2, 7.1)

| # | level | case | expected behaviour | test file::test name | status |
|---|---|---|---|---|---|
| 1 | unit | Missing file | FILE_NOT_FOUND; sha256 NULL; nothing landed for any side | unit/test_ingest.py::test_missing_file_is_file_not_found | covered |
| 2 | unit | Path is a directory | FILE_NOT_FOUND | unit/test_ingest.py::test_directory_is_file_not_found | covered |
| 3 | unit | Zero-byte file | EMPTY_FILE; sha256 of empty bytes | unit/test_ingest.py::test_zero_byte_file_is_empty_file | covered |
| 4 | unit | Only blank lines | EMPTY_FILE (no non-blank header) | unit/test_ingest.py::test_only_blank_lines_is_empty_file | covered |
| 5 | unit | Non-UTF-8 bytes | ENCODING_ERROR with byte offset; nothing landed | unit/test_ingest.py::test_non_utf8_is_encoding_error | covered |
| 6 | unit | Header record fails strict CSV parse | MALFORMED_FILE naming the header line | unit/test_ingest.py::test_unparseable_header_is_malformed_file | covered |
| 7 | unit | Unterminated quote | MALFORMED_FILE with the start line of the open record; nothing landed | unit/test_ingest.py::test_unterminated_quote_mid_file_is_malformed_file_with_start_line, unit/test_ingest.py::test_ends_in_open_quote[*] | covered |
| 8 | unit | Closed / escaped / mid-field quotes | not MALFORMED_FILE | unit/test_ingest.py::test_closed_and_escaped_quotes_are_not_malformed_file | covered |
| 9 | unit | Duplicate header (after trim + lower-case) | DUPLICATE_HEADER, column named | unit/test_ingest.py::test_duplicate_header_after_normalization | covered |
| 10 | unit | Missing required column | SCHEMA_MISSING_COLUMN, column named | unit/test_ingest.py::test_missing_required_column | covered |
| 11 | unit | Batch rejects on all three sides | every reject reported, SOURCE -> TARGET -> REFERENCE | unit/test_ingest.py::test_every_side_is_checked_and_rejects_are_ordered | covered |
| 12 | integration | Batch reject through the pipeline | REJECTED, exit 2, failure_reason = first code, failure_detail = all; only the run row | integration/test_pipeline.py::test_batch_reject_leaves_no_rows, integration/test_pipeline.py::test_cli_rejected_exits_2 | covered |
| 13 | unit | Extra column | SCHEMA_EXTRA_COLUMN warning; column ignored | unit/test_ingest.py::test_extra_column_warns_and_is_ignored | covered |
| 14 | unit | Missing optional column (`discount`) | MISSING_OPTIONAL_COLUMN warning; discount NULL on every row | unit/test_ingest.py::test_missing_optional_column_warns_and_maps_nothing, unit/test_validate.py::test_missing_optional_column_gives_null_discount | covered |
| 15 | unit | Header only | EMPTY_BATCH warning; header landed, zero data records | unit/test_ingest.py::test_header_only_lands_header_and_warns_empty_batch | covered |
| 16 | unit | Several warnings | sorted by code, then side | unit/test_ingest.py::test_warnings_sorted_by_code_then_side | covered |
| 17 | unit | Warning checks | SCHEMA_DRIFT / MISSING_OPTIONAL_COLUMN / EMPTY_BATCH rows are WARN with `[{side, columns}]` | unit/test_compare.py::test_warning_checks | covered |
| 18 | integration | Warnings reach report.json | `warnings` array sorted by code then side; WARN checks; run SUCCEEDED, recon PASS, exit 0 | integration/test_report.py::test_batch_warnings_reach_report_json_and_never_change_status | added |
| 19 | unit | Reordered columns | mapped by name | unit/test_ingest.py::test_reordered_columns_map_by_name | covered |
| 20 | unit | Header case and spaces | trimmed + lower-cased before mapping | unit/test_ingest.py::test_header_case_and_spaces_are_normalized | covered |
| 21 | unit | UTF-8 BOM | stripped from header; still part of the file hash | unit/test_ingest.py::test_bom_is_stripped_from_header_but_hashed | covered |
| 22 | unit | CRLF line endings | no `\r` in raw_text | unit/test_ingest.py::test_crlf_line_endings_are_not_in_raw_text | covered |
| 23 | unit | CRLF + interior and trailing blank lines | blanks not landed, counted as blank_lines; physical line numbers kept | unit/test_ingest.py::test_crlf_with_interior_and_trailing_blank_lines | added |
| 24 | unit | Blank lines (LF), whitespace-only lines | not landed, counted; line_no stays physical | unit/test_ingest.py::test_blank_lines_are_counted_not_landed_and_keep_physical_line_numbers | covered |
| 25 | unit | Blank lines before the header | header keeps its physical line_no | unit/test_ingest.py::test_leading_blank_lines_before_header | covered |
| 26 | unit | Last line without newline | landed normally | unit/test_ingest.py::test_last_line_without_newline | covered |
| 27 | unit | Quoted comma | one record | unit/test_ingest.py::test_quoted_comma_stays_one_record | covered |
| 28 | unit | Quoted newline (LF and CRLF) | one record, line_no = start line, raw_text keeps the inner break; validates | unit/test_ingest.py::test_quoted_newline_keeps_all_lines_and_first_line_no[*], unit/test_validate.py::test_multiline_quoted_record_is_valid | covered |
| 29 | unit | Malformed data rows | landed verbatim, judged later | unit/test_ingest.py::test_malformed_rows_are_landed_verbatim | covered |
| 30 | unit | Input changed between check and landing (appended row, now invalid UTF-8, deleted) | InputChangedError; landing rolled back; detail `{side, file, expected_sha256, actual_sha256}` (`MISSING` if deleted) | unit/test_ingest.py::test_input_changed_between_passes_fails_the_run[*] | covered |
| 31 | integration | INPUT_CHANGED through the pipeline | FAILED, exit 2, failure_reason INPUT_CHANGED; only the run row; rerun with the new bytes = new run_key, attempt 1 | integration/test_pipeline.py::test_input_changed_mid_run_fails_then_the_new_bytes_are_a_new_run_key | added |
| 32 | unit | Both sides empty (header only) | zero keys; ALL aggregate rows written with 0; recon PASS | unit/test_compare.py::test_empty_sides_still_write_the_all_rows | covered |
| 33 | unit | One side empty | keys MISSING_IN_TARGET; EMPTY_BATCH WARN; recon FAIL | unit/test_compare.py::test_warning_checks | covered |

## Row level (DESIGN 4.3, 7.1)

| # | level | case | expected behaviour | test file::test name | status |
|---|---|---|---|---|---|
| 34 | unit | MALFORMED_ROW (too many / too few fields, quoting error) | ROW_REJECT; order_id and updated_at NULL | unit/test_validate.py::test_every_row_reject_code[*], unit/test_validate.py::test_malformed_row_details | covered |
| 35 | unit | MISSING_REQUIRED (incl. blank order_id) | ROW_REJECT, field named | unit/test_validate.py::test_every_row_reject_code[*], unit/test_normalize.py::test_str_required_empty_is_missing_required[*] | covered |
| 36 | unit | BAD_TYPE (`abc`, `NaN`, `yesterday`, impossible dates) | ROW_REJECT | unit/test_validate.py::test_every_row_reject_code[*], unit/test_normalize.py::test_money_errors[*], unit/test_normalize.py::test_timestamp_errors[*] | covered |
| 37 | unit | BAD_FORMAT (EU decimals, thousands, scale > 2, exponent, leading `+`/`.`, currency symbol, non-ISO / date-only timestamp, currency shape) | ROW_REJECT | unit/test_validate.py::test_every_row_reject_code[*], unit/test_normalize.py::test_money_errors[*], unit/test_normalize.py::test_timestamp_errors[*], unit/test_normalize.py::test_currency_bad_format[*] | covered |
| 38 | unit | BAD_ENUM | ROW_REJECT | unit/test_validate.py::test_every_row_reject_code[*], unit/test_normalize.py::test_enum_bad[*] | covered |
| 39 | unit | OUT_OF_RANGE (amount or discount < 0; above max: see P1) | ROW_REJECT; `-0.00` is 0 and valid | unit/test_validate.py::test_every_row_reject_code[*], unit/test_normalize.py::test_money_errors[*], unit/test_normalize.py::test_money_ok[*] | covered |
| 40 | unit | INVALID_REFERENCE | only on rows that passed everything else | unit/test_validate.py::test_every_row_reject_code[*], unit/test_validate.py::test_first_failure_is_reason_and_detail_lists_all, unit/test_normalize.py::test_row_invalid_reference_only_when_otherwise_valid | covered |
| 41 | unit | Several failures in one row | reason_code = first in field order; detail lists all | unit/test_validate.py::test_first_failure_is_reason_and_detail_lists_all, unit/test_normalize.py::test_row_collects_all_failures_in_field_order | covered |
| 42 | unit | Rejected row with parsed key, unparsed version | order_id kept, updated_at NULL | unit/test_validate.py::test_reject_keeps_key_but_not_unparsed_version | covered |
| 43 | unit | Literal `NULL` / `null` / `None` | a value, not NULL: BAD_TYPE / BAD_ENUM, kept as text in a str field | unit/test_normalize.py::test_literal_null_strings_are_values_not_null[*], unit/test_normalize.py::test_str_ok[*] | added |
| 44 | unit | Timestamp forms | `T` or space, naive = UTC, offsets converted, fraction truncated | unit/test_normalize.py::test_timestamp_ok[*] | covered |
| 45 | unit | Money parsing | Decimal -> integer minor units (type int) | unit/test_normalize.py::test_money_ok[*] | covered |
| 46 | unit | Leading zeros in IDs (normalization) | `0012` stays `0012` | unit/test_normalize.py::test_str_ok[*] | covered |
| 47 | unit | Leading zeros in IDs (as keys) | `0012` and `12` are different keys (MISSING_IN_TARGET + EXTRA_IN_TARGET); ` 007 ` matches `007` | unit/test_compare.py::test_leading_zeros_make_different_keys | added |
| 48 | unit | Reference: blank id, duplicate id, malformed row, case/space | MISSING_REQUIRED / MALFORMED_ROW quarantined (side REFERENCE); duplicate kept once, not quarantined | unit/test_validate.py::test_reference_rejects_duplicates_and_normalization | covered |
| 49 | unit | Empty reference (header only) | every order INVALID_REFERENCE | unit/test_validate.py::test_empty_reference_rejects_every_order | covered |
| 50 | unit | Traceability of rejected rows | run_id, side, source_file, line_no, raw_text, reason_code, detail | unit/test_validate.py::test_every_row_reject_code[*], integration/test_report.py::test_quarantine_csv_count_equals_quarantine_table | covered |

## Duplicates and versions (DESIGN 4.4)

| # | level | case | expected behaviour | test file::test name | status |
|---|---|---|---|---|---|
| 51 | unit | Exact duplicates (also after normalization) | lowest line kept; others DUPLICATE_EXACT | unit/test_dedupe.py::test_exact_duplicate_keeps_first_line | covered |
| 52 | unit | Older version later in the file | newest updated_at wins; older SUPERSEDED | unit/test_dedupe.py::test_newer_version_first_in_file_wins, unit/test_dedupe.py::test_three_versions_plus_duplicate_of_winner | covered |
| 53 | unit | Different payloads at max updated_at | all top rows CONFLICTING_DUPLICATE; older SUPERSEDED; no clean row | unit/test_dedupe.py::test_conflicting_duplicate, unit/test_dedupe.py::test_conflict_with_an_identical_pair_still_quarantines_all_top_rows | covered |
| 54 | unit | Rejected newest version (newer, or tie) | no fallback; valid rows SUPERSEDED `shadowed by rejected line N` | unit/test_dedupe.py::test_newest_reject_shadows_valid_rows[*] | covered |
| 55 | unit | Older reject, newer valid row | valid row wins | unit/test_dedupe.py::test_older_reject_and_valid_row_valid_wins | covered |
| 56 | unit, integration | Reject with unparsed updated_at (final review) | shadows the key: no clean row, valid rows SUPERSEDED "shadowed by rejected line N (unparseable updated_at)", key SOURCE_REJECTED, recon FAIL | unit/test_dedupe.py::test_reject_with_unparsed_version_shadows_the_key, integration/test_pipeline.py::test_newer_row_with_unparseable_updated_at_shadows_the_key_instead_of_a_false_pass | added (step 12) |
| 57 | unit | Duplicates on the TARGET side | same rules, side TARGET | unit/test_dedupe.py::test_duplicates_on_target_side | covered |
| 58 | unit | Arrival order | every permutation gives the same outcome | unit/test_dedupe.py::test_arrival_order_never_changes_the_outcome | covered |
| 59 | unit | Row accounting per side | landed = clean + quarantine; counts balance | unit/test_dedupe.py::test_row_accounting_every_side | covered |
| 60 | unit | Composite key | joined with `\|`, escaping, dedupe per joined key, config errors | unit/test_dedupe.py::test_composite_key_dedupe, unit/test_dedupe.py::test_composite_key_escaping, unit/test_dedupe.py::test_composite_key_reject_keeps_joined_key, unit/test_dedupe.py::test_composite_key_config_errors[*] | covered |

## Keys, diffs, aggregates (DESIGN 4.5, 4.6)

| # | level | case | expected behaviour | test file::test name | status |
|---|---|---|---|---|---|
| 61 | unit | Every key status (MATCHED, MISMATCHED, MISSING_IN_TARGET, EXTRA_IN_TARGET, SOURCE_REJECTED, TARGET_REJECTED, CONFLICT on either side, shadowed) | one row per key, sorted; first matching status wins | unit/test_compare.py::test_every_key_status_exactly_one_row_per_key | covered |
| 62 | unit | Rejected on both sides | SOURCE_REJECTED, note `rejected on both sides` | unit/test_compare.py::test_every_key_status_exactly_one_row_per_key | covered |
| 63 | unit | Reject only on TARGET, nothing on SOURCE | TARGET_REJECTED | unit/test_compare.py::test_every_key_status_exactly_one_row_per_key | covered |
| 64 | unit | Deciding line per side | clean row, else lowest conflicting line, else newest reject | unit/test_compare.py::test_key_result_points_at_deciding_lines | covered |
| 65 | unit | Key sort order | byte order (`O1 < O10 < O3`) | unit/test_dedupe.py::test_winners_sorted_by_key_and_clean_row_traceable | covered |
| 66 | unit | Tolerance boundary 0.01 vs 0.02 (amount, discount, both directions) | ≤ 1 minor unit equal; 2 differs with delta_minor | unit/test_compare.py::test_tolerance_boundary[*] | covered |
| 67 | unit | NULL vs NULL / NULL vs 0 / NULL vs value | match / NULL_MISMATCH / NULL_MISMATCH (NULL ≠ 0) | unit/test_compare.py::test_null_vs_null_matches, unit/test_compare.py::test_null_vs_value_is_null_mismatch[*] | covered |
| 68 | unit | TARGET_STALE / TARGET_AHEAD / VALUE_MISMATCH | by updated_at comparison | unit/test_compare.py::test_stale_ahead_and_value_mismatch[*] | covered |
| 69 | unit | NULL_MISMATCH precedence | wins over STALE | unit/test_compare.py::test_null_mismatch_takes_precedence_over_stale | covered |
| 70 | unit | Several diffs on one key | one field_diff per field, sorted | unit/test_compare.py::test_several_diffs_on_one_key | covered |
| 71 | unit | updated_at differs only | not compared, MATCHED | unit/test_compare.py::test_updated_at_is_not_compared | covered |
| 72 | unit | Group present on one side only; NULL discounts | 0 on the other side; NULL ignored in SUM | unit/test_compare.py::test_group_on_one_side_only_gets_zero_on_the_other | covered |
| 73 | unit | Status change on one key; in-tolerance difference | value moves between groups; explained bottom-up | unit/test_compare.py::test_status_change_moves_value_between_groups | covered |
| 74 | unit / integration | Explained delta | unexplained 0; delta = source_only − target_only + both_sides; per-status split adds up | unit/test_compare.py::test_aggregate_explained_breakdown_on_sample, integration/test_report.py::test_aggregate_explanation_adds_up | covered |

## Checks and recon status (DESIGN 4.7, 4.8)

| # | level | case | expected behaviour | test file::test name | status |
|---|---|---|---|---|---|
| 75 | unit | All nine checks | written on every SUCCEEDED run, stable shape | unit/test_compare.py::test_all_nine_checks_written_in_a_stable_shape | covered |
| 76 | unit | SRC_ROW_ACCOUNTING / REFERENTIAL_INTEGRITY broken in the DB | InvariantViolation; no GOLD rows (rolled back) | unit/test_compare.py::test_row_accounting_violation_raises_and_rolls_back, unit/test_compare.py::test_referential_integrity_violation_raises | covered |
| 77 | integration | Each INVARIANT check failing (SRC/TGT_ROW_ACCOUNTING, KEY_UNIQUENESS, REFERENTIAL_INTEGRITY, KEY_COVERAGE, AGGREGATE_EXPLAINED) | FAILED, exit 2, `INVARIANT_VIOLATION:<check>`, counts as JSON; only the run row; retry = next attempt SUCCEEDED | integration/test_pipeline.py::test_invariant_violation_fails_the_run_then_retry_succeeds[*] | added |
| 78 | unit | Unattributable reject (MALFORMED_ROW), every key MATCHED | recon FAIL | unit/test_compare.py::test_unattributable_reject_fails_recon_even_when_every_key_matches | covered |
| 79 | unit | Reference-side rejects only | recon PASS | unit/test_compare.py::test_reference_rejects_alone_do_not_fail_recon | added |
| 80 | unit | SUPERSEDED, DUPLICATE_EXACT and an older losing reject only | recon PASS | unit/test_compare.py::test_duplicate_dispositions_and_older_rejects_alone_do_not_fail_recon | added |
| 81 | integration | Everything matches | recon PASS, exit 0, empty mismatches/quarantine CSVs | integration/test_report.py::test_summary_md_pass | covered |

## Run lifecycle, idempotency, retry (DESIGN 5, 6)

| # | level | case | expected behaviour | test file::test name | status |
|---|---|---|---|---|---|
| 82 | integration | Happy path | SUCCEEDED, run_id = run_key[:16]-a1, exit 1 on the sample | integration/test_pipeline.py::test_happy_path_matches_expected_sample | covered |
| 83 | integration | Rerun same inputs + config | SKIPPED row (skipped_of_run_id), no other writes, same exit code, report not rewritten | integration/test_pipeline.py::test_rerun_is_skipped_and_writes_nothing_else, integration/test_pipeline.py::test_cli_run_then_skip, integration/test_report.py::test_skipped_prints_existing_report_path_without_rewriting | covered |
| 84 | integration | SKIPPED but the report folder was deleted | warning with `recon report` hint, nothing written | integration/test_report.py::test_skipped_with_missing_report_warns_and_writes_nothing | covered |
| 85 | integration | SKIPPED after a later FAILED `--force` attempt | reuses the latest SUCCEEDED attempt | integration/test_pipeline.py::test_skipped_reuses_the_latest_succeeded_attempt | added |
| 86 | integration | `--force` | attempt 2, forced = 1, identical GOLD; report differs only in run identity | integration/test_pipeline.py::test_force_runs_attempt_2_with_identical_gold, integration/test_report.py::test_forced_attempt_differs_only_in_run_identity | covered |
| 87 | integration | Crash after bronze / silver / gold, then retry | FAILED UNEXPECTED_ERROR, nothing but the run row; retry = attempt 2 SUCCEEDED | integration/test_pipeline.py::test_injected_failure_rolls_back_everything_then_retry_succeeds[*] | covered |
| 88 | integration | Crash while publishing the report | previous report kept / no partial set; `recon report` recovers | integration/test_report.py::test_crash_while_regenerating_keeps_previous_report, integration/test_report.py::test_crash_on_first_write_leaves_no_partial_report, integration/test_report.py::test_recover_run_dir_finishes_an_interrupted_swap | covered |
| 89 | integration | Stale RUNNING | ABANDONED (STALE_RUNNING), new attempt runs | integration/test_pipeline.py::test_stale_running_becomes_abandoned | covered |
| 90 | integration | Fresh RUNNING | "run in progress", exit 2, nothing written | integration/test_pipeline.py::test_fresh_running_is_run_in_progress_and_writes_nothing, integration/test_pipeline.py::test_cli_run_in_progress_exits_2 | covered |
| 91 | integration | Stale + fresh RUNNING | stale ABANDONED (committed), then run in progress | integration/test_pipeline.py::test_stale_and_fresh_running_abandons_stale_then_reports_in_progress | covered |
| 92 | integration / unit | Config change | new run_key, attempt 1; hash stable across key order, includes defaults | integration/test_pipeline.py::test_config_change_gives_new_run_key, unit/test_config.py::test_sha256_stable_across_key_order, unit/test_config.py::test_sha256_includes_defaults, unit/test_config.py::test_sha256_changes_with_content | covered |
| 93 | integration | Different `--stale-after-minutes` | not part of run_key: SKIPPED | integration/test_pipeline.py::test_stale_after_minutes_is_not_part_of_the_run_key | added |
| 94 | integration | Retry after REJECTED | next attempt, no flag | integration/test_backfill.py::test_rerun_skips_succeeded_pairs | covered |
| 95 | integration | Bad config / unknown RECON_FAIL_AT | exit 2 before any run row; no DB | integration/test_pipeline.py::test_cli_bad_config_exits_2_without_run_row, integration/test_pipeline.py::test_unknown_fail_point_is_an_error_before_any_row | covered |
| 96 | integration | `recon report` on FAILED / unknown run / missing DB | exit 2 | integration/test_report.py::test_failed_and_rejected_runs_write_no_report, integration/test_report.py::test_report_command_errors_exit_2 | covered |
| 97 | integration | `recon report` on a SKIPPED run_id | exit 2, nothing written | integration/test_report.py::test_report_command_refuses_a_skipped_run_id | added |
| 98 | integration | `recon report` with tampered config_json | exit 2 (hash mismatch) | integration/test_report.py::test_report_refuses_config_that_does_not_match_hash | covered |

## Backfill (DESIGN 6.6)

| # | level | case | expected behaviour | test file::test name | status |
|---|---|---|---|---|---|
| 99 | integration | Order of runs and summary | manifest order; summary sorted by batch_id | integration/test_backfill.py::test_lines_run_in_file_order_and_summary_is_sorted_by_batch_id, integration/test_backfill.py::test_cli_prints_sorted_summary_and_writes_metrics | covered |
| 100 | integration | A failing line | others continue; exit = max | integration/test_backfill.py::test_failing_line_does_not_stop_the_others_and_exit_is_max, integration/test_backfill.py::test_exit_code_is_max_over_lines[*] | covered |
| 101 | integration | Unexpected exception in one line | status ERROR, exit 2, others continue | integration/test_backfill.py::test_unexpected_exception_in_one_line_is_reported_and_others_continue | covered |
| 102 | integration | Line blocked by a fresh RUNNING attempt | IN_PROGRESS, run_id = the RUNNING attempt, exit 2, no new row; others continue | integration/test_backfill.py::test_line_blocked_by_a_fresh_running_attempt_is_in_progress | added |
| 103 | integration | Re-run the same manifest; same pair twice | SUCCEEDED pairs SKIPPED; REJECTED retried | integration/test_backfill.py::test_rerun_skips_succeeded_pairs, integration/test_backfill.py::test_same_pair_twice_in_one_manifest_is_skipped_the_second_time | covered |
| 104 | integration | Relative paths | resolved against the manifest directory | integration/test_backfill.py::test_relative_paths_resolve_against_the_manifest_directory | covered |
| 105 | integration | Malformed / missing manifest, bad config | exit 2 before any run, no DB | integration/test_backfill.py::test_malformed_manifest_exits_2_before_any_run[*], integration/test_backfill.py::test_missing_manifest_exits_2, integration/test_backfill.py::test_bad_config_exits_2_before_any_run | covered |
| 106 | integration | Manifest header case / spaces / order | normalized | integration/test_backfill.py::test_manifest_header_is_trimmed_lower_cased_and_order_free | covered |
| 107 | integration | Reports per SUCCEEDED line | report files written | integration/test_report.py::test_backfill_writes_a_report_per_succeeded_line | covered |

## Determinism, symmetry, end to end (DESIGN 7.1 #12)

| # | level | case | expected behaviour | test file::test name | status |
|---|---|---|---|---|---|
| 108 | unit | Swap source and target (files + column maps) | GOLD mirrored: MISSING ↔ EXTRA, SOURCE_ ↔ TARGET_REJECTED, STALE ↔ AHEAD, deltas negated; same recon status | unit/test_compare.py::test_symmetry_on_every_status, unit/test_compare.py::test_symmetry_on_sample | covered (confirmed) |
| 109 | unit / integration | Same inputs, fresh DBs | identical GOLD; report files byte-identical apart from run timestamps; `recon report` rewrites byte-identical files | unit/test_compare.py::test_same_inputs_give_identical_gold, integration/test_report.py::test_fresh_dbs_give_identical_reports_apart_from_run_timestamps, integration/test_report.py::test_regenerating_twice_gives_byte_identical_files | covered |
| 110 | e2e | Data rows of both inputs shuffled (header kept), sample and generated N=2000 | report.json identical apart from run timestamps, input hashes (and run_key / run_id derived from them) and physical line numbers; field_diffs.csv and mismatches.csv identical apart from run_id / line numbers | e2e/test_end_to_end.py::test_shuffled_inputs_give_an_identical_report[*] | added |
| 111 | e2e | `generate --n 10000 --seed 7` then `recon run` | every result value in expected/<name>.json equals the report (key statuses, field diffs, per-side counts and reject codes, sums, totals, recon status); all checks PASS | e2e/test_end_to_end.py::test_generated_10k_report_matches_expected_answers | added |
| 112 | integration / unit | Committed sample vs its expected answers | DB and GOLD match data/expected/sample.json | integration/test_pipeline.py::test_happy_path_matches_expected_sample, unit/test_compare.py::test_sample_matches_generator_answers | covered |
| 113 | unit | Generator determinism | same seed = byte-identical files | unit/test_generator.py::test_same_seed_gives_byte_identical_files | covered |

## QA-pass edge cases P1–P16

Found by the skeptical-QA pass in step 9 and probed against the code; decisions signed off 2026-11-15 (P1, P2,
P4, P8 changed or documented behaviour; the rest pin existing behaviour that matches DESIGN). No probe
contradicted DESIGN. Every case has a test except P16b, which is documented only.

| # | level | case | expected behaviour | test file::test name | status |
|---|---|---|---|---|---|
| P1 | unit | Money `max` (orders: amount, discount `9999999999.99`) | max exactly accepted; max + 0.01 and `99999999999999999.99` are OUT_OF_RANGE | unit/test_normalize.py::test_money_max_is_inclusive[*], unit/test_normalize.py::test_money_above_max_is_out_of_range[*] | added (step 9, QA pass) |
| P1 | unit | No configured bound | ±92233720368.54 (MONEY_LIMIT_MINOR) accepted; one minor unit beyond is OUT_OF_RANGE | unit/test_normalize.py::test_money_without_configured_bounds_uses_the_supported_limit | added (step 9, QA pass) |
| P1 | unit | `max` in the config | decimal string; ≥ min; inside ±MONEY_LIMIT_MINOR (int64 with 10^6 rows of headroom); default null and hashed | unit/test_config.py::test_money_max_is_parsed_and_defaults_to_none, unit/test_config.py::test_money_max_config_errors[*], unit/test_config.py::test_money_max_at_the_limit_and_min_below_it | added (step 9, QA pass) |
| P1 | integration | Huge amount in a run | row reject (OUT_OF_RANGE, key SOURCE_REJECTED), run SUCCEEDED, never a run failure | integration/test_pipeline.py::test_amount_above_max_is_a_row_reject_and_the_run_succeeds | added (step 9, QA pass) |
| P2 | integration | SUM overflows int64 (cap lifted by monkeypatch; overflow in aggregates or in the AGGREGATE_EXPLAINED breakdown) | FAILED, exit 2, `AGGREGATE_OVERFLOW`, detail `{error, step}`, only the run row | integration/test_pipeline.py::test_aggregate_overflow_fails_the_run[*] | added (step 9, QA pass) |
| P3 | unit | Keys differing only by case / outer whitespace, within one side | one key: same payload DUPLICATE_EXACT, different payload CONFLICT | unit/test_dedupe.py::test_keys_differing_only_by_case_or_outer_whitespace_are_one_key | added (step 9, QA pass) |
| P3 | unit | … across sides | MATCHED, recon PASS | unit/test_compare.py::test_keys_differing_only_by_case_or_outer_whitespace_match_across_sides | added (step 9, QA pass) |
| P4 | unit | Non-ASCII letters under upper-casing | only a–z upper-cased: `straße` -> `STRAßE`, `café-1` -> `CAFé-1` (order_id and customer_id) | unit/test_normalize.py::test_upper_case_touches_ascii_letters_only[*] | added (step 9, QA pass) |
| P4 | unit | Non-ASCII letters in an enum (`ſhipped`: `str.upper()` would give SHIPPED) | BAD_ENUM: enums are upper-cased ASCII-only too | unit/test_normalize.py::test_enum_upper_case_is_ascii_only[*] | added (step 9, QA pass) |
| P4 | unit | `straße` vs `STRASSE` | two different keys (MISSING_IN_TARGET + EXTRA_IN_TARGET) | unit/test_compare.py::test_non_ascii_keys_are_not_case_folded | added (step 9, QA pass) |
| P5 | unit | Currency change on the same key (USD -> EUR) | currency VALUE_MISMATCH, no amount diff; value moves between CURRENCY groups, explained; ALL delta 0 | unit/test_compare.py::test_currency_change_on_the_same_key | added (step 9, QA pass) |
| P6 | integration | Reference header only | SUCCEEDED, recon FAIL; every order INVALID_REFERENCE on both sides -> SOURCE_REJECTED `rejected on both sides`; EMPTY_BATCH WARN on REFERENCE | integration/test_pipeline.py::test_header_only_reference_rejects_every_order | added (step 9, QA pass) |
| P6 | integration | Reference zero bytes | REJECTED EMPTY_FILE on REFERENCE, exit 2 | integration/test_pipeline.py::test_zero_byte_reference_is_rejected | added (step 9, QA pass) |
| P7 | integration | Same file as source and target | shipped config: REJECTED SCHEMA_MISSING_COLUMN on TARGET; identity mapping: all MATCHED, PASS, equal hashes | integration/test_pipeline.py::test_same_file_as_source_and_target | added (step 9, QA pass) |
| P8 | unit | Two versions within the same second (`.100`, `.900`), different payloads | tie after truncation: both CONFLICTING_DUPLICATE, key CONFLICT (DESIGN 7.2 #3) | unit/test_dedupe.py::test_versions_within_the_same_second_tie_and_conflict | added (step 9, QA pass) |
| P9 | unit | Same instant with different offsets | one version: lowest line kept, others DUPLICATE_EXACT | unit/test_dedupe.py::test_same_instant_with_different_offsets_is_the_same_version | added (step 9, QA pass) |
| P10 | unit | UTC conversion outside years 1–9999 | BAD_TYPE | unit/test_normalize.py::test_utc_conversion_outside_year_range_is_bad_type[*] | added (step 9, QA pass) |
| P11 | unit | Data field longer than the CSV field size limit | MALFORMED_ROW, detail `a field exceeds the CSV field size limit (131072 characters)` | unit/test_validate.py::test_field_over_the_csv_size_limit_is_malformed_row_with_a_clear_detail | added (step 9, QA pass) |
| P11 | unit | Header field longer than the limit | MALFORMED_FILE naming the field size limit | unit/test_ingest.py::test_header_field_over_the_csv_size_limit_is_malformed_file | added (step 9, QA pass) |
| P12 | unit | NUL byte in a value | kept in text (`O\x001` is its own key); a timestamp with NUL is BAD_FORMAT | unit/test_normalize.py::test_nul_byte_is_kept_in_text_and_breaks_a_timestamp, unit/test_validate.py::test_nul_byte_in_the_key_is_kept, unit/test_compare.py::test_nul_byte_in_a_key_is_a_different_key | added (step 9, QA pass) |
| P13 | unit | Mixed line endings (CRLF, LF, lone CR) in one file | every terminator ends a physical line; raw_text has none | unit/test_ingest.py::test_mixed_line_endings_in_one_file | added (step 9, QA pass) |
| P14 | integration | `--stale-after-minutes 0` (clock frozen) | RUNNING started this second: run in progress; one second older: ABANDONED, new attempt | integration/test_pipeline.py::test_stale_after_zero_minutes_boundary[*] | added (step 9, QA pass) |
| P15 | e2e | Composite key containing `\|` and `\` | escaped key in mismatches.csv / field_diffs.csv; Markdown-escaped in summary.md; business_key in report.json | e2e/test_end_to_end.py::test_composite_key_with_separator_characters_round_trips_through_every_report_file | added (step 9, QA pass) |
| P16 | integration | DB write-locked by another process | `database is locked`, exit 2, no run row; runs normally once released | integration/test_pipeline.py::test_locked_database_exits_2_and_writes_no_run_row | added (step 9, QA pass) |
| P16b | integration | DB lock lost during the data stage (after the RUNNING row is committed) | the data transaction fails and the FAILED update cannot be written either, so the run row stays RUNNING; a later invocation marks it ABANDONED (STALE_RUNNING) once it is older than `--stale-after-minutes` (DESIGN 7.2 #4) | — | documented, not tested |
