# Report schema (report_version 1)

Every SUCCEEDED run writes these files to `out/<run_id>/`. They are built from the DB only, by
`recon run` (after the run), `recon backfill` (per SUCCEEDED line) or `recon report --run-id ID`.

| file | content | sorted by |
|---|---|---|
| `report.json` | the machine-readable report described below | keys sorted; lists as stated per section |
| `mismatches.csv` | one row per non-MATCHED key, source and target values side by side | record_key |
| `field_diffs.csv` | one row per differing compared field | record_key, column_name |
| `quarantine.csv` | every quarantined record (row rejects and duplicate dispositions) | side (SOURCE, TARGET, REFERENCE), line_no |
| `summary.md` | human-readable one-pager, `# Reconciliation PASS` or `FAIL` on line 1 | as report.json |
| `metrics.json`, `run.log` | timings, memory, logs. Written by the run, never by the report; not deterministic | — |

Every CSV starts with a `run_id` column, so each exported row is traceable on its own, even when files
from several runs are concatenated.

Format rules (DESIGN 7.1 #12): UTF-8, LF line endings. JSON has sorted keys, 2-space indent,
`ensure_ascii=False` and a final newline. CSV has a header row, minimal quoting, and NULL written as an
empty field. Apart from `run.started_at` and `run.finished_at` (also shown in `summary.md`), the report
files are byte-identical for the same inputs and config (a `--force` attempt also changes `run_id`,
`attempt` and `forced`). The five report files are published together
(staging directory, then rename), so you never see a partial set.

Money is never a float. In `report.json` money aggregates are **integer minor units**, with `unit` and
`scale` stated next to them. In the CSVs, money is fixed-point text at the field's scale (`123.45`).
Timestamps are `YYYY-MM-DDTHH:MM:SSZ`.

## report.json

```jsonc
{
  "report_version": 1,

  "run": {
    "run_id": "1796ebea707e2f43-a1",   // run_key[:16] + "-a" + attempt
    "run_key": "<sha256>",
    "attempt": 1,
    "forced": false,                   // --force
    "status": "SUCCEEDED",             // always SUCCEEDED: only those runs have a report
    "recon_status": "FAIL",            // PASS | FAIL (DESIGN 4.8)
    "exit_code": 1,                    // 0 PASS, 1 FAIL
    "started_at": "2026-11-15T11:09:15Z",   // run timestamp: not deterministic
    "finished_at": "2026-11-15T11:09:15Z",  // run timestamp: not deterministic
    "config_sha256": "<sha256>",
    "dataset": "orders",
    "business_key": ["order_id"]       // one field, or the components of a composite key
  },

  "inputs": {                          // one entry per side: SOURCE, TARGET, REFERENCE
    "SOURCE": {"file": "data/sample/source.csv",  // path as given on the CLI
               "sha256": "<sha256 of raw bytes>",
               "data_records": 63}     // landed DATA records (blank lines are not records)
  },

  "sides": {
    "SOURCE": {                        // TARGET has the same shape
      "rows_read": 63,                 // landed DATA records
      "accepted": 55,                  // clean_record rows (one per key)
      "rejected": 4,                   // ROW_REJECT rows
      "rejected_by_reason": {          // all 7 row-reject codes, zeros included (DESIGN 4.3)
        "MALFORMED_ROW": 1, "MISSING_REQUIRED": 1, "BAD_TYPE": 1, "BAD_FORMAT": 0,
        "BAD_ENUM": 0, "OUT_OF_RANGE": 0, "INVALID_REFERENCE": 1
      },
      "duplicate_exact": 1,            // DUPLICATE_EXACT rows
      "superseded": 1,                 // SUPERSEDED rows
      "conflicting_rows": 2            // CONFLICTING_DUPLICATE rows (the keys are CONFLICT)
    },
    "REFERENCE": {
      "rows_read": 5, "accepted": 5, "rejected": 0, "rejected_by_reason": { /* same 7 codes */ },
      "duplicates_ignored": 0          // repeated customer_ids, kept once and not quarantined
    }
  },
  // per side: rows_read = accepted + rejected + duplicate_exact + superseded + conflicting_rows
  // (REFERENCE: accepted + rejected + duplicates_ignored)

  "key_status_counts": {               // all 7 statuses, zeros included (DESIGN 4.5)
    "CONFLICT": 1, "MATCHED": 50, "MISMATCHED": 3, "SOURCE_REJECTED": 3,
    "TARGET_REJECTED": 1, "MISSING_IN_TARGET": 1, "EXTRA_IN_TARGET": 2
  },

  "field_diff_counts": {               // every compare field x all 4 diff types, zeros included (DESIGN 4.6)
    "amount": {"NULL_MISMATCH": 0, "TARGET_AHEAD": 0, "TARGET_STALE": 0, "VALUE_MISMATCH": 1}
  },

  "totals": {
    "keys": 61,                        // key_result rows
    "non_matched_keys": 11,            // = rows in mismatches.csv
    "field_diffs": 3,                  // = rows in field_diffs.csv
    "quarantine_rows": 10,             // = rows in quarantine.csv
    "unattributable_rejects": 1        // SOURCE/TARGET ROW_REJECTs without a key (fail recon on their own)
  },

  "aggregates": [                      // every aggregate_result row, sorted by metric, group_by, group_value
    {"metric": "SUM_AMOUNT", "group_by": "CURRENCY", "group_value": "EUR",  // ALL rows have group_value "*"
     "source_value": 664515, "target_value": 756532, "delta": -92017,    // delta = source - target
     "explained_delta": -92017, "unexplained_delta": 0,                  // DESIGN 4.6; always 0 unexplained
     "unit": "MINOR_UNITS", "scale": 2}                                   // ROW_COUNT: unit "ROWS", scale 0
  ],

  "aggregate_explanation": [           // one entry per metric over group ALL, sorted by metric
    {"metric": "SUM_AMOUNT", "group_by": "ALL", "unit": "MINOR_UNITS", "scale": 2,
     "delta": -344328, "explained_delta": -344328,
     "source_only": 128020,            // keys with a clean SOURCE row only
     "target_only": 468477,            // keys with a clean TARGET row only
     "both_sides": -3871,              // sum(source - target) over keys clean on both sides
     // delta = source_only - target_only + both_sides
     "by_key_status": {                // all 7 statuses; the deltas add up to "delta"
       "MISSING_IN_TARGET": {"keys": 1, "source_value": 87516, "target_value": 0, "delta": 87516}
       // keys = keys of that status with a clean row on either side
     }}
  ],

  "checks": [                          // all 9 check_result rows, sorted by check_name (DESIGN 4.7)
    {"check_name": "KEY_COVERAGE", "severity": "INVARIANT", "status": "PASS",
     "expected": "61", "actual": "61",
     "detail": {"expected_keys": 61, "extra": 0, "key_results": 61, "missing": 0}}  // parsed JSON
  ],

  "warnings": [                        // batch warnings (DESIGN 4.2), sorted by code, then side
    {"code": "SCHEMA_EXTRA_COLUMN", "side": "TARGET", "columns": ["load_batch"]}
  ],

  "mismatch_samples": {
    "limit": 20,
    "keys": [                          // the first 20 rows of mismatches.csv, as objects
      {"record_key": "ORD-000022", "key_status": "SOURCE_REJECTED",
       "differing_columns": [],         // field_diff columns, sorted
       "note": null,                    // "rejected on both sides" or null
       "source_state": "MISSING_REQUIRED", "target_state": "CLEAN",
       "source_line_no": 24, "target_line_no": 53,
       "source": {"customer_id": null, "amount": null /* ... */},
       "target": {"customer_id": "C00005", "amount": "235.16" /* ... */}}
    ]
  }
}
```

A report never contains durations, peak memory or other metrics. Those are only in `metrics.json`
(DESIGN 7.1 #13).

## mismatches.csv

The isolated mismatched records: one row per key whose status is not MATCHED.

| column | meaning |
|---|---|
| `run_id` | the run this row belongs to |
| `record_key` | business key (a composite key is joined with `\|`, DESIGN 2.2) |
| `key_status` | CONFLICT, MISMATCHED, SOURCE_REJECTED, TARGET_REJECTED, MISSING_IN_TARGET or EXTRA_IN_TARGET |
| `differing_columns` | field_diff columns of the key, sorted, joined with `;` (empty unless MISMATCHED) |
| `note` | `rejected on both sides`, or empty |
| `source_state`, `target_state` | `CLEAN` (the side has a clean row), the `reason_code` of the deciding quarantined line (e.g. `BAD_TYPE`, `CONFLICTING_DUPLICATE`), or `ABSENT` |
| `source_line_no`, `target_line_no` | the clean line, or the deciding quarantined line (find it in `quarantine.csv`); empty when ABSENT |
| `source_<field>`, `target_<field>` | canonical values, in pairs, for every logical field except the business-key fields (in config order, including `updated_at`) |

Value columns are filled only for a side whose state is `CLEAN`. On a clean side an empty value is a
NULL (only `discount` can be NULL).

## field_diffs.csv

`run_id, record_key, column_name, source_value, target_value, diff_type`: one row per field_diff row. Values
are canonical text, money as fixed-point; an empty value is NULL (diff_type NULL_MISMATCH).

## quarantine.csv

`run_id, side, source_file, line_no, reason_code, detail, raw_text`: one row per quarantine row, both row
rejects (DESIGN 4.3) and duplicate dispositions (DESIGN 4.4). `raw_text` is the record exactly as it
was landed, so multi-line records are quoted.

## Compatibility

`report_version` changes whenever a field is removed, renamed or changes meaning. New fields can be
added without changing it.
