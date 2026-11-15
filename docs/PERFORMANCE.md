# Performance

All figures below were measured on one machine: Windows 11, 12th Gen Intel Core i5-1240P (12 cores / 16
threads, reported clock 1.7 GHz), Python 3.13.7, SQLite 3.50.4. Nothing is extrapolated.

## How it is measured

`recon bench --n N --seed S --repeat K --label L` generates a synthetic dataset into a temp dir (untimed),
then reconciles it K times, each time into a fresh SQLite DB, and adds one row to the table below.

- **Stage seconds** (`batch_checks`, `bronze`, `silver`, `gold`) come from the pipeline's own stage timer
  (`time.perf_counter`, the figures that go to `metrics.json`). `report s` = writing the five report files.
  `total s` = pipeline + report files, in-process wall time (no interpreter start-up, no data generation).
  All are **medians over the K runs**; `total min-max s` shows the spread.
- **CPU s** = CPU time of the process over the same span (`time.process_time`, median). It is much less
  sensitive to whatever else the laptop is doing than wall time, so before/after comparisons use both.
- **records/s** = data records of all three inputs / `total s`.
- **peak MiB** comes from one extra run with tracemalloc on (`--profile-memory`); tracing distorts timings,
  so it is never the timed run. tracemalloc sees Python allocations only, not SQLite's C heap.
- **DB MiB** = size of the SQLite file after one run.
- **result digest** = sha256 over the five report files with the run timestamps and the temp input directory
  masked. All K runs of a row must agree (bench fails otherwise); equal digests across rows mean
  byte-identical reports.

Commands (run from the repository root, with the dev venv active):

| what | command | output |
|---|---|---|
| a results row | `python -m recon bench --n 100000 --seed 1 --repeat 3 --label baseline` | appends one row to the table below |
| unprofiled split + cProfile of SILVER and GOLD | `python tools/profile_stages.py --n 100000 --seed 1 --top 15` | stdout: the split table and both top-15 lists |
| EXPLAIN QUERY PLAN of every GOLD query | `python tools/explain_gold.py --n 100000 --seed 1` | stdout: each distinct statement with its plan; last line `base-table full scans: N` |

Both tools generate their own dataset into a temp dir (pass `--work DIR` to keep it). They are dev tools: no
tests. The profiling sections below came from earlier scratch versions of these two scripts, folded into
`tools/` afterwards, so a rerun reproduces the method, not the exact seconds.

Why medians: the first single-run attempts on the same 100k input took 43.8 s and 26.7 s minutes apart,
and a 10k batch ranged 2.15-5.13 s. The laptop's clock and background load move wall time a lot; the
median of repeated runs plus CPU time is what the comparisons below rely on.

## Results

<!-- bench:results:start -->
| label | date (UTC) | env | n | seed | data records S / T / R | runs | batch_checks s | bronze s | silver s | gold s | report s | total s | total min-max s | CPU s | records/s | peak MiB (tracemalloc) | DB MiB | keys | field diffs | result digest |
|---|---|---|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|---|
| baseline | 2026-11-15 | win32 py3.13.7 | 10000 | 1 | 10070 / 9990 / 200 | 5 | 0.06 | 0.16 | 1.22 | 0.82 | 0.20 | 2.51 | 2.15-5.13 | 2.48 | 8,060 | 27.0 | 13.1 | 10030 | 100 | `162e4ab0cb3e` |
| baseline | 2026-11-15 | win32 py3.13.7 | 100000 | 1 | 100700 / 99900 / 2000 | 3 | 0.56 | 2.07 | 12.53 | 9.69 | 2.20 | 27.10 | 27.00-28.09 | 26.50 | 7,475 | 268.0 | 131.1 | 100300 | 1000 | `9f1b944ed32e` |
| after: one aggregate statement | 2026-11-15 | win32 py3.13.7 | 10000 | 1 | 10070 / 9990 / 200 | 5 | 0.05 | 0.19 | 1.19 | 0.53 | 0.24 | 2.27 | 2.15-2.30 | 2.19 | 8,909 | 27.0 | 13.1 | 10030 | 100 | `162e4ab0cb3e` |
| after: one aggregate statement | 2026-11-15 | win32 py3.13.7 | 100000 | 1 | 100700 / 99900 / 2000 | 3 | 0.51 | 1.91 | 12.42 | 6.38 | 2.20 | 23.50 | 23.02-37.99 | 22.48 | 8,620 | 268.0 | 131.1 | 100300 | 1000 | `9f1b944ed32e` |
<!-- bench:results:end -->

**Before -> after at 100k** (medians): gold 9.69 -> 6.38 s (-34%), total 27.10 -> 23.50 s (-13%), CPU
26.50 -> 22.48 s (-15%), 7,475 -> 8,620 records/s. At 10k: gold 0.82 -> 0.53 s, total 2.51 -> 2.27 s, CPU
2.48 -> 2.19 s. The result digests are identical before and after (`162e4ab0cb3e`, `9f1b944ed32e`), so
the report files are byte-identical; peak memory and DB size did not change. (One of the three "after"
100k runs took 37.99 s; the other two took 23.02 and 23.50 s. That spread is machine noise, which is why
the medians and CPU time are used.)

## Where the time goes (100k, baseline)

Stage shares of the baseline total (27.10 s): silver 46%, gold 36%, report 8%, bronze 8%, batch checks 2%.

### Unprofiled split

`perf_counter` wrappers around the main functions, no profiler, one 100k run before and one after the change
(single runs, so ±10% noise applies). Reproduce with `python tools/profile_stages.py --n 100000 --seed 1`
(first section of its output):

| part | before s | after s |
|---|---:|---:|
| **silver stage** | 12.98 | 12.83 |
| `validate_side` (parse + normalize + hash, both sides) | 8.98 | 8.88 |
| of which `payload_sha256` | 1.35 | 1.39 |
| of which `parse_record` (new strict csv reader per row) | 0.99 | 0.98 |
| `insert_clean_records` (executemany, 199,800 rows) | 3.25 | 3.17 |
| `insert_quarantine`, `validate_reference` | 0.05 | 0.05 |
| **gold stage** | 10.14 | 6.44 |
| `insert_aggregates` | **5.09** | **1.36** |
| `insert_key_results` | 2.17 | 2.12 |
| `insert_field_diffs` | 1.13 | 1.12 |
| `aggregate_breakdown` (AGGREGATE_EXPLAINED check, 3 calls) | 0.82 | 0.87 |
| `key_coverage` | 0.29 | 0.28 |
| `count_unknown_references` | 0.20 | 0.21 |
| `apply_diff_counts` | 0.16 | 0.20 |
| `count_duplicate_keys` | 0.16 | 0.17 |

### SILVER: cProfile, top 15 by cumulative time (100k, baseline)

Reproduce with `python tools/profile_stages.py --n 100000 --seed 1 --top 15` (the `silver` section).
cProfile adds heavy per-call overhead to Python code (the silver stage took 37.1 s under the profiler against
12.5 s without), so read the relative order, not the absolute seconds.

```
   ncalls  tottime  percall  cumtime  percall filename:lineno(function)
        1    0.048    0.048   37.138   37.138 recon\service\pipeline.py:175(build_silver)
        2    1.948    0.974   29.000   14.500 recon\service\validate.py:155(validate_side)
   200500    2.514    0.000   18.406    0.000 recon\service\normalize.py:91(validate_row)
  1606000    1.817    0.000   13.996    0.000 recon\service\normalize.py:73(normalize_value)
        6    4.377    0.730    6.009    1.001 {method 'executemany' of 'sqlite3.Connection' objects}
        2    0.000    0.000    5.911    2.956 recon\repository\silver_repo.py:33(insert_clean_records)
   400964    3.431    0.000    5.609    0.000 recon\service\normalize.py:149(parse_timestamp)
   199800    1.047    0.000    4.301    0.000 recon\service\validate.py:107(payload_sha256)
   396754    0.504    0.000    3.690    0.000 recon\service\normalize.py:199(_normalize_decimal)
   396754    1.375    0.000    3.186    0.000 recon\service\normalize.py:139(parse_money)
   199800    0.563    0.000    2.416    0.000 json\__init__.py:183(dumps)
   202603    1.538    0.000    2.145    0.000 recon\service\validate.py:114(parse_record)
        2    1.293    0.646    2.017    1.008 recon\service\dedupe.py:81(dedupe)
   603416    0.538    0.000    1.890    0.000 recon\service\normalize.py:179(_normalize_str)
   199800    0.477    0.000    1.722    0.000 json\encoder.py:183(encode)
```

Silver is per-row Python work spread over many small functions: 1.6 million `normalize_value` calls, with
timestamp parsing (400,964 calls, regex + `datetime` + formatting) the largest single leaf. The suspected
per-row csv reader (`parse_record`) is real but small: 0.99 s unprofiled, about 8% of silver.

### GOLD: cProfile, top 15 by cumulative time (100k)

Reproduce with the same command (the `gold` section). Gold is set-based SQL, so the profiler adds almost
nothing: its figures match the unprofiled split.

Before (31 `execute` calls, 10.15 s):

```
   ncalls  tottime  percall  cumtime  percall filename:lineno(function)
        1    0.000    0.000   10.149   10.149 recon\service\compare.py:150(reconcile)
       31   10.129    0.327   10.132    0.327 {method 'execute' of 'sqlite3.Connection' objects}
        1    0.000    0.000    4.884    4.884 recon\repository\gold_repo.py:279(insert_aggregates)
        1    0.000    0.000    2.223    2.223 recon\repository\gold_repo.py:150(insert_key_results)
        1    0.000    0.000    1.696    1.696 recon\service\compare.py:189(evaluate_checks)
        1    0.000    0.000    1.148    1.148 recon\repository\gold_repo.py:217(insert_field_diffs)
        1    0.000    0.000    0.830    0.830 recon\service\compare.py:230(_aggregate_explained)
        3    0.000    0.000    0.830    0.277 recon\repository\gold_repo.py:289(aggregate_breakdown)
        1    0.000    0.000    0.347    0.347 recon\service\compare.py:224(_key_coverage)
        1    0.000    0.000    0.347    0.347 recon\repository\gold_repo.py:351(key_coverage)
        1    0.000    0.000    0.250    0.250 recon\service\compare.py:217(_referential_integrity)
        1    0.000    0.000    0.250    0.250 recon\repository\gold_repo.py:325(count_unknown_references)
        1    0.000    0.000    0.181    0.181 recon\repository\gold_repo.py:167(apply_diff_counts)
        1    0.000    0.000    0.162    0.162 recon\service\compare.py:210(_key_uniqueness)
        1    0.000    0.000    0.162    0.162 recon\repository\gold_repo.py:314(count_duplicate_keys)
```

After (23 `execute` calls, 6.13 s):

```
   ncalls  tottime  percall  cumtime  percall filename:lineno(function)
        1    0.000    0.000    6.134    6.134 recon\service\compare.py:150(reconcile)
       23    6.116    0.266    6.119    0.266 {method 'execute' of 'sqlite3.Connection' objects}
        1    0.000    0.000    2.034    2.034 recon\repository\gold_repo.py:150(insert_key_results)
        1    0.000    0.000    1.512    1.512 recon\service\compare.py:189(evaluate_checks)
        1    0.000    0.000    1.355    1.355 recon\repository\gold_repo.py:298(insert_aggregates)
        1    0.000    0.000    1.061    1.061 recon\repository\gold_repo.py:217(insert_field_diffs)
        1    0.000    0.000    0.763    0.763 recon\service\compare.py:230(_aggregate_explained)
        3    0.000    0.000    0.763    0.254 recon\repository\gold_repo.py:304(aggregate_breakdown)
        1    0.000    0.000    0.262    0.262 recon\service\compare.py:224(_key_coverage)
        1    0.000    0.000    0.262    0.262 recon\repository\gold_repo.py:366(key_coverage)
        1    0.000    0.000    0.204    0.204 recon\service\compare.py:217(_referential_integrity)
        1    0.000    0.000    0.204    0.204 recon\repository\gold_repo.py:340(count_unknown_references)
        1    0.000    0.000    0.158    0.158 recon\repository\gold_repo.py:167(apply_diff_counts)
        1    0.000    0.000    0.154    0.154 recon\service\compare.py:210(_key_uniqueness)
        1    0.000    0.000    0.154    0.154 recon\repository\gold_repo.py:329(count_duplicate_keys)
```

## EXPLAIN QUERY PLAN on every GOLD query

Method (`python tools/explain_gold.py --n 100000 --seed 1`):
- every statement executed during the gold stage (`compare.reconcile`) is captured with `sqlite3`'s trace
  callback during a 100k run, plus the statements the report writer runs against the gold tables;
- `EXPLAIN QUERY PLAN` is run on each against the finished 100k DB, through
  `recon.repository.db.explain_query_plan`, so the SQL stays in the repository tier.

The tool marks a `SCAN` of a recon table, or of its alias in that statement, as
`FULL SCAN OF BASE TABLE`. A scan of anything else is a transient result the statement builds itself. The
tool ends with the count of base-table full scans; on the current code at n=100000 it reports 0.

**Finding: no statement does a full scan of a base table.** Every access to `clean_record`, `quarantine`,
`key_result`, `field_diff`, `aggregate_result`, `check_result`, `ref_customer`, `raw_record` and `run` is a
`SEARCH` through an index (a primary key, `ix_quarantine_key` or `ix_key_result_status`). The only `SCAN`
lines are over transient results that a statement builds itself (materialized CTEs and subqueries such as
`candidate`, `flags`, `walk`, `explained`, `groups`, `expected`), which is inherent to those queries. Every
primary key starts with `run_id`, so `SEARCH ... (run_id=?)` in a one-run DB still visits every row of the
run: that is the work, not a missing index. No index was added.

| statement (gold_repo) | runs per run | access path (base tables) | notable plan steps |
|---|---:|---|---|
| `insert_key_results` | 1 | clean_record PK (run_id), quarantine `ix_quarantine_key` | scans materialized `candidate` twice (flags, window function), temp B-trees for GROUP BY and window ORDER BY, automatic covering index for the `deciding` joins |
| `insert_field_diffs` | 1 (6 UNION ALL branches) | s: clean_record PK (run_id, side); t: clean_record PK (run_id, side, order_id) | none |
| `apply_diff_counts` (2 UPDATEs) | 1 | key_result `ix_key_result_status`; field_diff covering PK | correlated subquery per MATCHED key |
| `insert_aggregates`, **before** | **9** | clean_record PK (run_id) and (run_id, side); key_result covering PK | each statement materializes its own `actual`, `walk` (the key_result × clean_record join), `explained`, `groups` |
| `insert_aggregates`, **after** | **1** | same | `walk` materialized **once**; clean_record read once per grouping (3); automatic covering indexes for the group_value joins |
| `count_duplicate_keys` | 2 | covering PKs | scan of a GROUP BY subquery |
| `count_unknown_references` | 1 | clean_record PK; ref_customer covering PK per row | correlated NOT EXISTS |
| `key_coverage` | 1 | covering PKs / indexes | materialized `expected`, bloom filters for NOT IN |
| `aggregate_breakdown` | 3 | key_result covering PK; clean_record PK lookups | none |
| row accounting counts, `count_unexplained_aggregates`, check inserts, readers | several | PK or covering index | none |
| report: `iter_non_matched`, `aggregate_breakdown_by_status`, CSV readers | 1-3 each | PKs, `ix_key_result_status`, field_diff covering PK | temp B-tree for the quarantine ORDER BY (CASE side) |

## The optimization: all aggregates in one statement

**Evidence.** `insert_aggregates` was the largest single item of either stage: 5.09 s unprofiled (19% of
the 27.10 s run). It ran 9 statements (3 measures × 3 groupings). Each one re-read clean_record, rebuilt the
same key_result × clean_record join (`walk`) for the explained delta, and grouped a single measure. The
silver candidates measured smaller: the per-row csv reader 0.99 s, `payload_sha256` 1.35 s, and the
timestamp parsing is spread over 400,964 calls with no single fix of comparable size.

**Change** (`gold_repo.aggregates_sql`). One `INSERT ... SELECT` produces every aggregate row. A `walk` CTE
holds the key_result join once with every grouping value and every measure contribution as columns. A
`rows` CTE holds the clean rows likewise. Per grouping, `actual_<i>` and `explained_<i>` group those for all
measures at once, and one SELECT per (grouping, measure) emits the rows. The query plan confirms `walk` is
materialized once. The explained delta is still rebuilt from key_result, independently of the SQL SUMs
(DESIGN 4.6), and SUM still raises on integer overflow (AGGREGATE_OVERFLOW).

**Result.** `insert_aggregates` 5.09 -> 1.36 s; gold 9.69 -> 6.38 s; total 27.10 -> 23.50 s; CPU
26.50 -> 22.48 s (100k medians). Identical result digests, and the full test suite passes, including the
10k end-to-end test against the generator's expected answers and the shuffled-input determinism test.

**Next candidates, by measured size** (not done; one change per step):
1. `validate_side` (8.9 s at 100k): per-field Python normalization. Timestamp parsing is the largest leaf.
   A fast path for the canonical `YYYY-MM-DDTHH:MM:SSZ` form, plus building the payload hash string
   without `json.dumps` (1.35 s), are the obvious targets. Both need care to stay byte-identical.
2. `insert_clean_records` (3.2 s): 199,800 dict-parameter rows with a foreign-key check each. Tuple
   parameters and fewer, larger batches would help; the FK check itself stays (DESIGN).
3. `insert_key_results` (2.1 s): scans the materialized `candidate` CTE twice and sorts it for the window
   function.

## At 10x / 100x scale

Peak Python memory grew in step with the input (27.0 MiB at 10k, 268.0 MiB at 100k) and so did the DB
(13.1 -> 131.1 MiB). Time is roughly linear too (2.27 s -> 23.50 s). At 1M-10M orders the design limits are
memory (whole sides held in Python), single-threaded Python validation, and one SQLite writer. In order of
payoff:

1. **Stream instead of loading a whole side.** Today `fetch_raw_records` returns a full side as a list of
   dicts, `validate_side` keeps every `ValidRow`, and `dedupe` groups all of them in a dict. Instead:
   - iterate the raw_record cursor in line_no order in fixed-size chunks;
   - write each chunk's validated rows to a silver staging table;
   - do the dedupe set-based in SQL: `ROW_NUMBER()` / `COUNT(DISTINCT payload_sha256)` over
     `(side, order_id)` at `max(updated_at)`, including the rejected-row shadowing rule.

   Memory then stays bounded by the chunk size, not the input. Determinism is kept because every
   ordering is explicit (line_no, business key).
2. **Postgres with COPY + set-based SQL.**
   - Land raw lines with `COPY ... FROM STDIN` into an UNLOGGED bronze table: no per-row INSERT.
   - Validate with Python streaming chunks out through `COPY`, or partly in SQL (regex and numeric checks
     with `CASE`).
   - Gold is already set-based, and its CTEs and window functions port almost unchanged. Postgres adds
     hash joins and parallel query, which SQLite lacks; money stays `BIGINT` minor units.
   - An advisory lock per run_key would also close the concurrency gap in DESIGN 7.2 #4.
3. **Partition by batch date** (or run_id) for bronze, silver and gold.
   - Each run touches one partition, so every `run_id = ?` predicate prunes to it.
   - Indexes stay small per partition.
   - Retention becomes `DROP PARTITION` instead of `DELETE`.
   - Backfills of different dates write to different partitions without contending.
4. **Hash rows first; field-diff only rows whose hash differs.**
   - Store a `compare_sha256` over only the compared fields' normalized values (`payload_sha256` also
     covers `updated_at`, which is not compared).
   - Keys with equal hashes on both sides are MATCHED with no field comparison.
   - Only keys with different hashes go through the per-field diff with tolerance. A hash mismatch can
     still turn out MATCHED, because of the 0.01 money tolerance, so the rule stays exact.
   - At 1,000 field diffs per 100,300 keys (this dataset), the field-diff join shrinks to about 1% of the
     keys. Across two systems, comparing hashes per key range first (Merkle-style) avoids moving matching
     rows at all.
5. **Parallel ingest per side.**
   - SOURCE, TARGET and REFERENCE are independent until gold.
   - Hashing, CSV parsing and validation are CPU-bound Python. They can run in one worker process per side,
     or per line-range chunk, writing to per-side staging (separate partitions or files).
   - With SQLite there is still one writer, so workers hand rows to a single inserting connection. With
     Postgres, each worker can `COPY` its own partition.
   - Outputs stay deterministic because every row is keyed by (side, line_no) and ordered explicitly
     afterwards.
