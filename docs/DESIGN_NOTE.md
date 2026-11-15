# Design note: trade-offs and limitations

Full design: [DESIGN.md](DESIGN.md). Measurements: [PERFORMANCE.md](PERFORMANCE.md).

## Trade-offs

| choice | over | why | cost |
|---|---|---|---|
| **SQLite**, one file per DB | Postgres | zero setup, ships with Python, one file to inspect; every rule is visible SQL that ports almost unchanged | one writer; no hash joins or parallel query; ~23.5 s per 100k orders on a laptop |
| **Python stdlib** (csv, decimal, sqlite3) | pandas | pandas strips leading zeros (`0012` → `12`), reads money as float and does not match NULL to NULL the way the rules need | per-row Python validation is the slowest stage (silver, 46% of the 100k baseline) |
| **One transaction per run** | checkpointed stages | a run is all or nothing: REJECTED/FAILED runs leave zero data rows, and retry is simply "run again" | a crash late in the run repeats all the work; the write lock is held for the whole run |
| **Integer minor units** for money | float / Decimal in the DB | exact sums and comparisons; no float anywhere; SQLite INTEGER is exact | all currencies assumed 2 decimals (no JPY, KWD) |
| **Full field compare** in SQL | row-hash compare only | names *which* field differs and *how* (TARGET_STALE, NULL_MISMATCH…), and money tolerance cannot be expressed as a hash | compares every field of every key; a hash pre-filter is listed as a next step |
| **`updated_at` = version metadata**, not compared | comparing it like any field | it decides which version wins and classifies each diff as TARGET_STALE / TARGET_AHEAD / VALUE_MISMATCH; comparing it too would flag a re-loaded but otherwise identical row as a mismatch | a key whose only difference is `updated_at` counts as MATCHED |
| **Inclusive tolerance** (`\|src − tgt\| ≤ 1` minor unit) | strict equality | one-cent rounding differences between systems are not real mismatches | a true one-cent error is invisible per key (the aggregates still show the delta) |
| **Strict rejection** over guessing | lenient parsing | `1,234.50` (thousands separator) and `10.005` (scale 3) are BAD_FORMAT: a reconciliation tool must not invent the value it is checking | real exports in EU format must be fixed upstream |

Other rules that follow the same idea: a rejected newest version shadows older valid ones (no fallback to stale
data); a tie at the same second with different payloads is a CONFLICT, never a silent pick; upper-casing touches
ASCII letters only, so distinct keys never merge.

## Limitations

- **Single writer.** SQLite allows one writing connection. The admission step is one `BEGIN IMMEDIATE`, but there
  is no lock beyond the RUNNING/stale rule; two processes on the same inputs at the same instant are not
  guarded against (DESIGN 7.2 #4). If the lock is lost mid-run, the run row stays RUNNING until the stale rule
  marks it ABANDONED.
- **Long write lock on huge files.** Because the whole run is one data transaction, the DB is write-locked for
  the full run (~23.5 s at 100k). Other runs on the same DB wait or time out.
- **Reference and sides in memory.** The validated reference ids are one in-memory set, and validation holds a
  whole side's records, validated rows and dedupe groups in Python: 268.0 MiB peak at 100k orders, growing
  linearly. Streaming in chunks with a set-based dedupe is the fix (PERFORMANCE.md, "At 10x / 100x scale").
- **No cross-run trend alerts.** Each run is judged on its own. Nothing compares this run's mismatch counts or
  deltas with previous runs, although every run stays in the DB keyed by `run_id`.
