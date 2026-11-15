# AI usage

How I worked: Claude Code wrote the code one step at a time. A separate Claude chat reviewed every step with me before I committed. I made the final call on each decision below and verified the important ones by hand.

## Planning

1. Claude started building the whole project itself → I stopped it and had it write step-by-step prompts with a review gate after each → because I need to understand and defend every part of the code.
2. ChatGPT, which I used to cross-check the plan, suggested pandas + YAML → I went with Python stdlib + SQLite + JSON config → because pandas silently strips leading zeros, turns money into floats and breaks null = null matching, and SQL makes the comparison logic visible.
3. The plan had no rule for line endings → I added .gitattributes (LF, CSVs as binary) → because I'm on Windows and git rewriting CRLF would change file hashes and break the idempotency tests.

## Design review (Step 1)

4. Claude Code stored batch rejects in the quarantine table → I moved them to the run row → because a batch reject rolls back the transaction, so those quarantine rows would have disappeared.
5. If the newest version of an order was rejected, Claude Code fell back to the older valid version → I changed it so the key is marked rejected and the older row is SUPERSEDED → because otherwise stale data gets compared as if it were the truth.
6. "Live vs stale RUNNING run" had no concrete rule → I set a timeout (--stale-after-minutes, default 60) and no PID checks → because os.kill(pid, 0) actually kills the process on Windows.
7. A SKIPPED rerun re-emitted the old report → I changed it to just print the existing report path → because a skip shouldn't rewrite anything.
8. Claude Code asked whether KEY_UNIQUENESS / REFERENTIAL_INTEGRITY should fail on bad data → I made them engine invariants (failure = bug = run FAILED) → because conflicts and bad references are already quarantined, so data problems should only affect recon status.
9. Claude Code asked whether to reject fractional seconds → I accepted them and truncated to whole seconds → because warehouses often truncate milliseconds, which would cause false mismatches.
10. The design didn't say what happens to blank lines mid-file → I defined it: not landed, not quarantined, counted in metrics → because otherwise the row accounting check would break.
11. My first prompt accepted 1,234.50 and rounded 10.005 → I kept the stricter design instead (both BAD_FORMAT) → because a reconciliation tool should reject ambiguous money, not guess.

## Build review (Steps 2–8)

12. Claude Code forced physical column names to snake_case → I relaxed it to "non-empty and unique after trim + lower-case" → because real exports have headers like Order ID.
13. Claude Code offered to hash normalized column names in the config → I kept hashing the config as written → because a cosmetic change only causes a harmless rerun, while over-normalizing could wrongly skip a real change.
14. Claude Code ran git commit on its own → I added a rule that it never runs git write commands → because I review and commit every step myself.
15. The generator gave the 60-row sample a single customer → I set a minimum of 5 → because one customer makes the reference and group-by checks meaningless.
16. Claude Code wrote per-scenario key lists for every dataset size → I limited them to N ≤ 1000 → because they add megabytes at large N and only help when checking the sample by eye.
17. An unclosed quote silently swallowed the rest of the file into one "malformed row", and an unparseable header was reported as EMPTY_FILE → I added one new batch reject, MALFORMED_FILE → because otherwise every later order would wrongly show up as EXTRA_IN_TARGET.
18. A file could change between hashing and loading → I made it re-hash while loading and fail with INPUT_CHANGED → because the stored hash must match what was actually loaded.
19. For composite keys I picked the escaped | join over a plain join → because a plain join lets ('A|B','C') collide with ('A','B|C').
20. A key rejected only on the target side had no status → I chose TARGET_REJECTED over EXTRA_IN_TARGET → because it mirrors SOURCE_REJECTED and keeps the swap-sides test exact.
21. The schema hardcoded the orders aggregate names in CHECK constraints → I removed them and validate from config → because the pipeline is meant to be config-driven.
22. Config allowed grouping by a money field → I forbade it → because the group would show raw minor units like 150 instead of 1.50.
23. Claude Code's Step 7 report left out the backfill command → I sent it back to implement it with tests → because backfill is part of the brief's retry/restart/backfill requirement.
24. With a stale and a fresh RUNNING attempt together, the stale one was never cleaned up → I made it mark stale ones ABANDONED first.
25. For recon report --run-id, I stored the run's config on the run row instead of requiring --config → because old reports stay reproducible even if the config file changes later.
26. quarantine.csv had no run_id → I added run_id as the first column of every CSV → because exported files get copied out of their folder and each row must stay traceable.

## Edge-case hunt (Step 9)

27. One huge amount (99999999999999999.99) failed the entire run → I added a configurable max, so it's a row-level OUT_OF_RANGE → because one bad row should never kill a batch.
28. A sum overflow showed up as a generic error → I gave it a clear AGGREGATE_OVERFLOW failure reason → because the operator needs to know what actually broke.
29. straße upper-cased to STRASSE and silently matched a different key → I limited upper-casing to ASCII letters, for enums too → because a silent key merge is the worst kind of reconciliation bug.
30. Two versions in the same second become a CONFLICT after truncation → I kept that and documented it → because it's flagged, not silently lost, and changing timestamp precision would ripple through the whole design.
31. A DB lock lost mid-run leaves the run row as RUNNING → I documented it instead of fixing it → because the stale rule already cleans it up.

## What I verified by hand

- Hand-checked the money conversion (value × 100) on the normalization tests.
- Landed the sample into a scratch DB and read raw_record rows myself: header at line 1, data rows after it, raw_text identical to the CSV.
- Ran the quoted-newline test and checked that raw_text kept both lines and that the next record was numbered correctly.
- Confirmed with git grep that no SQL exists outside recon/repository.
- Injected a crash after Silver (RECON_FAIL_AT=after_silver), queried the DB and saw zero rows for the failed run in every tier. The retry ran as attempt 2 and succeeded, and a third run was SKIPPED.
- Read summary.md and spot-checked three rows of mismatches.csv against the rules (amount mismatch, TARGET_STALE, NULL_MISMATCH).
- Measured my own 100k baseline with Measure-Command (24.2 s) before the performance step, so the optimization was judged against numbers from my machine.
