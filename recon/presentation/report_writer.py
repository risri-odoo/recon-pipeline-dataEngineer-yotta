"""Writes the run outputs in out/<run_id>/ (docs/REPORT_SCHEMA.md, DESIGN 7.1 #12).

Report files (all rebuilt from the DB by recon.service.report):
    report.json       machine-readable report (report_version 1)
    mismatches.csv    one row per non-MATCHED key, canonical source and target values side by side
    field_diffs.csv   one row per differing compared field
    quarantine.csv    every quarantined record with its raw text
    summary.md        human-readable one-pager, PASS/FAIL at the top
Also in the run dir, written elsewhere and kept as-is by a report write: run.log, metrics.json (the
only non-deterministic outputs).

A report is published atomically as a set: every report file is written into a staging directory next
to the run dir, the run dir's other files are copied in, and the staging directory is renamed into
place (the old run dir is moved aside first and deleted afterwards). A crash therefore leaves either the
previous complete run dir or the new one, never half a report; `recover_run_dir` finishes an interrupted
swap. JSON: sorted keys, ensure_ascii=False, final newline. CSV: newline="" and "\\n" line endings.
"""

from __future__ import annotations

import csv
import json
import os
import shutil
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

from recon.service.compare import DIFF_TYPES, KEY_STATUSES
from recon.service.ingest import SIDE_ORDER
from recon.service.normalize import format_minor
from recon.service.report import (FIELD_DIFF_CSV_COLUMNS, MINOR_UNITS, QUARANTINE_CSV_COLUMNS, ROW_REJECT_CODES,
                                  ReportBundle)

REPORT_JSON = "report.json"
MISMATCHES_CSV = "mismatches.csv"
FIELD_DIFFS_CSV = "field_diffs.csv"
QUARANTINE_CSV = "quarantine.csv"
SUMMARY_MD = "summary.md"
METRICS_FILE = "metrics.json"
RUN_LOG = "run.log"
REPORT_FILES = (REPORT_JSON, MISMATCHES_CSV, FIELD_DIFFS_CSV, QUARANTINE_CSV, SUMMARY_MD)

STAGING_SUFFIX = ".staging"
BACKUP_SUFFIX = ".old"


# --------------------------------------------------------------------------- primitives


def dump_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False) + "\n"


def _replace_file(path: Path, text: str) -> Path:
    """Write text (UTF-8, LF) to a temp file beside `path`, then rename it over `path`."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8", newline="") as fh:
        fh.write(text)
    os.replace(tmp, path)
    return path


def write_json(path: Path, value: Any) -> Path:
    """Sorted keys, ensure_ascii=False, final newline, LF on every OS (DESIGN 7.1 #12); atomic per file."""
    return _replace_file(path, dump_json(value))


def write_metrics(run_dir: Path, metrics: dict[str, Any]) -> Path:
    """out/<run_id>/metrics.json: timings and memory only, never part of report.json (DESIGN 7.1 #13)."""
    recover_run_dir(run_dir)
    return write_json(run_dir / METRICS_FILE, metrics)


def write_csv(path: Path, header: Sequence[str], rows: Iterable[Sequence[Any]]) -> int:
    """UTF-8 CSV with '\\n' line endings; None is written as an empty field. Returns the data row count."""
    count = 0
    with open(path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh, lineterminator="\n")
        writer.writerow(header)
        for row in rows:
            writer.writerow(row)
            count += 1
    return count


# --------------------------------------------------------------------------- atomic publish


def _staging_paths(run_dir: Path) -> tuple[Path, Path]:
    return (run_dir.with_name(f".{run_dir.name}{STAGING_SUFFIX}"),
            run_dir.with_name(f".{run_dir.name}{BACKUP_SUFFIX}"))


def recover_run_dir(run_dir: Path) -> None:
    """Finish a swap that a crash interrupted. Backup but no run dir: the crash hit between the two
    renames, so the previous complete dir is restored. Both present: the swap completed, the backup is
    leftover. A leftover staging dir is always incomplete and is removed."""
    staging, backup = _staging_paths(run_dir)
    if backup.exists():
        if run_dir.exists():
            shutil.rmtree(backup)
        else:
            os.replace(backup, run_dir)
    if staging.exists():
        shutil.rmtree(staging)


def write_report(run_dir: Path, bundle: ReportBundle) -> Path:
    """Publish every report file into run_dir as one set. Returns the report.json path."""
    recover_run_dir(run_dir)
    staging, backup = _staging_paths(run_dir)
    staging.mkdir(parents=True)
    try:
        write_report_files(staging, bundle)
        if run_dir.is_dir():  # carry over run.log, metrics.json and anything else that is not the report
            for entry in sorted(run_dir.iterdir()):
                if entry.name in REPORT_FILES:
                    continue
                if entry.is_dir():
                    shutil.copytree(entry, staging / entry.name)
                else:
                    shutil.copy2(entry, staging / entry.name)
        _swap(staging, run_dir, backup)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return run_dir / REPORT_JSON


def _swap(staging: Path, run_dir: Path, backup: Path) -> None:
    if not run_dir.exists():
        os.replace(staging, run_dir)
        return
    os.replace(run_dir, backup)
    try:
        os.replace(staging, run_dir)
    except BaseException:
        os.replace(backup, run_dir)  # put the previous complete dir back
        raise
    shutil.rmtree(backup)


def write_report_files(directory: Path, bundle: ReportBundle) -> None:
    """The five report files, written straight into `directory` (callers pass a staging dir)."""
    write_csv(directory / MISMATCHES_CSV, bundle.mismatch_columns, bundle.mismatch_rows())
    write_csv(directory / FIELD_DIFFS_CSV, FIELD_DIFF_CSV_COLUMNS, bundle.field_diff_rows())
    write_csv(directory / QUARANTINE_CSV, QUARANTINE_CSV_COLUMNS, bundle.quarantine_rows())
    with open(directory / REPORT_JSON, "w", encoding="utf-8", newline="") as fh:
        fh.write(dump_json(bundle.report))
    with open(directory / SUMMARY_MD, "w", encoding="utf-8", newline="") as fh:
        fh.write(render_summary(bundle.report))


# --------------------------------------------------------------------------- summary.md


def md_cell(value: Any) -> str:
    """One Markdown table cell: None -> '-', '\\' and '|' escaped, line breaks flattened."""
    if value is None:
        return "-"
    if isinstance(value, bool):
        return "yes" if value else "no"
    text = str(value).replace("\\", "\\\\").replace("|", "\\|")
    return " ".join(text.splitlines()) if ("\n" in text or "\r" in text) else text


def _table(header: Sequence[str], rows: Iterable[Sequence[Any]]) -> list[str]:
    lines = ["| " + " | ".join(header) + " |", "|" + "|".join("---" for _ in header) + "|"]
    lines += ["| " + " | ".join(md_cell(v) for v in row) + " |" for row in rows]
    return lines


def _ordered(mapping: dict[str, Any], order: Sequence[str]) -> list[tuple[str, Any]]:
    """Items in catalogue order, then any other keys sorted, so the summary never depends on dict order
    (the same text whether rendered from the in-memory report or from the parsed report.json)."""
    rank = {k: i for i, k in enumerate(order)}
    return sorted(mapping.items(), key=lambda kv: (rank.get(kv[0], len(rank)), kv[0]))


def _amount(value: int, unit: str, scale: int) -> str:
    return format_minor(value, scale) if unit == MINOR_UNITS else str(value)


def render_summary(report: dict[str, Any]) -> str:
    run, totals = report["run"], report["totals"]
    verdict = run["recon_status"]
    lines = [f"# Reconciliation {verdict}", ""]
    if verdict == "PASS":
        lines.append(f"**{verdict}**: all {totals['keys']} keys matched and no record is unattributable.")
    else:
        lines.append(f"**{verdict}**: {totals['non_matched_keys']} of {totals['keys']} keys not matched, "
                     f"{totals['unattributable_rejects']} unattributable reject(s), "
                     f"{totals['field_diffs']} field difference(s), {totals['quarantine_rows']} quarantined row(s).")
    lines += ["", *_table(("run", ""), [
        ("run_id", run["run_id"]), ("dataset", run["dataset"]),
        ("business_key", ", ".join(run["business_key"])),
        ("run status", f"{run['status']} (exit code {run['exit_code']})"),
        ("attempt", f"{run['attempt']} (forced: {'yes' if run['forced'] else 'no'})"),
        ("started_at", run["started_at"]), ("finished_at", run["finished_at"]),
        ("config_sha256", run["config_sha256"]),
    ])]

    lines += ["", "## Inputs", "", *_table(("side", "file", "sha256", "data records"), [
        (side, i["file"], i["sha256"], i["data_records"]) for side, i in _ordered(report["inputs"], SIDE_ORDER)])]

    sides = report["sides"]
    lines += ["", "## Rows per side", "", *_table(
        ("side", "read", "accepted", "rejected", "duplicate_exact", "superseded", "conflicting_rows",
         "duplicates_ignored"),
        [(side, s["rows_read"], s["accepted"], s["rejected"], s.get("duplicate_exact"), s.get("superseded"),
          s.get("conflicting_rows"), s.get("duplicates_ignored")) for side, s in _ordered(sides, SIDE_ORDER)])]
    rejects = [(side, code, n) for side, s in _ordered(sides, SIDE_ORDER)
               for code, n in _ordered(s["rejected_by_reason"], ROW_REJECT_CODES) if n]
    lines += ["", "### Row rejects by reason", ""]
    lines += _table(("side", "reason_code", "rows"), rejects) if rejects else ["None."]

    lines += ["", "## Keys", "", *_table(("key_status", "keys"), [
        *_ordered(report["key_status_counts"], KEY_STATUSES), ("total", totals["keys"])])]

    diffs = [(col, t, n) for col, by_type in sorted(report["field_diff_counts"].items())
             for t, n in _ordered(by_type, DIFF_TYPES) if n]
    lines += ["", "## Field differences", ""]
    lines += _table(("column", "diff_type", "count"), diffs) if diffs else ["None."]

    aggs = report["aggregates"]
    lines += ["", "## Aggregates", "", "Overall:", "", *_table(
        ("metric", "source", "target", "delta"),
        [(a["metric"], *(_amount(a[k], a["unit"], a["scale"]) for k in ("source_value", "target_value", "delta")))
         for a in aggs if a["group_by"] == "ALL"])]
    grouped = [a for a in aggs if a["group_by"] != "ALL" and a["delta"] != 0]
    lines += ["", "Groups with a delta:", ""]
    lines += _table(("metric", "group_by", "group", "source", "target", "delta"), [
        (a["metric"], a["group_by"], a["group_value"],
         *(_amount(a[k], a["unit"], a["scale"]) for k in ("source_value", "target_value", "delta")))
        for a in grouped]) if grouped else ["None."]

    lines += ["", "## Where the overall deltas come from", ""]
    for e in report["aggregate_explanation"]:
        fmt = lambda v, e=e: _amount(v, e["unit"], e["scale"])  # noqa: E731
        lines += [f"**{e['metric']}**: delta {fmt(e['delta'])} = source-only {fmt(e['source_only'])}"
                  f" - target-only {fmt(e['target_only'])} + both sides {fmt(e['both_sides'])}", ""]
        lines += _table(("key_status", "keys", "source", "target", "delta"), [
            (s, b["keys"], fmt(b["source_value"]), fmt(b["target_value"]), fmt(b["delta"]))
            for s, b in _ordered(e["by_key_status"], KEY_STATUSES) if b["keys"]])
        lines.append("")

    lines += ["## Checks", "", *_table(("check", "severity", "status", "expected", "actual"), [
        (c["check_name"], c["severity"], c["status"], c["expected"], c["actual"]) for c in report["checks"]])]
    lines += ["", "## Warnings", ""]
    lines += [f"- {w['code']} on {w['side']}: {', '.join(w['columns']) or '(no columns)'}"
              for w in report["warnings"]] or ["None."]

    samples = report["mismatch_samples"]["keys"]
    lines += ["", f"## Non-matched keys (first {len(samples)} of {totals['non_matched_keys']})", ""]
    lines += _table(("record_key", "key_status", "differing columns", "source", "target"), [
        (m["record_key"], m["key_status"], ", ".join(m["differing_columns"]),
         _side_ref(m["source_state"], m["source_line_no"]), _side_ref(m["target_state"], m["target_line_no"]))
        for m in samples]) if samples else ["None."]

    lines += ["", "## Files", "",
              f"- `{REPORT_JSON}`: the full machine-readable report",
              f"- `{MISMATCHES_CSV}`: every non-matched key with source and target values side by side",
              f"- `{FIELD_DIFFS_CSV}`: every field difference",
              f"- `{QUARANTINE_CSV}`: every rejected or excluded record, with file, line and raw text",
              f"- `{METRICS_FILE}`, `{RUN_LOG}`: timings and logs (not deterministic)", ""]
    return "\n".join(lines)


def _side_ref(state: str, line_no: int | None) -> str:
    return state if line_no is None else f"{state} (line {line_no})"
