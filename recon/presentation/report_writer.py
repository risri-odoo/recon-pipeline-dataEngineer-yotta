"""Writes the run outputs in out/<run_id>/ (docs/REPORT_SCHEMA.md, DESIGN 7.1 #12).

Report files (all rebuilt from the DB by recon.service.report):
    report.json       machine-readable report (report_version 1)
    mismatches.csv    one row per non-MATCHED key, canonical source and target values side by side
    field_diffs.csv   one row per differing compared field
    quarantine.csv    every quarantined record with its raw text
    summary.md        human-readable one-pager, PASS/FAIL at the top
    summary.html      the same one-pager as a self-contained HTML page (inline CSS, no JavaScript)
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
import html
import json
import os
import shutil
from collections.abc import Iterable, Sequence
from decimal import ROUND_HALF_EVEN, Decimal
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
SUMMARY_HTML = "summary.html"
METRICS_FILE = "metrics.json"
RUN_LOG = "run.log"
REPORT_FILES = (REPORT_JSON, MISMATCHES_CSV, FIELD_DIFFS_CSV, QUARANTINE_CSV, SUMMARY_HTML, SUMMARY_MD)
# The bench result digest covers only the original five files, so digests stay comparable with the rows
# already recorded in docs/PERFORMANCE.md (summary.html is a pure rendering of report.json).
DIGEST_FILES = (REPORT_JSON, MISMATCHES_CSV, FIELD_DIFFS_CSV, QUARANTINE_CSV, SUMMARY_MD)

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
    """The six report files, written straight into `directory` (callers pass a staging dir)."""
    write_csv(directory / MISMATCHES_CSV, bundle.mismatch_columns, bundle.mismatch_rows())
    write_csv(directory / FIELD_DIFFS_CSV, FIELD_DIFF_CSV_COLUMNS, bundle.field_diff_rows())
    write_csv(directory / QUARANTINE_CSV, QUARANTINE_CSV_COLUMNS, bundle.quarantine_rows())
    with open(directory / REPORT_JSON, "w", encoding="utf-8", newline="") as fh:
        fh.write(dump_json(bundle.report))
    with open(directory / SUMMARY_HTML, "w", encoding="utf-8", newline="") as fh:
        fh.write(render_summary_html(bundle.report))
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


# --------------------------------------------------------------------------- summary.html

_CSS = """\
:root { color-scheme: light; }
* { box-sizing: border-box; }
body { margin: 0; background: #f6f7f9; color: #1f2328;
  font: 14px/1.45 system-ui, -apple-system, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; }
main { max-width: 1100px; margin: 0 auto; padding: 24px 16px 48px; }
h1 { margin: 0; font-size: 28px; }
h2 { margin: 32px 0 12px; font-size: 18px; }
h3 { margin: 20px 0 8px; font-size: 15px; }
code, .mono { font-family: ui-monospace, SFMono-Regular, Consolas, "Liberation Mono", monospace; font-size: 13px; }
.banner { border-radius: 8px; padding: 18px 20px; border: 1px solid; }
.banner.pass { background: #e6f4ea; border-color: #8fd19e; color: #0f5323; }
.banner.fail { background: #fdecea; border-color: #f1a7a0; color: #82160d; }
.banner p { margin: 6px 0 0; }
.meta { color: #57606a; font-size: 13px; }
.tiles { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 12px; margin-top: 16px; }
.tile { background: #fff; border: 1px solid #d0d7de; border-radius: 8px; padding: 12px 14px; }
.tile .label { color: #57606a; font-size: 12px; text-transform: uppercase; letter-spacing: .04em; }
.tile .value { font-size: 26px; font-weight: 600; font-variant-numeric: tabular-nums; }
.bars { background: #fff; border: 1px solid #d0d7de; border-radius: 8px; padding: 12px 14px; }
.bar-row { display: grid; grid-template-columns: 170px 1fr 110px; gap: 10px; align-items: center; margin: 4px 0; }
.bar-track { background: #eaeef2; border-radius: 4px; height: 14px; overflow: hidden; }
.bar-fill { background: #cf222e; height: 100%; }
.bar-fill.ok { background: #1a7f37; }
.bar-count { text-align: right; font-variant-numeric: tabular-nums; }
.scroll { overflow-x: auto; }
table { border-collapse: collapse; background: #fff; width: 100%; border: 1px solid #d0d7de; }
th, td { padding: 6px 10px; border-bottom: 1px solid #d0d7de; text-align: left; vertical-align: top; }
th { background: #f0f2f5; font-weight: 600; }
td.num, th.num { text-align: right; font-variant-numeric: tabular-nums; white-space: nowrap; }
.status-pass { color: #1a7f37; font-weight: 600; }
.status-other { color: #9a6700; font-weight: 600; }
.none { color: #57606a; }
@media (max-width: 600px) { .bar-row { grid-template-columns: 1fr 110px; } .bar-track { grid-column: 1 / -1; } }
"""


def h(value: Any) -> str:
    """HTML-escape one value from the data (quotes included). None -> '-', bool -> yes/no."""
    if value is None:
        return "-"
    if isinstance(value, bool):
        return "yes" if value else "no"
    return html.escape(str(value), quote=True)


def _share(part: int, whole: int) -> str:
    """Percentage as fixed 2-decimal text, computed in Decimal so it is the same text on every platform."""
    if whole <= 0:
        return "0.00"
    return str((Decimal(part) * 100 / Decimal(whole)).quantize(Decimal("0.01"), rounding=ROUND_HALF_EVEN))


def _html_table(header: Sequence[str], rows: Iterable[Sequence[Any]], numeric: Sequence[int] = ()) -> list[str]:
    """A table; every header and cell is escaped. `numeric` = column indexes to right-align."""
    num = set(numeric)

    def cls(i: int) -> str:
        return ' class="num"' if i in num else ""
    out = ['<div class="scroll"><table>',
           "<thead><tr>" + "".join(f"<th{cls(i)}>{h(c)}</th>" for i, c in enumerate(header)) + "</tr></thead>",
           "<tbody>"]
    out += ["<tr>" + "".join(f"<td{cls(i)}>{h(v)}</td>" for i, v in enumerate(row)) + "</tr>" for row in rows]
    out.append("</tbody></table></div>")
    return out


_NONE = '<p class="none">None.</p>'


def render_summary_html(report: dict[str, Any]) -> str:
    """summary.html: a self-contained page (inline CSS, no JavaScript, no external resources) built from the
    same report dict as summary.md, with the same numbers. Pure: depends only on `report`, so it renders the
    same bytes from the in-memory report or from the parsed report.json."""
    run, totals = report["run"], report["totals"]
    verdict = run["recon_status"]
    keys = totals["keys"]
    counts = report["key_status_counts"]

    body = ["<main>", f'<section class="banner {"pass" if verdict == "PASS" else "fail"}">',
            f"<h1>Reconciliation {h(verdict)}</h1>",
            f"<p>run_id <code>{h(run['run_id'])}</code> &middot; run status {h(run['status'])}"
            f" &middot; exit code {h(run['exit_code'])}</p>",
            f"<p class=\"meta\">dataset: {h(run['dataset'])} &middot; business_key: {h(', '.join(run['business_key']))}"
            f" &middot; attempt: {h(run['attempt'])} (forced: {h(bool(run['forced']))})</p>",
            f"<p class=\"meta\">started_at: {h(run['started_at'])} &middot; finished_at: {h(run['finished_at'])}</p>",
            "</section>"]

    tiles = (("total keys", keys), ("MATCHED", counts.get("MATCHED", 0)),
             ("non-matched", totals["non_matched_keys"]), ("field diffs", totals["field_diffs"]),
             ("quarantined rows", totals["quarantine_rows"]),
             ("unattributable rejects", totals["unattributable_rejects"]))
    body.append('<section class="tiles">')
    body += [f'<div class="tile"><div class="label">{h(label)}</div><div class="value">{h(v)}</div></div>'
             for label, v in tiles]
    body.append("</section>")

    body += ["<h2>Key status breakdown</h2>", '<section class="bars">']
    for status, n in _ordered(counts, KEY_STATUSES):
        pct = _share(n, keys)
        fill = "bar-fill ok" if status == "MATCHED" else "bar-fill"
        body.append(f'<div class="bar-row"><span class="mono">{h(status)}</span>'
                    f'<div class="bar-track"><div class="{fill}" style="width:{pct}%"></div></div>'
                    f'<span class="bar-count">{h(n)} ({pct}%)</span></div>')
    body += [f'<div class="bar-row"><strong>total</strong><span></span>'
             f'<span class="bar-count"><strong>{h(keys)}</strong></span></div>', "</section>"]

    body.append("<h2>Where the overall deltas come from</h2>")
    for e in report["aggregate_explanation"]:
        fmt = lambda v, e=e: _amount(v, e["unit"], e["scale"])  # noqa: E731
        body.append(f"<h3>{h(e['metric'])}</h3>")
        body.append(f"<p>delta <strong>{h(fmt(e['delta']))}</strong> = source-only {h(fmt(e['source_only']))}"
                    f" - target-only {h(fmt(e['target_only']))} + both sides {h(fmt(e['both_sides']))}</p>")
        rows = [(s, b["keys"], fmt(b["source_value"]), fmt(b["target_value"]), fmt(b["delta"]))
                for s, b in _ordered(e["by_key_status"], KEY_STATUSES) if b["keys"]]
        body += _html_table(("key_status", "keys", "source", "target", "delta"), rows, numeric=(1, 2, 3, 4)) \
            if rows else [_NONE]
    if not report["aggregate_explanation"]:
        body.append(_NONE)

    diffs = [(col, t, n) for col, by_type in sorted(report["field_diff_counts"].items())
             for t, n in _ordered(by_type, DIFF_TYPES) if n]
    body.append("<h2>Field differences</h2>")
    body += _html_table(("column", "diff_type", "count"), diffs, numeric=(2,)) if diffs else [_NONE]

    samples = report["mismatch_samples"]["keys"]
    body.append(f"<h2>Non-matched keys (first {h(len(samples))} of {h(totals['non_matched_keys'])})</h2>")
    body += _html_table(("record_key", "key_status", "differing columns", "source", "target"), [
        (m["record_key"], m["key_status"], ", ".join(m["differing_columns"]),
         _side_ref(m["source_state"], m["source_line_no"]), _side_ref(m["target_state"], m["target_line_no"]))
        for m in samples]) if samples else [_NONE]

    body += ["<h2>Checks</h2>", '<div class="scroll"><table>',
             "<thead><tr><th>check</th><th>severity</th><th>status</th><th>expected</th><th>actual</th></tr></thead>",
             "<tbody>"]
    for c in report["checks"]:
        css = "status-pass" if c["status"] == "PASS" else "status-other"
        body.append(f"<tr><td>{h(c['check_name'])}</td><td>{h(c['severity'])}</td>"
                    f'<td><span class="{css}">{h(c["status"])}</span></td>'
                    f"<td>{h(c['expected'])}</td><td>{h(c['actual'])}</td></tr>")
    body += ["</tbody></table></div>", "<h2>Warnings</h2>"]
    warnings = [f"<li>{h(w['code'])} on {h(w['side'])}: {h(', '.join(w['columns']) or '(no columns)')}</li>"
                for w in report["warnings"]]
    body += ["<ul>", *warnings, "</ul>"] if warnings else [_NONE]
    body += [f'<p class="meta">Full detail: {h(REPORT_JSON)}, {h(MISMATCHES_CSV)}, {h(FIELD_DIFFS_CSV)}, '
             f"{h(QUARANTINE_CSV)}.</p>", "</main>"]

    head = ["<!DOCTYPE html>", '<html lang="en">', "<head>", '<meta charset="utf-8">',
            '<meta name="viewport" content="width=device-width, initial-scale=1">',
            f"<title>Reconciliation {h(verdict)} - {h(run['run_id'])}</title>", "<style>", _CSS + "</style>",
            "</head>", "<body>"]
    return "\n".join([*head, *body, "</body>", "</html>", ""])
