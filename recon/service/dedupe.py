"""Duplicate dispositions and version ordering per business key (docs/DESIGN.md section 4.4).

Per (side, key), version ordering covers every valid row plus every rejected row whose updated_at parsed.
With max_u = the maximum updated_at over those rows:
1. a rejected row at max_u shadows the key: no clean row, every valid row is SUPERSEDED
   ("shadowed by rejected line N"); there is no fallback to an older version;
2. otherwise, valid rows below max_u are SUPERSEDED, and among the valid rows at max_u:
   - all payloads identical -> the lowest line_no is kept, the others are DUPLICATE_EXACT;
   - payloads differ -> all of them are CONFLICTING_DUPLICATE, nothing is kept, the key is a conflict key.
Rejected rows keep their ROW_REJECT code. Rows are processed in line_no order whatever order they come in,
so arrival order never changes the outcome.

`build_silver` runs the whole SILVER stage (reference, validate, dedupe) and writes it in the caller's
transaction.
"""

from __future__ import annotations

import sqlite3
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field

from recon.repository import silver_repo
from recon.service.config import Config
from recon.service.validate import (SOURCE, TARGET, QuarantineEntry, ReferenceValidation, ValidRow,
                                    validate_reference, validate_side)

DUPLICATE = "DUPLICATE"
DUPLICATE_EXACT = "DUPLICATE_EXACT"
SUPERSEDED = "SUPERSEDED"
CONFLICTING_DUPLICATE = "CONFLICTING_DUPLICATE"


@dataclass
class DedupeResult:
    winners: list[ValidRow] = field(default_factory=list)  # sorted by business key
    dispositions: list[QuarantineEntry] = field(default_factory=list)  # sorted by line_no
    conflict_keys: list[str] = field(default_factory=list)  # sorted

    def count(self, reason_code: str) -> int:
        return sum(1 for d in self.dispositions if d.reason_code == reason_code)


@dataclass(frozen=True)
class SideCounts:
    """Row accounting for one side. Dispositions are counted apart from rejects."""

    side: str
    rows_read: int
    accepted: int
    rejected: int
    duplicate_exact: int
    superseded: int
    conflicting_rows: int

    @property
    def balanced(self) -> bool:
        return self.rows_read == (self.accepted + self.rejected + self.duplicate_exact
                                  + self.superseded + self.conflicting_rows)


@dataclass
class SideSilver:
    counts: SideCounts
    winners: list[ValidRow]
    quarantine: list[QuarantineEntry]  # row rejects and dispositions, sorted by line_no
    conflict_keys: list[str]


@dataclass
class SilverResult:
    sides: dict[str, SideSilver]
    reference: ReferenceValidation | None


def _disposition(row: ValidRow, code: str, detail: str) -> QuarantineEntry:
    return QuarantineEntry(row.side, row.line_no, row.source_file, row.raw_text, DUPLICATE, code, detail,
                           order_id=row.key, updated_at=row.version)


def _lines(rows: Iterable[ValidRow]) -> str:
    return ", ".join(str(r.line_no) for r in rows)


def dedupe(valid: Iterable[ValidRow], rejects: Iterable[QuarantineEntry] = ()) -> DedupeResult:
    """Pick at most one winner per key among `valid`; `rejects` take part in version ordering only."""
    by_key: dict[str, list[ValidRow]] = defaultdict(list)
    for row in sorted(valid, key=lambda r: r.line_no):
        by_key[row.key].append(row)
    rejected_versions: dict[str, list[tuple[str, int]]] = defaultdict(list)
    for rej in rejects:
        if rej.order_id is not None and rej.updated_at is not None:
            rejected_versions[rej.order_id].append((rej.updated_at, rej.line_no))

    result = DedupeResult()
    for key in sorted(by_key):
        rows = by_key[key]
        max_u = max([r.version for r in rows] + [u for u, _ in rejected_versions.get(key, [])])
        shadow = sorted(line for u, line in rejected_versions.get(key, []) if u == max_u)
        if shadow:
            result.dispositions += [_disposition(r, SUPERSEDED, f"shadowed by rejected line {shadow[0]}")
                                    for r in rows]
            continue
        top = [r for r in rows if r.version == max_u]
        older = [r for r in rows if r.version != max_u]
        if len({r.payload_sha256 for r in top}) == 1:
            winner = top[0]
            result.winners.append(winner)
            result.dispositions += [_disposition(r, DUPLICATE_EXACT, f"exact duplicate of line {winner.line_no}")
                                    for r in top[1:]]
            older_detail = f"superseded by line {winner.line_no} (updated_at {max_u})"
        else:
            result.conflict_keys.append(key)
            result.dispositions += [
                _disposition(r, CONFLICTING_DUPLICATE,
                             f"{len({x.payload_sha256 for x in top})} different payloads at updated_at {max_u}"
                             f" on lines {_lines(top)}")
                for r in top]
            older_detail = f"superseded by conflicting versions at updated_at {max_u} (lines {_lines(top)})"
        result.dispositions += [_disposition(r, SUPERSEDED, older_detail) for r in older]

    result.dispositions.sort(key=lambda d: d.line_no)
    return result


def build_silver(conn: sqlite3.Connection, run_id: str, config: Config) -> SilverResult:
    """Validate the reference and both order sides, dedupe, and write SILVER. The caller owns the transaction."""
    reference: ReferenceValidation | None = None
    reference_ids: frozenset[str] | None = None
    if config.reference is not None:
        reference = validate_reference(conn, run_id, config)
        reference_ids = reference.ids  # loaded once, shared by both sides
        silver_repo.insert_ref_customers(conn, ({"run_id": run_id, "customer_id": cid, "customer_name": name}
                                                for cid, name in sorted(reference.customers)))
        silver_repo.insert_quarantine(conn, (q.as_row(run_id) for q in reference.rejects))

    result = SilverResult(sides={}, reference=reference)
    for side in (SOURCE, TARGET):
        validated = validate_side(conn, run_id, side, config, reference_ids)
        deduped = dedupe(validated.valid, validated.rejects)
        quarantine = sorted(validated.rejects + deduped.dispositions, key=lambda q: q.line_no)
        silver_repo.insert_quarantine(conn, (q.as_row(run_id) for q in quarantine))
        silver_repo.insert_clean_records(conn, (w.as_clean_row(run_id, config) for w in deduped.winners))
        counts = SideCounts(
            side=side,
            rows_read=validated.rows_read,
            accepted=len(deduped.winners),
            rejected=len(validated.rejects),
            duplicate_exact=deduped.count(DUPLICATE_EXACT),
            superseded=deduped.count(SUPERSEDED),
            conflicting_rows=deduped.count(CONFLICTING_DUPLICATE),
        )
        result.sides[side] = SideSilver(counts, deduped.winners, quarantine, deduped.conflict_keys)
    return result

