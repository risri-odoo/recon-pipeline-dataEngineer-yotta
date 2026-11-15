"""Validates and normalizes BRONZE records (docs/DESIGN.md sections 3 and 4.3).

For each side, raw_record DATA rows are read in line_no order and re-parsed strictly:
- the field count differs from the header, or the CSV quoting is broken -> MALFORMED_ROW (no trusted fields);
- otherwise every field is normalized (normalize.validate_row). reason_code = the first failure,
  detail = all of them, and the business key and updated_at are kept whenever they parsed;
- INVALID_REFERENCE is checked last, against the reference customer set (loaded once, a Python set).

The REFERENCE side is validated first: an empty customer_id is MISSING_REQUIRED, a duplicate
customer_id keeps its first occurrence and is not quarantined (DESIGN 7.1 #10).
Nothing is written here; dedupe.build_silver writes the results.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from recon.repository import bronze_repo
from recon.repository.silver_repo import KEY_COLUMN
from recon.service.config import Config, FieldSpec
from recon.service.normalize import NormalizeError, normalize_value, validate_row

SOURCE, TARGET, REFERENCE = "SOURCE", "TARGET", "REFERENCE"
ROW_REJECT = "ROW_REJECT"
MALFORMED_ROW = "MALFORMED_ROW"
NULL_TOKEN = "\\N"  # how NULL is rendered inside the payload hash


@dataclass(frozen=True)
class QuarantineEntry:
    """One excluded data record (row reject or duplicate disposition); maps 1:1 to a quarantine row."""

    side: str
    line_no: int
    source_file: str
    raw_text: str
    category: str
    reason_code: str
    detail: str
    field_name: str | None = None
    order_id: str | None = None  # business key, if it parsed
    updated_at: str | None = None  # canonical version, if it parsed

    def as_row(self, run_id: str) -> dict[str, Any]:
        return {"run_id": run_id, "side": self.side, "line_no": self.line_no, "source_file": self.source_file,
                "raw_text": self.raw_text, "category": self.category, "reason_code": self.reason_code,
                "field_name": self.field_name, "order_id": self.order_id, "updated_at": self.updated_at,
                "detail": self.detail}


@dataclass(frozen=True)
class ValidRow:
    """A data record that passed every row-level check, with typed canonical values."""

    side: str
    line_no: int
    source_file: str
    raw_text: str
    key: str
    version: str
    values: Mapping[str, object]  # logical field -> str | int (minor units) | None
    payload_sha256: str

    def as_clean_row(self, run_id: str, config: Config) -> dict[str, Any]:
        row: dict[str, Any] = {clean_column(spec): self.values[spec.name] for spec in config.fields}
        row.update(run_id=run_id, side=self.side, payload_sha256=self.payload_sha256,
                   source_file=self.source_file, line_no=self.line_no)
        row[KEY_COLUMN] = self.key
        return row


@dataclass
class SideValidation:
    side: str
    rows_read: int  # landed DATA records
    valid: list[ValidRow]
    rejects: list[QuarantineEntry]


@dataclass
class ReferenceValidation:
    rows_read: int
    customers: list[tuple[str, str | None]]  # (customer_id, customer_name), first occurrence, file order
    rejects: list[QuarantineEntry]

    @property
    def ids(self) -> frozenset[str]:
        return frozenset(cid for cid, _ in self.customers)


# --------------------------------------------------------------------------- helpers


def clean_column(spec: FieldSpec) -> str:
    """clean_record column for a logical field: money columns carry the `_minor` suffix (DESIGN 3)."""
    return f"{spec.name}_minor" if spec.type == "decimal" else spec.name


def payload_sha256(config: Config, values: Mapping[str, object]) -> str:
    """sha256 over all logical fields in config order; NULL rendered as \\N, minor units as integers."""
    parts = [NULL_TOKEN if values[spec.name] is None else str(values[spec.name]) for spec in config.fields]
    text = json.dumps(parts, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def parse_record(raw_text: str) -> list[str] | str:
    """Strictly parse one landed record. Returns its fields, or an error text on a CSV quoting error."""
    try:
        rows = list(csv.reader(io.StringIO(raw_text, newline=""), strict=True))
    except csv.Error as exc:
        return f"CSV quoting error: {exc}"
    if len(rows) != 1:
        return f"CSV quoting error: record parses as {len(rows)} rows"
    return rows[0]


def _split_header(records: list[dict[str, Any]], side: str) -> tuple[list[str], list[dict[str, Any]]]:
    if not records or records[0]["record_type"] != "HEADER":
        raise ValueError(f"{side}: raw_record has no HEADER record; was the side landed?")
    fields = parse_record(records[0]["raw_text"])
    if isinstance(fields, str):  # ingest rejects such files with MALFORMED_FILE, so this is a bug
        raise ValueError(f"{side}: landed header is not parseable: {fields}")
    return [name.strip().lower() for name in fields], records[1:]


def _column_index(header: list[str], columns: tuple[tuple[str, str], ...]) -> dict[str, int]:
    position: dict[str, int] = {}
    for i, name in enumerate(header):
        position.setdefault(name, i)  # first occurrence wins (duplicates are batch-rejected anyway)
    return {logical: position[phys] for logical, phys in columns if phys in position}


def _malformed(side: str, record: dict[str, Any], fields: list[str] | str, header_len: int) -> QuarantineEntry | None:
    if isinstance(fields, str):
        detail = fields
    elif len(fields) != header_len:
        detail = f"expected {header_len} fields, got {len(fields)}"
    else:
        return None
    return QuarantineEntry(side, record["line_no"], record["source_file"], record["raw_text"],
                           ROW_REJECT, MALFORMED_ROW, detail)


# --------------------------------------------------------------------------- orders sides


def validate_side(conn: sqlite3.Connection, run_id: str, side: str, config: Config,
                  reference_ids: frozenset[str] | None) -> SideValidation:
    """Validate every landed DATA record of SOURCE or TARGET, in line_no order."""
    if side not in (SOURCE, TARGET):
        raise ValueError(f"validate_side handles SOURCE/TARGET, not {side!r}")
    header, records = _split_header(bronze_repo.fetch_raw_records(conn, run_id, side), side)
    index = _column_index(header, tuple(config.columns_for(side.lower()).items()))
    result = SideValidation(side=side, rows_read=len(records), valid=[], rejects=[])

    for record in records:
        fields = parse_record(record["raw_text"])
        malformed = _malformed(side, record, fields, len(header))
        if malformed is not None:
            result.rejects.append(malformed)
            continue
        raw = {logical: fields[i] for logical, i in index.items()}
        row = validate_row(config, raw, reference_ids)
        if row.ok:
            result.valid.append(ValidRow(side, record["line_no"], record["source_file"], record["raw_text"],
                                         key=row.business_key, version=row.version, values=dict(row.values),
                                         payload_sha256=payload_sha256(config, row.values)))
        else:
            result.rejects.append(QuarantineEntry(
                side, record["line_no"], record["source_file"], record["raw_text"], ROW_REJECT,
                row.reason_code, row.detail, field_name=row.field_name,
                order_id=row.business_key, updated_at=row.version))
    return result


# --------------------------------------------------------------------------- reference side


def validate_reference(conn: sqlite3.Connection, run_id: str, config: Config) -> ReferenceValidation:
    """Validate the REFERENCE side: customer_id trim + upper-case and required; first occurrence wins."""
    if config.reference is None:
        raise ValueError("config has no reference section")
    key = config.reference.key
    key_spec = FieldSpec(name=key, type="str", required=True, upper=True)
    header, records = _split_header(bronze_repo.fetch_raw_records(conn, run_id, REFERENCE), REFERENCE)
    index = _column_index(header, config.reference.columns)
    name_index = index.get("customer_name")
    result = ReferenceValidation(rows_read=len(records), customers=[], rejects=[])
    seen: set[str] = set()

    for record in records:
        fields = parse_record(record["raw_text"])
        malformed = _malformed(REFERENCE, record, fields, len(header))
        if malformed is not None:
            result.rejects.append(malformed)
            continue
        try:
            customer_id = normalize_value(key_spec, fields[index[key]])
        except NormalizeError as exc:  # only MISSING_REQUIRED is possible for a plain upper-cased str
            result.rejects.append(QuarantineEntry(
                REFERENCE, record["line_no"], record["source_file"], record["raw_text"], ROW_REJECT,
                exc.code, f"{key}:{exc.code}:{exc.detail}", field_name=key))
            continue
        if customer_id in seen:
            continue
        seen.add(customer_id)
        name = fields[name_index].strip() if name_index is not None else ""
        result.customers.append((customer_id, name or None))
    return result
