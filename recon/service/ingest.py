"""Lands raw source/target/reference records into BRONZE (docs/DESIGN.md sections 3, 4.1, 4.2, 7.1).

Two passes per file:
1. `check_file`: stream raw bytes once (sha256 + strict UTF-8 decode), then scan the records once
   (header + whether the file ends inside an open quoted field). Produces batch rejects
   (FILE_NOT_FOUND, EMPTY_FILE, ENCODING_ERROR, MALFORMED_FILE, DUPLICATE_HEADER, SCHEMA_MISSING_COLUMN)
   and header warnings (SCHEMA_EXTRA_COLUMN, MISSING_OPTIONAL_COLUMN).
2. `land_file`: stream records verbatim into raw_record (HEADER + DATA), skipping and counting blank
   lines; adds the EMPTY_BATCH warning. The bytes are re-hashed while landing; a hash different from
   pass 1 raises InputChangedError (the run becomes FAILED / INPUT_CHANGED).

Nothing is parsed into fields here beyond what is needed to find record boundaries and the header;
malformed data records are landed as-is and judged later by validation.
"""

from __future__ import annotations

import codecs
import csv
import hashlib
import io
import json
import sqlite3
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import TextIO

from recon.repository import bronze_repo
from recon.service.config import Config

SOURCE, TARGET, REFERENCE = "SOURCE", "TARGET", "REFERENCE"
SIDE_ORDER = (SOURCE, TARGET, REFERENCE)

FILE_NOT_FOUND = "FILE_NOT_FOUND"
EMPTY_FILE = "EMPTY_FILE"
ENCODING_ERROR = "ENCODING_ERROR"
MALFORMED_FILE = "MALFORMED_FILE"
DUPLICATE_HEADER = "DUPLICATE_HEADER"
SCHEMA_MISSING_COLUMN = "SCHEMA_MISSING_COLUMN"
SCHEMA_EXTRA_COLUMN = "SCHEMA_EXTRA_COLUMN"
MISSING_OPTIONAL_COLUMN = "MISSING_OPTIONAL_COLUMN"
EMPTY_BATCH = "EMPTY_BATCH"

HASH_CHUNK_BYTES = 1 << 20
INSERT_CHUNK_ROWS = 5000
TEXT_ENCODING = "utf-8-sig"  # a leading BOM is stripped; everything else must be strict UTF-8
INPUT_CHANGED = "INPUT_CHANGED"
MISSING_HASH = "MISSING"  # the "hash" of a file that vanished (same literal as DESIGN 5)


class InputChangedError(RuntimeError):
    """A file's bytes differ between the check pass and the landing pass. The run must become FAILED."""

    failure_reason = INPUT_CHANGED

    def __init__(self, side: str, path: str, expected_sha256: str, actual_sha256: str):
        super().__init__(f"{side} file {path!r} changed during the run: "
                         f"sha256 {expected_sha256} at check, {actual_sha256} at landing")
        self.side = side
        self.path = path
        self.expected_sha256 = expected_sha256
        self.actual_sha256 = actual_sha256

    @property
    def failure_detail(self) -> str:
        return json.dumps({"side": self.side, "file": self.path, "expected_sha256": self.expected_sha256,
                           "actual_sha256": self.actual_sha256}, sort_keys=True, ensure_ascii=False)


# --------------------------------------------------------------------------- data types


@dataclass(frozen=True)
class SideSpec:
    """What one side's header must provide: logical -> physical column (normalized) and required fields."""

    side: str
    columns: tuple[tuple[str, str], ...]
    required: frozenset[str]


@dataclass(frozen=True)
class BatchIssue:
    side: str
    file: str
    code: str
    detail: str
    columns: tuple[str, ...] = ()

    def as_dict(self) -> dict:
        return {"side": self.side, "file": self.file, "code": self.code, "detail": self.detail,
                "columns": list(self.columns)}


@dataclass
class FileCheck:
    side: str
    path: str
    sha256: str | None = None
    header_line_no: int | None = None
    header: tuple[str, ...] = ()  # normalized (trim + lower-case) header names, file order
    column_index: dict[str, int] = field(default_factory=dict)  # logical field -> header position
    rejects: list[BatchIssue] = field(default_factory=list)
    warnings: list[BatchIssue] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.rejects


@dataclass
class SideIngest:
    side: str
    path: str
    sha256: str
    header_line_no: int
    header: tuple[str, ...]
    column_index: dict[str, int]
    data_records: int
    blank_lines: int
    physical_lines: int
    warnings: list[BatchIssue]


@dataclass
class IngestResult:
    checks: dict[str, FileCheck]
    sides: dict[str, SideIngest]

    @property
    def rejects(self) -> list[BatchIssue]:
        """All batch rejects, SOURCE -> TARGET -> REFERENCE, detection order within a file."""
        return [issue for side in SIDE_ORDER if side in self.checks for issue in self.checks[side].rejects]

    @property
    def ok(self) -> bool:
        return not self.rejects

    @property
    def warnings(self) -> list[BatchIssue]:
        """Batch warnings of landed sides, sorted by code then side (DESIGN 4.2)."""
        found = [w for s in self.sides.values() for w in s.warnings]
        return sorted(found, key=lambda w: (w.code, SIDE_ORDER.index(w.side)))


@dataclass(frozen=True)
class RawRecord:
    line_no: int  # physical line where the record starts (1-based)
    raw_text: str  # exact record text without the trailing line terminator
    fields: list[str] | None  # csv-parsed fields; None if the csv module raised on this record
    blank: bool


# --------------------------------------------------------------------------- side specs


def side_specs(config: Config) -> dict[str, SideSpec]:
    required = frozenset(f.name for f in config.fields if f.required)
    specs = {
        SOURCE: SideSpec(SOURCE, config.source_columns, required),
        TARGET: SideSpec(TARGET, config.target_columns, required),
    }
    if config.reference is not None:
        # Only the reference key is required; other reference columns are informational (optional).
        specs[REFERENCE] = SideSpec(REFERENCE, config.reference.columns, frozenset({config.reference.key}))
    return specs


# --------------------------------------------------------------------------- record reader


def iter_records(fh: TextIO) -> Iterator[RawRecord]:
    """Yield every CSV record with its exact raw text and starting physical line.

    The file iterator is wrapped so the physical lines csv.reader consumes for one record are captured;
    a quoted field spanning lines therefore keeps all of its lines in raw_text. `fh` must be opened with
    newline='' so line terminators reach us untranslated.
    """
    consumed: list[str] = []
    lines_read = 0

    def physical_lines() -> Iterator[str]:
        nonlocal lines_read
        for line in fh:
            lines_read += 1
            consumed.append(line)
            yield line

    reader = csv.reader(physical_lines())
    next_line_no = 1
    while True:
        try:
            fields: list[str] | None = next(reader)
        except StopIteration:
            break
        except csv.Error:
            fields = None
            if not consumed:
                break
        text = "".join(consumed)
        consumed.clear()
        start, next_line_no = next_line_no, lines_read + 1
        blank = start == lines_read and text.strip() == ""
        yield RawRecord(start, _strip_terminator(text), fields, blank)


def _strip_terminator(text: str) -> str:
    if text.endswith("\r\n"):
        return text[:-2]
    if text.endswith(("\n", "\r")):
        return text[:-1]
    return text


def _open_text(path: str | Path) -> TextIO:
    return open(path, encoding=TEXT_ENCODING, newline="")


def ends_in_open_quote(text: str) -> bool:
    """True if a record's text ends inside a quoted field (an unterminated quote).

    Mirrors csv.reader's default, non-strict dialect (delimiter ',', quotechar '"', doubled quotes as
    escapes): a quote opens a quoted field only at the start of a field, a quote inside an unquoted
    field is literal, and characters after a closing quote continue the field. csv.reader itself
    silently accepts an unterminated quote at end of file, which is why this check exists.
    """
    start, unquoted, quoted, quote_in_quoted = range(4)
    state = start
    for ch in text:
        if state == quoted:
            if ch == '"':
                state = quote_in_quoted
        elif state == quote_in_quoted:
            if ch == '"':
                state = quoted  # doubled quote = escaped quote
            elif ch == ",":
                state = start
            elif ch in "\r\n":
                state = start
            else:
                state = unquoted
        elif state == start:
            if ch == '"':
                state = quoted
            elif ch not in ",\r\n":
                state = unquoted
        elif ch in ",\r\n":  # unquoted
            state = start
    return state == quoted


def csv_error_detail(exc: csv.Error) -> str:
    """Human-readable text for a csv.Error; the field size limit is named as such, not as a quoting error."""
    if "field larger than field limit" in str(exc):
        return f"a field exceeds the CSV field size limit ({csv.field_size_limit()} characters)"
    return f"CSV quoting error: {exc}"


def _strict_parse_error(raw_text: str) -> str | None:
    """None if raw_text is exactly one strictly valid CSV record, else why not."""
    try:
        rows = list(csv.reader([raw_text], strict=True))
    except csv.Error as exc:
        return csv_error_detail(exc)
    return None if len(rows) == 1 else f"record parses as {len(rows)} rows"


class _HashingReader(io.RawIOBase):
    """Raw binary stream that feeds every byte it reads into a sha256."""

    def __init__(self, raw):
        self._raw = raw
        self.digest = hashlib.sha256()

    def readable(self) -> bool:
        return True

    def readinto(self, buffer) -> int:
        n = self._raw.readinto(buffer)
        if n:
            self.digest.update(memoryview(buffer)[:n])
        return n or 0

    def drain(self) -> None:
        """Hash whatever has not been read yet (e.g. after a decode error)."""
        while chunk := self._raw.read(HASH_CHUNK_BYTES):
            self.digest.update(chunk)

    def close(self) -> None:
        self._raw.close()
        super().close()


# --------------------------------------------------------------------------- pass 1: batch checks


def check_file(path: str | Path, spec: SideSpec) -> FileCheck:
    path_text = str(path)
    check = FileCheck(side=spec.side, path=path_text)

    def reject(code: str, detail: str, columns: tuple[str, ...] = ()) -> FileCheck:
        check.rejects.append(BatchIssue(spec.side, path_text, code, detail, columns))
        return check

    p = Path(path)
    if not p.is_file():
        return reject(FILE_NOT_FOUND, "path does not exist or is not a file")

    digest = hashlib.sha256()
    decoder = codecs.getincrementaldecoder(TEXT_ENCODING)(errors="strict")
    size = 0
    encoding_error: str | None = None
    with open(p, "rb") as fh:
        while chunk := fh.read(HASH_CHUNK_BYTES):
            digest.update(chunk)
            if encoding_error is None:
                try:
                    decoder.decode(chunk)
                except UnicodeDecodeError as exc:
                    encoding_error = f"invalid UTF-8 at byte offset {size + exc.start}: {exc.reason}"
            size += len(chunk)
    if encoding_error is None:
        try:
            decoder.decode(b"", final=True)
        except UnicodeDecodeError as exc:
            encoding_error = f"truncated UTF-8 sequence at end of file: {exc.reason}"
    check.sha256 = digest.hexdigest()

    if size == 0:
        return reject(EMPTY_FILE, "file has zero bytes")
    if encoding_error is not None:
        return reject(ENCODING_ERROR, encoding_error)

    header: RawRecord | None = None
    last: RawRecord | None = None
    with _open_text(p) as fh:
        for record in iter_records(fh):
            if header is None and not record.blank:
                header = record
            last = record
    if header is None:
        return reject(EMPTY_FILE, "no non-blank header line")
    if last is not None and ends_in_open_quote(last.raw_text):
        return reject(MALFORMED_FILE, f"unterminated quoted field: the record starting at line {last.line_no} "
                                      f"is still open at end of file")
    header_error = _strict_parse_error(header.raw_text)
    if header.fields is None or header_error is not None:
        reason = f": {header_error}" if header_error and "field size limit" in header_error else ""
        return reject(MALFORMED_FILE, f"header record starting at line {header.line_no} is not parseable CSV{reason}")

    names = tuple(name.strip().lower() for name in header.fields)
    check.header_line_no = header.line_no
    check.header = names

    seen: set[str] = set()
    duplicates: list[str] = []
    for name in names:
        if name in seen and name not in duplicates:
            duplicates.append(name)
        seen.add(name)
    if duplicates:
        reject(DUPLICATE_HEADER, f"duplicate header name(s): {', '.join(duplicates)}", tuple(duplicates))

    position = {name: i for i, name in reversed(list(enumerate(names)))}  # first occurrence wins
    missing_required = tuple(phys for logical, phys in spec.columns
                             if logical in spec.required and phys not in position)
    if missing_required:
        reject(SCHEMA_MISSING_COLUMN, f"required column(s) missing: {', '.join(missing_required)}",
               missing_required)

    missing_optional = tuple(phys for logical, phys in spec.columns
                             if logical not in spec.required and phys not in position)
    if missing_optional:
        check.warnings.append(BatchIssue(spec.side, path_text, MISSING_OPTIONAL_COLUMN,
                                         f"optional column(s) missing, values are NULL: {', '.join(missing_optional)}",
                                         missing_optional))
    mapped = {phys for _, phys in spec.columns}
    extra = tuple(name for name in names if name not in mapped)
    if extra:
        check.warnings.append(BatchIssue(spec.side, path_text, SCHEMA_EXTRA_COLUMN,
                                         f"unmapped column(s) ignored: {', '.join(extra)}", extra))

    check.column_index = {logical: position[phys] for logical, phys in spec.columns if phys in position}
    return check


def check_inputs(paths: Mapping[str, str | Path], config: Config) -> dict[str, FileCheck]:
    """Run batch checks on every side's file (all of them, so every batch reject is reported)."""
    specs = side_specs(config)
    return {side: check_file(paths[side], specs[side]) for side in SIDE_ORDER if side in specs}


# --------------------------------------------------------------------------- pass 2: landing


def land_file(conn: sqlite3.Connection, run_id: str, check: FileCheck) -> SideIngest:
    """Land the header and every data record of a checked file. The caller owns the transaction.

    The bytes are re-hashed as they are read; if they differ from the check pass (or the file vanished
    or no longer decodes), InputChangedError is raised and the caller must roll back.
    """
    if not check.ok:
        raise ValueError(f"{check.side} file {check.path!r} has batch rejects; refusing to land")
    expected = check.sha256 or ""
    try:
        raw = open(check.path, "rb")
    except OSError:
        raise InputChangedError(check.side, check.path, expected, MISSING_HASH) from None
    hasher = _HashingReader(raw)

    batch: list[bronze_repo.RawRow] = []
    data_records = blank_lines = physical_lines = 0
    header_seen = False
    with io.TextIOWrapper(io.BufferedReader(hasher), encoding=TEXT_ENCODING, newline="") as text:
        try:
            for record in iter_records(text):
                physical_lines = record.line_no + record.raw_text.count("\n") + _bare_cr_count(record.raw_text)
                if record.blank:
                    blank_lines += 1
                    continue
                record_type = "DATA" if header_seen else "HEADER"
                header_seen = True
                if record_type == "DATA":
                    data_records += 1
                batch.append((run_id, check.side, record.line_no, check.path, record_type, record.raw_text))
                if len(batch) >= INSERT_CHUNK_ROWS:
                    bronze_repo.insert_raw_records(conn, batch)
                    batch = []
        except UnicodeDecodeError:
            pass  # the bytes changed since the check pass; the hash comparison below reports it
        hasher.drain()  # hash any unread tail so the digest always covers the whole file
        actual = hasher.digest.hexdigest()
    if actual != expected:
        raise InputChangedError(check.side, check.path, expected, actual)
    if batch:
        bronze_repo.insert_raw_records(conn, batch)

    warnings = list(check.warnings)
    if data_records == 0:
        warnings.append(BatchIssue(check.side, check.path, EMPTY_BATCH, "valid header, zero data records"))
    return SideIngest(
        side=check.side, path=check.path, sha256=actual, header_line_no=check.header_line_no or 0,
        header=check.header, column_index=dict(check.column_index), data_records=data_records,
        blank_lines=blank_lines, physical_lines=physical_lines, warnings=warnings,
    )


def _bare_cr_count(text: str) -> int:
    return text.count("\r") - text.count("\r\n")


def ingest(conn: sqlite3.Connection, run_id: str, paths: Mapping[str, str | Path], config: Config,
           checks: dict[str, FileCheck] | None = None) -> IngestResult:
    """Check all sides; if any batch reject exists land nothing, otherwise land every side in order."""
    checks = checks if checks is not None else check_inputs(paths, config)
    result = IngestResult(checks=checks, sides={})
    if not result.ok:
        return result
    for side in SIDE_ORDER:
        if side in checks:
            result.sides[side] = land_file(conn, run_id, checks[side])
    return result
