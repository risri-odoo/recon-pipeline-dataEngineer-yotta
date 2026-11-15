"""Field normalization: Decimal money to integer minor units, UTC ISO-8601 timestamps, text cleanup.

Pure functions, no I/O. Rules follow docs/DESIGN.md sections 2.1, 4.3 and 7.1.
"""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Container, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from recon.service.config import Config, FieldSpec

MISSING_REQUIRED = "MISSING_REQUIRED"
BAD_TYPE = "BAD_TYPE"
BAD_FORMAT = "BAD_FORMAT"
BAD_ENUM = "BAD_ENUM"
OUT_OF_RANGE = "OUT_OF_RANGE"
INVALID_REFERENCE = "INVALID_REFERENCE"

_TS = re.compile(
    r"^([0-9]{4})-([0-9]{2})-([0-9]{2})[T ]([0-9]{2}):([0-9]{2}):([0-9]{2})"
    r"(?:\.[0-9]+)?(Z|[+-][0-9]{2}:[0-9]{2})?$"
)
_TS_LIKE = re.compile(r"^[0-9]{1,4}[-/.][0-9]{1,2}[-/.][0-9]{1,4}(?:[Tt ].*)?$")
_NUMBER_LIKE = re.compile(r"^[+-]?[0-9.,]*[0-9][0-9.,]*(?:[eE][+-]?[0-9]+)?$")


class NormalizeError(ValueError):
    def __init__(self, code: str, detail: str):
        super().__init__(f"{code}: {detail}")
        self.code = code
        self.detail = detail


@dataclass(frozen=True)
class Failure:
    field: str
    code: str
    detail: str


@dataclass(frozen=True)
class RowResult:
    values: Mapping[str, object]
    failures: tuple[Failure, ...]
    business_key: str | None
    version: str | None

    @property
    def ok(self) -> bool:
        return not self.failures

    @property
    def reason_code(self) -> str | None:
        return self.failures[0].code if self.failures else None

    @property
    def field_name(self) -> str | None:
        return self.failures[0].field if self.failures else None

    @property
    def detail(self) -> str:
        return "; ".join(f"{f.field}:{f.code}:{f.detail}" for f in self.failures)


def normalize_value(spec: FieldSpec, raw: str | None) -> object:
    """Return the canonical value (str, int minor units, or None) or raise NormalizeError."""
    text = "" if raw is None else raw.strip()
    if text == "":
        if spec.required:
            raise NormalizeError(MISSING_REQUIRED, "required value is empty")
        return None
    if spec.type == "str":
        return _normalize_str(spec, text)
    if spec.type == "enum":
        return _normalize_enum(spec, text)
    if spec.type == "decimal":
        return _normalize_decimal(spec, text)
    if spec.type == "timestamp":
        return parse_timestamp(text)
    raise ValueError(f"unsupported field type {spec.type!r}")


def validate_row(
    config: Config,
    raw: Mapping[str, str | None],
    reference_ids: Container[str] | None = None,
) -> RowResult:
    """Validate one logical row; collect ALL failures in field order.

    INVALID_REFERENCE is checked last and only when everything else passed.
    """
    values: dict[str, object] = {}
    failures: list[Failure] = []
    for spec in config.fields:
        try:
            values[spec.name] = normalize_value(spec, raw.get(spec.name))
        except NormalizeError as exc:
            values[spec.name] = None
            failures.append(Failure(spec.name, exc.code, exc.detail))
    if not failures and reference_ids is not None:
        for spec in config.fields:
            value = values[spec.name]
            if spec.reference and value is not None and value not in reference_ids:
                failures.append(Failure(spec.name, INVALID_REFERENCE, f"{value!r} not found in reference"))
    key = values.get(config.business_key)
    version = values.get(config.version_field)
    return RowResult(
        values=values,
        failures=tuple(failures),
        business_key=key if isinstance(key, str) else None,
        version=version if isinstance(version, str) else None,
    )


def parse_money(text: str, scale: int) -> int:
    """Parse a plain decimal string into integer minor units (no float, no rounding)."""
    strict = re.fullmatch(rf"-?[0-9]+(?:\.[0-9]{{1,{scale}}})?", text) if scale else re.fullmatch(r"-?[0-9]+", text)
    if strict:
        return int(Decimal(text).scaleb(scale))
    if _NUMBER_LIKE.match(_strip_currency_symbols(text)):
        raise NormalizeError(BAD_FORMAT, f"{text!r} is not a plain decimal with at most {scale} decimal places")
    raise NormalizeError(BAD_TYPE, f"{text!r} is not a number")


def parse_timestamp(text: str) -> str:
    """Parse an accepted timestamp form and return canonical UTC 'YYYY-MM-DDTHH:MM:SSZ'."""
    m = _TS.match(text)
    if not m:
        if _TS_LIKE.match(text):
            raise NormalizeError(BAD_FORMAT, f"{text!r} is not 'YYYY-MM-DD[T ]HH:MM:SS[.f][Z|+HH:MM]'")
        raise NormalizeError(BAD_TYPE, f"{text!r} is not a timestamp")
    year, month, day, hour, minute, second = (int(g) for g in m.groups()[:6])
    offset = m.group(7)
    try:
        dt = datetime(year, month, day, hour, minute, second)
        if offset and offset != "Z":
            oh, om = int(offset[1:3]), int(offset[4:6])
            if oh > 23 or om > 59:
                raise ValueError("offset out of range")
            delta = timedelta(hours=oh, minutes=om)
            dt = dt - delta if offset[0] == "+" else dt + delta
    except (ValueError, OverflowError):
        raise NormalizeError(BAD_TYPE, f"{text!r} is not a valid calendar timestamp") from None
    return (f"{dt.year:04d}-{dt.month:02d}-{dt.day:02d}"
            f"T{dt.hour:02d}:{dt.minute:02d}:{dt.second:02d}Z")


def format_minor(units: int, scale: int = 2) -> str:
    """Render integer minor units as a fixed-point string, e.g. 12345 -> '123.45'."""
    sign = "-" if units < 0 else ""
    whole, frac = divmod(abs(units), 10 ** scale)
    return f"{sign}{whole}.{frac:0{scale}d}" if scale else f"{sign}{whole}"


def _normalize_str(spec: FieldSpec, text: str) -> str:
    if spec.upper:
        text = text.upper()
    if spec.pattern is not None and not spec.pattern.fullmatch(text):
        raise NormalizeError(BAD_FORMAT, f"{text!r} does not match {spec.pattern.pattern!r}")
    return text


def _normalize_enum(spec: FieldSpec, text: str) -> str:
    value = text.upper()
    if value not in spec.values:
        raise NormalizeError(BAD_ENUM, f"{text!r} not in {'|'.join(spec.values)}")
    return value


def _normalize_decimal(spec: FieldSpec, text: str) -> int:
    minor = parse_money(text, spec.scale)
    if spec.min_minor is not None and minor < spec.min_minor:
        raise NormalizeError(OUT_OF_RANGE, f"{text!r} is below minimum {format_minor(spec.min_minor, spec.scale)}")
    return minor


def _strip_currency_symbols(text: str) -> str:
    return "".join(ch for ch in text if unicodedata.category(ch) != "Sc")
