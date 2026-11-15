"""Loads and validates reconciliation configuration (docs/DESIGN.md sections 2 and 5)."""

from __future__ import annotations

import copy
import hashlib
import json
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

FIELD_TYPES = ("str", "enum", "decimal", "timestamp")
SIDES = ("source", "target")

# Money limit (DESIGN 7.2): a decimal value, in minor units, may not exceed MONEY_LIMIT_MINOR in absolute
# value, so that a SUM over at least MONEY_SUM_HEADROOM_ROWS such values still fits SQLite's signed 64-bit
# INTEGER. Configured min/max must lie inside it; when no max (min) is configured, it is the bound itself.
INT64_MAX = 2**63 - 1
MONEY_SUM_HEADROOM_ROWS = 1_000_000
MONEY_LIMIT_MINOR = INT64_MAX // MONEY_SUM_HEADROOM_ROWS  # 9_223_372_036_854 (92233720368.54 at scale 2)

_SNAKE = re.compile(r"^[a-z][a-z0-9]*(_[a-z0-9]+)*$")
_TOP_KEYS = {"dataset", "fields", "business_key", "version_field", "compare_fields",
             "columns", "reference", "aggregates"}
_TYPE_KEYS = {
    "str": {"upper", "pattern", "reference"},
    "enum": {"values"},
    "decimal": {"scale", "min", "max", "tolerance"},
    "timestamp": set(),
}
_TYPE_DEFAULTS: dict[str, dict[str, Any]] = {
    "str": {"upper": False, "pattern": None, "reference": False},
    "enum": {},
    "decimal": {"scale": 2, "min": None, "max": None, "tolerance": "0"},
    "timestamp": {},
}


class ConfigError(ValueError):
    """Raised when a configuration file is missing, malformed or inconsistent."""


@dataclass(frozen=True)
class FieldSpec:
    name: str
    type: str
    required: bool
    upper: bool = False
    pattern: re.Pattern[str] | None = None
    reference: bool = False
    values: tuple[str, ...] = ()
    scale: int = 0
    min_minor: int | None = None
    max_minor: int | None = None
    tolerance_minor: int = 0


@dataclass(frozen=True)
class ReferenceSpec:
    key: str
    columns: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class AggregateSpec:
    sum_fields: tuple[str, ...]
    group_by: tuple[str, ...]


@dataclass(frozen=True)
class Config:
    dataset: str
    fields: tuple[FieldSpec, ...]
    business_key: tuple[str, ...]  # one field, or several for a composite key (config: string or list)
    version_field: str
    compare_fields: tuple[str, ...]
    source_columns: tuple[tuple[str, str], ...]
    target_columns: tuple[tuple[str, str], ...]
    reference: ReferenceSpec | None
    aggregates: AggregateSpec
    config_sha256: str
    config_json: str  # canonical JSON (sorted keys, defaults applied) that config_sha256 hashes; stored on run

    def field(self, name: str) -> FieldSpec:
        for spec in self.fields:
            if spec.name == name:
                return spec
        raise KeyError(name)

    def columns_for(self, side: str) -> dict[str, str]:
        """Logical name -> physical column name for 'source' or 'target'."""
        if side == "source":
            return dict(self.source_columns)
        if side == "target":
            return dict(self.target_columns)
        raise ValueError(f"unknown side {side!r}")


def load_config(path: str | Path) -> Config:
    path = Path(path)
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        raise ConfigError(f"config file not found: {path}") from None
    try:
        raw = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ConfigError(f"config {path} is not valid JSON: {exc}") from None
    return parse_config(raw)


def parse_config(raw: Any) -> Config:
    if not isinstance(raw, dict):
        raise ConfigError("config must be a JSON object")
    _no_unknown_keys(raw, _TOP_KEYS, "config")
    for key in ("dataset", "fields", "business_key", "version_field", "compare_fields",
                "columns", "aggregates"):
        if key not in raw:
            raise ConfigError(f"config is missing required key {key!r}")

    canonical = copy.deepcopy(raw)
    fields = tuple(_parse_field(f, i, canonical["fields"][i]) for i, f in enumerate(_list(raw["fields"], "fields")))
    if not fields:
        raise ConfigError("fields must not be empty")
    by_name: dict[str, FieldSpec] = {}
    for spec in fields:
        if spec.name in by_name:
            raise ConfigError(f"duplicate field name {spec.name!r}")
        by_name[spec.name] = spec

    version_field = _known(raw["version_field"], by_name, "version_field")
    if by_name[version_field].type != "timestamp":
        raise ConfigError(f"version_field {version_field!r} must be of type timestamp")
    if not by_name[version_field].required:
        raise ConfigError(f"version_field {version_field!r} must be required")
    business_key = _parse_business_key(raw["business_key"], by_name, version_field)

    compare_fields = tuple(_known(n, by_name, "compare_fields") for n in _list(raw["compare_fields"], "compare_fields"))
    for name in compare_fields:
        if name in business_key or name == version_field:
            raise ConfigError(f"compare_fields must not contain key/version field {name!r}")
    _no_duplicates(compare_fields, "compare_fields")

    columns = raw["columns"]
    if not isinstance(columns, dict):
        raise ConfigError("columns must be an object with 'source' and 'target' maps")
    _no_unknown_keys(columns, set(SIDES), "columns")
    side_maps = {side: _parse_column_map(columns.get(side), [f.name for f in fields], f"columns.{side}")
                 for side in SIDES}

    reference = _parse_reference(raw.get("reference"), fields)
    aggregates = _parse_aggregates(raw["aggregates"], by_name)

    return Config(
        dataset=_str(raw["dataset"], "dataset"),
        fields=fields,
        business_key=business_key,
        version_field=version_field,
        compare_fields=compare_fields,
        source_columns=side_maps["source"],
        target_columns=side_maps["target"],
        reference=reference,
        aggregates=aggregates,
        config_sha256=config_sha256(canonical),
        config_json=canonical_config_json(canonical),
    )


def canonical_config_json(canonical: dict[str, Any]) -> str:
    """The canonical JSON text of the config after defaults (DESIGN section 5)."""
    return json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def config_sha256(canonical: dict[str, Any]) -> str:
    """sha256 of the canonical JSON of the config after defaults (DESIGN section 5)."""
    return sha256_text(canonical_config_json(canonical))


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def load_stored_config(config_json: str, expected_sha256: str) -> Config:
    """Rebuild a Config from run.config_json, refusing it unless it hashes to run.config_sha256."""
    if sha256_text(config_json) != expected_sha256:
        raise ConfigError("stored config_json does not match the run's config_sha256")
    try:
        raw = json.loads(config_json)
    except json.JSONDecodeError as exc:
        raise ConfigError(f"stored config_json is not valid JSON: {exc}") from None
    return parse_config(raw)


def _parse_field(raw: Any, index: int, canonical: dict[str, Any]) -> FieldSpec:
    where = f"fields[{index}]"
    if not isinstance(raw, dict):
        raise ConfigError(f"{where} must be an object")
    name = _snake(raw.get("name"), f"{where}.name")
    where = f"field {name!r}"
    ftype = raw.get("type")
    if ftype not in FIELD_TYPES:
        raise ConfigError(f"{where}: unknown type {ftype!r}; expected one of {', '.join(FIELD_TYPES)}")
    _no_unknown_keys(raw, {"name", "type", "required"} | _TYPE_KEYS[ftype], where)
    canonical.setdefault("required", False)
    for key, default in _TYPE_DEFAULTS[ftype].items():
        canonical.setdefault(key, default)
    required = _bool(canonical["required"], f"{where}.required")

    if ftype == "str":
        pattern = canonical["pattern"]
        compiled = None
        if pattern is not None:
            try:
                compiled = re.compile(_str(pattern, f"{where}.pattern"))
            except re.error as exc:
                raise ConfigError(f"{where}: invalid pattern {pattern!r}: {exc}") from None
        return FieldSpec(name, ftype, required,
                         upper=_bool(canonical["upper"], f"{where}.upper"),
                         pattern=compiled,
                         reference=_bool(canonical["reference"], f"{where}.reference"))
    if ftype == "enum":
        values = _list(raw.get("values"), f"{where}.values")
        if not values:
            raise ConfigError(f"{where}: enum values must not be empty")
        for v in values:
            if not isinstance(v, str) or v != v.strip().upper() or not v:
                raise ConfigError(f"{where}: enum value {v!r} must be a non-empty upper-case string")
        _no_duplicates(values, f"{where}.values")
        return FieldSpec(name, ftype, required, values=tuple(values))
    if ftype == "decimal":
        scale = canonical["scale"]
        if not isinstance(scale, int) or isinstance(scale, bool) or not 0 <= scale <= 9:
            raise ConfigError(f"{where}: scale must be an integer 0..9")
        min_minor = None if canonical["min"] is None else _decimal_minor(canonical["min"], scale, f"{where}.min")
        max_minor = None if canonical["max"] is None else _decimal_minor(canonical["max"], scale, f"{where}.max")
        for key, bound in (("min", min_minor), ("max", max_minor)):
            if bound is not None and abs(bound) > MONEY_LIMIT_MINOR:
                raise ConfigError(f"{where}: {key} {canonical[key]!r} is outside the supported money range "
                                  f"+/-{MONEY_LIMIT_MINOR} minor units (int64 with headroom for summing)")
        if min_minor is not None and max_minor is not None and max_minor < min_minor:
            raise ConfigError(f"{where}: max {canonical['max']!r} must be >= min {canonical['min']!r}")
        tolerance = _decimal_minor(canonical["tolerance"], scale, f"{where}.tolerance")
        if tolerance < 0:
            raise ConfigError(f"{where}: tolerance must be >= 0")
        return FieldSpec(name, ftype, required, scale=scale, min_minor=min_minor, max_minor=max_minor,
                         tolerance_minor=tolerance)
    return FieldSpec(name, ftype, required)


def _parse_business_key(raw: Any, by_name: dict[str, FieldSpec], version_field: str) -> tuple[str, ...]:
    """A field name, or a non-empty list of distinct field names (composite key)."""
    names = [raw] if isinstance(raw, str) else _list(raw, "business_key")
    if not names:
        raise ConfigError("business_key list must not be empty")
    for name in names:
        _known(name, by_name, "business_key")
        spec = by_name[name]
        if not spec.required:
            raise ConfigError(f"business_key field {name!r} must be required")
        if spec.type == "decimal":
            raise ConfigError(f"business_key field {name!r} must not be of type decimal")
        if name == version_field:
            raise ConfigError(f"business_key must not contain the version_field {name!r}")
    _no_duplicates(names, "business_key")
    return tuple(names)


def _parse_column_map(raw: Any, logical: list[str], where: str) -> tuple[tuple[str, str], ...]:
    if not isinstance(raw, dict):
        raise ConfigError(f"{where} must be an object mapping logical -> physical column")
    for name in raw:
        if name not in logical:
            raise ConfigError(f"{where}: unknown logical field {name!r}")
    missing = [n for n in logical if n not in raw]
    if missing:
        raise ConfigError(f"{where}: logical field(s) not mapped: {', '.join(missing)}")
    physical = [_physical(raw[n], f"{where}.{n}") for n in logical]
    _no_duplicates(physical, f"{where} physical names (after trim + lower-case)")
    return tuple(zip(logical, physical))


def _parse_reference(raw: Any, fields: tuple[FieldSpec, ...]) -> ReferenceSpec | None:
    referencing = [f.name for f in fields if f.reference]
    if raw is None:
        if referencing:
            raise ConfigError(f"field(s) {', '.join(referencing)} set reference=true but no 'reference' section")
        return None
    if not isinstance(raw, dict):
        raise ConfigError("reference must be an object")
    _no_unknown_keys(raw, {"key", "columns"}, "reference")
    cols = raw.get("columns")
    if not isinstance(cols, dict) or not cols:
        raise ConfigError("reference.columns must be a non-empty object")
    pairs = tuple((_snake(k, "reference.columns key"), _physical(v, f"reference.columns.{k}")) for k, v in cols.items())
    _no_duplicates([p for _, p in pairs], "reference.columns physical names (after trim + lower-case)")
    key = raw.get("key")
    if key not in cols:
        raise ConfigError(f"reference.key {key!r} is not a column in reference.columns")
    if not referencing:
        raise ConfigError("reference section given but no field sets reference=true")
    return ReferenceSpec(key=key, columns=pairs)


def _parse_aggregates(raw: Any, by_name: dict[str, FieldSpec]) -> AggregateSpec:
    if not isinstance(raw, dict):
        raise ConfigError("aggregates must be an object")
    _no_unknown_keys(raw, {"sum_fields", "group_by"}, "aggregates")
    sums = tuple(_known(n, by_name, "aggregates.sum_fields") for n in _list(raw.get("sum_fields", []), "aggregates.sum_fields"))
    for name in sums:
        if by_name[name].type != "decimal":
            raise ConfigError(f"aggregates.sum_fields: money field {name!r} must be of type decimal")
    groups = tuple(_known(n, by_name, "aggregates.group_by") for n in _list(raw.get("group_by", []), "aggregates.group_by"))
    _no_duplicates(sums, "aggregates.sum_fields")
    _no_duplicates(groups, "aggregates.group_by")
    for name in groups:
        if by_name[name].type == "decimal":
            raise ConfigError(f"aggregates.group_by: decimal field {name!r} cannot be a grouping (money is a measure)")
    # aggregate_result.group_by is the field name upper-cased; 'ALL' is reserved for the overall grouping.
    if "all" in groups:
        raise ConfigError("aggregates.group_by: field 'all' collides with the reserved ALL grouping")
    return AggregateSpec(sum_fields=sums, group_by=groups)


def _decimal_minor(value: Any, scale: int, where: str) -> int:
    if not isinstance(value, str):
        raise ConfigError(f"{where} must be a decimal string such as \"0.01\" (no JSON numbers, to avoid floats)")
    try:
        d = Decimal(value)
    except InvalidOperation:
        raise ConfigError(f"{where}: {value!r} is not a decimal") from None
    if not d.is_finite():
        raise ConfigError(f"{where}: {value!r} is not finite")
    minor = d.scaleb(scale)
    if minor != minor.to_integral_value():
        raise ConfigError(f"{where}: {value!r} has more than {scale} decimal places")
    return int(minor)


def _known(name: Any, by_name: dict[str, FieldSpec], where: str) -> str:
    if name not in by_name:
        raise ConfigError(f"{where}: unknown field name {name!r}")
    return name


def _snake(value: Any, where: str) -> str:
    if not isinstance(value, str) or not _SNAKE.match(value):
        raise ConfigError(f"{where}: {value!r} is not a snake_case name")
    return value


def _physical(value: Any, where: str) -> str:
    """Physical column names are matched like headers: trim + lower-case, must stay non-empty."""
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{where}: physical column name {value!r} is empty")
    return value.strip().lower()


def _str(value: Any, where: str) -> str:
    if not isinstance(value, str) or not value:
        raise ConfigError(f"{where} must be a non-empty string")
    return value


def _bool(value: Any, where: str) -> bool:
    if not isinstance(value, bool):
        raise ConfigError(f"{where} must be true or false")
    return value


def _list(value: Any, where: str) -> list[Any]:
    if not isinstance(value, list):
        raise ConfigError(f"{where} must be a list")
    return value


def _no_unknown_keys(obj: dict[str, Any], allowed: set[str], where: str) -> None:
    unknown = sorted(set(obj) - allowed)
    if unknown:
        raise ConfigError(f"{where}: unknown key(s) {', '.join(map(repr, unknown))}")


def _no_duplicates(items: Any, where: str) -> None:
    seen: set[Any] = set()
    for item in items:
        if item in seen:
            raise ConfigError(f"{where}: duplicate {item!r}")
        seen.add(item)
