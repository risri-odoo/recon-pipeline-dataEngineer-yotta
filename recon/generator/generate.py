"""Generates deterministic source/target/reference datasets with seeded discrepancies.

The expected answers are built ONLY from this module's own bookkeeping of what it injected. This
module must never import pipeline code (recon.service / recon.repository): tests compare the
pipeline against these answers, so deriving them from the pipeline would make the tests circular.

Everything is driven by one `random.Random(seed)`; money is handled as integer minor units only.
"""

from __future__ import annotations

import csv
import io
import json
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

GENERATOR_VERSION = 1

STATUSES = ("PLACED", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED")
# The status an order had one step earlier in its life (used for stale / superseded versions).
PREVIOUS_STATUS = {"SHIPPED": "PLACED", "DELIVERED": "SHIPPED", "CANCELLED": "PLACED", "RETURNED": "DELIVERED"}
CURRENCIES = ("EUR", "GBP", "INR", "USD")

LOGICAL_FIELDS = ("order_id", "customer_id", "order_ts", "status", "amount", "currency", "discount", "updated_at")
SOURCE_HEADER = LOGICAL_FIELDS
TARGET_HEADER = ("order_id", "cust_id", "order_timestamp", "order_status", "total_amount",
                 "currency_code", "discount_amount", "last_modified")
CUSTOMER_HEADER = ("customer_id", "customer_name")

# Scenario -> rate in per-mille of N (integer math, no float). Order is fixed: it decides how the
# single rng.sample is sliced into buckets, so changing it changes every generated dataset.
SCENARIOS: tuple[tuple[str, int], ...] = (
    ("missing_in_target", 5),
    ("extra_in_target", 3),
    ("amount_mismatch", 5),
    ("within_tolerance", 5),
    ("cosmetic", 10),
    ("target_stale", 3),
    ("null_one_side", 2),
    ("null_both", 20),
    ("src_exact_dup", 3),
    ("src_out_of_order", 3),
    ("src_conflict", 1),
    ("src_invalid_ref", 2),
    ("src_missing_required", 2),
    ("src_bad_type", 2),
    ("src_malformed", 1),
    ("tgt_bad_type", 1),
    ("tgt_exact_dup", 1),
)
# extra_in_target gets brand-new keys; every other scenario takes a bucket of base keys.
NEW_KEY_SCENARIOS = frozenset({"extra_in_target"})

KEY_STATUSES = ("CONFLICT", "EXTRA_IN_TARGET", "MATCHED", "MISMATCHED", "MISSING_IN_TARGET",
                "SOURCE_REJECTED", "TARGET_REJECTED")
ROW_REJECT_CODES = ("BAD_ENUM", "BAD_FORMAT", "BAD_TYPE", "INVALID_REFERENCE", "MALFORMED_ROW",
                    "MISSING_REQUIRED", "OUT_OF_RANGE")

# Expected key status per scenario (DESIGN 4.5). Keys in no scenario are MATCHED.
SCENARIO_KEY_STATUS = {
    "missing_in_target": "MISSING_IN_TARGET",
    "extra_in_target": "EXTRA_IN_TARGET",
    "amount_mismatch": "MISMATCHED",
    "within_tolerance": "MATCHED",
    "cosmetic": "MATCHED",
    "target_stale": "MISMATCHED",
    "null_one_side": "MISMATCHED",
    "null_both": "MATCHED",
    "src_exact_dup": "MATCHED",
    "src_out_of_order": "MATCHED",
    "src_conflict": "CONFLICT",
    "src_invalid_ref": "SOURCE_REJECTED",
    "src_missing_required": "SOURCE_REJECTED",
    "src_bad_type": "SOURCE_REJECTED",
    # A MALFORMED_ROW has no trusted order_id, so the source side has nothing for the key and the
    # (valid) target row makes it EXTRA_IN_TARGET; the malformed line itself fails recon separately.
    "src_malformed": "EXTRA_IN_TARGET",
    "tgt_bad_type": "TARGET_REJECTED",
    "tgt_exact_dup": "MATCHED",
}
# Expected field diffs per scenario: (logical field, diff_type).
SCENARIO_FIELD_DIFF = {
    "amount_mismatch": ("amount", "VALUE_MISMATCH"),
    "target_stale": ("status", "TARGET_STALE"),
    "null_one_side": ("discount", "NULL_MISMATCH"),
}

# Injected bad values: (logical field, raw text, expected reason code).
BAD_TYPE_VALUES = (("amount", "abc", "BAD_TYPE"), ("order_ts", "yesterday", "BAD_TYPE"))
MISSING_REQUIRED_FIELDS = ("customer_id", "order_ts", "status", "amount", "currency")

_EPOCH = datetime(2026, 1, 1, tzinfo=timezone.utc)
_TS_WINDOW_SECONDS = 300 * 24 * 3600

# Dispositions recorded per written data line.
ACCEPTED = "ACCEPTED"
DUPLICATE_EXACT = "DUPLICATE_EXACT"
SUPERSEDED = "SUPERSEDED"
CONFLICTING_DUPLICATE = "CONFLICTING_DUPLICATE"


@dataclass
class Order:
    """One version of an order in logical form. Money in minor units; discount None = empty."""

    order_id: str
    customer_id: str
    order_ts: datetime
    status: str
    amount: int
    currency: str
    discount: int | None
    updated_at: datetime

    def copy(self, **changes) -> "Order":
        values = dict(self.__dict__)
        values.update(changes)
        return Order(**values)


@dataclass
class Line:
    """One data line to write, plus the generator's bookkeeping of what the pipeline must do with it."""

    cells: list[str]
    disposition: str  # ACCEPTED | DUPLICATE_EXACT | SUPERSEDED | CONFLICTING_DUPLICATE | ROW_REJECT:<code>
    order: Order | None = None  # the logical values; set for ACCEPTED lines (used for sums)


@dataclass
class Dataset:
    source_csv: bytes
    target_csv: bytes
    customers_csv: bytes
    expected: dict
    files: dict[str, bytes] = field(init=False)

    def __post_init__(self) -> None:
        self.files = {"source.csv": self.source_csv, "target.csv": self.target_csv,
                      "customers.csv": self.customers_csv}


def bucket_sizes(n: int) -> dict[str, int]:
    """Bucket size per scenario: floor(n * rate), at least 1."""
    return {name: max(1, n * per_mille // 1000) for name, per_mille in SCENARIOS}


def min_n() -> int:
    """Smallest N whose base keys can hold every base-key bucket."""
    return sum(1 for name, _ in SCENARIOS if name not in NEW_KEY_SCENARIOS)


def generate(n: int, seed: int) -> Dataset:
    if n < min_n():
        raise ValueError(f"n must be >= {min_n()} so every scenario gets at least one key (got {n})")
    rng = random.Random(seed)
    sizes = bucket_sizes(n)
    n_extra = sizes["extra_in_target"]
    width = max(6, len(str(n + n_extra)))

    n_customers = max(1, n // 50)
    customers = [f"C{i:05d}" for i in range(1, n_customers + 1)]

    base = [_random_order(rng, f"ORD-{i:0{width}d}", customers) for i in range(1, n + 1)]

    # Disjoint buckets: one sample of distinct base indices, sliced in fixed scenario order.
    base_scenarios = [(name, sizes[name]) for name, _ in SCENARIOS if name not in NEW_KEY_SCENARIOS]
    picked = rng.sample(range(n), sum(size for _, size in base_scenarios))
    scenario_of: dict[str, str] = {}
    buckets: dict[str, list[str]] = {}
    pos = 0
    for name, size in base_scenarios:
        keys = [base[i].order_id for i in picked[pos:pos + size]]
        pos += size
        buckets[name] = sorted(keys)
        for key in keys:
            scenario_of[key] = name

    extra = [_random_order(rng, f"ORD-{i:0{width}d}", customers) for i in range(n + 1, n + n_extra + 1)]
    buckets["extra_in_target"] = [o.order_id for o in extra]

    source_lines: list[Line] = []
    target_lines: list[Line] = []
    for order in base:  # base is already in business-key order
        src, tgt = _inject(rng, scenario_of.get(order.order_id), order)
        source_lines.extend(src)
        target_lines.extend(tgt)
    for order in extra:
        target_lines.append(Line(_target_cells(order), ACCEPTED, order))

    rng.shuffle(target_lines)

    customer_rows = [[c, f"Customer {c[1:]}"] for c in customers]
    expected = _expected(n, seed, n_extra, customers, buckets, scenario_of, source_lines, target_lines)
    return Dataset(
        source_csv=_csv_bytes(SOURCE_HEADER, [line.cells for line in source_lines]),
        target_csv=_csv_bytes(TARGET_HEADER, [line.cells for line in target_lines]),
        customers_csv=_csv_bytes(CUSTOMER_HEADER, customer_rows),
        expected=expected,
    )


def write_dataset(dataset: Dataset, name: str, generated_dir: Path, expected_dir: Path) -> tuple[Path, Path]:
    """Write <generated_dir>/<name>/{source,target,customers}.csv and <expected_dir>/<name>.json."""
    data_dir = Path(generated_dir) / name
    data_dir.mkdir(parents=True, exist_ok=True)
    for filename, payload in dataset.files.items():
        (data_dir / filename).write_bytes(payload)
    expected_dir = Path(expected_dir)
    expected_dir.mkdir(parents=True, exist_ok=True)
    expected_path = expected_dir / f"{name}.json"
    expected_path.write_bytes(expected_json_bytes(dataset.expected))
    return data_dir, expected_path


def expected_json_bytes(expected: dict) -> bytes:
    return (json.dumps(expected, sort_keys=True, indent=2, ensure_ascii=False) + "\n").encode("utf-8")


# --------------------------------------------------------------------------- base data


def _random_order(rng: random.Random, order_id: str, customers: list[str]) -> Order:
    order_ts = _EPOCH + timedelta(seconds=rng.randrange(_TS_WINDOW_SECONDS))
    amount = rng.randrange(100, 100_000)  # 1.00 .. 999.99
    return Order(
        order_id=order_id,
        customer_id=rng.choice(customers),
        order_ts=order_ts,
        status=rng.choice(STATUSES),
        amount=amount,
        currency=rng.choice(CURRENCIES),
        discount=rng.randrange(0, amount // 10 + 1),
        # >= 2h after order_ts, so an "older" version 1h earlier is still after order_ts.
        updated_at=order_ts + timedelta(seconds=rng.randrange(2 * 3600, 72 * 3600)),
    )


def _with_history(order: Order) -> Order:
    """Make sure the order's status has a predecessor (PLACED has none)."""
    return order if order.status in PREVIOUS_STATUS else order.copy(status="SHIPPED")


# --------------------------------------------------------------------------- scenario injection


def _inject(rng: random.Random, scenario: str | None, order: Order) -> tuple[list[Line], list[Line]]:
    """Return (source lines, target lines) for one base key, with their expected dispositions."""
    if scenario is None:
        return [_src_ok(order)], [_tgt_ok(order)]

    if scenario == "missing_in_target":
        return [_src_ok(order)], []

    if scenario == "amount_mismatch":
        delta = rng.randrange(100, 5001)  # >= 1.00
        if rng.randrange(2) == 0 and order.amount - delta >= 0:
            delta = -delta
        return [_src_ok(order)], [_tgt_ok(order.copy(amount=order.amount + delta))]

    if scenario == "within_tolerance":
        return [_src_ok(order)], [_tgt_ok(order.copy(amount=order.amount + 1))]

    if scenario == "cosmetic":
        cells = _target_cells(order)
        cells[2] = _fmt_ts_space(order.order_ts)
        cells[3] = f" {order.status.lower()}  "
        cells[7] = _fmt_ts_space(order.updated_at)
        return [_src_ok(order)], [Line(cells, ACCEPTED, order)]

    if scenario == "target_stale":
        order = _with_history(order)
        stale = order.copy(status=PREVIOUS_STATUS[order.status],
                           updated_at=order.updated_at - timedelta(seconds=rng.randrange(60, 3600)))
        return [_src_ok(order)], [_tgt_ok(stale)]

    if scenario == "null_one_side":
        return [_src_ok(order)], [_tgt_ok(order.copy(discount=None))]

    if scenario == "null_both":
        order = order.copy(discount=None)
        return [_src_ok(order)], [_tgt_ok(order)]

    if scenario == "src_exact_dup":
        return [_src_ok(order), Line(_source_cells(order), DUPLICATE_EXACT)], [_tgt_ok(order)]

    if scenario == "src_out_of_order":
        order = _with_history(order)
        older = order.copy(status=PREVIOUS_STATUS[order.status],
                           updated_at=order.updated_at - timedelta(seconds=rng.randrange(60, 3600)))
        # Newer version first, older version written AFTER it.
        return [_src_ok(order), Line(_source_cells(older), SUPERSEDED)], [_tgt_ok(order)]

    if scenario == "src_conflict":
        other = order.copy(amount=order.amount + rng.randrange(100, 5001))  # same updated_at, other payload
        return ([Line(_source_cells(order), CONFLICTING_DUPLICATE),
                 Line(_source_cells(other), CONFLICTING_DUPLICATE)],
                [_tgt_ok(order)])

    if scenario == "src_invalid_ref":
        cells = _source_cells(order)
        cells[1] = f"CX{order.order_id[4:]}"  # never in the reference (reference ids are C<digits>)
        return [Line(cells, "ROW_REJECT:INVALID_REFERENCE")], [_tgt_ok(order)]

    if scenario == "src_missing_required":
        cells = _source_cells(order)
        cells[LOGICAL_FIELDS.index(rng.choice(MISSING_REQUIRED_FIELDS))] = ""
        return [Line(cells, "ROW_REJECT:MISSING_REQUIRED")], [_tgt_ok(order)]

    if scenario == "src_bad_type":
        field_name, raw, code = rng.choice(BAD_TYPE_VALUES)
        cells = _source_cells(order)
        cells[LOGICAL_FIELDS.index(field_name)] = raw
        return [Line(cells, f"ROW_REJECT:{code}")], [_tgt_ok(order)]

    if scenario == "src_malformed":
        cells = _source_cells(order) + ["UNEXPECTED_EXTRA_FIELD"]
        return [Line(cells, "ROW_REJECT:MALFORMED_ROW")], [_tgt_ok(order)]

    if scenario == "tgt_bad_type":
        field_name, raw, code = rng.choice(BAD_TYPE_VALUES)
        cells = _target_cells(order)
        cells[LOGICAL_FIELDS.index(field_name)] = raw
        return [_src_ok(order)], [Line(cells, f"ROW_REJECT:{code}")]

    if scenario == "tgt_exact_dup":
        return [_src_ok(order)], [_tgt_ok(order), Line(_target_cells(order), DUPLICATE_EXACT)]

    raise AssertionError(f"unhandled scenario {scenario!r}")


def _src_ok(order: Order) -> Line:
    return Line(_source_cells(order), ACCEPTED, order)


def _tgt_ok(order: Order) -> Line:
    return Line(_target_cells(order), ACCEPTED, order)


def _source_cells(order: Order) -> list[str]:
    return [order.order_id, order.customer_id, _fmt_ts(order.order_ts), order.status, _fmt_money(order.amount),
            order.currency, "" if order.discount is None else _fmt_money(order.discount), _fmt_ts(order.updated_at)]


def _target_cells(order: Order) -> list[str]:
    # Same logical order as the source; the target header carries the renamed columns.
    return _source_cells(order)


# --------------------------------------------------------------------------- expected answers


def _expected(n: int, seed: int, n_extra: int, customers: list[str], buckets: dict[str, list[str]],
              scenario_of: dict[str, str], source_lines: list[Line], target_lines: list[Line]) -> dict:
    key_status_counts = dict.fromkeys(KEY_STATUSES, 0)
    key_status_counts["MATCHED"] = n - len(scenario_of)
    for name, keys in buckets.items():
        key_status_counts[SCENARIO_KEY_STATUS[name]] += len(keys)

    field_diff_counts: dict[tuple[str, str], int] = {}
    for name, (field_name, diff_type) in SCENARIO_FIELD_DIFF.items():
        field_diff_counts[(field_name, diff_type)] = field_diff_counts.get((field_name, diff_type), 0) + len(buckets[name])

    unattributable = sum(1 for line in source_lines + target_lines if line.disposition == "ROW_REJECT:MALFORMED_ROW")
    all_matched = key_status_counts["MATCHED"] == n + n_extra
    return {
        "generator_version": GENERATOR_VERSION,
        "n": n,
        "seed": seed,
        "total_keys": n + n_extra,
        "bucket_sizes": {name: len(keys) for name, keys in sorted(buckets.items())},
        "sides": {"SOURCE": _side_summary(source_lines), "TARGET": _side_summary(target_lines),
                  "REFERENCE": {"rows_read": len(customers), "accepted": len(customers)}},
        "key_status_counts": key_status_counts,
        "field_diff_counts": [{"field_name": f, "diff_type": d, "count": c}
                              for (f, d), c in sorted(field_diff_counts.items())],
        "unattributable_rejects": unattributable,
        "recon_status": "PASS" if all_matched and unattributable == 0 else "FAIL",
        "scenario_keys": {name: keys for name, keys in sorted(buckets.items())},
    }


def _side_summary(lines: list[Line]) -> dict:
    rejected = dict.fromkeys(ROW_REJECT_CODES, 0)
    counts = {ACCEPTED: 0, DUPLICATE_EXACT: 0, SUPERSEDED: 0, CONFLICTING_DUPLICATE: 0}
    sum_amount = 0
    sum_discount = 0
    for line in lines:
        if line.disposition.startswith("ROW_REJECT:"):
            rejected[line.disposition.split(":", 1)[1]] += 1
            continue
        counts[line.disposition] += 1
        if line.disposition == ACCEPTED:
            sum_amount += line.order.amount
            sum_discount += line.order.discount or 0
    return {
        "rows_read": len(lines),
        "accepted": counts[ACCEPTED],
        "rejected_by_reason": rejected,
        "duplicate_exact": counts[DUPLICATE_EXACT],
        "superseded": counts[SUPERSEDED],
        "conflicting_rows": counts[CONFLICTING_DUPLICATE],
        "sum_amount": _fmt_money(sum_amount),
        "sum_discount": _fmt_money(sum_discount),
    }


# --------------------------------------------------------------------------- formatting


def _fmt_money(minor: int) -> str:
    sign = "-" if minor < 0 else ""
    whole, frac = divmod(abs(minor), 100)
    return f"{sign}{whole}.{frac:02d}"


def _fmt_ts(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def _fmt_ts_space(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%d %H:%M:%S")


def _csv_bytes(header: tuple[str, ...], rows: list[list[str]]) -> bytes:
    buf = io.StringIO(newline="")
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(header)
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")
