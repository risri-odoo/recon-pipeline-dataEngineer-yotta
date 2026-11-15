import json
import re
from collections import OrderedDict

import pytest

from recon.service.config import ConfigError, load_config, parse_config


def field(raw, name):
    return next(f for f in raw["fields"] if f["name"] == name)


def test_shipped_config_loads(config):
    assert config.business_key == ("order_id",)
    assert config.version_field == "updated_at"
    assert [f.name for f in config.fields] == [
        "order_id", "customer_id", "order_ts", "status", "amount", "currency", "discount", "updated_at"]
    assert config.columns_for("target")["customer_id"] == "cust_id"
    assert config.field("amount").tolerance_minor == 1
    assert config.field("amount").min_minor == 0
    assert config.field("discount").required is False
    assert config.reference.key == "customer_id"
    assert config.aggregates.sum_fields == ("amount", "discount")
    assert len(config.config_sha256) == 64


@pytest.mark.parametrize("physical, expected", [
    ("Order ID", "order id"),
    ("orderTimestamp", "ordertimestamp"),
    ("  Total-Amount ", "total-amount"),
])
def test_physical_names_relaxed_and_normalized(raw_config, physical, expected):
    raw_config["columns"]["target"]["order_ts"] = physical
    assert parse_config(raw_config).columns_for("target")["order_ts"] == expected


def test_logical_names_stay_snake_case(raw_config):
    field(raw_config, "order_ts")["name"] = "orderTimestamp"
    with pytest.raises(ConfigError, match="not a snake_case name"):
        parse_config(raw_config)


def test_config_is_frozen(config):
    with pytest.raises(AttributeError):
        config.business_key = "x"


def test_sha256_stable_across_key_order(raw_config):
    reordered = json.loads(json.dumps(raw_config), object_pairs_hook=lambda p: OrderedDict(reversed(p)))
    assert list(reordered) != list(raw_config)
    assert parse_config(reordered).config_sha256 == parse_config(raw_config).config_sha256


def test_sha256_includes_defaults(raw_config):
    explicit = json.loads(json.dumps(raw_config))
    field(explicit, "order_ts")["required"] = True
    field(explicit, "order_id").update(pattern=None, reference=False)
    assert parse_config(explicit).config_sha256 == parse_config(raw_config).config_sha256


def test_sha256_changes_with_content(raw_config):
    before = parse_config(raw_config).config_sha256
    field(raw_config, "amount")["tolerance"] = "0.02"
    assert parse_config(raw_config).config_sha256 != before


def test_load_missing_file(tmp_path):
    with pytest.raises(ConfigError, match="not found"):
        load_config(tmp_path / "nope.json")


def test_load_invalid_json(tmp_path):
    p = tmp_path / "bad.json"
    p.write_text("{not json", encoding="utf-8")
    with pytest.raises(ConfigError, match="not valid JSON"):
        load_config(p)


def _drop_key(raw):
    del raw["business_key"]


CASES = [
    ("unknown top-level key", lambda r: r.update(extra=1), "unknown key"),
    ("missing top-level key", _drop_key, "missing required key 'business_key'"),
    ("unknown field type", lambda r: field(r, "status").update(type="money"), "unknown type"),
    ("unknown field attribute", lambda r: field(r, "amount").update(upper=True), "unknown key"),
    ("non-snake logical name", lambda r: field(r, "order_id").update(name="OrderId"), "not a snake_case name"),
    ("duplicate field name", lambda r: r["fields"].append(dict(field(r, "status"))), "duplicate field name"),
    ("unknown business_key", lambda r: r.update(business_key="order_no"), "business_key: unknown field name"),
    ("business key not required", lambda r: field(r, "order_id").update(required=False), "must be required"),
    ("unknown version_field", lambda r: r.update(version_field="modified"), "version_field: unknown field name"),
    ("version field not timestamp", lambda r: r.update(version_field="status"), "must be of type timestamp"),
    ("unknown compare field", lambda r: r["compare_fields"].append("colour"), "compare_fields: unknown field name"),
    ("key in compare fields", lambda r: r["compare_fields"].append("order_id"), "must not contain key/version"),
    ("logical field not mapped", lambda r: r["columns"]["target"].pop("discount"), "not mapped: discount"),
    ("unknown logical field in map", lambda r: r["columns"]["source"].update(colour="colour"), "unknown logical field 'colour'"),
    ("empty physical name", lambda r: r["columns"]["target"].update(amount="   "), "physical column name '   ' is empty"),
    ("non-string physical name", lambda r: r["columns"]["target"].update(amount=7), "physical column name 7 is empty"),
    ("duplicate physical name", lambda r: r["columns"]["target"].update(discount="total_amount"), "duplicate 'total_amount'"),
    ("duplicate after normalization",
     lambda r: r["columns"]["target"].update(amount="Order_ID", order_id=" order_id "), "duplicate 'order_id'"),
    ("duplicate reference physical name",
     lambda r: r["reference"]["columns"].update(customer_name=" Customer_ID"), "duplicate 'customer_id'"),
    ("missing side map", lambda r: r["columns"].pop("target"), "columns.target must be an object"),
    ("sum field not decimal", lambda r: r["aggregates"]["sum_fields"].append("status"), "must be of type decimal"),
    ("unknown group_by", lambda r: r["aggregates"]["group_by"].append("region"), "group_by: unknown field name"),
    ("float tolerance", lambda r: field(r, "amount").update(tolerance=0.01), "decimal string"),
    ("tolerance beyond scale", lambda r: field(r, "amount").update(tolerance="0.001"), "more than 2 decimal places"),
    ("negative tolerance", lambda r: field(r, "amount").update(tolerance="-0.01"), "tolerance must be >= 0"),
    ("empty enum", lambda r: field(r, "status").update(values=[]), "must not be empty"),
    ("lower-case enum value", lambda r: field(r, "status").update(values=["placed"]), "upper-case"),
    ("bad regex", lambda r: field(r, "currency").update(pattern="["), "invalid pattern"),
    ("required not bool", lambda r: field(r, "amount").update(required="yes"), "must be true or false"),
    ("reference field without section", lambda r: r.pop("reference"), "no 'reference' section"),
    ("reference key not a column", lambda r: r["reference"].update(key="cust"), "not a column in reference.columns"),
]


@pytest.mark.parametrize("mutate, message", [(m, msg) for _, m, msg in CASES], ids=[c[0] for c in CASES])
def test_config_validation_errors(raw_config, mutate, message):
    mutate(raw_config)
    with pytest.raises(ConfigError, match=re.escape(message)):
        parse_config(raw_config)
