import pytest

from recon.service.normalize import NormalizeError, format_minor, normalize_value, validate_row


def ok(config, field, raw):
    return normalize_value(config.field(field), raw)


def err(config, field, raw):
    with pytest.raises(NormalizeError) as info:
        normalize_value(config.field(field), raw)
    return info.value.code


@pytest.mark.parametrize("field, raw, expected", [
    ("order_id", "  ab-1 ", "AB-1"),
    ("order_id", "0012", "0012"),
    ("customer_id", "c001", "C001"),
    ("customer_id", "NULL", "NULL"),
    ("customer_id", "null", "NULL"),
])
def test_str_ok(config, field, raw, expected):
    assert ok(config, field, raw) == expected


@pytest.mark.parametrize("raw", ["", "   ", "\t", None])
def test_str_required_empty_is_missing_required(config, raw):
    assert err(config, "order_id", raw) == "MISSING_REQUIRED"


@pytest.mark.parametrize("raw", ["", "   ", None])
def test_optional_empty_is_null(config, raw):
    assert ok(config, "discount", raw) is None


@pytest.mark.parametrize("raw, expected", [("inr", "INR"), (" usd ", "USD"), ("EUR", "EUR")])
def test_currency_ok(config, raw, expected):
    assert ok(config, "currency", raw) == expected


@pytest.mark.parametrize("raw", ["IN", "INRS", "1NR", "I N"])
def test_currency_bad_format(config, raw):
    assert err(config, "currency", raw) == "BAD_FORMAT"


@pytest.mark.parametrize("raw, expected", [(" shipped ", "SHIPPED"), ("Placed", "PLACED"), ("RETURNED", "RETURNED")])
def test_enum_ok(config, raw, expected):
    assert ok(config, "status", raw) == expected


@pytest.mark.parametrize("raw", ["LOST", "SHIP"])
def test_enum_bad(config, raw):
    assert err(config, "status", raw) == "BAD_ENUM"


@pytest.mark.parametrize("raw, expected", [
    ("12.5", 1250),
    ("12.50", 1250),
    ("0", 0),
    ("-0.00", 0),
    ("1234567890123.99", 123456789012399),
    (" 7 ", 700),
])
def test_money_ok(config, raw, expected):
    value = ok(config, "amount", raw)
    assert value == expected and type(value) is int


@pytest.mark.parametrize("raw, code", [
    ("-1", "OUT_OF_RANGE"),
    ("-0.01", "OUT_OF_RANGE"),
    ("10.005", "BAD_FORMAT"),
    ("1,234.56", "BAD_FORMAT"),
    ("1.234,56", "BAD_FORMAT"),
    ("12,50", "BAD_FORMAT"),
    ("1e3", "BAD_FORMAT"),
    ("+5", "BAD_FORMAT"),
    (".5", "BAD_FORMAT"),
    ("5.", "BAD_FORMAT"),
    ("₹5", "BAD_FORMAT"),
    ("$5.00", "BAD_FORMAT"),
    ("abc", "BAD_TYPE"),
    ("NaN", "BAD_TYPE"),
    ("12abc", "BAD_TYPE"),
])
def test_money_errors(config, raw, code):
    assert err(config, "amount", raw) == code


@pytest.mark.parametrize("raw, expected", [
    ("2026-11-15T10:00:00Z", "2026-11-15T10:00:00Z"),
    ("2026-11-15 10:00:00", "2026-11-15T10:00:00Z"),
    ("2026-11-15T10:00:00", "2026-11-15T10:00:00Z"),
    ("2026-11-15T10:00:00+05:30", "2026-11-15T04:30:00Z"),
    ("2026-11-15T01:00:00+05:30", "2026-11-14T19:30:00Z"),
    ("2026-11-15T22:00:00-03:00", "2026-11-16T01:00:00Z"),
    ("2026-11-15 10:00:00.123", "2026-11-15T10:00:00Z"),
    ("2026-11-15T10:00:00.999999Z", "2026-11-15T10:00:00Z"),
    ("2024-02-29 00:00:00", "2024-02-29T00:00:00Z"),
])
def test_timestamp_ok(config, raw, expected):
    assert ok(config, "order_ts", raw) == expected


@pytest.mark.parametrize("raw, code", [
    ("2026-11-15", "BAD_FORMAT"),
    ("15/11/2026", "BAD_FORMAT"),
    ("2026-11-15T10:00", "BAD_FORMAT"),
    ("2026-02-30 10:00:00", "BAD_TYPE"),
    ("2026-11-15 25:00:00", "BAD_TYPE"),
    ("2026-11-15T10:00:00+24:00", "BAD_TYPE"),
    ("yesterday", "BAD_TYPE"),
])
def test_timestamp_errors(config, raw, code):
    assert err(config, "order_ts", raw) == code


@pytest.mark.parametrize("units, scale, expected", [
    (12345, 2, "123.45"),
    (0, 2, "0.00"),
    (5, 2, "0.05"),
    (-5, 2, "-0.05"),
    (-12345, 2, "-123.45"),
    (100, 2, "1.00"),
    (7, 0, "7"),
    (1234, 3, "1.234"),
])
def test_format_minor(units, scale, expected):
    assert format_minor(units, scale) == expected


GOOD_ROW = {
    "order_id": " o-0012 ",
    "customer_id": "c001",
    "order_ts": "2026-11-15 10:00:00",
    "status": "placed",
    "amount": "12.50",
    "currency": "inr",
    "discount": "",
    "updated_at": "2026-11-15T10:05:00+05:30",
}


def test_row_valid(config):
    result = validate_row(config, GOOD_ROW)
    assert result.ok and result.reason_code is None and result.detail == ""
    assert result.values == {
        "order_id": "O-0012", "customer_id": "C001", "order_ts": "2026-11-15T10:00:00Z",
        "status": "PLACED", "amount": 1250, "currency": "INR", "discount": None,
        "updated_at": "2026-11-15T04:35:00Z",
    }
    assert result.business_key == "O-0012" and result.version == "2026-11-15T04:35:00Z"


def test_row_missing_optional_column_is_null(config):
    row = {k: v for k, v in GOOD_ROW.items() if k != "discount"}
    assert validate_row(config, row).values["discount"] is None


def test_row_collects_all_failures_in_field_order(config):
    row = dict(GOOD_ROW, customer_id="", status="LOST", amount="1,234.56", currency="IN")
    result = validate_row(config, row)
    assert [(f.field, f.code) for f in result.failures] == [
        ("customer_id", "MISSING_REQUIRED"),
        ("status", "BAD_ENUM"),
        ("amount", "BAD_FORMAT"),
        ("currency", "BAD_FORMAT"),
    ]
    assert result.reason_code == "MISSING_REQUIRED"
    assert result.field_name == "customer_id"
    assert result.detail.split("; ")[0].startswith("customer_id:MISSING_REQUIRED:")
    assert len(result.detail.split("; ")) == 4
    assert result.business_key == "O-0012"
    assert result.version == "2026-11-15T04:35:00Z"


def test_row_unparsed_key_and_version_are_none(config):
    result = validate_row(config, dict(GOOD_ROW, order_id=" ", updated_at="yesterday"))
    assert result.business_key is None and result.version is None
    assert result.reason_code == "MISSING_REQUIRED"


def test_row_invalid_reference_only_when_otherwise_valid(config):
    refs = frozenset({"C999"})
    assert validate_row(config, GOOD_ROW, refs).reason_code == "INVALID_REFERENCE"
    assert validate_row(config, GOOD_ROW, frozenset({"C001"})).ok
    bad = validate_row(config, dict(GOOD_ROW, amount="abc"), refs)
    assert [f.code for f in bad.failures] == ["BAD_TYPE"]
