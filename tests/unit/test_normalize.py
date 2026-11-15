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
    ("9999999999.99", 999999999999),  # the configured max (P1)
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


@pytest.mark.parametrize("field, raw, code", [
    ("amount", "NULL", "BAD_TYPE"),
    ("discount", "null", "BAD_TYPE"),  # optional field: a literal is a value, not an empty -> NULL
    ("order_ts", "None", "BAD_TYPE"),
    ("status", "NULL", "BAD_ENUM"),
])
def test_literal_null_strings_are_values_not_null(config, field, raw, code):
    assert err(config, field, raw) == code  # DESIGN 7.1 #3


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


# --------------------------------------------------------------------------- step 9: QA-pass edge cases (P1-P16)


@pytest.mark.parametrize("field, raw, expected", [
    ("amount", "9999999999.99", 999999999999),   # max exactly
    ("discount", "9999999999.99", 999999999999),
])
def test_money_max_is_inclusive(config, field, raw, expected):
    assert ok(config, field, raw) == expected


@pytest.mark.parametrize("field, raw", [
    ("amount", "10000000000.00"),               # max + 0.01
    ("discount", "10000000000.00"),
    ("amount", "99999999999999999.99"),         # beyond SQLite INTEGER: a row reject, never a crash
    ("amount", "1" + "0" * 40),
])
def test_money_above_max_is_out_of_range(config, field, raw):
    assert err(config, field, raw) == "OUT_OF_RANGE"


def test_money_without_configured_bounds_uses_the_supported_limit():
    from recon.service.config import MONEY_LIMIT_MINOR, FieldSpec
    spec = FieldSpec("x", "decimal", True, scale=2)  # no min, no max
    assert normalize_value(spec, "92233720368.54") == MONEY_LIMIT_MINOR
    assert normalize_value(spec, "-92233720368.54") == -MONEY_LIMIT_MINOR
    for raw in ("92233720368.55", "-92233720368.55"):
        with pytest.raises(NormalizeError) as info:
            normalize_value(spec, raw)
        assert info.value.code == "OUT_OF_RANGE" and "supported" in info.value.detail


@pytest.mark.parametrize("raw, expected", [
    ("straße", "STRAßE"),      # str.upper() would give STRASSE
    ("café-1", "CAFé-1"),
    ("ǆ1", "ǆ1"),              # title-case digraphs untouched
    ("abc-xyz", "ABC-XYZ"),
])
def test_upper_case_touches_ascii_letters_only(config, raw, expected):
    assert ok(config, "order_id", raw) == expected
    assert ok(config, "customer_id", raw) == expected


@pytest.mark.parametrize("raw", ["ſhipped", "ſHIPPED", "placéd"])
def test_enum_upper_case_is_ascii_only(config, raw):
    assert err(config, "status", raw) == "BAD_ENUM"  # str.upper() would turn 'ſhipped' into SHIPPED


@pytest.mark.parametrize("raw", ["9999-12-31T23:59:59-01:00", "0001-01-01T00:00:00+01:00"])
def test_utc_conversion_outside_year_range_is_bad_type(config, raw):
    assert err(config, "updated_at", raw) == "BAD_TYPE"


def test_nul_byte_is_kept_in_text_and_breaks_a_timestamp(config):
    assert ok(config, "order_id", "o\x001") == "O\x001"
    assert err(config, "updated_at", "2026-01-01T01:00:00Z\x00") == "BAD_FORMAT"
