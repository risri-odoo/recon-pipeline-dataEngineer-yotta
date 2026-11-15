"""Row-level validation into SILVER (DESIGN 4.3): every reject code, typed clean values, reference side."""

from pathlib import Path

import pytest

from recon.repository import silver_repo
from recon.service.validate import parse_record, payload_sha256
from tests.silver_helpers import SILVER_RUN_ID as RUN, SRC_HEADER, order_row

ROOT = Path(__file__).resolve().parents[2]


def quarantine(conn, side="SOURCE"):
    return silver_repo.fetch_quarantine(conn, RUN, side)


def clean(conn, side="SOURCE"):
    return silver_repo.fetch_clean_records(conn, RUN, side)


@pytest.mark.parametrize("line, code, field, key", [
    (order_row() + ",extra", "MALFORMED_ROW", None, None),
    ("O1,C1,2026-01-01T00:00:00Z,PLACED", "MALFORMED_ROW", None, None),
    ('O1,"C1"x,2026-01-01T00:00:00Z,PLACED,10.00,USD,1.00,2026-01-01T01:00:00Z', "MALFORMED_ROW", None, None),
    (order_row(amount=""), "MISSING_REQUIRED", "amount", "O1"),
    (order_row(order_id="  "), "MISSING_REQUIRED", "order_id", None),
    (order_row(amount="abc"), "BAD_TYPE", "amount", "O1"),
    (order_row(order_ts="yesterday"), "BAD_TYPE", "order_ts", "O1"),
    (order_row(amount="1.234"), "BAD_FORMAT", "amount", "O1"),
    (order_row(currency="US"), "BAD_FORMAT", "currency", "O1"),
    (order_row(order_ts="15/11/2026"), "BAD_FORMAT", "order_ts", "O1"),
    (order_row(status="LOST"), "BAD_ENUM", "status", "O1"),
    (order_row(amount="-1.00"), "OUT_OF_RANGE", "amount", "O1"),
    (order_row(discount="-0.01"), "OUT_OF_RANGE", "discount", "O1"),
    (order_row(customer_id="C9"), "INVALID_REFERENCE", "customer_id", "O1"),
])
def test_every_row_reject_code(run_silver, line, code, field, key):
    conn, result = run_silver(source=[line])
    [q] = quarantine(conn)
    assert (q["reason_code"], q["field_name"], q["order_id"]) == (code, field, key)
    assert q["category"] == "ROW_REJECT"
    assert (q["line_no"], q["raw_text"]) == (2, line)
    assert q["source_file"].endswith("source.csv")
    assert q["detail"]
    if code == "MALFORMED_ROW":
        assert q["updated_at"] is None  # no trusted fields
    else:
        assert q["updated_at"] == "2026-01-01T01:00:00Z"
    assert clean(conn) == []
    assert result.sides["SOURCE"].counts.rejected == 1


def test_malformed_row_details(run_silver):
    conn, _ = run_silver(source=[order_row() + ",x", '"O1"x,C1,a,b,c,d,e,f'])
    details = [q["detail"] for q in quarantine(conn)]
    assert details[0] == "expected 8 fields, got 9"
    assert details[1].startswith("CSV quoting error")


def test_first_failure_is_reason_and_detail_lists_all(run_silver):
    conn, _ = run_silver(source=[order_row(status="LOST", amount="abc", customer_id="C9")])
    [q] = quarantine(conn)
    assert q["reason_code"] == "BAD_ENUM" and q["field_name"] == "status"  # status precedes amount
    assert "status:BAD_ENUM" in q["detail"] and "amount:BAD_TYPE" in q["detail"]
    assert "INVALID_REFERENCE" not in q["detail"]  # reference is checked only on otherwise valid rows
    assert q["detail"].index("status:") < q["detail"].index("amount:")


def test_reject_keeps_key_but_not_unparsed_version(run_silver):
    conn, _ = run_silver(source=[order_row(order_id=" o1 ", updated_at="soon", amount="x")])
    [q] = quarantine(conn)
    assert q["order_id"] == "O1" and q["updated_at"] is None


def test_valid_row_typed_canonical_values(run_silver, config):
    line = order_row(order_id=" o-0012 ", customer_id="c1", order_ts="2026-01-01 05:30:00.999+05:30",
                     status="shipped", amount="10.5", currency="usd", discount="", updated_at="2026-01-02T00:00:00")
    conn, result = run_silver(source=[line])
    [row] = clean(conn)
    assert row == {
        "run_id": RUN, "side": "SOURCE", "order_id": "O-0012", "customer_id": "C1",
        "order_ts": "2026-01-01T00:00:00Z", "status": "SHIPPED", "amount_minor": 1050, "currency": "USD",
        "discount_minor": None, "updated_at": "2026-01-02T00:00:00Z",
        "payload_sha256": result.sides["SOURCE"].winners[0].payload_sha256,
        "source_file": row["source_file"], "line_no": 2,
    }
    assert isinstance(row["amount_minor"], int)
    assert row["source_file"].endswith("source.csv")
    assert quarantine(conn) == []


def test_target_uses_target_column_mapping(run_silver):
    tgt_header = "last_modified,order_id,cust_id,order_timestamp,order_status,total_amount,currency_code,discount_amount"
    conn, _ = run_silver(target=["2026-01-01T01:00:00Z,O1,C2,2026-01-01T00:00:00Z,PLACED,7.00,EUR,"],
                         tgt_header=tgt_header)
    [row] = clean(conn, "TARGET")
    assert (row["customer_id"], row["amount_minor"], row["currency"], row["discount_minor"]) == ("C2", 700, "EUR", None)


def test_missing_optional_column_gives_null_discount(run_silver):
    header = "order_id,customer_id,order_ts,status,amount,currency,updated_at"
    conn, _ = run_silver(source=["O1,C1,2026-01-01T00:00:00Z,PLACED,10.00,USD,2026-01-01T01:00:00Z"],
                         src_header=header)
    [row] = clean(conn)
    assert row["discount_minor"] is None


def test_multiline_quoted_record_is_valid(run_silver):
    conn, _ = run_silver(reference=['C1,"Alice\nSmith"', "C2,Bob"], source=[order_row(customer_id="C2")])
    assert [r["customer_name"] for r in silver_repo.fetch_ref_customers(conn, RUN)] == ["Alice\nSmith", "Bob"]
    assert len(clean(conn)) == 1


def test_reference_rejects_duplicates_and_normalization(run_silver):
    conn, result = run_silver(
        reference=[" c1 ,Alice", ",Nobody", "C1,Alice again", "C2,Bob,extra", "C3,"],
        source=[order_row(customer_id="C1"), order_row(order_id="O2", customer_id="C3"),
                order_row(order_id="O3", customer_id="C2")],
    )
    refs = silver_repo.fetch_ref_customers(conn, RUN)
    assert [(r["customer_id"], r["customer_name"]) for r in refs] == [("C1", "Alice"), ("C3", None)]
    ref_q = quarantine(conn, "REFERENCE")
    assert [(q["line_no"], q["reason_code"], q["field_name"]) for q in ref_q] == [
        (3, "MISSING_REQUIRED", "customer_id"), (5, "MALFORMED_ROW", None)]  # duplicate C1 is not quarantined
    assert result.reference.ids == frozenset({"C1", "C3"})
    assert [r["order_id"] for r in clean(conn)] == ["O1", "O2"]
    [q] = quarantine(conn)
    assert (q["order_id"], q["reason_code"]) == ("O3", "INVALID_REFERENCE")


def test_empty_reference_rejects_every_order(run_silver):
    conn, _ = run_silver(reference=[], source=[order_row()])
    assert [q["reason_code"] for q in quarantine(conn)] == ["INVALID_REFERENCE"]


def test_parse_record():
    assert parse_record('a,"b,c",d') == ["a", "b,c", "d"]
    assert parse_record('a,"b\r\nc"') == ["a", "b\r\nc"]
    assert parse_record('a,"b"c').startswith("CSV quoting error")


def test_payload_hash_null_differs_from_zero(config):
    base = {"order_id": "O1", "customer_id": "C1", "order_ts": "t", "status": "PLACED", "amount": 1000,
            "currency": "USD", "discount": None, "updated_at": "u"}
    assert payload_sha256(config, base) != payload_sha256(config, base | {"discount": 0})
    assert payload_sha256(config, base) == payload_sha256(config, dict(reversed(base.items())))


def test_header_constant_matches_shipped_sample():
    with open(ROOT / "data" / "sample" / "source.csv", encoding="utf-8") as fh:
        assert fh.readline().rstrip("\n") == SRC_HEADER


def test_field_over_the_csv_size_limit_is_malformed_row_with_a_clear_detail(run_silver):
    import csv
    line = order_row(customer_id="C" + "x" * csv.field_size_limit())
    conn, _ = run_silver(source=[line])
    [q] = quarantine(conn)
    assert (q["reason_code"], q["order_id"], q["raw_text"]) == ("MALFORMED_ROW", None, line)
    assert q["detail"] == f"a field exceeds the CSV field size limit ({csv.field_size_limit()} characters)"


def test_nul_byte_in_the_key_is_kept(run_silver):
    conn, _ = run_silver(source=[order_row(order_id="O\x001")])
    assert [r["order_id"] for r in clean(conn)] == ["O\x001"]
