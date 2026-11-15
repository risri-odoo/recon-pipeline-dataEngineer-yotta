"""Shared constants and line builders for SILVER tests."""

SRC_HEADER = "order_id,customer_id,order_ts,status,amount,currency,discount,updated_at"
TGT_HEADER = "order_id,cust_id,order_timestamp,order_status,total_amount,currency_code,discount_amount,last_modified"
REF_HEADER = "customer_id,customer_name"
SILVER_RUN_ID = "silver-a1"


def order_row(order_id="O1", customer_id="C1", order_ts="2026-01-01T00:00:00Z", status="PLACED",
              amount="10.00", currency="USD", discount="1.00", updated_at="2026-01-01T01:00:00Z") -> str:
    """One CSV data line in logical-field order (same column order on both sides)."""
    return ",".join([order_id, customer_id, order_ts, status, amount, currency, discount, updated_at])
