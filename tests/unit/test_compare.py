"""GOLD stage: key statuses, field diffs, aggregates, checks and recon status (DESIGN 4.5-4.8)."""

import json

import pytest

from recon.repository import bronze_repo, gold_repo, silver_repo
from recon.repository.db import transaction
from recon.service.compare import (CHECK_ORDER, InvariantViolation, compare_fields, reconcile)
from tests.gold_helpers import (GOLD_RUN_ID as RUN, SAMPLE_PATHS, gold_snapshot, reconcile_files,
                                swapped_config, swapped_paths)
from tests.silver_helpers import SILVER_RUN_ID, order_row

T1, T2, T3 = "2026-01-01T01:00:00Z", "2026-01-02T01:00:00Z", "2026-01-03T01:00:00Z"


def keys(conn):
    return {k["order_id"]: k for k in gold_repo.fetch_key_results(conn, RUN)}


def diffs(conn):
    return gold_repo.fetch_field_diffs(conn, RUN)


def aggregates(conn):
    return {(a["metric"], a["group_by"], a["group_value"]): a for a in gold_repo.fetch_aggregates(conn, RUN)}


def checks(conn):
    return {c["check_name"]: c for c in gold_repo.fetch_check_results(conn, RUN)}


# --------------------------------------------------------------------------- field_diff: money tolerance


@pytest.mark.parametrize("field, target_value, differs", [
    ("amount", "10.01", False),   # |delta| = 1 minor unit = tolerance 0.01 -> equal (inclusive)
    ("amount", "9.99", False),
    ("amount", "10.02", True),
    ("amount", "9.98", True),
    ("discount", "1.01", False),
    ("discount", "1.02", True),
])
def test_tolerance_boundary(run_gold, field, target_value, differs):
    conn, result = run_gold(source=[order_row(amount="10.00", discount="1.00")],
                            target=[order_row(**{field: target_value})])
    if not differs:
        assert keys(conn)["O1"]["key_status"] == "MATCHED"
        assert diffs(conn) == []
        return
    [d] = diffs(conn)
    source_value = "10.00" if field == "amount" else "1.00"
    expected_delta = int(source_value.replace(".", "")) - int(target_value.replace(".", ""))
    assert (d["field_name"], d["source_value"], d["target_value"], d["delta_minor"], d["diff_type"]) == \
        (field, source_value, target_value, expected_delta, "VALUE_MISMATCH")
    assert (keys(conn)["O1"]["key_status"], keys(conn)["O1"]["diff_count"]) == ("MISMATCHED", 1)


def test_null_vs_null_matches(run_gold):
    conn, result = run_gold(source=[order_row(discount="")], target=[order_row(discount="")])
    assert keys(conn)["O1"]["key_status"] == "MATCHED"
    assert diffs(conn) == []
    assert result.recon_status == "PASS"


@pytest.mark.parametrize("src, tgt", [("", "0.00"), ("0.00", ""), ("", "5.00")])
def test_null_vs_value_is_null_mismatch(run_gold, src, tgt):
    conn, _ = run_gold(source=[order_row(discount=src)], target=[order_row(discount=tgt)])
    [d] = diffs(conn)
    assert (d["field_name"], d["diff_type"], d["delta_minor"]) == ("discount", "NULL_MISMATCH", None)
    assert (d["source_value"], d["target_value"]) == (src or None, tgt or None)  # NULL is not 0


# --------------------------------------------------------------------------- field_diff: diff types


@pytest.mark.parametrize("target_updated_at, diff_type", [
    (T1, "TARGET_STALE"), (T3, "TARGET_AHEAD"), (T2, "VALUE_MISMATCH")])
def test_stale_ahead_and_value_mismatch(run_gold, target_updated_at, diff_type):
    conn, _ = run_gold(source=[order_row(status="SHIPPED", updated_at=T2)],
                       target=[order_row(status="PLACED", updated_at=target_updated_at)])
    [d] = diffs(conn)
    assert (d["field_name"], d["source_value"], d["target_value"], d["diff_type"]) == \
        ("status", "SHIPPED", "PLACED", diff_type)
    k = keys(conn)["O1"]
    assert (k["source_updated_at"], k["target_updated_at"]) == (T2, target_updated_at)


def test_null_mismatch_takes_precedence_over_stale(run_gold):
    conn, _ = run_gold(source=[order_row(discount="1.00", updated_at=T2)],
                       target=[order_row(discount="", updated_at=T1)])
    assert [d["diff_type"] for d in diffs(conn)] == ["NULL_MISMATCH"]


def test_several_diffs_on_one_key(run_gold):
    conn, _ = run_gold(
        source=[order_row(customer_id="C1", order_ts="2026-01-01T00:00:00Z", status="SHIPPED", amount="10.00",
                          currency="USD", discount="1.00", updated_at=T2)],
        target=[order_row(customer_id="C2", order_ts="2026-01-01T00:00:01Z", status="PLACED", amount="20.00",
                          currency="EUR", discount="", updated_at=T1)])
    [k] = gold_repo.fetch_key_results(conn, RUN)
    assert (k["order_id"], k["key_status"], k["diff_count"]) == ("O1", "MISMATCHED", 6)
    rows = diffs(conn)
    assert [(d["field_name"], d["diff_type"]) for d in rows] == [  # sorted by key, then field_name
        ("amount", "TARGET_STALE"), ("currency", "TARGET_STALE"), ("customer_id", "TARGET_STALE"),
        ("discount", "NULL_MISMATCH"), ("order_ts", "TARGET_STALE"), ("status", "TARGET_STALE")]
    amount = rows[0]
    assert (amount["source_value"], amount["target_value"], amount["delta_minor"]) == ("10.00", "20.00", -1000)
    assert all(d["delta_minor"] is None for d in rows if d["field_name"] not in ("amount", "discount"))


def test_updated_at_is_not_compared(run_gold):
    conn, _ = run_gold(source=[order_row(updated_at=T1)], target=[order_row(updated_at=T3)])
    assert keys(conn)["O1"]["key_status"] == "MATCHED"


# --------------------------------------------------------------------------- key_result: every status

# order_id -> (source lines, target lines, expected status). Lines are generated below; data lines are
# appended per side in this dict's order, starting at physical line 2.
STATUS_CASES = {
    "A_MATCHED": ([order_row("A_MATCHED")], [order_row("A_MATCHED")], "MATCHED"),
    "B_MISMATCHED": ([order_row("B_MISMATCHED", amount="10.00")],
                     [order_row("B_MISMATCHED", amount="12.00")], "MISMATCHED"),
    "C_MISSING": ([order_row("C_MISSING")], [], "MISSING_IN_TARGET"),
    "D_EXTRA": ([], [order_row("D_EXTRA")], "EXTRA_IN_TARGET"),
    "E_SRC_REJ": ([order_row("E_SRC_REJ", status="LOST")], [order_row("E_SRC_REJ")], "SOURCE_REJECTED"),
    "F_TGT_REJ": ([order_row("F_TGT_REJ")], [order_row("F_TGT_REJ", amount="abc")], "TARGET_REJECTED"),
    "G_BOTH_REJ": ([order_row("G_BOTH_REJ", amount="abc")], [order_row("G_BOTH_REJ", status="LOST")],
                   "SOURCE_REJECTED"),
    "H_SRC_ONLY_REJ": ([order_row("H_SRC_ONLY_REJ", amount="abc")], [], "SOURCE_REJECTED"),
    "I_TGT_ONLY_REJ": ([], [order_row("I_TGT_ONLY_REJ", amount="abc")], "TARGET_REJECTED"),
    "J_SRC_CONFLICT": ([order_row("J_SRC_CONFLICT", status="PLACED"), order_row("J_SRC_CONFLICT", status="SHIPPED")],
                       [order_row("J_SRC_CONFLICT")], "CONFLICT"),
    "K_TGT_CONFLICT": ([order_row("K_TGT_CONFLICT")],
                       [order_row("K_TGT_CONFLICT", amount="1.00"), order_row("K_TGT_CONFLICT", amount="2.00")],
                       "CONFLICT"),
    "L_SHADOWED": ([order_row("L_SHADOWED", updated_at=T1), order_row("L_SHADOWED", status="LOST", updated_at=T2)],
                   [order_row("L_SHADOWED", updated_at=T1)], "SOURCE_REJECTED"),
}
STATUS_SOURCE = [line for src, _, _ in STATUS_CASES.values() for line in src]
STATUS_TARGET = [line for _, tgt, _ in STATUS_CASES.values() for line in tgt]


def test_every_key_status_exactly_one_row_per_key(run_gold):
    conn, result = run_gold(source=STATUS_SOURCE, target=STATUS_TARGET)
    rows = gold_repo.fetch_key_results(conn, RUN)
    assert [r["order_id"] for r in rows] == sorted(STATUS_CASES)  # one row per key, sorted by key
    assert {r["order_id"]: r["key_status"] for r in rows} == {k: v[2] for k, v in STATUS_CASES.items()}
    notes = {r["order_id"]: r["note"] for r in rows if r["note"] is not None}
    assert notes == {"G_BOTH_REJ": "rejected on both sides"}
    assert result.recon_status == "FAIL"
    assert result.total_keys == len(STATUS_CASES)
    assert all(c.passed for c in result.checks if c.severity == "INVARIANT")


def test_key_result_points_at_deciding_lines(run_gold):
    conn, _ = run_gold(source=STATUS_SOURCE, target=STATUS_TARGET)
    k = keys(conn)
    src_line = {line: i for i, line in enumerate(STATUS_SOURCE, start=2)}
    tgt_line = {line: i for i, line in enumerate(STATUS_TARGET, start=2)}
    conflict_src = STATUS_CASES["J_SRC_CONFLICT"][0]
    assert k["J_SRC_CONFLICT"]["source_line_no"] == src_line[conflict_src[0]]  # lowest conflicting line
    assert k["L_SHADOWED"]["source_line_no"] == src_line[STATUS_CASES["L_SHADOWED"][0][1]]  # shadowing reject
    assert k["L_SHADOWED"]["source_updated_at"] == T2
    assert (k["C_MISSING"]["target_line_no"], k["C_MISSING"]["target_updated_at"]) == (None, None)
    assert k["D_EXTRA"]["target_line_no"] == tgt_line[STATUS_CASES["D_EXTRA"][1][0]]
    assert k["I_TGT_ONLY_REJ"]["source_line_no"] is None
    assert all(r["diff_count"] == 0 for name, r in k.items() if name != "B_MISMATCHED")


# --------------------------------------------------------------------------- aggregate_result


def test_group_on_one_side_only_gets_zero_on_the_other(run_gold):
    conn, _ = run_gold(
        source=[order_row("O1", status="PLACED", currency="EUR", amount="10.00", discount="1.00"),
                order_row("O2", status="PLACED", currency="EUR", amount="5.00", discount="")],
        target=[order_row("O1", status="PLACED", currency="EUR", amount="10.00", discount="1.00"),
                order_row("O3", status="RETURNED", currency="GBP", amount="7.50", discount="0.50")])
    agg = aggregates(conn)
    assert agg[("SUM_AMOUNT", "CURRENCY", "GBP")] | {"run_id": None} == {
        "run_id": None, "metric": "SUM_AMOUNT", "group_by": "CURRENCY", "group_value": "GBP",
        "source_value": 0, "target_value": 750, "delta": -750, "explained_delta": -750, "unexplained_delta": 0}
    assert (agg[("ROW_COUNT", "STATUS", "RETURNED")]["source_value"],
            agg[("ROW_COUNT", "STATUS", "RETURNED")]["target_value"]) == (0, 1)
    assert (agg[("ROW_COUNT", "ALL", "*")]["source_value"], agg[("ROW_COUNT", "ALL", "*")]["target_value"]) == (2, 2)
    eur = agg[("SUM_DISCOUNT", "CURRENCY", "EUR")]  # NULL discount ignored, not 0-vs-something
    assert (eur["source_value"], eur["target_value"], eur["delta"]) == (100, 100, 0)
    assert {k for k in agg if k[1] == "CURRENCY"} == {(m, "CURRENCY", c) for m in
                                                     ("ROW_COUNT", "SUM_AMOUNT", "SUM_DISCOUNT") for c in ("EUR", "GBP")}
    assert all(a["unexplained_delta"] == 0 for a in agg.values())


def test_status_change_moves_value_between_groups(run_gold):
    conn, _ = run_gold(source=[order_row(status="SHIPPED", amount="10.00")],
                       target=[order_row(status="PLACED", amount="10.01")])  # in tolerance, still in the walk
    agg = aggregates(conn)
    shipped, placed = agg[("SUM_AMOUNT", "STATUS", "SHIPPED")], agg[("SUM_AMOUNT", "STATUS", "PLACED")]
    assert (shipped["delta"], shipped["explained_delta"]) == (1000, 1000)
    assert (placed["delta"], placed["explained_delta"]) == (-1001, -1001)
    assert agg[("SUM_AMOUNT", "ALL", "*")]["delta"] == -1


def test_empty_sides_still_write_the_all_rows(run_gold):
    conn, result = run_gold()
    agg = aggregates(conn)
    assert sorted(agg) == [("ROW_COUNT", "ALL", "*"), ("SUM_AMOUNT", "ALL", "*"), ("SUM_DISCOUNT", "ALL", "*")]
    assert all(a["source_value"] == a["target_value"] == a["delta"] == 0 for a in agg.values())
    assert result.recon_status == "PASS"  # zero keys, nothing unattributable


# --------------------------------------------------------------------------- checks and recon status


def test_all_nine_checks_written_in_a_stable_shape(run_gold):
    conn, result = run_gold(source=[order_row()], target=[order_row()])
    c = checks(conn)
    assert set(c) == set(CHECK_ORDER) and len(CHECK_ORDER) == 9
    assert all(row["status"] == "PASS" for row in c.values())
    assert [x.check_name for x in result.checks] == list(CHECK_ORDER)
    assert json.loads(c["SRC_ROW_ACCOUNTING"]["detail"]) == {"clean": 1, "landed": 1, "quarantine": 0,
                                                             "side": "SOURCE"}
    assert result.recon_status == "PASS"


def test_warning_checks(run_gold):
    from tests.silver_helpers import SRC_HEADER
    conn, result = run_gold(source=[order_row() + ",x"], target=[],
                            src_header=SRC_HEADER + ",extra_col",
                            tgt_header="order_id,cust_id,order_timestamp,order_status,total_amount,currency_code,"
                                       "last_modified")
    c = checks(conn)
    assert (c["SCHEMA_DRIFT"]["severity"], c["SCHEMA_DRIFT"]["status"]) == ("WARN", "WARN")
    assert json.loads(c["SCHEMA_DRIFT"]["detail"]) == [{"columns": ["extra_col"], "side": "SOURCE"}]
    assert json.loads(c["MISSING_OPTIONAL_COLUMN"]["detail"]) == [{"columns": ["discount_amount"], "side": "TARGET"}]
    assert json.loads(c["EMPTY_BATCH"]["detail"]) == [{"columns": [], "side": "TARGET"}]
    assert c["MISSING_OPTIONAL_COLUMN"]["status"] == c["EMPTY_BATCH"]["status"] == "WARN"
    assert result.recon_status == "FAIL"  # the key is MISSING_IN_TARGET; warnings themselves never decide


def test_unattributable_reject_fails_recon_even_when_every_key_matches(run_gold):
    conn, result = run_gold(source=[order_row(), order_row("O2") + ",extra"], target=[order_row()])
    assert result.key_status_counts == {"MATCHED": 1}
    assert result.unattributable_rejects == 1
    assert result.recon_status == "FAIL"


def test_aggregate_explained_breakdown_on_sample(config):
    import tempfile
    from pathlib import Path
    with tempfile.TemporaryDirectory() as d:
        conn, result = reconcile_files(Path(d) / "recon.db", SAMPLE_PATHS, config)
        try:
            c = checks(conn)
            assert c["AGGREGATE_EXPLAINED"]["status"] == "PASS"
            detail = json.loads(c["AGGREGATE_EXPLAINED"]["detail"])
            assert detail["unexplained_rows"] == 0
            for part in detail["breakdown"]:
                assert part["delta"] == part["source_only"] - part["target_only"] + part["both_sides"]
            assert all(a["unexplained_delta"] == 0 for a in aggregates(conn).values())
        finally:
            conn.close()


def test_sample_matches_generator_answers(config):
    import tempfile
    from pathlib import Path
    expected = json.loads((SAMPLE_PATHS["SOURCE"].parents[1] / "expected" / "sample.json").read_text("utf-8"))
    with tempfile.TemporaryDirectory() as d:
        conn, result = reconcile_files(Path(d) / "recon.db", SAMPLE_PATHS, config)
        try:
            assert result.key_status_counts == {k: v for k, v in expected["key_status_counts"].items() if v}
            assert result.field_diff_counts == expected["field_diff_counts"]
            assert result.recon_status == expected["recon_status"]
            assert result.total_keys == expected["total_keys"]
            assert result.unattributable_rejects == expected["unattributable_rejects"]
            agg = aggregates(conn)
            assert agg[("SUM_AMOUNT", "ALL", "*")]["source_value"] == \
                int(expected["sides"]["SOURCE"]["sum_amount"].replace(".", ""))
            assert agg[("SUM_DISCOUNT", "ALL", "*")]["source_value"] == \
                int(expected["sides"]["SOURCE"]["sum_discount"].replace(".", ""))
            assert all(c.status == "PASS" for c in result.checks)
        finally:
            conn.close()


# --------------------------------------------------------------------------- invariants roll back


def _corrupt_and_reconcile(conn, config, corrupt):
    with pytest.raises(InvariantViolation) as info:
        with transaction(conn):
            corrupt()
            reconcile(conn, SILVER_RUN_ID, config)
    for fetch in (gold_repo.fetch_key_results, gold_repo.fetch_field_diffs, gold_repo.fetch_aggregates,
                  gold_repo.fetch_check_results):
        assert fetch(conn, SILVER_RUN_ID) == []  # the data transaction rolled back: no GOLD rows
    return info.value


def test_row_accounting_violation_raises_and_rolls_back(run_silver, config):
    conn, _ = run_silver(source=[order_row()], target=[order_row()])
    err = _corrupt_and_reconcile(conn, config, lambda: bronze_repo.insert_raw_records(
        conn, [(SILVER_RUN_ID, "SOURCE", 99, "s", "DATA", "orphan line")]))
    assert err.failure_reason == "INVARIANT_VIOLATION:SRC_ROW_ACCOUNTING"
    assert json.loads(err.failure_detail) == {"clean": 1, "landed": 2, "quarantine": 0, "side": "SOURCE"}
    assert bronze_repo.count_raw_records(conn, SILVER_RUN_ID, "SOURCE", "DATA") == 1


def test_referential_integrity_violation_raises(run_silver, config):
    conn, _ = run_silver(source=[order_row()], target=[order_row()])
    [row] = silver_repo.fetch_clean_records(conn, SILVER_RUN_ID, "TARGET")

    def corrupt():  # a clean row whose customer is not in ref_customer, with a landed raw line behind it
        bronze_repo.insert_raw_records(conn, [(SILVER_RUN_ID, "TARGET", 50, "t", "DATA", "x")])
        silver_repo.insert_clean_records(conn, [row | {"order_id": "O9", "customer_id": "C404", "line_no": 50}])

    err = _corrupt_and_reconcile(conn, config, corrupt)
    assert err.failure_reason == "INVARIANT_VIOLATION:REFERENTIAL_INTEGRITY"
    assert json.loads(err.failure_detail) == {"fields": ["customer_id"], "unknown_references": 1}


# --------------------------------------------------------------------------- symmetry

MIRROR_STATUS = {"MISSING_IN_TARGET": "EXTRA_IN_TARGET", "EXTRA_IN_TARGET": "MISSING_IN_TARGET",
                 "SOURCE_REJECTED": "TARGET_REJECTED", "TARGET_REJECTED": "SOURCE_REJECTED"}
MIRROR_DIFF = {"TARGET_STALE": "TARGET_AHEAD", "TARGET_AHEAD": "TARGET_STALE"}


def mirror(snapshot):
    """What the GOLD tables must look like when source and target are exchanged."""
    def neg(v):
        return None if v is None else -v
    keys_ = []
    for k in snapshot["key_result"]:
        status = k["key_status"] if k["note"] else MIRROR_STATUS.get(k["key_status"], k["key_status"])
        keys_.append(k | {"key_status": status,
                          "source_line_no": k["target_line_no"], "target_line_no": k["source_line_no"],
                          "source_updated_at": k["target_updated_at"],
                          "target_updated_at": k["source_updated_at"]})
    diffs_ = [d | {"source_value": d["target_value"], "target_value": d["source_value"],
                   "delta_minor": neg(d["delta_minor"]), "diff_type": MIRROR_DIFF.get(d["diff_type"], d["diff_type"])}
              for d in snapshot["field_diff"]]
    aggs = [a | {"source_value": a["target_value"], "target_value": a["source_value"], "delta": -a["delta"],
                 "explained_delta": -a["explained_delta"], "unexplained_delta": -a["unexplained_delta"]}
            for a in snapshot["aggregate_result"]]
    return {"key_result": keys_, "field_diff": diffs_, "aggregate_result": aggs}


def assert_mirrored(original, swapped):
    assert {k: v for k, v in swapped.items() if k != "check_result"} == mirror(original)
    checks_o = {c["check_name"]: c for c in original["check_result"]}
    checks_s = {c["check_name"]: c for c in swapped["check_result"]}
    assert {n: c["status"] for n, c in checks_o.items()} == {n: c["status"] for n, c in checks_s.items()}
    for a, b in (("SRC_ROW_ACCOUNTING", "TGT_ROW_ACCOUNTING"), ("TGT_ROW_ACCOUNTING", "SRC_ROW_ACCOUNTING")):
        assert (checks_o[a]["expected"], checks_o[a]["actual"]) == (checks_s[b]["expected"], checks_s[b]["actual"])


def test_symmetry_on_every_status(run_gold):
    stale = ([order_row("S_STALE", status="SHIPPED", updated_at=T2)], [order_row("S_STALE", updated_at=T1)])
    ahead = ([order_row("S_AHEAD", amount="3.00", updated_at=T1)], [order_row("S_AHEAD", amount="4.00", updated_at=T2)])
    source = STATUS_SOURCE + stale[0] + ahead[0]
    target = STATUS_TARGET + stale[1] + ahead[1]
    conn_o, res_o = run_gold(source=source, target=target)
    conn_s, res_s = run_gold(source=source, target=target, swap=True)
    original, swapped = gold_snapshot(conn_o), gold_snapshot(conn_s)
    assert {d["diff_type"] for d in original["field_diff"]} >= {"TARGET_STALE", "TARGET_AHEAD"}
    assert_mirrored(original, swapped)
    assert res_o.recon_status == res_s.recon_status


def test_symmetry_on_sample(tmp_path, config, orders_config_dict):
    conn_o, res_o = reconcile_files(tmp_path / "o.db", SAMPLE_PATHS, config)
    conn_s, res_s = reconcile_files(tmp_path / "s.db", swapped_paths(SAMPLE_PATHS), swapped_config(orders_config_dict))
    try:
        original, swapped = gold_snapshot(conn_o), gold_snapshot(conn_s)
        assert_mirrored(original, swapped)
        assert res_o.key_status_counts["MISSING_IN_TARGET"] == res_s.key_status_counts["EXTRA_IN_TARGET"]
        assert res_o.unattributable_rejects == res_s.unattributable_rejects
    finally:
        conn_o.close()
        conn_s.close()


def test_same_inputs_give_identical_gold(tmp_path, config):
    conn_a, _ = reconcile_files(tmp_path / "a.db", SAMPLE_PATHS, config)
    conn_b, _ = reconcile_files(tmp_path / "b.db", SAMPLE_PATHS, config)
    try:
        assert gold_snapshot(conn_a) == gold_snapshot(conn_b)
    finally:
        conn_a.close()
        conn_b.close()


# --------------------------------------------------------------------------- generated SQL


def test_field_diff_sql_shape(config):
    sql = gold_repo.field_diff_sql(compare_fields(config))
    assert sql.count("UNION ALL") == len(config.compare_fields) - 1
    assert "ABS(s.amount_minor - t.amount_minor) > :tolerance_amount" in sql
    assert "s.status IS NOT t.status" in sql
    params = gold_repo.field_diff_params(RUN, compare_fields(config))
    assert params == {"run_id": RUN, "scale_amount": 2, "tolerance_amount": 1,
                      "scale_discount": 2, "tolerance_discount": 1}


def test_unsafe_identifier_is_refused():
    with pytest.raises(ValueError):
        gold_repo.field_diff_sql([gold_repo.CompareField("x", "amount; DROP TABLE run")])
