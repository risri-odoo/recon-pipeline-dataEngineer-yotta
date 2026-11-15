"""Duplicate dispositions, version ordering and row accounting (DESIGN 4.4, 4.7 row accounting)."""

import itertools
import json

import pytest

from recon.repository import bronze_repo, silver_repo
from recon.service.config import ConfigError, load_config, parse_config
from recon.service.dedupe import dedupe
from recon.service.normalize import make_business_key
from recon.service.validate import validate_side
from tests.silver_helpers import SILVER_RUN_ID as RUN, SRC_HEADER, order_row

T1, T2, T3 = "2026-01-01T01:00:00Z", "2026-01-02T01:00:00Z", "2026-01-03T01:00:00Z"


def q_by_line(conn, side="SOURCE"):
    return {q["line_no"]: q for q in silver_repo.fetch_quarantine(conn, RUN, side)}


def dispositions(conn, side="SOURCE"):
    return {line: (q["category"], q["reason_code"]) for line, q in q_by_line(conn, side).items()}


def clean(conn, side="SOURCE"):
    return silver_repo.fetch_clean_records(conn, RUN, side)


# --------------------------------------------------------------------------- rejects vs valid rows


def test_older_reject_and_valid_row_valid_wins(run_silver):
    conn, result = run_silver(source=[order_row(amount="abc", updated_at=T2), order_row(updated_at=T3)])
    [row] = clean(conn)
    assert (row["order_id"], row["line_no"], row["updated_at"]) == ("O1", 3, T3)
    assert dispositions(conn) == {2: ("ROW_REJECT", "BAD_TYPE")}
    assert result.sides["SOURCE"].counts.accepted == 1


def test_reject_with_unparsed_version_cannot_win(run_silver):
    conn, _ = run_silver(source=[order_row(amount="abc", updated_at="later"), order_row(updated_at=T1)])
    assert [r["line_no"] for r in clean(conn)] == [3]
    assert dispositions(conn) == {2: ("ROW_REJECT", "BAD_TYPE")}


@pytest.mark.parametrize("reject_version", [T3, T2], ids=["newer", "tie"])
def test_newest_reject_shadows_valid_rows(run_silver, reject_version):
    conn, result = run_silver(source=[order_row(updated_at=T1), order_row(updated_at=T2),
                                      order_row(status="LOST", updated_at=reject_version)])
    assert clean(conn) == []  # no fallback to an older valid version
    q = q_by_line(conn)
    assert dispositions(conn) == {2: ("DUPLICATE", "SUPERSEDED"), 3: ("DUPLICATE", "SUPERSEDED"),
                                  4: ("ROW_REJECT", "BAD_ENUM")}
    assert q[2]["detail"] == q[3]["detail"] == "shadowed by rejected line 4"
    assert (q[4]["order_id"], q[4]["updated_at"]) == ("O1", reject_version)
    assert result.sides["SOURCE"].conflict_keys == []


# --------------------------------------------------------------------------- duplicates and versions


def test_exact_duplicate_keeps_first_line(run_silver):
    same_after_normalization = order_row(order_id=" o1", status="placed", amount="10.0", currency="usd")
    conn, result = run_silver(source=[order_row(), order_row(), same_after_normalization])
    assert [r["line_no"] for r in clean(conn)] == [2]
    assert dispositions(conn) == {3: ("DUPLICATE", "DUPLICATE_EXACT"), 4: ("DUPLICATE", "DUPLICATE_EXACT")}
    assert q_by_line(conn)[3]["detail"] == "exact duplicate of line 2"
    assert q_by_line(conn)[3]["order_id"] == "O1"
    assert result.sides["SOURCE"].counts.duplicate_exact == 2


def test_newer_version_first_in_file_wins(run_silver):
    conn, _ = run_silver(source=[order_row(status="SHIPPED", updated_at=T2), order_row(status="PLACED", updated_at=T1)])
    [row] = clean(conn)
    assert (row["line_no"], row["status"], row["updated_at"]) == (2, "SHIPPED", T2)
    assert dispositions(conn) == {3: ("DUPLICATE", "SUPERSEDED")}
    assert q_by_line(conn)[3]["detail"] == f"superseded by line 2 (updated_at {T2})"


def test_three_versions_plus_duplicate_of_winner(run_silver):
    conn, result = run_silver(source=[
        order_row(status="PLACED", updated_at=T1),
        order_row(status="DELIVERED", updated_at=T3),
        order_row(status="SHIPPED", updated_at=T2),
        order_row(status="DELIVERED", updated_at=T3),
    ])
    [row] = clean(conn)
    assert (row["line_no"], row["status"]) == (3, "DELIVERED")
    assert dispositions(conn) == {2: ("DUPLICATE", "SUPERSEDED"), 4: ("DUPLICATE", "SUPERSEDED"),
                                  5: ("DUPLICATE", "DUPLICATE_EXACT")}
    counts = result.sides["SOURCE"].counts
    assert (counts.accepted, counts.superseded, counts.duplicate_exact) == (1, 2, 1)


def test_conflicting_duplicate(run_silver):
    conn, result = run_silver(source=[
        order_row(status="SHIPPED", updated_at=T2),
        order_row(status="PLACED", updated_at=T1),
        order_row(status="CANCELLED", updated_at=T2),
        order_row(order_id="O2"),
    ])
    assert [r["order_id"] for r in clean(conn)] == ["O2"]
    assert dispositions(conn) == {2: ("DUPLICATE", "CONFLICTING_DUPLICATE"), 3: ("DUPLICATE", "SUPERSEDED"),
                                  4: ("DUPLICATE", "CONFLICTING_DUPLICATE")}
    q = q_by_line(conn)
    assert q[2]["detail"] == f"2 different payloads at updated_at {T2} on lines 2, 4"
    assert q[3]["detail"] == f"superseded by conflicting versions at updated_at {T2} (lines 2, 4)"
    assert result.sides["SOURCE"].conflict_keys == ["O1"]
    assert result.sides["SOURCE"].counts.conflicting_rows == 2


def test_conflict_with_an_identical_pair_still_quarantines_all_top_rows(run_silver):
    conn, result = run_silver(source=[order_row(updated_at=T2), order_row(updated_at=T2),
                                      order_row(status="SHIPPED", updated_at=T2)])
    assert clean(conn) == []
    assert {v[1] for v in dispositions(conn).values()} == {"CONFLICTING_DUPLICATE"}
    assert result.sides["SOURCE"].counts.conflicting_rows == 3


def test_duplicates_on_target_side(run_silver):
    conn, result = run_silver(source=[order_row()],
                              target=[order_row(updated_at=T1), order_row(updated_at=T2), order_row(updated_at=T2)])
    assert [r["line_no"] for r in clean(conn, "TARGET")] == [3]
    assert dispositions(conn, "TARGET") == {2: ("DUPLICATE", "SUPERSEDED"), 4: ("DUPLICATE", "DUPLICATE_EXACT")}
    assert all(q["source_file"].endswith("target.csv") for q in q_by_line(conn, "TARGET").values())
    assert dispositions(conn, "SOURCE") == {}
    assert result.sides["TARGET"].counts.duplicate_exact == 1


def test_arrival_order_never_changes_the_outcome(run_silver, config):
    conn, _ = run_silver(source=[
        order_row(status="PLACED", updated_at=T1), order_row(status="SHIPPED", updated_at=T3),
        order_row(status="SHIPPED", updated_at=T3), order_row(order_id="O2", updated_at=T2),
        order_row(order_id="O2", status="CANCELLED", updated_at=T2), order_row(order_id="O3", amount="x", updated_at=T3),
        order_row(order_id="O3", updated_at=T1),
    ])
    validated = validate_side(conn, RUN, "SOURCE", config, frozenset({"C1"}))
    expected = dedupe(validated.valid, validated.rejects)
    for perm in itertools.permutations(validated.valid):
        got = dedupe(list(perm), list(reversed(validated.rejects)))
        assert got == expected
    assert [w.line_no for w in expected.winners] == [3]
    assert expected.conflict_keys == ["O2"]


def test_winners_sorted_by_key_and_clean_row_traceable(run_silver):
    conn, result = run_silver(source=[order_row(order_id="O3"), order_row(order_id="O10"), order_row(order_id="O1")])
    assert [w.key for w in result.sides["SOURCE"].winners] == ["O1", "O10", "O3"]  # byte order
    assert [(r["order_id"], r["line_no"]) for r in clean(conn)] == [("O1", 4), ("O10", 3), ("O3", 2)]


# --------------------------------------------------------------------------- composite keys

COMPOSITE_CONFIG = {
    "dataset": "orders_by_currency",
    "business_key": ["order_id", "currency"],
    "version_field": "updated_at",
    "fields": [
        {"name": "order_id", "type": "str", "required": True, "upper": True},
        {"name": "customer_id", "type": "str", "required": True, "upper": True},
        {"name": "order_ts", "type": "timestamp", "required": True},
        {"name": "status", "type": "enum", "required": True, "values": ["PLACED", "SHIPPED"]},
        {"name": "amount", "type": "decimal", "required": True, "min": "0"},
        {"name": "currency", "type": "str", "required": True, "upper": True},
        {"name": "discount", "type": "decimal"},
        {"name": "updated_at", "type": "timestamp", "required": True},
    ],
    "compare_fields": ["customer_id", "amount"],
    "columns": {side: {n: n for n in ("order_id", "customer_id", "order_ts", "status", "amount", "currency",
                                      "discount", "updated_at")} for side in ("source", "target")},
    "aggregates": {"sum_fields": ["amount"], "group_by": ["status"]},
}


@pytest.fixture
def composite_config(tmp_path):
    path = tmp_path / "composite.json"
    path.write_text(json.dumps(COMPOSITE_CONFIG), encoding="utf-8")
    return load_config(path)


def test_composite_key_dedupe(run_silver, composite_config):
    conn, result = run_silver(config=composite_config, tgt_header=SRC_HEADER,
                              reference=(),
                              source=[order_row(currency="USD"), order_row(currency="EUR"),
                                      order_row(currency="usd"), order_row(currency="EUR", status="SHIPPED", updated_at=T2),
                                      order_row(order_id="O2", customer_id="ANY", currency="EUR")],
                              target=[order_row(currency="EUR", updated_at=T2), order_row(currency="EUR", status="SHIPPED", updated_at=T2)])
    assert [(r["order_id"], r["currency"], r["line_no"]) for r in clean(conn)] == [
        ("O1|EUR", "EUR", 5), ("O1|USD", "USD", 2), ("O2|EUR", "EUR", 6)]  # no reference check in this config
    assert dispositions(conn) == {3: ("DUPLICATE", "SUPERSEDED"), 4: ("DUPLICATE", "DUPLICATE_EXACT")}
    assert q_by_line(conn)[4]["order_id"] == "O1|USD"
    assert result.sides["TARGET"].conflict_keys == ["O1|EUR"]
    assert result.reference is None


def test_composite_key_reject_keeps_joined_key(run_silver, composite_config):
    conn, _ = run_silver(config=composite_config, reference=(),
                         tgt_header=SRC_HEADER,
                         source=[order_row(amount="abc", currency="gbp"), order_row(currency="")])
    q = q_by_line(conn)
    assert (q[2]["order_id"], q[2]["reason_code"]) == ("O1|GBP", "BAD_TYPE")
    assert (q[3]["order_id"], q[3]["reason_code"]) == (None, "MISSING_REQUIRED")  # a key part is missing


def test_composite_key_escaping(composite_config):
    def key(order_id, currency):
        return make_business_key(composite_config, {"order_id": order_id, "currency": currency})
    assert key("O1", "USD") == "O1|USD"
    assert key("A|B", "C") == "A\\|B|C"
    assert key("A", "B|C") == "A|B\\|C"
    assert key("A\\", "B") == "A\\\\|B"
    assert len({key("A|B", "C"), key("A", "B|C"), key("A\\", "|C"), key("A\\|", "C")}) == 4
    assert key("O1", None) is None


def test_single_key_is_not_escaped(config):
    assert make_business_key(config, {"order_id": "A|B\\C"}) == "A|B\\C"


@pytest.mark.parametrize("business_key, message", [
    ([], "must not be empty"),
    (["order_id", "nope"], "unknown field name"),
    (["order_id", "order_id"], "duplicate"),
    (["order_id", "discount"], "must be required"),
    (["order_id", "amount"], "must not be of type decimal"),
    (["order_id", "updated_at"], "must not contain the version_field"),
    ({"order_id": 1}, "must be a list"),
])
def test_composite_key_config_errors(business_key, message):
    with pytest.raises(ConfigError, match=message):
        parse_config(COMPOSITE_CONFIG | {"business_key": business_key})


def test_key_fields_cannot_be_compared():
    with pytest.raises(ConfigError, match="must not contain key/version field 'currency'"):
        parse_config(COMPOSITE_CONFIG | {"compare_fields": ["currency"]})


def test_one_element_list_equals_string_key(raw_config, config):
    assert parse_config(raw_config | {"business_key": ["order_id"]}).business_key == config.business_key


# --------------------------------------------------------------------------- row accounting


def test_row_accounting_every_side(run_silver):
    source = [
        order_row(order_id="A"),                                   # accepted
        order_row(order_id="A"),                                   # duplicate_exact
        order_row(order_id="B", updated_at=T1),                    # superseded
        order_row(order_id="B", updated_at=T2),                    # accepted
        order_row(order_id="C", status="SHIPPED", updated_at=T2),  # conflicting
        order_row(order_id="C", status="PLACED", updated_at=T2),   # conflicting
        order_row(order_id="C", updated_at=T1),                    # superseded
        order_row(order_id="D") + ",x",                            # rejected (malformed)
        order_row(order_id="E", customer_id="C9"),                 # rejected (invalid reference)
        order_row(order_id="F", updated_at=T1),                    # superseded (shadowed)
        order_row(order_id="F", amount="-5", updated_at=T2),       # rejected
    ]
    target = [order_row(order_id="A"), order_row(order_id="A"), order_row(order_id="B", amount="1.234")]
    conn, result = run_silver(source=source, target=target)

    src, tgt = result.sides["SOURCE"].counts, result.sides["TARGET"].counts
    assert (src.rows_read, src.accepted, src.rejected, src.duplicate_exact, src.superseded, src.conflicting_rows) \
        == (11, 2, 3, 1, 3, 2)
    assert (tgt.rows_read, tgt.accepted, tgt.rejected, tgt.duplicate_exact, tgt.superseded, tgt.conflicting_rows) \
        == (3, 1, 1, 1, 0, 0)
    for side, counts in (("SOURCE", src), ("TARGET", tgt)):
        assert counts.balanced
        landed = bronze_repo.count_raw_records(conn, RUN, side, "DATA")
        assert landed == counts.rows_read
        assert landed == silver_repo.count_clean_records(conn, RUN, side) + silver_repo.count_quarantine(conn, RUN, side)
        assert silver_repo.count_quarantine(conn, RUN, side, "ROW_REJECT") == counts.rejected
        assert silver_repo.count_quarantine(conn, RUN, side, "DUPLICATE") == (
            counts.duplicate_exact + counts.superseded + counts.conflicting_rows)
    assert result.sides["SOURCE"].conflict_keys == ["C"]
    assert [q.line_no for q in result.sides["SOURCE"].quarantine] == sorted(q.line_no for q in result.sides["SOURCE"].quarantine)


def test_same_lines_give_same_clean_rows_on_both_sides(run_silver):
    lines = [order_row(order_id="B", updated_at=T2), order_row(order_id="A"), order_row(order_id="B", updated_at=T1),
             order_row(order_id="C", amount="x")]
    conn, _ = run_silver(source=lines, target=lines)
    snapshot = (clean(conn), clean(conn, "TARGET"), silver_repo.fetch_quarantine(conn, RUN))
    assert [r["order_id"] for r in snapshot[0]] == ["A", "B"]
    assert snapshot[1] == [r | {"side": "TARGET", "source_file": snapshot[1][i]["source_file"]}
                           for i, r in enumerate(snapshot[0])]
