"""File / schema edge cases for BRONZE ingest. Every fixture is built in tmp_path."""

import hashlib
import json

import pytest

from recon.repository import bronze_repo, run_repo
from recon.repository.db import connect, transaction
from recon.repository.schema import create_schema
from recon.service import ingest as ing

SRC_HEADER = "order_id,customer_id,order_ts,status,amount,currency,discount,updated_at"
TGT_HEADER = "order_id,cust_id,order_timestamp,order_status,total_amount,currency_code,discount_amount,last_modified"
REF_HEADER = "customer_id,customer_name"
SRC_ROW = "O1,C1,2026-01-01T00:00:00Z,PLACED,10.00,USD,1.00,2026-01-01T01:00:00Z"
TGT_ROW = SRC_ROW
REF_ROW = "C1,Alice"
RUN_ID = "r-a1"


@pytest.fixture
def conn(tmp_path):
    c = connect(tmp_path / "recon.db")
    create_schema(c)
    with transaction(c):
        run_repo.create_run(c, run_id=RUN_ID, run_key="k", attempt=1, status="RUNNING", source_file="s",
                            target_file="t", reference_file="r", config_sha256="c",
                            started_at="2026-11-15T00:00:00Z")
    yield c
    c.close()


def write(tmp_path, name, data):
    path = tmp_path / name
    path.write_bytes(data if isinstance(data, bytes) else data.encode("utf-8"))
    return path


def inputs(tmp_path, source=None, target=None, reference=None):
    return {
        ing.SOURCE: write(tmp_path, "src.csv", source if source is not None else f"{SRC_HEADER}\n{SRC_ROW}\n"),
        ing.TARGET: write(tmp_path, "tgt.csv", target if target is not None else f"{TGT_HEADER}\n{TGT_ROW}\n"),
        ing.REFERENCE: write(tmp_path, "ref.csv", reference if reference is not None else f"{REF_HEADER}\n{REF_ROW}\n"),
    }


def run_ingest(conn, paths, config):
    with transaction(conn):
        return ing.ingest(conn, RUN_ID, paths, config)


def check_source(tmp_path, config, data):
    return ing.check_file(write(tmp_path, "one.csv", data), ing.side_specs(config)[ing.SOURCE])


def codes(issues):
    return [i.code for i in issues]


# --------------------------------------------------------------------------- batch rejects


def test_missing_file_is_file_not_found(tmp_path, config, conn):
    paths = inputs(tmp_path)
    paths[ing.SOURCE] = tmp_path / "nope.csv"
    result = run_ingest(conn, paths, config)
    assert codes(result.rejects) == ["FILE_NOT_FOUND"]
    assert result.checks[ing.SOURCE].sha256 is None
    assert bronze_repo.count_raw_records(conn, RUN_ID) == 0  # nothing landed for any side


def test_directory_is_file_not_found(tmp_path, config):
    check = ing.check_file(tmp_path, ing.side_specs(config)[ing.SOURCE])
    assert codes(check.rejects) == ["FILE_NOT_FOUND"]


def test_zero_byte_file_is_empty_file(tmp_path, config):
    check = check_source(tmp_path, config, b"")
    assert codes(check.rejects) == ["EMPTY_FILE"]
    assert check.sha256 == hashlib.sha256(b"").hexdigest()


def test_only_blank_lines_is_empty_file(tmp_path, config):
    assert codes(check_source(tmp_path, config, "\n  \n\r\n").rejects) == ["EMPTY_FILE"]


def test_header_only_lands_header_and_warns_empty_batch(tmp_path, config, conn):
    result = run_ingest(conn, inputs(tmp_path, source=f"{SRC_HEADER}\n"), config)
    assert result.ok
    src = result.sides[ing.SOURCE]
    assert src.data_records == 0
    assert [(w.code, w.side) for w in result.warnings] == [("EMPTY_BATCH", "SOURCE")]
    rows = bronze_repo.fetch_raw_records(conn, RUN_ID, "SOURCE")
    assert [(r["line_no"], r["record_type"], r["raw_text"]) for r in rows] == [(1, "HEADER", SRC_HEADER)]


def test_non_utf8_is_encoding_error(tmp_path, config, conn):
    bad = f"{SRC_HEADER}\n".encode() + b"O1,C1,2026-01-01T00:00:00Z,PLACED,10.00,USD,1.00,caf\xe9\n"
    result = run_ingest(conn, inputs(tmp_path, source=bad), config)
    [issue] = result.rejects
    assert issue.code == "ENCODING_ERROR" and issue.side == "SOURCE"
    assert "byte offset" in issue.detail
    assert result.checks[ing.SOURCE].sha256 == hashlib.sha256(bad).hexdigest()
    assert bronze_repo.count_raw_records(conn, RUN_ID) == 0


def test_duplicate_header_after_normalization(tmp_path, config):
    check = check_source(tmp_path, config, f"{SRC_HEADER}, Amount \n{SRC_ROW},1\n")
    assert codes(check.rejects) == ["DUPLICATE_HEADER"]
    assert check.rejects[0].columns == ("amount",)


def test_missing_required_column(tmp_path, config):
    header = SRC_HEADER.replace(",currency,", ",")
    check = check_source(tmp_path, config, f"{header}\n")
    assert codes(check.rejects) == ["SCHEMA_MISSING_COLUMN"]
    assert check.rejects[0].columns == ("currency",)


def test_unparseable_header_is_malformed_file(tmp_path, config):
    header = SRC_HEADER.replace("customer_id", '"customer_id"x')
    check = check_source(tmp_path, config, f"\n{header}\n{SRC_ROW}\n")
    assert codes(check.rejects) == ["MALFORMED_FILE"]
    assert "line 2" in check.rejects[0].detail


def test_unterminated_quote_mid_file_is_malformed_file_with_start_line(tmp_path, config, conn):
    rows = [SRC_ROW.replace("O1", f"O{i}") for i in range(1, 6)]
    rows[2] = rows[2].replace(",PLACED,", ',"PLACED,')  # record on line 4 opens a quote that never closes
    src = f"{SRC_HEADER}\n" + "\n".join(rows) + "\n"
    result = run_ingest(conn, inputs(tmp_path, source=src), config)
    [issue] = result.rejects
    assert (issue.side, issue.code) == ("SOURCE", "MALFORMED_FILE")
    assert "starting at line 4" in issue.detail
    assert bronze_repo.count_raw_records(conn, RUN_ID) == 0


def test_closed_and_escaped_quotes_are_not_malformed_file(tmp_path, config):
    ref = f'{REF_HEADER}\nC1,"He said ""hi"""\nC2,"multi\nline"\nC3,mid"quote\n'
    check = ing.check_file(write(tmp_path, "r.csv", ref), ing.side_specs(config)[ing.REFERENCE])
    assert check.ok


@pytest.mark.parametrize("text, is_open", [
    ('a,"b', True), ('a,"b""', True), ('a,"b"""', False), ('a,"b"', False), ('a,b"c', False),
    ('a,"b"x,"c', True), ('"x\ny', True), ('"x\ny"', False), ("", False), ('a,""', False),
])
def test_ends_in_open_quote(text, is_open):
    assert ing.ends_in_open_quote(text) is is_open


def test_every_side_is_checked_and_rejects_are_ordered(tmp_path, config, conn):
    paths = inputs(tmp_path, source=b"", target=f"{TGT_HEADER},cust_id\n", reference="customer_name\nAlice\n")
    result = run_ingest(conn, paths, config)
    assert [(i.side, i.code) for i in result.rejects] == [
        ("SOURCE", "EMPTY_FILE"), ("TARGET", "DUPLICATE_HEADER"), ("REFERENCE", "SCHEMA_MISSING_COLUMN")]
    assert result.sides == {}
    assert bronze_repo.count_raw_records(conn, RUN_ID) == 0


# --------------------------------------------------------------------------- batch warnings


def test_missing_optional_column_warns_and_maps_nothing(tmp_path, config, conn):
    header = SRC_HEADER.replace(",discount,", ",")
    row = SRC_ROW.replace(",1.00,", ",")
    result = run_ingest(conn, inputs(tmp_path, source=f"{header}\n{row}\n"), config)
    assert result.ok
    [warning] = result.warnings
    assert (warning.code, warning.side, warning.columns) == ("MISSING_OPTIONAL_COLUMN", "SOURCE", ("discount",))
    assert "discount" not in result.sides[ing.SOURCE].column_index


def test_extra_column_warns_and_is_ignored(tmp_path, config, conn):
    result = run_ingest(conn, inputs(tmp_path, source=f"{SRC_HEADER},Note\n{SRC_ROW},hello\n"), config)
    assert result.ok
    [warning] = result.warnings
    assert (warning.code, warning.columns) == ("SCHEMA_EXTRA_COLUMN", ("note",))
    assert result.sides[ing.SOURCE].column_index["order_id"] == 0
    assert result.sides[ing.SOURCE].data_records == 1


def test_warnings_sorted_by_code_then_side(tmp_path, config, conn):
    paths = inputs(tmp_path, source=f"{SRC_HEADER},x\n{SRC_ROW},1\n", target=f"{TGT_HEADER},y\n",
                   reference="customer_id\nC1\n")
    result = run_ingest(conn, paths, config)
    assert [(w.code, w.side) for w in result.warnings] == [
        ("EMPTY_BATCH", "TARGET"), ("MISSING_OPTIONAL_COLUMN", "REFERENCE"),
        ("SCHEMA_EXTRA_COLUMN", "SOURCE"), ("SCHEMA_EXTRA_COLUMN", "TARGET")]


# --------------------------------------------------------------------------- header handling


def test_reordered_columns_map_by_name(tmp_path, config):
    names = SRC_HEADER.split(",")
    check = check_source(tmp_path, config, ",".join(reversed(names)) + "\n")
    assert check.ok and not check.warnings
    assert check.column_index == {n: len(names) - 1 - i for i, n in enumerate(names)}


def test_header_case_and_spaces_are_normalized(tmp_path, config):
    header = ",".join(f"  {n.upper()} " for n in TGT_HEADER.split(","))
    check = ing.check_file(write(tmp_path, "t.csv", header + "\n"), ing.side_specs(config)[ing.TARGET])
    assert check.ok and not check.warnings
    assert check.header == tuple(TGT_HEADER.split(","))


def test_bom_is_stripped_from_header_but_hashed(tmp_path, config, conn):
    data = b"\xef\xbb\xbf" + f"{SRC_HEADER}\n{SRC_ROW}\n".encode()
    result = run_ingest(conn, inputs(tmp_path, source=data), config)
    assert result.ok and not result.warnings
    assert result.sides[ing.SOURCE].header[0] == "order_id"
    assert result.sides[ing.SOURCE].sha256 == hashlib.sha256(data).hexdigest()
    rows = bronze_repo.fetch_raw_records(conn, RUN_ID, "SOURCE")
    assert rows[0]["raw_text"] == SRC_HEADER


def test_leading_blank_lines_before_header(tmp_path, config, conn):
    result = run_ingest(conn, inputs(tmp_path, source=f"\n\n{SRC_HEADER}\n{SRC_ROW}\n"), config)
    rows = bronze_repo.fetch_raw_records(conn, RUN_ID, "SOURCE")
    assert [(r["line_no"], r["record_type"]) for r in rows] == [(3, "HEADER"), (4, "DATA")]
    assert result.sides[ing.SOURCE].blank_lines == 2


# --------------------------------------------------------------------------- record boundaries / raw text


def test_crlf_line_endings_are_not_in_raw_text(tmp_path, config, conn):
    run_ingest(conn, inputs(tmp_path, source=f"{SRC_HEADER}\r\n{SRC_ROW}\r\n{SRC_ROW.replace('O1', 'O2')}\r\n"), config)
    rows = bronze_repo.fetch_raw_records(conn, RUN_ID, "SOURCE")
    assert [(r["line_no"], r["raw_text"]) for r in rows] == [
        (1, SRC_HEADER), (2, SRC_ROW), (3, SRC_ROW.replace("O1", "O2"))]


def test_crlf_with_interior_and_trailing_blank_lines(tmp_path, config, conn):
    row2 = SRC_ROW.replace("O1", "O2")
    src = f"{SRC_HEADER}\r\n{SRC_ROW}\r\n\r\n{row2}\r\n\r\n  \r\n\r\n"
    result = run_ingest(conn, inputs(tmp_path, source=src), config)
    rows = bronze_repo.fetch_raw_records(conn, RUN_ID, "SOURCE")
    assert [(r["line_no"], r["record_type"], r["raw_text"]) for r in rows] == [
        (1, "HEADER", SRC_HEADER), (2, "DATA", SRC_ROW), (4, "DATA", row2)]  # no \r, physical line numbers
    assert (result.sides[ing.SOURCE].data_records, result.sides[ing.SOURCE].blank_lines) == (2, 4)
    assert not result.warnings


def test_mixed_line_endings_in_one_file(tmp_path, config, conn):
    rows = [SRC_ROW.replace("O1", f"O{i}") for i in range(1, 4)]
    src = f"{SRC_HEADER}\r\n{rows[0]}\n{rows[1]}\r{rows[2]}\r\n"  # CRLF, LF, lone CR, CRLF
    result = run_ingest(conn, inputs(tmp_path, source=src), config)
    rows_landed = bronze_repo.fetch_raw_records(conn, RUN_ID, "SOURCE")
    assert [(r["line_no"], r["raw_text"]) for r in rows_landed] == [
        (1, SRC_HEADER), (2, rows[0]), (3, rows[1]), (4, rows[2])]  # every terminator ends a physical line
    assert result.sides[ing.SOURCE].data_records == 3


def test_header_field_over_the_csv_size_limit_is_malformed_file(tmp_path, config):
    import csv
    check = check_source(tmp_path, config, f"{SRC_HEADER},{'x' * (csv.field_size_limit() + 1)}\n{SRC_ROW},1\n")
    assert codes(check.rejects) == ["MALFORMED_FILE"]
    assert check.rejects[0].detail == ("header record starting at line 1 is not parseable CSV: a field exceeds the "
                                       f"CSV field size limit ({csv.field_size_limit()} characters)")


def test_quoted_comma_stays_one_record(tmp_path, config, conn):
    ref = f'{REF_HEADER}\nC1,"Smith, Jane"\n'
    run_ingest(conn, inputs(tmp_path, reference=ref), config)
    rows = bronze_repo.fetch_raw_records(conn, RUN_ID, "REFERENCE")
    assert [(r["line_no"], r["raw_text"]) for r in rows] == [(1, REF_HEADER), (2, 'C1,"Smith, Jane"')]


@pytest.mark.parametrize("eol", ["\n", "\r\n"])
def test_quoted_newline_keeps_all_lines_and_first_line_no(tmp_path, config, conn, eol):
    ref = f'{REF_HEADER}{eol}C1,"Line one{eol}line two"{eol}C2,Bob{eol}'
    result = run_ingest(conn, inputs(tmp_path, reference=ref), config)
    rows = bronze_repo.fetch_raw_records(conn, RUN_ID, "REFERENCE")
    assert [(r["line_no"], r["raw_text"]) for r in rows] == [
        (1, REF_HEADER), (2, f'C1,"Line one{eol}line two"'), (4, "C2,Bob")]
    assert result.sides[ing.REFERENCE].data_records == 2
    assert result.sides[ing.REFERENCE].physical_lines == 4


def test_blank_lines_are_counted_not_landed_and_keep_physical_line_numbers(tmp_path, config, conn):
    src = f"{SRC_HEADER}\n{SRC_ROW}\n\n   \n{SRC_ROW.replace('O1', 'O2')}\n\n"
    result = run_ingest(conn, inputs(tmp_path, source=src), config)
    rows = bronze_repo.fetch_raw_records(conn, RUN_ID, "SOURCE")
    assert [r["line_no"] for r in rows] == [1, 2, 5]
    assert result.sides[ing.SOURCE].blank_lines == 3
    assert result.sides[ing.SOURCE].data_records == 2


def test_malformed_rows_are_landed_verbatim(tmp_path, config, conn):
    src = f'{SRC_HEADER}\nO1,too,few\n{SRC_ROW},extra\nO3,"bad"quote,x\n'
    result = run_ingest(conn, inputs(tmp_path, source=src), config)
    rows = bronze_repo.fetch_raw_records(conn, RUN_ID, "SOURCE")
    assert [r["raw_text"] for r in rows[1:]] == ["O1,too,few", f"{SRC_ROW},extra", 'O3,"bad"quote,x']
    assert result.sides[ing.SOURCE].data_records == 3


def test_last_line_without_newline(tmp_path, config, conn):
    run_ingest(conn, inputs(tmp_path, source=f"{SRC_HEADER}\n{SRC_ROW}"), config)
    rows = bronze_repo.fetch_raw_records(conn, RUN_ID, "SOURCE")
    assert rows[-1]["raw_text"] == SRC_ROW


def test_landing_is_chunked(tmp_path, config, conn, monkeypatch):
    calls = []
    real = bronze_repo.insert_raw_records
    monkeypatch.setattr(ing, "INSERT_CHUNK_ROWS", 4)
    monkeypatch.setattr(bronze_repo, "insert_raw_records", lambda c, rows: (calls.append(len(rows)), real(c, rows)))
    body = "".join(SRC_ROW.replace("O1", f"O{i}") + "\n" for i in range(10))
    run_ingest(conn, inputs(tmp_path, source=f"{SRC_HEADER}\n{body}"), config)
    assert calls[:3] == [4, 4, 3]  # source: header + 10 rows = 11 records
    assert bronze_repo.count_raw_records(conn, RUN_ID, "SOURCE", "DATA") == 10


@pytest.mark.parametrize("change", [
    pytest.param(lambda p: p.write_bytes(p.read_bytes() + b"O9,C1,2026-01-01T00:00:00Z,PLACED,1.00,USD,,"
                                                         b"2026-01-01T00:00:00Z\n"), id="appended-row"),
    pytest.param(lambda p: p.write_bytes(p.read_bytes() + b"caf\xe9\n"), id="now-invalid-utf8"),
    pytest.param(lambda p: p.unlink(), id="deleted"),
])
def test_input_changed_between_passes_fails_the_run(tmp_path, config, conn, monkeypatch, change):
    paths = inputs(tmp_path)
    real_check_inputs = ing.check_inputs

    def check_then_modify(p, c):
        checks = real_check_inputs(p, c)
        change(paths[ing.TARGET])
        return checks

    monkeypatch.setattr(ing, "check_inputs", check_then_modify)
    with pytest.raises(ing.InputChangedError) as info:
        run_ingest(conn, paths, config)
    err = info.value
    expected_sha = hashlib.sha256(f"{TGT_HEADER}\n{TGT_ROW}\n".encode()).hexdigest()
    assert (err.side, err.expected_sha256) == ("TARGET", expected_sha)
    assert err.actual_sha256 != expected_sha
    if not paths[ing.TARGET].exists():
        assert err.actual_sha256 == "MISSING"
    assert bronze_repo.count_raw_records(conn, RUN_ID) == 0  # SOURCE had landed; the transaction rolled back

    # What the pipeline does with it: a separate short transaction marks the run FAILED.
    with transaction(conn):
        run_repo.finish_run(conn, RUN_ID, status="FAILED", finished_at="2026-11-15T00:01:00Z", exit_code=2,
                            failure_reason=err.failure_reason, failure_detail=err.failure_detail)
    row = run_repo.get_run(conn, RUN_ID)
    assert (row["status"], row["failure_reason"]) == ("FAILED", "INPUT_CHANGED")
    detail = json.loads(row["failure_detail"])
    assert detail == {"side": "TARGET", "file": str(paths[ing.TARGET]),
                      "expected_sha256": expected_sha, "actual_sha256": err.actual_sha256}


def test_unchanged_input_reports_the_landing_hash(tmp_path, config, conn):
    paths = inputs(tmp_path)
    result = run_ingest(conn, paths, config)
    for side, path in paths.items():
        assert result.sides[side].sha256 == hashlib.sha256(path.read_bytes()).hexdigest()


def test_land_file_refuses_rejected_check(tmp_path, config, conn):
    check = check_source(tmp_path, config, b"")
    with pytest.raises(ValueError):
        ing.land_file(conn, RUN_ID, check)


def test_generated_sample_lands_every_line(config, conn):
    from pathlib import Path
    root = Path(__file__).resolve().parents[2] / "data" / "sample"
    paths = {ing.SOURCE: root / "source.csv", ing.TARGET: root / "target.csv", ing.REFERENCE: root / "customers.csv"}
    result = run_ingest(conn, paths, config)
    assert result.ok and not result.warnings
    for side, path in paths.items():
        lines = path.read_bytes().decode("utf-8").splitlines()
        assert result.sides[side].data_records == len(lines) - 1
        assert bronze_repo.count_raw_records(conn, RUN_ID, side) == len(lines)
