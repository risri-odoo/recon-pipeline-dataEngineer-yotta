import ast
import csv
import io
from pathlib import Path

import pytest

from recon.generator import generate as gen
from recon.presentation.cli import main

ROOT = Path(__file__).resolve().parents[2]
FILES = ("source.csv", "target.csv", "customers.csv")


def _run_cli(tmp_path, sub, n, seed, name="ds"):
    gen_dir, exp_dir = tmp_path / sub / "generated", tmp_path / sub / "expected"
    rc = main(["generate", "--n", str(n), "--seed", str(seed), "--name", name,
               "--generated-dir", str(gen_dir), "--expected-dir", str(exp_dir)])
    assert rc == 0
    return {f: (gen_dir / name / f).read_bytes() for f in FILES} | {"expected": (exp_dir / f"{name}.json").read_bytes()}


def _data_rows(payload: bytes) -> list[list[str]]:
    return list(csv.reader(io.StringIO(payload.decode("utf-8"))))[1:]


# --------------------------------------------------------------------------- determinism


def test_same_seed_gives_byte_identical_files(tmp_path):
    a = _run_cli(tmp_path, "a", 500, 7)
    b = _run_cli(tmp_path, "b", 500, 7)
    assert a == b


def test_different_seed_gives_different_files(tmp_path):
    a = _run_cli(tmp_path, "a", 500, 7)
    b = _run_cli(tmp_path, "b", 500, 8)
    assert a["source.csv"] != b["source.csv"]
    assert a["expected"] != b["expected"]


def test_files_use_lf_and_utf8_without_bom():
    ds = gen.generate(100, 1)
    for payload in ds.files.values():
        assert b"\r" not in payload
        assert not payload.startswith(b"\xef\xbb\xbf")
        assert payload.endswith(b"\n")


def test_committed_sample_matches_generator():
    ds = gen.generate(60, 42)
    for name, payload in ds.files.items():
        assert (ROOT / "data" / "sample" / name).read_bytes() == payload, name
    assert (ROOT / "data" / "expected" / "sample.json").read_bytes() == gen.expected_json_bytes(ds.expected)


# --------------------------------------------------------------------------- buckets


@pytest.mark.parametrize("n, seed", [(60, 42), (2000, 3)])
def test_buckets_are_disjoint_and_non_empty(n, seed):
    exp = gen.generate(n, seed).expected
    buckets = exp["scenario_keys"]
    assert set(buckets) == {name for name, _ in gen.SCENARIOS}
    seen: set[str] = set()
    for name, keys in buckets.items():
        assert len(keys) >= 1, name
        assert len(set(keys)) == len(keys), name
        assert seen.isdisjoint(keys), name
        seen.update(keys)


def test_bucket_sizes_follow_rates():
    exp = gen.generate(2000, 3).expected
    for name, per_mille in gen.SCENARIOS:
        assert exp["bucket_sizes"][name] == 2000 * per_mille // 1000, name


def test_extra_keys_are_new_and_base_buckets_are_base_keys():
    ds = gen.generate(300, 5)
    source_keys = {row[0] for row in _data_rows(ds.source_csv)}
    buckets = ds.expected["scenario_keys"]
    assert source_keys.isdisjoint(buckets["extra_in_target"])
    for name, keys in buckets.items():
        if name != "extra_in_target":
            assert set(keys) <= source_keys, name


def test_n_too_small_is_rejected():
    with pytest.raises(ValueError):
        gen.generate(gen.min_n() - 1, 1)
    gen.generate(gen.min_n(), 1)


def test_cli_rejects_too_small_n(tmp_path, capsys):
    rc = main(["generate", "--n", "3", "--seed", "1", "--name", "x",
               "--generated-dir", str(tmp_path), "--expected-dir", str(tmp_path)])
    assert rc == 2
    assert "n must be" in capsys.readouterr().err


# --------------------------------------------------------------------------- expected answers


@pytest.mark.parametrize("n, seed", [(60, 42), (1000, 11), (2000, 3)])
def test_expected_counts_are_internally_consistent(n, seed):
    ds = gen.generate(n, seed)
    exp = ds.expected
    for side, payload in (("SOURCE", ds.source_csv), ("TARGET", ds.target_csv)):
        s = exp["sides"][side]
        assert s["rows_read"] == len(_data_rows(payload)), side
        assert s["rows_read"] == (s["accepted"] + sum(s["rejected_by_reason"].values())
                                  + s["duplicate_exact"] + s["superseded"] + s["conflicting_rows"]), side
    ref = exp["sides"]["REFERENCE"]
    assert ref["rows_read"] == len(_data_rows(ds.customers_csv)) == max(1, n // 50)

    total_keys = n + exp["bucket_sizes"]["extra_in_target"]
    assert exp["total_keys"] == total_keys
    assert sum(exp["key_status_counts"].values()) == total_keys
    distinct_keys = {r[0] for r in _data_rows(ds.source_csv)} | {r[0] for r in _data_rows(ds.target_csv)}
    assert len(distinct_keys) == total_keys

    # Clean source keys = every base key except the source-rejected / conflicting / malformed ones.
    b = exp["bucket_sizes"]
    src = exp["sides"]["SOURCE"]
    assert src["accepted"] == n - b["src_conflict"] - b["src_invalid_ref"] - b["src_missing_required"] \
        - b["src_bad_type"] - b["src_malformed"]
    tgt = exp["sides"]["TARGET"]
    assert tgt["accepted"] == n - b["missing_in_target"] - b["tgt_bad_type"] + b["extra_in_target"]

    # MISMATCHED keys each carry exactly one field diff.
    assert sum(d["count"] for d in exp["field_diff_counts"]) == exp["key_status_counts"]["MISMATCHED"]
    assert exp["recon_status"] == "FAIL"


def test_sums_are_decimal_strings():
    exp = gen.generate(60, 42).expected
    for side in ("SOURCE", "TARGET"):
        for key in ("sum_amount", "sum_discount"):
            value = exp["sides"][side][key]
            assert isinstance(value, str)
            whole, frac = value.split(".")
            assert whole.isdigit() and len(frac) == 2


def test_target_is_shuffled_and_renamed_and_source_is_key_ordered():
    ds = gen.generate(500, 9)
    source_keys = [r[0] for r in _data_rows(ds.source_csv)]
    target_keys = [r[0] for r in _data_rows(ds.target_csv)]
    assert source_keys == sorted(source_keys)
    assert target_keys != sorted(target_keys)
    assert ds.target_csv.split(b"\n", 1)[0].decode() == ",".join(gen.TARGET_HEADER)


def test_out_of_order_older_version_is_written_after_newer():
    ds = gen.generate(1000, 2)
    rows = _data_rows(ds.source_csv)
    for key in ds.expected["scenario_keys"]["src_out_of_order"]:
        versions = [r[7] for r in rows if r[0] == key]
        assert len(versions) == 2 and versions[0] > versions[1], key


# --------------------------------------------------------------------------- independence


def test_generator_does_not_import_pipeline_code():
    tree = ast.parse(Path(gen.__file__).read_text(encoding="utf-8"))
    modules = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            modules.add(node.module or "")
    assert not any(m.startswith("recon") for m in modules), modules
