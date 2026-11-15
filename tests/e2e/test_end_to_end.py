"""End to end through the CLI: generator -> `recon run` -> report.json, and input-order determinism."""

import copy
import csv
import io
import json
import random
import shutil
from decimal import Decimal
from pathlib import Path

import pytest

from recon.presentation.cli import main
from recon.service import pipeline

ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "config" / "orders_recon.json"
SAMPLE = ROOT / "data" / "sample"
FILES = ("source.csv", "target.csv", "customers.csv")
# Keys of data/expected/<name>.json that describe the generator run, not the reconciliation result.
GENERATOR_METADATA = {"bucket_sizes", "generator_version", "n", "scenario_keys", "seed"}


@pytest.fixture(autouse=True)
def _no_fault_injection(monkeypatch):
    monkeypatch.delenv(pipeline.FAIL_AT_ENV, raising=False)


def run_cli(args: list[str], capsys) -> tuple[int, dict[str, str]]:
    capsys.readouterr()
    code = main(args)
    out = capsys.readouterr().out
    return code, dict(part.split("=", 1) for part in out.split())


def run_args(folder: Path, out: Path) -> list[str]:
    return ["run", "--source", str(folder / "source.csv"), "--target", str(folder / "target.csv"),
            "--reference", str(folder / "customers.csv"), "--config", str(CONFIG),
            "--db", str(out / "recon.db"), "--out", str(out)]


def generate(tmp_path: Path, n: int, seed: int, name: str) -> tuple[Path, dict]:
    gen_dir, exp_dir = tmp_path / "generated", tmp_path / "expected"
    assert main(["generate", "--n", str(n), "--seed", str(seed), "--name", name,
                 "--generated-dir", str(gen_dir), "--expected-dir", str(exp_dir)]) == 0
    return gen_dir / name, json.loads((exp_dir / f"{name}.json").read_text(encoding="utf-8"))


def as_expected(doc: dict) -> dict:
    """Project report.json onto the shape of data/expected/<name>.json (everything except generator metadata)."""
    overall = {a["metric"]: a for a in doc["aggregates"] if a["group_by"] == "ALL"}

    def money(metric: str, column: str) -> str:
        agg = overall[metric]
        return f"{Decimal(agg[column]).scaleb(-agg['scale']):.{agg['scale']}f}"

    sides = {}
    for side, column in (("SOURCE", "source_value"), ("TARGET", "target_value")):
        s = doc["sides"][side]
        sides[side] = {key: s[key] for key in ("rows_read", "accepted", "duplicate_exact", "superseded",
                                               "conflicting_rows", "rejected_by_reason")}
        sides[side] |= {"sum_amount": money("SUM_AMOUNT", column), "sum_discount": money("SUM_DISCOUNT", column)}
    sides["REFERENCE"] = {key: doc["sides"]["REFERENCE"][key] for key in ("rows_read", "accepted")}
    return {
        "field_diff_counts": [{"count": n, "diff_type": t, "field_name": f}
                              for f, by_type in sorted(doc["field_diff_counts"].items())
                              for t, n in sorted(by_type.items()) if n],
        "key_status_counts": doc["key_status_counts"],
        "recon_status": doc["run"]["recon_status"],
        "sides": sides,
        "total_keys": doc["totals"]["keys"],
        "unattributable_rejects": doc["totals"]["unattributable_rejects"],
    }


# --------------------------------------------------------------------------- generator -> report


def test_generated_10k_report_matches_expected_answers(tmp_path, capsys):
    folder, expected = generate(tmp_path, 10_000, 7, "e2e_10k")
    assert "scenario_keys" not in expected  # N > 1000: counts only

    code, fields = run_cli(run_args(folder, tmp_path / "out"), capsys)

    assert fields["status"] == "SUCCEEDED"
    doc = json.loads(Path(fields["report"]).read_text(encoding="utf-8"))
    answers = {k: v for k, v in expected.items() if k not in GENERATOR_METADATA}
    got = as_expected(doc)
    assert set(got) == set(answers)  # a new field in expected/<name>.json must be mapped here, not skipped
    for key in sorted(answers):
        assert got[key] == answers[key], key
    assert code == (0 if expected["recon_status"] == "PASS" else 1)
    assert doc["inputs"]["SOURCE"]["data_records"] == expected["sides"]["SOURCE"]["rows_read"]
    assert all(c["status"] == "PASS" for c in doc["checks"])
    assert all(a["unexplained_delta"] == 0 for a in doc["aggregates"])


# --------------------------------------------------------------------------- determinism


def shuffle_data_rows(path: Path, rng: random.Random) -> None:
    """Shuffle every data line of a CSV in place; the header stays first."""
    header, *rows = path.read_bytes().decode("utf-8").splitlines(keepends=True)
    assert len(list(csv.reader(io.StringIO("".join(rows))))) == len(rows)  # one physical line per record
    shuffled = rows[:]
    while shuffled == rows:
        rng.shuffle(shuffled)
    path.write_bytes((header + "".join(shuffled)).encode("utf-8"))


def mask_order_dependent(doc: dict) -> dict:
    """Blank the fields that legitimately change when the same rows arrive in a different order:
    run timestamps, input hashes and what derives from them (run_key, run_id), and physical line numbers."""
    doc = copy.deepcopy(doc)
    for key in ("run_id", "run_key", "started_at", "finished_at"):
        doc["run"].pop(key)
    for side in doc["inputs"].values():
        side.pop("sha256")
    for sample in doc["mismatch_samples"]["keys"]:
        sample.pop("source_line_no")
        sample.pop("target_line_no")
    return doc


def csv_rows_without(path: Path, *columns: str) -> list[dict[str, str]]:
    rows = csv.DictReader(io.StringIO(path.read_bytes().decode("utf-8"), newline=""))
    return [{k: v for k, v in r.items() if k not in columns} for r in rows]


@pytest.mark.parametrize("dataset", ["sample", "generated-2000"])
def test_shuffled_inputs_give_an_identical_report(tmp_path, capsys, dataset):
    folder = tmp_path / "in"
    if dataset == "sample":
        folder.mkdir()
        for name in FILES:
            shutil.copyfile(SAMPLE / name, folder / name)
    else:
        generated, _ = generate(tmp_path, 2000, 3, "shuffle")
        shutil.copytree(generated, folder)

    _, a = run_cli(run_args(folder, tmp_path / "a"), capsys)
    rng = random.Random(7)
    for name in ("source.csv", "target.csv"):  # same paths, shuffled bytes, fresh DB
        shuffle_data_rows(folder / name, rng)
    _, b = run_cli(run_args(folder, tmp_path / "b"), capsys)

    assert a["status"] == b["status"] == "SUCCEEDED"
    dir_a, dir_b = tmp_path / "a" / a["run_id"], tmp_path / "b" / b["run_id"]
    doc_a, doc_b = (json.loads((d / "report.json").read_text(encoding="utf-8")) for d in (dir_a, dir_b))
    assert doc_a["inputs"]["SOURCE"]["sha256"] != doc_b["inputs"]["SOURCE"]["sha256"]  # the input really changed
    assert mask_order_dependent(doc_a) == mask_order_dependent(doc_b)
    assert csv_rows_without(dir_a / "field_diffs.csv", "run_id") == csv_rows_without(dir_b / "field_diffs.csv", "run_id")
    assert csv_rows_without(dir_a / "mismatches.csv", "run_id", "source_line_no", "target_line_no") == \
        csv_rows_without(dir_b / "mismatches.csv", "run_id", "source_line_no", "target_line_no")


# --------------------------------------------------------------------------- composite key


def test_composite_key_with_separator_characters_round_trips_through_every_report_file(tmp_path, capsys):
    from recon.presentation.report_writer import md_cell
    from tests.silver_helpers import REF_HEADER, SRC_HEADER, TGT_HEADER, order_row

    raw = json.loads(CONFIG.read_text(encoding="utf-8"))
    raw["business_key"] = ["order_id", "currency"]
    raw["compare_fields"].remove("currency")
    config = tmp_path / "composite.json"
    config.write_text(json.dumps(raw), encoding="utf-8")
    folder = tmp_path / "in"
    folder.mkdir()
    source = [order_row("A|B", currency="USD", amount="10.00"), order_row("A", currency="USD"),
              order_row("C\\", currency="EUR")]
    target = [order_row("A|B", currency="USD", amount="12.00"), order_row("A", currency="EUR"),
              order_row("C\\", currency="EUR")]
    for name, header, lines in (("source.csv", SRC_HEADER, source), ("target.csv", TGT_HEADER, target),
                                ("customers.csv", REF_HEADER, ["C1,Alice"])):
        (folder / name).write_bytes(("\n".join([header, *lines]) + "\n").encode("utf-8"))
    args = run_args(folder, tmp_path / "out")
    args[args.index("--config") + 1] = str(config)

    code, fields = run_cli(args, capsys)

    assert (code, fields["status"]) == (1, "SUCCEEDED")
    run_dir = tmp_path / "out" / fields["run_id"]
    doc = json.loads((run_dir / "report.json").read_text(encoding="utf-8"))
    assert doc["run"]["business_key"] == ["order_id", "currency"]
    mismatches = {r["record_key"]: r for r in csv_rows_without(run_dir / "mismatches.csv", "run_id")}
    assert {k: r["key_status"] for k, r in mismatches.items()} == {
        "A\\|B|USD": "MISMATCHED", "A|EUR": "EXTRA_IN_TARGET", "A|USD": "MISSING_IN_TARGET"}  # C\ matched
    assert doc["key_status_counts"]["MATCHED"] == 1
    [diff] = csv_rows_without(run_dir / "field_diffs.csv", "run_id")
    assert (diff["record_key"], diff["column_name"], diff["source_value"], diff["target_value"]) == \
        ("A\\|B|USD", "amount", "10.00", "12.00")
    summary = (run_dir / "summary.md").read_text(encoding="utf-8")
    assert md_cell("A\\|B|USD") in summary  # escaped, so the key cannot break the Markdown table
    assert "| A\\|B|USD |" not in summary
