"""`recon bench` (recon/presentation/bench.py) and the one-statement aggregates it motivated (docs/PERFORMANCE.md)."""

from pathlib import Path

from recon.presentation import bench
from recon.presentation.cli import main
from recon.repository import gold_repo

ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "config" / "orders_recon.json"


def bench_cli(doc: Path, label: str, *extra: str) -> list[str]:
    return ["bench", "--n", "60", "--seed", "42", "--repeat", "2", "--config", str(CONFIG), "--doc", str(doc),
            "--label", label, *extra]


def table_rows(doc: Path) -> list[list[str]]:
    body = doc.read_text(encoding="utf-8").split(bench.TABLE_START)[1].split(bench.TABLE_END)[0]
    lines = [line for line in body.splitlines() if line.strip()][2:]  # skip header and separator
    return [[c.strip() for c in line.strip("|").split("|")] for line in lines]


def test_bench_creates_the_document_and_appends_one_row_per_run(tmp_path, capsys):
    doc = tmp_path / "PERFORMANCE.md"
    assert main(bench_cli(doc, "first")) == 0
    assert main(bench_cli(doc, "second")) == 0

    rows = table_rows(doc)
    assert [r[0] for r in rows] == ["first", "second"]
    header = [c.strip() for c in bench.TABLE_HEADER.splitlines()[0].strip("|").split("|")]
    for row in rows:
        cells = dict(zip(header, row))
        assert (cells["n"], cells["seed"], cells["runs"]) == ("60", "42", "2")
        assert cells["data records S / T / R"] == "63 / 61 / 5"  # the committed sample (n=60, seed=42)
        assert (cells["keys"], cells["field diffs"]) == ("61", "3")
        assert float(cells["total s"]) > 0 and float(cells["peak MiB (tracemalloc)"]) > 0
    assert rows[0][-1] == rows[1][-1]  # same inputs -> same result digest across invocations
    assert "appended to" in capsys.readouterr().out


def test_bench_keeps_hand_written_sections(tmp_path):
    doc = tmp_path / "PERFORMANCE.md"
    doc.write_text("# Perf\n\nIntro.\n\n## Results\n\n" + bench.TABLE_START + "\n" + bench.TABLE_HEADER
                   + bench.TABLE_END + "\n\n## Analysis\n\nKeep me.\n", encoding="utf-8")
    assert main(bench_cli(doc, "only")) == 0
    text = doc.read_text(encoding="utf-8")
    assert text.startswith("# Perf\n\nIntro.") and text.endswith("## Analysis\n\nKeep me.\n")
    assert [r[0] for r in table_rows(doc)] == ["only"]


def test_bench_rejects_bad_arguments(tmp_path, capsys):
    assert main(bench_cli(tmp_path / "p.md", "x", "--repeat", "0")) == 2
    assert main(["bench", "--n", "3", "--doc", str(tmp_path / "p.md")]) == 2  # generator minimum
    assert not (tmp_path / "p.md").exists()
    capsys.readouterr()


def test_report_digest_masks_run_timestamps_and_the_input_directory(tmp_path):
    def run_dir(name: str, input_dir: str, ts: str, body: str = "x") -> Path:
        d = tmp_path / name
        d.mkdir()
        for f in bench.REPORT_FILES:
            d.joinpath(f).write_text(f'{{"started_at": "{ts}", "file": "{input_dir}\\\\s.csv", "v": "{body}"}}\n',
                                     encoding="utf-8")
        return d

    a = run_dir("a", "C:\\\\tmp\\\\one", "2026-11-15T10:00:00Z")
    b = run_dir("b", "C:\\\\tmp\\\\two", "2026-11-15T11:11:11Z")
    c = run_dir("c", "C:\\\\tmp\\\\two", "2026-11-15T11:11:11Z", body="y")
    assert bench.report_digest(a, Path("C:\\tmp\\one")) == bench.report_digest(b, Path("C:\\tmp\\two"))
    assert bench.report_digest(b, Path("C:\\tmp\\two")) != bench.report_digest(c, Path("C:\\tmp\\two"))


def test_all_aggregates_are_one_statement():
    measures = [gold_repo.Measure("ROW_COUNT", None), gold_repo.Measure("SUM_AMOUNT", "amount_minor")]
    groupings = [gold_repo.Grouping("ALL", None), gold_repo.Grouping("STATUS", "status"),
                 gold_repo.Grouping("CURRENCY", "currency")]
    sql = gold_repo.aggregates_sql(measures, groupings)
    assert sql.count("INSERT INTO aggregate_result") == 1
    assert sql.count("UNION ALL\nSELECT :run_id") == len(measures) * len(groupings) - 1
    assert sql.count("FROM key_result AS k") == 2  # the walk is written once (source + target branch)
    params = gold_repo.aggregates_params("r1", measures, groupings)
    assert params == {"run_id": "r1", "metric_0": "ROW_COUNT", "metric_1": "SUM_AMOUNT",
                      "group_by_0": "ALL", "group_by_1": "STATUS", "group_by_2": "CURRENCY"}
