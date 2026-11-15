"""Shared helpers for GOLD tests: run bronze -> silver -> gold in one transaction, and swap sides."""

import copy
from pathlib import Path

from recon.repository import gold_repo, run_repo
from recon.repository.db import connect, transaction
from recon.repository.schema import create_schema
from recon.service import ingest
from recon.service.compare import reconcile
from recon.service.config import Config, parse_config
from recon.service.dedupe import build_silver

GOLD_RUN_ID = "gold-a1"
ROOT = Path(__file__).resolve().parents[1]
SAMPLE = ROOT / "data" / "sample"
SAMPLE_PATHS = {ingest.SOURCE: SAMPLE / "source.csv", ingest.TARGET: SAMPLE / "target.csv",
                ingest.REFERENCE: SAMPLE / "customers.csv"}


def create_run_row(conn, run_id=GOLD_RUN_ID):
    with transaction(conn):
        run_repo.create_run(conn, run_id=run_id, run_key="k", attempt=1, status="RUNNING", source_file="s",
                            target_file="t", reference_file="r", config_sha256="c",
                            started_at="2026-11-15T00:00:00Z")


def reconcile_files(db_path, paths, config: Config, run_id=GOLD_RUN_ID):
    """Fresh DB at db_path; land, build SILVER and GOLD in one data transaction. Returns (conn, CompareResult)."""
    conn = connect(db_path)
    create_schema(conn)
    create_run_row(conn, run_id)
    with transaction(conn):
        landed = ingest.ingest(conn, run_id, paths, config)
        assert landed.ok, landed.rejects
        build_silver(conn, run_id, config)
        result = reconcile(conn, run_id, config, landed.warnings)
    return conn, result


def swapped_config(raw_config: dict) -> Config:
    """The same config with columns.source and columns.target exchanged."""
    raw = copy.deepcopy(raw_config)
    raw["columns"]["source"], raw["columns"]["target"] = raw["columns"]["target"], raw["columns"]["source"]
    return parse_config(raw)


def swapped_paths(paths: dict) -> dict:
    return {**paths, ingest.SOURCE: paths[ingest.TARGET], ingest.TARGET: paths[ingest.SOURCE]}


def gold_snapshot(conn, run_id=GOLD_RUN_ID):
    """Every GOLD table of a run, sorted, without run_id (for comparing two runs)."""
    def strip(rows):
        return [{k: v for k, v in r.items() if k != "run_id"} for r in rows]
    return {"key_result": strip(gold_repo.fetch_key_results(conn, run_id)),
            "field_diff": strip(gold_repo.fetch_field_diffs(conn, run_id)),
            "aggregate_result": strip(gold_repo.fetch_aggregates(conn, run_id)),
            "check_result": strip(gold_repo.fetch_check_results(conn, run_id))}
