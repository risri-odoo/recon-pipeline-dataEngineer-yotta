import copy
import json
from pathlib import Path

import pytest

from recon.service.config import Config, load_config
from tests.silver_helpers import REF_HEADER, SILVER_RUN_ID, SRC_HEADER, TGT_HEADER

ROOT = Path(__file__).resolve().parents[1]
ORDERS_CONFIG = ROOT / "config" / "orders_recon.json"


@pytest.fixture(scope="session")
def orders_config_dict() -> dict:
    return json.loads(ORDERS_CONFIG.read_text(encoding="utf-8"))


@pytest.fixture
def raw_config(orders_config_dict) -> dict:
    """A mutable deep copy of the shipped orders config."""
    return copy.deepcopy(orders_config_dict)


@pytest.fixture(scope="session")
def config() -> Config:
    return load_config(ORDERS_CONFIG)


# --------------------------------------------------------------------------- SILVER helpers

@pytest.fixture
def run_silver(tmp_path):
    """Land source/target/reference lines into a fresh DB and run the SILVER stage.

    Returns (conn, SilverResult). Data lines start at physical line 2 (line 1 is the header).
    """
    from recon.repository import run_repo
    from recon.repository.db import connect, transaction
    from recon.repository.schema import create_schema
    from recon.service import ingest
    from recon.service.dedupe import build_silver

    conns = []

    def _run(source=(), target=(), reference=("C1,Alice", "C2,Bob"), config=None, *,
             src_header=SRC_HEADER, tgt_header=TGT_HEADER, ref_header=REF_HEADER):
        config = config or load_config(ORDERS_CONFIG)
        files = {ingest.SOURCE: (src_header, source), ingest.TARGET: (tgt_header, target),
                 ingest.REFERENCE: (ref_header, reference)}
        paths = {}
        for side, (header, lines) in files.items():
            path = tmp_path / f"{side.lower()}.csv"
            path.write_bytes(("\n".join([header, *lines]) + "\n").encode("utf-8"))
            paths[side] = path
        conn = connect(tmp_path / "recon.db")
        conns.append(conn)
        create_schema(conn)
        with transaction(conn):
            run_repo.create_run(conn, run_id=SILVER_RUN_ID, run_key="k", attempt=1, status="RUNNING",
                                source_file="s", target_file="t", reference_file="r", config_sha256="c",
                                started_at="2026-11-15T00:00:00Z")
        with transaction(conn):
            landed = ingest.ingest(conn, SILVER_RUN_ID, paths, config)
            assert landed.ok, landed.rejects
            result = build_silver(conn, SILVER_RUN_ID, config)
        return conn, result

    yield _run
    for c in conns:
        c.close()


# --------------------------------------------------------------------------- GOLD helpers

@pytest.fixture
def run_gold(tmp_path, orders_config_dict):
    """Write source/target/reference lines, then run bronze -> silver -> gold in a fresh DB.

    Returns (conn, CompareResult). Each call uses its own directory, so one test can run several times.
    swap=True exchanges the two files and the config's columns.source / columns.target maps.
    """
    from recon.service import ingest
    from tests.gold_helpers import reconcile_files, swapped_config, swapped_paths

    conns = []

    def _run(source=(), target=(), reference=("C1,Alice", "C2,Bob"), config=None, *, swap=False,
             src_header=SRC_HEADER, tgt_header=TGT_HEADER, ref_header=REF_HEADER):
        work = tmp_path / f"run{len(conns)}"
        work.mkdir()
        files = {ingest.SOURCE: (src_header, source), ingest.TARGET: (tgt_header, target),
                 ingest.REFERENCE: (ref_header, reference)}
        paths = {}
        for side, (header, lines) in files.items():
            path = work / f"{side.lower()}.csv"
            path.write_bytes(("\n".join([header, *lines]) + "\n").encode("utf-8"))
            paths[side] = path
        if swap:
            config, paths = swapped_config(orders_config_dict), swapped_paths(paths)
        conn, result = reconcile_files(work / "recon.db", paths, config or load_config(ORDERS_CONFIG))
        conns.append(conn)
        return conn, result

    yield _run
    for c in conns:
        c.close()
