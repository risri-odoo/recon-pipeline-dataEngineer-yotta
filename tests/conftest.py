import copy
import json
from pathlib import Path

import pytest

from recon.service.config import Config, load_config

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
