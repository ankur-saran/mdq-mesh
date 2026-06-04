"""Unit tests for config loading and validation (PRD §11, NFR-9)."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from mdq.core.config import Config, load_config


def test_load_default_config() -> None:
    cfg = load_config("config/default.yaml", "config/universe.yaml")
    assert cfg.runtime.transport == "asyncio"
    assert cfg.runtime.parallelism == 4
    assert cfg.sources.yfinance.enabled is True
    assert cfg.sources.ecb.enabled is False
    assert cfg.narrator.enabled is False


def test_universe_loaded() -> None:
    cfg = load_config("config/default.yaml", "config/universe.yaml")
    assert len(cfg.universe.instruments) == 5
    ids = [i.instrument_id for i in cfg.universe.instruments]
    assert "AAPL" in ids
    assert "SPY" in ids


def test_instrument_symbol_mappings() -> None:
    cfg = load_config("config/default.yaml", "config/universe.yaml")
    aapl = next(i for i in cfg.universe.instruments if i.instrument_id == "AAPL")
    assert aapl.symbols["yfinance"] == "AAPL"
    assert aapl.symbols["stooq"] == "aapl.us"


def test_reconciliation_tolerances() -> None:
    cfg = load_config("config/default.yaml", "config/universe.yaml")
    assert cfg.reconciliation.tolerances["CLOSE"].value == 25
    assert cfg.reconciliation.tolerances["VOLUME"].type == "relative_pct"
    assert cfg.reconciliation.quorum.min_agreeing_sources == 2


def test_remediation_config() -> None:
    cfg = load_config("config/default.yaml", "config/universe.yaml")
    assert cfg.remediation.max_retries == 3
    assert cfg.remediation.lookback_widen_steps == [5, 20, 60]
    assert cfg.remediation.hold_downstream_on_break is True


def test_invalid_parallelism_type() -> None:
    with pytest.raises(ValidationError):
        Config.model_validate({"runtime": {"parallelism": "not_an_int"}})


def test_missing_universe_file_is_ok(tmp_path: object) -> None:
    cfg = load_config("config/default.yaml", "/nonexistent/universe.yaml")
    assert cfg.universe.instruments == []
