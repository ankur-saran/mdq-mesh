"""Unit tests for YFinanceAgent (FR-A1).

All tests run fully offline — live yfinance calls are replaced by synthetic Bronze
fixtures injected via the harness fixture mechanism (FR-T2, runtime.use_fixtures=True).
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import pytest
import yaml

import harness.fixtures as fixtures_mod
from harness.fixtures import snapshot
from mdq.core.blackboard import Blackboard
from mdq.core.config import Config
from mdq.core.events import Event, TopicType
from mdq.core.store import MedallionStore

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_INSTRUMENTS = [
    ("AAPL", "AAPL"),
    ("MSFT", "MSFT"),
    ("NVDA", "NVDA"),
    ("JPM", "JPM"),
    ("SPY", "SPY"),
]

_BDATE = date(2024, 1, 2)


def _make_bronze_df(
    instruments: list[tuple[str, str]] = _INSTRUMENTS,
    business_date: date = _BDATE,
) -> pd.DataFrame:
    """Synthetic Bronze-format DataFrame — identical columns to a live yfinance fetch."""
    fetch_ts = pd.Timestamp(datetime(2024, 1, 2, 21, 0, 0, tzinfo=UTC))
    rows = []
    for instrument_id, source_symbol in instruments:
        rows.append(
            {
                "Date": pd.Timestamp(business_date),
                "Open": 100.0,
                "High": 110.0,
                "Low": 95.0,
                "Close": 102.0,
                "Adj Close": 101.5,
                "Volume": 1_000_000,
                "instrument_id": instrument_id,
                "source_symbol": source_symbol,
                "fetch_ts": fetch_ts,
            }
        )
    return pd.DataFrame(rows)


def _make_store(tmp_path: Path) -> MedallionStore:
    store = MedallionStore(
        bronze_root=tmp_path / "bronze",
        silver_root=tmp_path / "silver",
        gold_root=tmp_path / "gold",
        quarantine_root=tmp_path / "quarantine",
        lineage_root=tmp_path / "lineage",
        duckdb_path=tmp_path / "mdq.duckdb",
    )
    store.init_dirs()
    store.open()
    return store


def _make_cfg(tmp_path: Path, use_fixtures: bool = True) -> Config:
    base = yaml.safe_load(Path("config/default.yaml").read_text())
    base["runtime"]["use_fixtures"] = use_fixtures
    base["runtime"]["storage"]["bronze"] = str(tmp_path / "bronze")
    base["runtime"]["storage"]["silver"] = str(tmp_path / "silver")
    base["runtime"]["storage"]["gold"] = str(tmp_path / "gold")
    base["runtime"]["storage"]["quarantine"] = str(tmp_path / "quarantine")
    base["runtime"]["storage"]["lineage"] = str(tmp_path / "lineage")
    base["runtime"]["duckdb_path"] = str(tmp_path / "mdq.duckdb")
    universe = yaml.safe_load(Path("config/universe.yaml").read_text())
    base["universe"] = universe
    return Config.model_validate(base)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_fixture_mode_writes_bronze(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """INGESTION_COMPLETE published and Bronze Parquet written when use_fixtures=True."""
    monkeypatch.setattr(fixtures_mod, "_FIXTURES_DIR", tmp_path / "fixtures")

    bronze_df = _make_bronze_df()
    snapshot("yfinance", bronze_df, tag="default")

    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path, use_fixtures=True)
    bb = Blackboard(db_path=":memory:")
    bb.open()

    from mdq.agents.ingestion.yfinance_agent import YFinanceAgent

    agent = YFinanceAgent(bb, store, cfg)
    event = Event(
        topic=TopicType.RUN_STARTED,
        agent="supervisor",
        run_id="run-1",
        payload={"business_date": _BDATE.isoformat()},
    )
    await agent.act(event)

    # Bronze file must exist
    bronze_path = store.bronze_path("yfinance", "run-1", _BDATE)
    assert bronze_path.exists(), f"Expected Bronze at {bronze_path}"

    # Bronze round-trip preserves row count
    stored = pd.read_parquet(bronze_path)
    assert len(stored) == len(bronze_df)

    # INGESTION_COMPLETE published (not INGESTION_FAILED)
    events = bb.get_events(topic=TopicType.INGESTION_COMPLETE)
    assert len(events) == 1
    payload = json.loads(events[0]["payload"])
    assert payload["source_id"] == "yfinance"
    assert payload["row_count"] == len(bronze_df)

    store.close()
    bb.close()


async def test_missing_fixture_publishes_ingestion_failed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If fixture file does not exist, INGESTION_FAILED is published (not a crash)."""
    monkeypatch.setattr(fixtures_mod, "_FIXTURES_DIR", tmp_path / "fixtures")
    # No snapshot() call — fixture is absent

    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path, use_fixtures=True)
    bb = Blackboard(db_path=":memory:")
    bb.open()

    from mdq.agents.ingestion.yfinance_agent import YFinanceAgent

    agent = YFinanceAgent(bb, store, cfg)
    event = Event(
        topic=TopicType.RUN_STARTED,
        agent="supervisor",
        run_id="run-2",
        payload={"business_date": _BDATE.isoformat()},
    )
    await agent.act(event)

    failed = bb.get_events(topic=TopicType.INGESTION_FAILED)
    assert len(failed) == 1
    payload = json.loads(failed[0]["payload"])
    assert payload["source_id"] == "yfinance"
    assert "error" in payload

    # No Bronze file created
    assert not store.bronze_path("yfinance", "run-2", _BDATE).exists()

    store.close()
    bb.close()


async def test_subscriptions_and_name() -> None:
    """Agent metadata is correct without constructing real deps."""
    from unittest.mock import MagicMock

    from mdq.agents.ingestion.yfinance_agent import YFinanceAgent

    bb = MagicMock()
    store = MagicMock()
    cfg = MagicMock()
    cfg.universe.instruments = []

    agent = YFinanceAgent(bb, store, cfg)
    assert agent.name == "yfinance_source"
    assert TopicType.RUN_STARTED in agent.subscriptions


async def test_should_act_filters_by_topic() -> None:
    """should_act returns True only for RUN_STARTED."""
    from unittest.mock import MagicMock

    from mdq.agents.ingestion.yfinance_agent import YFinanceAgent

    bb = MagicMock()
    store = MagicMock()
    cfg = MagicMock()
    cfg.universe.instruments = []

    agent = YFinanceAgent(bb, store, cfg)

    run_started = Event(
        topic=TopicType.RUN_STARTED,
        agent="supervisor",
        run_id="x",
        payload={"business_date": "2024-01-02"},
    )
    other = Event(topic=TopicType.INGESTION_COMPLETE, agent="supervisor", run_id="x")
    assert agent.should_act(run_started) is True
    assert agent.should_act(other) is False


async def test_bronze_immutability_skip_on_second_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Writing Bronze twice for the same run_id + date skips the second write (FR-S1)."""
    monkeypatch.setattr(fixtures_mod, "_FIXTURES_DIR", tmp_path / "fixtures")

    bronze_df = _make_bronze_df()
    snapshot("yfinance", bronze_df, tag="default")

    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path, use_fixtures=True)
    bb = Blackboard(db_path=":memory:")
    bb.open()

    from mdq.agents.ingestion.yfinance_agent import YFinanceAgent

    agent = YFinanceAgent(bb, store, cfg)
    event = Event(
        topic=TopicType.RUN_STARTED,
        agent="supervisor",
        run_id="run-imm",
        payload={"business_date": _BDATE.isoformat()},
    )

    await agent.act(event)
    mtime_first = store.bronze_path("yfinance", "run-imm", _BDATE).stat().st_mtime

    # Act again (same run_id, same date) — Bronze must NOT be overwritten
    await agent.act(event)
    mtime_second = store.bronze_path("yfinance", "run-imm", _BDATE).stat().st_mtime

    assert mtime_first == mtime_second, "Bronze was mutated on second write — FR-S1 violated"

    store.close()
    bb.close()
