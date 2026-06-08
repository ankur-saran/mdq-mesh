"""Unit tests for StooqAgent — fixture mode, name/subscriptions, failure path (FR-A1, NFR-3)."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
import yaml

import harness.fixtures as fixtures_mod
from harness.fixtures import snapshot
from mdq.agents.ingestion.stooq_agent import StooqAgent
from mdq.core.blackboard import Blackboard
from mdq.core.config import Config
from mdq.core.events import Event, TopicType
from mdq.core.store import MedallionStore

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_BDATE = date(2024, 1, 2)
_SOURCE_ID = "stooq"
_INSTRUMENTS = [
    ("AAPL", "aapl.us"),
    ("MSFT", "msft.us"),
    ("NVDA", "nvda.us"),
    ("JPM", "jpm.us"),
    ("SPY", "spy.us"),
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_stooq_bronze_df(business_date: date = _BDATE) -> pd.DataFrame:
    """Return a Stooq-format Bronze DataFrame in fixture mode."""
    fetch_ts = pd.Timestamp(datetime(2024, 1, 2, 21, 0, 0, tzinfo=UTC))
    rows = []
    for instrument_id, source_symbol in _INSTRUMENTS:
        rows.append(
            {
                "Date": pd.Timestamp(business_date),
                "Open": 100.0,
                "High": 110.0,
                "Low": 95.0,
                "Close": 102.0,
                "Adj Close": 102.0,
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


def _run_started(run_id: str = "stooq-run") -> Event:
    return Event(
        topic=TopicType.RUN_STARTED,
        agent="supervisor",
        run_id=run_id,
        payload={"business_date": _BDATE.isoformat()},
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_stooq_agent_fixture_mode_writes_bronze(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """In fixture mode, StooqAgent reads the frozen fixture and writes Bronze."""
    monkeypatch.setattr(fixtures_mod, "_FIXTURES_DIR", tmp_path / "fixtures")
    snapshot(_SOURCE_ID, _make_stooq_bronze_df(), tag="default")

    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path, use_fixtures=True)

    bb = Blackboard(db_path=":memory:")
    bb.register(StooqAgent(bb, store, cfg))
    await bb.start()
    await bb.publish(_run_started())
    await bb.drain()

    events = bb.get_events(topic=TopicType.INGESTION_COMPLETE)
    assert len(events) == 1
    import json

    payload = json.loads(events[0]["payload"])
    assert payload["source_id"] == _SOURCE_ID
    assert payload["row_count"] == len(_INSTRUMENTS)

    bronze_path = store.bronze_path(_SOURCE_ID, "stooq-run", _BDATE)
    assert bronze_path.exists(), f"Bronze not written: {bronze_path}"
    df = pd.read_parquet(bronze_path)
    assert len(df) == len(_INSTRUMENTS)

    await bb.stop()
    store.close()


async def test_stooq_agent_ingestion_failed_on_error(tmp_path: Path) -> None:
    """If fetch raises, INGESTION_FAILED is published with source_id and business_date."""
    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path, use_fixtures=False)

    patch_target = "mdq.agents.ingestion.stooq_agent._fetch_blocking"
    with patch(patch_target, side_effect=RuntimeError("network error")):
        bb = Blackboard(db_path=":memory:")
        bb.register(StooqAgent(bb, store, cfg))
        await bb.start()
        await bb.publish(_run_started("fail-run"))
        await bb.drain()

    failed_events = bb.get_events(topic=TopicType.INGESTION_FAILED)
    assert len(failed_events) == 1
    import json

    payload = json.loads(failed_events[0]["payload"])
    assert payload["source_id"] == _SOURCE_ID
    assert payload["business_date"] == _BDATE.isoformat()
    assert "error" in payload

    await bb.stop()
    store.close()


def test_stooq_agent_name_and_subscriptions(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)
    bb = Blackboard(db_path=":memory:")
    agent = StooqAgent(bb, store, cfg)
    assert agent.name == "stooq_source"
    assert TopicType.RUN_STARTED in agent.subscriptions
    store.close()
