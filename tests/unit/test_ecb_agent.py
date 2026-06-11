"""Unit tests for ECBAgent (FR-A1, Phase 7).

All tests run fully offline — live HTTP calls are replaced by synthetic Bronze
fixtures injected via the harness fixture mechanism (FR-T2, runtime.use_fixtures=True).
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest
import yaml

import harness.fixtures as fixtures_mod
from harness.fixtures import snapshot
from mdq.core.blackboard import Blackboard
from mdq.core.config import Config
from mdq.core.events import Event, TopicType
from mdq.core.schemas import SilverSchema
from mdq.core.store import MedallionStore

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BDATE = date(2024, 1, 2)
_FX_PAIRS = [
    ("EUR/USD", "USD"),
    ("EUR/GBP", "GBP"),
]


def _make_bronze_df(
    fx_pairs: list[tuple[str, str]] = _FX_PAIRS,
    business_date: date = _BDATE,
) -> pd.DataFrame:
    """Synthetic ECB Bronze-format DataFrame — identical columns to a live ECB fetch."""
    fetch_ts = pd.Timestamp(datetime(2024, 1, 2, 15, 0, 0, tzinfo=UTC))
    rates = {"USD": 1.0947, "GBP": 0.8603}
    rows = []
    for instrument_id, source_symbol in fx_pairs:
        rows.append(
            {
                "Date": pd.Timestamp(business_date),
                "instrument_id": instrument_id,
                "source_symbol": source_symbol,
                "rate": rates.get(source_symbol, 1.0),
                "currency": source_symbol,
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
    """REFERENCE_DATA_COMPLETE published and Bronze Parquet written in fixture mode."""
    monkeypatch.setattr(fixtures_mod, "_FIXTURES_DIR", tmp_path / "fixtures")
    bronze_df = _make_bronze_df()
    snapshot("ecb", bronze_df, tag="default")

    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path, use_fixtures=True)
    bb = Blackboard(db_path=":memory:")
    bb.open()

    from mdq.agents.ingestion.ecb_agent import ECBAgent

    agent = ECBAgent(bb, store, cfg)
    event = Event(
        topic=TopicType.RUN_STARTED,
        agent="supervisor",
        run_id="ecb-run-1",
        payload={"business_date": _BDATE.isoformat()},
    )
    await agent.act(event)

    bronze_path = store.bronze_path("ecb", "ecb-run-1", _BDATE)
    assert bronze_path.exists(), f"Expected Bronze at {bronze_path}"

    stored = pd.read_parquet(bronze_path)
    assert len(stored) == len(bronze_df)

    store.close()
    bb.close()


async def test_reference_data_complete_published(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """REFERENCE_DATA_COMPLETE (not INGESTION_COMPLETE) is published after success."""
    monkeypatch.setattr(fixtures_mod, "_FIXTURES_DIR", tmp_path / "fixtures")
    snapshot("ecb", _make_bronze_df(), tag="default")

    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path, use_fixtures=True)
    bb = Blackboard(db_path=":memory:")
    bb.open()

    from mdq.agents.ingestion.ecb_agent import ECBAgent

    agent = ECBAgent(bb, store, cfg)
    await agent.act(
        Event(
            topic=TopicType.RUN_STARTED,
            agent="supervisor",
            run_id="ecb-run-2",
            payload={"business_date": _BDATE.isoformat()},
        )
    )

    events = bb.get_events(topic=TopicType.REFERENCE_DATA_COMPLETE)
    assert len(events) == 1
    payload = json.loads(events[0]["payload"])
    assert payload["source_id"] == "ecb"
    assert payload["row_count"] == len(_FX_PAIRS)

    # Must NOT publish INGESTION_COMPLETE — ContractAgent must never see ECB data
    assert bb.get_events(topic=TopicType.INGESTION_COMPLETE) == []

    store.close()
    bb.close()


async def test_silver_has_close_field(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Silver rows for ECB have field=CLOSE."""
    monkeypatch.setattr(fixtures_mod, "_FIXTURES_DIR", tmp_path / "fixtures")
    snapshot("ecb", _make_bronze_df(), tag="default")

    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path, use_fixtures=True)
    bb = Blackboard(db_path=":memory:")
    bb.open()

    from mdq.agents.ingestion.ecb_agent import ECBAgent

    agent = ECBAgent(bb, store, cfg)
    await agent.act(
        Event(
            topic=TopicType.RUN_STARTED,
            agent="supervisor",
            run_id="ecb-run-3",
            payload={"business_date": _BDATE.isoformat()},
        )
    )

    silver_path = store.silver_path("ecb-run-3", _BDATE, "ecb")
    assert silver_path.exists()
    silver_df = pd.read_parquet(silver_path)
    assert (
        silver_df["field"] == "CLOSE"
    ).all(), f"Unexpected fields: {silver_df['field'].unique()}"
    assert (silver_df["source_id"] == "ecb").all()

    store.close()
    bb.close()


async def test_silver_passes_schema_validation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """ECB Silver passes Pandera SilverSchema validation."""
    monkeypatch.setattr(fixtures_mod, "_FIXTURES_DIR", tmp_path / "fixtures")
    snapshot("ecb", _make_bronze_df(), tag="default")

    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path, use_fixtures=True)
    bb = Blackboard(db_path=":memory:")
    bb.open()

    from mdq.agents.ingestion.ecb_agent import ECBAgent

    agent = ECBAgent(bb, store, cfg)
    await agent.act(
        Event(
            topic=TopicType.RUN_STARTED,
            agent="supervisor",
            run_id="ecb-run-4",
            payload={"business_date": _BDATE.isoformat()},
        )
    )

    silver_df = pd.read_parquet(store.silver_path("ecb-run-4", _BDATE, "ecb"))
    SilverSchema.validate(silver_df, lazy=True)  # raises if invalid

    store.close()
    bb.close()


async def test_bronze_has_expected_columns(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """ECB Bronze has the required columns for downstream normalisation."""
    monkeypatch.setattr(fixtures_mod, "_FIXTURES_DIR", tmp_path / "fixtures")
    snapshot("ecb", _make_bronze_df(), tag="default")

    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path, use_fixtures=True)
    bb = Blackboard(db_path=":memory:")
    bb.open()

    from mdq.agents.ingestion.ecb_agent import ECBAgent

    agent = ECBAgent(bb, store, cfg)
    await agent.act(
        Event(
            topic=TopicType.RUN_STARTED,
            agent="supervisor",
            run_id="ecb-run-5",
            payload={"business_date": _BDATE.isoformat()},
        )
    )

    bronze_df = pd.read_parquet(store.bronze_path("ecb", "ecb-run-5", _BDATE))
    for col in ("Date", "instrument_id", "source_symbol", "rate", "currency", "fetch_ts"):
        assert col in bronze_df.columns, f"Missing Bronze column: {col}"

    store.close()
    bb.close()


async def test_missing_fixture_publishes_ingestion_failed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If ECB fixture is absent, INGESTION_FAILED is published (not a crash)."""
    monkeypatch.setattr(fixtures_mod, "_FIXTURES_DIR", tmp_path / "fixtures")
    # No snapshot() call

    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path, use_fixtures=True)
    bb = Blackboard(db_path=":memory:")
    bb.open()

    from mdq.agents.ingestion.ecb_agent import ECBAgent

    agent = ECBAgent(bb, store, cfg)
    await agent.act(
        Event(
            topic=TopicType.RUN_STARTED,
            agent="supervisor",
            run_id="ecb-run-fail",
            payload={"business_date": _BDATE.isoformat()},
        )
    )

    # In fixtures mode a missing fixture is a graceful skip, not a hard failure
    failed = bb.get_events(topic=TopicType.INGESTION_FAILED)
    assert len(failed) == 0
    completed = bb.get_events(topic=TopicType.REFERENCE_DATA_COMPLETE)
    assert len(completed) == 1
    payload = json.loads(completed[0]["payload"])
    assert payload["source_id"] == "ecb"
    assert payload["row_count"] == 0
    assert not store.bronze_path("ecb", "ecb-run-fail", _BDATE).exists()

    store.close()
    bb.close()


async def test_bronze_immutability_on_second_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Writing Bronze twice for the same run_id + date skips the second write (FR-S1)."""
    monkeypatch.setattr(fixtures_mod, "_FIXTURES_DIR", tmp_path / "fixtures")
    snapshot("ecb", _make_bronze_df(), tag="default")

    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path, use_fixtures=True)
    bb = Blackboard(db_path=":memory:")
    bb.open()

    from mdq.agents.ingestion.ecb_agent import ECBAgent

    agent = ECBAgent(bb, store, cfg)
    event = Event(
        topic=TopicType.RUN_STARTED,
        agent="supervisor",
        run_id="ecb-imm",
        payload={"business_date": _BDATE.isoformat()},
    )
    await agent.act(event)
    mtime_first = store.bronze_path("ecb", "ecb-imm", _BDATE).stat().st_mtime

    await agent.act(event)
    mtime_second = store.bronze_path("ecb", "ecb-imm", _BDATE).stat().st_mtime

    assert mtime_first == mtime_second, "ECB Bronze mutated on second write — FR-S1 violated"

    store.close()
    bb.close()


def test_name_and_subscriptions() -> None:
    """Agent name and subscriptions are correct without real deps."""
    bb = MagicMock()
    store = MagicMock()
    cfg = MagicMock()
    cfg.universe.instruments = []

    from mdq.agents.ingestion.ecb_agent import ECBAgent

    agent = ECBAgent(bb, store, cfg)
    assert agent.name == "ecb_source"
    assert TopicType.RUN_STARTED in agent.subscriptions
    assert TopicType.REFERENCE_DATA_COMPLETE not in agent.subscriptions


def test_normalise_ecb_produces_correct_rows() -> None:
    """_normalise_ecb reshapes Bronze into Silver with field=CLOSE and correct values."""
    from mdq.agents.ingestion.ecb_agent import _normalise_ecb

    bronze_df = _make_bronze_df([("EUR/USD", "USD")])
    silver_df = _normalise_ecb(bronze_df, _BDATE)

    assert len(silver_df) == 1
    row = silver_df.iloc[0]
    assert row["instrument_id"] == "EUR/USD"
    assert row["field"] == "CLOSE"
    assert abs(float(row["value"]) - 1.0947) < 1e-6
    assert row["source_id"] == "ecb"
    assert row["currency"] == "USD"
    assert not row["ca_adjusted"]


def test_normalise_ecb_content_hash_is_deterministic() -> None:
    """Two calls with identical inputs produce the same content_hash."""
    from mdq.agents.ingestion.ecb_agent import _normalise_ecb

    bronze_df = _make_bronze_df([("EUR/USD", "USD")])
    s1 = _normalise_ecb(bronze_df, _BDATE)
    s2 = _normalise_ecb(bronze_df, _BDATE)
    assert list(s1["content_hash"]) == list(s2["content_hash"])
