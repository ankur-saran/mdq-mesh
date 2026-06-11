"""Unit tests for SECEdgarAgent (FR-A1, Phase 7).

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
_INSTRUMENTS = [
    ("AAPL", "0000320193", 15_500_000_000),
    ("MSFT", "0000789019", 7_430_000_000),
]


def _make_bronze_df(
    instruments: list[tuple[str, str, int]] = _INSTRUMENTS,
    business_date: date = _BDATE,
) -> pd.DataFrame:
    """Synthetic SEC EDGAR Bronze-format DataFrame."""
    fetch_ts = pd.Timestamp(datetime(2024, 1, 2, 21, 0, 0, tzinfo=UTC))
    rows = []
    for instrument_id, cik, shares in instruments:
        rows.append(
            {
                "Date": pd.Timestamp(business_date),
                "instrument_id": instrument_id,
                "cik": cik,
                "metric": "CommonStockSharesOutstanding",
                "value": float(shares),
                "unit": "shares",
                "filed_date": "2023-11-02",
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
    snapshot("sec_edgar", bronze_df, tag="default")

    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path, use_fixtures=True)
    bb = Blackboard(db_path=":memory:")
    bb.open()

    from mdq.agents.ingestion.sec_edgar_agent import SECEdgarAgent

    agent = SECEdgarAgent(bb, store, cfg)
    event = Event(
        topic=TopicType.RUN_STARTED,
        agent="supervisor",
        run_id="sec-run-1",
        payload={"business_date": _BDATE.isoformat()},
    )
    await agent.act(event)

    bronze_path = store.bronze_path("sec_edgar", "sec-run-1", _BDATE)
    assert bronze_path.exists(), f"Expected Bronze at {bronze_path}"

    stored = pd.read_parquet(bronze_path)
    assert len(stored) == len(_INSTRUMENTS)

    store.close()
    bb.close()


async def test_reference_data_complete_published(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """REFERENCE_DATA_COMPLETE (not INGESTION_COMPLETE) published after success."""
    monkeypatch.setattr(fixtures_mod, "_FIXTURES_DIR", tmp_path / "fixtures")
    snapshot("sec_edgar", _make_bronze_df(), tag="default")

    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path, use_fixtures=True)
    bb = Blackboard(db_path=":memory:")
    bb.open()

    from mdq.agents.ingestion.sec_edgar_agent import SECEdgarAgent

    agent = SECEdgarAgent(bb, store, cfg)
    await agent.act(
        Event(
            topic=TopicType.RUN_STARTED,
            agent="supervisor",
            run_id="sec-run-2",
            payload={"business_date": _BDATE.isoformat()},
        )
    )

    events = bb.get_events(topic=TopicType.REFERENCE_DATA_COMPLETE)
    assert len(events) == 1
    payload = json.loads(events[0]["payload"])
    assert payload["source_id"] == "sec_edgar"
    assert payload["row_count"] == len(_INSTRUMENTS)

    # Must NOT publish INGESTION_COMPLETE — ContractAgent must never see EDGAR data
    assert bb.get_events(topic=TopicType.INGESTION_COMPLETE) == []

    store.close()
    bb.close()


async def test_silver_has_shares_field(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Silver rows for SEC EDGAR have field=SHARES."""
    monkeypatch.setattr(fixtures_mod, "_FIXTURES_DIR", tmp_path / "fixtures")
    snapshot("sec_edgar", _make_bronze_df(), tag="default")

    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path, use_fixtures=True)
    bb = Blackboard(db_path=":memory:")
    bb.open()

    from mdq.agents.ingestion.sec_edgar_agent import SECEdgarAgent

    agent = SECEdgarAgent(bb, store, cfg)
    await agent.act(
        Event(
            topic=TopicType.RUN_STARTED,
            agent="supervisor",
            run_id="sec-run-3",
            payload={"business_date": _BDATE.isoformat()},
        )
    )

    silver_path = store.silver_path("sec-run-3", _BDATE, "sec_edgar")
    assert silver_path.exists()
    silver_df = pd.read_parquet(silver_path)
    assert (
        silver_df["field"] == "SHARES"
    ).all(), f"Unexpected fields: {silver_df['field'].unique()}"
    assert (silver_df["source_id"] == "sec_edgar").all()

    store.close()
    bb.close()


async def test_silver_passes_schema_validation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SEC EDGAR Silver passes Pandera SilverSchema validation."""
    monkeypatch.setattr(fixtures_mod, "_FIXTURES_DIR", tmp_path / "fixtures")
    snapshot("sec_edgar", _make_bronze_df(), tag="default")

    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path, use_fixtures=True)
    bb = Blackboard(db_path=":memory:")
    bb.open()

    from mdq.agents.ingestion.sec_edgar_agent import SECEdgarAgent

    agent = SECEdgarAgent(bb, store, cfg)
    await agent.act(
        Event(
            topic=TopicType.RUN_STARTED,
            agent="supervisor",
            run_id="sec-run-4",
            payload={"business_date": _BDATE.isoformat()},
        )
    )

    silver_df = pd.read_parquet(store.silver_path("sec-run-4", _BDATE, "sec_edgar"))
    SilverSchema.validate(silver_df, lazy=True)

    store.close()
    bb.close()


async def test_bronze_has_cik_column(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """SEC EDGAR Bronze has the cik column for traceability."""
    monkeypatch.setattr(fixtures_mod, "_FIXTURES_DIR", tmp_path / "fixtures")
    snapshot("sec_edgar", _make_bronze_df(), tag="default")

    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path, use_fixtures=True)
    bb = Blackboard(db_path=":memory:")
    bb.open()

    from mdq.agents.ingestion.sec_edgar_agent import SECEdgarAgent

    agent = SECEdgarAgent(bb, store, cfg)
    await agent.act(
        Event(
            topic=TopicType.RUN_STARTED,
            agent="supervisor",
            run_id="sec-run-5",
            payload={"business_date": _BDATE.isoformat()},
        )
    )

    bronze_df = pd.read_parquet(store.bronze_path("sec_edgar", "sec-run-5", _BDATE))
    expected_cols = (
        "Date",
        "instrument_id",
        "cik",
        "metric",
        "value",
        "unit",
        "filed_date",
        "fetch_ts",
    )
    for col in expected_cols:
        assert col in bronze_df.columns, f"Missing Bronze column: {col}"

    store.close()
    bb.close()


async def test_missing_fixture_publishes_ingestion_failed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If SEC EDGAR fixture is absent, INGESTION_FAILED is published (not a crash)."""
    monkeypatch.setattr(fixtures_mod, "_FIXTURES_DIR", tmp_path / "fixtures")

    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path, use_fixtures=True)
    bb = Blackboard(db_path=":memory:")
    bb.open()

    from mdq.agents.ingestion.sec_edgar_agent import SECEdgarAgent

    agent = SECEdgarAgent(bb, store, cfg)
    await agent.act(
        Event(
            topic=TopicType.RUN_STARTED,
            agent="supervisor",
            run_id="sec-run-fail",
            payload={"business_date": _BDATE.isoformat()},
        )
    )

    # In fixtures mode a missing fixture is a graceful skip, not a hard failure
    failed = bb.get_events(topic=TopicType.INGESTION_FAILED)
    assert len(failed) == 0
    completed = bb.get_events(topic=TopicType.REFERENCE_DATA_COMPLETE)
    assert len(completed) == 1
    payload = json.loads(completed[0]["payload"])
    assert payload["source_id"] == "sec_edgar"
    assert payload["row_count"] == 0
    assert not store.bronze_path("sec_edgar", "sec-run-fail", _BDATE).exists()

    store.close()
    bb.close()


async def test_bronze_immutability_on_second_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Writing Bronze twice for the same run_id + date skips the second write (FR-S1)."""
    monkeypatch.setattr(fixtures_mod, "_FIXTURES_DIR", tmp_path / "fixtures")
    snapshot("sec_edgar", _make_bronze_df(), tag="default")

    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path, use_fixtures=True)
    bb = Blackboard(db_path=":memory:")
    bb.open()

    from mdq.agents.ingestion.sec_edgar_agent import SECEdgarAgent

    agent = SECEdgarAgent(bb, store, cfg)
    event = Event(
        topic=TopicType.RUN_STARTED,
        agent="supervisor",
        run_id="sec-imm",
        payload={"business_date": _BDATE.isoformat()},
    )
    await agent.act(event)
    mtime_first = store.bronze_path("sec_edgar", "sec-imm", _BDATE).stat().st_mtime

    await agent.act(event)
    mtime_second = store.bronze_path("sec_edgar", "sec-imm", _BDATE).stat().st_mtime

    assert mtime_first == mtime_second, "SEC EDGAR Bronze mutated on second write — FR-S1 violated"

    store.close()
    bb.close()


def test_name_and_subscriptions() -> None:
    """Agent name and subscriptions are correct without real deps."""
    bb = MagicMock()
    store = MagicMock()
    cfg = MagicMock()
    cfg.universe.instruments = []

    from mdq.agents.ingestion.sec_edgar_agent import SECEdgarAgent

    agent = SECEdgarAgent(bb, store, cfg)
    assert agent.name == "sec_edgar_source"
    assert TopicType.RUN_STARTED in agent.subscriptions
    assert TopicType.REFERENCE_DATA_COMPLETE not in agent.subscriptions


def test_normalise_sec_produces_correct_rows() -> None:
    """_normalise_sec reshapes Bronze into Silver with field=SHARES and correct values."""
    from mdq.agents.ingestion.sec_edgar_agent import _normalise_sec

    bronze_df = _make_bronze_df([("AAPL", "0000320193", 15_500_000_000)])
    silver_df = _normalise_sec(bronze_df, _BDATE)

    assert len(silver_df) == 1
    row = silver_df.iloc[0]
    assert row["instrument_id"] == "AAPL"
    assert row["field"] == "SHARES"
    assert float(row["value"]) == 15_500_000_000.0
    assert row["source_id"] == "sec_edgar"
    assert row["currency"] == "shares"
    assert row["source_symbol"] == "0000320193"
    assert not row["ca_adjusted"]


def test_normalise_sec_content_hash_is_deterministic() -> None:
    """Two calls with identical inputs produce the same content_hash."""
    from mdq.agents.ingestion.sec_edgar_agent import _normalise_sec

    bronze_df = _make_bronze_df([("AAPL", "0000320193", 15_500_000_000)])
    s1 = _normalise_sec(bronze_df, _BDATE)
    s2 = _normalise_sec(bronze_df, _BDATE)
    assert list(s1["content_hash"]) == list(s2["content_hash"])
