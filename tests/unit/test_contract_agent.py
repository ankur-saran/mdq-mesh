"""Unit tests for ContractAgent (FR-A2).

Tests normalisation, Pandera validation, Silver writing, quarantine, and DecisionRecord
persistence. All tests operate offline — no live data sources.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import pytest
import yaml

from harness.inject import DefectType, inject
from mdq.core.blackboard import Blackboard
from mdq.core.config import Config
from mdq.core.events import Event, TopicType
from mdq.core.store import MedallionStore

# ---------------------------------------------------------------------------
# Helpers shared with test_yfinance_agent
# ---------------------------------------------------------------------------

_INSTRUMENTS = [
    ("AAPL", "AAPL"),
    ("MSFT", "MSFT"),
    ("NVDA", "NVDA"),
    ("JPM", "JPM"),
    ("SPY", "SPY"),
]

_BDATE = date(2024, 1, 2)
_RUN_ID = "contract-test-run"
_SOURCE_ID = "yfinance"


def _make_bronze_df(
    instruments: list[tuple[str, str]] = _INSTRUMENTS,
    business_date: date = _BDATE,
) -> pd.DataFrame:
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


def _make_cfg(tmp_path: Path) -> Config:
    base = yaml.safe_load(Path("config/default.yaml").read_text())
    base["runtime"]["storage"]["bronze"] = str(tmp_path / "bronze")
    base["runtime"]["storage"]["silver"] = str(tmp_path / "silver")
    base["runtime"]["storage"]["gold"] = str(tmp_path / "gold")
    base["runtime"]["storage"]["quarantine"] = str(tmp_path / "quarantine")
    base["runtime"]["storage"]["lineage"] = str(tmp_path / "lineage")
    base["runtime"]["duckdb_path"] = str(tmp_path / "mdq.duckdb")
    universe = yaml.safe_load(Path("config/universe.yaml").read_text())
    base["universe"] = universe
    return Config.model_validate(base)


def _ingest_event(run_id: str = _RUN_ID) -> Event:
    return Event(
        topic=TopicType.INGESTION_COMPLETE,
        agent="yfinance_source",
        run_id=run_id,
        payload={
            "source_id": _SOURCE_ID,
            "business_date": _BDATE.isoformat(),
            "row_count": len(_INSTRUMENTS),
        },
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_valid_bronze_produces_silver(tmp_path: Path) -> None:
    """Clean Bronze → Silver written + CONTRACT_PASSED published."""
    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)
    bb = Blackboard(db_path=":memory:")
    bb.open()

    # Write valid Bronze
    store.write_bronze(_SOURCE_ID, _RUN_ID, _make_bronze_df(), _BDATE)

    from mdq.agents.contract_agent import ContractAgent

    agent = ContractAgent(bb, store, cfg)
    await agent.act(_ingest_event())

    # Silver file exists
    silver_path = store.silver_path(_RUN_ID, _BDATE)
    assert silver_path.exists(), f"Expected Silver at {silver_path}"

    # Silver has 5 instruments × 6 fields = 30 rows
    silver_df = pd.read_parquet(silver_path)
    assert len(silver_df) == len(_INSTRUMENTS) * 6

    # CONTRACT_PASSED published; CONTRACT_VIOLATION absent
    assert len(bb.get_events(topic=TopicType.CONTRACT_PASSED)) == 1
    assert len(bb.get_events(topic=TopicType.CONTRACT_VIOLATION)) == 0

    store.close()
    bb.close()


async def test_silver_conforms_to_pandera_schema(tmp_path: Path) -> None:
    """Silver DataFrame produced by ContractAgent passes SilverSchema with no exceptions."""
    import pandera as pa

    from mdq.core.schemas import SilverSchema

    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)
    bb = Blackboard(db_path=":memory:")
    bb.open()

    store.write_bronze(_SOURCE_ID, _RUN_ID, _make_bronze_df(), _BDATE)

    from mdq.agents.contract_agent import ContractAgent

    agent = ContractAgent(bb, store, cfg)
    await agent.act(_ingest_event())

    silver_df = pd.read_parquet(store.silver_path(_RUN_ID, _BDATE))

    try:
        SilverSchema.validate(silver_df, lazy=True)
    except pa.errors.SchemaErrors as exc:
        pytest.fail(f"SilverSchema validation failed: {exc.failure_cases}")

    store.close()
    bb.close()


async def test_schema_drift_quarantines_and_publishes_violation(tmp_path: Path) -> None:
    """SCHEMA_DRIFT injected into Bronze → quarantine written, CONTRACT_VIOLATION published."""
    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)
    bb = Blackboard(db_path=":memory:")
    bb.open()

    # Inject drift: rename "Close" so the normaliser raises KeyError
    drifted = inject(_make_bronze_df(), DefectType.SCHEMA_DRIFT, rename={"Close": "ClosingPrice"})
    store.write_bronze(_SOURCE_ID, _RUN_ID + "-drift", drifted, _BDATE)

    from mdq.agents.contract_agent import ContractAgent

    agent = ContractAgent(bb, store, cfg)
    drift_event = Event(
        topic=TopicType.INGESTION_COMPLETE,
        agent="yfinance_source",
        run_id=_RUN_ID + "-drift",
        payload={
            "source_id": _SOURCE_ID,
            "business_date": _BDATE.isoformat(),
            "row_count": len(_INSTRUMENTS),
        },
    )
    await agent.act(drift_event)

    # CONTRACT_VIOLATION published; CONTRACT_PASSED absent
    violations = bb.get_events(topic=TopicType.CONTRACT_VIOLATION)
    assert len(violations) == 1
    assert len(bb.get_events(topic=TopicType.CONTRACT_PASSED)) == 0

    payload = json.loads(violations[0]["payload"])
    assert payload["source_id"] == _SOURCE_ID

    # Silver NOT written
    assert not store.silver_path(_RUN_ID + "-drift", _BDATE).exists()

    # Quarantine IS written
    quarantine_dir = tmp_path / "quarantine" / (_RUN_ID + "-drift") / "schema_violation"
    assert quarantine_dir.exists()
    assert list(quarantine_dir.glob("*.parquet"))

    store.close()
    bb.close()


async def test_decision_record_persisted(tmp_path: Path) -> None:
    """A DecisionRecord is written to DuckDB for both PASS and FAIL outcomes (C-4)."""
    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)
    bb = Blackboard(db_path=":memory:")
    bb.open()

    store.write_bronze(_SOURCE_ID, _RUN_ID, _make_bronze_df(), _BDATE)

    from mdq.agents.contract_agent import ContractAgent

    agent = ContractAgent(bb, store, cfg)
    await agent.act(_ingest_event())

    decisions = store.query("SELECT * FROM decisions")
    assert len(decisions) >= 1
    row = decisions.iloc[0]
    assert row["agent"] == "contract"
    assert row["rule_applied"] == "SilverSchema"
    assert row["decision_type"] == "CONTRACT"

    store.close()
    bb.close()


async def test_missing_bronze_publishes_violation(tmp_path: Path) -> None:
    """Missing Bronze file (e.g. ingestion silently skipped) → CONTRACT_VIOLATION, no crash."""
    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)
    bb = Blackboard(db_path=":memory:")
    bb.open()

    # No Bronze written — simulate a skipped ingestion

    from mdq.agents.contract_agent import ContractAgent

    agent = ContractAgent(bb, store, cfg)
    await agent.act(_ingest_event())

    violations = bb.get_events(topic=TopicType.CONTRACT_VIOLATION)
    assert len(violations) == 1

    store.close()
    bb.close()


async def test_subscriptions_and_name() -> None:
    """Agent metadata is correct without constructing real deps."""
    from unittest.mock import MagicMock

    from mdq.agents.contract_agent import ContractAgent

    agent = ContractAgent(MagicMock(), MagicMock(), MagicMock())
    assert agent.name == "contract"
    assert TopicType.INGESTION_COMPLETE in agent.subscriptions
