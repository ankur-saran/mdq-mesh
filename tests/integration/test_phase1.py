"""Phase 1 acceptance criteria integration tests (PRD §12).

AC1 — For a 5-instrument universe, raw data lands in Bronze from yfinance.
AC2 — That data conforms to the Silver canonical schema.
AC3 — Schema drift is detected on a deliberately broken fixture.

All tests run fully offline in fixture mode (runtime.use_fixtures=True).
The blackboard is wired end-to-end with both agents so event flow is realistic.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import pytest
import yaml

import harness.fixtures as fixtures_mod
from harness.fixtures import snapshot
from harness.inject import DefectType, inject
from mdq.core.blackboard import Blackboard
from mdq.core.config import Config
from mdq.core.events import Event, TopicType
from mdq.core.store import MedallionStore

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_BDATE = date(2024, 1, 2)
_INSTRUMENTS = [
    ("AAPL", "AAPL"),
    ("MSFT", "MSFT"),
    ("NVDA", "NVDA"),
    ("JPM", "JPM"),
    ("SPY", "SPY"),
]
_SOURCE_ID = "yfinance"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_bronze_df(business_date: date = _BDATE) -> pd.DataFrame:
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


async def _run_pipeline(
    store: MedallionStore,
    cfg: Config,
    run_id: str = "phase1-run",
) -> Blackboard:
    """Wire both agents and drive a full RUN_STARTED → drain cycle."""
    from mdq.agents.contract_agent import ContractAgent
    from mdq.agents.ingestion.yfinance_agent import YFinanceAgent

    bb = Blackboard(db_path=":memory:")
    bb.register(YFinanceAgent(bb, store, cfg))
    bb.register(ContractAgent(bb, store, cfg))

    await bb.start()
    await bb.publish(
        Event(
            topic=TopicType.RUN_STARTED,
            agent="supervisor",
            run_id=run_id,
            payload={"business_date": _BDATE.isoformat()},
        )
    )
    await bb.drain()
    return bb


# ---------------------------------------------------------------------------
# AC1 — Bronze landing
# ---------------------------------------------------------------------------


async def test_ac1_bronze_lands_for_all_5_instruments(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AC1: All 5 universe instruments land in Bronze Parquet."""
    monkeypatch.setattr(fixtures_mod, "_FIXTURES_DIR", tmp_path / "fixtures")
    snapshot(_SOURCE_ID, _make_bronze_df(), tag="default")

    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)
    bb = await _run_pipeline(store, cfg)

    # INGESTION_COMPLETE from yfinance_source agent
    events = bb.get_events(topic=TopicType.INGESTION_COMPLETE)
    assert len(events) == 1, "Expected exactly one INGESTION_COMPLETE event"

    # Bronze Parquet exists for this run
    bronze_path = store.bronze_path(_SOURCE_ID, "phase1-run", _BDATE)
    assert bronze_path.exists(), f"Bronze not written: {bronze_path}"

    # Correct number of rows (5 instruments)
    bronze_df = pd.read_parquet(bronze_path)
    assert len(bronze_df) == len(_INSTRUMENTS)

    await bb.stop()
    store.close()


# ---------------------------------------------------------------------------
# AC2 — Silver conforms to canonical schema
# ---------------------------------------------------------------------------


async def test_ac2_silver_conforms_to_silver_schema(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AC2: Silver Parquet passes Pandera SilverSchema validation."""
    import pandera as pa

    from mdq.core.schemas import SilverSchema

    monkeypatch.setattr(fixtures_mod, "_FIXTURES_DIR", tmp_path / "fixtures")
    snapshot(_SOURCE_ID, _make_bronze_df(), tag="default")

    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)
    bb = await _run_pipeline(store, cfg)

    # CONTRACT_PASSED published
    assert len(bb.get_events(topic=TopicType.CONTRACT_PASSED)) == 1
    assert len(bb.get_events(topic=TopicType.CONTRACT_VIOLATION)) == 0

    # Silver Parquet written
    silver_path = store.silver_path("phase1-run", _BDATE, "yfinance")
    assert silver_path.exists(), f"Silver not written: {silver_path}"

    # 5 instruments × 6 fields = 30 rows
    silver_df = pd.read_parquet(silver_path)
    assert len(silver_df) == len(_INSTRUMENTS) * 6

    # Pandera schema must pass cleanly
    try:
        SilverSchema.validate(silver_df, lazy=True)
    except pa.errors.SchemaErrors as exc:
        pytest.fail(f"SilverSchema validation failed on AC2 Silver output: {exc.failure_cases}")

    await bb.stop()
    store.close()


# ---------------------------------------------------------------------------
# AC3 — Schema drift detected
# ---------------------------------------------------------------------------


async def test_ac3_schema_drift_detected(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """AC3: Schema drift injected into Bronze → CONTRACT_VIOLATION, no Silver written."""
    monkeypatch.setattr(fixtures_mod, "_FIXTURES_DIR", tmp_path / "fixtures")

    # Create a fixture with SCHEMA_DRIFT (rename "Close" to trigger KeyError in normaliser)
    drifted_df = inject(
        _make_bronze_df(), DefectType.SCHEMA_DRIFT, rename={"Close": "ClosingPrice"}
    )
    snapshot(_SOURCE_ID, drifted_df, tag="default")

    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)

    # Run the pipeline with the drifted fixture
    bb = await _run_pipeline(store, cfg, run_id="drift-run")

    # CONTRACT_VIOLATION event published
    violations = bb.get_events(topic=TopicType.CONTRACT_VIOLATION)
    assert len(violations) == 1, "Expected CONTRACT_VIOLATION for schema drift"
    assert len(bb.get_events(topic=TopicType.CONTRACT_PASSED)) == 0

    # Silver NOT written (quarantine instead)
    silver_path = store.silver_path("drift-run", _BDATE, "yfinance")
    assert not silver_path.exists(), "Silver must NOT be written when schema drift is detected"

    # Quarantine IS written
    quarantine_root = tmp_path / "quarantine" / "drift-run" / "schema_violation"
    assert quarantine_root.exists() and list(
        quarantine_root.glob("*.parquet")
    ), "Quarantine Parquet must be written for drifted data"

    await bb.stop()
    store.close()


# ---------------------------------------------------------------------------
# Additional gate: event log integrity
# ---------------------------------------------------------------------------


async def test_all_events_persisted_to_blackboard(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """RUN_STARTED, INGESTION_COMPLETE, and CONTRACT_PASSED all appear in the event log."""
    monkeypatch.setattr(fixtures_mod, "_FIXTURES_DIR", tmp_path / "fixtures")
    snapshot(_SOURCE_ID, _make_bronze_df(), tag="default")

    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)
    bb = await _run_pipeline(store, cfg, run_id="event-log-run")

    all_events = bb.get_events(run_id="event-log-run")
    topics = {e["topic"] for e in all_events}

    assert TopicType.RUN_STARTED.value in topics
    assert TopicType.INGESTION_COMPLETE.value in topics
    assert TopicType.CONTRACT_PASSED.value in topics

    await bb.stop()
    store.close()
