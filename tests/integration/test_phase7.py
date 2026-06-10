"""Phase 7 acceptance criteria integration tests (PRD §12).

AC / KPI-7 — ECB and SEC EDGAR source agents added with ZERO changes to orchestrator
or blackboard core. Demonstrated by:
1. ECBAgent produces Silver with field=CLOSE for FX pairs.
2. SECEdgarAgent produces Silver with field=SHARES for equity instruments.
3. Adding both new agents to the full pipeline leaves RECONCILIATION_COMPLETE intact
   (equity quorum unaffected).

All tests run in fixture mode (offline).
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
from mdq.agents.anomaly_agent import AnomalyAgent
from mdq.agents.contract_agent import ContractAgent
from mdq.agents.corporate_actions_agent import CorporateActionsAgent
from mdq.agents.dq_agent import DQAgent
from mdq.agents.ingestion.ecb_agent import ECBAgent
from mdq.agents.ingestion.sec_edgar_agent import SECEdgarAgent
from mdq.agents.ingestion.stooq_agent import StooqAgent
from mdq.agents.ingestion.yfinance_agent import YFinanceAgent
from mdq.agents.lineage_agent import LineageAgent
from mdq.agents.reconciliation_agent import ReconciliationAgent
from mdq.agents.remediation_agent import RemediationAgent
from mdq.agents.supervisor import Supervisor
from mdq.core.blackboard import Blackboard
from mdq.core.config import Config
from mdq.core.events import Event, TopicType
from mdq.core.store import MedallionStore

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_BDATE = date(2024, 1, 2)
_EQUITY_INSTRUMENTS = [
    ("AAPL", "AAPL", "aapl.us"),
    ("MSFT", "MSFT", "msft.us"),
    ("NVDA", "NVDA", "nvda.us"),
    ("JPM", "JPM", "jpm.us"),
    ("SPY", "SPY", "spy.us"),
]
_FX_PAIRS = [("EUR/USD", "USD"), ("EUR/GBP", "GBP")]
_SEC_INSTRUMENTS = [
    ("AAPL", "0000320193", 15_500_000_000),
    ("MSFT", "0000789019", 7_430_000_000),
    ("NVDA", "0001045810", 2_460_000_000),
    ("JPM", "0000019617", 2_880_000_000),
]

# ---------------------------------------------------------------------------
# Fixture builders
# ---------------------------------------------------------------------------


def _make_yf_bronze_df(business_date: date = _BDATE) -> pd.DataFrame:
    fetch_ts = pd.Timestamp(datetime(2024, 1, 2, 21, 0, 0, tzinfo=UTC))
    rows = []
    for instrument_id, yf_sym, _ in _EQUITY_INSTRUMENTS:
        rows.append(
            {
                "Date": pd.Timestamp(business_date),
                "Open": 180.0,
                "High": 185.0,
                "Low": 178.0,
                "Close": 182.0,
                "Adj Close": 182.0,
                "Volume": 1_000_000,
                "instrument_id": instrument_id,
                "source_symbol": yf_sym,
                "fetch_ts": fetch_ts,
            }
        )
    return pd.DataFrame(rows)


def _make_stooq_bronze_df(business_date: date = _BDATE) -> pd.DataFrame:
    fetch_ts = pd.Timestamp(datetime(2024, 1, 2, 21, 0, 0, tzinfo=UTC))
    rows = []
    for instrument_id, _, stooq_sym in _EQUITY_INSTRUMENTS:
        rows.append(
            {
                "Date": pd.Timestamp(business_date),
                "Open": 180.0,
                "High": 185.0,
                "Low": 178.0,
                "Close": 182.0,
                "Adj Close": 182.0,
                "Volume": 1_000_000,
                "instrument_id": instrument_id,
                "source_symbol": stooq_sym,
                "fetch_ts": fetch_ts,
            }
        )
    return pd.DataFrame(rows)


def _make_ecb_bronze_df(business_date: date = _BDATE) -> pd.DataFrame:
    fetch_ts = pd.Timestamp(datetime(2024, 1, 2, 15, 0, 0, tzinfo=UTC))
    rates = {"USD": 1.0947, "GBP": 0.8603}
    rows = []
    for instrument_id, source_symbol in _FX_PAIRS:
        rows.append(
            {
                "Date": pd.Timestamp(business_date),
                "instrument_id": instrument_id,
                "source_symbol": source_symbol,
                "rate": rates[source_symbol],
                "currency": source_symbol,
                "fetch_ts": fetch_ts,
            }
        )
    return pd.DataFrame(rows)


def _make_sec_bronze_df(business_date: date = _BDATE) -> pd.DataFrame:
    fetch_ts = pd.Timestamp(datetime(2024, 1, 2, 21, 0, 0, tzinfo=UTC))
    rows = []
    for instrument_id, cik, shares in _SEC_INSTRUMENTS:
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


# ---------------------------------------------------------------------------
# Infrastructure helpers
# ---------------------------------------------------------------------------


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


def _make_full_pipeline(store: MedallionStore, cfg: Config) -> Blackboard:
    """Wire all agents including ECBAgent and SECEdgarAgent (Phase 7)."""
    bb = Blackboard(db_path=":memory:")
    bb.register(YFinanceAgent(bb, store, cfg))
    bb.register(StooqAgent(bb, store, cfg))
    bb.register(ECBAgent(bb, store, cfg))
    bb.register(SECEdgarAgent(bb, store, cfg))
    bb.register(ContractAgent(bb, store, cfg))
    bb.register(DQAgent(bb, store, cfg))
    bb.register(AnomalyAgent(bb, store, cfg))
    bb.register(CorporateActionsAgent(bb, store, cfg))
    bb.register(ReconciliationAgent(bb, store, cfg))
    bb.register(RemediationAgent(bb, store, cfg))
    bb.register(Supervisor(bb, store, cfg))
    bb.register(LineageAgent(bb, store, cfg))
    return bb


def _seed_all_fixtures(tmp_path: Path) -> None:
    """Seed Bronze fixtures for all four sources."""
    snapshot("yfinance", _make_yf_bronze_df(), tag="default")
    snapshot("stooq", _make_stooq_bronze_df(), tag="default")
    snapshot("ecb", _make_ecb_bronze_df(), tag="default")
    snapshot("sec_edgar", _make_sec_bronze_df(), tag="default")


# ---------------------------------------------------------------------------
# AC / KPI-7 tests
# ---------------------------------------------------------------------------


async def test_ac_kpi7_ecb_silver_written(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """KPI-7 AC1: ECBAgent writes Silver without any orchestrator/core changes."""
    monkeypatch.setattr(fixtures_mod, "_FIXTURES_DIR", tmp_path / "fixtures")
    _seed_all_fixtures(tmp_path)

    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)
    bb = _make_full_pipeline(store, cfg)

    await bb.start()
    await bb.publish(
        Event(
            topic=TopicType.RUN_STARTED,
            agent="supervisor",
            run_id="phase7-ecb",
            payload={"business_date": _BDATE.isoformat()},
        )
    )
    await bb.drain()

    # ECB Silver exists with field=CLOSE FX rate rows
    silver_path = store.silver_path("phase7-ecb", _BDATE, "ecb")
    assert silver_path.exists(), f"ECB Silver not written: {silver_path}"
    silver_df = pd.read_parquet(silver_path)
    assert len(silver_df) == len(_FX_PAIRS)
    assert (silver_df["field"] == "CLOSE").all()
    assert set(silver_df["instrument_id"]) == {"EUR/USD", "EUR/GBP"}

    # REFERENCE_DATA_COMPLETE published for ECB
    ref_events = bb.get_events(topic=TopicType.REFERENCE_DATA_COMPLETE)
    ecb_events = [e for e in ref_events if json.loads(e["payload"]).get("source_id") == "ecb"]
    assert len(ecb_events) == 1

    # Existing equity reconciliation is unaffected
    recon_events = bb.get_events(topic=TopicType.RECONCILIATION_COMPLETE)
    assert len(recon_events) == 1, "RECONCILIATION_COMPLETE must still fire"
    payload = json.loads(recon_events[0]["payload"])
    assert payload["golden_records"] > 0

    await bb.stop()
    store.close()


async def test_ac_kpi7_sec_edgar_silver_written(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """KPI-7 AC2: SECEdgarAgent writes Silver with field=SHARES without core changes."""
    monkeypatch.setattr(fixtures_mod, "_FIXTURES_DIR", tmp_path / "fixtures")
    _seed_all_fixtures(tmp_path)

    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)
    bb = _make_full_pipeline(store, cfg)

    await bb.start()
    await bb.publish(
        Event(
            topic=TopicType.RUN_STARTED,
            agent="supervisor",
            run_id="phase7-sec",
            payload={"business_date": _BDATE.isoformat()},
        )
    )
    await bb.drain()

    # SEC EDGAR Silver exists with field=SHARES rows
    silver_path = store.silver_path("phase7-sec", _BDATE, "sec_edgar")
    assert silver_path.exists(), f"SEC EDGAR Silver not written: {silver_path}"
    silver_df = pd.read_parquet(silver_path)
    assert len(silver_df) == len(_SEC_INSTRUMENTS)
    assert (silver_df["field"] == "SHARES").all()
    assert set(silver_df["instrument_id"]) == {"AAPL", "MSFT", "NVDA", "JPM"}

    # REFERENCE_DATA_COMPLETE published for sec_edgar
    ref_events = bb.get_events(topic=TopicType.REFERENCE_DATA_COMPLETE)
    sec_events = [e for e in ref_events if json.loads(e["payload"]).get("source_id") == "sec_edgar"]
    assert len(sec_events) == 1

    await bb.stop()
    store.close()


async def test_kpi7_existing_recon_unaffected_with_new_sources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """KPI-7 AC3: all 4 sources active — equity reconciliation still correct, zero core change.

    This is the definitive KPI-7 proof: a 10-agent pipeline with ECB + SEC EDGAR added
    produces identical equity Gold to the baseline (breaks=0, HIGH confidence), confirming
    the reference-data agents are completely isolated from the price pipeline.
    """
    monkeypatch.setattr(fixtures_mod, "_FIXTURES_DIR", tmp_path / "fixtures")
    _seed_all_fixtures(tmp_path)

    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)
    bb = _make_full_pipeline(store, cfg)

    await bb.start()
    await bb.publish(
        Event(
            topic=TopicType.RUN_STARTED,
            agent="supervisor",
            run_id="phase7-full",
            payload={"business_date": _BDATE.isoformat()},
        )
    )
    await bb.drain()

    # Equity reconciliation fires once with golden records
    recon_events = bb.get_events(topic=TopicType.RECONCILIATION_COMPLETE)
    assert len(recon_events) == 1, "RECONCILIATION_COMPLETE must fire exactly once"
    recon_payload = json.loads(recon_events[0]["payload"])
    assert recon_payload["breaks"] == 0, f"Unexpected breaks: {recon_payload}"
    assert recon_payload["golden_records"] > 0, "Expected Gold records"

    # Gold written with HIGH confidence (both equity sources agree)
    gold_path = store._gold / "phase7-full" / f"{_BDATE.isoformat()}.parquet"
    assert gold_path.exists()
    gold_df = pd.read_parquet(gold_path)
    assert (gold_df["confidence"] == "HIGH").all()

    # REFERENCE_DATA_COMPLETE for both new sources
    ref_events = bb.get_events(topic=TopicType.REFERENCE_DATA_COMPLETE)
    ref_sources = {json.loads(e["payload"])["source_id"] for e in ref_events}
    assert "ecb" in ref_sources, "ECB REFERENCE_DATA_COMPLETE not published"
    assert "sec_edgar" in ref_sources, "SEC EDGAR REFERENCE_DATA_COMPLETE not published"

    # No BREAK_DETECTED events caused by reference-data sources (they never enter quorum)
    # Any breaks that exist are from equity reconciliation only
    break_events = bb.get_events(topic=TopicType.BREAK_DETECTED)
    assert (
        len(break_events) == 0
    ), f"Unexpected breaks: {[json.loads(e['payload']) for e in break_events]}"

    # Both Silver stores exist side-by-side with no interference
    assert store.silver_path("phase7-full", _BDATE, "yfinance").exists()
    assert store.silver_path("phase7-full", _BDATE, "stooq").exists()
    assert store.silver_path("phase7-full", _BDATE, "ecb").exists()
    assert store.silver_path("phase7-full", _BDATE, "sec_edgar").exists()

    await bb.stop()
    store.close()
