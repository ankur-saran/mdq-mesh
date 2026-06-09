"""Phase 3 acceptance criteria integration tests (PRD §12).

AC1 — Cross-source agreement produces a HIGH-confidence golden value in Gold.
AC2 — A seeded disagreement beyond tolerance produces a flagged BREAK_DETECTED
       with recorded dissent; no Gold written for the breaking field.

All tests run fully offline in fixture mode. The blackboard is wired end-to-end
with all six agents so event flow is realistic.
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
from mdq.agents.dq_agent import DQAgent
from mdq.agents.ingestion.stooq_agent import StooqAgent
from mdq.agents.ingestion.yfinance_agent import YFinanceAgent
from mdq.agents.reconciliation_agent import ReconciliationAgent
from mdq.core.blackboard import Blackboard
from mdq.core.config import Config
from mdq.core.events import Event, TopicType
from mdq.core.store import MedallionStore

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_BDATE = date(2024, 1, 2)
_INSTRUMENTS = [
    ("AAPL", "AAPL", "aapl.us"),
    ("MSFT", "MSFT", "msft.us"),
    ("NVDA", "NVDA", "nvda.us"),
    ("JPM", "JPM", "jpm.us"),
    ("SPY", "SPY", "spy.us"),
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_yf_bronze_df(business_date: date = _BDATE) -> pd.DataFrame:
    fetch_ts = pd.Timestamp(datetime(2024, 1, 2, 21, 0, 0, tzinfo=UTC))
    rows = []
    for instrument_id, yf_sym, _ in _INSTRUMENTS:
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
    for instrument_id, _, stooq_sym in _INSTRUMENTS:
        rows.append(
            {
                "Date": pd.Timestamp(business_date),
                "Open": 180.0,
                "High": 185.0,
                "Low": 178.0,
                "Close": 182.0,  # identical to yfinance → within 25bps
                "Adj Close": 182.0,
                "Volume": 1_000_000,
                "instrument_id": instrument_id,
                "source_symbol": stooq_sym,
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
    run_id: str = "phase3-run",
    business_date: date = _BDATE,
) -> Blackboard:
    """Wire all 6 agents and drive a full RUN_STARTED → drain cycle."""
    bb = Blackboard(db_path=":memory:")
    bb.register(YFinanceAgent(bb, store, cfg))
    bb.register(StooqAgent(bb, store, cfg))
    bb.register(ContractAgent(bb, store, cfg))
    bb.register(DQAgent(bb, store, cfg))
    bb.register(AnomalyAgent(bb, store, cfg))
    bb.register(ReconciliationAgent(bb, store, cfg))

    await bb.start()
    await bb.publish(
        Event(
            topic=TopicType.RUN_STARTED,
            agent="supervisor",
            run_id=run_id,
            payload={"business_date": business_date.isoformat()},
        )
    )
    await bb.drain()
    return bb


# ---------------------------------------------------------------------------
# AC1 — Cross-source agreement → HIGH-confidence Gold
# ---------------------------------------------------------------------------


async def test_ac1_agreement_produces_high_confidence_gold(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AC1: yfinance and stooq agree within 25bps → RECONCILIATION_COMPLETE + HIGH Gold."""
    monkeypatch.setattr(fixtures_mod, "_FIXTURES_DIR", tmp_path / "fixtures")
    # Identical values for both sources → guaranteed agreement
    snapshot("yfinance", _make_yf_bronze_df(), tag="default")
    snapshot("stooq", _make_stooq_bronze_df(), tag="default")

    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)
    bb = await _run_pipeline(store, cfg, run_id="ac1-run")

    # RECONCILIATION_COMPLETE must be published
    recon_events = bb.get_events(topic=TopicType.RECONCILIATION_COMPLETE)
    assert len(recon_events) == 1, "Expected RECONCILIATION_COMPLETE"
    payload = json.loads(recon_events[0]["payload"])
    assert payload["breaks"] == 0, f"Unexpected breaks: {payload}"
    assert payload["golden_records"] > 0, "Expected Gold records to be written"

    # No breaks
    assert bb.get_events(topic=TopicType.BREAK_DETECTED) == []

    # Gold Parquet exists and all rows have confidence=HIGH
    gold_path = store._gold / "ac1-run" / f"{_BDATE.isoformat()}.parquet"
    assert gold_path.exists(), f"Gold not written: {gold_path}"
    gold_df = pd.read_parquet(gold_path)
    assert len(gold_df) > 0
    unique_conf = gold_df["confidence"].unique()
    assert (gold_df["confidence"] == "HIGH").all(), f"Not all rows HIGH: {unique_conf}"

    # Both sources in quorum_sources for every row
    for _, row in gold_df.iterrows():
        sources = json.loads(str(row["quorum_sources"]))
        assert "yfinance" in sources, f"yfinance missing from quorum: {sources}"
        assert "stooq" in sources, f"stooq missing from quorum: {sources}"

    # DecisionRecord persisted (one per elected GoldenRecord — FR-A8 design change)
    decisions = store.query(
        "SELECT * FROM decisions WHERE agent = 'reconciliation' AND decision_type = 'RECONCILE'"
    )
    assert len(decisions) >= 1, "Expected at least one RECONCILE DecisionRecord"
    assert len(decisions) == len(gold_df), "One RECONCILE decision per GoldenRecord (FK linkage)"

    await bb.stop()
    store.close()


# ---------------------------------------------------------------------------
# AC2 — Seeded disagreement beyond tolerance → BREAK_DETECTED + dissent recorded
# ---------------------------------------------------------------------------


async def test_ac2_disagreement_produces_break_with_dissent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AC2: stooq values shifted 10% beyond 25bps tolerance → BREAK_DETECTED + quarantine."""
    monkeypatch.setattr(fixtures_mod, "_FIXTURES_DIR", tmp_path / "fixtures")
    snapshot("yfinance", _make_yf_bronze_df(), tag="default")

    # Shift stooq price columns by 10% → ~1000bps, far above 25bps tolerance.
    # DESIGN-NOTE: CROSS_SOURCE_BREAK defaults to column="value" (Silver format);
    # Bronze fixtures use OHLCAV columns, so we shift those directly here (FR-T1).
    stooq_df = _make_stooq_bronze_df()
    for col in ("Open", "High", "Low", "Close", "Adj Close"):
        stooq_df[col] = stooq_df[col] * 1.10
    snapshot("stooq", stooq_df, tag="default")

    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)
    bb = await _run_pipeline(store, cfg, run_id="ac2-run")

    # BREAK_DETECTED events published (one per breaking field)
    break_events = bb.get_events(topic=TopicType.BREAK_DETECTED)
    assert len(break_events) > 0, "Expected at least one BREAK_DETECTED"

    # Each break event has required fields
    for ev in break_events:
        brk = json.loads(ev["payload"])
        assert "instrument_id" in brk
        assert "field" in brk
        assert "dissenting_sources" in brk, "dissenting_sources must be recorded"
        assert "source_values" in brk, "source_values must be recorded"
        assert "tolerance_band" in brk, "tolerance_band must be recorded"

    # RECONCILIATION_COMPLETE still published (pipeline continues even with breaks)
    assert len(bb.get_events(topic=TopicType.RECONCILIATION_COMPLETE)) == 1

    # Quarantine written for reconciliation breaks
    quarantine_root = tmp_path / "quarantine" / "ac2-run" / "reconciliation_break"
    assert quarantine_root.exists() and list(
        quarantine_root.glob("*.parquet")
    ), "Quarantine must be written for reconciliation breaks"

    # DecisionRecord persisted for each elected GoldenRecord (non-breaking fields only)
    decisions = store.query("SELECT * FROM decisions WHERE agent = 'reconciliation'")
    assert len(decisions) >= 1, "At least some reconciliation decisions must be written"

    await bb.stop()
    store.close()


# ---------------------------------------------------------------------------
# NFR-6 — Single source failure does not abort the run
# ---------------------------------------------------------------------------


async def test_nfr6_single_source_failure_does_not_abort(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """NFR-6: stooq INGESTION_FAILED → reconciliation still fires with yfinance only."""
    monkeypatch.setattr(fixtures_mod, "_FIXTURES_DIR", tmp_path / "fixtures")
    snapshot("yfinance", _make_yf_bronze_df(), tag="default")
    # stooq fixture deliberately omitted → fixture load will raise → INGESTION_FAILED

    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)
    bb = await _run_pipeline(store, cfg, run_id="nfr6-run")

    # yfinance should succeed
    ingest_complete = bb.get_events(topic=TopicType.INGESTION_COMPLETE)
    yf_complete = [
        e for e in ingest_complete if json.loads(e["payload"]).get("source_id") == "yfinance"
    ]
    assert len(yf_complete) == 1

    # stooq should fail
    ingest_failed = bb.get_events(topic=TopicType.INGESTION_FAILED)
    st_failed = [e for e in ingest_failed if json.loads(e["payload"]).get("source_id") == "stooq"]
    assert len(st_failed) == 1, "Expected INGESTION_FAILED for stooq"

    # Reconciliation still fires (with just yfinance — will produce breaks since min_agreeing=2)
    recon_events = bb.get_events(topic=TopicType.RECONCILIATION_COMPLETE)
    assert len(recon_events) == 1, "RECONCILIATION_COMPLETE must fire even with one source failing"

    await bb.stop()
    store.close()
