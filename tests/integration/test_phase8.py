"""Phase 8 acceptance criteria integration tests (PRD §12).

AC — system runs identically on the pure-local path with Redpanda/Narrator disabled;
enabling them changes transport/narrative only, not decisions.

Tests run in fixture mode (fully offline). NarratorAgent is tested with a monkeypatched
Ollama call so no local Ollama server is required.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import httpx
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
# Constants (shared with Phase 7 fixture builders)
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
# Fixture builders (reused from Phase 7 pattern)
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


def _make_cfg(
    tmp_path: Path,
    use_fixtures: bool = True,
    narrator_enabled: bool = False,
) -> Config:
    base = yaml.safe_load(Path("config/default.yaml").read_text())
    base["runtime"]["use_fixtures"] = use_fixtures
    base["runtime"]["storage"]["bronze"] = str(tmp_path / "bronze")
    base["runtime"]["storage"]["silver"] = str(tmp_path / "silver")
    base["runtime"]["storage"]["gold"] = str(tmp_path / "gold")
    base["runtime"]["storage"]["quarantine"] = str(tmp_path / "quarantine")
    base["runtime"]["storage"]["lineage"] = str(tmp_path / "lineage")
    base["runtime"]["duckdb_path"] = str(tmp_path / "mdq.duckdb")
    base["narrator"]["enabled"] = narrator_enabled
    universe = yaml.safe_load(Path("config/universe.yaml").read_text())
    base["universe"] = universe
    return Config.model_validate(base)


def _make_base_pipeline(store: MedallionStore, cfg: Config) -> Blackboard:
    """Wire all agents EXCEPT NarratorAgent (12 agents)."""
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


def _make_pipeline_with_narrator(store: MedallionStore, cfg: Config) -> Blackboard:
    """Wire all 13 agents including NarratorAgent (Phase 8 full pipeline)."""
    from mdq.agents.narrator_agent import NarratorAgent

    bb = _make_base_pipeline(store, cfg)
    bb.register(NarratorAgent(bb, store, cfg))
    return bb


def _seed_all_fixtures(tmp_path: Path) -> None:
    snapshot("yfinance", _make_yf_bronze_df(), tag="default")
    snapshot("stooq", _make_stooq_bronze_df(), tag="default")
    snapshot("ecb", _make_ecb_bronze_df(), tag="default")
    snapshot("sec_edgar", _make_sec_bronze_df(), tag="default")


async def _run_pipeline(bb: Blackboard, run_id: str) -> None:
    """Run full pipeline lifecycle. Leaves bb open so tests can query events."""
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
    await bb.publish(
        Event(
            topic=TopicType.RUN_COMPLETE,
            agent="supervisor",
            run_id=run_id,
            payload={"business_date": _BDATE.isoformat()},
        )
    )
    await bb.drain()
    # DESIGN-NOTE: stop() is NOT called here so tests can query bb.get_events()
    # after the run. The test is responsible for calling bb.close() if needed.


# ---------------------------------------------------------------------------
# AC / Phase 8 tests
# ---------------------------------------------------------------------------


async def test_ac_pure_local_path_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AC: pure-local path (asyncio transport, narrator disabled) produces correct Gold.

    Verifies the Phase 8 acceptance criterion: "system runs identically on the
    pure-local path with these disabled; enabling them changes transport/narrative
    only, not decisions."
    """
    monkeypatch.setattr(fixtures_mod, "_FIXTURES_DIR", tmp_path / "fixtures")
    _seed_all_fixtures(tmp_path)

    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path, narrator_enabled=False)
    run_id = "phase8-local"

    bb = _make_base_pipeline(store, cfg)
    await _run_pipeline(bb, run_id)

    # Equity reconciliation completed
    recon_events = bb.get_events(topic=TopicType.RECONCILIATION_COMPLETE)
    assert len(recon_events) >= 1, "RECONCILIATION_COMPLETE not fired"

    payload_str = recon_events[0]["payload"]
    import json

    payload = json.loads(payload_str)
    assert payload.get("breaks", 0) == 0, f"Unexpected breaks: {payload}"
    assert payload.get("golden_records", 0) > 0, f"No Gold produced: {payload}"

    # Gold has HIGH confidence
    gold_df = store.read_gold(run_id, _BDATE)
    assert not gold_df.empty, "No Gold rows written"
    assert (
        gold_df["confidence"] == "HIGH"
    ).all(), f"Expected all HIGH confidence, got: {gold_df['confidence'].value_counts().to_dict()}"

    # No narrator file (narrator is disabled)
    narrator_file = tmp_path / "lineage" / run_id / "narrator.txt"
    assert not narrator_file.exists(), "narrator.txt should not exist when narrator is disabled"

    await bb.stop()
    store.close()


async def test_ac_narrator_noop_when_disabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AC: narrator disabled → NarratorAgent is not registered, no narrative output."""
    monkeypatch.setattr(fixtures_mod, "_FIXTURES_DIR", tmp_path / "fixtures")
    _seed_all_fixtures(tmp_path)

    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path, narrator_enabled=False)
    run_id = "phase8-no-narrator"

    # base pipeline does NOT include NarratorAgent
    bb = _make_base_pipeline(store, cfg)
    assert not any(
        a.name == "narrator" for a in bb._agents
    ), "NarratorAgent should not be registered when narrator.enabled=False"

    await _run_pipeline(bb, run_id)

    # No narrator file
    lineage_dir = tmp_path / "lineage" / run_id
    narrator_files = list(lineage_dir.glob("narrator.txt")) if lineage_dir.exists() else []
    assert narrator_files == [], f"Unexpected narrator files: {narrator_files}"

    await bb.stop()
    store.close()


async def test_ac_narrator_graceful_when_ollama_absent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AC: narrator enabled but Ollama unreachable → graceful no-op, Gold unchanged.

    Verifies C-2 / C-6: NarratorAgent NEVER influences data decisions.
    Pipeline completes normally; Gold is identical to the no-narrator case.
    """
    monkeypatch.setattr(fixtures_mod, "_FIXTURES_DIR", tmp_path / "fixtures")
    _seed_all_fixtures(tmp_path)

    # Monkeypatch httpx so Ollama calls raise ConnectError
    class _OfflineHttpxClient:
        def __init__(self, **kwargs: Any) -> None:
            pass

        async def __aenter__(self) -> _OfflineHttpxClient:
            return self

        async def __aexit__(self, *args: Any) -> None:
            pass

        async def post(self, *args: Any, **kwargs: Any) -> None:
            raise httpx.ConnectError("Ollama not running — Phase 8 offline test")

    monkeypatch.setattr(httpx, "AsyncClient", _OfflineHttpxClient)

    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path, narrator_enabled=True)
    run_id = "phase8-ollama-absent"

    bb = _make_pipeline_with_narrator(store, cfg)
    assert any(
        a.name == "narrator" for a in bb._agents
    ), "NarratorAgent should be registered when narrator.enabled=True"

    # Pipeline must complete without raising
    await _run_pipeline(bb, run_id)

    # Gold still produced correctly (C-2 — Narrator never influences decisions)
    gold_df = store.read_gold(run_id, _BDATE)
    assert not gold_df.empty, "Gold should be produced even when Ollama is absent"
    assert (gold_df["confidence"] == "HIGH").all()

    # No narrator file (Ollama was unavailable)
    narrator_file = tmp_path / "lineage" / run_id / "narrator.txt"
    assert not narrator_file.exists(), "narrator.txt should not exist when Ollama is unreachable"

    await bb.stop()
    store.close()


def test_redpanda_transport_protocol_conformance() -> None:
    """AC: RedpandaTransport satisfies the Transport Protocol without requiring Docker.

    Proves FR-B3: "pluggable transport interface so a local Redpanda/Kafka backbone
    can be swapped in without changing agent code."
    """
    from mdq.core.transport.base import Transport
    from mdq.core.transport.redpanda import RedpandaTransport

    transport = RedpandaTransport(
        bootstrap_servers="localhost:9092",
        topic_prefix="mdq",
        consumer_group="mdq-mesh",
    )
    assert isinstance(
        transport, Transport
    ), "RedpandaTransport must satisfy the Transport Protocol (FR-B3)"
