"""Unit tests for AnomalyAgent — z-score, IQR, and volatility-regime filter (FR-A5, NFR-3)."""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import yaml

from harness.inject import build_silver_history
from mdq.agents.anomaly_agent import AnomalyAgent
from mdq.core.blackboard import Blackboard
from mdq.core.config import Config
from mdq.core.events import Event, TopicType
from mdq.core.store import MedallionStore

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_BDATE = date(2024, 2, 1)  # end of a 25-day history window
_SOURCE_ID = "yfinance"
_INSTRUMENTS = ["AAPL"]

# ---------------------------------------------------------------------------
# Helpers
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


def _write_history(store: MedallionStore, history_df: pd.DataFrame, end_date: date) -> None:
    """Write each day of history_df as a separate Silver file."""
    for bdate, day_df in history_df.groupby(history_df["business_date"].dt.date):
        run_id = f"hist-{bdate.isoformat()}"
        store.write_silver(day_df.reset_index(drop=True), run_id, bdate, "yfinance")  # type: ignore[arg-type]


def _contract_passed_event(run_id: str, business_date: date = _BDATE) -> Event:
    return Event(
        topic=TopicType.CONTRACT_PASSED,
        agent="contract",
        run_id=run_id,
        payload={
            "source_id": _SOURCE_ID,
            "business_date": business_date.isoformat(),
            "silver_rows": 6,
        },
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_anomaly_agent_detects_large_spike(tmp_path: Path) -> None:
    """A 100× price spike triggers ANOMALY_DETECTED with is_likely_error=True."""
    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)

    # Build 25 days of normal history (no elevated recent vol)
    end_hist = _BDATE - timedelta(days=1)
    history_df = build_silver_history(_INSTRUMENTS, n_days=25, end_date=end_hist, seed=1)
    _write_history(store, history_df, end_hist)

    # Current day: inject a 100× spike on CLOSE
    current_df = build_silver_history(_INSTRUMENTS, n_days=1, end_date=_BDATE, seed=99)
    close_mask = current_df["field"] == "CLOSE"
    current_df.loc[close_mask, "value"] = current_df.loc[close_mask, "value"] * 100.0
    store.write_silver(current_df, "spike-run", _BDATE, "yfinance")

    bb = Blackboard(db_path=":memory:")
    bb.register(AnomalyAgent(bb, store, cfg))
    await bb.start()
    await bb.publish(_contract_passed_event("spike-run"))
    await bb.drain()

    events = bb.get_events(topic=TopicType.ANOMALY_DETECTED)
    assert len(events) == 1
    anomalies = json.loads(events[0]["payload"])["anomalies"]
    close_anomaly = next((a for a in anomalies if a["field"] == "CLOSE"), None)
    assert close_anomaly is not None
    assert close_anomaly["is_likely_error"] is True
    assert close_anomaly["reason"] == "zscore_exceeded"

    await bb.stop()
    store.close()


async def test_anomaly_agent_volatility_regime_not_error(tmp_path: Path) -> None:
    """A spike during elevated recent volatility is classified as is_likely_error=False."""
    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)

    # History: last 5 days have 10× normal volatility to establish a regime
    end_hist = _BDATE - timedelta(days=1)
    vol_multipliers = {0: 10.0, 1: 10.0, 2: 10.0, 3: 10.0, 4: 10.0}
    history_df = build_silver_history(
        _INSTRUMENTS, n_days=25, end_date=end_hist, vol_multipliers=vol_multipliers, seed=2
    )
    _write_history(store, history_df, end_hist)

    # Current day: a 3× spike (large, but regime is already very volatile)
    current_df = build_silver_history(_INSTRUMENTS, n_days=1, end_date=_BDATE, seed=77)
    close_mask = current_df["field"] == "CLOSE"
    current_df.loc[close_mask, "value"] = current_df.loc[close_mask, "value"] * 3.0
    store.write_silver(current_df, "regime-run", _BDATE, "yfinance")

    bb = Blackboard(db_path=":memory:")
    bb.register(AnomalyAgent(bb, store, cfg))
    await bb.start()
    await bb.publish(_contract_passed_event("regime-run"))
    await bb.drain()

    events = bb.get_events(topic=TopicType.ANOMALY_DETECTED)
    # If an anomaly event is published, the CLOSE anomaly must NOT be a likely error
    if events:
        anomalies = json.loads(events[0]["payload"])["anomalies"]
        close_anomaly = next((a for a in anomalies if a["field"] == "CLOSE"), None)
        if close_anomaly is not None:
            assert close_anomaly["is_likely_error"] is False
            assert close_anomaly["reason"] == "volatility_regime"

    await bb.stop()
    store.close()


async def test_anomaly_agent_clean_data_no_event(tmp_path: Path) -> None:
    """Clean, normal data produces no ANOMALY_DETECTED event."""
    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)

    end_hist = _BDATE - timedelta(days=1)
    history_df = build_silver_history(_INSTRUMENTS, n_days=25, end_date=end_hist, seed=3)
    _write_history(store, history_df, end_hist)

    current_df = build_silver_history(_INSTRUMENTS, n_days=1, end_date=_BDATE, seed=4)
    store.write_silver(current_df, "clean-run", _BDATE, "yfinance")

    bb = Blackboard(db_path=":memory:")
    bb.register(AnomalyAgent(bb, store, cfg))
    await bb.start()
    await bb.publish(_contract_passed_event("clean-run"))
    await bb.drain()

    assert bb.get_events(topic=TopicType.ANOMALY_DETECTED) == []

    await bb.stop()
    store.close()


async def test_anomaly_agent_writes_decision_record(tmp_path: Path) -> None:
    """AnomalyAgent persists a DecisionRecord per batch regardless of anomaly count."""
    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)

    end_hist = _BDATE - timedelta(days=1)
    history_df = build_silver_history(_INSTRUMENTS, n_days=25, end_date=end_hist, seed=5)
    _write_history(store, history_df, end_hist)

    current_df = build_silver_history(_INSTRUMENTS, n_days=1, end_date=_BDATE, seed=6)
    store.write_silver(current_df, "dec-run", _BDATE, "yfinance")

    bb = Blackboard(db_path=":memory:")
    bb.register(AnomalyAgent(bb, store, cfg))
    await bb.start()
    await bb.publish(_contract_passed_event("dec-run"))
    await bb.drain()

    decisions = store.query(
        "SELECT * FROM decisions WHERE agent = 'anomaly' AND rule_applied = 'anomaly_zscore_iqr'"
    )
    assert len(decisions) == 1

    await bb.stop()
    store.close()


async def test_anomaly_agent_name_and_subscriptions(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)
    bb = Blackboard(db_path=":memory:")
    agent = AnomalyAgent(bb, store, cfg)
    assert agent.name == "anomaly"
    assert TopicType.CONTRACT_PASSED in agent.subscriptions
    store.close()
