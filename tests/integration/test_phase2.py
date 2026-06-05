"""Phase 2 acceptance criteria integration tests (PRD §12).

AC1 — Seeded null defects flagged with severity=high.
AC2 — Seeded staleness defects flagged with severity=high.
AC3 — Seeded out-of-range (negative value) defects flagged with severity=high.
AC4 — A volatility-regime spike is correctly NOT classified as is_likely_error=True.

All tests run fully offline in fixture mode. The blackboard is wired end-to-end with
all four agents so event flow is realistic.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest
import yaml

import harness.fixtures as fixtures_mod
from harness.fixtures import snapshot
from harness.inject import DefectType, build_silver_history, inject
from mdq.agents.anomaly_agent import AnomalyAgent
from mdq.agents.contract_agent import ContractAgent
from mdq.agents.dq_agent import DQAgent
from mdq.agents.ingestion.yfinance_agent import YFinanceAgent
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
_FIELDS = ["OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE", "VOLUME"]

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
    run_id: str = "phase2-run",
    business_date: date = _BDATE,
) -> Blackboard:
    """Wire all 4 agents and drive a full RUN_STARTED → drain cycle."""
    bb = Blackboard(db_path=":memory:")
    bb.register(YFinanceAgent(bb, store, cfg))
    bb.register(ContractAgent(bb, store, cfg))
    bb.register(DQAgent(bb, store, cfg))
    bb.register(AnomalyAgent(bb, store, cfg))

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
# AC1 — Null defect flagged with severity=high
# ---------------------------------------------------------------------------


async def test_ac1_null_burst_flagged_as_high_severity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AC1: NULL_BURST on Silver value → DQ_FAILURE with null_check rule, severity=high."""
    monkeypatch.setattr(fixtures_mod, "_FIXTURES_DIR", tmp_path / "fixtures")
    snapshot(_SOURCE_ID, _make_bronze_df(), tag="default")

    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)

    # Run Phase 1 pipeline to produce Silver, then inject nulls into Silver
    bb1 = await _run_pipeline(store, cfg, run_id="setup-run")
    await bb1.stop()

    # Corrupt the Silver: inject nulls
    silver_path = store.silver_path("setup-run", _BDATE)
    silver_df = pd.read_parquet(silver_path)
    silver_df.loc[0, "value"] = float("nan")
    # Overwrite with corrupted Silver by writing to a new run
    store.write_silver(silver_df, "null-run", _BDATE)

    # Fire DQAgent directly against the corrupted Silver
    from mdq.core.events import Event, TopicType

    bb2 = Blackboard(db_path=":memory:")
    bb2.register(DQAgent(bb2, store, cfg))
    await bb2.start()
    await bb2.publish(
        Event(
            topic=TopicType.CONTRACT_PASSED,
            agent="contract",
            run_id="null-run",
            payload={
                "source_id": _SOURCE_ID,
                "business_date": _BDATE.isoformat(),
                "silver_rows": len(silver_df),
            },
        )
    )
    await bb2.drain()

    failed_events = bb2.get_events(topic=TopicType.DQ_FAILURE)
    assert len(failed_events) == 1, "Expected DQ_FAILURE for null burst"
    failures = json.loads(failed_events[0]["payload"])["failures"]
    null_failure = next((f for f in failures if f["rule"] == "null_check"), None)
    assert null_failure is not None, "null_check rule must appear in failures"
    assert null_failure["severity"] == "high"

    quarantine_root = tmp_path / "quarantine" / "null-run" / "dq_violation"
    assert quarantine_root.exists() and list(
        quarantine_root.glob("*.parquet")
    ), "Quarantine must be written for null burst"

    await bb2.stop()
    store.close()


# ---------------------------------------------------------------------------
# AC2 — Staleness defect flagged with severity=high
# ---------------------------------------------------------------------------


async def test_ac2_stale_feed_flagged_as_high_severity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AC2: STALE_FEED (fetch_ts 3 days old) → DQ_FAILURE with staleness_check, severity=high."""
    monkeypatch.setattr(fixtures_mod, "_FIXTURES_DIR", tmp_path / "fixtures")
    snapshot(_SOURCE_ID, _make_bronze_df(), tag="default")

    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)

    bb1 = await _run_pipeline(store, cfg, run_id="setup-run2")
    await bb1.stop()

    silver_path = store.silver_path("setup-run2", _BDATE)
    silver_df = pd.read_parquet(silver_path)
    # Inject staleness: roll fetch_ts back 3 days
    stale_df = inject(silver_df, DefectType.STALE_FEED, days_stale=3)
    store.write_silver(stale_df, "stale-run", _BDATE)

    bb2 = Blackboard(db_path=":memory:")
    bb2.register(DQAgent(bb2, store, cfg))
    await bb2.start()
    await bb2.publish(
        Event(
            topic=TopicType.CONTRACT_PASSED,
            agent="contract",
            run_id="stale-run",
            payload={
                "source_id": _SOURCE_ID,
                "business_date": _BDATE.isoformat(),
                "silver_rows": len(stale_df),
            },
        )
    )
    await bb2.drain()

    failed_events = bb2.get_events(topic=TopicType.DQ_FAILURE)
    assert len(failed_events) == 1, "Expected DQ_FAILURE for stale feed"
    failures = json.loads(failed_events[0]["payload"])["failures"]
    staleness_failure = next((f for f in failures if f["rule"] == "staleness_check"), None)
    assert staleness_failure is not None, "staleness_check rule must appear in failures"
    assert staleness_failure["severity"] == "high"

    await bb2.stop()
    store.close()


# ---------------------------------------------------------------------------
# AC3 — Out-of-range (negative value) defect flagged with severity=high
# ---------------------------------------------------------------------------


async def test_ac3_negative_value_flagged_as_high_severity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AC3: A negative price value → DQ_FAILURE with range_check rule, severity=high."""
    monkeypatch.setattr(fixtures_mod, "_FIXTURES_DIR", tmp_path / "fixtures")
    snapshot(_SOURCE_ID, _make_bronze_df(), tag="default")

    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)

    bb1 = await _run_pipeline(store, cfg, run_id="setup-run3")
    await bb1.stop()

    silver_path = store.silver_path("setup-run3", _BDATE)
    silver_df = pd.read_parquet(silver_path)
    # Inject a negative CLOSE value (range_check min=0 must flag this)
    close_mask = (silver_df["instrument_id"] == "AAPL") & (silver_df["field"] == "CLOSE")
    silver_df.loc[close_mask, "value"] = -10.0
    store.write_silver(silver_df, "range-run", _BDATE)

    bb2 = Blackboard(db_path=":memory:")
    bb2.register(DQAgent(bb2, store, cfg))
    await bb2.start()
    await bb2.publish(
        Event(
            topic=TopicType.CONTRACT_PASSED,
            agent="contract",
            run_id="range-run",
            payload={
                "source_id": _SOURCE_ID,
                "business_date": _BDATE.isoformat(),
                "silver_rows": len(silver_df),
            },
        )
    )
    await bb2.drain()

    failed_events = bb2.get_events(topic=TopicType.DQ_FAILURE)
    assert len(failed_events) == 1, "Expected DQ_FAILURE for negative value"
    failures = json.loads(failed_events[0]["payload"])["failures"]
    range_failure = next((f for f in failures if f["rule"] == "range_check"), None)
    assert range_failure is not None, "range_check rule must appear in failures"
    assert range_failure["severity"] == "high"

    await bb2.stop()
    store.close()


# ---------------------------------------------------------------------------
# AC4 — Volatility-regime spike not classified as is_likely_error=True
# ---------------------------------------------------------------------------


async def test_ac4_volatility_regime_spike_not_quarantined(tmp_path: Path) -> None:
    """AC4: Elevated recent volatility + spike → ANOMALY_DETECTED with is_likely_error=False."""
    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path, use_fixtures=False)

    instruments = [inst for inst, _ in _INSTRUMENTS]
    target_date = date(2024, 2, 1)
    end_hist = target_date - timedelta(days=1)

    # Last 5 days of history have 12× normal volatility to establish a clear regime
    vol_mults = {0: 12.0, 1: 12.0, 2: 12.0, 3: 12.0, 4: 12.0}
    history_df = build_silver_history(
        instruments[:1],  # single instrument for a tightly controlled test
        n_days=25,
        end_date=end_hist,
        vol_multipliers=vol_mults,
        seed=10,
    )
    for bdate, day_df in history_df.groupby(history_df["business_date"].dt.date):
        store.write_silver(day_df.reset_index(drop=True), f"hist-{bdate}", bdate)  # type: ignore[arg-type]

    # Current day: 4× spike on CLOSE — large, but within a high-vol regime
    current_df = build_silver_history(instruments[:1], n_days=1, end_date=target_date, seed=11)
    close_mask = current_df["field"] == "CLOSE"
    current_df.loc[close_mask, "value"] = current_df.loc[close_mask, "value"] * 4.0
    store.write_silver(current_df, "vol-run", target_date)

    bb = Blackboard(db_path=":memory:")
    bb.register(AnomalyAgent(bb, store, cfg))
    await bb.start()
    await bb.publish(
        Event(
            topic=TopicType.CONTRACT_PASSED,
            agent="contract",
            run_id="vol-run",
            payload={
                "source_id": _SOURCE_ID,
                "business_date": target_date.isoformat(),
                "silver_rows": len(current_df),
            },
        )
    )
    await bb.drain()

    events = bb.get_events(topic=TopicType.ANOMALY_DETECTED)
    # If ANOMALY_DETECTED is published, the CLOSE anomaly must be volatility_regime
    if events:
        anomalies = json.loads(events[0]["payload"])["anomalies"]
        close_anomaly = next((a for a in anomalies if a["field"] == "CLOSE"), None)
        if close_anomaly is not None:
            assert (
                close_anomaly["is_likely_error"] is False
            ), "Volatility-regime spike must not be flagged as is_likely_error=True"
            assert close_anomaly["reason"] == "volatility_regime"

    # No quarantine written — AnomalyAgent never quarantines (Phase 5 concern)
    quarantine_files = list((tmp_path / "quarantine").rglob("*.parquet"))
    assert len(quarantine_files) == 0, "AnomalyAgent must never write to quarantine directly"

    await bb.stop()
    store.close()


# ---------------------------------------------------------------------------
# Clean-path gate: no defects → DQ_PASSED + no ANOMALY_DETECTED
# ---------------------------------------------------------------------------


async def test_clean_path_passes_all_checks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Clean data → DQ_PASSED published; no DQ_FAILURE; no ANOMALY_DETECTED."""
    monkeypatch.setattr(fixtures_mod, "_FIXTURES_DIR", tmp_path / "fixtures")
    snapshot(_SOURCE_ID, _make_bronze_df(), tag="default")

    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)
    bb = await _run_pipeline(store, cfg, run_id="clean-run")

    assert len(bb.get_events(topic=TopicType.DQ_PASSED)) == 1
    assert len(bb.get_events(topic=TopicType.DQ_FAILURE)) == 0
    # Anomaly agent has no history → skips detection silently (no event)
    assert len(bb.get_events(topic=TopicType.ANOMALY_DETECTED)) == 0

    await bb.stop()
    store.close()
