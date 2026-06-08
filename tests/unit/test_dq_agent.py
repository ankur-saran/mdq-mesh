"""Unit tests for DQAgent — each of the 5 rules in isolation (FR-A3, NFR-3)."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import yaml

from mdq.agents.dq_agent import (
    DQAgent,
    _check_duplicates,
    _check_monotonicity,
    _check_nulls,
    _check_range,
    _check_staleness,
)
from mdq.core.blackboard import Blackboard
from mdq.core.config import Config
from mdq.core.events import Event, TopicType
from mdq.core.store import MedallionStore

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_BDATE = date(2024, 1, 2)
_SOURCE_ID = "yfinance"
_INSTRUMENTS = ["AAPL", "MSFT", "NVDA"]
_FIELDS = ["OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE", "VOLUME"]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_silver_df(
    business_date: date = _BDATE,
    instruments: list[str] = _INSTRUMENTS,
    fetch_ts: datetime | None = None,
) -> pd.DataFrame:
    """Return a clean synthetic Silver DataFrame."""
    if fetch_ts is None:
        fetch_ts = datetime(2024, 1, 2, 21, 0, 0, tzinfo=UTC)
    rows = []
    field_values = {
        "OPEN": 100.0,
        "HIGH": 110.0,
        "LOW": 95.0,
        "CLOSE": 102.0,
        "ADJ_CLOSE": 101.5,
        "VOLUME": 1_000_000.0,
    }
    for inst in instruments:
        for field, val in field_values.items():
            rows.append(
                {
                    "instrument_id": inst,
                    "business_date": pd.Timestamp(business_date),
                    "field": field,
                    "value": val,
                    "currency": "USD",
                    "source_id": _SOURCE_ID,
                    "source_symbol": inst,
                    "fetch_ts": pd.Timestamp(fetch_ts),
                    "event_ts": pd.Timestamp(fetch_ts),
                    "content_hash": f"hash-{inst}-{field}",
                    "ca_adjusted": False,
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


def _contract_passed_event(run_id: str = "test-run") -> Event:
    return Event(
        topic=TopicType.CONTRACT_PASSED,
        agent="contract",
        run_id=run_id,
        payload={"source_id": _SOURCE_ID, "business_date": _BDATE.isoformat(), "silver_rows": 18},
    )


# ---------------------------------------------------------------------------
# Rule unit tests (pure function — no I/O)
# ---------------------------------------------------------------------------


def test_null_check_detects_nan() -> None:
    df = _make_silver_df()
    df.loc[0, "value"] = float("nan")
    failures = _check_nulls(df, severity="high")
    assert len(failures) == 1
    assert failures[0].rule == "null_check"
    assert failures[0].severity == "high"


def test_null_check_passes_clean_data() -> None:
    df = _make_silver_df()
    assert _check_nulls(df, severity="high") == []


def test_range_check_detects_negative_value() -> None:
    df = _make_silver_df()
    # Set a CLOSE value to negative — must trigger range_check
    close_mask = (df["instrument_id"] == "AAPL") & (df["field"] == "CLOSE")
    df.loc[close_mask, "value"] = -10.0
    failures = _check_range(df, severity="high", min_val=0.0)
    assert len(failures) == 1
    assert failures[0].rule == "range_check"
    assert failures[0].field == "CLOSE"


def test_range_check_skips_volume() -> None:
    """VOLUME = 0 is valid; range_check must not flag it."""
    df = _make_silver_df()
    vol_mask = df["field"] == "VOLUME"
    df.loc[vol_mask, "value"] = 0.0
    failures = _check_range(df, severity="high", min_val=0.0)
    assert all(f.field != "VOLUME" for f in failures)


def test_staleness_check_flags_old_fetch_ts() -> None:
    stale_ts = datetime(2023, 12, 29, 21, 0, 0, tzinfo=UTC)  # 4 days before 2024-01-02
    df = _make_silver_df(fetch_ts=stale_ts)
    failures = _check_staleness(df, _BDATE, max_days=1, severity="high")
    assert len(failures) > 0
    assert all(f.rule == "staleness_check" for f in failures)
    assert all(f.severity == "high" for f in failures)


def test_staleness_check_passes_fresh_data() -> None:
    fresh_ts = datetime(2024, 1, 2, 20, 0, 0, tzinfo=UTC)
    df = _make_silver_df(fetch_ts=fresh_ts)
    assert _check_staleness(df, _BDATE, max_days=1, severity="high") == []


def test_monotonicity_check_detects_high_lt_low() -> None:
    df = _make_silver_df()
    # Set HIGH = 90 < LOW = 95 for AAPL to trigger monotonicity violation
    high_mask = (df["instrument_id"] == "AAPL") & (df["field"] == "HIGH")
    df.loc[high_mask, "value"] = 90.0
    failures = _check_monotonicity(df, severity="medium")
    assert any(f.rule == "monotonicity_check" for f in failures)
    assert any(f.severity == "medium" for f in failures)


def test_duplicate_check_detects_dup_row() -> None:
    df = _make_silver_df()
    dup = df.iloc[:1].copy()
    combined = pd.concat([df, dup], ignore_index=True)
    failures = _check_duplicates(combined, severity="medium")
    assert len(failures) == 1
    assert failures[0].rule == "duplicate_check"


def test_duplicate_check_passes_clean_data() -> None:
    df = _make_silver_df()
    assert _check_duplicates(df, severity="medium") == []


# ---------------------------------------------------------------------------
# Agent integration tests (with store + blackboard)
# ---------------------------------------------------------------------------


async def test_dq_agent_passes_clean_silver(tmp_path: Path) -> None:
    """Clean Silver → DQ_PASSED event published, no quarantine written."""
    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)
    silver_df = _make_silver_df()
    store.write_silver(silver_df, "test-run", _BDATE, "yfinance")

    bb = Blackboard(db_path=":memory:")
    bb.register(DQAgent(bb, store, cfg))
    await bb.start()
    await bb.publish(_contract_passed_event())
    await bb.drain()

    passed = bb.get_events(topic=TopicType.DQ_PASSED)
    failed = bb.get_events(topic=TopicType.DQ_FAILURE)
    assert len(passed) == 1
    assert len(failed) == 0

    quarantine = list((tmp_path / "quarantine").rglob("*.parquet"))
    assert len(quarantine) == 0

    await bb.stop()
    store.close()


async def test_dq_agent_null_burst_publishes_dq_failure(tmp_path: Path) -> None:
    """NULL_BURST → DQ_FAILURE with null_check rule, high severity."""
    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)
    silver_df = _make_silver_df()
    silver_df.loc[0, "value"] = float("nan")
    store.write_silver(silver_df, "test-run", _BDATE, "yfinance")

    bb = Blackboard(db_path=":memory:")
    bb.register(DQAgent(bb, store, cfg))
    await bb.start()
    await bb.publish(_contract_passed_event())
    await bb.drain()

    failed_events = bb.get_events(topic=TopicType.DQ_FAILURE)
    assert len(failed_events) == 1
    failures = json.loads(failed_events[0]["payload"])["failures"]
    rules = [f["rule"] for f in failures]
    assert "null_check" in rules
    assert any(f["severity"] == "high" for f in failures if f["rule"] == "null_check")

    await bb.stop()
    store.close()


async def test_dq_agent_high_severity_writes_quarantine(tmp_path: Path) -> None:
    """High-severity DQ failure → offending rows written to quarantine."""
    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)
    silver_df = _make_silver_df()
    silver_df.loc[0, "value"] = float("nan")
    store.write_silver(silver_df, "test-run", _BDATE, "yfinance")

    bb = Blackboard(db_path=":memory:")
    bb.register(DQAgent(bb, store, cfg))
    await bb.start()
    await bb.publish(_contract_passed_event())
    await bb.drain()

    quarantine_files = list((tmp_path / "quarantine").rglob("*.parquet"))
    assert len(quarantine_files) > 0

    await bb.stop()
    store.close()


async def test_dq_agent_writes_decision_record(tmp_path: Path) -> None:
    """DQAgent persists exactly one DecisionRecord per batch."""
    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)
    store.write_silver(_make_silver_df(), "test-run", _BDATE, "yfinance")

    bb = Blackboard(db_path=":memory:")
    bb.register(DQAgent(bb, store, cfg))
    await bb.start()
    await bb.publish(_contract_passed_event())
    await bb.drain()

    decisions = store.query("SELECT * FROM decisions WHERE agent = 'dq' AND decision_type = 'DQ'")
    assert len(decisions) == 1

    await bb.stop()
    store.close()


async def test_dq_agent_name_and_subscriptions(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)
    bb = Blackboard(db_path=":memory:")
    agent = DQAgent(bb, store, cfg)
    assert agent.name == "dq"
    assert TopicType.CONTRACT_PASSED in agent.subscriptions
    store.close()
