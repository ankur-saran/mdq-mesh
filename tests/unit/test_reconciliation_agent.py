"""Unit tests for ReconciliationAgent — tolerance, quorum election, confidence (FR-A6, NFR-3)."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import yaml

from mdq.agents.reconciliation_agent import ReconciliationAgent, _elect, _within_tolerance
from mdq.core.blackboard import Blackboard
from mdq.core.config import Config, ReconciliationConfig, ToleranceConfig
from mdq.core.events import Event, TopicType
from mdq.core.schemas import Confidence
from mdq.core.store import MedallionStore

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_BDATE = date(2024, 1, 2)
_SOURCE_YF = "yfinance"
_SOURCE_ST = "stooq"

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


def _make_silver_row(
    instrument_id: str,
    field: str,
    value: float,
    source_id: str,
    business_date: date = _BDATE,
) -> dict[str, object]:
    fetch_ts = pd.Timestamp(datetime(2024, 1, 2, 21, 0, 0, tzinfo=UTC))
    return {
        "instrument_id": instrument_id,
        "business_date": pd.Timestamp(business_date),
        "field": field,
        "value": value,
        "currency": "USD",
        "source_id": source_id,
        "source_symbol": instrument_id,
        "fetch_ts": fetch_ts,
        "event_ts": fetch_ts,
        "content_hash": f"hash-{instrument_id}-{field}-{source_id}",
        "ca_adjusted": False,
    }


def _make_rec_cfg(bps: float = 25.0, min_agreeing: int = 2) -> ReconciliationConfig:
    return ReconciliationConfig(
        quorum={"min_agreeing_sources": min_agreeing},
        tolerances={
            "CLOSE": ToleranceConfig(type="relative_bps", value=bps),
            "OPEN": ToleranceConfig(type="relative_bps", value=bps),
            "VOLUME": ToleranceConfig(type="relative_pct", value=5.0),
        },
        on_no_quorum="break",
    )


def _dq_passed(source_id: str, run_id: str = "rec-run") -> Event:
    return Event(
        topic=TopicType.DQ_PASSED,
        agent="dq",
        run_id=run_id,
        payload={
            "source_id": source_id,
            "business_date": _BDATE.isoformat(),
        },
    )


def _dq_failed(source_id: str, run_id: str = "rec-run") -> Event:
    return Event(
        topic=TopicType.DQ_FAILURE,
        agent="dq",
        run_id=run_id,
        payload={
            "source_id": source_id,
            "business_date": _BDATE.isoformat(),
            "failures": [],
        },
    )


# ---------------------------------------------------------------------------
# Pure function tests — _within_tolerance
# ---------------------------------------------------------------------------


def test_within_tolerance_relative_bps_agree() -> None:
    tol = ToleranceConfig(type="relative_bps", value=25.0)
    # 182.00 vs 182.04 → diff=0.04/182.04*10000 ≈ 2.2 bps → within 25bps
    assert _within_tolerance(182.00, 182.04, tol) is True


def test_within_tolerance_relative_bps_disagree() -> None:
    tol = ToleranceConfig(type="relative_bps", value=25.0)
    # 182.00 vs 200.00 → diff=18/200*10000 = 900bps → outside 25bps
    assert _within_tolerance(182.00, 200.00, tol) is False


def test_within_tolerance_relative_pct_agree() -> None:
    tol = ToleranceConfig(type="relative_pct", value=5.0)
    # 1_000_000 vs 1_040_000 → 4% → within 5%
    assert _within_tolerance(1_000_000.0, 1_040_000.0, tol) is True


def test_within_tolerance_relative_pct_disagree() -> None:
    tol = ToleranceConfig(type="relative_pct", value=5.0)
    # 1_000_000 vs 1_100_000 → 10% → outside 5%
    assert _within_tolerance(1_000_000.0, 1_100_000.0, tol) is False


def test_within_tolerance_both_zero() -> None:
    tol = ToleranceConfig(type="relative_bps", value=25.0)
    assert _within_tolerance(0.0, 0.0, tol) is True


def test_within_tolerance_absolute() -> None:
    tol = ToleranceConfig(type="absolute", value=0.5)
    assert _within_tolerance(100.0, 100.4, tol) is True
    assert _within_tolerance(100.0, 100.6, tol) is False


# ---------------------------------------------------------------------------
# Pure function tests — _elect
# ---------------------------------------------------------------------------


def test_elect_two_sources_agree_high_confidence() -> None:
    """Two sources within tolerance → golden value elected, HIGH confidence."""
    rec_cfg = _make_rec_cfg(bps=25.0, min_agreeing=2)
    values = {_SOURCE_YF: 182.00, _SOURCE_ST: 182.02}
    golden, quorum, dissent, conf = _elect(values, "CLOSE", rec_cfg)
    assert golden is not None
    assert abs(golden - 182.01) < 0.01  # mean
    assert set(quorum) == {_SOURCE_YF, _SOURCE_ST}
    assert dissent == []
    assert conf == Confidence.HIGH


def test_elect_two_sources_disagree_break() -> None:
    """Two sources outside tolerance → no quorum, BREAK returned."""
    rec_cfg = _make_rec_cfg(bps=25.0, min_agreeing=2)
    values = {_SOURCE_YF: 182.00, _SOURCE_ST: 200.00}
    golden, quorum, dissent, conf = _elect(values, "CLOSE", rec_cfg)
    assert golden is None
    assert quorum == []
    assert conf == Confidence.LOW


def test_elect_single_source_below_min_agreeing() -> None:
    """Single source with min_agreeing=2 → break (insufficient quorum)."""
    rec_cfg = _make_rec_cfg(bps=25.0, min_agreeing=2)
    values = {_SOURCE_YF: 182.00}
    golden, quorum, dissent, conf = _elect(values, "CLOSE", rec_cfg)
    assert golden is None


def test_elect_three_sources_partial_agreement() -> None:
    """Three sources, two agree, one dissents → MEDIUM confidence."""
    rec_cfg = ReconciliationConfig(
        quorum={"min_agreeing_sources": 2},
        tolerances={"CLOSE": ToleranceConfig(type="relative_bps", value=25.0)},
        on_no_quorum="break",
    )
    values = {"src_a": 182.00, "src_b": 182.02, "src_c": 200.00}
    golden, quorum, dissent, conf = _elect(values, "CLOSE", rec_cfg)
    assert golden is not None
    assert set(quorum) == {"src_a", "src_b"}
    assert "src_c" in dissent
    assert conf == Confidence.MEDIUM


def test_elect_no_tolerance_config_falls_back_to_unanimous() -> None:
    """Field with no tolerance configured → unanimous agreement required."""
    rec_cfg = ReconciliationConfig(
        quorum={"min_agreeing_sources": 2},
        tolerances={},
        on_no_quorum="break",
    )
    values = {_SOURCE_YF: 182.00, _SOURCE_ST: 182.00}
    golden, quorum, dissent, conf = _elect(values, "CLOSE", rec_cfg)
    assert golden == 182.00
    assert conf == Confidence.HIGH


# ---------------------------------------------------------------------------
# Agent integration tests
# ---------------------------------------------------------------------------


async def test_reconciliation_agent_elects_gold_on_both_passed(tmp_path: Path) -> None:
    """DQ_PASSED from both sources → RECONCILIATION_COMPLETE + Gold Parquet written."""
    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)

    # Write Silver for both sources
    yf_rows = [_make_silver_row("AAPL", "CLOSE", 182.00, _SOURCE_YF)]
    st_rows = [_make_silver_row("AAPL", "CLOSE", 182.02, _SOURCE_ST)]
    store.write_silver(pd.DataFrame(yf_rows), "rec-run", _BDATE, _SOURCE_YF)
    store.write_silver(pd.DataFrame(st_rows), "rec-run", _BDATE, _SOURCE_ST)

    bb = Blackboard(db_path=":memory:")
    bb.register(ReconciliationAgent(bb, store, cfg))
    await bb.start()
    await bb.publish(_dq_passed(_SOURCE_YF))
    await bb.publish(_dq_passed(_SOURCE_ST))
    await bb.drain()

    recon_events = bb.get_events(topic=TopicType.RECONCILIATION_COMPLETE)
    assert len(recon_events) == 1
    payload = json.loads(recon_events[0]["payload"])
    assert payload["golden_records"] == 1
    assert payload["breaks"] == 0

    gold_path = store._gold / "rec-run" / f"{_BDATE.isoformat()}.parquet"
    assert gold_path.exists(), "Gold Parquet must be written"
    gold_df = pd.read_parquet(gold_path)
    assert len(gold_df) == 1
    assert gold_df["confidence"].iloc[0] == "HIGH"

    await bb.stop()
    store.close()


async def test_reconciliation_agent_break_on_disagreement(tmp_path: Path) -> None:
    """Sources outside tolerance → BREAK_DETECTED + quarantine written."""
    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)

    # stooq value is 10% higher — far outside 25bps tolerance
    yf_rows = [_make_silver_row("AAPL", "CLOSE", 182.00, _SOURCE_YF)]
    st_rows = [_make_silver_row("AAPL", "CLOSE", 200.00, _SOURCE_ST)]
    store.write_silver(pd.DataFrame(yf_rows), "break-run", _BDATE, _SOURCE_YF)
    store.write_silver(pd.DataFrame(st_rows), "break-run", _BDATE, _SOURCE_ST)

    bb = Blackboard(db_path=":memory:")
    bb.register(ReconciliationAgent(bb, store, cfg))
    await bb.start()
    bdate = _BDATE.isoformat()
    for src in [_SOURCE_YF, _SOURCE_ST]:
        await bb.publish(
            Event(
                topic=TopicType.DQ_PASSED,
                agent="dq",
                run_id="break-run",
                payload={"source_id": src, "business_date": bdate},
            )
        )
    await bb.drain()

    break_events = bb.get_events(topic=TopicType.BREAK_DETECTED)
    assert len(break_events) == 1
    brk = json.loads(break_events[0]["payload"])
    assert brk["instrument_id"] == "AAPL"
    assert brk["field"] == "CLOSE"
    assert "dissenting_sources" in brk
    assert "source_values" in brk

    quarantine_root = tmp_path / "quarantine" / "break-run" / "reconciliation_break"
    assert quarantine_root.exists() and list(quarantine_root.glob("*.parquet"))

    await bb.stop()
    store.close()


async def test_reconciliation_agent_fires_once_not_twice(tmp_path: Path) -> None:
    """Publishing DQ_PASSED for both sources triggers reconciliation exactly once."""
    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)

    yf_rows = [_make_silver_row("AAPL", "CLOSE", 182.00, _SOURCE_YF)]
    st_rows = [_make_silver_row("AAPL", "CLOSE", 182.02, _SOURCE_ST)]
    store.write_silver(pd.DataFrame(yf_rows), "rec-run", _BDATE, _SOURCE_YF)
    store.write_silver(pd.DataFrame(st_rows), "rec-run", _BDATE, _SOURCE_ST)

    bb = Blackboard(db_path=":memory:")
    bb.register(ReconciliationAgent(bb, store, cfg))
    await bb.start()
    await bb.publish(_dq_passed(_SOURCE_YF))
    await bb.publish(_dq_passed(_SOURCE_ST))
    await bb.drain()

    assert len(bb.get_events(topic=TopicType.RECONCILIATION_COMPLETE)) == 1

    await bb.stop()
    store.close()


async def test_reconciliation_agent_dq_failure_excludes_source(tmp_path: Path) -> None:
    """DQ_FAILURE from stooq → stooq excluded; yfinance-only → reconciliation proceeds."""
    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)

    yf_rows = [_make_silver_row("AAPL", "CLOSE", 182.00, _SOURCE_YF)]
    store.write_silver(pd.DataFrame(yf_rows), "partial-run", _BDATE, _SOURCE_YF)

    bb = Blackboard(db_path=":memory:")
    bb.register(ReconciliationAgent(bb, store, cfg))
    await bb.start()
    bdate = _BDATE.isoformat()
    await bb.publish(
        Event(
            topic=TopicType.DQ_PASSED,
            agent="dq",
            run_id="partial-run",
            payload={"source_id": _SOURCE_YF, "business_date": bdate},
        )
    )
    await bb.publish(
        Event(
            topic=TopicType.DQ_FAILURE,
            agent="dq",
            run_id="partial-run",
            payload={"source_id": _SOURCE_ST, "business_date": bdate, "failures": []},
        )
    )
    await bb.drain()

    # Reconciliation fires — only yfinance Silver available
    recon_events = bb.get_events(topic=TopicType.RECONCILIATION_COMPLETE)
    assert len(recon_events) == 1
    # With only 1 source and min_agreeing=2 → break (no quorum)
    payload = json.loads(recon_events[0]["payload"])
    assert payload["breaks"] == 1  # AAPL CLOSE has no quorum

    await bb.stop()
    store.close()


async def test_reconciliation_agent_writes_decision_record(tmp_path: Path) -> None:
    """ReconciliationAgent persists one DecisionRecord per reconciliation batch."""
    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)

    yf_rows = [_make_silver_row("AAPL", "CLOSE", 182.00, _SOURCE_YF)]
    st_rows = [_make_silver_row("AAPL", "CLOSE", 182.02, _SOURCE_ST)]
    store.write_silver(pd.DataFrame(yf_rows), "dec-run", _BDATE, _SOURCE_YF)
    store.write_silver(pd.DataFrame(st_rows), "dec-run", _BDATE, _SOURCE_ST)

    bb = Blackboard(db_path=":memory:")
    bb.register(ReconciliationAgent(bb, store, cfg))
    await bb.start()
    bdate = _BDATE.isoformat()
    for src in [_SOURCE_YF, _SOURCE_ST]:
        await bb.publish(
            Event(
                topic=TopicType.DQ_PASSED,
                agent="dq",
                run_id="dec-run",
                payload={"source_id": src, "business_date": bdate},
            )
        )
    await bb.drain()

    decisions = store.query(
        "SELECT * FROM decisions WHERE agent = 'reconciliation' AND decision_type = 'RECONCILE'"
    )
    assert len(decisions) == 1

    await bb.stop()
    store.close()


def test_reconciliation_agent_name_and_subscriptions(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)
    bb = Blackboard(db_path=":memory:")
    agent = ReconciliationAgent(bb, store, cfg)
    assert agent.name == "reconciliation"
    assert TopicType.DQ_PASSED in agent.subscriptions
    assert TopicType.DQ_FAILURE in agent.subscriptions
    assert TopicType.INGESTION_FAILED in agent.subscriptions
    store.close()
