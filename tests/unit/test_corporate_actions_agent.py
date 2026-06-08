"""Unit tests for CorporateActionsAgent — pure functions + agent behavior (FR-A4)."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd
import yaml

from harness.inject import build_silver_history
from mdq.agents.corporate_actions_agent import (
    CorporateActionsAgent,
    _back_adjust,
    _detect_cross_source_splits,
    _detect_single_source_splits,
)
from mdq.core.blackboard import Blackboard
from mdq.core.config import Config, CorporateActionsConfig
from mdq.core.events import Event, TopicType
from mdq.core.store import MedallionStore

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_BDATE = date(2024, 1, 10)
_INSTRUMENTS = ["AAPL", "MSFT", "NVDA"]

# ---------------------------------------------------------------------------
# Fixtures
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
    base["runtime"]["use_fixtures"] = True
    base["runtime"]["storage"]["bronze"] = str(tmp_path / "bronze")
    base["runtime"]["storage"]["silver"] = str(tmp_path / "silver")
    base["runtime"]["storage"]["gold"] = str(tmp_path / "gold")
    base["runtime"]["storage"]["quarantine"] = str(tmp_path / "quarantine")
    base["runtime"]["storage"]["lineage"] = str(tmp_path / "lineage")
    base["runtime"]["duckdb_path"] = str(tmp_path / "mdq.duckdb")
    universe = yaml.safe_load(Path("config/universe.yaml").read_text())
    base["universe"] = universe
    return Config.model_validate(base)


def _ca_cfg(
    split_ratio_tolerance: float = 0.05,
    candidate_split_ratios: list[float] | None = None,
    require_cross_source_corroboration: bool = False,
    ca_window_days: int = 30,
) -> CorporateActionsConfig:
    return CorporateActionsConfig(
        split_ratio_tolerance=split_ratio_tolerance,
        candidate_split_ratios=candidate_split_ratios or [2.0, 3.0, 1.5, 4.0],
        require_cross_source_corroboration=require_cross_source_corroboration,
        ca_window_days=ca_window_days,
    )


def _make_window(
    instrument_ids: list[str],
    business_date: date,
    curr_close: float,
    prev_close: float,
    source_id: str = "yfinance",
    n_hist_days: int = 3,
) -> pd.DataFrame:
    """Build a minimal Silver window DataFrame for unit tests."""
    rows: list[dict] = []
    fetch_ts = pd.Timestamp(datetime(2024, 1, 10, 21, 0, 0, tzinfo=UTC))

    def _price_row(inst: str, bdate: date, field: str, value: float) -> dict:
        return {
            "instrument_id": inst,
            "business_date": pd.Timestamp(bdate),
            "field": field,
            "value": value,
            "currency": "USD",
            "source_id": source_id,
            "source_symbol": inst,
            "fetch_ts": fetch_ts,
            "event_ts": fetch_ts,
            "content_hash": f"{inst}-{bdate}-{field}",
            "ca_adjusted": False,
        }

    for inst in instrument_ids:
        # Historical days with prev_close
        for offset in range(n_hist_days, 0, -1):
            d = business_date - timedelta(days=offset)
            for field in ("OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE"):
                rows.append(_price_row(inst, d, field, prev_close))
            rows.append(_price_row(inst, d, "VOLUME", 1_000_000.0))

        # Current day with curr_close
        for field in ("OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE"):
            rows.append(_price_row(inst, business_date, field, curr_close))
        rows.append(_price_row(inst, business_date, "VOLUME", 2_000_000.0))

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Pure-function tests: _detect_single_source_splits
# ---------------------------------------------------------------------------


def test_detect_single_source_2to1_split() -> None:
    """prev_close=200, curr_close=100 → ratio=2.0 → split detected."""
    window = _make_window(["AAPL"], _BDATE, curr_close=100.0, prev_close=200.0)
    actions = _detect_single_source_splits(window, _BDATE, _ca_cfg())
    assert len(actions) == 1
    assert actions[0]["instrument_id"] == "AAPL"
    assert actions[0]["ratio"] == 2.0
    assert actions[0]["action_type"] == "SPLIT"


def test_detect_single_source_3to1_split() -> None:
    """prev_close=300, curr_close=100 → ratio=3.0 → split detected."""
    window = _make_window(["AAPL"], _BDATE, curr_close=100.0, prev_close=300.0)
    actions = _detect_single_source_splits(window, _BDATE, _ca_cfg())
    assert len(actions) == 1
    assert actions[0]["ratio"] == 3.0


def test_detect_single_source_no_split() -> None:
    """Normal 2% price move → no detection (well within tolerance)."""
    window = _make_window(["AAPL"], _BDATE, curr_close=100.0, prev_close=102.0)
    actions = _detect_single_source_splits(window, _BDATE, _ca_cfg())
    assert actions == []


def test_detect_single_source_insufficient_history() -> None:
    """Only current day in window → no history to compare → graceful noop."""
    fetch_ts = pd.Timestamp(datetime(2024, 1, 10, 21, 0, 0, tzinfo=UTC))
    df = pd.DataFrame(
        [
            {
                "instrument_id": "AAPL",
                "business_date": pd.Timestamp(_BDATE),
                "field": "CLOSE",
                "value": 100.0,
                "currency": "USD",
                "source_id": "yfinance",
                "source_symbol": "AAPL",
                "fetch_ts": fetch_ts,
                "event_ts": fetch_ts,
                "content_hash": "x",
                "ca_adjusted": False,
            }
        ]
    )
    actions = _detect_single_source_splits(df, _BDATE, _ca_cfg())
    assert actions == []


def test_detect_single_source_multiple_instruments() -> None:
    """Each instrument with a 2:1 split is independently detected."""
    window = _make_window(["AAPL", "MSFT", "NVDA"], _BDATE, curr_close=100.0, prev_close=200.0)
    actions = _detect_single_source_splits(window, _BDATE, _ca_cfg())
    detected = {a["instrument_id"] for a in actions}
    assert detected == {"AAPL", "MSFT", "NVDA"}


# ---------------------------------------------------------------------------
# Pure-function tests: _detect_cross_source_splits
# ---------------------------------------------------------------------------


def _make_two_source_silvers(
    instrument_ids: list[str],
    business_date: date,
    src_a_close: float,
    src_b_close: float,
    src_a_prev_close: float = 100.0,
    src_b_prev_close: float = 100.0,
    src_a: str = "yfinance",
    src_b: str = "stooq",
) -> dict[str, pd.DataFrame]:
    """Build a two-source Silver dict for cross-source detection tests."""
    return {
        src_a: _make_window(
            instrument_ids,
            business_date,
            curr_close=src_a_close,
            prev_close=src_a_prev_close,
            source_id=src_a,
        ),
        src_b: _make_window(
            instrument_ids,
            business_date,
            curr_close=src_b_close,
            prev_close=src_b_prev_close,
            source_id=src_b,
        ),
    }


def test_detect_cross_source_unadjusted_stooq() -> None:
    """yfinance=100 (adjusted), stooq=200 (unadjusted) → stooq flagged."""
    silvers = _make_two_source_silvers(
        ["AAPL"],
        _BDATE,
        src_a_close=100.0,  # yfinance: adjusted
        src_b_close=200.0,  # stooq: unadjusted
        src_a_prev_close=100.0,  # yfinance history: smooth (no jump)
        src_b_prev_close=200.0,  # stooq history: smooth (no jump — prev=curr=200)
    )
    actions = _detect_cross_source_splits(silvers, _BDATE, _ca_cfg())
    assert len(actions) == 1
    # The source with higher current price is unadjusted
    assert actions[0]["source_id"] == "stooq"
    assert actions[0]["ratio"] == 2.0


def test_detect_cross_source_unadjusted_yfinance() -> None:
    """yfinance=200 (unadjusted), stooq=100 (adjusted) → yfinance flagged."""
    silvers = _make_two_source_silvers(
        ["AAPL"],
        _BDATE,
        src_a_close=200.0,  # yfinance: unadjusted
        src_b_close=100.0,  # stooq: adjusted
        src_a_prev_close=200.0,
        src_b_prev_close=100.0,
    )
    actions = _detect_cross_source_splits(silvers, _BDATE, _ca_cfg())
    assert len(actions) == 1
    assert actions[0]["source_id"] == "yfinance"
    assert actions[0]["ratio"] == 2.0


def test_detect_cross_source_both_agree() -> None:
    """Both sources at same price → no cross-source split detected."""
    silvers = _make_two_source_silvers(["AAPL"], _BDATE, src_a_close=100.0, src_b_close=100.0)
    actions = _detect_cross_source_splits(silvers, _BDATE, _ca_cfg())
    assert actions == []


def test_detect_cross_source_small_diff_not_split() -> None:
    """Sources differ by 5% (not near any split ratio) → no action."""
    silvers = _make_two_source_silvers(["AAPL"], _BDATE, src_a_close=100.0, src_b_close=105.0)
    actions = _detect_cross_source_splits(silvers, _BDATE, _ca_cfg())
    assert actions == []


# ---------------------------------------------------------------------------
# Pure-function tests: _back_adjust
# ---------------------------------------------------------------------------


def test_back_adjust_price_fields(tmp_path: Path) -> None:
    """Historical CLOSE halved, VOLUME doubled, ca_adjusted=True after 2:1 back-adjust."""
    store = _make_store(tmp_path)
    run_id = "ba-run"
    source_id = "yfinance"

    # Write 3 days of historical Silver
    hist_dates = [_BDATE - timedelta(days=i) for i in range(3, 0, -1)]
    for d in hist_dates:
        hist = build_silver_history(
            ["AAPL"], n_days=1, end_date=d, base_price=200.0, source_id=source_id
        )
        store.write_silver(hist, run_id, d, source_id)

    # Verify original CLOSE ≈ 200 before adjustment
    pre_df = pd.read_parquet(store.silver_path(run_id, hist_dates[0], source_id))
    close_mask = (pre_df["instrument_id"] == "AAPL") & (pre_df["field"] == "CLOSE")
    orig_close = float(pre_df[close_mask]["value"].iloc[0])
    assert orig_close > 0

    count = _back_adjust(
        source_id=source_id,
        instrument_id="AAPL",
        ratio=2.0,
        business_date=_BDATE,
        run_id=run_id,
        store=store,
        ca_window_days=30,
    )

    assert count > 0

    # Verify adjusted CLOSE is halved
    post_df = pd.read_parquet(store.silver_path(run_id, hist_dates[0], source_id))
    adj_close_mask = (post_df["instrument_id"] == "AAPL") & (post_df["field"] == "CLOSE")
    adj_close = float(post_df[adj_close_mask]["value"].iloc[0])
    assert abs(adj_close - orig_close / 2.0) < 0.001

    # Verify volume is doubled
    orig_vol_row = pre_df[(pre_df["instrument_id"] == "AAPL") & (pre_df["field"] == "VOLUME")]
    adj_vol_row = post_df[(post_df["instrument_id"] == "AAPL") & (post_df["field"] == "VOLUME")]
    if not orig_vol_row.empty and not adj_vol_row.empty:
        orig_vol = float(orig_vol_row["value"].iloc[0])
        adj_vol = float(adj_vol_row["value"].iloc[0])
        assert abs(adj_vol - orig_vol * 2.0) < 1.0

    # Verify ca_adjusted flag set
    aapl_rows = post_df[post_df["instrument_id"] == "AAPL"]
    assert aapl_rows["ca_adjusted"].all()

    store.close()


def test_back_adjust_skips_current_date(tmp_path: Path) -> None:
    """Current-day Silver file is NOT modified by back-adjustment."""
    store = _make_store(tmp_path)
    run_id = "ba-skip-run"
    source_id = "yfinance"

    # Write current-day Silver
    curr_df = build_silver_history(
        ["AAPL"], n_days=1, end_date=_BDATE, base_price=100.0, source_id=source_id
    )
    store.write_silver(curr_df, run_id, _BDATE, source_id)

    orig_curr_df = pd.read_parquet(store.silver_path(run_id, _BDATE, source_id))
    curr_close_mask = (orig_curr_df["instrument_id"] == "AAPL") & (orig_curr_df["field"] == "CLOSE")
    orig_close = float(orig_curr_df[curr_close_mask]["value"].iloc[0])

    # Also write one historical day
    hist_date = _BDATE - timedelta(days=1)
    hist_df = build_silver_history(
        ["AAPL"], n_days=1, end_date=hist_date, base_price=200.0, source_id=source_id
    )
    store.write_silver(hist_df, run_id, hist_date, source_id)

    _back_adjust(
        source_id=source_id,
        instrument_id="AAPL",
        ratio=2.0,
        business_date=_BDATE,
        run_id=run_id,
        store=store,
        ca_window_days=30,
    )

    # Current day must be unchanged
    post_curr = pd.read_parquet(store.silver_path(run_id, _BDATE, source_id))
    post_close_mask = (post_curr["instrument_id"] == "AAPL") & (post_curr["field"] == "CLOSE")
    post_close = float(post_curr[post_close_mask]["value"].iloc[0])
    assert abs(post_close - orig_close) < 0.001

    store.close()


def test_back_adjust_returns_row_count(tmp_path: Path) -> None:
    """back_adjust returns the number of rows adjusted (> 0)."""
    store = _make_store(tmp_path)
    run_id = "ba-count-run"
    source_id = "yfinance"

    # 2 historical days, 1 instrument → 6 fields each → 12 rows
    for offset in range(2, 0, -1):
        d = _BDATE - timedelta(days=offset)
        hist = build_silver_history(
            ["AAPL"], n_days=1, end_date=d, base_price=200.0, source_id=source_id
        )
        store.write_silver(hist, run_id, d, source_id)

    count = _back_adjust(
        source_id=source_id,
        instrument_id="AAPL",
        ratio=2.0,
        business_date=_BDATE,
        run_id=run_id,
        store=store,
        ca_window_days=30,
    )
    assert count == 12  # 6 fields × 2 days

    store.close()


def test_back_adjust_no_history_returns_zero(tmp_path: Path) -> None:
    """No historical Silver → _back_adjust returns 0 (graceful noop)."""
    store = _make_store(tmp_path)
    count = _back_adjust(
        source_id="yfinance",
        instrument_id="AAPL",
        ratio=2.0,
        business_date=_BDATE,
        run_id="empty-run",
        store=store,
        ca_window_days=30,
    )
    assert count == 0
    store.close()


# ---------------------------------------------------------------------------
# Agent behavior tests
# ---------------------------------------------------------------------------


def _dq_passed_event(
    source_id: str,
    run_id: str = "test-run",
    business_date: date = _BDATE,
) -> Event:
    return Event(
        topic=TopicType.DQ_PASSED,
        agent="dq_agent",
        run_id=run_id,
        payload={"source_id": source_id, "business_date": business_date.isoformat()},
    )


def _ingestion_failed_event(
    source_id: str,
    run_id: str = "test-run",
    business_date: date = _BDATE,
) -> Event:
    return Event(
        topic=TopicType.INGESTION_FAILED,
        agent="yfinance_source",
        run_id=run_id,
        payload={
            "source_id": source_id,
            "business_date": business_date.isoformat(),
            "error": "network error",
        },
    )


def test_agent_name_and_subscriptions(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)
    bb = Blackboard(db_path=":memory:")
    agent = CorporateActionsAgent(bb, store, cfg)
    assert agent.name == "corporate_actions"
    subs = agent.subscriptions
    assert TopicType.DQ_PASSED in subs
    assert TopicType.DQ_FAILURE in subs
    assert TopicType.INGESTION_FAILED in subs
    store.close()


async def test_agent_accumulates_before_firing(tmp_path: Path) -> None:
    """DQ_PASSED(yfinance) only → no CA detection; DQ_PASSED(stooq) → detection fires."""
    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)
    bb = Blackboard(db_path=":memory:")
    bb.register(CorporateActionsAgent(bb, store, cfg))
    await bb.start()

    # Publish only one source — should NOT fire
    await bb.publish(_dq_passed_event("yfinance"))
    await bb.drain()
    assert bb.get_events(topic=TopicType.CORPORATE_ACTION_DETECTED) == []

    # Publish second source — now fires (no Silver history → graceful noop, no CA event)
    await bb.publish(_dq_passed_event("stooq"))
    await bb.drain()
    # No CA event expected (no Silver history to compare)
    assert bb.get_events(topic=TopicType.CORPORATE_ACTION_DETECTED) == []

    await bb.stop()
    store.close()


async def test_agent_handles_ingestion_failed(tmp_path: Path) -> None:
    """INGESTION_FAILED(stooq) + DQ_PASSED(yfinance) → agent fires (graceful degradation)."""
    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)
    bb = Blackboard(db_path=":memory:")
    bb.register(CorporateActionsAgent(bb, store, cfg))
    await bb.start()

    await bb.publish(_ingestion_failed_event("stooq"))
    await bb.publish(_dq_passed_event("yfinance"))
    await bb.drain()

    # Agent should have fired (no Silver → noop, but DecisionRecord written)
    decisions = store.query("SELECT * FROM decisions WHERE agent = 'corporate_actions'")
    assert len(decisions) == 1

    await bb.stop()
    store.close()


async def test_agent_noop_if_no_history(tmp_path: Path) -> None:
    """No Silver history → no CORPORATE_ACTION_DETECTED, but DecisionRecord persisted."""
    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)
    bb = Blackboard(db_path=":memory:")
    bb.register(CorporateActionsAgent(bb, store, cfg))
    await bb.start()

    await bb.publish(_dq_passed_event("yfinance"))
    await bb.publish(_dq_passed_event("stooq"))
    await bb.drain()

    assert bb.get_events(topic=TopicType.CORPORATE_ACTION_DETECTED) == []
    decisions = store.query("SELECT * FROM decisions WHERE agent = 'corporate_actions'")
    assert len(decisions) == 1

    await bb.stop()
    store.close()


async def test_agent_publishes_corporate_action_detected(tmp_path: Path) -> None:
    """Seeded 2:1 split in Silver → CORPORATE_ACTION_DETECTED published."""
    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)
    run_id = "ca-detect-run"

    # Write 3 days of history at base_price=200 for both sources
    for src in ("yfinance", "stooq"):
        for offset in range(3, 0, -1):
            d = _BDATE - timedelta(days=offset)
            hist = build_silver_history(
                ["AAPL"], n_days=1, end_date=d, base_price=200.0, source_id=src
            )
            store.write_silver(hist, run_id, d, src)
        # Write current day at 100 (post-split price) → prev/curr = 2.0
        curr = build_silver_history(
            ["AAPL"], n_days=1, end_date=_BDATE, base_price=100.0, source_id=src
        )
        store.write_silver(curr, run_id, _BDATE, src)

    bb = Blackboard(db_path=":memory:")
    bb.register(CorporateActionsAgent(bb, store, cfg))
    await bb.start()

    await bb.publish(
        Event(
            topic=TopicType.DQ_PASSED,
            agent="dq_agent",
            run_id=run_id,
            payload={"source_id": "yfinance", "business_date": _BDATE.isoformat()},
        )
    )
    await bb.publish(
        Event(
            topic=TopicType.DQ_PASSED,
            agent="dq_agent",
            run_id=run_id,
            payload={"source_id": "stooq", "business_date": _BDATE.isoformat()},
        )
    )
    await bb.drain()

    ca_events = bb.get_events(topic=TopicType.CORPORATE_ACTION_DETECTED)
    assert len(ca_events) > 0, "Expected CORPORATE_ACTION_DETECTED for 2:1 split"
    payload = json.loads(ca_events[0]["payload"])
    assert payload["action_type"] == "SPLIT"
    assert payload["ratio"] == 2.0
    assert payload["instrument_id"] == "AAPL"

    await bb.stop()
    store.close()


async def test_agent_writes_decision_record(tmp_path: Path) -> None:
    """DecisionRecord with CORP_ACTION is persisted after detection run."""
    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)
    run_id = "ca-decision-run"

    # Write minimal Silver history
    for src in ("yfinance", "stooq"):
        hist = build_silver_history(
            ["AAPL"], n_days=2, end_date=_BDATE - timedelta(days=1), base_price=200.0, source_id=src
        )
        store.write_silver(hist, run_id, _BDATE - timedelta(days=1), src)
        curr = build_silver_history(
            ["AAPL"], n_days=1, end_date=_BDATE, base_price=100.0, source_id=src
        )
        store.write_silver(curr, run_id, _BDATE, src)

    bb = Blackboard(db_path=":memory:")
    bb.register(CorporateActionsAgent(bb, store, cfg))
    await bb.start()

    for src in ("yfinance", "stooq"):
        await bb.publish(
            Event(
                topic=TopicType.DQ_PASSED,
                agent="dq_agent",
                run_id=run_id,
                payload={"source_id": src, "business_date": _BDATE.isoformat()},
            )
        )
    await bb.drain()

    decisions = store.query(
        "SELECT * FROM decisions WHERE agent = 'corporate_actions'"
        " AND decision_type = 'CORP_ACTION'"
    )
    assert len(decisions) == 1

    await bb.stop()
    store.close()
