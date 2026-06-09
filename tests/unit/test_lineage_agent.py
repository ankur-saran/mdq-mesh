"""Unit tests for LineageAgent (FR-A8) and its pure-function helpers.

All tests are I/O-free where possible; file-writing tests use tmp_path.
Uses asyncio.run() for async act() calls (Python 3.10+ compatible).
"""

from __future__ import annotations

import asyncio
import json
from datetime import date
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pandas as pd

from mdq.agents.lineage_agent import (
    LineageAgent,
    _build_lineage,
    _build_scorecard,
    _write_html,
    _write_json,
)
from mdq.core.events import Event, TopicType
from mdq.core.schemas import OverallStatus, ScorecardRecord

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BDATE = date(2024, 1, 10)
_RUN_ID = "test-run-lineage"
_INSTRUMENTS = ["AAPL", "MSFT"]
_DECISION_ID_A = "aaaa-0000"
_DECISION_ID_B = "bbbb-0000"


def _make_gold_df(instruments: list[str] | None = None) -> pd.DataFrame:
    instrs = instruments or _INSTRUMENTS
    rows = []
    for i, instr in enumerate(instrs):
        rows.append(
            {
                "instrument_id": instr,
                "field": "CLOSE",
                "golden_value": str(100.0 + i),
                "confidence": "HIGH",
                "quorum_sources": json.dumps(["yfinance", "stooq"]),
                "dissenting_sources": json.dumps([]),
                "tolerance_band": "CLOSE:25bps",
                "currency": "USD",
                "business_date": _BDATE.isoformat(),
                "decision_id": _DECISION_ID_A if i == 0 else _DECISION_ID_B,
            }
        )
    return pd.DataFrame(rows)


def _make_decision(decision_id: str, decision_type: str = "RECONCILE") -> dict[str, Any]:
    return {
        "decision_id": decision_id,
        "ts": "2024-01-10T10:00:00+00:00",
        "agent": "reconciliation",
        "instrument_id": "AAPL",
        "business_date": _BDATE,
        "decision_type": decision_type,
        "inputs": json.dumps({"sources": ["yfinance", "stooq"], "values": {"yfinance": 100.0}}),
        "outcome": json.dumps({"golden_value": "100.0"}),
        "rule_applied": "QuorumVote",
        "verified": True,
    }


def _make_store(
    gold_df: pd.DataFrame | None = None,
    decisions: dict[str, dict] | None = None,
    counts_df: pd.DataFrame | None = None,
) -> MagicMock:
    store = MagicMock()
    store.read_gold.return_value = gold_df if gold_df is not None else _make_gold_df()
    # Explicit None check — an empty dict is valid (caller wants no matching decisions).
    if decisions is None:
        decisions = {
            _DECISION_ID_A: _make_decision(_DECISION_ID_A),
            _DECISION_ID_B: _make_decision(_DECISION_ID_B),
        }
    store.get_decision.side_effect = lambda did: decisions.get(did)
    if counts_df is not None:
        store.query.return_value = counts_df
    else:
        store.query.return_value = pd.DataFrame({"decision_type": [], "n": []})
    return store


def _make_cfg(lineage_dir: str = "data/lineage") -> MagicMock:
    cfg = MagicMock()
    cfg.runtime.storage.lineage = lineage_dir
    cfg.scorecard.output_formats = ["json", "html"]
    return cfg


def _make_agent(
    store: MagicMock | None = None,
    lineage_dir: str = "data/lineage",
) -> tuple[LineageAgent, MagicMock, MagicMock]:
    async def _async_publish(event: Event) -> None:
        pass

    bb = MagicMock()
    bb.publish = _async_publish
    s = store or _make_store()
    cfg = _make_cfg(lineage_dir)
    agent = LineageAgent(bb, s, cfg)  # type: ignore[arg-type]
    return agent, bb, s


def _run_complete_event(bdate: date = _BDATE) -> Event:
    return Event(
        topic=TopicType.RUN_COMPLETE,
        agent="supervisor",
        run_id=_RUN_ID,
        payload={"business_date": bdate.isoformat()},
    )


# ---------------------------------------------------------------------------
# Pure function tests — _build_lineage
# ---------------------------------------------------------------------------


def test_build_lineage_joins_gold_to_decision_inputs() -> None:
    gold_df = _make_gold_df(["AAPL"])
    store = _make_store(gold_df, {_DECISION_ID_A: _make_decision(_DECISION_ID_A)})
    records = _build_lineage(gold_df, store)
    assert len(records) == 1
    assert records[0]["instrument_id"] == "AAPL"
    assert records[0]["decision_id"] == _DECISION_ID_A
    assert "sources" in records[0]["inputs"]


def test_build_lineage_handles_missing_decision() -> None:
    """A Gold row whose decision_id is not found in decisions returns None fields."""
    gold_df = _make_gold_df(["AAPL"])
    store = _make_store(gold_df, {})  # no matching decision
    records = _build_lineage(gold_df, store)
    assert records[0]["decision_type"] is None
    assert records[0]["inputs"] == {}


def test_build_lineage_parses_quorum_sources() -> None:
    gold_df = _make_gold_df(["AAPL"])
    store = _make_store(gold_df)
    records = _build_lineage(gold_df, store)
    assert isinstance(records[0]["quorum_sources"], list)
    assert "yfinance" in records[0]["quorum_sources"]


# ---------------------------------------------------------------------------
# Pure function tests — _build_scorecard
# ---------------------------------------------------------------------------


def test_build_scorecard_green_when_no_decisions() -> None:
    gold_df = _make_gold_df()
    store = _make_store(gold_df, counts_df=pd.DataFrame({"decision_type": [], "n": []}))
    sc = _build_scorecard(_RUN_ID, _BDATE, gold_df, store)
    assert sc.overall_status == OverallStatus.GREEN
    assert sc.escalations == 0
    assert sc.records_ingested == len(_INSTRUMENTS)


def test_build_scorecard_amber_when_remediate() -> None:
    gold_df = _make_gold_df()
    counts = pd.DataFrame({"decision_type": ["REMEDIATE"], "n": [1]})
    store = _make_store(gold_df, counts_df=counts)
    sc = _build_scorecard(_RUN_ID, _BDATE, gold_df, store)
    assert sc.overall_status == OverallStatus.AMBER
    assert sc.remediations_succeeded == 1
    assert sc.escalations == 0


def test_build_scorecard_red_when_escalate() -> None:
    gold_df = _make_gold_df()
    counts = pd.DataFrame({"decision_type": ["ESCALATE", "REMEDIATE"], "n": [1, 2]})
    store = _make_store(gold_df, counts_df=counts)
    sc = _build_scorecard(_RUN_ID, _BDATE, gold_df, store)
    assert sc.overall_status == OverallStatus.RED
    assert sc.escalations == 1


# ---------------------------------------------------------------------------
# Pure function tests — _write_json / _write_html
# ---------------------------------------------------------------------------


def test_write_json_valid_and_contains_required_keys(tmp_path: Path) -> None:
    sc = ScorecardRecord(run_id=_RUN_ID, business_date=_BDATE, source_id="batch")
    lineage = [
        {
            "instrument_id": "AAPL",
            "field": "CLOSE",
            "golden_value": "100.0",
            "confidence": "HIGH",
            "quorum_sources": ["yfinance"],
            "dissenting_sources": [],
            "decision_id": "abc-123",
            "decision_type": "RECONCILE",
            "inputs": {},
            "outcome": {},
            "rule_applied": "QuorumVote",
            "verified": True,
        }
    ]
    dest = tmp_path / "out.json"
    _write_json(lineage, sc, dest)
    parsed = json.loads(dest.read_text())
    for key in ("run_id", "business_date", "overall_status", "summary", "lineage"):
        assert key in parsed
    assert parsed["lineage"][0]["instrument_id"] == "AAPL"


def test_write_html_has_no_external_assets(tmp_path: Path) -> None:
    sc = ScorecardRecord(run_id=_RUN_ID, business_date=_BDATE, source_id="batch")
    dest = tmp_path / "out.html"
    _write_html([], sc, dest)
    html = dest.read_text()
    assert "http://" not in html
    assert "https://" not in html
    assert "<style>" in html
    assert "<table" in html


def test_write_html_contains_instrument(tmp_path: Path) -> None:
    sc = ScorecardRecord(run_id=_RUN_ID, business_date=_BDATE, source_id="batch")
    lineage = [
        {
            "instrument_id": "TSLA",
            "field": "CLOSE",
            "golden_value": "200.0",
            "confidence": "HIGH",
            "quorum_sources": ["yfinance"],
            "dissenting_sources": [],
            "decision_id": "xyz-0001",
            "decision_type": None,
            "inputs": {},
            "outcome": {},
            "rule_applied": None,
            "verified": None,
        }
    ]
    dest = tmp_path / "out.html"
    _write_html(lineage, sc, dest)
    assert "TSLA" in dest.read_text()


# ---------------------------------------------------------------------------
# Agent behaviour tests
# ---------------------------------------------------------------------------


def test_name_and_subscriptions() -> None:
    agent, _, _ = _make_agent()
    assert agent.name == "lineage"
    assert TopicType.RUN_COMPLETE in agent.subscriptions


def test_ignores_run_complete_without_business_date() -> None:
    agent, _, store = _make_agent()
    event = Event(topic=TopicType.RUN_COMPLETE, agent="supervisor", run_id=_RUN_ID, payload={})
    asyncio.run(agent.act(event))
    store.read_gold.assert_not_called()


def test_ignores_run_complete_when_gold_empty() -> None:
    store = _make_store(gold_df=pd.DataFrame())
    agent, _, _ = _make_agent(store=store)
    asyncio.run(agent.act(_run_complete_event()))
    store.write_scorecard.assert_not_called()


def test_scorecard_written_to_store(tmp_path: Path) -> None:
    store = _make_store()
    agent, _, _ = _make_agent(store=store, lineage_dir=str(tmp_path / "lineage"))
    asyncio.run(agent.act(_run_complete_event()))
    store.write_scorecard.assert_called_once()
    record = store.write_scorecard.call_args[0][0]
    assert record.run_id == _RUN_ID
    assert record.business_date == _BDATE


def test_json_written_on_run_complete(tmp_path: Path) -> None:
    store = _make_store()
    agent, _, _ = _make_agent(store=store, lineage_dir=str(tmp_path / "lineage"))
    asyncio.run(agent.act(_run_complete_event()))
    json_file = tmp_path / "lineage" / _RUN_ID / f"{_BDATE}.json"
    assert json_file.exists()
    parsed = json.loads(json_file.read_text())
    assert parsed["run_id"] == _RUN_ID


def test_html_written_on_run_complete(tmp_path: Path) -> None:
    store = _make_store()
    agent, _, _ = _make_agent(store=store, lineage_dir=str(tmp_path / "lineage"))
    asyncio.run(agent.act(_run_complete_event()))
    html_file = tmp_path / "lineage" / _RUN_ID / f"{_BDATE}.html"
    assert html_file.exists()
    html = html_file.read_text()
    assert "http://" not in html
    assert "<table" in html


def test_decision_recorded_published(tmp_path: Path) -> None:
    published: list[Event] = []

    async def _capture(event: Event) -> None:
        published.append(event)

    store = _make_store()
    cfg = _make_cfg(str(tmp_path / "lineage"))
    bb = MagicMock()
    bb.publish = _capture
    agent = LineageAgent(bb, store, cfg)  # type: ignore[arg-type]
    asyncio.run(agent.act(_run_complete_event()))
    topics = [e.topic for e in published]
    assert TopicType.DECISION_RECORDED in topics
