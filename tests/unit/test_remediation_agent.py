"""Unit tests for RemediationAgent (FR-A7).

Pure-function tests for _identify_bad_source, and agent-behaviour tests with a mock
blackboard and store. All tests are I/O-free and C-5-deterministic.
"""

from __future__ import annotations

import asyncio
from datetime import date
from typing import Any
from unittest.mock import MagicMock

from mdq.agents.remediation_agent import RemediationAgent, _identify_bad_source
from mdq.core.events import Event, TopicType
from mdq.core.schemas import DecisionType

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BDATE = date(2024, 1, 10)
_ENABLED = frozenset({"yfinance", "stooq"})


def _make_event(
    topic: TopicType,
    run_id: str = "test-run",
    payload: dict[str, Any] | None = None,
) -> Event:
    return Event(
        topic=topic,
        agent="test",
        run_id=run_id,
        payload=payload or {},
    )


def _break_event(
    run_id: str = "test-run",
    dissenting: list[str] | None = None,
) -> Event:
    return _make_event(
        TopicType.BREAK_DETECTED,
        run_id=run_id,
        payload={
            "instrument_id": "AAPL",
            "field": "CLOSE",
            "business_date": _BDATE.isoformat(),
            "source_values": {"yfinance": 100.0, "stooq": 120.0},
            "tolerance_band": "CLOSE:25bps",
            "dissenting_sources": dissenting or ["stooq"],
        },
    )


def _recon_complete_event(run_id: str = "test-run", breaks: int = 1) -> Event:
    return _make_event(
        TopicType.RECONCILIATION_COMPLETE,
        run_id=run_id,
        payload={
            "source_id": "batch",
            "business_date": _BDATE.isoformat(),
            "golden_records": 5,
            "breaks": breaks,
        },
    )


def _make_cfg(max_retries: int = 3) -> MagicMock:
    cfg = MagicMock()
    cfg.sources.yfinance.enabled = True
    cfg.sources.stooq.enabled = True
    cfg.remediation.max_retries = max_retries
    cfg.remediation.lookback_widen_steps = [5, 20, 60]
    return cfg


class _FakeBB:
    """Lightweight blackboard stub that captures published events."""

    def __init__(self) -> None:
        self.published: list[Event] = []

    async def publish(self, event: Event) -> None:
        self.published.append(event)

    def topics_published(self) -> list[TopicType]:
        return [e.topic for e in self.published]

    def payloads_for(self, topic: TopicType) -> list[dict[str, Any]]:
        return [e.payload for e in self.published if e.topic == topic]


def _make_agent(max_retries: int = 3) -> tuple[RemediationAgent, _FakeBB, MagicMock]:
    bb = _FakeBB()
    store = MagicMock()
    cfg = _make_cfg(max_retries=max_retries)
    agent = RemediationAgent(bb, store, cfg)  # type: ignore[arg-type]
    return agent, bb, store


# ---------------------------------------------------------------------------
# Pure function tests — _identify_bad_source
# ---------------------------------------------------------------------------


def test_identify_bad_source_alphabetical() -> None:
    """First attempt picks first source alphabetically from dissenting list."""
    breaks = [{"dissenting_sources": ["stooq", "yfinance"]}]
    result = _identify_bad_source(breaks, attempt=1, enabled_sources=_ENABLED)
    assert result == "stooq"


def test_identify_bad_source_cycles_on_retry() -> None:
    """Retry 2 picks the second sorted source."""
    breaks = [{"dissenting_sources": ["stooq", "yfinance"]}]
    first = _identify_bad_source(breaks, attempt=1, enabled_sources=_ENABLED)
    second = _identify_bad_source(breaks, attempt=2, enabled_sources=_ENABLED)
    assert first == "stooq"
    assert second == "yfinance"


def test_identify_bad_source_wraps_around() -> None:
    """Attempt 3 wraps around to first source when there are only 2."""
    breaks = [{"dissenting_sources": ["stooq", "yfinance"]}]
    third = _identify_bad_source(breaks, attempt=3, enabled_sources=_ENABLED)
    assert third == "stooq"


def test_identify_bad_source_fallback_to_enabled() -> None:
    """When dissenting_sources is empty, falls back to sorted enabled_sources."""
    breaks: list[dict[str, Any]] = [{"dissenting_sources": []}]
    result = _identify_bad_source(breaks, attempt=1, enabled_sources=_ENABLED)
    assert result in _ENABLED


def test_identify_bad_source_prefers_absent_over_dissenting() -> None:
    """Absent source (all-NaN, not in source_values) is picked over dissenting source."""
    # stooq absent from source_values (NULL_BURST scenario)
    breaks = [
        {
            "source_values": {"yfinance": 100.0},  # stooq absent
            "dissenting_sources": ["yfinance"],  # yfinance listed as dissenting (can't form quorum)
        }
    ]
    result = _identify_bad_source(breaks, attempt=1, enabled_sources=_ENABLED)
    assert result == "stooq"  # stooq is absent → primary remediation target


def test_identify_bad_source_single_dissenting() -> None:
    """Single dissenting source is always picked regardless of attempt."""
    # source_values has both sources → absent is empty → only stooq is dissenting
    breaks = [
        {"dissenting_sources": ["stooq"], "source_values": {"yfinance": 100.0, "stooq": 120.0}}
    ]
    assert _identify_bad_source(breaks, 1, _ENABLED) == "stooq"
    assert _identify_bad_source(breaks, 2, _ENABLED) == "stooq"


# ---------------------------------------------------------------------------
# Agent behaviour tests
# ---------------------------------------------------------------------------


def test_name_and_subscriptions() -> None:
    agent, _, _ = _make_agent()
    assert agent.name == "remediation"
    assert TopicType.BREAK_DETECTED in agent.subscriptions
    assert TopicType.RECONCILIATION_COMPLETE in agent.subscriptions
    assert TopicType.INGESTION_FAILED in agent.subscriptions


def test_accumulates_breaks_before_acting() -> None:
    """BREAK_DETECTED alone does not trigger any published events."""
    agent, bb, _ = _make_agent()
    asyncio.run(agent.act(_break_event()))
    assert bb.published == []


def test_starts_remediation_on_reconciliation_complete_with_breaks() -> None:
    """RECONCILIATION_COMPLETE(breaks>0) after BREAK_DETECTED triggers remediation."""
    agent, bb, _ = _make_agent()
    asyncio.run(agent.act(_break_event()))
    asyncio.run(agent.act(_recon_complete_event(breaks=1)))
    assert TopicType.REFETCH_REQUESTED in bb.topics_published()


def test_publishes_refetch_requested_for_bad_source() -> None:
    """REFETCH_REQUESTED targets stooq (first alphabetically in dissenting list)."""
    agent, bb, _ = _make_agent()
    asyncio.run(agent.act(_break_event(dissenting=["stooq"])))
    asyncio.run(agent.act(_recon_complete_event(breaks=1)))
    payloads = bb.payloads_for(TopicType.REFETCH_REQUESTED)
    assert len(payloads) == 1
    assert payloads[0]["source_id"] == "stooq"


def test_publishes_synthetic_dq_passed_for_good_source() -> None:
    """RemediationAgent re-publishes DQ_PASSED for yfinance (the good source)."""
    agent, bb, _ = _make_agent()
    asyncio.run(agent.act(_break_event(dissenting=["stooq"])))
    asyncio.run(agent.act(_recon_complete_event(breaks=1)))
    dq_passed = bb.payloads_for(TopicType.DQ_PASSED)
    sources = [p["source_id"] for p in dq_passed]
    assert "yfinance" in sources
    assert "stooq" not in sources


def test_lookback_widening_on_retry() -> None:
    """First attempt uses lookback_widen_steps[0]=5."""
    agent, bb, _ = _make_agent(max_retries=3)
    asyncio.run(agent.act(_break_event()))
    asyncio.run(agent.act(_recon_complete_event(breaks=1)))
    refetch_payloads = bb.payloads_for(TopicType.REFETCH_REQUESTED)
    assert refetch_payloads[0]["lookback_days"] == 5


def test_escalates_after_max_retries() -> None:
    """After max_retries exhausted, ESCALATION and REMEDIATION_FAILED are published."""
    agent, bb, store = _make_agent(max_retries=1)
    asyncio.run(agent.act(_break_event()))
    # First RECONCILIATION_COMPLETE with breaks → attempt 1 (within limit)
    asyncio.run(agent.act(_recon_complete_event(breaks=1)))
    # Second RECONCILIATION_COMPLETE with breaks still → max_retries=1 exhausted → escalate
    asyncio.run(agent.act(_recon_complete_event(breaks=1)))
    assert TopicType.ESCALATION in bb.topics_published()
    assert TopicType.REMEDIATION_FAILED in bb.topics_published()


def test_publishes_remediation_complete_when_breaks_zero_while_active() -> None:
    """RECONCILIATION_COMPLETE(breaks=0) while active → REMEDIATION_COMPLETE."""
    agent, bb, store = _make_agent()
    asyncio.run(agent.act(_break_event()))
    asyncio.run(agent.act(_recon_complete_event(breaks=1)))
    # Re-fetch succeeds: reconciliation now passes
    asyncio.run(agent.act(_recon_complete_event(breaks=0)))
    assert TopicType.REMEDIATION_COMPLETE in bb.topics_published()
    assert TopicType.ESCALATION not in bb.topics_published()


def test_writes_decision_record_on_remediation_complete() -> None:
    """DecisionRecord(REMEDIATE, verified=True) is written on success."""
    agent, bb, store = _make_agent()
    asyncio.run(agent.act(_break_event()))
    asyncio.run(agent.act(_recon_complete_event(breaks=1)))
    asyncio.run(agent.act(_recon_complete_event(breaks=0)))
    store.write_decision.assert_called_once()
    record = store.write_decision.call_args[0][0]
    assert record.decision_type == DecisionType.REMEDIATE
    assert record.verified is True


def test_ingestion_failed_escalates_immediately() -> None:
    """INGESTION_FAILED → ESCALATION published immediately."""
    agent, bb, store = _make_agent()
    event = _make_event(
        TopicType.INGESTION_FAILED,
        payload={"source_id": "stooq", "business_date": _BDATE.isoformat()},
    )
    asyncio.run(agent.act(event))
    assert TopicType.ESCALATION in bb.topics_published()
    assert TopicType.REMEDIATION_FAILED in bb.topics_published()


def test_ignores_reconciliation_complete_when_not_active() -> None:
    """RECONCILIATION_COMPLETE with no prior BREAK_DETECTED is ignored."""
    agent, bb, _ = _make_agent()
    asyncio.run(agent.act(_recon_complete_event(breaks=1)))
    assert bb.published == []


def test_clears_state_after_remediation_complete() -> None:
    """State for run_id is cleared after REMEDIATION_COMPLETE so a new run starts fresh."""
    agent, bb, store = _make_agent()
    asyncio.run(agent.act(_break_event("run-1")))
    asyncio.run(agent.act(_recon_complete_event("run-1", breaks=1)))
    asyncio.run(agent.act(_recon_complete_event("run-1", breaks=0)))
    assert "run-1" not in agent._breaks
    assert "run-1" not in agent._active
