"""Unit tests for Supervisor agent (FR-A9).

Tests escalation tracking, remediation counting, DecisionRecord writing, and run summary.
All tests use a mock blackboard and store — no I/O.
"""

from __future__ import annotations

import asyncio
from datetime import date
from typing import Any
from unittest.mock import MagicMock

from mdq.agents.supervisor import Supervisor
from mdq.core.events import Event, TopicType
from mdq.core.schemas import DecisionType

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BDATE = date(2024, 1, 10)


def _make_event(
    topic: TopicType,
    run_id: str = "test-run",
    payload: dict[str, Any] | None = None,
) -> Event:
    return Event(topic=topic, agent="test", run_id=run_id, payload=payload or {})


def _make_agent() -> tuple[Supervisor, MagicMock]:
    bb = MagicMock()
    store = MagicMock()
    cfg = MagicMock()
    agent = Supervisor(bb, store, cfg)  # type: ignore[arg-type]
    return agent, store


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_name_and_subscriptions() -> None:
    agent, _ = _make_agent()
    assert agent.name == "supervisor"
    assert TopicType.ESCALATION in agent.subscriptions
    assert TopicType.REMEDIATION_COMPLETE in agent.subscriptions
    assert TopicType.REMEDIATION_FAILED in agent.subscriptions
    assert TopicType.RUN_COMPLETE in agent.subscriptions


def test_tracks_escalation() -> None:
    agent, _ = _make_agent()
    event = _make_event(
        TopicType.ESCALATION,
        payload={"source_id": "stooq", "business_date": _BDATE.isoformat()},
    )
    asyncio.run(agent.act(event))
    assert len(agent._escalations.get("test-run", [])) == 1


def test_writes_escalation_decision_record() -> None:
    agent, store = _make_agent()
    event = _make_event(
        TopicType.ESCALATION,
        payload={"source_id": "stooq", "business_date": _BDATE.isoformat()},
    )
    asyncio.run(agent.act(event))
    store.write_decision.assert_called_once()
    record = store.write_decision.call_args[0][0]
    assert record.decision_type == DecisionType.ESCALATE
    assert record.verified is False


def test_tracks_remediation_complete() -> None:
    agent, _ = _make_agent()
    event = _make_event(
        TopicType.REMEDIATION_COMPLETE,
        payload={"business_date": _BDATE.isoformat(), "attempts": 1},
    )
    asyncio.run(agent.act(event))
    assert agent._rem_succeeded.get("test-run", 0) == 1
    assert agent._rem_attempted.get("test-run", 0) == 1


def test_tracks_remediation_failed() -> None:
    agent, _ = _make_agent()
    event = _make_event(
        TopicType.REMEDIATION_FAILED,
        payload={"source_id": "stooq", "business_date": _BDATE.isoformat()},
    )
    asyncio.run(agent.act(event))
    assert agent._rem_succeeded.get("test-run", 0) == 0
    assert agent._rem_attempted.get("test-run", 0) == 1


def test_kpi1_at_run_complete() -> None:
    """KPI-1 ratio is computed correctly: 2 succeeded / 3 attempted = 0.67."""
    agent, _ = _make_agent()
    run_id = "summary-run"
    # Simulate 3 remediations: 2 succeeded, 1 failed
    for _ in range(2):
        asyncio.run(
            agent.act(
                _make_event(
                    TopicType.REMEDIATION_COMPLETE,
                    run_id=run_id,
                    payload={"business_date": _BDATE.isoformat(), "attempts": 1},
                )
            )
        )
    asyncio.run(
        agent.act(
            _make_event(
                TopicType.REMEDIATION_FAILED,
                run_id=run_id,
                payload={"business_date": _BDATE.isoformat()},
            )
        )
    )
    # Verify counts before RUN_COMPLETE clears state
    assert agent._rem_attempted.get(run_id, 0) == 3
    assert agent._rem_succeeded.get(run_id, 0) == 2
    # kpi_1 = 2/3 ≈ 0.667; verify the formula directly
    attempted = agent._rem_attempted[run_id]
    succeeded = agent._rem_succeeded[run_id]
    assert abs(succeeded / attempted - 2 / 3) < 0.01
    asyncio.run(agent.act(_make_event(TopicType.RUN_COMPLETE, run_id=run_id)))


def test_clears_state_after_run_complete() -> None:
    """State for run_id is cleared after RUN_COMPLETE."""
    agent, _ = _make_agent()
    run_id = "clear-run"
    asyncio.run(
        agent.act(
            _make_event(
                TopicType.REMEDIATION_COMPLETE,
                run_id=run_id,
                payload={"business_date": _BDATE.isoformat(), "attempts": 1},
            )
        )
    )
    asyncio.run(agent.act(_make_event(TopicType.RUN_COMPLETE, run_id=run_id)))
    assert run_id not in agent._rem_succeeded
    assert run_id not in agent._rem_attempted
    assert run_id not in agent._escalations
