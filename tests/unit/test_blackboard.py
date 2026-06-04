"""Unit tests for the Blackboard: routing, persistence, drain (FR-B1–FR-B4)."""

from __future__ import annotations

import pytest

from mdq.core.agent import Agent
from mdq.core.blackboard import Blackboard
from mdq.core.events import Event, TopicType


class _MockAgent(Agent):
    """Minimal agent that records every event it receives."""

    def __init__(self, name: str, subs: list[TopicType]) -> None:
        self._name = name
        self._subs = subs
        self.received: list[Event] = []

    @property
    def name(self) -> str:
        return self._name

    @property
    def subscriptions(self) -> list[TopicType]:
        return self._subs

    async def act(self, event: Event) -> None:
        self.received.append(event)


@pytest.fixture
def db_path(tmp_path: object) -> str:
    import pathlib

    return str(pathlib.Path(str(tmp_path)) / "test_events.duckdb")


async def test_publish_routes_to_subscribed_agent(db_path: str) -> None:
    agent = _MockAgent("watcher", [TopicType.RUN_STARTED])
    bb = Blackboard(db_path=db_path)
    bb.register(agent)
    await bb.start()
    try:
        event = Event(topic=TopicType.RUN_STARTED, agent="supervisor", run_id="r1")
        await bb.publish(event)
        await bb.drain()
        assert len(agent.received) == 1
        assert agent.received[0].event_id == event.event_id
    finally:
        await bb.stop()


async def test_unsubscribed_agent_does_not_receive(db_path: str) -> None:
    agent = _MockAgent("ingestion_watcher", [TopicType.INGESTION_COMPLETE])
    bb = Blackboard(db_path=db_path)
    bb.register(agent)
    await bb.start()
    try:
        await bb.publish(Event(topic=TopicType.RUN_STARTED, agent="supervisor", run_id="r1"))
        await bb.drain()
        assert len(agent.received) == 0
    finally:
        await bb.stop()


async def test_event_persisted_to_duckdb(db_path: str) -> None:
    bb = Blackboard(db_path=db_path)
    await bb.start()
    try:
        event = Event(topic=TopicType.RUN_STARTED, agent="supervisor", run_id="r-persist")
        await bb.publish(event)
        events = bb.get_events(run_id="r-persist")
        assert len(events) == 1
        assert events[0]["event_id"] == event.event_id
        assert events[0]["topic"] == TopicType.RUN_STARTED.value
    finally:
        await bb.stop()


async def test_get_events_by_topic_filter(db_path: str) -> None:
    bb = Blackboard(db_path=db_path)
    await bb.start()
    try:
        await bb.publish(Event(topic=TopicType.RUN_STARTED, agent="supervisor", run_id="r1"))
        await bb.publish(Event(topic=TopicType.RUN_COMPLETE, agent="supervisor", run_id="r1"))
        started = bb.get_events(topic=TopicType.RUN_STARTED)
        assert len(started) == 1
        assert started[0]["topic"] == TopicType.RUN_STARTED.value
    finally:
        await bb.stop()


async def test_publish_before_open_raises(db_path: str) -> None:
    bb = Blackboard(db_path=db_path)
    with pytest.raises(RuntimeError, match="not open"):
        await bb.publish(Event(topic=TopicType.RUN_STARTED, agent="supervisor", run_id="r1"))


async def test_multiple_agents_routed_independently(db_path: str) -> None:
    a1 = _MockAgent("a1", [TopicType.RUN_STARTED])
    a2 = _MockAgent("a2", [TopicType.RUN_COMPLETE])
    bb = Blackboard(db_path=db_path)
    bb.register(a1)
    bb.register(a2)
    await bb.start()
    try:
        await bb.publish(Event(topic=TopicType.RUN_STARTED, agent="supervisor", run_id="r1"))
        await bb.publish(Event(topic=TopicType.RUN_COMPLETE, agent="supervisor", run_id="r1"))
        await bb.drain()
        assert len(a1.received) == 1
        assert len(a2.received) == 1
        assert a1.received[0].topic == TopicType.RUN_STARTED
        assert a2.received[0].topic == TopicType.RUN_COMPLETE
    finally:
        await bb.stop()
