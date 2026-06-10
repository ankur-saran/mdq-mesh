"""Unit tests for the Transport Protocol and both transport implementations (FR-B3, Phase 8).

InProcessTransport — regression tests to confirm existing behaviour is unchanged.
RedpandaTransport  — Protocol conformance + unit tests with mocked aiokafka.
"""

from __future__ import annotations

import asyncio
import sys
import types
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from mdq.core.events import Event, TopicType
from mdq.core.transport.base import Transport
from mdq.core.transport.inprocess import InProcessTransport

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_agent(name: str = "test_agent") -> MagicMock:
    agent = MagicMock()
    agent.name = name
    agent.subscriptions = [TopicType.RUN_STARTED]
    agent.should_act.return_value = True
    agent.act = AsyncMock()
    return agent


def _make_event(topic: TopicType = TopicType.RUN_STARTED) -> Event:
    return Event(
        topic=topic,
        agent="test",
        run_id="test-run",
        payload={},
    )


def _make_mock_aiokafka_module(
    producer: Any | None = None,
    consumer: Any | None = None,
) -> types.ModuleType:
    """Return a lightweight fake aiokafka module for sys.modules injection.

    If *producer* or *consumer* are provided they are used as-is (no method override),
    allowing callers to configure specific side effects before passing them in.
    """
    if producer is None:
        producer = AsyncMock()
        producer.start = AsyncMock()
        producer.stop = AsyncMock()
        producer.send_and_wait = AsyncMock()

    if consumer is None:
        consumer = AsyncMock()
        consumer.start = AsyncMock()
        consumer.stop = AsyncMock()
        # Async iterator that immediately stops (no messages)
        consumer.__aiter__ = MagicMock(return_value=consumer)
        consumer.__anext__ = AsyncMock(side_effect=StopAsyncIteration)

    mod = types.ModuleType("aiokafka")
    mod.AIOKafkaProducer = MagicMock(return_value=producer)  # type: ignore[attr-defined]
    mod.AIOKafkaConsumer = MagicMock(return_value=consumer)  # type: ignore[attr-defined]

    return mod


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


def test_inprocess_satisfies_transport_protocol() -> None:
    """InProcessTransport is a structural instance of Transport (runtime_checkable)."""
    transport = InProcessTransport()
    assert isinstance(transport, Transport)


def test_redpanda_satisfies_transport_protocol() -> None:
    """RedpandaTransport is a structural instance of Transport without requiring Docker."""
    from mdq.core.transport.redpanda import RedpandaTransport

    transport = RedpandaTransport()
    assert isinstance(transport, Transport)


def test_transport_protocol_has_five_methods() -> None:
    """Any class with the 5 required methods satisfies the Transport Protocol."""

    # A minimal class with all 5 methods is a Transport; missing any one fails.
    class _Full:
        def register(self, agent: Any) -> None: ...

        async def deliver(self, event: Any, agent: Any) -> None: ...

        async def start(self, agents: Any) -> None: ...

        async def drain(self) -> None: ...

        async def stop(self) -> None: ...

    class _Missing:
        def register(self, agent: Any) -> None: ...

        async def deliver(self, event: Any, agent: Any) -> None: ...

        async def start(self, agents: Any) -> None: ...

        async def drain(self) -> None: ...

        # stop() missing

    assert isinstance(_Full(), Transport)
    assert not isinstance(_Missing(), Transport)


# ---------------------------------------------------------------------------
# InProcessTransport regression (ensure Phase 8 changes didn't break it)
# ---------------------------------------------------------------------------


async def test_inprocess_full_dispatch_roundtrip() -> None:
    """InProcessTransport correctly dispatches an event to a subscribed agent."""
    agent = _make_agent()
    transport = InProcessTransport()
    transport.register(agent)

    event = _make_event()
    await transport.start([agent])
    await transport.deliver(event, agent)
    await transport.drain()
    await transport.stop()

    agent.act.assert_called_once_with(event)


async def test_inprocess_drain_waits_for_all_events() -> None:
    """drain() returns only after all queued events have been processed."""
    processed: list[str] = []

    agent = _make_agent()
    agent.act = AsyncMock(side_effect=lambda e: processed.append(e.event_id))

    transport = InProcessTransport()
    transport.register(agent)
    await transport.start([agent])

    events = [_make_event() for _ in range(5)]
    for ev in events:
        await transport.deliver(ev, agent)

    await transport.drain()
    await transport.stop()

    assert len(processed) == 5


# ---------------------------------------------------------------------------
# RedpandaTransport with mocked aiokafka
# ---------------------------------------------------------------------------


async def test_redpanda_start_creates_consumer_per_agent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """start() creates one consumer task per registered agent."""
    mock_aiokafka = _make_mock_aiokafka_module()
    monkeypatch.setitem(sys.modules, "aiokafka", mock_aiokafka)

    from mdq.core.transport.redpanda import RedpandaTransport

    transport = RedpandaTransport(topic_prefix="test")
    agent_a = _make_agent("agent_a")
    agent_b = _make_agent("agent_b")
    transport.register(agent_a)
    transport.register(agent_b)

    await transport.start([agent_a, agent_b])

    assert len(transport._tasks) == 2
    assert "agent_a" in transport._tasks
    assert "agent_b" in transport._tasks

    await transport.stop()


async def test_redpanda_deliver_produces_to_correct_topic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """deliver() produces event bytes to {prefix}.agent.{agent_name} topic."""
    sent: list[tuple[str, bytes]] = []

    mock_producer = AsyncMock()
    mock_producer.start = AsyncMock()
    mock_producer.stop = AsyncMock()
    mock_producer.send_and_wait = AsyncMock(side_effect=lambda t, v: sent.append((t, v)))

    mock_aiokafka = _make_mock_aiokafka_module(producer=mock_producer)
    monkeypatch.setitem(sys.modules, "aiokafka", mock_aiokafka)

    from mdq.core.transport.redpanda import RedpandaTransport

    transport = RedpandaTransport(topic_prefix="mdq")
    agent = _make_agent("my_agent")
    transport.register(agent)
    await transport.start([agent])

    event = _make_event()
    await transport.deliver(event, agent)

    await transport.stop()

    topics_used = [topic for topic, _ in sent]
    assert any(
        t == "mdq.agent.my_agent" for t in topics_used
    ), f"Expected topic 'mdq.agent.my_agent'; got {topics_used}"


async def test_redpanda_raises_on_unreachable_broker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """start() raises RuntimeError when the broker is unreachable (NFR-9)."""
    mock_producer = AsyncMock()
    mock_producer.start = AsyncMock(side_effect=OSError("connection refused"))

    mock_aiokafka = _make_mock_aiokafka_module(producer=mock_producer)
    monkeypatch.setitem(sys.modules, "aiokafka", mock_aiokafka)

    from mdq.core.transport.redpanda import RedpandaTransport

    transport = RedpandaTransport(bootstrap_servers="bad-host:9999")
    with pytest.raises(RuntimeError, match="Redpanda unreachable at bad-host:9999"):
        await transport.start([])


async def test_redpanda_drain_resolves_when_no_pending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """drain() resolves immediately when no messages are in-flight."""
    mock_aiokafka = _make_mock_aiokafka_module()
    monkeypatch.setitem(sys.modules, "aiokafka", mock_aiokafka)

    from mdq.core.transport.redpanda import RedpandaTransport

    transport = RedpandaTransport()
    # drain() before start() should resolve immediately (no in-flight messages)
    await asyncio.wait_for(transport.drain(), timeout=1.0)


async def test_redpanda_topic_uses_configured_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Topic name uses the configured prefix from constructor."""
    from mdq.core.transport.redpanda import RedpandaTransport

    transport = RedpandaTransport(topic_prefix="custom-prefix")
    assert transport._topic("some_agent") == "custom-prefix.agent.some_agent"
