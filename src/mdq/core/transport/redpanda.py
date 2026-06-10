"""Optional Redpanda/Kafka transport backend for the blackboard (FR-B3, Phase 8).

# DESIGN-NOTE: C-3 — this module is ONLY imported when cfg.runtime.transport == "redpanda".
# It is never on the pure-local code path. aiokafka is imported lazily inside start()
# so importing this module does not require aiokafka to be installed.
# DESIGN-NOTE: FR-B3 — satisfies the Transport Protocol structurally (same 5 methods).
# DESIGN-NOTE: One Kafka topic per agent: {topic_prefix}.agent.{agent_name}. This mirrors
# the per-agent asyncio.Queue isolation in InProcessTransport (single-consumer group per
# agent → ordered delivery, no cross-agent interference).
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime
from typing import TYPE_CHECKING, Any

from mdq.utils.logging import get_logger

if TYPE_CHECKING:
    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

    from mdq.core.agent import Agent
    from mdq.core.events import Event

log = get_logger("transport.redpanda")

_STOP_SENTINEL: bytes = b'{"__mdq_stop__": true}'


class RedpandaTransport:
    """Kafka/Redpanda-backed event transport. One topic per agent (FR-B3).

    Requires a running Redpanda/Kafka broker at bootstrap_servers.
    Fails fast at start() if the broker is unreachable (NFR-9 spirit).
    """

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic_prefix: str = "mdq",
        consumer_group: str = "mdq-mesh",
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._topic_prefix = topic_prefix
        self._consumer_group = consumer_group
        self._agents: dict[str, Agent] = {}
        self._producer: AIOKafkaProducer | None = None
        self._consumers: dict[str, AIOKafkaConsumer] = {}
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._pending: int = 0
        # _drain_event is set (drained) when no in-flight messages remain
        self._drain_event: asyncio.Event = asyncio.Event()
        self._drain_event.set()

    # ------------------------------------------------------------------
    # Transport Protocol interface
    # ------------------------------------------------------------------

    def register(self, agent: Agent) -> None:
        """Note *agent* for topic routing (idempotent)."""
        if agent.name not in self._agents:
            self._agents[agent.name] = agent

    async def deliver(self, event: Event, agent: Agent) -> None:
        """Produce serialised *event* JSON to agent's Kafka topic."""
        assert self._producer is not None, "call start() before deliver()"
        data: dict[str, Any] = {
            "event_id": event.event_id,
            "topic": event.topic.value,
            "ts": event.ts.isoformat(),
            "agent_src": event.agent,
            "run_id": event.run_id,
            "payload": event.payload,
        }
        self._pending += 1
        if self._pending == 1:
            self._drain_event.clear()
        await self._producer.send_and_wait(self._topic(agent.name), json.dumps(data).encode())

    async def start(self, agents: list[Agent]) -> None:
        """Start Kafka producer + one consumer task per registered agent."""
        from aiokafka import AIOKafkaConsumer, AIOKafkaProducer  # lazy import — C-3

        self._producer = AIOKafkaProducer(bootstrap_servers=self._bootstrap_servers)
        try:
            await self._producer.start()
        except Exception as exc:
            # DESIGN-NOTE: broad catch — fail fast on any connection failure (NFR-9 spirit).
            # KafkaConnectionError, OSError, ConnectionRefusedError all indicate no broker.
            raise RuntimeError(f"Redpanda unreachable at {self._bootstrap_servers}: {exc}") from exc

        for agent in agents:
            if agent.name not in self._agents:
                continue
            consumer: AIOKafkaConsumer = AIOKafkaConsumer(
                self._topic(agent.name),
                bootstrap_servers=self._bootstrap_servers,
                group_id=f"{self._consumer_group}.{agent.name}",
                auto_offset_reset="latest",
            )
            await consumer.start()
            self._consumers[agent.name] = consumer
            task = asyncio.create_task(
                self._consume_loop(agent, consumer),
                name=f"redpanda-{agent.name}",
            )
            self._tasks[agent.name] = task

        log.debug("RedpandaTransport: started %d consumer loops", len(self._tasks))

    async def drain(self) -> None:
        """Wait until all in-flight messages have been processed."""
        await self._drain_event.wait()

    async def stop(self) -> None:
        """Send stop-sentinels, wait for consumer tasks, close producer and consumers."""
        if self._producer is not None:
            for agent_name in self._consumers:
                await self._producer.send_and_wait(self._topic(agent_name), _STOP_SENTINEL)
        if self._tasks:
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)
        if self._producer is not None:
            await self._producer.stop()
        for consumer in self._consumers.values():
            await consumer.stop()
        log.debug("RedpandaTransport: all consumer loops stopped")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _topic(self, agent_name: str) -> str:
        return f"{self._topic_prefix}.agent.{agent_name}"

    async def _consume_loop(self, agent: Agent, consumer: AIOKafkaConsumer) -> None:
        """Read messages from agent's Kafka topic and dispatch to agent.act()."""
        from mdq.core.events import Event as _Event
        from mdq.core.events import TopicType as _TopicType

        async for msg in consumer:
            value: bytes = msg.value
            if value == _STOP_SENTINEL:
                break
            try:
                data = json.loads(value)
                event = _Event(
                    event_id=data["event_id"],
                    topic=_TopicType(data["topic"]),
                    ts=datetime.fromisoformat(data["ts"]),
                    agent=data["agent_src"],
                    run_id=data["run_id"],
                    payload=data["payload"],
                )
                if agent.should_act(event):
                    await agent.act(event)
            except Exception:
                log.exception(
                    "RedpandaTransport: error processing message for agent %r", agent.name
                )
            finally:
                self._pending -= 1
                if self._pending <= 0:
                    self._pending = 0
                    self._drain_event.set()
