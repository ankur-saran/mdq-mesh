"""Default in-process asyncio transport for the blackboard (FR-B3).

# DESIGN-NOTE: Transport is implemented as a concrete class that satisfies a
# structural Protocol (not an ABC) so Phase 8's optional RedpandaTransport can
# live in an opt-in module and is never imported on the pure-local path (C-3).
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from mdq.utils.logging import get_logger

if TYPE_CHECKING:
    from mdq.core.agent import Agent
    from mdq.core.events import Event

log = get_logger("transport.inprocess")

# Sentinel used to signal dispatch loops to exit cleanly
_STOP: object = object()


class InProcessTransport:
    """Per-agent asyncio.Queue dispatch. Zero external infrastructure (C-3)."""

    def __init__(self) -> None:
        self._queues: dict[str, asyncio.Queue[Event | object]] = {}
        self._tasks: dict[str, asyncio.Task[None]] = {}

    def register(self, agent: Agent) -> None:
        """Create a dedicated queue for *agent* (idempotent)."""
        if agent.name not in self._queues:
            self._queues[agent.name] = asyncio.Queue()

    async def deliver(self, event: Event, agent: Agent) -> None:
        """Enqueue *event* for the agent's dispatch loop."""
        queue = self._queues.get(agent.name)
        if queue is not None:
            await queue.put(event)

    async def start(self, agents: list[Agent]) -> None:
        """Start one dispatch loop task per registered agent."""
        for agent in agents:
            if agent.name in self._queues:
                task = asyncio.create_task(
                    self._dispatch_loop(agent, self._queues[agent.name]),
                    name=f"dispatch-{agent.name}",
                )
                self._tasks[agent.name] = task
        log.debug("InProcessTransport: started %d dispatch loops", len(self._tasks))

    async def drain(self) -> None:
        """Wait until every enqueued event has been fully processed by its agent."""
        await asyncio.gather(*[q.join() for q in self._queues.values()])

    async def stop(self) -> None:
        """Send stop sentinels and wait for all dispatch loops to finish."""
        for queue in self._queues.values():
            await queue.put(_STOP)
        if self._tasks:
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)
        log.debug("InProcessTransport: all dispatch loops stopped")

    async def _dispatch_loop(
        self,
        agent: Agent,
        queue: asyncio.Queue[Event | object],
    ) -> None:
        while True:
            item = await queue.get()
            if item is _STOP:
                queue.task_done()
                break
            # item is guaranteed to be Event here (only _STOP is non-Event)
            from mdq.core.events import Event as _Event  # local import avoids cycle

            if not isinstance(item, _Event):
                queue.task_done()
                continue
            try:
                if agent.should_act(item):
                    await agent.act(item)
            except Exception:
                log.exception(
                    "Agent %r raised while handling event %s (%s)",
                    agent.name,
                    item.event_id,
                    item.topic,
                )
            finally:
                queue.task_done()
