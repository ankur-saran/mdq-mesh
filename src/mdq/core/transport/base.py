"""Transport Protocol for the blackboard event bus (FR-B3).

# DESIGN-NOTE: Protocol (not ABC) so InProcessTransport satisfies it structurally
# without modification. RedpandaTransport lives in an opt-in module that is never
# imported on the pure-local code path (C-3).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from mdq.core.agent import Agent
    from mdq.core.events import Event


@runtime_checkable
class Transport(Protocol):
    """Structural interface every blackboard transport must satisfy (FR-B3)."""

    def register(self, agent: Agent) -> None:
        """Prepare routing for *agent* (called by Blackboard.register)."""
        ...

    async def deliver(self, event: Event, agent: Agent) -> None:
        """Route *event* to *agent* for processing."""
        ...

    async def start(self, agents: list[Agent]) -> None:
        """Start dispatch loops for all registered agents."""
        ...

    async def drain(self) -> None:
        """Block until all in-flight events have been fully processed."""
        ...

    async def stop(self) -> None:
        """Shut down all dispatch loops cleanly."""
        ...
