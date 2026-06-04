"""Agent ABC — every mdq-mesh agent implements this interface (PRD §8.1, NFR-3)."""

from __future__ import annotations

from abc import ABC, abstractmethod

from mdq.core.events import Event, TopicType


class Agent(ABC):
    """Common agent interface.

    Agents are independently testable in isolation: construct with a mock blackboard,
    call should_act() / act() directly — no real transport needed (NFR-3).
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique agent identifier used for routing and structured logging."""
        ...

    @property
    @abstractmethod
    def subscriptions(self) -> list[TopicType]:
        """Topics this agent wants to receive from the blackboard."""
        ...

    def should_act(self, event: Event) -> bool:
        """Return True if this agent should process *event*. Override to add filters."""
        return event.topic in self.subscriptions

    @abstractmethod
    async def act(self, event: Event) -> None:
        """Process one event. Called by the blackboard dispatch loop."""
        ...

    async def start(self) -> None:  # noqa: B027
        """Lifecycle hook: called once before the run loop begins. Default no-op."""

    async def stop(self) -> None:  # noqa: B027
        """Lifecycle hook: called once after the run loop ends. Default no-op."""
