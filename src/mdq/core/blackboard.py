"""Blackboard: shared, append-only event bus and event log (FR-B1–FR-B4).

# DESIGN-NOTE: persist-before-deliver guarantees that if dispatch crashes mid-flight
# the event is still in the DuckDB log and can be replayed (C-4, C-5).
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import duckdb

from mdq.core.events import Event, TopicType
from mdq.core.transport.inprocess import InProcessTransport
from mdq.utils.logging import get_logger

if TYPE_CHECKING:
    from mdq.core.agent import Agent

log = get_logger("blackboard")

_CREATE_EVENTS_TABLE = """
CREATE TABLE IF NOT EXISTS events (
    event_id  TEXT PRIMARY KEY,
    topic     TEXT NOT NULL,
    ts        TIMESTAMPTZ NOT NULL,
    agent     TEXT NOT NULL,
    run_id    TEXT NOT NULL,
    payload   TEXT NOT NULL
)
"""


class Blackboard:
    """Shared event bus: register agents, publish events, persist the log."""

    def __init__(
        self,
        db_path: str,
        transport: InProcessTransport | None = None,
    ) -> None:
        self._db_path = db_path
        self._transport = transport or InProcessTransport()
        self._agents: list[Agent] = []
        self._conn: duckdb.DuckDBPyConnection | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def open(self) -> None:
        """Open the DuckDB event log connection and create the events table."""
        self._conn = duckdb.connect(self._db_path)
        self._conn.execute(_CREATE_EVENTS_TABLE)

    def close(self) -> None:
        """Close the DuckDB connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    async def start(self) -> None:
        """Open the event log and start agent dispatch loops."""
        self.open()
        await self._transport.start(self._agents)

    async def drain(self) -> None:
        """Wait until all enqueued events have been processed by agents (useful in tests)."""
        await self._transport.drain()

    async def stop(self) -> None:
        """Drain dispatch loops then close the event log."""
        await self._transport.stop()
        self.close()

    # ------------------------------------------------------------------
    # Agent registration
    # ------------------------------------------------------------------

    def register(self, agent: Agent) -> None:
        """Register *agent* for topic-based routing."""
        self._agents.append(agent)
        self._transport.register(agent)
        log.debug("registered agent %r (topics: %s)", agent.name, agent.subscriptions)

    # ------------------------------------------------------------------
    # Publishing
    # ------------------------------------------------------------------

    async def publish(self, event: Event) -> None:
        """Persist *event* to the log, then route it to subscribed agents."""
        self._persist(event)
        for agent in self._agents:
            if event.topic in agent.subscriptions:
                await self._transport.deliver(event, agent)
        log.debug("published %s from %r (run=%s)", event.topic, event.agent, event.run_id)

    # ------------------------------------------------------------------
    # Event log queries (for replay and audit)
    # ------------------------------------------------------------------

    def get_events(
        self,
        run_id: str | None = None,
        topic: TopicType | None = None,
    ) -> list[dict[str, Any]]:
        """Return persisted events, optionally filtered by run_id and/or topic."""
        conn = self._require_conn()
        clauses: list[str] = []
        params: list[str] = []
        if run_id is not None:
            clauses.append("run_id = ?")
            params.append(run_id)
        if topic is not None:
            clauses.append("topic = ?")
            params.append(topic.value)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = conn.execute(
            f"SELECT event_id, topic, ts, agent, run_id, payload FROM events {where} ORDER BY ts",
            params,
        ).fetchall()
        cols = ["event_id", "topic", "ts", "agent", "run_id", "payload"]
        return [dict(zip(cols, row, strict=True)) for row in rows]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _persist(self, event: Event) -> None:
        conn = self._require_conn()
        conn.execute(
            """
            INSERT INTO events (event_id, topic, ts, agent, run_id, payload)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT DO NOTHING
            """,
            [
                event.event_id,
                event.topic.value,
                event.ts.isoformat(),
                event.agent,
                event.run_id,
                json.dumps(event.payload),
            ],
        )

    def _require_conn(self) -> duckdb.DuckDBPyConnection:
        if self._conn is None:
            raise RuntimeError("Blackboard is not open. Call open() or start() before publishing.")
        return self._conn
