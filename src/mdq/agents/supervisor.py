"""Supervisor Agent — run lifecycle tracking, escalation enforcement, and run summary (FR-A9).

Subscribes to ESCALATION, REMEDIATION_COMPLETE, REMEDIATION_FAILED, and RUN_COMPLETE.
Tracks remediation outcomes per run_id, writes a DecisionRecord(ESCALATE) for each
escalation, and emits a run summary (including KPI-1) at RUN_COMPLETE.

# DESIGN-NOTE: FR-A9 — Supervisor is intentionally lightweight in Phase 5: it tracks
# outcomes and produces the summary. The enforcement of "escalate after retries" is
# already implemented in RemediationAgent; Supervisor observes and records results.
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

from mdq.core.agent import Agent
from mdq.core.events import Event, TopicType
from mdq.core.schemas import DecisionRecord, DecisionType
from mdq.utils.logging import get_logger

if TYPE_CHECKING:
    from mdq.core.blackboard import Blackboard
    from mdq.core.config import Config
    from mdq.core.store import MedallionStore

log = get_logger("agents.supervisor")


class Supervisor(Agent):
    """Tracks escalations and remediation outcomes; emits run summary at RUN_COMPLETE (FR-A9)."""

    def __init__(self, bb: Blackboard, store: MedallionStore, cfg: Config) -> None:
        self._bb = bb
        self._store = store
        self._cfg = cfg
        # {run_id: [escalation_payload, ...]}
        self._escalations: dict[str, list[dict[str, object]]] = {}
        # {run_id: count}
        self._rem_succeeded: dict[str, int] = {}
        self._rem_attempted: dict[str, int] = {}

    @property
    def name(self) -> str:
        return "supervisor"

    @property
    def subscriptions(self) -> list[TopicType]:
        return [
            TopicType.ESCALATION,
            TopicType.REMEDIATION_COMPLETE,
            TopicType.REMEDIATION_FAILED,
            TopicType.RUN_COMPLETE,
        ]

    async def act(self, event: Event) -> None:
        run_id = event.run_id

        if event.topic == TopicType.ESCALATION:
            await self._handle_escalation(event)

        elif event.topic == TopicType.REMEDIATION_COMPLETE:
            self._rem_succeeded[run_id] = self._rem_succeeded.get(run_id, 0) + 1
            self._rem_attempted[run_id] = self._rem_attempted.get(run_id, 0) + 1
            log.info(
                "REMEDIATION_COMPLETE (run=%s, succeeded=%d, attempted=%d)",
                run_id,
                self._rem_succeeded[run_id],
                self._rem_attempted[run_id],
            )

        elif event.topic == TopicType.REMEDIATION_FAILED:
            self._rem_attempted[run_id] = self._rem_attempted.get(run_id, 0) + 1
            log.warning(
                "REMEDIATION_FAILED (run=%s, attempted=%d)",
                run_id,
                self._rem_attempted[run_id],
            )

        elif event.topic == TopicType.RUN_COMPLETE:
            self._emit_summary(run_id)
            self._clear_state(run_id)

    # ------------------------------------------------------------------
    # Handlers
    # ------------------------------------------------------------------

    async def _handle_escalation(self, event: Event) -> None:
        run_id = event.run_id
        if run_id not in self._escalations:
            self._escalations[run_id] = []
        self._escalations[run_id].append(dict(event.payload))

        source_id: str = str(event.payload.get("source_id", "unknown"))
        bdate_str: str = str(event.payload.get("business_date", date.today().isoformat()))
        business_date = date.fromisoformat(bdate_str)

        log.warning(
            "ESCALATION: source=%s requires human attention (run=%s, date=%s)",
            source_id,
            run_id,
            business_date,
        )
        record = DecisionRecord(
            agent=self.name,
            instrument_id="batch",
            business_date=business_date,
            decision_type=DecisionType.ESCALATE,
            inputs=dict(event.payload),
            outcome={"status": "escalated", "requires_human": True},
            rule_applied="supervisor_escalation_policy",
            verified=False,
        )
        self._store.write_decision(record)

    def _emit_summary(self, run_id: str) -> None:
        attempted = self._rem_attempted.get(run_id, 0)
        succeeded = self._rem_succeeded.get(run_id, 0)
        escalations = len(self._escalations.get(run_id, []))
        kpi_1 = succeeded / attempted if attempted > 0 else 1.0

        log.info(
            "RUN SUMMARY (run=%s): remediations=%d/%d (KPI-1=%.0f%%), escalations=%d",
            run_id,
            succeeded,
            attempted,
            kpi_1 * 100,
            escalations,
        )

    def _clear_state(self, run_id: str) -> None:
        self._escalations.pop(run_id, None)
        self._rem_succeeded.pop(run_id, None)
        self._rem_attempted.pop(run_id, None)
