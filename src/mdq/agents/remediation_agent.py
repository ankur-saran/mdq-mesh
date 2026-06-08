"""Remediation Agent — self-healing loop: quarantine → re-fetch → verify → retry → escalate (FR-A7).

Subscribes to BREAK_DETECTED, RECONCILIATION_COMPLETE, and INGESTION_FAILED.
Accumulates breaks per run_id, then triggers remediation when RECONCILIATION_COMPLETE
confirms the breaks. On success (RECONCILIATION_COMPLETE with breaks=0 while active),
publishes REMEDIATION_COMPLETE and writes a verified DecisionRecord.

# DESIGN-NOTE: FR-H4 — instruments failing quorum are NEVER written to Gold by
# ReconciliationAgent (they go to quarantine). The hold is therefore implicit and
# guaranteed by the existing design without any new mechanism.
# DESIGN-NOTE: FR-H2 — verification fires when RECONCILIATION_COMPLETE(breaks=0)
# arrives while _active[run_id]=True. DecisionRecord(REMEDIATE, verified=True) is
# written only at that point.
# DESIGN-NOTE: C-5 — _identify_bad_source() sorts dissenting sources alphabetically
# before cycling, so retry order is reproducible on identical inputs.
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

log = get_logger("agents.remediation")


class RemediationAgent(Agent):
    """Self-healing loop: detect breaks → re-fetch → verify → retry → escalate (FR-A7)."""

    def __init__(self, bb: Blackboard, store: MedallionStore, cfg: Config) -> None:
        self._bb = bb
        self._store = store
        self._cfg = cfg
        self._enabled_sources: frozenset[str] = frozenset(
            src
            for src, src_cfg in [
                ("yfinance", cfg.sources.yfinance),
                ("stooq", cfg.sources.stooq),
            ]
            if src_cfg.enabled
        )
        # {run_id: [break_payload, ...]} — accumulated before RECONCILIATION_COMPLETE fires
        self._breaks: dict[str, list[dict[str, object]]] = {}
        # {run_id: attempt_count} — total remediation attempts started for this run
        self._attempt_count: dict[str, int] = {}
        # {run_id: True} — True = a remediation attempt is in-flight
        self._active: dict[str, bool] = {}
        # {run_id: date}
        self._bdates: dict[str, date] = {}

    @property
    def name(self) -> str:
        return "remediation"

    @property
    def subscriptions(self) -> list[TopicType]:
        return [
            TopicType.BREAK_DETECTED,
            TopicType.RECONCILIATION_COMPLETE,
            TopicType.INGESTION_FAILED,
        ]

    async def act(self, event: Event) -> None:
        if event.topic == TopicType.BREAK_DETECTED:
            await self._handle_break_detected(event)

        elif event.topic == TopicType.RECONCILIATION_COMPLETE:
            await self._handle_reconciliation_complete(event)

        elif event.topic == TopicType.INGESTION_FAILED:
            await self._handle_ingestion_failed(event)

    # ------------------------------------------------------------------
    # Event handlers
    # ------------------------------------------------------------------

    async def _handle_break_detected(self, event: Event) -> None:
        run_id = event.run_id
        if run_id not in self._breaks:
            self._breaks[run_id] = []
        bdate_str: str = str(event.payload.get("business_date", ""))
        if bdate_str and run_id not in self._bdates:
            self._bdates[run_id] = date.fromisoformat(bdate_str)
        self._breaks[run_id].append(dict(event.payload))

    async def _handle_reconciliation_complete(self, event: Event) -> None:
        run_id = event.run_id
        n_breaks = int(event.payload.get("breaks", 0))
        bdate_str: str = str(event.payload.get("business_date", ""))
        if not bdate_str:
            return
        business_date = date.fromisoformat(bdate_str)
        if run_id not in self._bdates:
            self._bdates[run_id] = business_date

        if n_breaks == 0 and self._active.get(run_id, False):
            # Remediation succeeded — breaks resolved
            await self._complete_remediation(run_id, business_date)
            return

        if n_breaks > 0 and run_id in self._breaks:
            attempt = self._attempt_count.get(run_id, 0)
            if attempt >= self._cfg.remediation.max_retries:
                # Exhausted retries → escalate
                bad_src = _identify_bad_source(
                    self._breaks[run_id], attempt + 1, self._enabled_sources
                )
                await self._escalate(run_id, bad_src, business_date)
            else:
                # Start (or retry) remediation
                await self._start_remediation(run_id, business_date)

    async def _handle_ingestion_failed(self, event: Event) -> None:
        """INGESTION_FAILED escalates immediately — source is unavailable, re-fetch won't help."""
        run_id = event.run_id
        source_id: str = str(event.payload.get("source_id", "unknown"))
        bdate_str: str = str(event.payload.get("business_date", date.today().isoformat()))
        business_date = date.fromisoformat(bdate_str)
        log.warning(
            "INGESTION_FAILED for source=%s run=%s — escalating (source unavailable)",
            source_id,
            run_id,
        )
        await self._escalate(run_id, source_id, business_date)

    # ------------------------------------------------------------------
    # Remediation actions
    # ------------------------------------------------------------------

    async def _start_remediation(self, run_id: str, business_date: date) -> None:
        attempt = self._attempt_count.get(run_id, 0) + 1
        self._attempt_count[run_id] = attempt
        self._active[run_id] = True

        bad_source = _identify_bad_source(self._breaks[run_id], attempt, self._enabled_sources)
        widen_steps = self._cfg.remediation.lookback_widen_steps
        lookback_days = widen_steps[min(attempt - 1, len(widen_steps) - 1)]

        good_sources = sorted(self._enabled_sources - {bad_source})
        log.info(
            "Remediation attempt %d/%d: re-fetch %s, lookback_days=%d (run=%s, date=%s)",
            attempt,
            self._cfg.remediation.max_retries,
            bad_source,
            lookback_days,
            run_id,
            business_date,
        )

        # Re-queue good sources so ReconciliationAgent can re-accumulate
        for good in good_sources:
            await self._bb.publish(
                Event(
                    topic=TopicType.DQ_PASSED,
                    agent=self.name,
                    run_id=run_id,
                    payload={
                        "source_id": good,
                        "business_date": business_date.isoformat(),
                    },
                )
            )

        # Trigger targeted re-fetch for the bad source
        await self._bb.publish(
            Event(
                topic=TopicType.REFETCH_REQUESTED,
                agent=self.name,
                run_id=run_id,
                payload={
                    "source_id": bad_source,
                    "business_date": business_date.isoformat(),
                    "lookback_days": lookback_days,
                },
            )
        )

    async def _complete_remediation(self, run_id: str, business_date: date) -> None:
        attempts = self._attempt_count.get(run_id, 1)
        log.info(
            "Remediation SUCCEEDED after %d attempt(s) (run=%s, date=%s)",
            attempts,
            run_id,
            business_date,
        )
        record = DecisionRecord(
            agent=self.name,
            instrument_id="batch",
            business_date=business_date,
            decision_type=DecisionType.REMEDIATE,
            inputs={
                "run_id": run_id,
                "attempts": attempts,
                "breaks": [b.get("instrument_id") for b in self._breaks.get(run_id, [])],
            },
            outcome={"status": "remediated", "attempts": attempts},
            rule_applied="refetch_and_reconcile",
            verified=True,
        )
        self._store.write_decision(record)
        await self._bb.publish(
            Event(
                topic=TopicType.REMEDIATION_COMPLETE,
                agent=self.name,
                run_id=run_id,
                payload={
                    "business_date": business_date.isoformat(),
                    "attempts": attempts,
                    "decision_id": record.decision_id,
                },
            )
        )
        self._clear_state(run_id)

    async def _escalate(self, run_id: str, source_id: str, business_date: date) -> None:
        attempts = self._attempt_count.get(run_id, 0)
        log.warning(
            "ESCALATION: source=%s exhausted retries after %d attempt(s) (run=%s)",
            source_id,
            attempts,
            run_id,
        )
        record = DecisionRecord(
            agent=self.name,
            instrument_id="batch",
            business_date=business_date,
            decision_type=DecisionType.ESCALATE,
            inputs={
                "run_id": run_id,
                "source_id": source_id,
                "attempts": attempts,
                "breaks": [b.get("instrument_id") for b in self._breaks.get(run_id, [])],
            },
            outcome={"status": "escalated", "reason": "max_retries_exhausted"},
            rule_applied="escalate_after_retries",
            verified=False,
        )
        self._store.write_decision(record)
        await self._bb.publish(
            Event(
                topic=TopicType.ESCALATION,
                agent=self.name,
                run_id=run_id,
                payload={
                    "source_id": source_id,
                    "business_date": business_date.isoformat(),
                    "attempts": attempts,
                    "decision_id": record.decision_id,
                },
            )
        )
        await self._bb.publish(
            Event(
                topic=TopicType.REMEDIATION_FAILED,
                agent=self.name,
                run_id=run_id,
                payload={
                    "source_id": source_id,
                    "business_date": business_date.isoformat(),
                    "reason": "max_retries_exhausted",
                },
            )
        )
        self._clear_state(run_id)

    def _clear_state(self, run_id: str) -> None:
        self._breaks.pop(run_id, None)
        self._attempt_count.pop(run_id, None)
        self._active.pop(run_id, None)
        self._bdates.pop(run_id, None)


# ---------------------------------------------------------------------------
# Pure deterministic helper (C-4/C-5)
# ---------------------------------------------------------------------------


def _identify_bad_source(
    breaks: list[dict[str, object]],
    attempt: int,
    enabled_sources: frozenset[str],
) -> str:
    """Return the source_id to re-fetch on this attempt (1-based).

    # DESIGN-NOTE: C-5 — sorted alphabetically then cycled by (attempt-1) mod len
    # for reproducible retry order on identical inputs.
    # DESIGN-NOTE: FR-H1 — prioritises absent sources (all-NaN / missing from the
    # reconciliation pivot) over dissenting sources (value mismatch). A source absent
    # from source_values likely has all-NaN data (e.g. NULL_BURST) and is the primary
    # remediation candidate, even though it doesn't appear in dissenting_sources.
    """
    absent: set[str] = set()
    dissenting: set[str] = set()
    for b in breaks:
        sv = b.get("source_values", {})
        if isinstance(sv, dict):
            present = set(sv.keys())
            absent.update(enabled_sources - present)
        raw = b.get("dissenting_sources", [])
        if isinstance(raw, list):
            dissenting.update(str(s) for s in raw)

    if absent:
        candidates = sorted(absent)
    elif dissenting:
        candidates = sorted(dissenting)
    else:
        candidates = sorted(enabled_sources)
    return candidates[(attempt - 1) % len(candidates)]
