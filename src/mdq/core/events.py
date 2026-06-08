"""Typed event and topic definitions — the single vocabulary of the blackboard (FR-B1, FR-B2)."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class TopicType(StrEnum):
    """All topics agents may publish to or subscribe from."""

    # Pipeline lifecycle
    RUN_STARTED = "run.started"
    RUN_COMPLETE = "run.complete"

    # Ingestion
    INGESTION_COMPLETE = "ingestion.complete"
    INGESTION_FAILED = "ingestion.failed"

    # Contract validation
    CONTRACT_VIOLATION = "contract.violation"
    CONTRACT_PASSED = "contract.passed"

    # Data quality
    DQ_FAILURE = "dq.failure"
    DQ_PASSED = "dq.passed"

    # Anomaly detection
    ANOMALY_DETECTED = "anomaly.detected"

    # Corporate actions
    CORPORATE_ACTION_DETECTED = "corporate_action.detected"

    # Reconciliation
    RECONCILIATION_COMPLETE = "reconciliation.complete"
    BREAK_DETECTED = "reconciliation.break"

    # Remediation
    REFETCH_REQUESTED = "remediation.refetch_requested"
    REMEDIATION_STARTED = "remediation.started"
    REMEDIATION_COMPLETE = "remediation.complete"
    REMEDIATION_FAILED = "remediation.failed"

    # Escalation
    ESCALATION = "escalation"

    # Lineage
    DECISION_RECORDED = "lineage.decision"


class Event(BaseModel):
    """An immutable blackboard event. All agents read and write these.

    # DESIGN-NOTE: frozen=True makes events immutable after construction, matching the
    # append-only semantics of the event log (C-4 auditability, C-5 determinism).
    """

    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    topic: TopicType
    ts: datetime = Field(default_factory=lambda: datetime.now(tz=UTC))
    agent: str
    run_id: str
    payload: dict[str, Any] = Field(default_factory=dict)

    model_config = {"frozen": True}
