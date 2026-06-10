"""Canonical Pydantic v2 + Pandera schemas for all medallion layers (PRD §10).

Pydantic models are the record-level contract (one row = one model instance).
The Pandera DataFrameModel is the batch-level contract used by the Contract Agent.
"""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

import pandera as pa
import pandera.typing as pat
from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Shared enumerations
# ---------------------------------------------------------------------------


class FieldType(StrEnum):
    OPEN = "OPEN"
    HIGH = "HIGH"
    LOW = "LOW"
    CLOSE = "CLOSE"
    ADJ_CLOSE = "ADJ_CLOSE"
    VOLUME = "VOLUME"
    SHARES = "SHARES"  # SEC EDGAR shares outstanding (Phase 7)


class Confidence(StrEnum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class DecisionType(StrEnum):
    INGEST = "INGEST"
    CONTRACT = "CONTRACT"
    DQ = "DQ"
    CORP_ACTION = "CORP_ACTION"
    RECONCILE = "RECONCILE"
    REMEDIATE = "REMEDIATE"
    ESCALATE = "ESCALATE"


class OverallStatus(StrEnum):
    GREEN = "GREEN"
    AMBER = "AMBER"
    RED = "RED"


# ---------------------------------------------------------------------------
# Pydantic record models (PRD §10)
# ---------------------------------------------------------------------------


class PriceRecord(BaseModel):
    """Canonical Silver-layer price record."""

    instrument_id: str
    business_date: date
    field: FieldType
    value: Decimal
    currency: str
    source_id: str
    source_symbol: str
    fetch_ts: datetime
    event_ts: datetime
    content_hash: str
    ca_adjusted: bool = False


class GoldenRecord(BaseModel):
    """Gold-layer elected golden value."""

    instrument_id: str
    business_date: date
    field: FieldType
    golden_value: Decimal
    currency: str
    quorum_sources: list[str]
    dissenting_sources: list[str]
    tolerance_band: str
    confidence: Confidence
    decision_id: str = Field(default_factory=lambda: str(uuid.uuid4()))


class DecisionRecord(BaseModel):
    """Lineage/audit record for every data-affecting decision (C-4)."""

    decision_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    ts: datetime = Field(default_factory=lambda: datetime.now(tz=UTC))
    agent: str
    instrument_id: str
    business_date: date
    decision_type: DecisionType
    inputs: dict[str, Any]
    outcome: dict[str, Any]
    rule_applied: str
    verified: bool = False


class ScorecardRecord(BaseModel):
    """Daily DQ scorecard record (FR-O2)."""

    run_id: str
    business_date: date
    source_id: str
    records_ingested: int = 0
    dq_failures_by_rule: dict[str, int] = Field(default_factory=dict)
    breaks_detected: int = 0
    remediations_attempted: int = 0
    remediations_succeeded: int = 0
    escalations: int = 0
    corporate_actions_detected: int = 0
    overall_status: OverallStatus = OverallStatus.GREEN


# ---------------------------------------------------------------------------
# Pandera DataFrame schema — Silver layer contract (FR-A2)
# ---------------------------------------------------------------------------

_VALID_FIELDS = [f.value for f in FieldType]


class SilverSchema(pa.DataFrameModel):
    """Batch-level contract for the Silver DataFrame.

    Used by the Contract Agent (Phase 1) to validate shape and types before
    any downstream processing.  coerce=True converts compatible types automatically.
    """

    instrument_id: pat.Series[str]
    business_date: pat.Series[pa.dtypes.DateTime]
    field: pat.Series[str] = pa.Field(isin=_VALID_FIELDS)
    value: pat.Series[float]
    currency: pat.Series[str]
    source_id: pat.Series[str]
    source_symbol: pat.Series[str]
    fetch_ts: pat.Series[pa.dtypes.DateTime]
    event_ts: pat.Series[pa.dtypes.DateTime]
    content_hash: pat.Series[str]
    ca_adjusted: pat.Series[bool]

    class Config:
        coerce = True
        strict = "filter"
