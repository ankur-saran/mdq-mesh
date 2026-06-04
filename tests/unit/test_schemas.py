"""Unit tests for canonical Pydantic + Pandera schemas (PRD §10)."""

from __future__ import annotations

import datetime
from decimal import Decimal

import pandas as pd
import pandera as pa
import pytest

from mdq.core.schemas import (
    Confidence,
    DecisionRecord,
    DecisionType,
    FieldType,
    GoldenRecord,
    OverallStatus,
    PriceRecord,
    ScorecardRecord,
    SilverSchema,
)

_DATE = datetime.date(2024, 1, 2)
_DT = datetime.datetime(2024, 1, 2, 21, 0, tzinfo=datetime.UTC)


def _silver_row(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "instrument_id": "AAPL",
        "business_date": _DT,
        "field": "CLOSE",
        "value": 182.55,
        "currency": "USD",
        "source_id": "yfinance",
        "source_symbol": "AAPL",
        "fetch_ts": _DT,
        "event_ts": _DT,
        "content_hash": "abc123",
        "ca_adjusted": False,
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# PriceRecord
# ---------------------------------------------------------------------------


def test_price_record_valid() -> None:
    r = PriceRecord(
        instrument_id="AAPL",
        business_date=_DATE,
        field=FieldType.CLOSE,
        value=Decimal("182.55"),
        currency="USD",
        source_id="yfinance",
        source_symbol="AAPL",
        fetch_ts=_DT,
        event_ts=_DT,
        content_hash="abc123",
    )
    assert r.field == FieldType.CLOSE
    assert r.ca_adjusted is False


def test_price_record_invalid_field() -> None:
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        PriceRecord(
            instrument_id="AAPL",
            business_date=_DATE,
            field="BOGUS",  # type: ignore[arg-type]
            value=Decimal("182.55"),
            currency="USD",
            source_id="yfinance",
            source_symbol="AAPL",
            fetch_ts=_DT,
            event_ts=_DT,
            content_hash="abc123",
        )


# ---------------------------------------------------------------------------
# GoldenRecord
# ---------------------------------------------------------------------------


def test_golden_record_valid() -> None:
    g = GoldenRecord(
        instrument_id="AAPL",
        business_date=_DATE,
        field=FieldType.CLOSE,
        golden_value=Decimal("182.55"),
        currency="USD",
        quorum_sources=["yfinance", "stooq"],
        dissenting_sources=[],
        tolerance_band="25bps",
        confidence=Confidence.HIGH,
        decision_id="d-001",
    )
    assert g.confidence == Confidence.HIGH
    assert g.quorum_sources == ["yfinance", "stooq"]


def test_golden_record_auto_decision_id() -> None:
    g = GoldenRecord(
        instrument_id="AAPL",
        business_date=_DATE,
        field=FieldType.CLOSE,
        golden_value=Decimal("182.55"),
        currency="USD",
        quorum_sources=["yfinance"],
        dissenting_sources=[],
        tolerance_band="25bps",
        confidence=Confidence.LOW,
    )
    assert g.decision_id  # auto-generated UUID


# ---------------------------------------------------------------------------
# DecisionRecord
# ---------------------------------------------------------------------------


def test_decision_record_defaults() -> None:
    d = DecisionRecord(
        agent="dq_agent",
        instrument_id="AAPL",
        business_date=_DATE,
        decision_type=DecisionType.DQ,
        inputs={"value": 182.55},
        outcome={"status": "pass"},
        rule_applied="null_check",
    )
    assert d.decision_id  # UUID auto-generated
    assert d.verified is False
    assert d.ts is not None


# ---------------------------------------------------------------------------
# ScorecardRecord
# ---------------------------------------------------------------------------


def test_scorecard_defaults() -> None:
    s = ScorecardRecord(run_id="r1", business_date=_DATE, source_id="yfinance")
    assert s.overall_status == OverallStatus.GREEN
    assert s.records_ingested == 0


# ---------------------------------------------------------------------------
# Pandera Silver schema
# ---------------------------------------------------------------------------


def test_silver_schema_valid() -> None:
    df = pd.DataFrame([_silver_row()])
    validated = SilverSchema.validate(df)
    assert len(validated) == 1


def test_silver_schema_invalid_field() -> None:
    df = pd.DataFrame([_silver_row(field="BOGUS")])
    with pytest.raises(pa.errors.SchemaError):
        SilverSchema.validate(df)


def test_silver_schema_extra_column_filtered() -> None:
    df = pd.DataFrame([_silver_row(extra_col="ignored")])
    validated = SilverSchema.validate(df)
    assert "extra_col" not in validated.columns
