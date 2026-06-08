"""Unit tests for MedallionStore: Bronze/Silver/Gold/Quarantine/Decision (FR-S1–FR-S5)."""

from __future__ import annotations

import datetime
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest

from mdq.core.schemas import (
    Confidence,
    DecisionRecord,
    DecisionType,
    FieldType,
    GoldenRecord,
)
from mdq.core.store import MedallionStore

_DATE = datetime.date(2024, 1, 2)


@pytest.fixture
def store(tmp_path: Path) -> MedallionStore:
    s = MedallionStore(
        bronze_root=tmp_path / "bronze",
        silver_root=tmp_path / "silver",
        gold_root=tmp_path / "gold",
        quarantine_root=tmp_path / "quarantine",
        lineage_root=tmp_path / "lineage",
        duckdb_path=tmp_path / "mdq.duckdb",
    )
    s.init_dirs()
    s.open()
    return s


def test_init_dirs_creates_all(store: MedallionStore, tmp_path: Path) -> None:
    for d in ("bronze", "silver", "gold", "quarantine", "lineage"):
        assert (tmp_path / d).is_dir()


def test_write_and_read_bronze(store: MedallionStore) -> None:
    df = pd.DataFrame([{"instrument_id": "AAPL", "value": 182.55}])
    path = store.write_bronze("yfinance", "run-001", df, _DATE)
    assert path.exists()
    loaded = pd.read_parquet(path)
    assert list(loaded["instrument_id"]) == ["AAPL"]


def test_bronze_immutable_on_second_write(store: MedallionStore) -> None:
    df = pd.DataFrame([{"v": 1}])
    p1 = store.write_bronze("yfinance", "run-001", df, _DATE)
    p2 = store.write_bronze("yfinance", "run-001", df, _DATE)
    assert p1 == p2
    assert p1.exists()


def test_write_silver(store: MedallionStore) -> None:
    df = pd.DataFrame([{"instrument_id": "AAPL", "value": 182.55}])
    path = store.write_silver(df, "run-001", _DATE, "yfinance")
    assert path.exists()
    loaded = pd.read_parquet(path)
    assert len(loaded) == 1


def test_write_gold(store: MedallionStore) -> None:
    records = [
        GoldenRecord(
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
    ]
    path = store.write_gold(records, "run-001", _DATE)
    assert path.exists()
    df = pd.read_parquet(path)
    assert len(df) == 1
    assert df["instrument_id"].iloc[0] == "AAPL"


def test_write_gold_empty_raises(store: MedallionStore) -> None:
    with pytest.raises(ValueError, match="empty"):
        store.write_gold([], "run-001", _DATE)


def test_write_quarantine(store: MedallionStore) -> None:
    df = pd.DataFrame([{"instrument_id": "AAPL", "value": None}])
    path = store.write_quarantine(df, "null_burst", "run-001", _DATE)
    assert path.exists()
    loaded = pd.read_parquet(path)
    assert "quarantine_reason" in loaded.columns
    assert loaded["quarantine_reason"].iloc[0] == "null_burst"


def test_write_and_read_decision(store: MedallionStore) -> None:
    dec = DecisionRecord(
        agent="dq_agent",
        instrument_id="AAPL",
        business_date=_DATE,
        decision_type=DecisionType.DQ,
        inputs={"value": 182.55},
        outcome={"status": "pass"},
        rule_applied="null_check",
    )
    store.write_decision(dec)
    result = store.get_decision(dec.decision_id)
    assert result is not None
    assert result["instrument_id"] == "AAPL"
    assert result["rule_applied"] == "null_check"
    assert result["verified"] is False


def test_get_nonexistent_decision(store: MedallionStore) -> None:
    assert store.get_decision("does-not-exist") is None


def test_query_decisions(store: MedallionStore) -> None:
    dec = DecisionRecord(
        agent="reconciliation_agent",
        instrument_id="MSFT",
        business_date=_DATE,
        decision_type=DecisionType.RECONCILE,
        inputs={},
        outcome={"golden_value": 380.0},
        rule_applied="quorum",
    )
    store.write_decision(dec)
    df = store.query("SELECT count(*) AS n FROM decisions")
    assert df["n"].iloc[0] >= 1


def test_write_before_open_raises(tmp_path: Path) -> None:
    s = MedallionStore(
        bronze_root=tmp_path / "bronze",
        silver_root=tmp_path / "silver",
        gold_root=tmp_path / "gold",
        quarantine_root=tmp_path / "quarantine",
        lineage_root=tmp_path / "lineage",
        duckdb_path=tmp_path / "mdq.duckdb",
    )
    s.init_dirs()
    dec = DecisionRecord(
        agent="x",
        instrument_id="X",
        business_date=_DATE,
        decision_type=DecisionType.INGEST,
        inputs={},
        outcome={},
        rule_applied="ingest",
    )
    with pytest.raises(RuntimeError, match="not open"):
        s.write_decision(dec)
