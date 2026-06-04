"""Unit tests for the defect-injection harness and frozen fixtures (FR-T1, FR-T2)."""

from __future__ import annotations

import datetime
from pathlib import Path

import pandas as pd
import pytest

import harness.fixtures as fixtures_mod
from harness.fixtures import load_fixture, snapshot
from harness.inject import DefectType, inject

_DT = datetime.datetime(2024, 1, 2, 21, 0, tzinfo=datetime.UTC)


def _sample_df(n: int = 10) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
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
        ]
        * n
    )


# ---------------------------------------------------------------------------
# Injection tests (FR-T1)
# ---------------------------------------------------------------------------


def test_inject_null_burst() -> None:
    df = _sample_df(10)
    result = inject(df, DefectType.NULL_BURST, seed=42, column="value", rate=0.4)
    nulls = result["value"].isna().sum()
    assert nulls >= 1


def test_inject_does_not_mutate_original() -> None:
    df = _sample_df(5)
    _ = inject(df, DefectType.NULL_BURST, seed=42, column="value", rate=0.5)
    assert df["value"].isna().sum() == 0


def test_inject_stale_feed() -> None:
    df = _sample_df(5)
    original_ts = pd.to_datetime(df["fetch_ts"]).iloc[0]
    result = inject(df, DefectType.STALE_FEED, seed=42, days_stale=3)
    new_ts = pd.to_datetime(result["fetch_ts"]).iloc[0]
    assert new_ts < original_ts


def test_inject_out_of_range() -> None:
    df = _sample_df(5)
    result = inject(df, DefectType.OUT_OF_RANGE, seed=42, column="value", multiplier=50.0)
    assert result["value"].max() > df["value"].max() * 10


def test_inject_schema_drift_rename() -> None:
    df = _sample_df(3)
    result = inject(df, DefectType.SCHEMA_DRIFT, seed=42, rename={"value": "close_px"})
    assert "close_px" in result.columns
    assert "value" not in result.columns


def test_inject_schema_drift_drop() -> None:
    df = _sample_df(3)
    result = inject(df, DefectType.SCHEMA_DRIFT, seed=42, drop=["currency"])
    assert "currency" not in result.columns


def test_inject_unadjusted_split() -> None:
    df = _sample_df(5)
    result = inject(df, DefectType.UNADJUSTED_SPLIT, seed=42, ratio=2.0)
    assert result["value"].max() > df["value"].max()


def test_inject_cross_source_break() -> None:
    df = _sample_df(5)
    result = inject(df, DefectType.CROSS_SOURCE_BREAK, seed=42, shift_pct=0.10)
    expected = df["value"].iloc[0] * 1.10
    assert abs(result["value"].iloc[0] - expected) < 0.01


def test_inject_deterministic_with_same_seed() -> None:
    df = _sample_df(10)
    r1 = inject(df, DefectType.NULL_BURST, seed=7, column="value", rate=0.3)
    r2 = inject(df, DefectType.NULL_BURST, seed=7, column="value", rate=0.3)
    assert list(r1["value"].isna()) == list(r2["value"].isna())


# ---------------------------------------------------------------------------
# Frozen-fixture tests (FR-T2)
# ---------------------------------------------------------------------------


def test_snapshot_and_load_roundtrip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(fixtures_mod, "_FIXTURES_DIR", tmp_path)

    df = _sample_df(4)
    snap_path = snapshot("yfinance", df, tag="test")
    assert snap_path.exists()

    loaded = load_fixture("yfinance", tag="test")
    assert len(loaded) == len(df)
    assert list(loaded.columns) == list(df.columns)


def test_load_fixture_missing_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(fixtures_mod, "_FIXTURES_DIR", tmp_path)
    with pytest.raises(FileNotFoundError, match="No fixture"):
        load_fixture("nonexistent_source", tag="missing")
