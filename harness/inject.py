"""Defect-injection harness for the mdq-mesh test suite (FR-T1).

Seeds known faults into a DataFrame so agents can be verified against controlled inputs.
All injectors accept a deterministic *seed* argument (C-5 determinism in test runs).
"""

from __future__ import annotations

import random
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from typing import Any

import numpy as np
import pandas as pd


class DefectType(StrEnum):
    """Defect scenarios that can be seeded into a Bronze/Silver DataFrame."""

    STALE_FEED = "stale_feed"
    NULL_BURST = "null_burst"
    OUT_OF_RANGE = "out_of_range"
    SCHEMA_DRIFT = "schema_drift"
    UNADJUSTED_SPLIT = "unadjusted_split"
    CROSS_SOURCE_BREAK = "cross_source_break"
    VOLATILITY_REGIME = "volatility_regime"


def inject(
    df: pd.DataFrame,
    defect: DefectType | str,
    seed: int = 42,
    **params: Any,
) -> pd.DataFrame:
    """Return a copy of *df* with a named defect seeded into it.

    Args:
        df:     Source DataFrame (Bronze or Silver canonical format).
        defect: Which defect to inject (see DefectType).
        seed:   RNG seed for reproducible injection (C-5).
        **params: Defect-specific keyword arguments (documented per injector).
    """
    defect = DefectType(defect)
    rng = random.Random(seed)
    np_rng = np.random.default_rng(seed)
    result = df.copy()

    match defect:
        case DefectType.NULL_BURST:
            result = _null_burst(result, rng, **params)
        case DefectType.STALE_FEED:
            result = _stale_feed(result, **params)
        case DefectType.OUT_OF_RANGE:
            result = _out_of_range(result, np_rng, **params)
        case DefectType.SCHEMA_DRIFT:
            result = _schema_drift(result, **params)
        case DefectType.UNADJUSTED_SPLIT:
            result = _unadjusted_split(result, np_rng, **params)
        case DefectType.CROSS_SOURCE_BREAK:
            result = _cross_source_break(result, **params)
        case DefectType.VOLATILITY_REGIME:
            result = _volatility_regime(result, np_rng, **params)

    return result


# ---------------------------------------------------------------------------
# Individual injectors
# ---------------------------------------------------------------------------


def _null_burst(
    df: pd.DataFrame,
    rng: random.Random,
    column: str = "value",
    rate: float = 0.3,
) -> pd.DataFrame:
    """Set *rate* fraction of rows in *column* to NaN."""
    n = max(1, int(len(df) * rate))
    idx = rng.sample(range(len(df)), k=min(n, len(df)))
    df.loc[idx, column] = np.nan
    return df


def _stale_feed(
    df: pd.DataFrame,
    days_stale: int = 3,
) -> pd.DataFrame:
    """Roll fetch/event timestamps back by *days_stale* days."""
    import datetime

    offset = datetime.timedelta(days=days_stale)
    for col in ("fetch_ts", "event_ts"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col]) - offset
    return df


def _out_of_range(
    df: pd.DataFrame,
    np_rng: np.random.Generator,
    column: str = "value",
    multiplier: float = 100.0,
) -> pd.DataFrame:
    """Multiply one random row's value by *multiplier* to produce an outlier."""
    if len(df) == 0:
        return df
    idx = int(np_rng.integers(0, len(df)))
    df.loc[idx, column] = float(df.loc[idx, column]) * multiplier
    return df


def _schema_drift(
    df: pd.DataFrame,
    rename: dict[str, str] | None = None,
    drop: list[str] | None = None,
) -> pd.DataFrame:
    """Simulate schema drift by renaming or dropping columns."""
    if rename:
        df = df.rename(columns=rename)
    if drop:
        df = df.drop(columns=[c for c in drop if c in df.columns])
    return df


def _unadjusted_split(
    df: pd.DataFrame,
    np_rng: np.random.Generator,
    ratio: float = 2.0,
    column: str = "value",
) -> pd.DataFrame:
    """Multiply one row's value by *ratio* to simulate an unadjusted stock split."""
    if len(df) == 0:
        return df
    idx = int(np_rng.integers(0, len(df)))
    df.loc[idx, column] = float(df.loc[idx, column]) * ratio
    return df


def _cross_source_break(
    df: pd.DataFrame,
    column: str = "value",
    shift_pct: float = 0.10,
) -> pd.DataFrame:
    """Shift all values in *column* by *shift_pct* to create a systematic cross-source break."""
    if column in df.columns:
        df[column] = (df[column].astype(float) * (1.0 + shift_pct)).round(4)
    return df


def _volatility_regime(
    df: pd.DataFrame,
    np_rng: np.random.Generator,
    spike_multiplier: float = 3.0,
    column: str = "value",
) -> pd.DataFrame:
    """Multiply all values by *spike_multiplier* — used on the current-day slice of
    a history built with elevated recent volatility so the spike looks within-regime."""
    if column in df.columns:
        df[column] = (df[column].astype(float) * spike_multiplier).round(4)
    return df


# ---------------------------------------------------------------------------
# Multi-day Silver history builder (FR-T1 — anomaly window tests)
# ---------------------------------------------------------------------------

_SILVER_FIELDS = ["OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE", "VOLUME"]
_VOLUME_BASE = 1_000_000.0


def build_silver_history(
    instruments: list[str],
    n_days: int,
    end_date: date,
    base_price: float = 100.0,
    vol_multipliers: dict[int, float] | None = None,
    source_id: str = "yfinance",
    seed: int = 42,
) -> pd.DataFrame:
    """Build synthetic multi-day Silver DataFrame for anomaly rolling-window tests (FR-T1).

    Args:
        instruments:     List of instrument_id / source_symbol strings.
        n_days:          Number of calendar days of history to generate.
        end_date:        Last business date included (inclusive).
        base_price:      Starting price for the random walk.
        vol_multipliers: Mapping of ``offset_from_end → volatility_scale``.
                         E.g. ``{0: 5.0, 1: 4.0, 2: 3.0}`` makes the 3 most
                         recent days 3-5× more volatile (used to set up a regime).
        source_id:       Silver source_id label.
        seed:            RNG seed for C-5 determinism.

    Returns:
        A Silver-schema DataFrame covering *n_days* dates ending at *end_date*.
    """
    np_rng = np.random.default_rng(seed)
    vol_multipliers = vol_multipliers or {}
    dates = [end_date - timedelta(days=i) for i in range(n_days - 1, -1, -1)]
    fetch_ts = datetime(2024, 1, 2, 21, 0, 0, tzinfo=UTC)
    rows: list[dict[str, object]] = []

    for inst in instruments:
        price = base_price
        for i, d in enumerate(dates):
            offset_from_end = n_days - 1 - i
            vol_scale = vol_multipliers.get(offset_from_end, 1.0)
            daily_return = np_rng.normal(0.0, 0.01 * vol_scale)
            price = max(0.01, price * (1.0 + daily_return))
            high = price * (1.0 + abs(np_rng.normal(0.0, 0.005 * vol_scale)))
            low = price * (1.0 - abs(np_rng.normal(0.0, 0.005 * vol_scale)))
            low = min(low, price)
            high = max(high, price)

            field_values: dict[str, float] = {
                "OPEN": round(price * (1.0 + np_rng.normal(0.0, 0.002)), 4),
                "HIGH": round(high, 4),
                "LOW": round(low, 4),
                "CLOSE": round(price, 4),
                "ADJ_CLOSE": round(price, 4),
                "VOLUME": round(_VOLUME_BASE * (1.0 + np_rng.normal(0.0, 0.1)), 0),
            }
            bdate = pd.Timestamp(d)
            for field, val in field_values.items():
                rows.append(
                    {
                        "instrument_id": inst,
                        "business_date": bdate,
                        "field": field,
                        "value": val,
                        "currency": "USD",
                        "source_id": source_id,
                        "source_symbol": inst,
                        "fetch_ts": pd.Timestamp(fetch_ts),
                        "event_ts": pd.Timestamp(fetch_ts),
                        "content_hash": f"hist-{inst}-{d}-{field}",
                        "ca_adjusted": False,
                    }
                )

    return pd.DataFrame(rows)
