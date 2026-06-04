"""Defect-injection harness for the mdq-mesh test suite (FR-T1).

Seeds known faults into a DataFrame so agents can be verified against controlled inputs.
All injectors accept a deterministic *seed* argument (C-5 determinism in test runs).
"""

from __future__ import annotations

import random
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
