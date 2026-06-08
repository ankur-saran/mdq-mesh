"""Stooq Source Ingestion Agent (FR-A1).

Subscribes to RUN_STARTED. Fetches OHLCV for all universe instruments via direct
HTTP CSV download from Stooq (blocking I/O, run in a thread executor), tags each
row with source metadata, and writes an immutable Bronze Parquet. Falls back to
frozen fixture when runtime.use_fixtures=True.

# DESIGN-NOTE: FR-A1 — Stooq is the second independent price source (no API key,
# pure HTTP CSV endpoint accessed via httpx). The fetch runs in a thread executor so
# the asyncio event loop is never blocked during I/O (same pattern as yfinance_agent).
# DESIGN-NOTE: C-1 — Stooq CSV endpoint requires no key; User-Agent is descriptive only.
"""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING

import httpx
import pandas as pd

from mdq.core.agent import Agent
from mdq.core.events import Event, TopicType
from mdq.utils.logging import get_logger

if TYPE_CHECKING:
    from mdq.core.blackboard import Blackboard
    from mdq.core.config import Config
    from mdq.core.store import MedallionStore

log = get_logger("agents.stooq")

_SOURCE_ID = "stooq"

_REQUIRED_COLS: frozenset[str] = frozenset({"Open", "High", "Low", "Close", "Volume"})


class StooqAgent(Agent):
    """Fetches OHLCV from Stooq (or frozen fixture) and lands it in Bronze (FR-A1)."""

    def __init__(self, bb: Blackboard, store: MedallionStore, cfg: Config) -> None:
        self._bb = bb
        self._store = store
        self._cfg = cfg
        self._symbol_map: dict[str, str] = {
            inst.symbols[_SOURCE_ID]: inst.instrument_id
            for inst in cfg.universe.instruments
            if _SOURCE_ID in inst.symbols
        }

    @property
    def name(self) -> str:
        return "stooq_source"

    @property
    def subscriptions(self) -> list[TopicType]:
        return [TopicType.RUN_STARTED]

    async def act(self, event: Event) -> None:
        run_id = event.run_id
        business_date = date.fromisoformat(event.payload["business_date"])

        try:
            df = await self._load(business_date)
        except Exception as exc:
            log.error("Stooq ingestion failed for %s: %s", business_date, exc)
            await self._bb.publish(
                Event(
                    topic=TopicType.INGESTION_FAILED,
                    agent=self.name,
                    run_id=run_id,
                    payload={
                        "source_id": _SOURCE_ID,
                        "business_date": business_date.isoformat(),
                        "error": str(exc),
                    },
                )
            )
            return

        dest = self._store.write_bronze(_SOURCE_ID, run_id, df, business_date)
        log.info("Bronze written: %s (%d rows)", dest, len(df))
        await self._bb.publish(
            Event(
                topic=TopicType.INGESTION_COMPLETE,
                agent=self.name,
                run_id=run_id,
                payload={
                    "source_id": _SOURCE_ID,
                    "business_date": business_date.isoformat(),
                    "row_count": len(df),
                },
            )
        )

    async def _load(self, business_date: date) -> pd.DataFrame:
        if self._cfg.runtime.use_fixtures:
            # FR-T2: load frozen fixture so tests run fully offline
            from harness.fixtures import load_fixture

            return load_fixture(_SOURCE_ID)

        symbols = list(self._symbol_map.keys())
        src_cfg = self._cfg.sources.stooq
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            _fetch_blocking,
            symbols,
            self._symbol_map,
            business_date,
            src_cfg.retries,
            src_cfg.backoff_seconds,
        )


# ---------------------------------------------------------------------------
# Blocking fetch helpers (executed in a thread pool — must NOT use asyncio)
# ---------------------------------------------------------------------------


def _fetch_blocking(
    symbols: list[str],
    symbol_map: dict[str, str],
    business_date: date,
    retries: int,
    backoff_seconds: float,
) -> pd.DataFrame:
    """Download OHLCV for every symbol and reshape into Bronze format."""
    rows: list[dict[str, object]] = []
    fetch_ts = datetime.now(tz=UTC)

    for sym in symbols:
        raw = _download_with_retry(sym, business_date, retries, backoff_seconds)
        if raw.empty:
            raise ValueError(f"Stooq returned no data for {sym!r} on {business_date}")

        missing = _REQUIRED_COLS - set(raw.columns)
        if missing:
            raise ValueError(f"Stooq missing columns {missing} for {sym!r}")

        # DESIGN-NOTE: FR-A1 — Stooq does not provide an adjusted-close column via its
        # standard CSV endpoint. We fall back to Close so Silver always has ADJ_CLOSE.
        if "Adj Close" not in raw.columns:
            adj_close = float(raw["Close"].iloc[0])
        else:
            adj_close = float(raw["Adj Close"].iloc[0])

        rows.append(
            {
                "Date": pd.Timestamp(business_date),
                "Open": float(raw["Open"].iloc[0]),
                "High": float(raw["High"].iloc[0]),
                "Low": float(raw["Low"].iloc[0]),
                "Close": float(raw["Close"].iloc[0]),
                "Adj Close": adj_close,
                "Volume": int(raw["Volume"].iloc[0]),
                "instrument_id": symbol_map[sym],
                "source_symbol": sym,
                "fetch_ts": fetch_ts,
            }
        )

    if not rows:
        raise ValueError(f"No Bronze rows produced for {symbols} on {business_date}")

    return pd.DataFrame(rows)


_STOOQ_BASE = "https://stooq.com/q/d/l/"
# DESIGN-NOTE: C-1 — Stooq CSV endpoint requires no API key; User-Agent is descriptive only.
_HEADERS = {"User-Agent": "mdq-mesh research (educational use)"}


def _download_with_retry(
    sym: str,
    business_date: date,
    retries: int,
    backoff_seconds: float,
) -> pd.DataFrame:
    """Fetch EOD CSV from Stooq for one symbol; retry with linear backoff on failure.

    URL pattern: https://stooq.com/q/d/l/?s={sym}&d1={YYYYMMDD}&d2={YYYYMMDD}&i=d
    Returns a DataFrame with columns: Date, Open, High, Low, Close, Volume.
    """
    d = business_date.strftime("%Y%m%d")
    url = f"{_STOOQ_BASE}?s={sym}&d1={d}&d2={d}&i=d"
    last_exc: Exception = RuntimeError("unreachable")

    for attempt in range(max(1, retries)):
        try:
            resp = httpx.get(url, headers=_HEADERS, follow_redirects=True, timeout=15.0)
            resp.raise_for_status()
            from io import StringIO

            df = pd.read_csv(StringIO(resp.text))
            if df.empty or "Date" not in df.columns:
                raise ValueError(f"Empty or malformed CSV for {sym!r}")
            return df
        except Exception as exc:
            last_exc = exc
            if attempt < retries - 1:
                time.sleep(backoff_seconds * (attempt + 1))

    raise last_exc
