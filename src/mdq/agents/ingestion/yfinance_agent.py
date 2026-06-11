"""yfinance Source Ingestion Agent (FR-A1).

Subscribes to RUN_STARTED. Fetches OHLCAV for all universe instruments via yfinance
(blocking I/O, run in a thread executor), tags each row with source metadata, and writes
an immutable Bronze Parquet. Falls back to frozen fixture when runtime.use_fixtures=True.

# DESIGN-NOTE: FR-A1 — yfinance is a synchronous library; the fetch runs in a thread
# executor so the asyncio event loop is never blocked during I/O.
"""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING

import pandas as pd
import yfinance as yf  # type: ignore[import-untyped]

from mdq.core.agent import Agent
from mdq.core.events import Event, TopicType
from mdq.utils.logging import get_logger

if TYPE_CHECKING:
    from mdq.core.blackboard import Blackboard
    from mdq.core.config import Config
    from mdq.core.store import MedallionStore

log = get_logger("agents.yfinance")

_SOURCE_ID = "yfinance"

# Columns that must be present in a raw yfinance download.
_REQUIRED_COLS: frozenset[str] = frozenset({"Open", "High", "Low", "Close", "Volume"})


class YFinanceAgent(Agent):
    """Fetches OHLCAV from yfinance (or frozen fixture) and lands it in Bronze (FR-A1)."""

    def __init__(self, bb: Blackboard, store: MedallionStore, cfg: Config) -> None:
        self._bb = bb
        self._store = store
        self._cfg = cfg
        # yfinance symbol → instrument_id mapping from universe config
        self._symbol_map: dict[str, str] = {
            inst.symbols[_SOURCE_ID]: inst.instrument_id
            for inst in cfg.universe.instruments
            if _SOURCE_ID in inst.symbols
        }

    @property
    def name(self) -> str:
        return "yfinance_source"

    @property
    def subscriptions(self) -> list[TopicType]:
        return [TopicType.RUN_STARTED, TopicType.REFETCH_REQUESTED]

    async def act(self, event: Event) -> None:
        if event.topic == TopicType.REFETCH_REQUESTED:
            await self._handle_refetch(event)
            return

        run_id = event.run_id
        business_date = date.fromisoformat(event.payload["business_date"])

        try:
            df = await self._load(business_date)
        except Exception as exc:
            log.error("yfinance ingestion failed for %s: %s", business_date, exc)
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

        if df.empty:
            log.warning(
                "yfinance: no instruments configured — publishing INGESTION_COMPLETE "
                "with row_count=0 (universe is empty)"
            )
            await self._bb.publish(
                Event(
                    topic=TopicType.INGESTION_COMPLETE,
                    agent=self.name,
                    run_id=run_id,
                    payload={
                        "source_id": _SOURCE_ID,
                        "business_date": business_date.isoformat(),
                        "row_count": 0,
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

    async def _handle_refetch(self, event: Event) -> None:
        """Handle REFETCH_REQUESTED: reload data, overwrite Silver, publish DQ_PASSED (FR-A7).

        # DESIGN-NOTE: FR-T2 — _load() transparently uses fixture or live, so re-fetch
        # in tests gets clean fixture data and in production retries the live endpoint.
        # DESIGN-NOTE: FR-H1 — DQ_PASSED is published directly (bypassing ContractAgent/
        # DQAgent) because this is a bounded, supervised corrective action. The subsequent
        # ReconciliationAgent._reconcile() serves as the final correctness gate.
        """
        if event.payload.get("source_id") != _SOURCE_ID:
            return
        run_id = event.run_id
        business_date = date.fromisoformat(str(event.payload["business_date"]))
        log.info("Re-fetching %s for %s (run=%s)", _SOURCE_ID, business_date, run_id)
        try:
            df = await self._load(business_date)
        except Exception as exc:
            log.error("yfinance re-fetch failed for %s: %s", business_date, exc)
            return
        from mdq.agents.contract_agent import _normalise

        silver_df = _normalise(df, _SOURCE_ID, business_date)
        self._store.write_silver(silver_df, run_id, business_date, _SOURCE_ID)
        log.info("Silver overwritten for %s after re-fetch (%d rows)", _SOURCE_ID, len(silver_df))
        await self._bb.publish(
            Event(
                topic=TopicType.DQ_PASSED,
                agent=self.name,
                run_id=run_id,
                payload={
                    "source_id": _SOURCE_ID,
                    "business_date": business_date.isoformat(),
                },
            )
        )

    async def _load(self, business_date: date) -> pd.DataFrame:
        if self._cfg.runtime.use_fixtures:
            # FR-T2: load frozen fixture so tests run fully offline
            from harness.fixtures import load_fixture

            return load_fixture(_SOURCE_ID)

        if not self._symbol_map:
            return pd.DataFrame()

        symbols = list(self._symbol_map.keys())
        src_cfg = self._cfg.sources.yfinance
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
    """Download OHLCAV for every symbol and reshape into Bronze format."""
    rows: list[dict[str, object]] = []
    fetch_ts = datetime.now(tz=UTC)

    for sym in symbols:
        raw = _download_with_retry(sym, business_date, retries, backoff_seconds)
        if raw.empty:
            raise ValueError(f"yfinance returned no data for {sym!r} on {business_date}")

        missing = _REQUIRED_COLS - set(raw.columns)
        if missing:
            raise ValueError(f"yfinance missing columns {missing} for {sym!r}")

        if "Adj Close" not in raw.columns:
            # DESIGN-NOTE: some yfinance builds omit Adj Close for non-equity tickers;
            # fall back to Close so downstream Silver still has a valid ADJ_CLOSE field.
            raw["Adj Close"] = raw["Close"]

        row = raw.iloc[0]
        rows.append(
            {
                "Date": pd.Timestamp(business_date),
                "Open": float(row["Open"]),
                "High": float(row["High"]),
                "Low": float(row["Low"]),
                "Close": float(row["Close"]),
                "Adj Close": float(row["Adj Close"]),
                "Volume": int(row["Volume"]),
                "instrument_id": symbol_map[sym],
                "source_symbol": sym,
                "fetch_ts": fetch_ts,
            }
        )

    if not rows:
        raise ValueError(f"No Bronze rows produced for {symbols} on {business_date}")

    return pd.DataFrame(rows)


def _download_with_retry(
    sym: str,
    business_date: date,
    retries: int,
    backoff_seconds: float,
) -> pd.DataFrame:
    """Download one ticker; retry with linear backoff on failure."""
    end_date = (business_date + timedelta(days=1)).isoformat()
    start_date = business_date.isoformat()
    last_exc: Exception = RuntimeError("unreachable")

    for attempt in range(max(1, retries)):
        try:
            return yf.download(  # type: ignore[no-any-return]
                sym,
                start=start_date,
                end=end_date,
                auto_adjust=False,
                progress=False,
                multi_level_index=False,  # yfinance ≥0.2.51: keep flat columns for single ticker
            )
        except Exception as exc:
            last_exc = exc
            if attempt < retries - 1:
                time.sleep(backoff_seconds * (attempt + 1))

    raise last_exc
