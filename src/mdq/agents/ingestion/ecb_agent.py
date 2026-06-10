"""ECB SDMX FX Reference Rate Source Agent (FR-A1, Phase 7).

Subscribes to RUN_STARTED. Fetches daily FX reference rates from the European Central
Bank SDMX REST API (keyless — C-1), writes Bronze Parquet, normalises to Silver, and
publishes REFERENCE_DATA_COMPLETE (not INGESTION_COMPLETE) so the price reconciliation
pipeline (ContractAgent / DQAgent / ReconciliationAgent) is unaffected (KPI-7).

# DESIGN-NOTE: KPI-7 — reference-data agents publish REFERENCE_DATA_COMPLETE instead of
# INGESTION_COMPLETE so ContractAgent (which subscribes only to INGESTION_COMPLETE) never
# attempts OHLCAV normalisation on FX data, and ReconciliationAgent's hardcoded
# _enabled_sources={'yfinance','stooq'} is never touched. Zero changes to core required.
# DESIGN-NOTE: C-1 — ECB SDMX REST endpoint is genuinely keyless. The User-Agent header
# is descriptive only, not a credential.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from datetime import UTC, date, datetime
from io import StringIO
from typing import TYPE_CHECKING

import httpx
import pandas as pd

from mdq.core.agent import Agent
from mdq.core.events import Event, TopicType
from mdq.core.schemas import FieldType, SilverSchema
from mdq.utils.logging import get_logger

if TYPE_CHECKING:
    from mdq.core.blackboard import Blackboard
    from mdq.core.config import Config
    from mdq.core.store import MedallionStore

log = get_logger("agents.ecb")

_SOURCE_ID = "ecb"

# DESIGN-NOTE: C-1 — ECB SDMX REST API; no authentication required.
# lastNObservations=1 fetches only the most recent available rate.
_ECB_URL_TMPL = (
    "https://data-api.ecb.europa.eu/service/data/EXR/"
    "D.{currency}.EUR.SP00.A?lastNObservations=1&format=csvdata"
)
_HEADERS = {"User-Agent": "mdq-mesh research (educational use)"}


class ECBAgent(Agent):
    """Fetches EUR-based FX reference rates from ECB SDMX (or frozen fixture) (FR-A1)."""

    def __init__(self, bb: Blackboard, store: MedallionStore, cfg: Config) -> None:
        self._bb = bb
        self._store = store
        self._cfg = cfg
        # source_symbol (ECB target-currency code, e.g. "USD") → instrument_id (e.g. "EUR/USD")
        self._symbol_map: dict[str, str] = {
            inst.symbols[_SOURCE_ID]: inst.instrument_id
            for inst in cfg.universe.instruments
            if _SOURCE_ID in inst.symbols
        }

    @property
    def name(self) -> str:
        return "ecb_source"

    @property
    def subscriptions(self) -> list[TopicType]:
        return [TopicType.RUN_STARTED]

    async def act(self, event: Event) -> None:
        run_id = event.run_id
        business_date = date.fromisoformat(event.payload["business_date"])

        try:
            df = await self._load(business_date)
        except Exception as exc:
            log.error("ECB ingestion failed for %s: %s", business_date, exc)
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
        log.info("ECB Bronze written: %s (%d rows)", dest, len(df))

        silver_df = _normalise_ecb(df, business_date)
        self._store.write_silver(silver_df, run_id, business_date, _SOURCE_ID)
        log.info("ECB Silver written: %d rows", len(silver_df))

        await self._bb.publish(
            Event(
                topic=TopicType.REFERENCE_DATA_COMPLETE,
                agent=self.name,
                run_id=run_id,
                payload={
                    "source_id": _SOURCE_ID,
                    "business_date": business_date.isoformat(),
                    "row_count": len(silver_df),
                },
            )
        )

    async def _load(self, business_date: date) -> pd.DataFrame:
        if self._cfg.runtime.use_fixtures:
            # FR-T2: load frozen fixture so tests run fully offline
            from harness.fixtures import load_fixture

            return load_fixture(_SOURCE_ID)

        currencies = list(self._symbol_map.keys())
        src_cfg = self._cfg.sources.ecb
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            _fetch_blocking,
            currencies,
            self._symbol_map,
            business_date,
            src_cfg.retries,
            src_cfg.backoff_seconds,
        )


# ---------------------------------------------------------------------------
# Normalisation (pure, deterministic — C-4)
# ---------------------------------------------------------------------------


def _normalise_ecb(bronze_df: pd.DataFrame, business_date: date) -> pd.DataFrame:
    """Reshape ECB Bronze into canonical Silver long format.

    # DESIGN-NOTE: C-4 — pure function; identical inputs → identical output.
    # field="CLOSE" is used for the daily FX reference rate (the single published
    # value per currency pair per day). No OHLCAV structure exists in FX reference data.
    """
    rows = []
    event_ts = pd.Timestamp(datetime.now(tz=UTC))
    for _, row in bronze_df.iterrows():
        payload = {
            "instrument_id": str(row["instrument_id"]),
            "business_date": str(row["Date"]),
            "field": FieldType.CLOSE,
            "value": float(row["rate"]),
            "source_id": _SOURCE_ID,
        }
        ch = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
        rows.append(
            {
                "instrument_id": str(row["instrument_id"]),
                "business_date": pd.Timestamp(business_date),
                "field": FieldType.CLOSE,
                "value": float(row["rate"]),
                "currency": str(row["currency"]),
                "source_id": _SOURCE_ID,
                "source_symbol": str(row["source_symbol"]),
                "fetch_ts": pd.to_datetime(row["fetch_ts"]),
                "event_ts": event_ts,
                "content_hash": ch,
                "ca_adjusted": False,
            }
        )
    silver_df = pd.DataFrame(rows)
    SilverSchema.validate(silver_df, lazy=True)
    return silver_df


# ---------------------------------------------------------------------------
# Blocking fetch helpers (executed in a thread pool — must NOT use asyncio)
# ---------------------------------------------------------------------------


def _fetch_blocking(
    currencies: list[str],
    symbol_map: dict[str, str],
    business_date: date,
    retries: int,
    backoff_seconds: float,
) -> pd.DataFrame:
    """Fetch EUR-based FX rates from ECB SDMX REST for each currency code."""
    rows: list[dict[str, object]] = []
    fetch_ts = datetime.now(tz=UTC)

    for currency in currencies:
        raw = _download_with_retry(currency, retries, backoff_seconds)
        if raw.empty:
            raise ValueError(f"ECB returned no data for EUR/{currency!r}")

        obs_value = float(raw["OBS_VALUE"].iloc[-1])
        rows.append(
            {
                "Date": pd.Timestamp(business_date),
                "instrument_id": symbol_map[currency],
                "source_symbol": currency,
                "rate": obs_value,
                "currency": currency,
                "fetch_ts": fetch_ts,
            }
        )

    if not rows:
        raise ValueError(f"No ECB Bronze rows produced for currencies {currencies}")

    return pd.DataFrame(rows)


def _download_with_retry(
    currency: str,
    retries: int,
    backoff_seconds: float,
) -> pd.DataFrame:
    """Fetch ECB SDMX CSV for one EUR/X currency pair; retry with linear backoff."""
    url = _ECB_URL_TMPL.format(currency=currency)
    last_exc: Exception = RuntimeError("unreachable")

    for attempt in range(max(1, retries)):
        try:
            resp = httpx.get(url, headers=_HEADERS, follow_redirects=True, timeout=15.0)
            resp.raise_for_status()
            df = pd.read_csv(StringIO(resp.text))
            if df.empty or "OBS_VALUE" not in df.columns:
                raise ValueError(f"Empty or malformed ECB CSV for EUR/{currency!r}")
            return df
        except Exception as exc:
            last_exc = exc
            if attempt < retries - 1:
                time.sleep(backoff_seconds * (attempt + 1))

    raise last_exc
