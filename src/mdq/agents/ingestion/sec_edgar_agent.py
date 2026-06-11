"""SEC EDGAR Company-Facts Reference Data Source Agent (FR-A1, Phase 7).

Subscribes to RUN_STARTED. Fetches the most recent shares-outstanding figure for each
universe instrument from the SEC EDGAR XBRL company-facts API (keyless — C-1; requires
a descriptive User-Agent header per SEC fair-access policy). Writes Bronze Parquet,
normalises to Silver (field=SHARES), and publishes REFERENCE_DATA_COMPLETE.

# DESIGN-NOTE: KPI-7 — same isolation strategy as ECBAgent: publishes REFERENCE_DATA_COMPLETE
# instead of INGESTION_COMPLETE so ContractAgent and ReconciliationAgent are untouched.
# DESIGN-NOTE: C-1 — SEC EDGAR requires only a descriptive User-Agent header (not a key).
# The user_agent string is sourced from config (cfg.sources.sec_edgar.user_agent), never
# hard-coded (C-4). If the header is absent or empty, SEC may throttle; the agent will
# publish INGESTION_FAILED after retries (fail fast and loud — NFR-9 spirit).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

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

log = get_logger("agents.sec_edgar")

_SOURCE_ID = "sec_edgar"

# DESIGN-NOTE: C-1 — SEC EDGAR company facts API; User-Agent required per SEC policy, NOT a key.
_EDGAR_URL_TMPL = "https://data.sec.gov/api/xbrl/companyfacts/{cik}.json"
_SHARES_CONCEPT = "CommonStockSharesOutstanding"


class SECEdgarAgent(Agent):
    """Fetches shares-outstanding from SEC EDGAR (or frozen fixture) and lands in Bronze (FR-A1)."""

    def __init__(self, bb: Blackboard, store: MedallionStore, cfg: Config) -> None:
        self._bb = bb
        self._store = store
        self._cfg = cfg
        # CIK string → instrument_id mapping built from universe config
        self._cik_map: dict[str, str] = {
            inst.symbols[_SOURCE_ID]: inst.instrument_id
            for inst in cfg.universe.instruments
            if _SOURCE_ID in inst.symbols
        }

    @property
    def name(self) -> str:
        return "sec_edgar_source"

    @property
    def subscriptions(self) -> list[TopicType]:
        return [TopicType.RUN_STARTED]

    async def act(self, event: Event) -> None:
        run_id = event.run_id
        business_date = date.fromisoformat(event.payload["business_date"])

        try:
            df = await self._load(business_date)
        except Exception as exc:
            log.error("SEC EDGAR ingestion failed for %s: %s", business_date, exc)
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
                "sec_edgar: no instruments with CIK configured — publishing REFERENCE_DATA_COMPLETE "
                "with row_count=0 (universe is empty)"
            )
            await self._bb.publish(
                Event(
                    topic=TopicType.REFERENCE_DATA_COMPLETE,
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
        log.info("SEC EDGAR Bronze written: %s (%d rows)", dest, len(df))

        silver_df = _normalise_sec(df, business_date)
        self._store.write_silver(silver_df, run_id, business_date, _SOURCE_ID)
        log.info("SEC EDGAR Silver written: %d rows", len(silver_df))

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

        if not self._cik_map:
            return pd.DataFrame()

        user_agent = self._cfg.sources.sec_edgar.user_agent or "mdq-mesh research"
        src_cfg = self._cfg.sources.sec_edgar
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            _fetch_blocking,
            self._cik_map,
            user_agent,
            business_date,
            src_cfg.retries,
            src_cfg.backoff_seconds,
        )


# ---------------------------------------------------------------------------
# Normalisation (pure, deterministic — C-4)
# ---------------------------------------------------------------------------


def _normalise_sec(bronze_df: pd.DataFrame, business_date: date) -> pd.DataFrame:
    """Reshape SEC EDGAR Bronze into canonical Silver long format.

    # DESIGN-NOTE: C-4 — pure function; identical inputs → identical output.
    # field=SHARES carries shares outstanding; currency="shares" is a unit marker
    # (shares counts are dimensionless, so ISO 4217 does not apply — use "shares"
    # as a descriptive unit string to satisfy the non-null Silver schema requirement).
    """
    rows = []
    event_ts = pd.Timestamp(datetime.now(tz=UTC))
    for _, row in bronze_df.iterrows():
        payload = {
            "instrument_id": str(row["instrument_id"]),
            "business_date": str(row["Date"]),
            "field": FieldType.SHARES,
            "value": float(row["value"]),
            "source_id": _SOURCE_ID,
        }
        ch = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
        rows.append(
            {
                "instrument_id": str(row["instrument_id"]),
                "business_date": pd.Timestamp(business_date),
                "field": FieldType.SHARES,
                "value": float(row["value"]),
                "currency": "shares",
                "source_id": _SOURCE_ID,
                "source_symbol": str(row["cik"]),
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
    cik_map: dict[str, str],
    user_agent: str,
    business_date: date,
    retries: int,
    backoff_seconds: float,
) -> pd.DataFrame:
    """Fetch shares outstanding from SEC EDGAR for each CIK."""
    rows: list[dict[str, Any]] = []
    fetch_ts = datetime.now(tz=UTC)
    headers = {"User-Agent": user_agent}

    for cik, instrument_id in cik_map.items():
        facts = _fetch_company_facts(cik, headers, retries, backoff_seconds)
        shares = _extract_latest_shares(facts, cik)
        if shares is None:
            log.warning(
                "No %s found in SEC EDGAR for CIK %s (%s)",
                _SHARES_CONCEPT,
                cik,
                instrument_id,
            )
            continue
        rows.append(
            {
                "Date": pd.Timestamp(business_date),
                "instrument_id": instrument_id,
                "cik": cik,
                "metric": _SHARES_CONCEPT,
                "value": float(shares["val"]),
                "unit": "shares",
                "filed_date": str(shares.get("filed", "")),
                "fetch_ts": fetch_ts,
            }
        )

    if not rows:
        raise ValueError(f"No SEC EDGAR Bronze rows produced for CIKs {list(cik_map.keys())}")

    return pd.DataFrame(rows)


def _fetch_company_facts(
    cik: str,
    headers: dict[str, str],
    retries: int,
    backoff_seconds: float,
) -> dict[str, Any]:
    """Fetch company facts JSON from SEC EDGAR; retry with linear backoff."""
    url = _EDGAR_URL_TMPL.format(cik=cik)
    last_exc: Exception = RuntimeError("unreachable")

    for attempt in range(max(1, retries)):
        try:
            resp = httpx.get(url, headers=headers, follow_redirects=True, timeout=20.0)
            resp.raise_for_status()
            return resp.json()  # type: ignore[no-any-return]
        except Exception as exc:
            last_exc = exc
            if attempt < retries - 1:
                time.sleep(backoff_seconds * (attempt + 1))

    raise last_exc


def _extract_latest_shares(facts: dict[str, Any], cik: str) -> dict[str, Any] | None:
    """Extract the most recently filed shares-outstanding entry from company facts."""
    try:
        units = facts["facts"]["us-gaap"][_SHARES_CONCEPT]["units"]["shares"]
    except (KeyError, TypeError):
        return None

    # Filter to annual (10-K) or quarterly (10-Q) filings; pick most recent by end-date.
    # DESIGN-NOTE: some entries are duplicated across amendments; we pick latest by `end`.
    candidates = [u for u in units if u.get("form") in {"10-K", "10-Q"}]
    if not candidates:
        # Fall back to any available entry
        candidates = list(units)
    if not candidates:
        return None

    result: dict[str, Any] = max(candidates, key=lambda u: u.get("end", ""))
    return result
