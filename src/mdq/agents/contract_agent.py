"""Contract Agent — schema validation & normalisation for Bronze → Silver (FR-A2).

Subscribes to INGESTION_COMPLETE. Reads the source's Bronze Parquet, normalises the
wide-format raw data into the canonical long Silver schema, validates with Pandera, and
either writes Silver or quarantines the batch. Every outcome is persisted as a DecisionRecord.

# DESIGN-NOTE: FR-A2 — Pandera lazy=True collects all schema violations in one pass,
# so the CONTRACT_VIOLATION payload contains every drift point, not just the first.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import TYPE_CHECKING

import pandas as pd
import pandera as pa

from mdq.core.agent import Agent
from mdq.core.events import Event, TopicType
from mdq.core.schemas import DecisionRecord, DecisionType, SilverSchema
from mdq.utils.hashing import content_hash
from mdq.utils.logging import get_logger

if TYPE_CHECKING:
    from mdq.core.blackboard import Blackboard
    from mdq.core.config import Config
    from mdq.core.store import MedallionStore

log = get_logger("agents.contract")

# Map raw yfinance column names → canonical FieldType values
_FIELD_RENAME: dict[str, str] = {
    "Open": "OPEN",
    "High": "HIGH",
    "Low": "LOW",
    "Close": "CLOSE",
    "Adj Close": "ADJ_CLOSE",
    "Volume": "VOLUME",
}

# Silver column order must match SilverSchema
_SILVER_COLS: list[str] = [
    "instrument_id",
    "business_date",
    "field",
    "value",
    "currency",
    "source_id",
    "source_symbol",
    "fetch_ts",
    "event_ts",
    "content_hash",
    "ca_adjusted",
]


class ContractAgent(Agent):
    """Normalises Bronze → Silver and validates the Pandera contract (FR-A2)."""

    def __init__(self, bb: Blackboard, store: MedallionStore, cfg: Config) -> None:
        self._bb = bb
        self._store = store
        self._cfg = cfg

    @property
    def name(self) -> str:
        return "contract"

    @property
    def subscriptions(self) -> list[TopicType]:
        return [TopicType.INGESTION_COMPLETE]

    async def act(self, event: Event) -> None:
        source_id: str = event.payload["source_id"]
        business_date = date.fromisoformat(event.payload["business_date"])
        run_id = event.run_id

        bronze_path = self._store.bronze_path(source_id, run_id, business_date)
        try:
            bronze_df = pd.read_parquet(bronze_path)
        except FileNotFoundError:
            log.error("Bronze file not found: %s", bronze_path)
            await self._publish_violation(
                source_id, business_date, run_id, len_bronze=0, errors=["Bronze file not found"]
            )
            return

        # Step 1 — normalise wide Bronze → long Silver format
        try:
            silver_df = _normalise(bronze_df, source_id, business_date)
        except (KeyError, ValueError) as exc:
            log.warning("Normalisation failed (%s): %s", source_id, exc)
            self._store.write_quarantine(bronze_df, "schema_violation", run_id, business_date)
            await self._write_decision(
                source_id,
                business_date,
                inputs={"source_id": source_id, "row_count": len(bronze_df)},
                outcome={"status": "failed", "error": str(exc)},
            )
            await self._publish_violation(
                source_id, business_date, run_id, len(bronze_df), errors=[str(exc)]
            )
            return

        # Step 2 — Pandera schema validation (lazy → collect all errors)
        try:
            SilverSchema.validate(silver_df, lazy=True)
        except pa.errors.SchemaErrors as exc:
            cases = exc.failure_cases.head(20).to_dict(orient="records")
            log.warning("Silver schema violations (%s): %d failure cases", source_id, len(cases))
            self._store.write_quarantine(bronze_df, "schema_violation", run_id, business_date)
            await self._write_decision(
                source_id,
                business_date,
                inputs={"source_id": source_id, "row_count": len(bronze_df)},
                outcome={"status": "failed", "error_count": len(cases), "cases": cases},
            )
            await self._publish_violation(
                source_id, business_date, run_id, len(bronze_df), errors=cases
            )
            return

        # Step 3 — write Silver + lineage record + CONTRACT_PASSED
        self._store.write_silver(silver_df, run_id, business_date, source_id)
        await self._write_decision(
            source_id,
            business_date,
            inputs={"source_id": source_id, "row_count": len(bronze_df)},
            outcome={"status": "passed", "silver_rows": len(silver_df)},
        )
        log.info("CONTRACT_PASSED: %s (%d Silver rows, run=%s)", source_id, len(silver_df), run_id)
        await self._bb.publish(
            Event(
                topic=TopicType.CONTRACT_PASSED,
                agent=self.name,
                run_id=run_id,
                payload={
                    "source_id": source_id,
                    "business_date": business_date.isoformat(),
                    "silver_rows": len(silver_df),
                },
            )
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _publish_violation(
        self,
        source_id: str,
        business_date: date,
        run_id: str,
        len_bronze: int,
        errors: list[object],
    ) -> None:
        await self._bb.publish(
            Event(
                topic=TopicType.CONTRACT_VIOLATION,
                agent=self.name,
                run_id=run_id,
                payload={
                    "source_id": source_id,
                    "business_date": business_date.isoformat(),
                    "bronze_rows": len_bronze,
                    "error_count": len(errors),
                },
            )
        )

    async def _write_decision(
        self,
        source_id: str,
        business_date: date,
        inputs: dict[str, object],
        outcome: dict[str, object],
    ) -> None:
        record = DecisionRecord(
            agent=self.name,
            instrument_id=f"{source_id}:batch",
            business_date=business_date,
            decision_type=DecisionType.CONTRACT,
            inputs=dict(inputs),
            outcome=dict(outcome),
            rule_applied="SilverSchema",
        )
        self._store.write_decision(record)


# ---------------------------------------------------------------------------
# Normalisation — wide Bronze → long canonical Silver (no I/O, pure transform)
# ---------------------------------------------------------------------------


def _normalise(bronze_df: pd.DataFrame, source_id: str, business_date: date) -> pd.DataFrame:
    """Reshape a Bronze wide DataFrame into the canonical Silver long format.

    Raises KeyError if a required Bronze column is missing (signals schema drift).
    """
    id_vars = ["Date", "instrument_id", "source_symbol", "fetch_ts"]
    value_vars = list(_FIELD_RENAME.keys())  # Open, High, Low, Close, Adj Close, Volume

    # KeyError propagates to caller — detected as schema drift (FR-A2)
    for col in id_vars + value_vars:
        if col not in bronze_df.columns:
            raise KeyError(f"Required Bronze column missing: {col!r}")

    long_df = bronze_df.melt(
        id_vars=id_vars,
        value_vars=value_vars,
        var_name="field",
        value_name="value",
    )

    long_df["field"] = long_df["field"].map(_FIELD_RENAME)
    long_df["business_date"] = pd.to_datetime(long_df["Date"])
    long_df["currency"] = "USD"
    # DESIGN-NOTE: C-1 — all Phase 1 instruments (AAPL, MSFT, NVDA, JPM, SPY) are
    # USD-denominated equities. Multi-currency support added in Phase 7 via ECB FX rates.
    long_df["source_id"] = source_id
    long_df["ca_adjusted"] = False
    long_df["event_ts"] = pd.Timestamp(datetime.now(tz=UTC))
    long_df["fetch_ts"] = pd.to_datetime(long_df["fetch_ts"])
    long_df["value"] = pd.to_numeric(long_df["value"], errors="coerce")
    long_df["content_hash"] = long_df.apply(_row_hash, axis=1)

    return long_df[_SILVER_COLS].reset_index(drop=True)


def _row_hash(row: pd.Series) -> str:
    return content_hash(
        {
            "instrument_id": str(row["instrument_id"]),
            "business_date": str(row["business_date"]),
            "field": str(row["field"]),
            "value": str(row["value"]),
            "source_id": str(row["source_id"]),
        }
    )
