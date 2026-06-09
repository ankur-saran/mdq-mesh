"""Reconciliation Agent — cross-source quorum vote and golden-value election (FR-A6).

Subscribes to DQ_PASSED, DQ_FAILURE, and INGESTION_FAILED. Accumulates source outcomes
per run; once all enabled sources have reported, applies per-field tolerance bands and a
quorum vote to elect a golden value for each (instrument_id, field). Writes GoldenRecords
to Gold and publishes RECONCILIATION_COMPLETE or BREAK_DETECTED.

# DESIGN-NOTE: FR-A6 — ReconciliationAgent accumulates events in self._completed until
# all enabled sources have reported (DQ_PASSED, DQ_FAILURE, or INGESTION_FAILED). This
# prevents partial-quorum spurious reconciliation in the asyncio drain loop (C-5).
# DESIGN-NOTE: FR-R3/R5 — _elect() and _within_tolerance() are pure functions, fully
# deterministic on identical inputs (C-4, C-5). All tolerances from config (never hard-coded).
# DESIGN-NOTE: NFR-6 — sources with DQ_FAILURE or INGESTION_FAILED are excluded from
# reconciliation but do not abort the run. Remaining quorum proceeds if min_agreeing_sources met.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import TYPE_CHECKING

import pandas as pd

from mdq.core.agent import Agent
from mdq.core.events import Event, TopicType
from mdq.core.schemas import Confidence, DecisionRecord, DecisionType, GoldenRecord
from mdq.utils.logging import get_logger

if TYPE_CHECKING:
    from mdq.core.blackboard import Blackboard
    from mdq.core.config import Config, ReconciliationConfig, ToleranceConfig
    from mdq.core.store import MedallionStore

log = get_logger("agents.reconciliation")


class ReconciliationAgent(Agent):
    """Elects golden values by quorum across source Silver lanes (FR-A6)."""

    def __init__(self, bb: Blackboard, store: MedallionStore, cfg: Config) -> None:
        self._bb = bb
        self._store = store
        self._cfg = cfg
        # Sources that are enabled and will be tracked
        self._enabled_sources: frozenset[str] = frozenset(
            src
            for src, src_cfg in [
                ("yfinance", cfg.sources.yfinance),
                ("stooq", cfg.sources.stooq),
            ]
            if src_cfg.enabled
        )
        # {run_id: {source_id: True (DQ_PASSED) | False (failed/excluded)}}
        self._completed: dict[str, dict[str, bool]] = {}
        # {run_id: business_date} — captured from first event received per run
        self._bdates: dict[str, date] = {}

    @property
    def name(self) -> str:
        return "reconciliation"

    @property
    def subscriptions(self) -> list[TopicType]:
        return [TopicType.DQ_PASSED, TopicType.DQ_FAILURE, TopicType.INGESTION_FAILED]

    async def act(self, event: Event) -> None:
        source_id: str = event.payload.get("source_id", "")
        if not source_id or source_id not in self._enabled_sources:
            return

        run_id = event.run_id
        bdate_str: str = event.payload.get("business_date", "")
        if not bdate_str:
            return
        business_date = date.fromisoformat(bdate_str)

        if run_id not in self._completed:
            self._completed[run_id] = {}
        if run_id not in self._bdates:
            self._bdates[run_id] = business_date

        # Silver exists for DQ_PASSED and DQ_FAILURE (ContractAgent wrote it in both cases).
        # Only INGESTION_FAILED means no Silver was produced at all.
        # DESIGN-NOTE: NFR-6 — DQ_FAILURE with only medium-severity issues (monotonicity,
        # duplicates) should not prevent reconciliation; the Silver is still usable data.
        self._completed[run_id][source_id] = event.topic != TopicType.INGESTION_FAILED

        # Fire reconciliation only once, when all enabled sources have reported
        if self._enabled_sources <= set(self._completed[run_id]):
            bd = self._bdates.pop(run_id, business_date)
            completed_snapshot = self._completed.pop(run_id)
            await self._reconcile(run_id, bd, completed_snapshot)

    async def _reconcile(
        self,
        run_id: str,
        business_date: date,
        completed: dict[str, bool],
    ) -> None:
        """Load Silver per passing source and run the quorum election."""
        passing_sources = [src for src, passed in completed.items() if passed]
        rec_cfg = self._cfg.reconciliation

        # Load Silver for each DQ_PASSED source
        source_silvers: dict[str, pd.DataFrame] = {}
        for src in passing_sources:
            p = self._store.silver_path(run_id, business_date, src)
            try:
                source_silvers[src] = pd.read_parquet(p)
            except FileNotFoundError:
                log.warning("Silver file missing for reconciliation: %s", p)

        if not source_silvers:
            log.warning("No usable Silver data for reconciliation on %s", business_date)
            return

        # Build pivot: {(instrument_id, field): {source_id: value}}
        pivot: dict[tuple[str, str], dict[str, float]] = {}
        for src, sdf in source_silvers.items():
            for _, row in sdf.iterrows():
                key = (str(row["instrument_id"]), str(row["field"]))
                if pd.isna(row["value"]):
                    continue
                if key not in pivot:
                    pivot[key] = {}
                pivot[key][src] = float(row["value"])

        golden_records: list[GoldenRecord] = []
        breaks: list[dict[str, object]] = []

        for (instrument_id, field), source_values in pivot.items():
            golden_val, quorum_srcs, dissenting_srcs, confidence = _elect(
                source_values, field, rec_cfg
            )

            if golden_val is None:
                # No quorum — record break and quarantine
                log.warning(
                    "BREAK: %s %s — sources %s disagree beyond tolerance",
                    instrument_id,
                    field,
                    list(source_values.keys()),
                )
                tol = rec_cfg.tolerances.get(field)
                tol_band = _tol_band(field, tol)
                break_info: dict[str, object] = {
                    "instrument_id": instrument_id,
                    "field": field,
                    "business_date": business_date.isoformat(),
                    "source_values": source_values,
                    "tolerance_band": tol_band,
                    "dissenting_sources": dissenting_srcs,
                }
                breaks.append(break_info)
            else:
                tol = rec_cfg.tolerances.get(field)
                tol_band = _tol_band(field, tol)
                # Look up currency from first available Silver row for this instrument
                currency = "USD"
                for sdf in source_silvers.values():
                    mask = (sdf["instrument_id"] == instrument_id) & (sdf["field"] == field)
                    matching = sdf[mask]
                    if not matching.empty and "currency" in matching.columns:
                        currency = str(matching["currency"].iloc[0])
                        break

                gold_rec = GoldenRecord(
                    instrument_id=instrument_id,
                    business_date=business_date,
                    field=field,  # type: ignore[arg-type]
                    golden_value=Decimal(str(round(golden_val, 6))),
                    currency=currency,
                    quorum_sources=sorted(quorum_srcs),
                    dissenting_sources=sorted(dissenting_srcs),
                    tolerance_band=tol_band,
                    confidence=confidence,
                )
                golden_records.append(gold_rec)
                # DESIGN-NOTE: FR-A8 / C-4 — one DecisionRecord per GoldenRecord so that
                # every Gold value has a traceable FK (Gold.decision_id → decisions.decision_id)
                # with the source values used to elect it stored in `inputs`.
                decision_rec = DecisionRecord(
                    decision_id=gold_rec.decision_id,
                    agent=self.name,
                    instrument_id=instrument_id,
                    business_date=business_date,
                    decision_type=DecisionType.RECONCILE,
                    inputs={
                        "field": field,
                        "source_values": {src: round(v, 6) for src, v in source_values.items()},
                        "quorum_sources": sorted(quorum_srcs),
                        "dissenting_sources": sorted(dissenting_srcs),
                        "tolerance_band": tol_band,
                    },
                    outcome={
                        "golden_value": str(round(golden_val, 6)),
                        "confidence": confidence.value,
                    },
                    rule_applied="quorum_tolerance",
                    verified=True,
                )
                self._store.write_decision(decision_rec)

        # Write quarantine for breaks
        if breaks:
            import json

            break_df = pd.DataFrame(
                [
                    {
                        "instrument_id": b["instrument_id"],
                        "field": b["field"],
                        "business_date": b["business_date"],
                        "source_values": json.dumps(b["source_values"]),
                        "tolerance_band": b["tolerance_band"],
                        "dissenting_sources": json.dumps(b["dissenting_sources"]),
                    }
                    for b in breaks
                ]
            )
            self._store.write_quarantine(break_df, "reconciliation_break", run_id, business_date)

        # Write Gold (only if any records elected)
        if golden_records:
            self._store.write_gold(golden_records, run_id, business_date)
            log.info(
                "Gold written: %d records, %d breaks (run=%s)",
                len(golden_records),
                len(breaks),
                run_id,
            )

        # Publish BREAK_DETECTED events (one per break)
        for brk in breaks:
            await self._bb.publish(
                Event(
                    topic=TopicType.BREAK_DETECTED,
                    agent=self.name,
                    run_id=run_id,
                    payload=brk,
                )
            )

        # Publish RECONCILIATION_COMPLETE
        await self._bb.publish(
            Event(
                topic=TopicType.RECONCILIATION_COMPLETE,
                agent=self.name,
                run_id=run_id,
                payload={
                    "source_id": "batch",
                    "business_date": business_date.isoformat(),
                    "golden_records": len(golden_records),
                    "breaks": len(breaks),
                },
            )
        )


# ---------------------------------------------------------------------------
# Pure quorum/tolerance functions (FR-R2, FR-R3, FR-R5 — no I/O, deterministic)
# ---------------------------------------------------------------------------


def _tol_band(field: str, tol: ToleranceConfig | None) -> str:
    """Return a human-readable tolerance band label for audit records."""
    if tol is None:
        return field
    suffix = "bps" if tol.type == "relative_bps" else "pct"
    return f"{field}:{tol.value}{suffix}"


def _within_tolerance(v1: float, v2: float, tol: ToleranceConfig) -> bool:
    """Return True if v1 and v2 agree within the configured tolerance band."""
    denom = max(abs(v1), abs(v2))
    if denom == 0:
        return True  # both zero → agree by definition
    if tol.type == "relative_bps":
        return abs(v1 - v2) / denom * 10_000 <= tol.value
    elif tol.type == "relative_pct":
        return abs(v1 - v2) / denom * 100 <= tol.value
    else:  # absolute
        return abs(v1 - v2) <= tol.value


def _elect(
    values: dict[str, float],
    field: str,
    rec_cfg: ReconciliationConfig,
) -> tuple[float | None, list[str], list[str], Confidence]:
    """Elect a golden value by quorum/contract-net vote (FR-R3).

    Returns (golden_value | None, quorum_sources, dissenting_sources, confidence).
    golden_value is None when no quorum can be formed (break condition, FR-R4).

    Algorithm: find the largest group of sources whose pairwise values are all within
    the configured tolerance band for this field (O(N²), N ≤ 5). If that group meets
    min_agreeing_sources, elect the mean of the group as golden_value.
    """
    tol = rec_cfg.tolerances.get(field)
    min_n = rec_cfg.quorum.min_agreeing_sources
    sources = list(values.keys())
    n = len(sources)

    if n == 0:
        return None, [], [], Confidence.LOW

    if tol is None:
        # No tolerance defined for this field — unanimous agreement required
        all_vals = list(values.values())
        if len(set(all_vals)) == 1:
            return all_vals[0], sources, [], Confidence.HIGH
        return None, [], sources, Confidence.LOW

    # Find largest agreeing group (O(N²))
    best_group: list[str] = []
    for candidate in sources:
        group = [candidate]
        for other in sources:
            if other == candidate:
                continue
            # Check `other` is within tolerance of ALL current group members (FR-R2)
            if all(_within_tolerance(values[other], values[m], tol) for m in group):
                group.append(other)
        if len(group) > len(best_group):
            best_group = group

    if len(best_group) < min_n:
        # No quorum (FR-R4 break condition)
        return None, [], sources, Confidence.LOW

    dissenting = [s for s in sources if s not in best_group]
    golden_val = sum(values[s] for s in best_group) / len(best_group)

    # Confidence: HIGH if all available sources agree; MEDIUM if some dissent
    confidence = Confidence.HIGH if not dissenting else Confidence.MEDIUM

    return golden_val, best_group, dissenting, confidence
