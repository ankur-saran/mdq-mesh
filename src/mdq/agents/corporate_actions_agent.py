"""Corporate Actions Agent — split detection and historical back-adjustment (FR-A4).

Subscribes to DQ_PASSED, DQ_FAILURE, and INGESTION_FAILED. Accumulates source outcomes
per run (same pattern as ReconciliationAgent). When all enabled sources have reported,
detects price-ratio jumps consistent with known split ratios, back-adjusts all prior-day
Silver files for the unadjusted source, and publishes CORPORATE_ACTION_DETECTED.

# DESIGN-NOTE: FR-A4 — Agent must be registered BEFORE ReconciliationAgent in cli.py.
# The Blackboard delivers events to subscribers in registration order (sequential await).
# By running first, this agent back-adjusts Silver before _reconcile() reads it, so
# the golden-value election always sees adjusted prices (C-4, C-5).
# DESIGN-NOTE: C-2 — All detection and adjustment logic is deterministic code. No LLM.
# DESIGN-NOTE: C-4/C-5 — Identical frozen inputs produce identical adjustments. Pure
# functions (_detect_single_source_splits, _detect_cross_source_splits) have no I/O.
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

import pandas as pd

from mdq.core.agent import Agent
from mdq.core.events import Event, TopicType
from mdq.core.schemas import DecisionRecord, DecisionType
from mdq.utils.logging import get_logger

if TYPE_CHECKING:
    from mdq.core.blackboard import Blackboard
    from mdq.core.config import Config, CorporateActionsConfig
    from mdq.core.store import MedallionStore

log = get_logger("agents.corporate_actions")

# Price fields that need to be divided by the split ratio during back-adjustment.
# Volume is multiplied (shares outstanding increase proportionally).
_PRICE_FIELDS: frozenset[str] = frozenset({"OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE"})


class CorporateActionsAgent(Agent):
    """Detects stock splits via price-ratio jumps and back-adjusts Silver history (FR-A4)."""

    def __init__(self, bb: Blackboard, store: MedallionStore, cfg: Config) -> None:
        self._bb = bb
        self._store = store
        self._cfg = cfg
        self._enabled_sources: frozenset[str] = frozenset(
            src
            for src, src_cfg in [
                ("yfinance", cfg.sources.yfinance),
                ("stooq", cfg.sources.stooq),
            ]
            if src_cfg.enabled
        )
        # {run_id: {source_id: True (Silver exists) | False (failed)}}
        self._completed: dict[str, dict[str, bool]] = {}
        # {run_id: business_date}
        self._bdates: dict[str, date] = {}

    @property
    def name(self) -> str:
        return "corporate_actions"

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

        # INGESTION_FAILED means no Silver was written; DQ_FAILURE Silver still exists.
        self._completed[run_id][source_id] = event.topic != TopicType.INGESTION_FAILED

        if self._enabled_sources <= set(self._completed[run_id]):
            bd = self._bdates.pop(run_id, business_date)
            completed_snapshot = self._completed.pop(run_id)
            await self._detect_and_adjust(run_id, bd, completed_snapshot)

    async def _detect_and_adjust(
        self,
        run_id: str,
        business_date: date,
        completed: dict[str, bool],
    ) -> None:
        """Load Silver windows, detect splits, back-adjust, publish events."""
        passing_sources = [src for src, passed in completed.items() if passed]
        ca_cfg = self._cfg.corporate_actions

        # Load Silver window for each passing source
        source_silvers: dict[str, pd.DataFrame] = {}
        for src in passing_sources:
            window = self._store.read_silver_window(business_date, ca_cfg.ca_window_days, src)
            if not window.empty:
                source_silvers[src] = window

        if not source_silvers:
            log.debug("No Silver history available for CA detection on %s", business_date)
            self._store.write_decision(
                DecisionRecord(
                    agent=self.name,
                    instrument_id="batch",
                    business_date=business_date,
                    decision_type=DecisionType.CORP_ACTION,
                    inputs={"run_id": run_id, "passing_sources": passing_sources},
                    outcome={"actions": 0, "reason": "no_history"},
                    rule_applied="split_ratio_detection",
                )
            )
            return

        # Single-source detection per source
        single_source_actions: list[dict[str, object]] = []
        for src, window_df in source_silvers.items():
            actions = _detect_single_source_splits(window_df, business_date, ca_cfg)
            for a in actions:
                a["source_id"] = src
            single_source_actions.extend(actions)

        # Cross-source detection (optional, requires ≥2 sources)
        cross_source_actions: list[dict[str, object]] = []
        if ca_cfg.require_cross_source_corroboration and len(source_silvers) >= 2:
            cross_source_actions = _detect_cross_source_splits(
                source_silvers, business_date, ca_cfg
            )

        # Merge: cross-source actions take precedence (more specific).
        # Key on (instrument_id, source_id) so cross-source can override single-source.
        action_map: dict[tuple[str, str], dict[str, object]] = {}
        for a in single_source_actions:
            key = (str(a["instrument_id"]), str(a["source_id"]))
            action_map[key] = a
        for a in cross_source_actions:
            key = (str(a["instrument_id"]), str(a["source_id"]))
            action_map[key] = a  # cross-source overrides single-source

        confirmed_actions = list(action_map.values())

        # Back-adjust Silver for each confirmed action
        total_adjusted_rows = 0
        for action in confirmed_actions:
            src = str(action["source_id"])
            instrument_id = str(action["instrument_id"])
            ratio = float(action["ratio"])  # type: ignore[arg-type]
            log.info(
                "Back-adjusting %s / %s by %.2f:1 (run=%s)",
                src,
                instrument_id,
                ratio,
                run_id,
            )
            adjusted = _back_adjust(
                source_id=src,
                instrument_id=instrument_id,
                ratio=ratio,
                business_date=business_date,
                run_id=run_id,
                store=self._store,
                ca_window_days=ca_cfg.ca_window_days,
            )
            total_adjusted_rows += adjusted
            action["adjusted_rows"] = adjusted

        # Publish one CORPORATE_ACTION_DETECTED event per confirmed action
        for action in confirmed_actions:
            await self._bb.publish(
                Event(
                    topic=TopicType.CORPORATE_ACTION_DETECTED,
                    agent=self.name,
                    run_id=run_id,
                    payload={
                        "source_id": action["source_id"],
                        "instrument_id": action["instrument_id"],
                        "business_date": business_date.isoformat(),
                        "action_type": action.get("action_type", "SPLIT"),
                        "ratio": action["ratio"],
                        "detected_ratio": action.get("detected_ratio", action["ratio"]),
                        "adjusted_rows": action.get("adjusted_rows", 0),
                    },
                )
            )

        # One DecisionRecord per batch (C-4)
        self._store.write_decision(
            DecisionRecord(
                agent=self.name,
                instrument_id="batch",
                business_date=business_date,
                decision_type=DecisionType.CORP_ACTION,
                inputs={
                    "run_id": run_id,
                    "passing_sources": passing_sources,
                    "excluded_sources": [s for s, p in completed.items() if not p],
                },
                outcome={
                    "actions": len(confirmed_actions),
                    "adjusted_rows": total_adjusted_rows,
                    "action_instruments": [
                        f"{a['source_id']}/{a['instrument_id']}" for a in confirmed_actions
                    ],
                },
                rule_applied="split_ratio_detection",
            )
        )


# ---------------------------------------------------------------------------
# Pure detection functions (no I/O — deterministic, FR-A4, C-4, C-5)
# ---------------------------------------------------------------------------


def _detect_single_source_splits(
    window_df: pd.DataFrame,
    business_date: date,
    cfg: CorporateActionsConfig,
) -> list[dict[str, object]]:
    """Detect price-ratio jumps consistent with candidate split ratios (FR-A4).

    For each instrument: ratio = prev_close / curr_close.
    If abs(ratio - candidate) / candidate <= split_ratio_tolerance → split detected.
    Skips instruments with < 2 distinct business dates (no history to compare against).
    Returns list of {instrument_id, action_type, ratio, detected_ratio}.
    source_id is added by the caller.
    """
    actions: list[dict[str, object]] = []
    bdate_ts = pd.Timestamp(business_date)

    close_df = window_df[window_df["field"] == "CLOSE"].copy()
    if close_df.empty:
        return actions

    close_df["business_date"] = pd.to_datetime(close_df["business_date"])

    for instrument_id, grp in close_df.groupby("instrument_id"):
        grp = grp.sort_values("business_date")
        curr_rows = grp[grp["business_date"] == bdate_ts]
        hist_rows = grp[grp["business_date"] < bdate_ts]

        if curr_rows.empty or hist_rows.empty:
            continue

        curr_close = float(curr_rows["value"].iloc[0])
        prev_close = float(hist_rows.sort_values("business_date")["value"].iloc[-1])

        if curr_close <= 0 or prev_close <= 0:
            continue

        # ratio > 1 means price dropped (prev > curr) — consistent with split
        detected_ratio = prev_close / curr_close

        for candidate in cfg.candidate_split_ratios:
            if abs(detected_ratio - candidate) / candidate <= cfg.split_ratio_tolerance:
                actions.append(
                    {
                        "instrument_id": instrument_id,
                        "action_type": "SPLIT",
                        "ratio": candidate,
                        "detected_ratio": round(detected_ratio, 6),
                    }
                )
                break  # use the first (closest) matching candidate

    return actions


def _detect_cross_source_splits(
    source_silvers: dict[str, pd.DataFrame],
    business_date: date,
    cfg: CorporateActionsConfig,
) -> list[dict[str, object]]:
    """Detect inter-source price disagreement consistent with one source being unadjusted.

    For each instrument: compare CLOSE values across all source pairs on business_date.
    If max_close / min_close ≈ candidate_ratio → one source is unadjusted.
    Disambiguation: the source whose own prev/curr ratio ≈ candidate_ratio is unadjusted
    (its own history shows a discontinuity). The other source's history is smooth.
    Returns actions tagged with the source_id that needs back-adjustment.
    """
    actions: list[dict[str, object]] = []
    bdate_ts = pd.Timestamp(business_date)
    sources = list(source_silvers.keys())

    if len(sources) < 2:
        return actions

    # Build pivot: {instrument_id: {source_id: (curr_close, prev_close | None)}}
    pivot: dict[str, dict[str, tuple[float, float | None]]] = {}
    for src, window_df in source_silvers.items():
        close_df = window_df[window_df["field"] == "CLOSE"].copy()
        if close_df.empty:
            continue
        close_df["business_date"] = pd.to_datetime(close_df["business_date"])

        for instrument_id, grp in close_df.groupby("instrument_id"):
            grp = grp.sort_values("business_date")
            curr_rows = grp[grp["business_date"] == bdate_ts]
            hist_rows = grp[grp["business_date"] < bdate_ts]

            if curr_rows.empty:
                continue

            curr_close = float(curr_rows["value"].iloc[0])
            prev_close = (
                float(hist_rows.sort_values("business_date")["value"].iloc[-1])
                if not hist_rows.empty
                else None
            )

            iid = str(instrument_id)
            if iid not in pivot:
                pivot[iid] = {}
            pivot[iid][src] = (curr_close, prev_close)

    # Compare each source pair for each instrument
    seen: set[tuple[str, str]] = set()  # avoid double-counting (instrument, src)
    for instrument_id, src_data in pivot.items():
        src_list = [s for s in sources if s in src_data]
        if len(src_list) < 2:
            continue

        for i in range(len(src_list)):
            for j in range(i + 1, len(src_list)):
                src_a, src_b = src_list[i], src_list[j]
                curr_a, prev_a = src_data[src_a]
                curr_b, prev_b = src_data[src_b]

                if curr_a <= 0 or curr_b <= 0:
                    continue

                hi = max(curr_a, curr_b)
                lo = min(curr_a, curr_b)
                cross_ratio = hi / lo

                for candidate in cfg.candidate_split_ratios:
                    if abs(cross_ratio - candidate) / candidate <= cfg.split_ratio_tolerance:
                        # Identify the unadjusted source: the one whose own prev/curr
                        # ratio also ≈ candidate (its history has the discontinuity).
                        unadjusted_src: str | None = None

                        if prev_a is not None and prev_a > 0:
                            own_ratio_a = prev_a / curr_a
                            if (
                                abs(own_ratio_a - candidate) / candidate
                                <= cfg.split_ratio_tolerance
                            ):
                                unadjusted_src = src_a

                        if unadjusted_src is None and prev_b is not None and prev_b > 0:
                            own_ratio_b = prev_b / curr_b
                            if (
                                abs(own_ratio_b - candidate) / candidate
                                <= cfg.split_ratio_tolerance
                            ):
                                unadjusted_src = src_b

                        # Fallback: if disambiguation fails, flag the source with higher
                        # current price as unadjusted (splits reduce nominal price).
                        if unadjusted_src is None:
                            unadjusted_src = src_a if curr_a > curr_b else src_b

                        key = (instrument_id, unadjusted_src)
                        if key not in seen:
                            seen.add(key)
                            actions.append(
                                {
                                    "instrument_id": instrument_id,
                                    "source_id": unadjusted_src,
                                    "action_type": "SPLIT",
                                    "ratio": candidate,
                                    "detected_ratio": round(cross_ratio, 6),
                                }
                            )
                        break  # matched a candidate; move to next pair

    return actions


# ---------------------------------------------------------------------------
# Back-adjustment (writes Silver — deterministic given identical inputs, C-5)
# ---------------------------------------------------------------------------


def _back_adjust(
    source_id: str,
    instrument_id: str,
    ratio: float,
    business_date: date,
    run_id: str,
    store: MedallionStore,
    ca_window_days: int,
) -> int:
    """Divide historical price fields by ratio; multiply VOLUME by ratio (FR-A4).

    Only adjusts dates strictly before business_date (current day already reflects the split).
    Overwrites each Silver Parquet file atomically via store.write_silver().
    Sets ca_adjusted=True on adjusted rows. Returns total row count adjusted.
    """
    window = store.read_silver_window(business_date, ca_window_days, source_id)
    if window.empty:
        return 0

    window["business_date"] = pd.to_datetime(window["business_date"])
    bdate_ts = pd.Timestamp(business_date)

    # Collect all distinct historical dates (strictly before today)
    hist_dates = sorted(d for d in window["business_date"].unique() if d < bdate_ts)

    total_adjusted = 0
    for hist_ts in hist_dates:
        hist_date = hist_ts.date()
        p = store.silver_path(run_id, hist_date, source_id)
        if not p.exists():
            continue

        hist_df = pd.read_parquet(p)
        inst_mask = hist_df["instrument_id"] == instrument_id

        for field in _PRICE_FIELDS:
            field_mask = inst_mask & (hist_df["field"] == field)
            if field_mask.any():
                hist_df.loc[field_mask, "value"] = (
                    hist_df.loc[field_mask, "value"].astype(float) / ratio
                )
                total_adjusted += int(field_mask.sum())

        # Volume scales up proportionally with share count after a split
        vol_mask = inst_mask & (hist_df["field"] == "VOLUME")
        if vol_mask.any():
            hist_df.loc[vol_mask, "value"] = hist_df.loc[vol_mask, "value"].astype(float) * ratio
            total_adjusted += int(vol_mask.sum())

        # Mark as back-adjusted for lineage
        hist_df.loc[inst_mask, "ca_adjusted"] = True

        store.write_silver(hist_df, run_id, hist_date, source_id)

    return total_adjusted
