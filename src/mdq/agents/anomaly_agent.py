"""Anomaly Agent — statistical outlier detection for Silver data (FR-A5).

Subscribes to CONTRACT_PASSED (parallel with DQAgent). Computes rolling z-scores and
IQR fences over a configurable window of historical Silver data. A volatility-aware
filter distinguishes "anomalous but real" (volatility-regime move) from "anomalous and
likely wrong" using a two-window standard-deviation ratio. Publishes ANOMALY_DETECTED
with an is_likely_error flag for each detected anomaly.

# DESIGN-NOTE: FR-A5 — pure statistical detection (no ML model, no external service).
# C-2 invariant preserved: anomaly classification is fully deterministic given the same
# historical Silver window. AnomalyAgent never quarantines directly — that is the
# Remediation Agent's responsibility (Phase 5).
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

from mdq.core.agent import Agent
from mdq.core.events import Event, TopicType
from mdq.core.schemas import DecisionRecord, DecisionType
from mdq.utils.logging import get_logger

if TYPE_CHECKING:
    from mdq.core.blackboard import Blackboard
    from mdq.core.config import Config
    from mdq.core.store import MedallionStore

log = get_logger("agents.anomaly")


class AnomalyAgent(Agent):
    """Detects statistical outliers in Silver data using z-score and IQR (FR-A5)."""

    def __init__(self, bb: Blackboard, store: MedallionStore, cfg: Config) -> None:
        self._bb = bb
        self._store = store
        self._cfg = cfg

    @property
    def name(self) -> str:
        return "anomaly"

    @property
    def subscriptions(self) -> list[TopicType]:
        return [TopicType.CONTRACT_PASSED]

    async def act(self, event: Event) -> None:
        source_id: str = event.payload["source_id"]
        business_date = date.fromisoformat(event.payload["business_date"])
        run_id = event.run_id
        cfg = self._cfg.anomaly

        if event.payload.get("silver_rows") == 0:
            log.warning("ANOMALY: no Silver rows for %s (empty universe) — skipping anomaly check", source_id)
            return

        # Load current-day Silver
        silver_path = self._store.silver_path(run_id, business_date, source_id)
        try:
            current_df = pd.read_parquet(silver_path)
        except FileNotFoundError:
            log.error("Silver not found for anomaly check: %s", silver_path)
            return

        # Load rolling history window — filtered to this source so we compare like-for-like.
        window_df = self._store.read_silver_window(business_date, cfg.zscore_window, source_id)

        anomalies: list[dict[str, object]] = []

        for (instrument_id, field), grp in current_df.groupby(["instrument_id", "field"]):
            if grp["value"].isna().all():
                continue
            current_val = float(grp["value"].iloc[0])

            # Historical series sorted by date — required for correct recent-window slice.
            hist_df = window_df[
                (window_df["instrument_id"] == instrument_id)
                & (window_df["field"] == field)
                & (window_df["business_date"].dt.date < business_date)
            ].sort_values("business_date")
            hist = hist_df["value"].dropna()

            if len(hist) < 3:
                # Insufficient history — skip anomaly detection for this series
                continue

            long_mean = float(hist.mean())
            long_std = float(hist.std(ddof=1))
            if long_std == 0.0:
                continue

            z_score = (current_val - long_mean) / long_std

            # IQR fence check
            q1, q3 = float(np.percentile(hist, 25)), float(np.percentile(hist, 75))
            iqr = q3 - q1
            iqr_lo = q1 - cfg.iqr_multiplier * iqr
            iqr_hi = q3 + cfg.iqr_multiplier * iqr
            iqr_outlier = current_val < iqr_lo or current_val > iqr_hi

            is_z_outlier = abs(z_score) > cfg.zscore_threshold

            if not (is_z_outlier or iqr_outlier):
                continue

            is_likely_error = True
            reason = "zscore_exceeded" if is_z_outlier else "iqr_fence"

            # Volatility-regime check: compare recent returns-vol vs long-term returns-vol.
            # DESIGN-NOTE: FR-A5 — using pct_change (returns) rather than price-level std
            # avoids false negatives from cumulative price drift in the random walk.
            if cfg.volatility_aware and is_z_outlier and len(hist) >= 4:
                returns = hist.pct_change().dropna()
                long_returns_std = float(returns.std(ddof=1))
                recent_n = min(cfg.volatility_regime_window, len(returns))
                if recent_n >= 2 and long_returns_std > 0:
                    recent_returns_std = float(returns.iloc[-recent_n:].std(ddof=1))
                    if recent_returns_std / long_returns_std > cfg.volatility_regime_ratio:
                        is_likely_error = False
                        reason = "volatility_regime"

            anomalies.append(
                {
                    "instrument_id": instrument_id,
                    "field": field,
                    "value": current_val,
                    "zscore": round(z_score, 4),
                    "iqr_outlier": iqr_outlier,
                    "is_likely_error": is_likely_error,
                    "reason": reason,
                }
            )

        outcome: dict[str, object] = {
            "anomaly_count": len(anomalies),
            "likely_errors": sum(1 for a in anomalies if a["is_likely_error"]),
            "volatility_regime": sum(1 for a in anomalies if a["reason"] == "volatility_regime"),
        }
        record = DecisionRecord(
            agent=self.name,
            instrument_id=f"{source_id}:batch",
            business_date=business_date,
            decision_type=DecisionType.DQ,
            inputs={"source_id": source_id, "silver_rows": len(current_df)},
            outcome=outcome,
            rule_applied="anomaly_zscore_iqr",
        )
        self._store.write_decision(record)

        if anomalies:
            log.warning(
                "ANOMALY_DETECTED: %s — %d anomalies (%d likely errors)",
                source_id,
                len(anomalies),
                outcome["likely_errors"],
            )
            await self._bb.publish(
                Event(
                    topic=TopicType.ANOMALY_DETECTED,
                    agent=self.name,
                    run_id=run_id,
                    payload={
                        "source_id": source_id,
                        "business_date": business_date.isoformat(),
                        "anomalies": anomalies,
                    },
                )
            )
        else:
            log.info("Anomaly check clean: %s (%d series checked)", source_id, len(current_df))
