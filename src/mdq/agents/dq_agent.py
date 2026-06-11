"""Data Quality Agent — deterministic rule suite for Silver data (FR-A3).

Subscribes to CONTRACT_PASSED. Reads the validated Silver Parquet and applies
five config-driven DQ rules. High-severity failures quarantine the offending rows
and publish DQ_FAILURE; clean or medium-only batches publish DQ_PASSED.
Every outcome is persisted as a DecisionRecord (C-4).

# DESIGN-NOTE: FR-A3 — DQAgent and AnomalyAgent both subscribe to CONTRACT_PASSED
# and run in parallel. Neither depends on the other's output; they are independent
# quality dimensions on the same Silver data.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING

import pandas as pd

from mdq.core.agent import Agent
from mdq.core.events import Event, TopicType
from mdq.core.schemas import DecisionRecord, DecisionType
from mdq.utils.logging import get_logger

if TYPE_CHECKING:
    from mdq.core.blackboard import Blackboard
    from mdq.core.config import Config
    from mdq.core.store import MedallionStore

log = get_logger("agents.dq")


@dataclass
class DQFailure:
    """A single DQ rule violation."""

    rule: str
    severity: str
    instrument_id: str
    field: str
    value: object
    reason: str


class DQAgent(Agent):
    """Applies the DQ rule suite to Silver data (FR-A3)."""

    def __init__(self, bb: Blackboard, store: MedallionStore, cfg: Config) -> None:
        self._bb = bb
        self._store = store
        self._cfg = cfg

    @property
    def name(self) -> str:
        return "dq"

    @property
    def subscriptions(self) -> list[TopicType]:
        return [TopicType.CONTRACT_PASSED]

    async def act(self, event: Event) -> None:
        source_id: str = event.payload["source_id"]
        business_date = date.fromisoformat(event.payload["business_date"])
        run_id = event.run_id

        if event.payload.get("silver_rows") == 0:
            log.warning("DQ: no Silver rows for %s (empty universe) — publishing DQ_PASSED", source_id)
            await self._bb.publish(
                Event(
                    topic=TopicType.DQ_PASSED,
                    agent=self.name,
                    run_id=run_id,
                    payload={
                        "source_id": source_id,
                        "business_date": business_date.isoformat(),
                        "silver_rows": 0,
                        "rules_checked": 0,
                    },
                )
            )
            return

        silver_path = self._store.silver_path(run_id, business_date, source_id)
        try:
            silver_df = pd.read_parquet(silver_path)
        except FileNotFoundError:
            log.error("Silver file not found for DQ check: %s", silver_path)
            await self._publish_failure(
                source_id,
                business_date,
                run_id,
                failures=[
                    DQFailure(
                        rule="file_missing",
                        severity="high",
                        instrument_id="N/A",
                        field="N/A",
                        value=None,
                        reason=f"Silver not found: {silver_path}",
                    )
                ],
                quarantined=0,
            )
            return

        rules_cfg = self._cfg.dq_rules
        # DESIGN-NOTE: Phase 3 adds per-source staleness routing; Phase 2 uses yfinance as default.
        src_cfg = self._cfg.sources.yfinance

        failures: list[DQFailure] = []

        if rules_cfg.null_check.enabled:
            failures.extend(_check_nulls(silver_df, rules_cfg.null_check.severity))

        if rules_cfg.range_check.enabled:
            min_val = rules_cfg.range_check.min if rules_cfg.range_check.min is not None else 0.0
            failures.extend(_check_range(silver_df, rules_cfg.range_check.severity, min_val))

        if rules_cfg.staleness_check.enabled:
            failures.extend(
                _check_staleness(
                    silver_df,
                    business_date,
                    src_cfg.staleness_max_days,
                    rules_cfg.staleness_check.severity,
                )
            )

        if rules_cfg.monotonicity_check.enabled:
            failures.extend(_check_monotonicity(silver_df, rules_cfg.monotonicity_check.severity))

        if rules_cfg.duplicate_check.enabled:
            failures.extend(_check_duplicates(silver_df, rules_cfg.duplicate_check.severity))

        high_failures = [f for f in failures if f.severity == "high"]
        quarantined = 0
        if high_failures:
            failing_ids = {(f.instrument_id, f.field) for f in high_failures}
            mask = silver_df.apply(
                lambda row: (row["instrument_id"], row["field"]) in failing_ids, axis=1
            )
            failing_rows = silver_df[mask]
            if not failing_rows.empty:
                self._store.write_quarantine(failing_rows, "dq_violation", run_id, business_date)
                quarantined = len(failing_rows)

        outcome: dict[str, object] = {
            "status": "failed" if failures else "passed",
            "total_failures": len(failures),
            "high_severity": len(high_failures),
            "medium_severity": len(failures) - len(high_failures),
            "quarantined_rows": quarantined,
            "failures": [
                {
                    "rule": f.rule,
                    "severity": f.severity,
                    "instrument_id": f.instrument_id,
                    "field": f.field,
                    "reason": f.reason,
                }
                for f in failures
            ],
        }
        record = DecisionRecord(
            agent=self.name,
            instrument_id=f"{source_id}:batch",
            business_date=business_date,
            decision_type=DecisionType.DQ,
            inputs={"source_id": source_id, "silver_rows": len(silver_df)},
            outcome=outcome,
            rule_applied="dq_rule_suite",
        )
        self._store.write_decision(record)

        if failures:
            log.warning(
                "DQ_FAILURE: %s — %d failures (%d high, %d quarantined)",
                source_id,
                len(failures),
                len(high_failures),
                quarantined,
            )
            await self._publish_failure(source_id, business_date, run_id, failures, quarantined)
        else:
            log.info("DQ_PASSED: %s (%d Silver rows)", source_id, len(silver_df))
            await self._bb.publish(
                Event(
                    topic=TopicType.DQ_PASSED,
                    agent=self.name,
                    run_id=run_id,
                    payload={
                        "source_id": source_id,
                        "business_date": business_date.isoformat(),
                        "silver_rows": len(silver_df),
                        "rules_checked": 5,
                    },
                )
            )

    async def _publish_failure(
        self,
        source_id: str,
        business_date: date,
        run_id: str,
        failures: list[DQFailure],
        quarantined: int,
    ) -> None:
        by_rule: dict[str, list[str]] = {}
        for f in failures:
            by_rule.setdefault(f.rule, []).append(f.instrument_id)

        await self._bb.publish(
            Event(
                topic=TopicType.DQ_FAILURE,
                agent=self.name,
                run_id=run_id,
                payload={
                    "source_id": source_id,
                    "business_date": business_date.isoformat(),
                    "failures": [
                        {
                            "rule": rule,
                            "severity": next(f.severity for f in failures if f.rule == rule),
                            "count": len(insts),
                            "instruments": list(set(insts)),
                        }
                        for rule, insts in by_rule.items()
                    ],
                    "quarantined_rows": quarantined,
                },
            )
        )


# ---------------------------------------------------------------------------
# Rule implementations (pure transforms — no I/O, no side effects)
# ---------------------------------------------------------------------------


def _check_nulls(df: pd.DataFrame, severity: str) -> list[DQFailure]:
    mask = df["value"].isna()
    return [
        DQFailure(
            rule="null_check",
            severity=severity,
            instrument_id=row["instrument_id"],
            field=row["field"],
            value=None,
            reason="value is null",
        )
        for _, row in df[mask].iterrows()
    ]


def _check_range(df: pd.DataFrame, severity: str, min_val: float) -> list[DQFailure]:
    # VOLUME is a count and can legitimately be 0; exclude it from range check
    non_volume = df[df["field"] != "VOLUME"]
    mask = non_volume["value"].notna() & (non_volume["value"] < min_val)
    return [
        DQFailure(
            rule="range_check",
            severity=severity,
            instrument_id=row["instrument_id"],
            field=row["field"],
            value=row["value"],
            reason=f"value {row['value']} < min {min_val}",
        )
        for _, row in non_volume[mask].iterrows()
    ]


def _check_staleness(
    df: pd.DataFrame, business_date: date, max_days: int, severity: str
) -> list[DQFailure]:
    failures: list[DQFailure] = []
    if "fetch_ts" not in df.columns:
        return failures
    # Strip tz so subtraction works regardless of whether fetch_ts is tz-aware or naive.
    fetch_dates = pd.to_datetime(df["fetch_ts"]).dt.tz_convert(None).dt.normalize()
    bdate_ts = pd.Timestamp(business_date)
    lag_days = (bdate_ts - fetch_dates).dt.days
    stale_mask = lag_days > max_days
    for _, row in df[stale_mask].iterrows():
        fetch_date = pd.to_datetime(row["fetch_ts"]).date()
        lag = (business_date - fetch_date).days
        failures.append(
            DQFailure(
                rule="staleness_check",
                severity=severity,
                instrument_id=row["instrument_id"],
                field=row["field"],
                value=row["value"],
                reason=f"fetch_ts lag {lag}d > max {max_days}d",
            )
        )
    return failures


def _check_monotonicity(df: pd.DataFrame, severity: str) -> list[DQFailure]:
    """Check HIGH >= LOW and HIGH >= OPEN/CLOSE >= LOW for each instrument per date."""
    failures: list[DQFailure] = []
    pivot_fields = ["OPEN", "HIGH", "LOW", "CLOSE"]
    pivot_df = df[df["field"].isin(pivot_fields)]
    if pivot_df.empty:
        return failures

    wide = pivot_df.pivot_table(
        index=["instrument_id", "business_date"],
        columns="field",
        values="value",
        aggfunc="first",
    ).reset_index()

    for col in pivot_fields:
        if col not in wide.columns:
            return failures  # incomplete data — skip silently

    # HIGH must be >= LOW
    bad = wide[wide["HIGH"] < wide["LOW"]]
    for _, row in bad.iterrows():
        failures.append(
            DQFailure(
                rule="monotonicity_check",
                severity=severity,
                instrument_id=str(row["instrument_id"]),
                field="HIGH/LOW",
                value=f"H={row['HIGH']},L={row['LOW']}",
                reason="HIGH < LOW",
            )
        )
    # OPEN and CLOSE must be within [LOW, HIGH]
    for price_field in ("OPEN", "CLOSE"):
        bad_hi = wide[wide[price_field] > wide["HIGH"]]
        bad_lo = wide[wide[price_field] < wide["LOW"]]
        for _, row in pd.concat([bad_hi, bad_lo]).iterrows():
            failures.append(
                DQFailure(
                    rule="monotonicity_check",
                    severity=severity,
                    instrument_id=str(row["instrument_id"]),
                    field=price_field,
                    value=row[price_field],
                    reason=f"{price_field} outside [LOW={row['LOW']}, HIGH={row['HIGH']}]",
                )
            )
    return failures


def _check_duplicates(df: pd.DataFrame, severity: str) -> list[DQFailure]:
    dup_mask = df.duplicated(subset=["instrument_id", "business_date", "field", "source_id"])
    return [
        DQFailure(
            rule="duplicate_check",
            severity=severity,
            instrument_id=row["instrument_id"],
            field=row["field"],
            value=row["value"],
            reason="duplicate (instrument_id, business_date, field, source_id)",
        )
        for _, row in df[dup_mask].iterrows()
    ]
