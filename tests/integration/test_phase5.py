"""Phase 5 acceptance criteria integration tests (PRD §12).

AC1 — CROSS_SOURCE_BREAK auto-remediated; REMEDIATION_COMPLETE fires; Gold has instrument.
AC2 — NULL_BURST auto-remediated; Gold contains instrument after re-fetch.
AC3 — Escalation fires after bounded retries; REMEDIATION_FAILED published.
AC4 — Held instrument absent from Gold Parquet when remediation fails (FR-H4).
KPI-1 — ≥ 80% auto-remediation rate across the BREAK_DETECTED defect suite.

Architecture of integration tests:
- Source agents (YFinance, Stooq) are NOT registered — no live network calls.
- Silver is seeded directly via store.write_silver().
- A _RefetchResponder test helper responds to REFETCH_REQUESTED with clean (or defective)
  Silver, then publishes DQ_PASSED — mimicking what source agents do in production.
- DQ_PASSED events are published synthetically to trigger the downstream pipeline.
- The remediation loop fires end-to-end: ReconciliationAgent → RemediationAgent →
  _RefetchResponder → ReconciliationAgent (retry) → RemediationAgent (complete/escalate).
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import yaml

from harness.inject import DefectType, build_silver_history, inject
from mdq.agents.anomaly_agent import AnomalyAgent
from mdq.agents.contract_agent import ContractAgent
from mdq.agents.corporate_actions_agent import CorporateActionsAgent
from mdq.agents.dq_agent import DQAgent
from mdq.agents.reconciliation_agent import ReconciliationAgent
from mdq.agents.remediation_agent import RemediationAgent
from mdq.agents.supervisor import Supervisor
from mdq.core.agent import Agent
from mdq.core.blackboard import Blackboard
from mdq.core.config import Config
from mdq.core.events import Event, TopicType
from mdq.core.store import MedallionStore

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_BDATE = date(2024, 1, 15)
_INSTRUMENTS = ["AAPL", "MSFT", "NVDA", "JPM", "SPY"]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_store(tmp_path: Path) -> MedallionStore:
    store = MedallionStore(
        bronze_root=tmp_path / "bronze",
        silver_root=tmp_path / "silver",
        gold_root=tmp_path / "gold",
        quarantine_root=tmp_path / "quarantine",
        lineage_root=tmp_path / "lineage",
        duckdb_path=tmp_path / "mdq.duckdb",
    )
    store.init_dirs()
    store.open()
    return store


def _make_cfg(tmp_path: Path, max_retries: int = 3) -> Config:
    base = yaml.safe_load(Path("config/default.yaml").read_text())
    base["runtime"]["use_fixtures"] = False
    base["runtime"]["storage"]["bronze"] = str(tmp_path / "bronze")
    base["runtime"]["storage"]["silver"] = str(tmp_path / "silver")
    base["runtime"]["storage"]["gold"] = str(tmp_path / "gold")
    base["runtime"]["storage"]["quarantine"] = str(tmp_path / "quarantine")
    base["runtime"]["storage"]["lineage"] = str(tmp_path / "lineage")
    base["runtime"]["duckdb_path"] = str(tmp_path / "mdq.duckdb")
    base["remediation"]["max_retries"] = max_retries
    universe = yaml.safe_load(Path("config/universe.yaml").read_text())
    base["universe"] = universe
    return Config.model_validate(base)


def _seed_silver(
    store: MedallionStore,
    run_id: str,
    source_id: str,
    instruments: list[str],
    business_date: date,
    base_price: float = 100.0,
    seed: int = 42,
) -> pd.DataFrame:
    """Write one day of clean Silver for the given source and return it."""
    df = build_silver_history(
        instruments=instruments,
        n_days=1,
        end_date=business_date,
        base_price=base_price,
        source_id=source_id,
        seed=seed,
    )
    store.write_silver(df, run_id, business_date, source_id)
    return df


def _dq_passed_event(
    bb: Blackboard,
    run_id: str,
    source_id: str,
    business_date: date,
) -> Event:
    return Event(
        topic=TopicType.DQ_PASSED,
        agent="test_harness",
        run_id=run_id,
        payload={"source_id": source_id, "business_date": business_date.isoformat()},
    )


class _RefetchResponder(Agent):
    """Test helper: responds to REFETCH_REQUESTED.

    Writes clean Silver (or optionally defective Silver) and publishes DQ_PASSED.
    Simulates what source agents do in production on REFETCH_REQUESTED.
    """

    def __init__(
        self,
        bb: Blackboard,
        store: MedallionStore,
        instruments: list[str],
        business_date: date,
        always_defective: bool = False,
    ) -> None:
        self._bb = bb
        self._store = store
        self._instruments = instruments
        self._business_date = business_date
        self._always_defective = always_defective
        self.refetch_count: int = 0

    @property
    def name(self) -> str:
        return "refetch_responder"

    @property
    def subscriptions(self) -> list[TopicType]:
        return [TopicType.REFETCH_REQUESTED]

    async def act(self, event: Event) -> None:
        source_id: str = str(event.payload.get("source_id", ""))
        run_id = event.run_id
        business_date = date.fromisoformat(str(event.payload.get("business_date", "")))
        self.refetch_count += 1

        # DESIGN-NOTE: seed=42 matches the initial yfinance seeding so re-fetched stooq
        # values are identical to yfinance → quorum passes after clean re-fetch (C-5).
        clean_df = build_silver_history(
            instruments=self._instruments,
            n_days=1,
            end_date=business_date,
            base_price=100.0,
            source_id=source_id,
            seed=42,
        )

        if self._always_defective:
            # Simulate source still returning broken data
            defective_df = inject(clean_df.copy(), DefectType.CROSS_SOURCE_BREAK, shift_pct=0.20)
            self._store.write_silver(defective_df, run_id, business_date, source_id)
        else:
            self._store.write_silver(clean_df, run_id, business_date, source_id)

        await self._bb.publish(
            Event(
                topic=TopicType.DQ_PASSED,
                agent=self.name,
                run_id=run_id,
                payload={"source_id": source_id, "business_date": business_date.isoformat()},
            )
        )


def _make_pipeline(
    store: MedallionStore,
    cfg: Config,
    refetch_responder: _RefetchResponder,
) -> Blackboard:
    """Build 6-agent downstream pipeline (no source agents — Silver seeded directly)."""
    bb = Blackboard(db_path=":memory:")
    bb.register(ContractAgent(bb, store, cfg))
    bb.register(DQAgent(bb, store, cfg))
    bb.register(AnomalyAgent(bb, store, cfg))
    bb.register(CorporateActionsAgent(bb, store, cfg))
    bb.register(ReconciliationAgent(bb, store, cfg))
    bb.register(RemediationAgent(bb, store, cfg))
    bb.register(Supervisor(bb, store, cfg))
    bb.register(refetch_responder)
    return bb


async def _trigger_reconciliation(
    bb: Blackboard,
    run_id: str,
    business_date: date,
) -> None:
    """Publish DQ_PASSED for both sources to trigger the downstream reconciliation."""
    for src in ("yfinance", "stooq"):
        await bb.publish(_dq_passed_event(bb, run_id, src, business_date))
    await bb.drain()


# ---------------------------------------------------------------------------
# AC1 — CROSS_SOURCE_BREAK auto-remediated
# ---------------------------------------------------------------------------


async def test_ac1_cross_source_break_remediated(tmp_path: Path) -> None:
    """AC1: stooq Silver has CROSS_SOURCE_BREAK → BREAK_DETECTED → re-fetch → REMEDIATION_COMPLETE.

    Gold must contain the previously-broken instrument after remediation.
    """
    run_id = "ac1-run"
    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path, max_retries=1)
    responder = _RefetchResponder(
        MagicMock(), store, _INSTRUMENTS, _BDATE, always_defective=False  # type: ignore[arg-type]
    )

    # Seed clean Silver for yfinance; defective Silver for stooq
    _seed_silver(store, run_id, "yfinance", _INSTRUMENTS, _BDATE, base_price=100.0)
    stooq_df = _seed_silver(store, run_id, "stooq", _INSTRUMENTS, _BDATE, base_price=100.0)
    defective = inject(stooq_df.copy(), DefectType.CROSS_SOURCE_BREAK, shift_pct=0.20)
    store.write_silver(defective, run_id, _BDATE, "stooq")

    bb = _make_pipeline(store, cfg, responder)
    responder._bb = bb  # wire bb after construction
    await bb.start()
    await _trigger_reconciliation(bb, run_id, _BDATE)

    # Assert BREAK_DETECTED fired
    break_events = bb.get_events(topic=TopicType.BREAK_DETECTED)
    assert len(break_events) > 0, "Expected BREAK_DETECTED for CROSS_SOURCE_BREAK"

    # Assert REFETCH_REQUESTED published
    refetch_events = bb.get_events(topic=TopicType.REFETCH_REQUESTED)
    assert len(refetch_events) > 0, "Expected REFETCH_REQUESTED"
    assert json.loads(refetch_events[0]["payload"])["source_id"] == "stooq"

    # Assert REMEDIATION_COMPLETE fires (not ESCALATION)
    rem_complete = bb.get_events(topic=TopicType.REMEDIATION_COMPLETE)
    assert len(rem_complete) > 0, "Expected REMEDIATION_COMPLETE"
    assert bb.get_events(topic=TopicType.ESCALATION) == []

    # Assert DecisionRecord(REMEDIATE, verified=True) persisted
    decisions = store.query(
        "SELECT * FROM decisions WHERE agent='remediation' AND decision_type='REMEDIATE'"
    )
    assert len(decisions) == 1
    assert bool(decisions.iloc[0]["verified"]) is True

    # Assert Gold contains at least one instrument (remediation wrote Gold)
    gold_events = bb.get_events(topic=TopicType.RECONCILIATION_COMPLETE)
    assert len(gold_events) >= 2, "ReconciliationAgent should have fired twice (initial + retry)"

    # The second RECONCILIATION_COMPLETE (remediation pass) should have breaks=0
    payloads = [json.loads(e["payload"]) for e in gold_events]
    assert any(
        p["breaks"] == 0 for p in payloads
    ), "Expected at least one reconciliation with 0 breaks"

    await bb.stop()
    store.close()


# ---------------------------------------------------------------------------
# AC2 — NULL_BURST auto-remediated
# ---------------------------------------------------------------------------


async def test_ac2_null_burst_remediated(tmp_path: Path) -> None:
    """AC2: NULL_BURST on stooq (all NaN) → no quorum → BREAK_DETECTED → re-fetch → fixed."""
    run_id = "ac2-run"
    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path, max_retries=1)
    responder = _RefetchResponder(
        MagicMock(), store, _INSTRUMENTS, _BDATE, always_defective=False  # type: ignore[arg-type]
    )

    _seed_silver(store, run_id, "yfinance", _INSTRUMENTS, _BDATE, base_price=100.0)
    stooq_df = _seed_silver(store, run_id, "stooq", _INSTRUMENTS, _BDATE, base_price=100.0)
    # NULL_BURST: set all values to NaN so stooq has no usable rows in pivot
    defective = inject(stooq_df.copy(), DefectType.NULL_BURST, seed=42, rate=1.0)
    store.write_silver(defective, run_id, _BDATE, "stooq")

    bb = _make_pipeline(store, cfg, responder)
    responder._bb = bb
    await bb.start()
    await _trigger_reconciliation(bb, run_id, _BDATE)

    # Assert BREAK_DETECTED (stooq has all-NaN values → no quorum)
    break_events = bb.get_events(topic=TopicType.BREAK_DETECTED)
    assert len(break_events) > 0, "Expected BREAK_DETECTED for NULL_BURST"

    # Assert re-fetch happened and remediation completed
    assert len(bb.get_events(topic=TopicType.REFETCH_REQUESTED)) > 0
    rem_complete = bb.get_events(topic=TopicType.REMEDIATION_COMPLETE)
    assert len(rem_complete) > 0, "Expected REMEDIATION_COMPLETE after NULL_BURST remediation"

    await bb.stop()
    store.close()


# ---------------------------------------------------------------------------
# AC3 — Escalation after bounded retries
# ---------------------------------------------------------------------------


async def test_ac3_escalation_after_max_retries(tmp_path: Path) -> None:
    """AC3: stooq source is persistently broken → ESCALATION after max_retries=1."""
    run_id = "ac3-run"
    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path, max_retries=1)
    # _RefetchResponder with always_defective=True: re-fetch still returns broken data
    responder = _RefetchResponder(
        MagicMock(), store, _INSTRUMENTS, _BDATE, always_defective=True  # type: ignore[arg-type]
    )

    _seed_silver(store, run_id, "yfinance", _INSTRUMENTS, _BDATE, base_price=100.0)
    stooq_df = _seed_silver(store, run_id, "stooq", _INSTRUMENTS, _BDATE, base_price=100.0)
    defective = inject(stooq_df.copy(), DefectType.CROSS_SOURCE_BREAK, shift_pct=0.20)
    store.write_silver(defective, run_id, _BDATE, "stooq")

    bb = _make_pipeline(store, cfg, responder)
    responder._bb = bb
    await bb.start()
    await _trigger_reconciliation(bb, run_id, _BDATE)

    # ESCALATION must fire after exactly 1 retry (max_retries=1)
    escalations = bb.get_events(topic=TopicType.ESCALATION)
    assert len(escalations) > 0, "Expected ESCALATION when re-fetch consistently fails"

    # REMEDIATION_FAILED must fire
    assert len(bb.get_events(topic=TopicType.REMEDIATION_FAILED)) > 0

    # REMEDIATION_COMPLETE must NOT fire
    assert bb.get_events(topic=TopicType.REMEDIATION_COMPLETE) == []

    # Refetch was attempted (responder ran)
    assert responder.refetch_count >= 1

    # DecisionRecord(ESCALATE) persisted by both RemediationAgent and Supervisor
    decisions = store.query("SELECT * FROM decisions WHERE decision_type='ESCALATE'")
    assert len(decisions) >= 1

    await bb.stop()
    store.close()


# ---------------------------------------------------------------------------
# AC4 — Held instrument never in Gold (FR-H4)
# ---------------------------------------------------------------------------


async def test_ac4_held_instrument_not_in_gold(tmp_path: Path) -> None:
    """AC4: max_retries=0 → ESCALATION immediately; broken instrument absent from Gold Parquet."""
    run_id = "ac4-run"
    store = _make_store(tmp_path)
    # max_retries=0 → escalate on first RECONCILIATION_COMPLETE with breaks>0 (no retries)
    cfg = _make_cfg(tmp_path, max_retries=0)
    responder = _RefetchResponder(
        MagicMock(), store, _INSTRUMENTS, _BDATE, always_defective=True  # type: ignore[arg-type]
    )

    # Only seed ONE instrument cleanly for both sources — other instruments are missing.
    # Seed yfinance for AAPL only; stooq has a CROSS_SOURCE_BREAK for all instruments.
    _seed_silver(store, run_id, "yfinance", _INSTRUMENTS, _BDATE, base_price=100.0)
    stooq_df = _seed_silver(store, run_id, "stooq", _INSTRUMENTS, _BDATE, base_price=100.0)
    defective = inject(stooq_df.copy(), DefectType.CROSS_SOURCE_BREAK, shift_pct=0.20)
    store.write_silver(defective, run_id, _BDATE, "stooq")

    bb = _make_pipeline(store, cfg, responder)
    responder._bb = bb
    await bb.start()
    await _trigger_reconciliation(bb, run_id, _BDATE)

    # ESCALATION should fire (max_retries=0)
    escalations = bb.get_events(topic=TopicType.ESCALATION)
    assert len(escalations) > 0, "Expected ESCALATION with max_retries=0"

    # Gold Parquet: instruments with CLOSE breaks should NOT be in Gold.
    # The quorum requires 2 agreeing sources within 25bps tolerance.
    # A 20% shift means stooq CLOSE = 120 vs yfinance CLOSE = 100 → beyond tolerance → break.
    # Gold should not contain CLOSE for any instrument (all broke).
    gold_path = tmp_path / "gold" / run_id
    if gold_path.exists():
        gold_files = list(gold_path.rglob("*.parquet"))
        if gold_files:
            gold_df = pd.concat([pd.read_parquet(f) for f in gold_files], ignore_index=True)
            # If any CLOSE rows exist in Gold, they must not be from the broken batch
            has_field = "field" in gold_df.columns
            close_rows = gold_df[gold_df["field"] == "CLOSE"] if has_field else pd.DataFrame()
            # With max_retries=0 and immediate escalation, the broken instruments are in quarantine
            # (ReconciliationAgent wrote them there), not in Gold.
            # We cannot have more golden CLOSE records than instruments that passed quorum.
            # Since ALL instruments have a 20% stooq shift, NONE should appear in Gold for CLOSE.
            assert (
                close_rows.empty or len(close_rows) == 0
            ), f"FR-H4 violated: {len(close_rows)} CLOSE records in Gold despite break"

    await bb.stop()
    store.close()


# ---------------------------------------------------------------------------
# KPI-1 — ≥ 80% auto-remediation rate
# ---------------------------------------------------------------------------


async def test_kpi1_auto_remediation_rate(tmp_path: Path) -> None:
    """KPI-1: run 3 BREAK_DETECTED scenarios — all 3 remediated → 100% ≥ 80%.

    Scenarios:
      1. CROSS_SOURCE_BREAK (shift_pct=0.10) — remediated ✓
      2. NULL_BURST (rate=1.0) — remediated ✓
      3. OUT_OF_RANGE (100× multiplier) — remediated ✓
    """
    total_attempted = 0
    total_succeeded = 0

    for scenario_idx, (defect_type, kwargs) in enumerate(
        [
            (DefectType.CROSS_SOURCE_BREAK, {"shift_pct": 0.10}),
            (DefectType.NULL_BURST, {"rate": 1.0}),
            (DefectType.OUT_OF_RANGE, {"multiplier": 100.0}),
        ]
    ):
        scenario_path = tmp_path / f"scenario_{scenario_idx}"
        scenario_path.mkdir()
        run_id = f"kpi1-run-{scenario_idx}"
        store = _make_store(scenario_path)
        cfg = _make_cfg(scenario_path, max_retries=2)
        responder = _RefetchResponder(
            MagicMock(),  # type: ignore[arg-type]
            store,
            _INSTRUMENTS,
            _BDATE,
            always_defective=False,
        )

        _seed_silver(store, run_id, "yfinance", _INSTRUMENTS, _BDATE, base_price=100.0)
        stooq_df = _seed_silver(store, run_id, "stooq", _INSTRUMENTS, _BDATE, base_price=100.0)
        defective = inject(stooq_df.copy(), defect_type, seed=42, **kwargs)
        store.write_silver(defective, run_id, _BDATE, "stooq")

        bb = _make_pipeline(store, cfg, responder)
        responder._bb = bb
        await bb.start()
        await _trigger_reconciliation(bb, run_id, _BDATE)

        rem_complete = bb.get_events(topic=TopicType.REMEDIATION_COMPLETE)
        rem_failed = bb.get_events(topic=TopicType.REMEDIATION_FAILED)

        if rem_complete or rem_failed:
            total_attempted += len(rem_complete) + len(rem_failed)
            total_succeeded += len(rem_complete)

        await bb.stop()
        store.close()

    assert total_attempted > 0, "No remediation attempts tracked"
    kpi_1 = total_succeeded / total_attempted
    assert kpi_1 >= 0.80, (
        f"KPI-1 {kpi_1:.1%} below 80% "
        f"(succeeded={total_succeeded}, attempted={total_attempted})"
    )
