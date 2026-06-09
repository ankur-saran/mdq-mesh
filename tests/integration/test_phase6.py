"""Phase 6 acceptance criteria integration tests (PRD §12).

AC1 — Every Gold value traces to a decision_id and its source inputs.
AC2 — Scorecard renders offline: HTML has no external assets; JSON is valid.
AC3 / KPI-6 — mdq replay produces bit-identical Gold for any run with stored data.
Bonus — Scorecard overall_status is GREEN on a clean run.

Architecture:
- Source agents are NOT registered — Silver is seeded directly via store.write_silver().
- LineageAgent is registered and receives RUN_COMPLETE with business_date payload.
- Replay tests import _replay_run() directly from mdq.cli.
"""

from __future__ import annotations

import asyncio
import json
from datetime import date
from pathlib import Path

import yaml

from harness.inject import build_silver_history
from mdq.agents.anomaly_agent import AnomalyAgent
from mdq.agents.contract_agent import ContractAgent
from mdq.agents.corporate_actions_agent import CorporateActionsAgent
from mdq.agents.dq_agent import DQAgent
from mdq.agents.lineage_agent import LineageAgent
from mdq.agents.reconciliation_agent import ReconciliationAgent
from mdq.agents.remediation_agent import RemediationAgent
from mdq.agents.supervisor import Supervisor
from mdq.cli import _replay_run
from mdq.core.blackboard import Blackboard
from mdq.core.config import Config
from mdq.core.events import Event, TopicType
from mdq.core.store import MedallionStore

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_BDATE = date(2024, 2, 20)
_INSTRUMENTS = ["AAPL", "MSFT", "NVDA"]
_RUN_ID = "phase6-int-run-001"

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


def _make_cfg(tmp_path: Path) -> Config:
    base = yaml.safe_load(Path("config/default.yaml").read_text())
    base["runtime"]["use_fixtures"] = False
    base["runtime"]["storage"]["bronze"] = str(tmp_path / "bronze")
    base["runtime"]["storage"]["silver"] = str(tmp_path / "silver")
    base["runtime"]["storage"]["gold"] = str(tmp_path / "gold")
    base["runtime"]["storage"]["quarantine"] = str(tmp_path / "quarantine")
    base["runtime"]["storage"]["lineage"] = str(tmp_path / "lineage")
    base["runtime"]["duckdb_path"] = str(tmp_path / "mdq.duckdb")
    universe = yaml.safe_load(Path("config/universe.yaml").read_text())
    base["universe"] = universe
    return Config.model_validate(base)


def _seed_silver(
    store: MedallionStore,
    run_id: str,
    source_id: str,
    business_date: date,
    seed: int = 42,
) -> None:
    df = build_silver_history(
        instruments=_INSTRUMENTS,
        n_days=1,
        end_date=business_date,
        base_price=100.0,
        source_id=source_id,
        seed=seed,
    )
    store.write_silver(df, run_id, business_date, source_id)


def _make_pipeline(store: MedallionStore, cfg: Config) -> Blackboard:
    """7-agent pipeline with LineageAgent. No source agents — Silver seeded directly."""
    bb = Blackboard(db_path=":memory:")
    bb.register(ContractAgent(bb, store, cfg))
    bb.register(DQAgent(bb, store, cfg))
    bb.register(AnomalyAgent(bb, store, cfg))
    bb.register(CorporateActionsAgent(bb, store, cfg))
    bb.register(ReconciliationAgent(bb, store, cfg))
    bb.register(RemediationAgent(bb, store, cfg))
    bb.register(Supervisor(bb, store, cfg))
    bb.register(LineageAgent(bb, store, cfg))
    return bb


async def _trigger_and_complete(
    bb: Blackboard,
    run_id: str,
    business_date: date,
    sources: tuple[str, ...] = ("yfinance", "stooq"),
) -> None:
    """Publish DQ_PASSED for each source, drain, then publish RUN_COMPLETE."""
    for src in sources:
        await bb.publish(
            Event(
                topic=TopicType.DQ_PASSED,
                agent="test_harness",
                run_id=run_id,
                payload={"source_id": src, "business_date": business_date.isoformat()},
            )
        )
    await bb.drain()
    await bb.publish(
        Event(
            topic=TopicType.RUN_COMPLETE,
            agent="supervisor",
            run_id=run_id,
            payload={"business_date": business_date.isoformat()},
        )
    )
    await bb.drain()


async def _run_clean_pipeline(
    store: MedallionStore,
    cfg: Config,
    run_id: str = _RUN_ID,
    business_date: date = _BDATE,
) -> None:
    """Seed clean Silver for both sources and run the full pipeline to completion."""
    _seed_silver(store, run_id, "yfinance", business_date, seed=42)
    _seed_silver(store, run_id, "stooq", business_date, seed=42)

    bb = _make_pipeline(store, cfg)
    await bb.start()
    try:
        await _trigger_and_complete(bb, run_id, business_date)
    finally:
        await bb.stop()


# ---------------------------------------------------------------------------
# AC1 — every Gold value traces to a decision_id and its source inputs
# ---------------------------------------------------------------------------


def test_ac1_gold_traces_to_decision_id(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)
    asyncio.run(_run_clean_pipeline(store, cfg))

    gold_df = store.read_gold(_RUN_ID, _BDATE)
    assert not gold_df.empty, "Gold must be non-empty after a clean pipeline run"

    for _, row in gold_df.iterrows():
        decision_id = str(row["decision_id"])
        assert decision_id, f"Gold row {row['instrument_id']}/{row['field']} has no decision_id"
        decision = store.get_decision(decision_id)
        assert decision is not None, f"decision_id={decision_id} not found in decisions table"
        assert decision["inputs"], f"decision {decision_id} has empty inputs"

    store.close()


# ---------------------------------------------------------------------------
# AC2 — scorecard renders offline (HTML + JSON)
# ---------------------------------------------------------------------------


def test_ac2_scorecard_html_renders_offline(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)
    asyncio.run(_run_clean_pipeline(store, cfg))

    html_file = tmp_path / "lineage" / _RUN_ID / f"{_BDATE}.html"
    assert html_file.exists(), "HTML scorecard file must be created"
    html = html_file.read_text(encoding="utf-8")
    assert "http://" not in html, "HTML must not reference external http:// URLs"
    assert "https://" not in html, "HTML must not reference external https:// URLs"
    assert "<table" in html, "HTML must contain a table element"
    assert "</html>" in html, "HTML must be well-formed"

    store.close()


def test_ac2_scorecard_json_valid(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)
    asyncio.run(_run_clean_pipeline(store, cfg))

    json_file = tmp_path / "lineage" / _RUN_ID / f"{_BDATE}.json"
    assert json_file.exists(), "JSON scorecard file must be created"
    parsed = json.loads(json_file.read_text(encoding="utf-8"))
    for key in ("run_id", "business_date", "overall_status", "summary", "lineage"):
        assert key in parsed, f"JSON output must contain key '{key}'"
    assert parsed["run_id"] == _RUN_ID
    assert isinstance(parsed["lineage"], list)
    assert len(parsed["lineage"]) > 0, "Lineage array must be non-empty"

    store.close()


# ---------------------------------------------------------------------------
# AC3 / KPI-6 — replay is bit-identical
# ---------------------------------------------------------------------------


def test_ac3_replay_bit_identical(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)
    asyncio.run(_run_clean_pipeline(store, cfg))
    store.close()

    # Run replay against the same config/run_id
    config_path = tmp_path / "test_config.yaml"
    base = yaml.safe_load(Path("config/default.yaml").read_text())
    base["runtime"]["use_fixtures"] = False
    base["runtime"]["storage"]["bronze"] = str(tmp_path / "bronze")
    base["runtime"]["storage"]["silver"] = str(tmp_path / "silver")
    base["runtime"]["storage"]["gold"] = str(tmp_path / "gold")
    base["runtime"]["storage"]["quarantine"] = str(tmp_path / "quarantine")
    base["runtime"]["storage"]["lineage"] = str(tmp_path / "lineage")
    base["runtime"]["duckdb_path"] = str(tmp_path / "mdq.duckdb")
    universe = yaml.safe_load(Path("config/universe.yaml").read_text())
    base["universe"] = universe
    config_path.write_text(yaml.dump(base))

    passed = asyncio.run(_replay_run(config_path, _RUN_ID))
    assert passed, "Replay must produce bit-identical Gold (KPI-6)"


# ---------------------------------------------------------------------------
# Bonus — scorecard status GREEN on clean run
# ---------------------------------------------------------------------------


def test_scorecard_status_green_on_clean_run(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    cfg = _make_cfg(tmp_path)
    asyncio.run(_run_clean_pipeline(store, cfg))

    records = store.read_scorecard(_RUN_ID)
    assert records, "At least one scorecard record must be written"
    assert (
        records[0].overall_status.value == "GREEN"
    ), f"Clean run must produce GREEN scorecard, got {records[0].overall_status}"

    store.close()
