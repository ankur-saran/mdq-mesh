"""CLI entrypoint: run / run-agent / replay / inject (FR-O4).

Invoke as `mdq <command>` (installed) or `python -m mdq <command>` (editable).
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import date, timedelta
from pathlib import Path
from typing import Annotated

import typer

app = typer.Typer(
    name="mdq",
    help="Market-Data Quality & Reconciliation Mesh — local, keyless, self-healing.",
    add_completion=False,
)


@app.command()
def run(
    config: Annotated[
        Path,
        typer.Option("--config", "-c", help="Path to YAML config file."),
    ] = Path("config/default.yaml"),
    date_str: Annotated[
        str | None,
        typer.Option("--date", "-d", help="Business date YYYY-MM-DD (default: yesterday)."),
    ] = None,
) -> None:
    """Run the full mdq-mesh pipeline end-to-end."""
    business_date = date.fromisoformat(date_str) if date_str else date.today() - timedelta(days=1)
    asyncio.run(_run_pipeline(config, business_date))


async def _run_pipeline(config_path: Path, business_date: date | None = None) -> None:
    from mdq.agents.anomaly_agent import AnomalyAgent
    from mdq.agents.contract_agent import ContractAgent
    from mdq.agents.corporate_actions_agent import CorporateActionsAgent
    from mdq.agents.dq_agent import DQAgent
    from mdq.agents.ingestion.ecb_agent import ECBAgent
    from mdq.agents.ingestion.sec_edgar_agent import SECEdgarAgent
    from mdq.agents.ingestion.stooq_agent import StooqAgent
    from mdq.agents.ingestion.yfinance_agent import YFinanceAgent
    from mdq.agents.lineage_agent import LineageAgent
    from mdq.agents.reconciliation_agent import ReconciliationAgent
    from mdq.agents.remediation_agent import RemediationAgent
    from mdq.agents.supervisor import Supervisor
    from mdq.core.blackboard import Blackboard
    from mdq.core.config import load_config
    from mdq.core.events import Event, TopicType
    from mdq.core.store import MedallionStore
    from mdq.utils.logging import configure_root, get_logger

    configure_root("INFO")
    log = get_logger("cli.run")

    if business_date is None:
        business_date = date.today() - timedelta(days=1)

    cfg = load_config(config_path)
    run_id = str(uuid.uuid4())
    log.info("Starting run %s for %s", run_id, business_date)

    store = MedallionStore(
        bronze_root=Path(cfg.runtime.storage.bronze),
        silver_root=Path(cfg.runtime.storage.silver),
        gold_root=Path(cfg.runtime.storage.gold),
        quarantine_root=Path(cfg.runtime.storage.quarantine),
        lineage_root=Path(cfg.runtime.storage.lineage),
        duckdb_path=Path(cfg.runtime.duckdb_path),
    )
    store.init_dirs()
    store.open()

    bb = Blackboard(db_path=str(Path(cfg.runtime.duckdb_path)))

    bb.register(YFinanceAgent(bb, store, cfg))
    bb.register(StooqAgent(bb, store, cfg))
    # DESIGN-NOTE: KPI-7 — reference-data agents registered conditionally from config;
    # they publish REFERENCE_DATA_COMPLETE and never interact with the price pipeline.
    if cfg.sources.ecb.enabled:
        bb.register(ECBAgent(bb, store, cfg))
    if cfg.sources.sec_edgar.enabled:
        bb.register(SECEdgarAgent(bb, store, cfg))
    bb.register(ContractAgent(bb, store, cfg))
    bb.register(DQAgent(bb, store, cfg))
    bb.register(AnomalyAgent(bb, store, cfg))
    # DESIGN-NOTE: FR-A4 — CorporateActionsAgent MUST be registered before
    # ReconciliationAgent so back-adjustment completes before _reconcile() reads Silver.
    bb.register(CorporateActionsAgent(bb, store, cfg))
    bb.register(ReconciliationAgent(bb, store, cfg))
    # DESIGN-NOTE: FR-A7/FR-A9 — RemediationAgent and Supervisor registered after
    # ReconciliationAgent so they receive BREAK_DETECTED/RECONCILIATION_COMPLETE from Recon.
    bb.register(RemediationAgent(bb, store, cfg))
    bb.register(Supervisor(bb, store, cfg))
    # DESIGN-NOTE: FR-A8 — LineageAgent registered last so RUN_COMPLETE is processed
    # after Supervisor clears its per-run state (ordering is cosmetic here since they
    # are independent subscribers, but last = semantically "after all run work is done").
    bb.register(LineageAgent(bb, store, cfg))

    try:
        await bb.start()
        await bb.publish(
            Event(
                topic=TopicType.RUN_STARTED,
                agent="supervisor",
                run_id=run_id,
                payload={"business_date": business_date.isoformat()},
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
        log.info("Run %s complete", run_id)
    finally:
        await bb.stop()
        store.close()


@app.command(name="run-agent")
def run_agent(
    name: Annotated[str, typer.Argument(help="Agent name to run in isolation.")],
    config: Annotated[
        Path,
        typer.Option("--config", "-c", help="Path to YAML config file."),
    ] = Path("config/default.yaml"),
) -> None:
    """Run a single named agent in isolation (implemented per agent in Phase 1+)."""
    typer.echo(f"[Phase 0] run-agent {name!r} — no agents implemented yet.")


@app.command()
def replay(
    run_id: Annotated[str, typer.Argument(help="Run ID to replay.")],
    config: Annotated[
        Path,
        typer.Option("--config", "-c", help="Path to YAML config file."),
    ] = Path("config/default.yaml"),
) -> None:
    """Replay a frozen run deterministically and verify bit-identical Gold (C-4, C-5, KPI-6)."""
    passed = asyncio.run(_replay_run(config, run_id))
    if not passed:
        raise typer.Exit(code=1)


async def _replay_run(config_path: Path, run_id: str) -> bool:
    """Replay frozen Silver through CorporateActions+Reconciliation; compare Gold deterministically.

    # DESIGN-NOTE: KPI-6 — replay skips ingestion/DQ/Anomaly agents. The invariant tested
    # is: given identical Silver inputs, CorporateActions+Reconciliation produce identical Gold.
    # Upstream agents are deterministic by C-5 and covered by their own unit tests.
    # DESIGN-NOTE: C-4 — comparison excludes decision_id (new UUIDs generated each run) and
    # timestamps. Only instrument_id, field, golden_value, confidence, quorum_sources, and
    # dissenting_sources are compared (the data-bearing fields).
    """
    import tempfile

    import pandas as pd

    from mdq.agents.corporate_actions_agent import CorporateActionsAgent
    from mdq.agents.reconciliation_agent import ReconciliationAgent
    from mdq.core.blackboard import Blackboard
    from mdq.core.config import load_config
    from mdq.core.events import Event, TopicType
    from mdq.core.store import MedallionStore
    from mdq.utils.logging import configure_root, get_logger

    configure_root("INFO")
    log = get_logger("cli.replay")

    cfg = load_config(config_path)
    store = MedallionStore(
        bronze_root=Path(cfg.runtime.storage.bronze),
        silver_root=Path(cfg.runtime.storage.silver),
        gold_root=Path(cfg.runtime.storage.gold),
        quarantine_root=Path(cfg.runtime.storage.quarantine),
        lineage_root=Path(cfg.runtime.storage.lineage),
        duckdb_path=Path(cfg.runtime.duckdb_path),
    )
    store.open()

    try:
        silver_sources = store.read_silver_for_replay(run_id)
        if not silver_sources:
            typer.echo(f"[ERROR] No Silver data found for run_id={run_id!r}.")
            return False

        # Determine business_date from Silver content
        sample_df = next(iter(silver_sources.values()))
        dates = pd.to_datetime(sample_df["business_date"]).dt.date.unique()
        business_date = max(dates)

        stored_gold_df = store.read_gold(run_id, business_date)
        if stored_gold_df.empty:
            typer.echo(f"[ERROR] No Gold data found for run_id={run_id!r}.")
            return False

        log.info(
            "Replaying run=%s date=%s (%d sources, %d stored Gold rows)",
            run_id,
            business_date,
            len(silver_sources),
            len(stored_gold_df),
        )
    except Exception:
        store.close()
        raise

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        replay_store = MedallionStore(
            bronze_root=tmp_path / "bronze",
            silver_root=tmp_path / "silver",
            gold_root=tmp_path / "gold",
            quarantine_root=tmp_path / "quarantine",
            lineage_root=tmp_path / "lineage",
            duckdb_path=tmp_path / "replay.duckdb",
        )
        replay_store.init_dirs()
        replay_store.open()

        for source_id, df in silver_sources.items():
            replay_store.write_silver(df, run_id, business_date, source_id)

        bb = Blackboard(db_path=":memory:")
        # DESIGN-NOTE: FR-A4 — CorporateActionsAgent registered before ReconciliationAgent.
        bb.register(CorporateActionsAgent(bb, replay_store, cfg))
        bb.register(ReconciliationAgent(bb, replay_store, cfg))

        await bb.start()
        for source_id in silver_sources:
            await bb.publish(
                Event(
                    topic=TopicType.DQ_PASSED,
                    agent="replay",
                    run_id=run_id,
                    payload={
                        "source_id": source_id,
                        "business_date": business_date.isoformat(),
                    },
                )
            )
        await bb.drain()
        await bb.stop()
        replay_store.close()

        replay_gold_df = replay_store.read_gold(run_id, business_date)

    store.close()

    # Compare deterministic columns only (decision_id UUIDs and timestamps are excluded)
    compare_cols = [
        "instrument_id",
        "field",
        "golden_value",
        "confidence",
        "quorum_sources",
        "dissenting_sources",
    ]
    available = [
        c for c in compare_cols if c in stored_gold_df.columns and c in replay_gold_df.columns
    ]

    def _sort_df(df: pd.DataFrame) -> pd.DataFrame:
        return df[available].sort_values(["instrument_id", "field"]).reset_index(drop=True)

    stored_cmp = _sort_df(stored_gold_df)
    replay_cmp = _sort_df(replay_gold_df)

    if stored_cmp.equals(replay_cmp):
        typer.echo(
            f"PASS — replay of {run_id!r} is bit-identical "
            f"({len(stored_cmp)} Gold records verified)."
        )
        return True
    else:
        typer.echo(
            f"FAIL — replay of {run_id!r} diverged! "
            f"{len(stored_cmp)} stored vs {len(replay_cmp)} replayed."
        )
        return False


@app.command()
def inject(
    scenario: Annotated[str, typer.Argument(help="Defect scenario name (FR-T1).")],
    config: Annotated[
        Path,
        typer.Option("--config", "-c", help="Path to YAML config file."),
    ] = Path("config/default.yaml"),
) -> None:
    """Inject a named test-defect scenario into the harness fixtures (FR-T1)."""
    typer.echo(f"[Phase 0] inject {scenario!r} — full defect injection wired in Phase 1+.")
