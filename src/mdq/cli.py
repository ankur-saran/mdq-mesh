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
    if not cfg.universe.instruments:
        log.warning(
            "Universe is empty — pipeline will boot and complete with zero data. "
            "Add instruments to config/universe.yaml to fetch market data."
        )
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

    from mdq.core.transport.base import Transport
    from mdq.core.transport.inprocess import InProcessTransport

    _transport: Transport
    if cfg.runtime.transport == "redpanda":
        # DESIGN-NOTE: FR-B3 / C-3 — RedpandaTransport is lazily imported so it
        # never appears on the pure-local code path when transport == "asyncio".
        from mdq.core.transport.redpanda import RedpandaTransport

        _transport = RedpandaTransport(
            bootstrap_servers=cfg.runtime.redpanda.bootstrap_servers,
            topic_prefix=cfg.runtime.redpanda.topic_prefix,
            consumer_group=cfg.runtime.redpanda.consumer_group,
        )
        log.info("Using Redpanda transport (%s)", cfg.runtime.redpanda.bootstrap_servers)
    else:
        _transport = InProcessTransport()

    bb = Blackboard(db_path=str(Path(cfg.runtime.duckdb_path)), transport=_transport)

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
    # DESIGN-NOTE: FR-A10 / C-2 — NarratorAgent is optional, edge-only, and NEVER
    # influences data decisions. Registered last so RUN_COMPLETE arrives after all
    # pipeline work is done. Disabled by default (cfg.narrator.enabled = False).
    if cfg.narrator.enabled:
        from mdq.agents.narrator_agent import NarratorAgent

        bb.register(NarratorAgent(bb, store, cfg))

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
    name: Annotated[str, typer.Argument(help="Agent name (yfinance | stooq | ecb | sec_edgar).")],
    config: Annotated[
        Path,
        typer.Option("--config", "-c", help="Path to YAML config file."),
    ] = Path("config/default.yaml"),
    date_str: Annotated[
        str | None,
        typer.Option("--date", "-d", help="Business date YYYY-MM-DD (default: yesterday)."),
    ] = None,
) -> None:
    """Run a single ingestion agent in isolation and write its Bronze output."""
    _INGESTION_AGENTS = {"yfinance", "stooq", "ecb", "sec_edgar"}
    if name not in _INGESTION_AGENTS:
        typer.echo(
            f"[ERROR] run-agent supports ingestion agents only: {sorted(_INGESTION_AGENTS)}.\n"
            f"Downstream agents require upstream data — use `python -m mdq run` instead.",
            err=True,
        )
        raise typer.Exit(code=1)
    business_date = date.fromisoformat(date_str) if date_str else date.today() - timedelta(days=1)
    asyncio.run(_run_single_agent(config, name, business_date))


async def _run_single_agent(config_path: Path, agent_name: str, business_date: date) -> None:
    from mdq.core.blackboard import Blackboard
    from mdq.core.config import load_config
    from mdq.core.events import Event, TopicType
    from mdq.core.store import MedallionStore
    from mdq.core.transport.inprocess import InProcessTransport
    from mdq.utils.logging import configure_root, get_logger

    configure_root("INFO")
    log = get_logger("cli.run-agent")

    cfg = load_config(config_path)
    run_id = str(uuid.uuid4())
    log.info("run-agent %r: run=%s date=%s", agent_name, run_id, business_date)

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

    bb = Blackboard(db_path=str(Path(cfg.runtime.duckdb_path)), transport=InProcessTransport())

    if agent_name == "yfinance":
        from mdq.agents.ingestion.yfinance_agent import YFinanceAgent
        bb.register(YFinanceAgent(bb=bb, store=store, cfg=cfg))
    elif agent_name == "stooq":
        from mdq.agents.ingestion.stooq_agent import StooqAgent
        bb.register(StooqAgent(bb=bb, store=store, cfg=cfg))
    elif agent_name == "ecb":
        from mdq.agents.ingestion.ecb_agent import ECBAgent
        bb.register(ECBAgent(bb=bb, store=store, cfg=cfg))
    else:
        from mdq.agents.ingestion.sec_edgar_agent import SECEdgarAgent
        bb.register(SECEdgarAgent(bb=bb, store=store, cfg=cfg))

    try:
        await bb.start()
        await bb.publish(
            Event(
                topic=TopicType.RUN_STARTED,
                agent="cli",
                run_id=run_id,
                payload={"business_date": business_date.isoformat()},
            )
        )
        await bb.drain()
        log.info("run-agent %r complete (run=%s)", agent_name, run_id)
    finally:
        await bb.stop()
        store.close()


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
    scenario: Annotated[
        str | None,
        typer.Argument(
            help="Defect scenario: clean | null_burst | stale_feed | out_of_range | schema_drift | "
            "split_2to1 | cross_source_break | volatility_regime | mixed_defects"
        ),
    ] = None,
    config: Annotated[
        Path,
        typer.Option("--config", "-c", help="Path to YAML config file."),
    ] = Path("config/default.yaml"),
    freeze_fixtures: Annotated[
        bool,
        typer.Option("--freeze-fixtures", help="Snapshot live data as clean base fixtures (FR-T2)."),
    ] = False,
    date_str: Annotated[
        str | None,
        typer.Option("--date", "-d", help="Business date YYYY-MM-DD (default: yesterday)."),
    ] = None,
) -> None:
    """Inject a named defect into harness fixtures, or snapshot live data as fixtures (FR-T1/FR-T2)."""
    business_date = date.fromisoformat(date_str) if date_str else date.today() - timedelta(days=1)
    if freeze_fixtures:
        asyncio.run(_freeze_fixtures(config, business_date))
    elif scenario:
        asyncio.run(_inject_scenario(config, scenario, business_date))
    else:
        typer.echo(
            "Provide a scenario name or --freeze-fixtures.\n"
            "Scenarios: clean | null_burst | stale_feed | out_of_range | schema_drift | "
            "split_2to1 | cross_source_break | volatility_regime | mixed_defects",
            err=True,
        )
        raise typer.Exit(code=1)


# Scenario → (DefectType value, Bronze-column kwargs)
_BRONZE_SCENARIOS: dict[str, tuple[str, dict[str, object]]] = {
    "null_burst":        ("null_burst",         {"column": "Close", "rate": 0.3}),
    "stale_feed":        ("stale_feed",          {"days_stale": 3}),
    "out_of_range":      ("out_of_range",        {"column": "Close"}),
    "schema_drift":      ("schema_drift",        {"drop": ["Adj Close"]}),
    "split_2to1":        ("unadjusted_split",    {"column": "Close", "ratio": 2.0}),
    "cross_source_break":("cross_source_break",  {"column": "Close"}),
    "volatility_regime": ("volatility_regime",   {"column": "Close", "spike_multiplier": 3.0}),
}


async def _inject_scenario(config_path: Path, scenario: str, business_date: date) -> None:
    """Apply a named defect to each price-source fixture and write default.parquet (FR-T1)."""
    import numpy as np
    import pandas as pd

    from harness.fixtures import _FIXTURES_DIR, snapshot
    from harness.inject import inject as harness_inject
    from mdq.core.config import load_config
    from mdq.utils.logging import configure_root, get_logger

    configure_root("INFO")
    log = get_logger("cli.inject")

    _VALID = sorted(list(_BRONZE_SCENARIOS) + ["clean", "mixed_defects"])
    if scenario not in _VALID:
        typer.echo(
            f"[ERROR] Unknown scenario {scenario!r}. Valid: {_VALID}",
            err=True,
        )
        raise typer.Exit(code=1)

    cfg = load_config(config_path)

    for source_id in ("yfinance", "stooq"):
        # Prefer live-snapshotted clean base; fall back to synthetic generation
        clean_path = _FIXTURES_DIR / source_id / "clean.parquet"
        if clean_path.exists():
            base_df = pd.read_parquet(clean_path)
            log.info("Loaded clean fixture for %s (%d rows)", source_id, len(base_df))
        else:
            base_df = _synthetic_bronze(source_id, cfg, business_date)
            log.info("Generated synthetic Bronze for %s (%d rows)", source_id, len(base_df))

        if scenario == "clean":
            df = base_df
        elif scenario == "mixed_defects":
            df = harness_inject(base_df, "null_burst", seed=42, column="Close", rate=0.2)
            df = harness_inject(df, "out_of_range", seed=7, column="Close")
        else:
            defect_type, params = _BRONZE_SCENARIOS[scenario]
            df = harness_inject(base_df, defect_type, seed=42, **params)

        dest = snapshot(source_id, df, tag="default")
        log.info("Injected %r into %s → %s (%d rows)", scenario, source_id, dest, len(df))

    typer.echo(f"Fixtures ready. Run `python -m mdq run` with use_fixtures=True to observe defects.")


async def _freeze_fixtures(config_path: Path, business_date: date) -> None:
    """Fetch live data from each ingestion agent and snapshot as clean base fixtures (FR-T2)."""
    from mdq.agents.ingestion.stooq_agent import StooqAgent
    from mdq.agents.ingestion.yfinance_agent import YFinanceAgent
    from mdq.core.blackboard import Blackboard
    from mdq.core.config import load_config
    from mdq.core.store import MedallionStore
    from mdq.core.transport.inprocess import InProcessTransport
    from mdq.utils.logging import configure_root, get_logger
    from harness.fixtures import snapshot

    configure_root("INFO")
    log = get_logger("cli.inject.freeze")

    cfg = load_config(config_path)

    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        store = MedallionStore(
            bronze_root=tmp_path / "bronze",
            silver_root=tmp_path / "silver",
            gold_root=tmp_path / "gold",
            quarantine_root=tmp_path / "quarantine",
            lineage_root=tmp_path / "lineage",
            duckdb_path=tmp_path / "freeze.duckdb",
        )
        store.init_dirs()
        store.open()

        bb = Blackboard(db_path=":memory:", transport=InProcessTransport())

        for AgentCls, source_id in [
            (YFinanceAgent, "yfinance"),
            (StooqAgent, "stooq"),
        ]:
            agent = AgentCls(bb=bb, store=store, cfg=cfg)  # type: ignore[operator]
            try:
                df = await agent._load(business_date)  # type: ignore[attr-defined]
            except Exception as exc:
                log.error("Live fetch failed for %s: %s — skipping", source_id, exc)
                continue
            if df.empty:
                log.warning("No data from %s for %s — skipping fixture", source_id, business_date)
                continue
            # Save as both "clean" (backup) and "default" (active)
            snapshot(source_id, df, tag="clean")
            snapshot(source_id, df, tag="default")
            log.info("Snapshotted %s → clean + default (%d rows)", source_id, len(df))

        store.close()

    typer.echo(f"Fixtures frozen for {business_date}. Run `python -m mdq run` with use_fixtures=True.")


def _synthetic_bronze(source_id: str, cfg: object, business_date: date) -> "pd.DataFrame":
    """Generate a deterministic synthetic Bronze DataFrame for *source_id* universe instruments."""
    from datetime import UTC, datetime

    import numpy as np
    import pandas as pd

    rng = np.random.default_rng(42)
    fetch_ts = pd.Timestamp(datetime(business_date.year, business_date.month, business_date.day, 21, 0, 0, tzinfo=UTC))
    rows = []

    for inst in cfg.universe.instruments:  # type: ignore[union-attr]
        if source_id not in inst.symbols:
            continue
        sym = inst.symbols[source_id]
        close = round(float(rng.uniform(50.0, 500.0)), 4)
        rows.append({
            "Date": pd.Timestamp(business_date),
            "Open": round(float(close * (1.0 + float(rng.normal(0, 0.005)))), 4),
            "High": round(float(close * (1.0 + abs(float(rng.normal(0, 0.01))))), 4),
            "Low":  round(float(close * (1.0 - abs(float(rng.normal(0, 0.01))))), 4),
            "Close": close,
            "Adj Close": close,
            "Volume": int(rng.integers(100_000, 10_000_000)),
            "instrument_id": inst.instrument_id,
            "source_symbol": sym,
            "fetch_ts": fetch_ts,
        })

    return pd.DataFrame(rows)
