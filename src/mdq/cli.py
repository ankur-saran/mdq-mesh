"""CLI entrypoint: run / run-agent / replay / inject (FR-O4).

Invoke as `mdq <command>` (installed) or `python -m mdq <command>` (editable).
"""

from __future__ import annotations

import asyncio
import uuid
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
) -> None:
    """Run the full mdq-mesh pipeline end-to-end."""
    asyncio.run(_run_pipeline(config))


async def _run_pipeline(config_path: Path) -> None:
    from mdq.core.blackboard import Blackboard
    from mdq.core.config import load_config
    from mdq.core.events import Event, TopicType
    from mdq.core.store import MedallionStore
    from mdq.utils.logging import configure_root, get_logger

    configure_root("INFO")
    log = get_logger("cli.run")

    cfg = load_config(config_path)
    run_id = str(uuid.uuid4())
    log.info("Starting run %s", run_id)

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

    try:
        await bb.start()
        await bb.publish(Event(topic=TopicType.RUN_STARTED, agent="supervisor", run_id=run_id))
        # Phase 0: skeleton only — no agents registered; pipeline boots and completes.
        await bb.publish(Event(topic=TopicType.RUN_COMPLETE, agent="supervisor", run_id=run_id))
        log.info("Run %s complete (Phase 0 skeleton — 0 agents active)", run_id)
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
    """Replay a frozen run deterministically from the event log (C-4, C-5, KPI-6).

    Full replay is wired in Phase 6 (Lineage Agent).
    """
    typer.echo(f"[Phase 0] replay {run_id!r} — lineage replay implemented in Phase 6.")


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
