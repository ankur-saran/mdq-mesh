"""Phase 0 acceptance criteria (PRD §12 Phase 0).

Each test corresponds to one gate in the acceptance criteria:
  Gate 1 — `mdq run --help` exits 0
  Gate 2 — empty pipeline boots and completes without error
  Gate 3 — Bronze/Silver/Gold/Quarantine/Lineage dirs initialise
  Gate 4 — blackboard persists a RUN_STARTED event readable from DuckDB
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).parent.parent.parent


def test_gate1_cli_help_exits_zero() -> None:
    """Gate 1: `python -m mdq run --help` exits 0 and mentions the command."""
    result = subprocess.run(
        [sys.executable, "-m", "mdq", "run", "--help"],
        capture_output=True,
        cwd=str(PROJECT_ROOT),
    )
    assert result.returncode == 0, (
        f"Expected exit 0, got {result.returncode}.\n" f"stderr: {result.stderr.decode()}"
    )
    output = result.stdout.decode().lower()
    assert "run" in output


def test_gate2_empty_pipeline_boots(tmp_path: Path) -> None:
    """Gate 2: pipeline starts and completes with 0 agents registered."""
    import asyncio

    from mdq.core.blackboard import Blackboard
    from mdq.core.events import Event, TopicType

    db = str(tmp_path / "events.duckdb")
    bb = Blackboard(db_path=db)

    async def _run() -> None:
        await bb.start()
        try:
            await bb.publish(Event(topic=TopicType.RUN_STARTED, agent="supervisor", run_id="r-ac"))
            await bb.publish(Event(topic=TopicType.RUN_COMPLETE, agent="supervisor", run_id="r-ac"))
        finally:
            await bb.stop()

    asyncio.run(_run())


def test_gate3_medallion_dirs_initialise(tmp_path: Path) -> None:
    """Gate 3: init_dirs() creates all five medallion directories."""
    from mdq.core.store import MedallionStore

    store = MedallionStore(
        bronze_root=tmp_path / "bronze",
        silver_root=tmp_path / "silver",
        gold_root=tmp_path / "gold",
        quarantine_root=tmp_path / "quarantine",
        lineage_root=tmp_path / "lineage",
        duckdb_path=tmp_path / "mdq.duckdb",
    )
    store.init_dirs()

    for d in ("bronze", "silver", "gold", "quarantine", "lineage"):
        assert (tmp_path / d).is_dir(), f"Missing directory: {d}"


def test_gate4_event_persisted_to_duckdb(tmp_path: Path) -> None:
    """Gate 4: a published event is readable from DuckDB after the run ends."""
    import asyncio

    from mdq.core.blackboard import Blackboard
    from mdq.core.events import Event, TopicType

    db_path = str(tmp_path / "events.duckdb")
    bb = Blackboard(db_path=db_path)

    async def _run() -> None:
        await bb.start()
        try:
            await bb.publish(
                Event(topic=TopicType.RUN_STARTED, agent="supervisor", run_id="r-persist")
            )
        finally:
            await bb.stop()

    asyncio.run(_run())

    # Re-open the DuckDB file independently and verify the row is there
    conn = duckdb.connect(db_path, read_only=True)
    try:
        count = conn.execute("SELECT count(*) FROM events WHERE run_id = 'r-persist'").fetchone()
        assert count is not None
        assert count[0] == 1
    finally:
        conn.close()
