"""FR-T2: Frozen-fixture support — snapshot and load offline datasets.

Fixtures are stored as Parquet files in the same format as Bronze so that
source agents can swap between live fetch and fixture load via a single
config flag (runtime.use_fixtures) with no code-path differences.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Module-level root so tests can monkeypatch it
_FIXTURES_DIR: Path = Path(__file__).parent


def snapshot(source_id: str, df: pd.DataFrame, tag: str = "default") -> Path:
    """Write *df* as a frozen fixture Parquet file.

    Args:
        source_id: Logical source name (e.g. "yfinance").
        df:        DataFrame to snapshot (Bronze/canonical format).
        tag:       Identifies the fixture variant (e.g. "2024-01-02").
    """
    dest = _FIXTURES_DIR / source_id / f"{tag}.parquet"
    dest.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, dest)  # type: ignore[no-untyped-call]
    return dest


def load_fixture(source_id: str, tag: str = "default") -> pd.DataFrame:
    """Load a frozen fixture for offline/deterministic replay (FR-T2)."""
    path = _FIXTURES_DIR / source_id / f"{tag}.parquet"
    if not path.exists():
        raise FileNotFoundError(
            f"No fixture for source {source_id!r} (tag={tag!r}) at {path}. "
            "Create one with snapshot() or run `mdq inject <scenario>` first."
        )
    return pd.read_parquet(path)


def list_fixtures() -> list[str]:
    """Return all available fixture identifiers as 'source_id/tag' strings."""
    return [
        f"{p.parent.name}/{p.stem}"
        for p in _FIXTURES_DIR.rglob("*.parquet")
        if p.parent != _FIXTURES_DIR
    ]
