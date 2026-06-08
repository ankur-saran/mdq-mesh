"""MedallionStore: Bronze/Silver/Gold/Quarantine Parquet + DuckDB lineage (FR-S1–FR-S5).

# DESIGN-NOTE: All Parquet writes use an atomic temp-file → rename pattern so a
# crashed write never leaves a partial file visible to readers (FR-S1 immutability).
"""

from __future__ import annotations

import json
import tempfile
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from mdq.core.schemas import DecisionRecord, GoldenRecord
from mdq.utils.logging import get_logger

log = get_logger("store")

_CREATE_DECISIONS_TABLE = """
CREATE TABLE IF NOT EXISTS decisions (
    decision_id   TEXT PRIMARY KEY,
    ts            TIMESTAMPTZ NOT NULL,
    agent         TEXT NOT NULL,
    instrument_id TEXT NOT NULL,
    business_date DATE NOT NULL,
    decision_type TEXT NOT NULL,
    inputs        TEXT NOT NULL,
    outcome       TEXT NOT NULL,
    rule_applied  TEXT NOT NULL,
    verified      BOOLEAN NOT NULL
)
"""


class MedallionStore:
    """Bronze/Silver/Gold/Quarantine Parquet writers + DuckDB decision/lineage log."""

    def __init__(
        self,
        bronze_root: Path,
        silver_root: Path,
        gold_root: Path,
        quarantine_root: Path,
        lineage_root: Path,
        duckdb_path: Path,
    ) -> None:
        self._bronze = bronze_root
        self._silver = silver_root
        self._gold = gold_root
        self._quarantine = quarantine_root
        self._lineage = lineage_root
        self._duckdb_path = duckdb_path
        self._conn: duckdb.DuckDBPyConnection | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def open(self) -> None:
        """Open the DuckDB lineage connection and create the decisions table."""
        self._conn = duckdb.connect(str(self._duckdb_path))
        self._conn.execute(_CREATE_DECISIONS_TABLE)

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def init_dirs(self) -> None:
        """Create all medallion directories if they do not already exist (Phase 0 AC)."""
        for root in (
            self._bronze,
            self._silver,
            self._gold,
            self._quarantine,
            self._lineage,
        ):
            root.mkdir(parents=True, exist_ok=True)
        log.info("Medallion directories ready under %s", self._bronze.parent)

    # ------------------------------------------------------------------
    # Bronze (FR-S1 — immutable raw landing zone)
    # ------------------------------------------------------------------

    def write_bronze(
        self,
        source_id: str,
        run_id: str,
        df: pd.DataFrame,
        business_date: date | None = None,
    ) -> Path:
        """Write raw Bronze Parquet. Skips silently if the file already exists (FR-S1)."""
        d = (business_date or date.today()).isoformat()
        dest = self._bronze / source_id / run_id / f"{d}.parquet"
        if dest.exists():
            log.warning("Bronze already exists — skipping write: %s", dest)
            return dest
        return self._write_parquet(df, dest)

    # ------------------------------------------------------------------
    # Path helpers (read-only, no I/O)
    # ------------------------------------------------------------------

    def bronze_path(self, source_id: str, run_id: str, business_date: date) -> Path:
        """Return the expected Bronze Parquet path without performing I/O."""
        return self._bronze / source_id / run_id / f"{business_date.isoformat()}.parquet"

    def silver_path(self, run_id: str, business_date: date, source_id: str) -> Path:
        """Return the expected Silver Parquet path without performing I/O."""
        return self._silver / run_id / source_id / f"{business_date.isoformat()}.parquet"

    def read_silver_window(
        self,
        business_date: date,
        window_days: int,
        source_id: str | None = None,
    ) -> pd.DataFrame:
        """Read all Silver rows for the rolling window ending on business_date (inclusive).

        # DESIGN-NOTE: FR-A5 — uses pathlib rglob + pandas concat instead of DuckDB glob
        # for Windows path portability (NFR-7). Window sizes are small (≤ 60 days × 30 rows).
        # DESIGN-NOTE: FR-A6 — source_id filter ensures AnomalyAgent compares each source
        # against its own history only, preventing false anomalies from inter-source bias.
        """
        from datetime import timedelta

        cutoff = business_date - timedelta(days=window_days)
        frames: list[pd.DataFrame] = []
        for p in sorted(self._silver.rglob("*.parquet")):
            df = pd.read_parquet(p)
            if "business_date" not in df.columns:
                continue
            if source_id is not None and "source_id" in df.columns:
                df = df[df["source_id"] == source_id]
            df["business_date"] = pd.to_datetime(df["business_date"])
            mask = (df["business_date"].dt.date >= cutoff) & (
                df["business_date"].dt.date <= business_date
            )
            filtered = df[mask]
            if not filtered.empty:
                frames.append(filtered)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    # ------------------------------------------------------------------
    # Silver (FR-S2)
    # ------------------------------------------------------------------

    def write_silver(
        self,
        df: pd.DataFrame,
        run_id: str,
        business_date: date | None = None,
        source_id: str = "unknown",
    ) -> Path:
        d = (business_date or date.today()).isoformat()
        dest = self._silver / run_id / source_id / f"{d}.parquet"
        return self._write_parquet(df, dest)

    # ------------------------------------------------------------------
    # Gold (FR-S3)
    # ------------------------------------------------------------------

    def write_gold(
        self,
        records: list[GoldenRecord],
        run_id: str,
        business_date: date | None = None,
    ) -> Path:
        if not records:
            raise ValueError("Cannot write an empty Gold batch.")
        rows = []
        for r in records:
            row = r.model_dump()
            # Serialise list columns to JSON strings for Parquet portability
            row["quorum_sources"] = json.dumps(row["quorum_sources"])
            row["dissenting_sources"] = json.dumps(row["dissenting_sources"])
            # Decimal → str for Parquet (precise round-trip)
            row["golden_value"] = str(row["golden_value"])
            rows.append(row)
        df = pd.DataFrame(rows)
        d = (business_date or date.today()).isoformat()
        dest = self._gold / run_id / f"{d}.parquet"
        return self._write_parquet(df, dest)

    # ------------------------------------------------------------------
    # Quarantine (FR-S5)
    # ------------------------------------------------------------------

    def write_quarantine(
        self,
        df: pd.DataFrame,
        reason: str,
        run_id: str,
        business_date: date | None = None,
    ) -> Path:
        d = (business_date or date.today()).isoformat()
        dest = self._quarantine / run_id / reason / f"{d}.parquet"
        tagged = df.copy()
        tagged["quarantine_reason"] = reason
        tagged["quarantine_ts"] = datetime.now(tz=UTC).isoformat()
        return self._write_parquet(tagged, dest)

    # ------------------------------------------------------------------
    # Decision / Lineage log (C-4, FR-O3)
    # ------------------------------------------------------------------

    def write_decision(self, record: DecisionRecord) -> None:
        """Persist a decision record to the DuckDB lineage log."""
        conn = self._require_conn()
        conn.execute(
            """
            INSERT INTO decisions
                (decision_id, ts, agent, instrument_id, business_date,
                 decision_type, inputs, outcome, rule_applied, verified)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT DO NOTHING
            """,
            [
                record.decision_id,
                record.ts.isoformat(),
                record.agent,
                record.instrument_id,
                record.business_date.isoformat(),
                record.decision_type.value,
                json.dumps(record.inputs),
                json.dumps(record.outcome),
                record.rule_applied,
                record.verified,
            ],
        )

    def get_decision(self, decision_id: str) -> dict[str, Any] | None:
        """Look up a single decision by its ID (used by Lineage Agent in Phase 6)."""
        conn = self._require_conn()
        row = conn.execute(
            "SELECT * FROM decisions WHERE decision_id = ?", [decision_id]
        ).fetchone()
        if row is None:
            return None
        cols = [
            "decision_id",
            "ts",
            "agent",
            "instrument_id",
            "business_date",
            "decision_type",
            "inputs",
            "outcome",
            "rule_applied",
            "verified",
        ]
        return dict(zip(cols, row, strict=True))

    # ------------------------------------------------------------------
    # Generic DuckDB query (for replay and audit — FR-O3, KPI-6)
    # ------------------------------------------------------------------

    def query(self, sql: str) -> pd.DataFrame:
        """Execute arbitrary SQL against the DuckDB lineage database."""
        conn = self._require_conn()
        return conn.execute(sql).df()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _write_parquet(self, df: pd.DataFrame, dest: Path) -> Path:
        dest.parent.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pandas(df, preserve_index=False)
        # Atomic write: temp file in same directory → rename
        with tempfile.NamedTemporaryFile(
            dir=dest.parent, suffix=".parquet.tmp", delete=False
        ) as tmp:
            tmp_path = Path(tmp.name)
        try:
            pq.write_table(table, tmp_path)  # type: ignore[no-untyped-call]
            tmp_path.rename(dest)
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise
        log.debug("wrote %d rows → %s", len(df), dest)
        return dest

    def _require_conn(self) -> duckdb.DuckDBPyConnection:
        if self._conn is None:
            raise RuntimeError("MedallionStore is not open. Call open() before writing.")
        return self._conn
