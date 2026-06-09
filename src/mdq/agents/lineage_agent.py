"""Lineage/Catalog Agent — Gold traceability, JSON export, self-contained HTML scorecard (FR-A8).

Subscribes to RUN_COMPLETE. Reads Gold Parquet and the decisions table to build a complete
lineage trace, writes it as JSON and a self-contained offline HTML file, and persists a
ScorecardRecord (FR-O2, FR-O3).

# DESIGN-NOTE: FR-A8 — no run_id column in the decisions table. Lineage is traversed via
# the decision_id FK already embedded in each GoldenRecord (Gold Parquet). Scorecard
# aggregate counts are filtered by business_date (one-run-per-day design assumption, C-3).
# DESIGN-NOTE: C-2 — LineageAgent is read/export only. It reads decisions and Gold written
# by other agents and never influences data values, quorum votes, or remediation outcomes.
# DESIGN-NOTE: C-3 — HTML output is fully self-contained: all CSS in a <style> tag with no
# external fonts, CDN links, or remote URLs. Testable by asserting "http://" not in file.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Any

from mdq.core.agent import Agent
from mdq.core.events import Event, TopicType
from mdq.core.schemas import OverallStatus, ScorecardRecord
from mdq.utils.logging import get_logger

if TYPE_CHECKING:
    from mdq.core.blackboard import Blackboard
    from mdq.core.config import Config
    from mdq.core.store import MedallionStore

log = get_logger("agents.lineage")


class LineageAgent(Agent):
    """Lineage/Catalog: traces Gold↔decisions, writes JSON+HTML scorecard (FR-A8, FR-O2, FR-O3)."""

    def __init__(self, bb: Blackboard, store: MedallionStore, cfg: Config) -> None:
        self._bb = bb
        self._store = store
        self._cfg = cfg

    @property
    def name(self) -> str:
        return "lineage"

    @property
    def subscriptions(self) -> list[TopicType]:
        return [TopicType.RUN_COMPLETE]

    async def act(self, event: Event) -> None:
        run_id = event.run_id
        bdate_str = event.payload.get("business_date")
        if not bdate_str:
            log.debug(
                "RUN_COMPLETE has no business_date — skipping lineage export (run=%s)", run_id
            )
            return

        business_date = date.fromisoformat(str(bdate_str))
        gold_df = self._store.read_gold(run_id, business_date)
        if gold_df.empty:
            log.info(
                "No Gold data for run=%s date=%s — skipping lineage export", run_id, business_date
            )
            return

        lineage_records = _build_lineage(gold_df, self._store)
        scorecard = _build_scorecard(run_id, business_date, gold_df, self._store)
        self._store.write_scorecard(scorecard)

        export_dir = Path(self._cfg.runtime.storage.lineage) / run_id
        export_dir.mkdir(parents=True, exist_ok=True)
        date_str = business_date.isoformat()

        if "json" in self._cfg.scorecard.output_formats:
            _write_json(lineage_records, scorecard, export_dir / f"{date_str}.json")
        if "html" in self._cfg.scorecard.output_formats:
            _write_html(lineage_records, scorecard, export_dir / f"{date_str}.html")

        log.info(
            "Lineage export: %d Gold records traced, status=%s (run=%s, date=%s)",
            len(lineage_records),
            scorecard.overall_status.value,
            run_id,
            business_date,
        )

        await self._bb.publish(
            Event(
                topic=TopicType.DECISION_RECORDED,
                agent=self.name,
                run_id=run_id,
                payload={
                    "business_date": date_str,
                    "lineage_records": len(lineage_records),
                    "overall_status": scorecard.overall_status.value,
                },
            )
        )


# ---------------------------------------------------------------------------
# Pure helpers — testable in isolation (C-4/C-5)
# ---------------------------------------------------------------------------


def _parse_json_or_list(value: Any) -> list[Any]:
    """Safely parse a JSON string or pass through an existing list."""
    if isinstance(value, str):
        return json.loads(value)  # type: ignore[no-any-return]
    return list(value) if value is not None else []


def _build_lineage(gold_df: Any, store: Any) -> list[dict[str, Any]]:
    """Join each Gold row with its DecisionRecord via decision_id FK.

    # DESIGN-NOTE: C-4 — pure function; no side effects; result is deterministic
    # for fixed inputs (same Gold Parquet + same decisions table).
    """
    records: list[dict[str, Any]] = []
    for _, row in gold_df.iterrows():
        decision_id = str(row.get("decision_id", ""))
        decision = store.get_decision(decision_id) if decision_id else None
        records.append(
            {
                "instrument_id": str(row["instrument_id"]),
                "field": str(row["field"]),
                "golden_value": str(row["golden_value"]),
                "confidence": str(row.get("confidence", "")),
                "quorum_sources": _parse_json_or_list(row.get("quorum_sources", "[]")),
                "dissenting_sources": _parse_json_or_list(row.get("dissenting_sources", "[]")),
                "decision_id": decision_id,
                "decision_type": decision["decision_type"] if decision else None,
                "inputs": json.loads(decision["inputs"]) if decision else {},
                "outcome": json.loads(decision["outcome"]) if decision else {},
                "rule_applied": decision["rule_applied"] if decision else None,
                "verified": bool(decision["verified"]) if decision else None,
            }
        )
    return records


def _build_scorecard(
    run_id: str,
    business_date: date,
    gold_df: Any,
    store: Any,
) -> ScorecardRecord:
    """Aggregate decisions for business_date into a ScorecardRecord.

    # DESIGN-NOTE: FR-A8 — filtered by business_date because the decisions table has
    # no run_id column; one-run-per-day partitioning makes this equivalent (C-3).
    # DESIGN-NOTE: C-4 — status rule: RED if ESCALATE > 0, AMBER if REMEDIATE > 0,
    # GREEN otherwise. Deterministic on identical decisions table state.
    """
    counts_df = store.query(
        f"SELECT decision_type, COUNT(*) AS n FROM decisions "
        f"WHERE business_date = '{business_date.isoformat()}' "
        f"GROUP BY decision_type"
    )
    counts: dict[str, int] = {}
    if not counts_df.empty:
        counts = dict(
            zip(
                counts_df["decision_type"].tolist(),
                [int(v) for v in counts_df["n"].tolist()],
                strict=True,
            )
        )

    escalations = counts.get("ESCALATE", 0)
    remediations = counts.get("REMEDIATE", 0)
    corp_actions = counts.get("CORP_ACTION", 0)

    if escalations > 0:
        status = OverallStatus.RED
    elif remediations > 0:
        status = OverallStatus.AMBER
    else:
        status = OverallStatus.GREEN

    return ScorecardRecord(
        run_id=run_id,
        business_date=business_date,
        source_id="batch",
        records_ingested=len(gold_df),
        breaks_detected=escalations + remediations,
        remediations_attempted=escalations + remediations,
        remediations_succeeded=remediations,
        escalations=escalations,
        corporate_actions_detected=corp_actions,
        overall_status=status,
    )


def _write_json(
    lineage_records: list[dict[str, Any]],
    scorecard: ScorecardRecord,
    dest: Path,
) -> None:
    """Write lineage trace + scorecard summary as JSON (FR-O2, FR-O3)."""
    payload = {
        "run_id": scorecard.run_id,
        "business_date": scorecard.business_date.isoformat(),
        "overall_status": scorecard.overall_status.value,
        "summary": scorecard.model_dump(mode="json"),
        "lineage": lineage_records,
    }
    dest.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    log.debug("wrote lineage JSON → %s", dest)


def _write_html(
    lineage_records: list[dict[str, Any]],
    scorecard: ScorecardRecord,
    dest: Path,
) -> None:
    """Write a self-contained offline HTML scorecard (no external assets — C-3, FR-O2)."""
    status = scorecard.overall_status.value
    status_css = status.lower()

    rows_html = "\n".join(
        f"    <tr>"
        f"<td>{r['instrument_id']}</td>"
        f"<td>{r['field']}</td>"
        f"<td>{r['golden_value']}</td>"
        f"<td>{r['confidence']}</td>"
        f"<td>{', '.join(r.get('quorum_sources', []))}</td>"
        f"<td><code>{r['decision_id'][:8]}…</code></td>"
        f"</tr>"
        for r in lineage_records
    )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>MDQ-Mesh Scorecard {scorecard.business_date}</title>
<style>
body {{
  font-family: monospace;
  margin: 2em;
  background: #f5f5f5;
  color: #222;
}}
h1, h2 {{ color: #1a1a2e; margin-bottom: 0.3em; }}
p {{ margin: 0.2em 0; }}
.status-green {{ color: #2d6a2d; font-weight: bold; }}
.status-amber {{ color: #a66a00; font-weight: bold; }}
.status-red   {{ color: #8b0000; font-weight: bold; }}
table {{
  border-collapse: collapse;
  width: 100%;
  margin: 1em 0;
  background: #fff;
}}
th, td {{
  border: 1px solid #ccc;
  padding: 0.4em 0.8em;
  text-align: left;
}}
th {{ background: #1a1a2e; color: #fff; }}
tr:nth-child(even) {{ background: #eef2ff; }}
code {{
  background: #eee;
  padding: 0 0.3em;
  border-radius: 3px;
  font-size: 0.9em;
}}
</style>
</head>
<body>
<h1>MDQ-Mesh Scorecard</h1>
<p><strong>Run:</strong> {scorecard.run_id}</p>
<p><strong>Business Date:</strong> {scorecard.business_date}</p>
<p><strong>Status:</strong> <span class="status-{status_css}">{status}</span></p>

<h2>Summary</h2>
<table>
  <tr><th>Metric</th><th>Value</th></tr>
  <tr><td>Gold Records</td><td>{scorecard.records_ingested}</td></tr>
  <tr><td>Corporate Actions Detected</td><td>{scorecard.corporate_actions_detected}</td></tr>
  <tr><td>Breaks / Remediations Attempted</td><td>{scorecard.remediations_attempted}</td></tr>
  <tr><td>Remediations Succeeded</td><td>{scorecard.remediations_succeeded}</td></tr>
  <tr><td>Escalations</td><td>{scorecard.escalations}</td></tr>
</table>

<h2>Gold Lineage Trace</h2>
<table>
  <tr>
    <th>Instrument</th>
    <th>Field</th>
    <th>Golden Value</th>
    <th>Confidence</th>
    <th>Quorum Sources</th>
    <th>Decision ID</th>
  </tr>
{rows_html}
</table>
</body>
</html>"""

    dest.write_text(html, encoding="utf-8")
    log.debug("wrote lineage HTML → %s", dest)
