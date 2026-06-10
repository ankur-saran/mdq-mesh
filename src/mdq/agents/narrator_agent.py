"""Optional Narrator Agent — human-facing incident summaries via local Ollama (FR-A10).

Subscribes to RUN_COMPLETE. Reads the run's event log, builds a context prompt,
calls the local Ollama HTTP API, and writes narrator.txt to the run's lineage dir.
Must be a pure no-op when Ollama is absent (C-2, C-6).

# DESIGN-NOTE: C-2 — NarratorAgent NEVER publishes any blackboard event and NEVER
# reads or influences Gold, Silver, or any decision record. It is purely edge output.
# DESIGN-NOTE: C-6 — uses httpx (already a required dep) for the Ollama HTTP API.
# Returns None on any exception so the pipeline never depends on Ollama availability.
# DESIGN-NOTE: FR-A10 — registered in cli.py only when cfg.narrator.enabled is True.
# Disabled by default in config/default.yaml (C-6 — off by default, no LLM in hot path).
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx

from mdq.core.agent import Agent
from mdq.core.events import Event, TopicType
from mdq.utils.logging import get_logger

if TYPE_CHECKING:
    from mdq.core.blackboard import Blackboard
    from mdq.core.config import Config
    from mdq.core.store import MedallionStore

log = get_logger("agents.narrator")

_PROMPT_TEMPLATE = """\
You are a market data quality analyst reviewing a pipeline run.
Summarise the following run in 2-3 sentences of plain English.
Focus on anomalies, data breaks, escalations, and overall pipeline health.
Do not invent information beyond the event counts provided.

Run ID: {run_id}
Business Date: {business_date}
Total events: {total_events}
ANOMALY_DETECTED: {anomaly_count}
BREAK_DETECTED: {break_count}
ESCALATION: {escalation_count}
CORPORATE_ACTION_DETECTED: {corp_action_count}
REMEDIATION_COMPLETE: {remediation_count}
RECONCILIATION_COMPLETE: {recon_count}
"""


class NarratorAgent(Agent):
    """Optional run summariser using local Ollama. Pure no-op when absent (FR-A10)."""

    def __init__(self, bb: Blackboard, store: MedallionStore, cfg: Config) -> None:
        self._bb = bb
        self._store = store
        self._cfg = cfg

    @property
    def name(self) -> str:
        return "narrator"

    @property
    def subscriptions(self) -> list[TopicType]:
        return [TopicType.RUN_COMPLETE]

    async def act(self, event: Event) -> None:
        run_id = event.run_id
        bdate_str = str(event.payload.get("business_date", "unknown"))

        all_events = self._bb.get_events(run_id=run_id)
        context = _build_context(run_id, bdate_str, all_events)

        narrative = await _call_ollama(self._cfg.narrator.host, self._cfg.narrator.model, context)
        if narrative is None:
            return  # graceful no-op — Ollama unavailable (C-6)

        out_dir = Path(self._cfg.runtime.storage.lineage) / run_id
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "narrator.txt"
        out_path.write_text(narrative, encoding="utf-8")
        log.info("Narrator: wrote summary to %s", out_path)


# ---------------------------------------------------------------------------
# Pure helpers (C-4 — deterministic; no side effects)
# ---------------------------------------------------------------------------


def _build_context(run_id: str, business_date: str, events: list[dict[str, Any]]) -> str:
    """Format run event counts into an Ollama prompt string."""
    topic_counts: dict[str, int] = {}
    for ev in events:
        topic = str(ev.get("topic", ""))
        topic_counts[topic] = topic_counts.get(topic, 0) + 1

    return _PROMPT_TEMPLATE.format(
        run_id=run_id,
        business_date=business_date,
        total_events=len(events),
        anomaly_count=topic_counts.get("anomaly.detected", 0),
        break_count=topic_counts.get("reconciliation.break", 0),
        escalation_count=topic_counts.get("escalation", 0),
        corp_action_count=topic_counts.get("corporate_action.detected", 0),
        remediation_count=topic_counts.get("remediation.complete", 0),
        recon_count=topic_counts.get("reconciliation.complete", 0),
    )


async def _call_ollama(host: str, model: str, prompt: str) -> str | None:
    """Call local Ollama /api/generate. Returns None on any failure (never raises).

    # DESIGN-NOTE: C-2 / C-6 — returns None on ANY exception so the pipeline
    # never depends on Ollama availability. 30-second timeout prevents stall.
    """
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                f"{host}/api/generate",
                json={"model": model, "prompt": prompt, "stream": False},
            )
            resp.raise_for_status()
            text: str = str(resp.json().get("response", ""))
            return text if text else None
    except Exception as exc:
        log.info("Narrator: Ollama unavailable (%s) — skipping narration", type(exc).__name__)
        return None
