"""Unit tests for NarratorAgent (FR-A10, Phase 8).

All tests run fully offline — Ollama HTTP calls are intercepted via monkeypatch.
NarratorAgent is tested in isolation with a mocked blackboard (NFR-3).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest

from mdq.core.events import Event, TopicType

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_RUN_ID = "narrator-test-run"
_BDATE = "2024-01-02"


def _make_events(extra_topics: list[str] | None = None) -> list[dict[str, Any]]:
    base = [
        {"topic": "run.started"},
        {"topic": "ingestion.complete"},
        {"topic": "reconciliation.complete"},
        {"topic": "run.complete"},
    ]
    for t in extra_topics or []:
        base.append({"topic": t})
    return base


def _make_bb(events: list[dict[str, Any]] | None = None) -> MagicMock:
    bb = MagicMock()
    bb.get_events.return_value = events if events is not None else _make_events()
    return bb


def _make_cfg(tmp_path: Path, enabled: bool = True) -> MagicMock:
    cfg = MagicMock()
    cfg.narrator.enabled = enabled
    cfg.narrator.host = "http://localhost:11434"
    cfg.narrator.model = "llama3.1:8b"
    cfg.runtime.storage.lineage = str(tmp_path / "lineage")
    return cfg


def _make_agent(tmp_path: Path, events: list[dict[str, Any]] | None = None) -> Any:
    from mdq.agents.narrator_agent import NarratorAgent

    return NarratorAgent(_make_bb(events), MagicMock(), _make_cfg(tmp_path))


def _run_complete_event() -> Event:
    return Event(
        topic=TopicType.RUN_COMPLETE,
        agent="supervisor",
        run_id=_RUN_ID,
        payload={"business_date": _BDATE},
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_name_and_subscriptions() -> None:
    """Agent name == "narrator" and subscribes to RUN_COMPLETE only."""
    from mdq.agents.narrator_agent import NarratorAgent

    agent = NarratorAgent(MagicMock(), MagicMock(), MagicMock())
    assert agent.name == "narrator"
    assert agent.subscriptions == [TopicType.RUN_COMPLETE]


async def test_narrator_writes_file_when_ollama_available(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Narrative text file is written when Ollama returns a response."""
    narrative = "All equity sources agreed. No anomalies detected. Pipeline healthy."

    async def mock_call_ollama(host: str, model: str, prompt: str) -> str | None:
        return narrative

    monkeypatch.setattr("mdq.agents.narrator_agent._call_ollama", mock_call_ollama)

    agent = _make_agent(tmp_path)
    await agent.act(_run_complete_event())

    out_path = tmp_path / "lineage" / _RUN_ID / "narrator.txt"
    assert out_path.exists(), f"Expected narrator file at {out_path}"
    assert narrative in out_path.read_text(encoding="utf-8")


async def test_narrator_noop_when_ollama_returns_none(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No file is written when _call_ollama returns None (Ollama unavailable)."""

    async def mock_call_ollama(host: str, model: str, prompt: str) -> str | None:
        return None

    monkeypatch.setattr("mdq.agents.narrator_agent._call_ollama", mock_call_ollama)

    agent = _make_agent(tmp_path)
    await agent.act(_run_complete_event())

    out_path = tmp_path / "lineage" / _RUN_ID / "narrator.txt"
    assert not out_path.exists()


async def test_call_ollama_returns_none_on_connect_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_call_ollama returns None when Ollama is unreachable (ConnectError)."""
    from mdq.agents.narrator_agent import _call_ollama

    class _MockClient:
        def __init__(self, **kwargs: Any) -> None:
            pass

        async def __aenter__(self) -> _MockClient:
            return self

        async def __aexit__(self, *args: Any) -> None:
            pass

        async def post(self, *args: Any, **kwargs: Any) -> None:
            raise httpx.ConnectError("connection refused")

    monkeypatch.setattr(httpx, "AsyncClient", _MockClient)
    result = await _call_ollama("http://localhost:11434", "llama3.1:8b", "test")
    assert result is None


async def test_call_ollama_returns_none_on_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_call_ollama returns None on timeout (no pipeline stall — C-2)."""
    from mdq.agents.narrator_agent import _call_ollama

    class _MockClient:
        def __init__(self, **kwargs: Any) -> None:
            pass

        async def __aenter__(self) -> _MockClient:
            return self

        async def __aexit__(self, *args: Any) -> None:
            pass

        async def post(self, *args: Any, **kwargs: Any) -> None:
            raise httpx.TimeoutException("timeout")

    monkeypatch.setattr(httpx, "AsyncClient", _MockClient)
    result = await _call_ollama("http://localhost:11434", "llama3.1:8b", "test")
    assert result is None


def test_build_context_includes_event_counts() -> None:
    """_build_context formats event counts correctly into the prompt."""
    from mdq.agents.narrator_agent import _build_context

    events = [
        {"topic": "anomaly.detected"},
        {"topic": "anomaly.detected"},
        {"topic": "reconciliation.break"},
        {"topic": "escalation"},
        {"topic": "reconciliation.complete"},
        {"topic": "run.complete"},
    ]
    context = _build_context(_RUN_ID, _BDATE, events)

    assert _RUN_ID in context
    assert _BDATE in context
    assert "6" in context  # total_events
    assert "ANOMALY_DETECTED: 2" in context
    assert "BREAK_DETECTED: 1" in context
    assert "ESCALATION: 1" in context


def test_build_context_empty_events_does_not_raise() -> None:
    """_build_context handles an empty event list without raising."""
    from mdq.agents.narrator_agent import _build_context

    context = _build_context(_RUN_ID, _BDATE, [])
    assert isinstance(context, str)
    assert len(context) > 0
    assert "ANOMALY_DETECTED: 0" in context


async def test_narrator_never_publishes_events(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """NarratorAgent never calls bb.publish (C-2 — never influences pipeline)."""

    async def mock_call_ollama(host: str, model: str, prompt: str) -> str | None:
        return "Summary text."

    monkeypatch.setattr("mdq.agents.narrator_agent._call_ollama", mock_call_ollama)

    bb = _make_bb()
    from mdq.agents.narrator_agent import NarratorAgent

    agent = NarratorAgent(bb, MagicMock(), _make_cfg(tmp_path))
    await agent.act(_run_complete_event())

    bb.publish.assert_not_called()


async def test_narrator_does_not_read_gold(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """NarratorAgent never reads Gold Parquet (C-2)."""

    async def mock_call_ollama(host: str, model: str, prompt: str) -> str | None:
        return "Summary."

    monkeypatch.setattr("mdq.agents.narrator_agent._call_ollama", mock_call_ollama)

    store = MagicMock()
    from mdq.agents.narrator_agent import NarratorAgent

    agent = NarratorAgent(_make_bb(), store, _make_cfg(tmp_path))
    await agent.act(_run_complete_event())

    store.read_gold.assert_not_called()
