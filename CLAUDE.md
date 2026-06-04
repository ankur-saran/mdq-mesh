# CLAUDE.md — Project Operating Instructions for `mdq-mesh`

> Read this at the start of every session. The full specification is in `docs/PRD.md`.
> This file is the *how*; the PRD is the *what*. When they conflict, ask.

## What this project is
A local, keyless multi-agent system that ingests the same market instruments from
several independent public sources, cross-reconciles them, detects anomalies and
corporate actions, elects a "golden" value by quorum, and self-heals — writing a
full audit trail. EOD batch cadence, event-driven core. See `docs/PRD.md`.

## NON-NEGOTIABLE INVARIANTS (these override every other instruction)
- **C-1 — NO KEYS.** No API keys, tokens, paid services, or cloud auth anywhere.
  Only genuinely keyless sources: yfinance, Stooq, SEC EDGAR (descriptive User-Agent
  header only — that is NOT a key), ECB SDMX. **Never introduce FRED** (it needs a key).
  If any code path tries to read a secret/key, that is a bug — fail fast and loud (NFR-9).
- **C-2 — NO LLM IN THE DATA HOT PATH.** Golden values, reconciliation verdicts,
  anomaly flags, and corporate-action adjustments MUST be produced by deterministic
  code. Any LLM use (the Narrator agent) is OPTIONAL, LOCAL (Ollama), edge-only, and
  must be a graceful no-op when absent. It must NEVER influence a data decision.
- **C-4 / C-5 — Auditable & deterministic.** Every data-affecting decision is persisted
  with inputs, rule applied, outcome, and verification. Identical frozen inputs MUST
  produce identical Gold + decision log (modulo timestamps). `mdq replay` must prove this.
- **C-3 — Single laptop.** No mandatory external infra. Docker (Redpanda) and Ollama are
  optional enhancements behind interfaces; the pure-local path must run without them.

## BUILD PROTOCOL (follow strictly)
1. Build **phase-by-phase** in the order in PRD §12 (Phase 0 → Phase 8). Do NOT skip ahead.
2. For each phase: enter **plan mode** first, propose an implementation for THAT PHASE ONLY,
   stop at the phase's acceptance criteria, and wait for my review before writing code.
3. A phase is "done" only when its acceptance criteria in §12 demonstrably pass.
4. **Build the test/fixtures harness (FR-T1, FR-T2) as part of Phase 0**, before the agents
   that are supposed to catch defects. Without deterministic fixtures we cannot verify anything.
5. Reference requirement IDs (FR-*, NFR-*, KPI-*, C-*) in commit messages and in
   `# DESIGN-NOTE:` comments where a non-obvious choice is made.
6. When a requirement is ambiguous, choose the MORE deterministic / MORE auditable option
   and leave a `# DESIGN-NOTE:` explaining why.

## CONVENTIONS
- Python ≥ 3.11. Type hints everywhere. `asyncio` for the in-process blackboard/bus.
- Storage: DuckDB + Parquet for the Bronze/Silver/Gold medallion. No server process.
- Schemas: Pydantic v2 (records) + Pandera (DataFrame contracts) are the source of truth;
  all source agents normalise into the canonical schemas in PRD §10.
- Config-driven, never hard-coded: tolerances, quorum rules, DQ thresholds, retry limits,
  source policies all live in `config/` (PRD §11). No magic numbers in agent code.
- Each agent implements the common `Agent` interface and is unit-testable in isolation
  with a mocked blackboard.
- New source = new agent in `src/mdq/agents/ingestion/` + config entry ONLY. Never edit
  the orchestrator or blackboard core to add a source (KPI-7).

## COMMANDS (use these exact ones)
- Install / sync deps:      `uv sync`   (or `pip install -e ".[dev]"`)
- Run the pipeline:         `python -m mdq run --config config/default.yaml`
- Run a single agent:       `python -m mdq run-agent <name>`
- Replay a frozen run:      `python -m mdq replay <run_id>`
- Inject test defects:      `python -m mdq inject <scenario>`
- Tests:                    `pytest -q`
- Lint / format / types:    `ruff check . && black --check . && mypy src`

## DEFINITION OF DONE (per phase, before moving on)
- Acceptance criteria for the phase pass.
- `ruff`, `black`, `mypy`, and `pytest` are green.
- New decisions are persisted to the lineage store and traceable by `decision_id`.
- The pure-local, keyless path still runs end-to-end.

## DO NOT
- Do not add FRED or any keyed/paid source.
- Do not let the Narrator (or any LLM) read or influence data decisions.
- Do not hard-code tolerances, symbols, or thresholds — they belong in `config/`.
- Do not let a "held"/quarantined instrument appear in Gold.
- Do not introduce streaming, trading, or a web server (explicit non-goals, PRD §4.2).
