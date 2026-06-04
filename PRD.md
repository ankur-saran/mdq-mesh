# Product Requirements Document
## Self-Healing Market-Data Quality & Reconciliation Mesh
### A Local, Keyless Multi-Agent System for Capital-Markets Data Engineering

---

| Field | Value |
|---|---|
| **Document version** | 1.0 |
| **Status** | Ready for build |
| **Author** | Ankur (Principal AI Architect / Lead Context Engineer) |
| **Target builder** | Claude Code |
| **Project codename** | `mdq-mesh` |
| **Last updated** | 2026-06-04 |

---

## 0. How to use this document (instructions for Claude Code)

This PRD is the single source of truth for the build. When implementing:

1. **Build phase-by-phase** in the order defined in §12. Do not skip ahead. Each phase has explicit acceptance criteria; treat a phase as "done" only when its criteria pass.
2. **Honour the hard constraints in §3 absolutely.** They are non-negotiable design invariants, not preferences. The two that override everything else: (a) **no API keys / no paid services / no cloud auth of any kind**, and (b) **no LLM in the data hot path** — golden values must be produced by deterministic code.
3. **Requirements are IDs.** Functional requirements are tagged `FR-*`, non-functional `NFR-*`. Reference these IDs in commit messages and code comments so traceability is preserved.
4. **Prefer standard library and embedded tooling** over heavyweight infrastructure. Everything must run on a single developer laptop with `python` and (optionally) `docker`.
5. When a requirement is ambiguous, **default to the more auditable, more deterministic option** and leave a `# DESIGN-NOTE:` comment explaining the choice.

---

## 1. Executive summary

In capital-markets data engineering, the recurring, expensive problem is not *building* pipelines — it is *trusting* them. End-of-day prices, reference data, and corporate actions arrive from multiple independent sources that disagree, drift in schema, go stale, and occasionally publish wrong values. Bad market and reference data propagate downstream into risk, P&L, and regulatory reporting, where a single missed stock split or mis-keyed close becomes a P&L break, a failed trade, or a reportable regulatory error. Today this is policed by data-operations staff manually inspecting reconciliation breaks each morning.

The **Market-Data Quality & Reconciliation Mesh (`mdq-mesh`)** is a multi-agent system that autonomously ingests the same instruments from several independent, keyless public sources, cross-reconciles them, detects anomalies and corporate-action effects, elects a "golden" value by quorum, and **self-heals** — quarantining bad data, re-fetching, holding downstream publication, and escalating to a human only when its own remediation fails. Every decision is written to an immutable, queryable lineage trail.

The system runs entirely on a local machine. It requires **no API keys, no paid data, and no cloud services**. It is deliberately architected so that all data-affecting decisions are made by deterministic, statistical, rules-based agents; any optional LLM use is confined to human-facing narration at the edges and can be served locally.

---

## 2. Business context & problem statement

### 2.1 The problem
- **Multi-source disagreement is the norm, not the exception.** Independent feeds for the same instrument routinely differ on close price, currency, timestamp, or corporate-action adjustment.
- **Silent failures are the dangerous ones.** A stale feed that returns *yesterday's* price looks healthy to a naive pipeline. A split that one source adjusts and another doesn't creates a 2:1 or 4:1 discontinuity that contaminates returns and volatility downstream.
- **Reconciliation is manual and reactive.** Analysts eyeball break reports each morning; remediation is ad hoc and undocumented; the same breaks recur.
- **Regulatory exposure is real.** Transaction and position reporting regimes (e.g. MiFID II / MiFIR transaction reporting, EMIR trade reporting) require accurate, timely, auditable data. Data-quality failures become reporting failures.

### 2.2 The cost of the status quo
- Recurring human toil on low-value reconciliation work.
- Downstream rework when bad data reaches risk/P&L engines.
- Audit findings and regulatory risk from undocumented or unrepeatable data fixes.
- Slow onboarding of new data sources because logic is entangled in monolithic pipelines.

### 2.3 The opportunity
A closed-loop agentic system that converts this from a *reactive, manual, undocumented* process into a *proactive, autonomous, fully-audited* one — while remaining cheap to run and trivially extensible (adding a source = adding an agent).

---

## 3. Hard constraints (non-negotiable invariants)

| ID | Constraint | Rationale |
|---|---|---|
| **C-1** | No API keys, tokens, paid subscriptions, or cloud authentication of any kind. | Project must run fully local and free; sources are restricted to genuinely keyless public endpoints. |
| **C-2** | No LLM in the data hot path. Golden values, reconciliation verdicts, anomaly flags, and corporate-action adjustments must be produced by deterministic code. | Non-determinism gating regulated market data is a liability. Reproducibility is mandatory. |
| **C-3** | Runs on a single laptop. No mandatory external infrastructure. | Docker-based services (Redpanda, Ollama) are strictly optional enhancements with pure-local fallbacks. |
| **C-4** | Every data-affecting decision is auditable and reproducible. | Lineage and decision logs are first-class outputs, not afterthoughts. |
| **C-5** | Determinism: given the same inputs, the system produces byte-identical Gold outputs and identical decision logs (modulo timestamps). | Enables replay, testing, and audit defence. |
| **C-6** | All LLM usage (if any) is optional, local, and confined to human-facing narration. | If Ollama is absent, the system must run identically minus the narrative text. |

> **Claude Code: if any requirement below appears to conflict with C-1 through C-6, the constraint wins. Flag the conflict in a `# DESIGN-NOTE:` and choose the constraint-compliant path.**

---

## 4. Goals & non-goals

### 4.1 Goals
- **G-1** Produce a trustworthy Gold-layer "golden" dataset of prices and reference data from reconciled multi-source inputs.
- **G-2** Autonomously detect and remediate data-quality defects (staleness, nulls, out-of-range, schema drift, cross-source breaks, corporate-action discontinuities) with minimal human intervention.
- **G-3** Emit a daily Data-Quality Scorecard suitable for a data-ops standup.
- **G-4** Maintain a complete, queryable lineage and decision audit trail.
- **G-5** Make source onboarding additive: a new source is a new agent + config, with zero changes to core orchestration.

### 4.2 Non-goals (explicitly out of scope for v1)
- **NG-1** Intraday / tick-level streaming. v1 is end-of-day (EOD) batch cadence with an event-driven core.
- **NG-2** Trade execution, order management, or any actual trading.
- **NG-3** Portfolio risk/P&L computation. The mesh *feeds* such engines; it does not implement them.
- **NG-4** A production web UI. v1 deliverables are files + a CLI + a static HTML scorecard.
- **NG-5** Real regulatory submission. We model the data-quality preconditions for reporting, not the submission itself.
- **NG-6** Multi-user concurrency, RBAC, or distributed deployment.

---

## 5. Stakeholders & personas

| Persona | Role | What they need from the system |
|---|---|---|
| **Data Ops Analyst** | Runs the morning data check | A scorecard that says "trust today's data / here are the N breaks I must look at," not a wall of raw diffs. |
| **Data Engineer** | Owns pipelines & sources | Easy source onboarding; clear logs when an agent remediates or escalates. |
| **Risk / Quant consumer** | Downstream consumer of Gold | A golden dataset with a known, documented quality bar and lineage. |
| **Compliance / Audit** | Reviews data provenance | An immutable, queryable record of every decision and adjustment, reproducible on replay. |
| **Platform / Architect (you)** | Owns the design | A clean agent abstraction, deterministic core, and an optional-LLM edge. |

---

## 6. Success metrics & KPIs

| ID | Metric | Target (v1) |
|---|---|---|
| **KPI-1** | Auto-remediation rate (defects resolved without human escalation) | ≥ 80% of injected defects in the test harness |
| **KPI-2** | Golden-value accuracy vs. known-truth in test scenarios | ≥ 99% within tolerance |
| **KPI-3** | False-escalation rate (healthy data wrongly escalated) | ≤ 5% |
| **KPI-4** | Corporate-action detection recall on seeded splits/dividends | ≥ 95% |
| **KPI-5** | End-to-end run time for a 50-instrument universe, 3 sources, on a laptop | ≤ 60 seconds (pure-local path) |
| **KPI-6** | Replay determinism: two runs on identical frozen inputs | 100% identical Gold + decision log (modulo run timestamps) |
| **KPI-7** | New-source onboarding effort | New agent + config only; zero edits to orchestrator/blackboard core |

---

## 7. System overview & architecture

### 7.1 Architectural stance
- **Closed-loop, not open-loop.** The system implements *detect → decide → act → verify → (retry / escalate)*. This control-loop shape is why a multi-agent design is justified over a static DAG.
- **Blackboard + Supervisor + Contract-Net.** Agents communicate through a shared event log (the **blackboard**). A **Supervisor** owns escalation policy. Source-of-truth election uses a **contract-net / quorum vote** among source agents.
- **Medallion storage, locally.** Bronze (raw per-source) → Silver (validated, conformed, reconciled) → Gold (golden dataset + scorecards), all on embedded storage.
- **Deterministic core, optional narrative edge.** All decisions are code. Optional local LLM only narrates incidents for humans.

### 7.2 Logical diagram (textual)

```
                ┌─────────────────────────────────────────────────┐
                │                  BLACKBOARD                       │
                │      (shared, append-only event log / bus)        │
                └─────────────────────────────────────────────────┘
                      ▲          ▲          ▲          ▲
   ┌──────────────────┘          │          │          └──────────────────┐
   │                             │          │                              │
┌──┴───────────┐   ┌─────────────┴──┐  ┌────┴───────────┐   ┌──────────────┴─┐
│ Source Agents │   │ Contract Agent │  │ Data Quality   │   │ Corporate      │
│ (1 per feed)  │   │ (schema/drift) │  │ Agent          │   │ Actions Agent  │
└──┬───────────┘   └────────────────┘  └────────────────┘   └────────────────┘
   │ Bronze write
   ▼
┌─────────────┐   ┌────────────────┐  ┌────────────────┐   ┌────────────────┐
│ Anomaly     │   │ Reconciliation │  │ Remediation    │   │ Lineage/Catalog│
│ Agent       │   │ Agent (quorum) │  │ Agent          │   │ Agent          │
└─────────────┘   └────────────────┘  └────────────────┘   └────────────────┘
                              ▲
                              │ escalation / policy
                  ┌───────────┴────────────┐
                  │  Supervisor / Orchestr. │
                  └─────────────────────────┘
                              │ (optional, keyless, edge-only)
                  ┌───────────┴────────────┐
                  │ Narrator (local Ollama) │  ← OPTIONAL, never in hot path
                  └─────────────────────────┘
```

---

## 8. Functional requirements

### 8.1 Agents (the core roster)

> Each agent must implement a common `Agent` interface: it subscribes to event types on the blackboard, has an autonomous `should_act(event) -> bool` gate, performs `act(event)`, publishes results, and logs every decision. Agents must be independently testable in isolation.

| ID | Agent | Responsibility | Key behaviours |
|---|---|---|---|
| **FR-A1** | **Source Ingestion Agent** (one instance per feed) | Fetch raw data from one keyless source; land it in Bronze. | Owns its source's retry, backoff, and staleness policy. Tags every record with source id, fetch timestamp, and a content hash. |
| **FR-A2** | **Contract Agent** | Validate schema and data contracts before downstream processing. | Detect schema drift (new/missing/renamed columns, type changes). Reject or quarantine contract violations; emit a drift event. |
| **FR-A3** | **Data Quality Agent** | Apply DQ rule suite. | Null checks, range/sanity checks, staleness (timestamp freshness), monotonicity/continuity, duplicate detection. Emit per-rule DQ events with severity. |
| **FR-A4** | **Corporate Actions Agent** | Detect and adjust for splits/dividends. | Detect price discontinuities consistent with split ratios (e.g. ~2:1, ~3:1, ~3:2) and dividend drops; back-adjust historical series; flag suspected actions for reconciliation. |
| **FR-A5** | **Anomaly Agent** | Statistical outlier detection (NO ML keys; pure stats, optional local scikit-learn). | Rolling z-score, IQR fences, rolling-volatility bands. Distinguish "anomalous but real" (volatility regime) from "anomalous and likely wrong." |
| **FR-A6** | **Reconciliation Agent** | Cross-source source-of-truth election. | Run a quorum/contract-net vote across sources for each instrument-field within a tolerance band; elect golden value; record agreeing/dissenting sources; flag unresolved breaks. |
| **FR-A7** | **Remediation Agent** | Autonomous self-healing. | On defect: quarantine offending record, trigger targeted re-fetch (widen lookback if needed), hold downstream publish for affected instruments, verify the fix, retry up to policy limit, then escalate. |
| **FR-A8** | **Lineage / Catalog Agent** | Provenance & audit. | Record source → transformation → decision → Gold lineage for every instrument-field; maintain a queryable catalog of decisions. |
| **FR-A9** | **Supervisor / Orchestrator Agent** | Coordination & escalation. | Drive the run lifecycle, enforce escalation policy and retry limits, decide when human attention is required, produce the run summary. |
| **FR-A10** | **Narrator Agent** *(optional)* | Human-facing incident narration only. | If a local LLM (Ollama) is available, summarise incidents/root-cause hypotheses in plain English for the scorecard. Must be a pure no-op (graceful skip) if absent. **Never** influences data decisions. |

### 8.2 Blackboard & messaging
- **FR-B1** Provide an append-only, ordered event log that all agents read from and write to (the blackboard).
- **FR-B2** Support typed events / topics and per-agent subscriptions (pub/sub semantics).
- **FR-B3** Default transport is in-process `asyncio` (zero infra). Provide a pluggable transport interface so a local **Redpanda/Kafka** (via Docker) backbone can be swapped in without changing agent code.
- **FR-B4** Every event is persisted so a run can be replayed and audited.

### 8.3 Storage & medallion layers
- **FR-S1** **Bronze:** raw, immutable, per-source landing zone (one partition per source per run). Parquet files; never mutated after write.
- **FR-S2** **Silver:** validated, schema-conformed, type-normalised, currency/timezone-aligned, reconciliation-ready data.
- **FR-S3** **Gold:** the golden dataset (one row per instrument-field per business date) plus the daily DQ scorecard.
- **FR-S4** Use **DuckDB** as the embedded query/transform engine over Parquet. No server process.
- **FR-S5** Quarantine zone: a separate location for records rejected by Contract/DQ/Reconciliation, with the reason attached.

### 8.4 Reconciliation logic (the heart)
- **FR-R1** For each instrument-field-date, gather all available source values.
- **FR-R2** Apply per-field tolerance bands (e.g. price within X bps, identical for categorical reference fields).
- **FR-R3** Elect golden value by quorum: majority-within-tolerance wins; record agreeing and dissenting sources.
- **FR-R4** No quorum → mark as an unresolved **break**, quarantine, and route to Remediation.
- **FR-R5** Tolerances and quorum rules are config-driven (see §11), never hard-coded.

### 8.5 Self-healing logic
- **FR-H1** Remediation actions: quarantine, targeted re-fetch, lookback widening, downstream hold, escalate.
- **FR-H2** Every remediation must **verify** that the action resolved the defect before clearing it.
- **FR-H3** Bounded retries (config-driven limit); exhausting retries triggers escalation, never a silent pass.
- **FR-H4** A held instrument must not appear in Gold until cleared or explicitly overridden by escalation policy.

### 8.6 Outputs
- **FR-O1** Gold golden dataset (Parquet + queryable via DuckDB).
- **FR-O2** Daily DQ Scorecard: per-source health, break counts, remediation outcomes, escalations, detected corporate actions. Emit as both machine-readable (JSON) and a **self-contained static HTML** report (no server, no external assets).
- **FR-O3** Decision/lineage log queryable by instrument, date, source, and decision type.
- **FR-O4** A CLI to run the pipeline, run a single agent, replay a frozen run, and inject test defects.

### 8.7 Test & simulation harness
- **FR-T1** A defect-injection harness that seeds known faults (stale feed, null burst, out-of-range spike, schema drift, unadjusted split, cross-source break) into otherwise-clean data.
- **FR-T2** A frozen-fixtures mode: snapshot real fetched data once, then run fully offline and deterministically against the snapshot (also satisfies C-1 in CI).
- **FR-T3** Known-truth scenarios to measure KPI-1..KPI-4 automatically.

---

## 9. Data sources (all genuinely keyless)

> **Claude Code: verify keyless/no-auth status at build time; if a source has changed its access policy, fall back to another keyless source and note it. Do NOT introduce any source that requires a key.**

| Source | Data | Auth | Notes |
|---|---|---|---|
| **yfinance** (Yahoo Finance) | EOD prices, OHLCV, splits/dividends | None | Primary equity price source. |
| **Stooq** | EOD prices, indices, FX (CSV endpoints) | None | Independent second price source — critical for cross-source reconciliation. |
| **SEC EDGAR** | Filings, company facts / reference data | None (requires a descriptive `User-Agent` header per SEC fair-access policy — this is **not** a key) | Reference-data and corporate-context source. |
| **ECB Data Portal** (SDMX REST) | FX reference rates, macro series | None | Keyless macro/FX. |

> **Deliberate exclusion — FRED.** FRED requires a free API *key*, which violates **C-1**. Do not use it. Stooq + ECB cover the FX/macro gap.

**Source-agent contract:** each source agent maps the source's native payload into a common canonical schema (see §10) and never leaks source-specific quirks past the Silver layer.

---

## 10. Data model (canonical schemas)

> Implement as typed schemas (e.g. Pydantic models + Pandera DataFrame schemas). All sources normalise into these.

**Canonical price record (Silver):**
```
instrument_id      string      # canonical symbol (mapping table resolves source symbols)
business_date      date
field              enum        # OPEN | HIGH | LOW | CLOSE | ADJ_CLOSE | VOLUME
value              decimal
currency           string      # ISO 4217
source_id          string
source_symbol      string
fetch_ts           timestamp   # when fetched
event_ts           timestamp   # the data's own timestamp / as-of
content_hash       string      # hash of raw payload for dedupe & lineage
ca_adjusted        bool        # corporate-action adjusted?
```

**Golden record (Gold):**
```
instrument_id      string
business_date      date
field              enum
golden_value       decimal
currency           string
quorum_sources     array<string>   # sources that agreed
dissenting_sources array<string>
tolerance_band     string          # rule applied
confidence         enum            # HIGH | MEDIUM | LOW
decision_id        string          # FK into decision log
```

**Decision / lineage record:**
```
decision_id        string
ts                 timestamp
agent              string
instrument_id      string
business_date      date
decision_type      enum   # INGEST | CONTRACT | DQ | CORP_ACTION | RECONCILE | REMEDIATE | ESCALATE
inputs             json   # source values considered
outcome            json   # what was decided
rule_applied       string
verified           bool
```

**DQ scorecard record:**
```
run_id, business_date, source_id,
records_ingested, dq_failures_by_rule, breaks_detected,
remediations_attempted, remediations_succeeded, escalations,
corporate_actions_detected, overall_status   # GREEN | AMBER | RED
```

---

## 11. Configuration (everything tunable, nothing hard-coded)

Provide a single typed config (YAML loaded into a validated config object). Required config surfaces:

- **`universe`**: list of instruments and their per-source symbol mappings.
- **`sources`**: enabled sources, per-source retry/backoff/staleness thresholds.
- **`reconciliation`**: per-field tolerance bands and quorum rule (e.g. min agreeing sources).
- **`dq_rules`**: enabled rules + thresholds + severities.
- **`corporate_actions`**: split-ratio detection sensitivity, dividend-drop thresholds.
- **`remediation`**: max retries, lookback-widening steps, escalation policy.
- **`runtime`**: transport (`asyncio` | `redpanda`), storage paths, parallelism.
- **`narrator`**: `enabled: false` by default; Ollama host/model if enabled.

---

## 12. Phased delivery plan (build in this order)

> Each phase ends with a working, demoable increment and must pass its acceptance criteria before the next begins.

### Phase 0 — Scaffold & contracts
- Repo structure (§14), config loader, canonical schemas, the `Agent` interface, the in-process `asyncio` blackboard, DuckDB/Parquet medallion writers, logging, CLI skeleton.
- **Acceptance:** `mdq run --help` works; an empty pipeline boots, the blackboard accepts/persists a test event, and Bronze/Silver/Gold dirs initialise.

### Phase 1 — Single-source ingestion → Bronze → Silver
- Implement one Source Ingestion Agent (yfinance) + Contract Agent. Land Bronze, conform to canonical Silver.
- **Acceptance:** for a 5-instrument universe, raw lands in Bronze, conforms to Silver, schema-drift is detected on a deliberately broken fixture.

### Phase 2 — Data Quality + Anomaly agents
- DQ rule suite + statistical anomaly detection. Quarantine zone.
- **Acceptance:** seeded null/staleness/out-of-range defects are flagged with correct severity; a volatility-regime spike is correctly **not** quarantined.

### Phase 3 — Second source + Reconciliation (quorum)
- Add Stooq Source Agent. Implement the Reconciliation Agent quorum vote and golden-value election into Gold.
- **Acceptance:** cross-source agreement produces a HIGH-confidence golden value; a seeded disagreement beyond tolerance produces a flagged break with recorded dissent.

### Phase 4 — Corporate Actions agent
- Split/dividend detection and back-adjustment.
- **Acceptance:** seeded 2:1 and 3:1 splits are detected (KPI-4 recall ≥ 95%) and adjusted; an unadjusted-vs-adjusted cross-source discrepancy is resolved correctly.

### Phase 5 — Remediation (self-healing loop) + Supervisor
- Quarantine → re-fetch → verify → retry → escalate, driven by the Supervisor.
- **Acceptance:** KPI-1 ≥ 80% auto-remediation on the injected-defect suite; escalations occur only after bounded retries; held instruments never leak into Gold.

### Phase 6 — Lineage/Catalog + Scorecard outputs
- Decision/lineage store; JSON + self-contained HTML scorecard.
- **Acceptance:** every Gold value traces to a decision_id and its source inputs; scorecard renders offline; replay (KPI-6) is bit-identical.

### Phase 7 — Reference data + ECB/SEC sources
- Add ECB (FX) and SEC EDGAR (reference) source agents through the same interface.
- **Acceptance:** new sources added with **zero** changes to orchestrator/blackboard core (KPI-7).

### Phase 8 — Optional enhancements
- Pluggable Redpanda transport (Docker) behind the same interface; optional local Ollama Narrator for incident summaries.
- **Acceptance:** system runs identically on the pure-local path with these disabled; enabling them changes transport/narrative only, not decisions.

---

## 13. Non-functional requirements

| ID | Requirement |
|---|---|
| **NFR-1 (Determinism)** | Identical frozen inputs → identical Gold + decision log (modulo timestamps). Replay command must prove this. |
| **NFR-2 (Auditability)** | Every data-affecting decision is persisted with inputs, rule, outcome, and verification status. |
| **NFR-3 (Isolation/Testability)** | Each agent is unit-testable in isolation with mocked blackboard events. |
| **NFR-4 (Extensibility)** | New source = new agent + config only. Core untouched. |
| **NFR-5 (Performance)** | Meet KPI-5 (≤ 60s for 50 instruments × 3 sources, pure-local). |
| **NFR-6 (Resilience)** | No single source failure aborts the run; the source is marked unhealthy and reconciliation proceeds with remaining quorum. |
| **NFR-7 (Portability)** | Runs on macOS/Linux/Windows with Python only; Docker strictly optional. |
| **NFR-8 (Observability)** | Structured logs + a run summary; clear distinction between INFO decisions and WARN/ERROR escalations. |
| **NFR-9 (No-key enforcement)** | A startup check fails fast and loudly if any code path attempts to read an API key/secret. |

---

## 14. Proposed project structure

```
mdq-mesh/
├── pyproject.toml
├── README.md
├── config/
│   ├── default.yaml
│   └── universe.yaml
├── src/mdq/
│   ├── __init__.py
│   ├── cli.py                  # entrypoint: run / run-agent / replay / inject
│   ├── core/
│   │   ├── agent.py            # Agent interface / base class
│   │   ├── blackboard.py       # asyncio event log + pluggable transport iface
│   │   ├── transport/
│   │   │   ├── inprocess.py
│   │   │   └── redpanda.py     # optional
│   │   ├── events.py           # typed event/topic definitions
│   │   ├── config.py           # typed config loader + validation
│   │   ├── schemas.py          # canonical Pydantic/Pandera schemas
│   │   └── store.py            # DuckDB + Parquet medallion writers/readers
│   ├── agents/
│   │   ├── ingestion/
│   │   │   ├── base_source.py
│   │   │   ├── yfinance_agent.py
│   │   │   ├── stooq_agent.py
│   │   │   ├── ecb_agent.py
│   │   │   └── sec_edgar_agent.py
│   │   ├── contract_agent.py
│   │   ├── dq_agent.py
│   │   ├── anomaly_agent.py
│   │   ├── corporate_actions_agent.py
│   │   ├── reconciliation_agent.py
│   │   ├── remediation_agent.py
│   │   ├── lineage_agent.py
│   │   ├── supervisor.py
│   │   └── narrator_agent.py    # optional, edge-only
│   ├── reporting/
│   │   ├── scorecard.py         # JSON + self-contained HTML
│   │   └── templates/
│   └── utils/
│       ├── hashing.py
│       └── logging.py
├── harness/
│   ├── inject.py                # defect injection
│   ├── fixtures/                # frozen snapshots for offline determinism
│   └── scenarios/               # known-truth KPI scenarios
├── data/
│   ├── bronze/  ├── silver/  ├── gold/  ├── quarantine/  └── lineage/
└── tests/
    ├── unit/                    # per-agent isolation tests
    └── integration/            # end-to-end + replay determinism
```

---

## 15. Tech stack requirements

| Layer | Choice | Version / note | Why |
|---|---|---|---|
| Language | **Python** | ≥ 3.11 | `asyncio` maturity, typing. |
| Concurrency | **asyncio** | stdlib | Zero-infra actor/message model. |
| Embedded query engine | **DuckDB** | latest stable | Serverless OLAP over Parquet; the medallion engine. |
| Columnar storage | **Parquet** (via pyarrow) | latest | Immutable Bronze; efficient Silver/Gold. |
| Schema/validation | **Pydantic v2** + **Pandera** | latest | Typed records + DataFrame contracts (Contract Agent). |
| Data sources | **yfinance**, **pandas-datareader**/direct CSV for **Stooq**, `requests`/`httpx` for **SEC EDGAR** & **ECB SDMX** | latest | All keyless. |
| Stats/anomaly | **NumPy**, **pandas**; optional **scikit-learn** | latest | Pure statistical detection; no external service. |
| Config | **YAML** via PyYAML + Pydantic-settings | latest | Typed, validated config (§11). |
| CLI | **Typer** (or argparse) | latest | Ergonomic commands. |
| Reporting | Plain HTML/CSS template (Jinja2), self-contained | latest | Offline scorecard, no server. |
| Testing | **pytest**, **pytest-asyncio** | latest | Unit + integration + replay. |
| Lint/format | **ruff**, **black**, **mypy** | latest | Quality gates. |
| Packaging | **pyproject.toml** (uv or pip) | — | Reproducible env. |
| *Optional* event backbone | **Redpanda** via Docker | optional | Real bus behind the transport interface. |
| *Optional* local LLM | **Ollama** (e.g. a small local model) | optional, edge-only | Narrator only; never hot path. |

> **Pin versions in `pyproject.toml`** and commit a lockfile so the build is reproducible (supports NFR-1).

---

## 16. Risks & mitigations

| Risk | Mitigation |
|---|---|
| A keyless source changes/blocks access (e.g. rate limits, layout change). | Frozen-fixtures mode (FR-T2) for offline determinism + multiple independent sources so the run survives one failure (NFR-6). |
| False positives quarantining good data. | Tunable tolerances/severities (§11); volatility-aware anomaly logic (FR-A5); KPI-3 guardrail. |
| Corporate-action false detection. | Conservative ratio bands + cross-source corroboration before adjusting; KPI-4 measures recall. |
| Scope creep toward streaming/trading. | Hard non-goals (§4.2); v1 is EOD batch with an event-driven core. |
| Determinism eroded by wall-clock or source ordering. | Inject clock; sort/normalise inputs; replay test (KPI-6) as a CI gate. |
| LLM creep into decisions. | C-2/C-6 invariants + NFR-9 startup check; Narrator strictly edge-only and optional. |

---

## 17. Definition of done (project-level)

- All eight phases pass their acceptance criteria.
- KPI-1..KPI-7 are measured by the harness and meet targets.
- `mdq run` produces Gold + scorecard offline from frozen fixtures with zero keys.
- `mdq replay` proves byte-identical determinism.
- A new source can be added in a single PR touching only `agents/ingestion/` + config.
- README documents setup, the keyless guarantee, the architecture, and how to run/replay/inject.

---

## 18. Glossary

- **Blackboard** — shared append-only event log all agents read/write; the coordination substrate.
- **Contract-net / quorum** — agents "bid" their values; a golden value is elected by majority-within-tolerance.
- **Medallion (Bronze/Silver/Gold)** — raw → conformed/validated → curated golden data layers.
- **Break** — an unresolved cross-source disagreement beyond tolerance.
- **Self-healing** — autonomous detect→remediate→verify loop that escalates only on failure.
- **Hot path** — the code path that decides golden values; must be deterministic (C-2).
- **Frozen fixtures** — snapshotted source data enabling fully offline, deterministic runs.

---

*End of PRD v1.0.*
