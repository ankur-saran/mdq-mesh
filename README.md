# mdq-mesh — Self-Healing Market-Data Quality & Reconciliation Mesh

> A local, keyless, fully-auditable multi-agent system that ingests EOD market data
> from independent public sources, cross-reconciles them, detects anomalies and corporate
> actions, elects a golden value by quorum, and self-heals — with a complete lineage trail.

---

## Table of Contents

1. [What it does](#1-what-it-does)
2. [Design invariants](#2-design-invariants)
3. [Architecture](#3-architecture)
4. [Data sources](#4-data-sources)
5. [Medallion storage model](#5-medallion-storage-model)
6. [Agent roster & event flow](#6-agent-roster--event-flow)
7. [Prerequisites](#7-prerequisites)
8. [Installation](#8-installation)
9. [Quick start](#9-quick-start)
10. [CLI reference](#10-cli-reference)
11. [Configuration](#11-configuration)
12. [Universe definition](#12-universe-definition)
13. [Defect injection & test harness](#13-defect-injection--test-harness)
14. [Optional enhancements](#14-optional-enhancements)
15. [Running the test suite](#15-running-the-test-suite)
16. [Project structure](#16-project-structure)
17. [Data model](#17-data-model)
18. [KPIs & success metrics](#18-kpis--success-metrics)
19. [Glossary](#19-glossary)

---

## 1. What it does

In capital-markets data engineering the expensive, recurring problem is not building
pipelines — it is *trusting* them. End-of-day prices, reference data, and corporate
actions arrive from multiple independent feeds that disagree, drift in schema, go stale,
and occasionally publish wrong values. One missed stock split or mis-keyed close
propagates into risk, P&L, and regulatory reporting.

**mdq-mesh** replaces manual morning reconciliation checks with an autonomous,
closed-loop agent system:

| Step | What happens |
|---|---|
| **Ingest** | Four independent keyless sources are fetched concurrently (yfinance, Stooq, ECB SDMX, SEC EDGAR). Raw data lands immutably in Bronze. |
| **Contract** | Schema and type contracts are validated; drift (missing columns, type changes) is quarantined. |
| **DQ** | A rule suite checks for nulls, out-of-range values, staleness, monotonicity violations, and duplicates. |
| **Anomaly** | Statistical outlier detection (z-score + IQR) distinguishes "anomalous and likely wrong" from "real volatility regime." |
| **Corporate actions** | Price discontinuities consistent with split ratios are detected; historical series are back-adjusted. |
| **Reconcile** | A quorum vote across sources elects a golden value per instrument-field. Disagreements outside tolerance become *breaks*. |
| **Self-heal** | Breaks trigger targeted re-fetch, quarantine, and downstream hold. After bounded retries the system escalates to a human. |
| **Lineage** | Every decision — ingest, validate, vote, adjust, remediate — is persisted with its inputs, rule applied, and outcome. |
| **Scorecard** | A static offline HTML report and machine-readable JSON are emitted for the data-ops standup. |

Everything runs on a single laptop with no API keys, no paid services, and no cloud
infrastructure.

---

## 2. Design invariants

These constraints override every other consideration and must never be violated:

| ID | Invariant |
|---|---|
| **C-1** | **No API keys, tokens, or paid services.** Only genuinely keyless sources: yfinance, Stooq, ECB SDMX, SEC EDGAR (descriptive `User-Agent` header per SEC fair-access policy — that is not a key). |
| **C-2** | **No LLM in the data hot path.** Golden values, reconciliation verdicts, anomaly flags, and corporate-action adjustments are produced by deterministic code only. The optional Narrator LLM is edge-only and never influences data decisions. |
| **C-3** | **Single laptop.** No mandatory external infrastructure. Docker (Redpanda) and Ollama are optional enhancements behind clean interfaces; the pure-local `asyncio` path runs without them. |
| **C-4** | **Auditable.** Every data-affecting decision is persisted with inputs, rule applied, outcome, and verification status. |
| **C-5** | **Deterministic.** Given identical frozen inputs, the system produces byte-identical Gold output and decision log (modulo timestamps). `mdq replay` proves this. |
| **C-6** | **Optional LLM is a graceful no-op.** If Ollama is absent, the pipeline runs identically minus narrative text. |

---

## 3. Architecture

```
                ┌─────────────────────────────────────────────────────┐
                │                     BLACKBOARD                        │
                │      (append-only, ordered, typed event log / bus)    │
                └─────────────────────────────────────────────────────┘
         ▲              ▲              ▲              ▲              ▲
         │              │              │              │              │
 ┌───────┴──────┐ ┌─────┴──────┐ ┌────┴──────┐ ┌────┴──────┐ ┌────┴──────┐
 │ yfinance     │ │ Stooq      │ │ ECB SDMX  │ │ SEC EDGAR │ │ Supervisor│
 │ Agent        │ │ Agent      │ │ Agent     │ │ Agent     │ │ (lifecycle│
 └──────┬───────┘ └──────┬─────┘ └─────┬─────┘ └─────┬─────┘ │  & policy)│
        │ INGESTION_     │              │ REFERENCE_   │       └────┬──────┘
        │ COMPLETE       │              │ DATA_COMPLETE │            │
        ▼                ▼              │              │             │
 ┌──────────────┐  ┌──────────────┐    │ (bypasses    │       ┌────┴──────┐
 │ Contract     │  │ Contract     │    │ price        │       │ Remediat- │
 │ Agent        │  │ Agent        │    │ pipeline)    │       │ ion Agent │
 └──────┬───────┘  └──────┬───────┘    └──────────────┘       └────┬──────┘
        │                 │                                          │
        ▼                 ▼                                          │
 ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │
 │ DQ Agent     │  │ DQ Agent     │  │ Anomaly      │◄─────────────┘
 └──────┬───────┘  └──────┬───────┘  │ Agent        │
        │                 │          └──────┬───────┘
        └────────┬─────────┘                │
                 ▼                          │
         ┌───────────────┐                  │
         │ Corporate     │◄─────────────────┘
         │ Actions Agent │
         └───────┬───────┘
                 ▼
         ┌───────────────┐      ┌───────────────┐      ┌───────────────┐
         │ Reconciliation│─────►│ Lineage /     │─────►│ Narrator      │
         │ Agent (quorum)│      │ Catalog Agent │      │ (Ollama opt.) │
         └───────────────┘      └───────────────┘      └───────────────┘
                 │ Gold Parquet + HTML/JSON Scorecard
                 ▼
           data/gold/  data/lineage/
```

**Transport:** by default, agents communicate through an in-process `asyncio` queue
(zero infrastructure). An optional Redpanda/Kafka backbone can be swapped in via a
pluggable `Transport` interface without changing any agent code.

---

## 4. Data sources

All sources are genuinely keyless (C-1 invariant):

| Source | Data type | Notes |
|---|---|---|
| **yfinance** (Yahoo Finance) | EOD OHLCAV prices, splits, dividends | Primary equity price source |
| **Stooq** | EOD prices, indices, FX (CSV endpoints) | Independent second price source — critical for quorum |
| **ECB SDMX REST** | EUR-based FX reference rates | `lastNObservations=1` endpoint, no auth |
| **SEC EDGAR** | Shares outstanding (`CommonStockSharesOutstanding`) | Requires a descriptive `User-Agent` header per SEC fair-access policy — **not** a key |

**Deliberate exclusion — FRED:** FRED requires a free API *key*, violating C-1. Stooq
and ECB cover the FX/macro gap instead.

Adding a new source requires only a new agent file in `src/mdq/agents/ingestion/` and
a config entry — zero changes to core orchestration (KPI-7).

---

## 5. Medallion storage model

Data moves through three immutable layers, stored as Parquet files with DuckDB as the
embedded query engine (no server process):

```
Bronze  ──►  Silver  ──►  Gold
  │              │             │
  │  raw,        │  validated,  │  golden value
  │  immutable,  │  conformed,  │  per instrument-
  │  per-source  │  reconcil-   │  field-date +
  │  per-run     │  iation-     │  decision_id
  │              │  ready       │
  └──────────────┴─────────────►  Quarantine
                                   (rejected records
                                    + reason)
```

| Layer | Location | Contents |
|---|---|---|
| **Bronze** | `data/bronze/<source>/<run_id>/<date>.parquet` | Raw source payload, one file per source per run. Never mutated after write. |
| **Silver** | `data/silver/<run_id>/<date>/<source>.parquet` | Validated, type-conformed, canonical long format (one row per instrument-field). |
| **Gold** | `data/gold/<run_id>/<date>.parquet` | One golden value per instrument-field; includes quorum metadata and `decision_id`. |
| **Quarantine** | `data/quarantine/` | Records rejected by Contract / DQ / Reconciliation with reason attached. |
| **Lineage** | `data/lineage/<run_id>/<date>.json` and `.html` | Decision audit trail + self-contained HTML scorecard. |
| **DuckDB** | `data/mdq.duckdb` | Embedded engine for cross-layer queries. |

---

## 6. Agent roster & event flow

### Agents

| Agent | Event in | Event(s) out | Responsibility |
|---|---|---|---|
| **YFinanceAgent** | `RUN_STARTED` | `INGESTION_COMPLETE` | Fetch EOD OHLCAV from Yahoo Finance; write Bronze |
| **StooqAgent** | `RUN_STARTED` | `INGESTION_COMPLETE` | Fetch EOD prices from Stooq CSV endpoints; write Bronze |
| **ECBAgent** | `RUN_STARTED` | `REFERENCE_DATA_COMPLETE` | Fetch EUR-based FX rates from ECB SDMX; write Bronze+Silver |
| **SECEdgarAgent** | `RUN_STARTED` | `REFERENCE_DATA_COMPLETE` | Fetch shares outstanding from SEC EDGAR; write Bronze+Silver |
| **ContractAgent** | `INGESTION_COMPLETE` | `CONTRACT_PASSED` / `CONTRACT_VIOLATION` | Pandera schema validation; drift detection; quarantine on violation |
| **DQAgent** | `CONTRACT_PASSED` | `DQ_PASSED` / `DQ_FAILURE` | Null, range, staleness, monotonicity, duplicate checks |
| **AnomalyAgent** | `DQ_PASSED` | `ANOMALY_DETECTED` | Rolling z-score + IQR fence; volatility-regime-aware so real market moves are not quarantined |
| **CorporateActionsAgent** | `DQ_PASSED` | `CORPORATE_ACTION_DETECTED` | Split ratio detection on price discontinuities; back-adjustment of affected rows |
| **ReconciliationAgent** | `DQ_PASSED` | `RECONCILIATION_COMPLETE` / `BREAK_DETECTED` | Quorum vote across sources; elect golden value; flag unresolved breaks |
| **RemediationAgent** | `BREAK_DETECTED` / `INGESTION_FAILED` | `REMEDIATION_COMPLETE` / `REMEDIATION_FAILED` / `ESCALATION` | Quarantine → re-fetch → verify → retry loop; escalate after bounded retries |
| **Supervisor** | `RUN_COMPLETE` (+ all events) | Run summary log | Lifecycle coordination; escalation policy; KPI-1 accounting |
| **LineageAgent** | `RUN_COMPLETE` | `DECISION_RECORDED` | Write decision audit trail; render JSON + HTML scorecard |
| **NarratorAgent** *(optional)* | `RUN_COMPLETE` | — | Summarise incidents in plain English via local Ollama; graceful no-op if absent |

### Event flow (price pipeline)

```
RUN_STARTED
    ├──► YFinanceAgent ──► INGESTION_COMPLETE(source=yfinance)
    │       └──► ContractAgent ──► CONTRACT_PASSED
    │               └──► DQAgent ──► DQ_PASSED
    │                       ├──► AnomalyAgent
    │                       ├──► CorporateActionsAgent ──► (CORPORATE_ACTION_DETECTED?)
    │                       └──► ReconciliationAgent ──► RECONCILIATION_COMPLETE / BREAK_DETECTED
    │                                   └──► RemediationAgent (on break)
    │
    ├──► StooqAgent ──► INGESTION_COMPLETE(source=stooq)
    │       └── (same chain as above) ──►
    │
    ├──► ECBAgent ──► REFERENCE_DATA_COMPLETE   ← bypasses price pipeline
    └──► SECEdgarAgent ──► REFERENCE_DATA_COMPLETE  ← bypasses price pipeline

RUN_COMPLETE
    └──► LineageAgent ──► scorecard HTML + JSON
    └──► NarratorAgent (if enabled) ──► incident narrative
```

---

## 7. Prerequisites

| Requirement | Version | Notes |
|---|---|---|
| **Python** | ≥ 3.11 | `asyncio` maturity, typing |
| **uv** or **pip** | any | `uv` is recommended for fast env management |
| **Docker** | any | Optional — only needed for Redpanda transport (C-3) |
| **Ollama** | any | Optional — only needed for Narrator LLM (C-6) |

No API keys. No cloud accounts. No paid subscriptions.

---

## 8. Installation

### With uv (recommended)

```powershell
git clone <repo-url>
cd mdq-mesh
uv sync
```

### With pip

```powershell
git clone <repo-url>
cd mdq-mesh
python -m venv .venv
.\.venv\Scripts\Activate.ps1   # Windows PowerShell
pip install -e ".[dev]"
```

### Optional extras

```powershell
pip install -e ".[ml]"       # scikit-learn for richer anomaly detection
pip install -e ".[narrator]" # Ollama client for the optional Narrator agent
pip install -e ".[redpanda]" # aiokafka for the optional Redpanda transport
```

---

## 9. Quick start

```powershell
# 1. Install dependencies
uv sync

# 2. Inject clean synthetic fixtures (no live network call needed)
python -m mdq inject clean

# 3. Run the full pipeline
python -m mdq run

# 4. Open the scorecard in your browser
#    File path printed at the end of the run, e.g.:
#    data\lineage\<run_id>\2026-06-10.html

# 5. Prove replay determinism
python -m mdq replay <run_id>
# → PASS — replay of '<run_id>' is bit-identical (N Gold records verified).
```

To fetch **live data** instead of fixtures:

```yaml
# config/default.yaml
runtime:
  use_fixtures: false   # fetch live from yfinance, Stooq, ECB, SEC EDGAR
```

```powershell
python -m mdq run
```

---

## 10. CLI reference

### `python -m mdq run`

Run the full pipeline end-to-end.

```
python -m mdq run [--config PATH] [--date YYYY-MM-DD]
```

| Option | Default | Description |
|---|---|---|
| `--config` / `-c` | `config/default.yaml` | Path to YAML config file |
| `--date` / `-d` | yesterday | Business date to process |

**Example:**

```powershell
python -m mdq run --date 2026-06-10
```

---

### `python -m mdq run-agent <name>`

Run a single ingestion agent in isolation. Useful for testing live connectivity to
one source or debugging fixture issues.

```
python -m mdq run-agent (yfinance|stooq|ecb|sec_edgar) [--config PATH] [--date YYYY-MM-DD]
```

**Example:**

```powershell
python -m mdq run-agent yfinance
python -m mdq run-agent ecb --date 2026-06-10
```

---

### `python -m mdq inject <scenario>`

Inject a named defect into harness fixtures, then run `python -m mdq run` to observe
the pipeline catching it.

```
python -m mdq inject <scenario> [--config PATH] [--date YYYY-MM-DD]
python -m mdq inject --freeze-fixtures [--config PATH] [--date YYYY-MM-DD]
```

| Scenario | What it seeds |
|---|---|
| `clean` | Restore fixtures to a clean (no-defect) baseline |
| `null_burst` | 30% of `Close` values set to `NaN` |
| `stale_feed` | `fetch_ts` / `event_ts` rolled back 3 days |
| `out_of_range` | One `Close` value multiplied by 100× |
| `schema_drift` | `Adj Close` column dropped |
| `split_2to1` | One `Close` value multiplied by 2.0 (unadjusted split) |
| `cross_source_break` | All `Close` values shifted +10% (systematic disagreement) |
| `volatility_regime` | All `Close` values multiplied by 3× (within-regime spike) |
| `mixed_defects` | `null_burst` (rate=0.2) + `out_of_range` applied sequentially |

`--freeze-fixtures` fetches live data and snapshots it as the clean baseline,
enabling deterministic offline runs (FR-T2).

**Examples:**

```powershell
# Standard defect-injection workflow
python -m mdq inject null_burst
python -m mdq run
# → ContractAgent: Silver schema violations (yfinance): 1 failure cases

python -m mdq inject split_2to1
python -m mdq run
# → CorporateActionsAgent: split detected

python -m mdq inject clean
python -m mdq run
# → Clean run; Gold produced with HIGH confidence

# Snapshot today's live data as clean fixtures
python -m mdq inject --freeze-fixtures --date 2026-06-10
```

---

### `python -m mdq replay <run_id>`

Replay a frozen run deterministically and verify byte-identical Gold output (C-4, C-5, KPI-6).

```
python -m mdq replay <run_id> [--config PATH]
```

Replay re-runs `CorporateActionsAgent` + `ReconciliationAgent` from the stored Silver
data, then compares the resulting Gold against the original on deterministic columns
(`instrument_id`, `field`, `golden_value`, `confidence`, `quorum_sources`,
`dissenting_sources`). Timestamps and `decision_id` UUIDs are excluded from comparison.

**Example:**

```powershell
python -m mdq replay a1b2c3d4-...
# PASS — replay of 'a1b2c3d4-...' is bit-identical (30 Gold records verified).
```

Exit code 0 on pass, 1 on divergence.

---

## 11. Configuration

All tunable parameters live in `config/default.yaml`. Nothing is hard-coded in agent
code (C-4).

```yaml
runtime:
  use_fixtures: true          # true = load harness/fixtures/ (offline); false = live fetch
  transport: asyncio          # asyncio | redpanda (redpanda requires Docker)
  redpanda:
    bootstrap_servers: "localhost:9092"
    topic_prefix: "mdq"
    consumer_group: "mdq-mesh"
  parallelism: 4
  storage:
    bronze: data/bronze
    silver: data/silver
    gold: data/gold
    quarantine: data/quarantine
    lineage: data/lineage
  duckdb_path: data/mdq.duckdb

sources:
  yfinance:
    enabled: true
    retries: 3
    backoff_seconds: 2
    staleness_max_days: 1
  stooq:
    enabled: true
    retries: 3
    backoff_seconds: 2
    staleness_max_days: 1
  ecb:
    enabled: true
    retries: 3
    backoff_seconds: 2
  sec_edgar:
    enabled: true
    retries: 2
    backoff_seconds: 3
    user_agent: "mdq-mesh research contact@example.com"  # per SEC fair-access; NOT a key

reconciliation:
  quorum:
    min_agreeing_sources: 2
  tolerances:                 # per-field agreement bands
    CLOSE:     { type: relative_bps, value: 25 }
    OPEN:      { type: relative_bps, value: 25 }
    HIGH:      { type: relative_bps, value: 25 }
    LOW:       { type: relative_bps, value: 25 }
    ADJ_CLOSE: { type: relative_bps, value: 50 }
    VOLUME:    { type: relative_pct, value: 5 }
  on_no_quorum: break         # break → quarantine → remediation

dq_rules:
  null_check:         { enabled: true, severity: high }
  range_check:        { enabled: true, severity: high, min: 0 }
  staleness_check:    { enabled: true, severity: high }
  monotonicity_check: { enabled: true, severity: medium }   # HIGH ≥ LOW etc.
  duplicate_check:    { enabled: true, severity: medium }

corporate_actions:
  split_ratio_tolerance: 0.05
  dividend_drop_min_pct: 0.5
  require_cross_source_corroboration: true
  candidate_split_ratios: [2.0, 3.0, 1.5, 4.0]
  ca_window_days: 30

anomaly:
  zscore_window: 20
  zscore_threshold: 4.0
  iqr_multiplier: 1.5
  volatility_aware: true
  volatility_regime_window: 5
  volatility_regime_ratio: 2.0

remediation:
  max_retries: 3
  lookback_widen_steps: [5, 20, 60]
  hold_downstream_on_break: true
  escalate_after_retries: true

narrator:
  enabled: false              # optional, edge-only (C-6)
  host: "http://localhost:11434"
  model: "llama3.1:8b"

scorecard:
  output_formats: [json, html]
```

---

## 12. Universe definition

Edit `config/universe.yaml` to change which instruments are tracked. Each entry maps a
canonical `instrument_id` to per-source symbol strings:

```yaml
instruments:
  - instrument_id: AAPL
    symbols: { yfinance: AAPL, stooq: aapl.us, sec_edgar: "0000320193" }
  - instrument_id: MSFT
    symbols: { yfinance: MSFT, stooq: msft.us, sec_edgar: "0000789019" }
  - instrument_id: NVDA
    symbols: { yfinance: NVDA, stooq: nvda.us, sec_edgar: "0001045810" }
  - instrument_id: JPM
    symbols: { yfinance: JPM,  stooq: jpm.us,  sec_edgar: "0000019617" }
  - instrument_id: SPY
    symbols: { yfinance: SPY,  stooq: spy.us }
  # ECB FX pairs: ecb symbol is the ISO 4217 target-currency code
  - instrument_id: EUR/USD
    symbols: { ecb: USD }
  - instrument_id: EUR/GBP
    symbols: { ecb: GBP }
```

An instrument only appears in a source agent's fetch if it has a mapping for that
source. Instruments with no `sec_edgar` mapping (e.g. ETFs) are silently skipped by
the SEC EDGAR agent.

---

## 13. Defect injection & test harness

The harness is in `harness/` and is used both by the CLI `inject` command and directly
in unit tests.

### Programmatic injection

```python
from harness.inject import inject, DefectType
import pandas as pd

df = pd.read_parquet("harness/fixtures/yfinance/clean.parquet")
broken = inject(df, DefectType.NULL_BURST, seed=42, column="Close", rate=0.3)
broken = inject(df, DefectType.UNADJUSTED_SPLIT, seed=42, column="Close", ratio=2.0)
```

All injectors accept a `seed` argument for reproducible output (C-5).

### Frozen fixtures

Fixtures are stored in `harness/fixtures/<source_id>/<tag>.parquet`:

- `clean.parquet` — live-snapshotted or synthetic baseline
- `default.parquet` — the fixture the pipeline loads (may be a defect-injected variant)

```python
from harness.fixtures import snapshot, load_fixture

# Save a DataFrame as a fixture
snapshot("yfinance", df, tag="clean")

# Load the active fixture for a source
df = load_fixture("yfinance")           # loads default.parquet
df = load_fixture("yfinance", "clean")  # loads clean.parquet
```

When `use_fixtures: true` in config, all ingestion agents load from
`harness/fixtures/<source_id>/default.parquet` instead of hitting the network.

---

## 14. Optional enhancements

### Redpanda transport (Phase 8)

Swap the in-process `asyncio` queue for a real Redpanda/Kafka backbone without
changing any agent code:

```bash
# Start Redpanda via Docker
docker run -d --name redpanda \
  -p 9092:9092 -p 9644:9644 \
  redpandadata/redpanda:latest \
  redpanda start --overprovisioned --smp 1 --memory 512M \
  --reserve-memory 0M --node-id 0 --check=false
```

```yaml
# config/default.yaml
runtime:
  transport: redpanda
  redpanda:
    bootstrap_servers: "localhost:9092"
    topic_prefix: "mdq"
    consumer_group: "mdq-mesh"
```

Pure-local path is unchanged when `transport: asyncio`.

### Ollama Narrator (Phase 8)

The optional `NarratorAgent` sends the run's incidents to a local Ollama model for
plain-English incident summaries appended to the scorecard. It **never** reads or
influences data decisions (C-2, C-6).

```bash
# Install Ollama: https://ollama.com
ollama pull llama3.1:8b
```

```yaml
# config/default.yaml
narrator:
  enabled: true
  host: "http://localhost:11434"
  model: "llama3.1:8b"
```

When `enabled: false` (the default), `NarratorAgent` is never registered and the run
is identical in every data-affecting way.

---

## 15. Running the test suite

```powershell
# Run all tests
pytest -q

# Lint, format check, type check
ruff check .
black --check .
mypy src
```

The suite is fully offline — all tests use frozen fixtures (FR-T2) and never hit the
network. 235 tests cover:

- Unit tests for every agent in isolation with a mocked blackboard
- Integration tests for each of the 8 build phases
- Replay determinism (KPI-6)
- Defect injection scenarios and detection recall (KPI-1, KPI-4)
- Schema validation (Pandera contracts)

---

## 16. Project structure

```
mdq-mesh/
├── pyproject.toml              # dependencies, scripts, tool config
├── CLAUDE.md                   # operating instructions for Claude Code
├── RUNBOOK.md                  # phase-by-phase verification checklist
├── docs/
│   └── PRD.md                  # full product requirements document
├── config/
│   ├── default.yaml            # all tunable parameters (§11)
│   └── universe.yaml           # instrument list + per-source symbol mappings
├── src/mdq/
│   ├── cli.py                  # CLI: run / run-agent / replay / inject
│   ├── core/
│   │   ├── agent.py            # Agent interface / base class
│   │   ├── blackboard.py       # asyncio event log + pluggable transport
│   │   ├── events.py           # typed TopicType + Event model
│   │   ├── config.py           # typed YAML config loader (Pydantic v2)
│   │   ├── schemas.py          # canonical Pydantic + Pandera schemas
│   │   ├── store.py            # DuckDB + Parquet medallion writers/readers
│   │   └── transport/
│   │       ├── base.py         # Transport interface
│   │       ├── inprocess.py    # default asyncio transport (no infra)
│   │       └── redpanda.py     # optional Redpanda/Kafka transport
│   ├── agents/
│   │   ├── ingestion/
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
│   │   └── narrator_agent.py   # optional, edge-only
│   ├── reporting/              # scorecard JSON + HTML rendering
│   └── utils/
│       ├── hashing.py
│       └── logging.py
├── harness/
│   ├── fixtures/               # frozen Parquet snapshots (FR-T2)
│   │   ├── yfinance/
│   │   │   ├── clean.parquet
│   │   │   └── default.parquet
│   │   └── stooq/
│   │       ├── clean.parquet
│   │       └── default.parquet
│   ├── inject.py               # DefectType enum + injector functions (FR-T1)
│   └── scenarios/              # known-truth KPI scenarios
├── data/
│   ├── bronze/                 # raw per-source per-run Parquet
│   ├── silver/                 # validated canonical long-format Parquet
│   ├── gold/                   # golden values + quorum metadata
│   ├── quarantine/             # rejected records with reason
│   └── lineage/                # decision audit trail + HTML/JSON scorecard
└── tests/
    ├── unit/                   # per-agent isolation tests (mocked blackboard)
    └── integration/            # end-to-end + phase acceptance criteria
```

---

## 17. Data model

### Silver (canonical price record)

```
instrument_id   string      canonical symbol (e.g. "AAPL", "EUR/USD")
business_date   date
field           enum        OPEN | HIGH | LOW | CLOSE | ADJ_CLOSE | VOLUME | SHARES
value           float
currency        string      ISO 4217 (or "shares" for SEC EDGAR counts)
source_id       string      yfinance | stooq | ecb | sec_edgar
source_symbol   string      source's native identifier
fetch_ts        timestamp   when the data was fetched
event_ts        timestamp   the data's own as-of timestamp
content_hash    string      SHA-256 of raw payload (for dedupe + lineage)
ca_adjusted     bool        corporate-action adjusted?
```

### Gold (golden record)

```
instrument_id      string
business_date      date
field              enum
golden_value       float
currency           string
quorum_sources     list[str]   sources that agreed within tolerance
dissenting_sources list[str]   sources outside tolerance band
tolerance_band     string      rule applied (e.g. "relative_bps:25")
confidence         enum        HIGH | MEDIUM | LOW
decision_id        string      FK into decision/lineage log
```

### Decision / lineage record

```
decision_id     string
ts              timestamp
agent           string
instrument_id   string
business_date   date
decision_type   enum    INGEST | CONTRACT | DQ | CORP_ACTION | RECONCILE | REMEDIATE | ESCALATE
inputs          json    source values considered
outcome         json    what was decided
rule_applied    string
verified        bool
```

### DQ Scorecard record

```
run_id, business_date, source_id,
records_ingested, dq_failures_by_rule, breaks_detected,
remediations_attempted, remediations_succeeded, escalations,
corporate_actions_detected, overall_status   # GREEN | AMBER | RED
```

---

## 18. KPIs & success metrics

| ID | Metric | Target |
|---|---|---|
| **KPI-1** | Auto-remediation rate (defects resolved without human escalation) | ≥ 80% of injected defects |
| **KPI-2** | Golden-value accuracy vs. known-truth | ≥ 99% within tolerance |
| **KPI-3** | False-escalation rate (healthy data wrongly escalated) | ≤ 5% |
| **KPI-4** | Corporate-action detection recall on seeded splits | ≥ 95% |
| **KPI-5** | End-to-end run time for 50-instrument universe, 3 sources, on a laptop | ≤ 60 seconds |
| **KPI-6** | Replay determinism: two runs on identical frozen inputs | 100% identical Gold + decision log |
| **KPI-7** | New-source onboarding: agent + config only | Zero edits to orchestrator/blackboard core |

---

## 19. Glossary

| Term | Definition |
|---|---|
| **Blackboard** | Shared append-only typed event log all agents read/write; the coordination substrate. Every event is persisted. |
| **Bronze / Silver / Gold** | Raw → validated/conformed → curated golden data layers (medallion architecture). |
| **Break** | An unresolved cross-source disagreement beyond tolerance; routed to Remediation. |
| **Content hash** | SHA-256 of the raw source payload; used for deduplication and lineage tracing. |
| **Decision ID** | UUID assigned to each data-affecting decision; the FK linking Gold records to their lineage. |
| **Frozen fixtures** | Parquet snapshots of live-fetched data enabling fully offline, deterministic runs (FR-T2). |
| **Golden value** | The quorum-elected authoritative value for an instrument-field on a given business date. |
| **Hot path** | The code path that produces golden values; must be deterministic (C-2). |
| **Quorum vote** | Majority-within-tolerance rule: sources whose values agree within the configured tolerance band elect the golden value. |
| **Quarantine** | Isolated storage zone for records rejected by Contract, DQ, or Reconciliation agents; never appear in Gold. |
| **Remediation** | The autonomous detect → quarantine → re-fetch → verify → retry loop; escalates to human after bounded retries. |
| **Self-healing** | The property that the system detects, remediates, and verifies data defects without human intervention. |
| **Transport** | The event delivery mechanism between agents; default is in-process `asyncio`; optional Redpanda/Kafka for distributed use. |
| **REFERENCE_DATA_COMPLETE** | Event published by ECB and SEC EDGAR agents (instead of `INGESTION_COMPLETE`) so they bypass the price reconciliation pipeline. |

---

*Built with Python ≥ 3.11 · DuckDB · Parquet · Pydantic v2 · Pandera · asyncio · Typer*  
*No API keys. No cloud. No mandatory infrastructure.*
