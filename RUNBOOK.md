# RUNBOOK — Building `mdq-mesh` in VS Code with Claude Code

Detailed, ordered, executable steps. Follow top to bottom.

---

## STEP 0 — Prerequisites (one-time, on your machine)

```bash
# 1. Node.js (Claude Code CLI runs on Node) and Python 3.11+
node --version        # need a recent LTS
python3 --version     # need >= 3.11

# 2. Install the uv package manager (fast, reproducible Python envs). Optional but recommended.
#    macOS/Linux:
curl -LsSf https://astral.sh/uv/install.sh | sh
#    (or use pip/venv if you prefer — commands below show both)

# 3. Install the Claude Code CLI
npm install -g @anthropic-ai/claude-code
claude --version
```

In **VS Code**: open the Extensions view (`Cmd/Ctrl+Shift+X`), search **"Claude Code"**, install it.
Open the Claude Code panel from the sidebar and **sign in with your Anthropic account** when prompted.

---

## STEP 1 — Create the repo and drop in the bootstrap kit

```bash
mkdir mdq-mesh && cd mdq-mesh
git init

# Create the directory skeleton the PRD expects
mkdir -p src/mdq config docs harness/fixtures harness/scenarios \
         data/{bronze,silver,gold,quarantine,lineage} tests/{unit,integration} \
         .vscode .claude
```

Copy these bootstrap files into the repo (provided alongside this runbook):

| File | Goes to | Purpose |
|---|---|---|
| `CLAUDE.md` | repo root | Session memory: invariants, build protocol, conventions |
| `.claudeignore` | repo root | Keeps context lean; blocks reading data/secrets |
| `pyproject.toml` | repo root | Dependency + tooling + CLI target |
| `.claude/settings.json` | `.claude/` | Permissions allow-list + plan mode |
| `.vscode/settings.json` | `.vscode/` | Auto-save, plan-mode default, Python tooling |
| `config/default.yaml` | `config/` | Tunable config surface (the contract) |
| `config/universe.yaml` | `config/` | Instruments + per-source symbol maps |
| `PRD.md` | `docs/PRD.md` | The full spec (single source of truth) |

```bash
# Initial commit so every later phase is a clean, revertible diff
git add -A
git commit -m "chore: bootstrap mdq-mesh (PRD, CLAUDE.md, config, tooling)"
```

---

## STEP 2 — Create the Python environment

```bash
# Using uv (recommended):
uv venv --python 3.11
uv pip install -e ".[dev]"

# OR using stdlib venv + pip:
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

Select the `.venv` interpreter in VS Code (`Cmd/Ctrl+Shift+P` → "Python: Select Interpreter").

---

## STEP 3 — Prime the Claude Code session

Open a Claude Code conversation in VS Code, then:

```
/terminal-setup        # registers Shift+Enter = newline for multi-line prompts
```

Confirm the mode indicator at the bottom of the prompt box shows **Plan** (set by
`.vscode/settings.json`; if not, click it and choose Plan).

First message to Claude Code:

```
Read @CLAUDE.md and @docs/PRD.md in full. Summarise back to me, in <10 bullets:
(1) the two non-negotiable invariants, (2) the phase order, and (3) what Phase 0
must deliver. Do NOT write any code yet — I want to confirm you have the spec right.
```

Only proceed once its summary is correct. This catches misunderstanding before any code exists.

---

## STEP 4 — The per-phase build loop (repeat for Phase 0 → 8)

For **each** phase, run this five-part cycle:

### 4a. Plan (in Plan mode)
Paste (substitute the phase number/name):
```
We are building Phase 0 only (Scaffold & contracts), per docs/PRD.md §12.
In plan mode, propose the implementation for THIS PHASE ONLY. Include:
- files you will create and their responsibilities
- the Agent interface + asyncio blackboard design
- the DuckDB/Parquet medallion writers
- the test/fixtures harness (FR-T1, FR-T2) — REQUIRED in Phase 0
- how you will demonstrate the phase acceptance criteria
Stop at the acceptance criteria. Do not start coding until I approve the plan.
```

### 4b. Review the plan
VS Code opens the plan as a markdown doc. **Annotate it inline** — e.g.
"keep the transport behind an interface (FR-B3)", "no hard-coded tolerances",
"fixtures must be frozen + offline". Approve when it's right.

### 4c. Implement
Let it implement (switch to normal/accept-edits mode, or approve diffs as they appear).
Watch the inline diffs; redirect early if it drifts.

### 4d. Verify the acceptance gate — DO NOT SKIP
```bash
ruff check . && black --check . && mypy src && pytest -q
```
Then run the phase's concrete acceptance check. For Phase 0:
```bash
python -m mdq run --help          # CLI boots
python -m mdq run --config config/default.yaml   # empty pipeline boots, dirs init, test event persists
```
If a criterion fails, tell Claude Code exactly which one; fix before advancing.

### 4e. Commit + compact
```
Commit this phase with a message referencing the requirement IDs implemented
(e.g. "feat(phase0): Agent iface, asyncio blackboard, medallion store, harness [FR-A*, FR-B*, FR-S*, FR-T*]").
```
Then run `/compact` before starting the next phase so context stays focused and
earlier decisions aren't lost to auto-compaction.

---

## STEP 5 — Phase-specific verification gates (your checklist)

| Phase | Run this to prove it | Pass when |
|---|---|---|
| **0** Scaffold + harness | `python -m mdq run --help`; boot empty pipeline | CLI works; blackboard persists an event; Bronze/Silver/Gold init |
| **1** Ingestion→Silver | `python -m mdq run-agent yfinance` | Bronze lands; conforms to canonical Silver; schema drift caught on broken fixture |
| **2** DQ + Anomaly | `python -m mdq inject null_burst` then run | Defects flagged w/ correct severity; volatility spike NOT quarantined |
| **3** 2nd source + Reconcile | run with stooq enabled | Agreement → HIGH-confidence golden value; seeded disagreement → flagged break w/ dissent recorded |
| **4** Corporate actions | `python -m mdq inject split_2to1` | Split detected (KPI-4 ≥95%) and adjusted; adj-vs-unadj cross-source resolved |
| **5** Remediation + Supervisor | `python -m mdq inject mixed_defects` | KPI-1 ≥80% auto-remediated; escalation only after bounded retries; held instrument never in Gold |
| **6** Lineage + Scorecard | `python -m mdq run` then open the HTML | Every Gold value traces to a decision_id; scorecard renders offline; `mdq replay` bit-identical (KPI-6) |
| **7** ECB + SEC sources | enable in config; run | new sources added with ZERO edits to orchestrator/blackboard core (KPI-7) |
| **8** Optional Redpanda/Ollama | toggle in config | pure-local path unchanged; enabling alters transport/narrative only, not decisions |

---

## STEP 6 — Run the full product (after Phase 6+)

```bash
# Freeze a real snapshot once, then run fully offline & deterministically
python -m mdq inject --freeze-fixtures
python -m mdq run --config config/default.yaml
python -m mdq replay <run_id>        # must reproduce identical Gold + decision log
open data/gold/scorecard.html        # or your OS equivalent
```

---

## Working tips specific to this build
- **Use @-mentions, not pasted paths:** `@docs/PRD.md`, `@src/mdq/core/schemas.py`. Faster and rename-safe.
- **Parallelism:** once the `Agent` interface + blackboard exist (Phase 0), the source agents and DQ rules
  are independent — a good place to let Claude Code spawn sub-agents for parallel implementation.
- **Toggle Extended Thinking** for the two genuinely hard bits: reconciliation quorum (Phase 3) and
  corporate-action detection (Phase 4).
- **Determinism discipline:** if `mdq replay` ever diverges, suspect wall-clock use or unsorted inputs —
  inject the clock and sort/normalise before any decision. Make the replay test a CI gate.
- **State lives on disk, not in the chat:** because `CLAUDE.md` + `docs/PRD.md` + committed code hold the
  state, you can `/compact`, close the session, or start fresh between phases and lose nothing.
- **An MCP server for DuckDB** becomes useful around Phase 6 (lets Claude Code query your real tables for
  the scorecard/lineage work). Skip it until there's data to query.
