"""Typed configuration loader (PRD §11). Validates default.yaml at startup.

# DESIGN-NOTE: NFR-9 no-key enforcement fires before any config value is returned,
# so the rest of the codebase never sees a secret-carrying environment.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Config models — mirror the YAML structure in config/default.yaml exactly
# ---------------------------------------------------------------------------


class StoragePaths(BaseModel):
    bronze: str = "data/bronze"
    silver: str = "data/silver"
    gold: str = "data/gold"
    quarantine: str = "data/quarantine"
    lineage: str = "data/lineage"


class RedpandaConfig(BaseModel):
    bootstrap_servers: str = "localhost:9092"
    topic_prefix: str = "mdq"
    consumer_group: str = "mdq-mesh"


class RuntimeConfig(BaseModel):
    transport: str = "asyncio"
    # DESIGN-NOTE: FR-B3 — redpanda sub-config used only when transport == "redpanda".
    # Kept inside RuntimeConfig so the YAML path is runtime.redpanda.* (PRD §11).
    redpanda: RedpandaConfig = RedpandaConfig()
    parallelism: int = 4
    storage: StoragePaths = StoragePaths()
    duckdb_path: str = "data/mdq.duckdb"
    # FR-T2: when True, source agents load from harness/fixtures/ instead of live fetch
    use_fixtures: bool = False


class SourceConfig(BaseModel):
    enabled: bool = True
    retries: int = 3
    backoff_seconds: int = 2
    staleness_max_days: int = 1
    user_agent: str | None = None


class SourcesConfig(BaseModel):
    yfinance: SourceConfig = SourceConfig()
    stooq: SourceConfig = SourceConfig()
    ecb: SourceConfig = SourceConfig(enabled=False)
    sec_edgar: SourceConfig = SourceConfig(enabled=False)


class QuorumConfig(BaseModel):
    min_agreeing_sources: int = 2


class ToleranceConfig(BaseModel):
    type: str  # relative_bps | relative_pct | absolute
    value: float


class ReconciliationConfig(BaseModel):
    quorum: QuorumConfig = QuorumConfig()
    tolerances: dict[str, ToleranceConfig] = {}
    on_no_quorum: str = "break"


class DQRuleConfig(BaseModel):
    enabled: bool = True
    severity: str = "high"
    min: float | None = None


class DQRulesConfig(BaseModel):
    null_check: DQRuleConfig = DQRuleConfig()
    range_check: DQRuleConfig = DQRuleConfig(min=0.0)
    staleness_check: DQRuleConfig = DQRuleConfig()
    monotonicity_check: DQRuleConfig = DQRuleConfig(severity="medium")
    duplicate_check: DQRuleConfig = DQRuleConfig(severity="medium")


class CorporateActionsConfig(BaseModel):
    split_ratio_tolerance: float = 0.05
    dividend_drop_min_pct: float = 0.5
    require_cross_source_corroboration: bool = True
    # DESIGN-NOTE: FR-A4 — candidate ratios are config-driven, never hard-coded (C-4).
    candidate_split_ratios: list[float] = [2.0, 3.0, 1.5, 4.0]
    ca_window_days: int = 30


class AnomalyConfig(BaseModel):
    zscore_window: int = 20
    zscore_threshold: float = 4.0
    iqr_multiplier: float = 1.5
    volatility_aware: bool = True
    # DESIGN-NOTE: FR-A5 — two-window ratio detects regime changes without a hard
    # price threshold, making the check instrument-agnostic (C-4 config-driven).
    volatility_regime_window: int = 5
    volatility_regime_ratio: float = 2.0


class RemediationConfig(BaseModel):
    max_retries: int = 3
    lookback_widen_steps: list[int] = [5, 20, 60]
    hold_downstream_on_break: bool = True
    escalate_after_retries: bool = True


class NarratorConfig(BaseModel):
    enabled: bool = False
    host: str = "http://localhost:11434"
    model: str = "llama3.1:8b"


class ScorecardConfig(BaseModel):
    output_formats: list[str] = ["json", "html"]


class InstrumentConfig(BaseModel):
    instrument_id: str
    symbols: dict[str, str]


class UniverseConfig(BaseModel):
    instruments: list[InstrumentConfig] = []


class Config(BaseModel):
    runtime: RuntimeConfig = RuntimeConfig()
    sources: SourcesConfig = SourcesConfig()
    reconciliation: ReconciliationConfig = ReconciliationConfig()
    dq_rules: DQRulesConfig = DQRulesConfig()
    corporate_actions: CorporateActionsConfig = CorporateActionsConfig()
    anomaly: AnomalyConfig = AnomalyConfig()
    remediation: RemediationConfig = RemediationConfig()
    narrator: NarratorConfig = NarratorConfig()
    scorecard: ScorecardConfig = ScorecardConfig()
    universe: UniverseConfig = UniverseConfig()


# ---------------------------------------------------------------------------
# Public loader
# ---------------------------------------------------------------------------

_KEY_PATTERN = re.compile(
    r"\b(API_KEY|SECRET_KEY|AUTH_TOKEN|ACCESS_TOKEN|PRIVATE_KEY)\b",
    re.IGNORECASE,
)


def _check_no_secrets() -> None:
    """NFR-9: fail fast and loudly if any env var looks like an API key/secret.

    # DESIGN-NOTE: pattern matches known secret variable name conventions.
    # Intentionally narrow to avoid false-positives on legitimate env vars.
    """
    suspicious = [k for k in os.environ if _KEY_PATTERN.search(k)]
    if suspicious:
        raise RuntimeError(
            f"NFR-9 violation — environment contains potential API key variables: "
            f"{suspicious}. mdq-mesh is keyless (C-1). Remove them from your shell."
        )


def load_config(
    config_path: Path | str = "config/default.yaml",
    universe_path: Path | str = "config/universe.yaml",
) -> Config:
    """Load and validate YAML configuration. Enforces the no-key invariant first."""
    _check_no_secrets()

    data: dict[str, Any] = {}
    with open(config_path) as f:
        loaded = yaml.safe_load(f)
        if isinstance(loaded, dict):
            data.update(loaded)

    u_path = Path(universe_path)
    if u_path.exists():
        with open(u_path) as f:
            universe_raw = yaml.safe_load(f)
        if isinstance(universe_raw, dict):
            data["universe"] = universe_raw

    return Config.model_validate(data)
