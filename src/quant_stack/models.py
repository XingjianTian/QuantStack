from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class StrategyStatus(StrEnum):
    RESEARCH = "research"
    BACKTESTED = "backtested"
    PAPER = "paper"
    LIVE = "live"
    RETIRED = "retired"


class MarketScope(StrictModel):
    regions: list[str] = Field(min_length=1)
    asset_classes: list[str] = Field(min_length=1)
    venues: list[str] = Field(default_factory=list)
    instruments: list[str] = Field(default_factory=list)
    currency: str


class DatasetRequirement(StrictModel):
    dataset_id: str
    kind: str
    frequency: str
    fields: list[str] = Field(min_length=1)
    point_in_time: bool = True


class RuleBlock(StrictModel):
    description: str
    expression: str | None = None
    execution_timing: str


class PositionRule(StrictModel):
    method: str
    max_position_pct: float = Field(gt=0, le=1)
    max_gross_exposure: float = Field(gt=0)
    rebalance: str


class RiskLimits(StrictModel):
    max_drawdown_pct: float = Field(gt=0, le=1)
    max_daily_loss_pct: float = Field(gt=0, le=1)
    max_positions: int = Field(gt=0)
    stop_loss_pct: float | None = Field(default=None, gt=0, le=1)
    kill_switch_required: bool = True


class BacktestConfig(StrictModel):
    engine: str
    start: str
    end: str
    initial_capital: float = Field(gt=0)
    base_currency: str
    commission_bps: float = Field(ge=0)
    slippage_bps: float = Field(ge=0)
    benchmark: str | None = None


class StrategySpec(StrictModel):
    schema_version: str
    strategy_id: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]*$")
    name: str
    version: str
    owner: str
    status: StrategyStatus
    thesis: str
    market_scope: MarketScope
    data_requirements: list[DatasetRequirement] = Field(min_length=1)
    signal_sources: list[str] = Field(min_length=1)
    entry_rules: list[RuleBlock] = Field(min_length=1)
    exit_rules: list[RuleBlock] = Field(min_length=1)
    position_rules: PositionRule
    risk_limits: RiskLimits
    backtest: BacktestConfig
    implementation: dict[str, str]
    tags: list[str] = Field(default_factory=list)


class DatasetManifest(StrictModel):
    schema_version: str
    dataset_id: str
    version: str
    kind: str
    market: str
    provider: str
    frequency: str
    storage_uri: str
    format: str
    schema_ref: str
    fetched_at: datetime
    available_at_policy: str
    revision_policy: str
    sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    row_count: int | None = Field(default=None, ge=0)
    quality_status: str
    license_scope: str
    source_url: str


class ExperimentManifest(StrictModel):
    schema_version: str = "1.0.0"
    experiment_id: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    strategy_id: str
    strategy_version: str
    strategy_spec_sha256: str
    code_commit: str
    upstream_versions: dict[str, str]
    data_versions: list[dict[str, str]]
    parameter_file: str
    result_file: str
    report_file: str
    status: str = "created"


def load_yaml(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as stream:
        payload = yaml.safe_load(stream)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected a YAML mapping: {path}")
    return payload


def load_strategy_spec(path: str | Path) -> StrategySpec:
    return StrategySpec.model_validate(load_yaml(path))


def load_dataset_manifest(path: str | Path) -> DatasetManifest:
    return DatasetManifest.model_validate(load_yaml(path))
