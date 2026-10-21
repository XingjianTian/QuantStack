from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from typing import Literal

from pydantic import Field, computed_field, model_validator

from ..models import DataModel

BACKFILL_DATASET_ID = "cn_equity_daily_backfill_pilot"


class BackfillUniverseInstrument(DataModel):
    instrument_id: str = Field(pattern=r"^(XSHG|XSHE)\.[0-9]{6}$")
    symbol: str = Field(pattern=r"^[0-9]{6}$")
    venue: Literal["XSHG", "XSHE"]
    provider_symbol: str = Field(pattern=r"^(sh|sz)\.[0-9]{6}$")
    selection_bucket: str = Field(min_length=1)
    listing_date: date | None = None
    delisting_date: date | None = None
    boundary_evidence: Literal["verified", "partial", "unavailable"] = "unavailable"
    first_observed_membership_date: date | None = None
    last_observed_membership_date: date | None = None
    membership_evidence: Literal["provider_observed", "curated", "unavailable"] = "curated"

    @model_validator(mode="after")
    def validate_identity(self) -> BackfillUniverseInstrument:
        expected_prefix = "sh" if self.venue == "XSHG" else "sz"
        if self.instrument_id != f"{self.venue}.{self.symbol}":
            raise ValueError("instrument_id must bind venue and symbol")
        if self.provider_symbol != f"{expected_prefix}.{self.symbol}":
            raise ValueError("provider_symbol must bind BaoStock exchange alias and symbol")
        if self.delisting_date and self.listing_date and self.delisting_date < self.listing_date:
            raise ValueError("delisting_date cannot precede listing_date")
        if (
            self.first_observed_membership_date
            and self.last_observed_membership_date
            and self.last_observed_membership_date < self.first_observed_membership_date
        ):
            raise ValueError("last observed membership cannot precede first observed membership")
        return self


class BackfillUniverseSource(DataModel):
    snapshot_version: str = Field(min_length=1)
    observed_date: date
    provider: Literal["baostock"] = "baostock"
    endpoint: Literal["query_all_stock"] = "query_all_stock"
    provider_version: str = Field(min_length=1)
    native_schema_version: str = Field(min_length=1)
    raw_uri: str = Field(pattern=r"^quant-data://")
    raw_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    row_count: int = Field(ge=0)
    xshg_count: int = Field(ge=0)
    xshe_count: int = Field(ge=0)
    evidence_scope: Literal["provider_observed_membership_only"] = (
        "provider_observed_membership_only"
    )


class BackfillUniverse(DataModel):
    schema_version: Literal["1.0.0"] = "1.0.0"
    universe_id: str = Field(pattern=r"^[a-z0-9][a-z0-9._-]*$")
    version: str = Field(min_length=1)
    selection_seed: int = Field(ge=0)
    selection_policy: str = Field(min_length=1)
    source_policy: str = Field(min_length=1)
    snapshot_versions: tuple[str, ...] = ()
    source_refs: tuple[BackfillUniverseSource, ...] = ()
    declared_content_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    instruments: tuple[BackfillUniverseInstrument, ...] = Field(min_length=50, max_length=10_000)

    @model_validator(mode="after")
    def validate_universe(self) -> BackfillUniverse:
        ids = [item.instrument_id for item in self.instruments]
        if len(ids) != len(set(ids)):
            raise ValueError("universe instrument IDs must be unique")
        if {item.venue for item in self.instruments} != {"XSHG", "XSHE"}:
            raise ValueError("pilot universe must include XSHG and XSHE")
        ordered = sorted(
            self.instruments,
            key=lambda item: ({"XSHG": 0, "XSHE": 1}[item.venue], item.symbol),
        )
        if list(self.instruments) != ordered:
            raise ValueError("pilot universe order must be deterministic")
        if self.snapshot_versions and self.snapshot_versions != tuple(
            item.snapshot_version for item in self.source_refs
        ):
            raise ValueError("snapshot versions must exactly match ordered source refs")
        if self.declared_content_sha256 and self.declared_content_sha256 != self.content_sha256:
            raise ValueError("declared universe content hash does not match content")
        return self

    @computed_field
    @property
    def content_sha256(self) -> str:
        payload = self.model_dump(
            mode="json",
            exclude={"declared_content_sha256"},
            exclude_computed_fields=True,
        )
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest()


class BackfillShard(DataModel):
    shard_id: str = Field(pattern=r"^shard-[a-f0-9]{24}$")
    dataset_id: Literal["cn_equity_daily_backfill_pilot"] = BACKFILL_DATASET_ID
    dataset_version: str = Field(min_length=1)
    universe_version: str = Field(min_length=1)
    instrument_ids: tuple[str, ...] = Field(min_length=1)
    start_date: date
    end_date: date

    @model_validator(mode="after")
    def validate_range(self) -> BackfillShard:
        if self.end_date < self.start_date:
            raise ValueError("shard end_date cannot precede start_date")
        return self


class BackfillShardResult(DataModel):
    shard_id: str
    task_id: str
    status: Literal["succeeded", "empty", "failed", "replayed"]
    attempts: int = Field(ge=0)
    instrument_ids: tuple[str, ...]
    start_date: date
    end_date: date
    row_count: int = Field(ge=0)
    first_date: date | None = None
    last_date: date | None = None
    raw_uri: str | None = Field(default=None, pattern=r"^quant-data://")
    raw_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    parquet_uri: str | None = Field(default=None, pattern=r"^quant-data://")
    parquet_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    quality_uri: str | None = Field(default=None, pattern=r"^quant-data://")
    duplicate_count: int = Field(default=0, ge=0)
    anomaly_count: int = Field(default=0, ge=0)
    missing_classifications: dict[str, str] = Field(default_factory=dict)
    error_class: str | None = None
    error_stage: str | None = None


class BackfillArtifactEvidence(DataModel):
    kind: Literal["raw", "parquet", "quality", "task_state", "universe"]
    uri: str = Field(pattern=r"^quant-data://")
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    bytes: int = Field(ge=0)
    row_count: int | None = Field(default=None, ge=0)


class BackfillShardInventoryEntry(DataModel):
    shard_id: str
    task_id: str
    dataset_version: str
    instrument_ids: tuple[str, ...]
    start_date: date
    end_date: date
    status: Literal["succeeded", "empty", "failed", "replayed"]
    attempts: int = Field(ge=0)
    row_count: int = Field(ge=0)
    first_date: date | None = None
    last_date: date | None = None
    raw_artifacts: tuple[BackfillArtifactEvidence, ...] = ()
    parquet_artifact: BackfillArtifactEvidence | None = None
    quality_artifact: BackfillArtifactEvidence | None = None
    task_state_artifact: BackfillArtifactEvidence
    missing_classifications: dict[str, str] = Field(default_factory=dict)
    error_class: str | None = None
    error_stage: str | None = None


class BackfillRunInventory(DataModel):
    schema_version: Literal["1.0.0"] = "1.0.0"
    dataset_id: Literal["cn_equity_daily_backfill_pilot"] = BACKFILL_DATASET_ID
    dataset_version: str
    universe_version: str
    universe_content_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    universe_artifact: BackfillArtifactEvidence
    created_at: datetime
    shards: tuple[BackfillShardInventoryEntry, ...]
    canonical_eligible: Literal[False] = False
    ss_candidate: Literal[False] = False
    production_pointer_updated: Literal[False] = False

    @model_validator(mode="after")
    def validate_inventory(self) -> BackfillRunInventory:
        shard_ids = [item.shard_id for item in self.shards]
        if len(shard_ids) != len(set(shard_ids)):
            raise ValueError("inventory shard IDs must be unique")
        version_marker = f"version={self.dataset_version}/"
        for shard in self.shards:
            if shard.dataset_version != self.dataset_version:
                raise ValueError("inventory cannot mix dataset versions")
            artifacts = (
                *shard.raw_artifacts,
                shard.parquet_artifact,
                shard.quality_artifact,
                shard.task_state_artifact,
            )
            for artifact in artifacts:
                if artifact is not None and version_marker not in artifact.uri:
                    raise ValueError("inventory artifact URI must bind the dataset version")
        return self

    @computed_field
    @property
    def content_sha256(self) -> str:
        payload = self.model_dump(mode="json", exclude_computed_fields=True)
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest()


class BackfillPilotReport(DataModel):
    schema_version: Literal["1.0.0"] = "1.0.0"
    dataset_id: Literal["cn_equity_daily_backfill_pilot"] = BACKFILL_DATASET_ID
    dataset_version: str
    universe_version: str
    universe_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    started_at: datetime
    completed_at: datetime
    requested_start: date
    requested_end: date
    provider: Literal["baostock"] = "baostock"
    calendar_basis: Literal["provider_observed_coverage_only"] = "provider_observed_coverage_only"
    canonical_eligible: Literal[False] = False
    ss_candidate: Literal[False] = False
    production_pointer_updated: Literal[False] = False
    shard_results: tuple[BackfillShardResult, ...]
    total_rows: int = Field(ge=0)
    succeeded_shards: int = Field(ge=0)
    empty_shards: int = Field(ge=0)
    failed_shards: int = Field(ge=0)
    replayed_shards: int = Field(ge=0)
    retries: int = Field(ge=0)
    duplicate_count: int = Field(ge=0)
    anomaly_count: int = Field(ge=0)
    wall_seconds: float = Field(ge=0)
    rows_per_second: float = Field(ge=0)
    peak_rss_mb: float | None = Field(default=None, ge=0)
    inventory_uri: str | None = Field(default=None, pattern=r"^quant-data://")
    inventory_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    query_index_uri: str | None = Field(default=None, pattern=r"^quant-data://")
    query_index_rebuildable: bool = False
    clean_single_version: bool = False
    query_proof: dict[str, object] = Field(default_factory=dict)
    generated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @model_validator(mode="after")
    def validate_totals(self) -> BackfillPilotReport:
        counted = {
            "succeeded": sum(item.status == "succeeded" for item in self.shard_results),
            "empty": sum(item.status == "empty" for item in self.shard_results),
            "failed": sum(item.status == "failed" for item in self.shard_results),
            "replayed": sum(item.status == "replayed" for item in self.shard_results),
        }
        if counted != {
            "succeeded": self.succeeded_shards,
            "empty": self.empty_shards,
            "failed": self.failed_shards,
            "replayed": self.replayed_shards,
        }:
            raise ValueError("pilot shard result counts do not reconcile")
        if sum(item.row_count for item in self.shard_results) != self.total_rows:
            raise ValueError("pilot row count does not reconcile")
        return self
