from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import date
from enum import StrEnum
from typing import Literal

from pydantic import Field, model_validator

from .models import DataModel, DatasetKind, DatasetManifest, Market


class FetchSemantics(StrEnum):
    BY_DATE = "by_date"
    SNAPSHOT = "snapshot"


class CapabilityState(StrEnum):
    UNAVAILABLE = "unavailable"
    PARTIAL = "partial"
    INEXACT = "inexact"
    EXACT = "exact"


class PITState(StrEnum):
    UNSUPPORTED = "unsupported"
    REQUIRED = "required"
    VERIFIED = "verified"


class RouteStatus(StrEnum):
    CANDIDATE = "candidate"
    VALIDATED = "validated"
    BLOCKED = "blocked"


class ContractEvidenceState(StrEnum):
    DOCS_FIXTURE_DERIVED = "docs_fixture_derived"
    LIVE_OBSERVED = "live_observed"


class RouteValidationStage(StrEnum):
    CONTRACT_DEFINED = "contract_defined"
    ENDPOINT_SAMPLE_OBSERVED = "endpoint_sample_observed"
    BOUNDED_SAMPLE_VALIDATED = "bounded_sample_validated"
    PRODUCTION_READY = "production_ready"


class RouteEvidenceScope(DataModel):
    instrument_ids: tuple[str, ...] = Field(min_length=1)
    exchanges: tuple[str, ...] = Field(min_length=1)
    start_date: date
    end_date: date
    provider_versions: tuple[str, ...] = Field(min_length=1)
    adapter_versions: tuple[str, ...] = Field(min_length=1)
    native_schema_versions: tuple[str, ...] = Field(min_length=1)
    calendar_evidence_version: str = Field(min_length=1)
    overlap_instrument_ids: tuple[str, ...] = ()
    sample_only: bool = True
    full_history_validated: bool = False
    universe_validated: bool = False
    production_ready: bool = False

    @model_validator(mode="after")
    def validate_scope(self) -> RouteEvidenceScope:
        if self.end_date < self.start_date:
            raise ValueError("route evidence end date cannot precede start date")
        if not set(self.overlap_instrument_ids).issubset(self.instrument_ids):
            raise ValueError("overlap instruments must be within the observed sample")
        if self.production_ready and (
            self.sample_only or not self.full_history_validated or not self.universe_validated
        ):
            raise ValueError("production-ready route evidence requires non-sample full scope")
        return self


class HistorySemantics(DataModel):
    availability: Literal["bounded_provider_history", "snapshot_accumulation"]
    watermark_field: str
    revision_policy: Literal["new_dataset_version"] = "new_dataset_version"


class PartitionSemantics(DataModel):
    keys: tuple[str, ...] = Field(min_length=1)
    target_rows_per_file: int = Field(ge=1)
    fragmentation_review_below_rows: int = Field(ge=1)


class CanonicalSourceRoute(DataModel):
    route_id: str
    provider: str
    endpoint: str
    exchanges: tuple[str, ...] = Field(min_length=1)
    purposes: tuple[Literal["bootstrap", "daily_increment", "gap_repair"], ...] = Field(
        min_length=1
    )
    status: RouteStatus
    contract_evidence: ContractEvidenceState
    validation_stage: RouteValidationStage
    evidence_scope: RouteEvidenceScope
    evidence_refs: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def validated_routes_require_live_evidence(self) -> CanonicalSourceRoute:
        if (
            self.status is RouteStatus.VALIDATED
            and self.contract_evidence is not ContractEvidenceState.LIVE_OBSERVED
        ):
            raise ValueError("validated routes require live-observed contract evidence")
        if (
            self.validation_stage is RouteValidationStage.PRODUCTION_READY
        ) != self.evidence_scope.production_ready:
            raise ValueError("route validation stage conflicts with evidence scope")
        return self


class CrossValidationRoute(DataModel):
    primary_provider: str
    comparison_provider: str
    exchanges: tuple[str, ...] = Field(min_length=1)
    status: RouteStatus
    validation_stage: RouteValidationStage
    evidence_scope: RouteEvidenceScope
    evidence_refs: tuple[str, ...] = Field(min_length=1)


class CanonicalRoutingSemantics(DataModel):
    routes: tuple[CanonicalSourceRoute, ...] = Field(min_length=1)
    cross_validation: tuple[CrossValidationRoute, ...] = ()
    automatic_switching: Literal[False] = False
    comparison_may_overwrite_canonical: Literal[False] = False
    rule_change_creates_new_dataset_version: Literal[True] = True


class ResearchQueryCapability(DataModel):
    schema_version: Literal["1.0.0"] = "1.0.0"
    dataset_id: str
    unadjusted: CapabilityState
    adjustment: CapabilityState
    universe: CapabilityState
    pit: PITState
    adjustment_factor_dataset_id: str | None = None
    universe_dataset_id: str | None = None
    blocked_operations: tuple[str, ...] = ()

    @model_validator(mode="after")
    def forbid_implicit_capabilities(self) -> ResearchQueryCapability:
        if self.adjustment is CapabilityState.EXACT and not self.adjustment_factor_dataset_id:
            raise ValueError("exact adjustment requires an adjustment factor dataset")
        if self.universe is CapabilityState.EXACT and not self.universe_dataset_id:
            raise ValueError("exact universe requires a versioned universe dataset")
        return self


class CalendarCapability(DataModel):
    status: Literal["verified", "partial", "unavailable", "blocked"]
    evidence_version: str
    exchanges: tuple[str, ...] = Field(min_length=1)
    supported_start: date
    supported_end: date
    official_exchange_evidence: bool
    temporary_changes_require_new_version: Literal[True] = True
    unknown_dates_fail_closed: Literal[True] = True
    evidence_refs: tuple[str, ...] = Field(min_length=1)
    limitations: tuple[str, ...] = ()

    @model_validator(mode="after")
    def validate_calendar_capability(self) -> CalendarCapability:
        if self.supported_end < self.supported_start:
            raise ValueError("calendar capability end cannot precede start")
        if self.status == "verified" and not self.official_exchange_evidence:
            raise ValueError("verified calendar capability requires official evidence")
        return self


class MarketStateCapability(DataModel):
    status: Literal["verified", "partial", "unavailable", "blocked"]
    evidence_version: str
    exchanges: tuple[str, ...] = Field(min_length=1)
    finite_sample_only: Literal[True] = True
    effective_dated: bool
    current_snapshot_not_historical: Literal[True] = True
    evidence_refs: tuple[str, ...] = Field(min_length=1)
    verified_states: tuple[str, ...] = ()
    partial_states: tuple[str, ...] = ()
    blocked_states: tuple[str, ...] = ()
    limitations: tuple[str, ...] = ()


class DatasetSemantics(DataModel):
    schema_version: Literal["1.2.0", "1.3.0"] = "1.2.0"
    dataset_id: str
    kind: DatasetKind
    market: Market
    frequency: str
    fetch_semantics: FetchSemantics
    history: HistorySemantics
    partition: PartitionSemantics
    research: ResearchQueryCapability
    routing: CanonicalRoutingSemantics
    calendar: CalendarCapability | None = None
    market_states: MarketStateCapability | None = None
    required_quality_status: tuple[Literal["passed", "warning"], ...] = ("passed",)
    eligible_consumers: tuple[Literal["batch_api", "research", "ss_shadow"], ...]

    @model_validator(mode="after")
    def identifiers_match(self) -> DatasetSemantics:
        if self.research.dataset_id != self.dataset_id:
            raise ValueError("research capability must describe the same dataset")
        return self

    def validate_manifest(self, manifest: DatasetManifest) -> None:
        mismatches = [
            name
            for name in ("dataset_id", "kind", "market", "frequency")
            if getattr(self, name) != getattr(manifest, name)
        ]
        if mismatches:
            raise ValueError("dataset semantics mismatch: " + ", ".join(sorted(mismatches)))
        if manifest.quality_status not in self.required_quality_status:
            raise ValueError(
                f"dataset quality {manifest.quality_status!r} is not consumer eligible"
            )
        if manifest.manifest_schema_version == "1.3.0":
            if self.calendar is not None:
                if manifest.calendar_evidence is None:
                    raise ValueError("current manifest requires official calendar evidence")
                calendar = manifest.calendar_evidence
                if (
                    calendar.source_kind != "exchange_announcement"
                    or calendar.version != self.calendar.evidence_version
                    or not set(self.calendar.exchanges).issubset(calendar.exchanges)
                ):
                    raise ValueError("manifest calendar evidence conflicts with semantics")
            if self.market_states is not None:
                if manifest.market_state_sample is None:
                    raise ValueError("current manifest requires market-state evidence")
                if manifest.market_state_sample.version != self.market_states.evidence_version:
                    raise ValueError("manifest market-state evidence conflicts with semantics")
        expected_suffix = f"/market={manifest.market}/version={manifest.version}/bars.parquet"
        if self.kind is DatasetKind.MARKET_BARS and not manifest.storage_uri.endswith(
            expected_suffix
        ):
            raise ValueError("dataset storage does not match declared partition layout")

    def fingerprint(self) -> str:
        payload = json.dumps(
            self.model_dump(mode="json"),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(payload.encode()).hexdigest()


class DatasetSemanticsRegistry:
    """Capability registry; Manifest remains the dataset-version source of truth."""

    def __init__(
        self,
        entries: tuple[DatasetSemantics, ...],
        *,
        consumer_policies: Mapping[str, tuple[str, ...]] | None = None,
    ) -> None:
        self._entries = {entry.dataset_id: entry for entry in entries}
        if len(self._entries) != len(entries):
            raise ValueError("dataset semantics registry contains duplicate dataset IDs")
        self._consumer_policies = dict(consumer_policies or {})
        for entry in entries:
            configured = self._consumer_policies.get(entry.dataset_id)
            if configured is not None and configured != entry.eligible_consumers:
                raise ValueError("consumer policy conflicts with dataset semantics")
            self._consumer_policies[entry.dataset_id] = entry.eligible_consumers

    def find(self, dataset_id: str) -> DatasetSemantics | None:
        return self._entries.get(dataset_id)

    def require(self, dataset_id: str) -> DatasetSemantics:
        entry = self.find(dataset_id)
        if entry is None:
            raise LookupError(f"dataset semantics are not registered: {dataset_id}")
        return entry

    def validate_manifest_if_registered(self, manifest: DatasetManifest) -> None:
        entry = self.find(manifest.dataset_id)
        if entry is not None:
            entry.validate_manifest(manifest)

    def consumer_eligible(self, dataset_id: str, consumer: str) -> bool:
        return consumer in self._consumer_policies.get(dataset_id, ())

    def fingerprint(self, dataset_id: str) -> str | None:
        entry = self.find(dataset_id)
        return entry.fingerprint() if entry is not None else None


CN_EQUITY_DAILY_SEMANTICS = DatasetSemantics(
    schema_version="1.3.0",
    dataset_id="cn_equity_daily",
    kind=DatasetKind.MARKET_BARS,
    market="CN",
    frequency="1d",
    fetch_semantics=FetchSemantics.BY_DATE,
    history=HistorySemantics(
        availability="bounded_provider_history",
        watermark_field="trade_date",
    ),
    partition=PartitionSemantics(
        keys=("market", "version"),
        target_rows_per_file=5_000_000,
        fragmentation_review_below_rows=1,
    ),
    research=ResearchQueryCapability(
        dataset_id="cn_equity_daily",
        unadjusted=CapabilityState.EXACT,
        adjustment=CapabilityState.UNAVAILABLE,
        universe=CapabilityState.PARTIAL,
        pit=PITState.UNSUPPORTED,
        blocked_operations=(
            "adjusted_price_query",
            "historical_universe_membership",
            "point_in_time_fundamentals",
        ),
    ),
    calendar=CalendarCapability(
        status="verified",
        evidence_version="cn-official-2026-window-v1",
        exchanges=("XSHG", "XSHE", "BJSE"),
        supported_start="2026-04-29",
        supported_end="2026-05-07",
        official_exchange_evidence=True,
        evidence_refs=(
            "docs/evidence/phase-2.3b-official-cn-calendar-2026-v1.json",
        ),
        limitations=(
            "bounded validation window only; dates outside the range are unknown",
            "temporary exchange changes require a new immutable evidence version",
        ),
    ),
    market_states=MarketStateCapability(
        status="partial",
        evidence_version="cn-market-state-sample-2026-08-04-v1",
        exchanges=("XSHG", "XSHE", "BJSE"),
        effective_dated=True,
        evidence_refs=(
            "docs/evidence/phase-2.3b-cn-market-state-sample-2026-08-04-v1.json",
        ),
        verified_states=(
            "risk_warning",
            "suspended",
            "recent_listing",
            "delisted",
            "inactive",
        ),
        partial_states=("normal", "short_history", "zero_volume"),
        blocked_states=("BJSE.normal", "BJSE.code_change"),
        limitations=(
            "representative finite sample, not a full-market universe",
            "provider snapshots cannot establish historical point-in-time state",
            "BJSE canonical bars remain blocked without reliable two-source evidence",
        ),
    ),
    routing=CanonicalRoutingSemantics(
        routes=(
            CanonicalSourceRoute(
                route_id="xshg-xshe-bootstrap",
                provider="baostock",
                endpoint="query_history_k_data_plus",
                exchanges=("XSHG", "XSHE"),
                purposes=("bootstrap",),
                status=RouteStatus.VALIDATED,
                contract_evidence=ContractEvidenceState.LIVE_OBSERVED,
                validation_stage=RouteValidationStage.BOUNDED_SAMPLE_VALIDATED,
                evidence_scope=RouteEvidenceScope(
                    instrument_ids=("XSHG.600000", "XSHE.000001"),
                    exchanges=("XSHG", "XSHE"),
                    start_date="2026-04-29",
                    end_date="2026-05-07",
                    provider_versions=("baostock@0.9.3",),
                    adapter_versions=("real-cn-v1",),
                    native_schema_versions=("live-observed-2026-08-03-v1",),
                    calendar_evidence_version="cn-official-2026-window-v1",
                    overlap_instrument_ids=("XSHG.600000", "XSHE.000001"),
                ),
                evidence_refs=(
                    "docs/evidence/phase-2.3b-calendar-market-state-2026-08-04.json",
                    "docs/evidence/phase-2.3b-provider-reliability-2026-08-04.json",
                ),
            ),
            CanonicalSourceRoute(
                route_id="cn-daily-and-repair",
                provider="akshare",
                endpoint="stock_zh_a_hist",
                exchanges=("XSHG", "XSHE", "BJSE"),
                purposes=("daily_increment", "gap_repair"),
                status=RouteStatus.BLOCKED,
                contract_evidence=ContractEvidenceState.DOCS_FIXTURE_DERIVED,
                validation_stage=RouteValidationStage.CONTRACT_DEFINED,
                evidence_scope=RouteEvidenceScope(
                    instrument_ids=(
                        "XSHG.600000",
                        "XSHE.000001",
                        "BJSE.920047",
                    ),
                    exchanges=("XSHG", "XSHE", "BJSE"),
                    start_date="2026-07-29",
                    end_date="2026-07-31",
                    provider_versions=("akshare@1.18.81",),
                    adapter_versions=("real-cn-v2",),
                    native_schema_versions=("live-observed-2026-08-03-v1",),
                    calendar_evidence_version=(
                        "provider-observed-noncanonical-0.9.3-controlled-window-v1"
                    ),
                    overlap_instrument_ids=("XSHG.600000", "XSHE.000001"),
                ),
                evidence_refs=(
                    "docs/evidence/phase-2.3b-provider-reliability-2026-08-04.json",
                ),
            ),
        ),
        cross_validation=(
            CrossValidationRoute(
                primary_provider="baostock",
                comparison_provider="tdx_reference",
                exchanges=("XSHG", "XSHE"),
                status=RouteStatus.VALIDATED,
                validation_stage=RouteValidationStage.BOUNDED_SAMPLE_VALIDATED,
                evidence_scope=RouteEvidenceScope(
                    instrument_ids=("XSHG.600000", "XSHE.000001"),
                    exchanges=("XSHG", "XSHE"),
                    start_date="2026-04-29",
                    end_date="2026-05-07",
                    provider_versions=("baostock@0.9.3", "tdxpy@0.2.7"),
                    adapter_versions=("real-cn-v1", "real-cn-tdx-v1"),
                    native_schema_versions=("live-observed-2026-08-03-v1",),
                    calendar_evidence_version="cn-official-2026-window-v1",
                    overlap_instrument_ids=("XSHG.600000", "XSHE.000001"),
                ),
                evidence_refs=(
                    "docs/evidence/phase-2.3b-calendar-market-state-2026-08-04.json",
                    "docs/evidence/phase-2.3b-provider-reliability-2026-08-04.json",
                ),
            ),
            CrossValidationRoute(
                primary_provider="baostock",
                comparison_provider="akshare",
                exchanges=("XSHG", "XSHE", "BJSE"),
                status=RouteStatus.BLOCKED,
                validation_stage=RouteValidationStage.CONTRACT_DEFINED,
                evidence_scope=RouteEvidenceScope(
                    instrument_ids=(
                        "XSHG.600000",
                        "XSHE.000001",
                        "BJSE.920047",
                    ),
                    exchanges=("XSHG", "XSHE", "BJSE"),
                    start_date="2026-07-29",
                    end_date="2026-07-31",
                    provider_versions=("baostock@0.9.3", "akshare@1.18.81"),
                    adapter_versions=("real-cn-v1", "real-cn-v2"),
                    native_schema_versions=("live-observed-2026-08-03-v1",),
                    calendar_evidence_version=(
                        "provider-observed-noncanonical-0.9.3-controlled-window-v1"
                    ),
                    overlap_instrument_ids=("XSHG.600000", "XSHE.000001"),
                ),
                evidence_refs=(
                    "docs/evidence/phase-2.3b-provider-reliability-2026-08-04.json",
                ),
            ),
        ),
    ),
    required_quality_status=("passed",),
    eligible_consumers=("batch_api", "research", "ss_shadow"),
)


DEFAULT_DATASET_SEMANTICS = DatasetSemanticsRegistry(
    (CN_EQUITY_DAILY_SEMANTICS,),
    consumer_policies={
        "instruments_cn": ("batch_api",),
        "instruments_us": ("batch_api",),
        "synthetic_cn_batch_benchmark": ("batch_api",),
        "synthetic_us_equity_daily": ("batch_api",),
        "us_equity_split_adjusted_shadow": ("batch_api",),
        "us_equity_total_return_shadow": ("batch_api",),
        "us_equity_split_adjusted": ("batch_api", "research", "ss_shadow"),
        "us_equity_total_return": ("batch_api", "research", "ss_shadow"),
    },
)
