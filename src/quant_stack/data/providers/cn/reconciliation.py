from __future__ import annotations

from datetime import date, datetime
from enum import StrEnum
from typing import Literal

from pydantic import Field, model_validator

from ...models import DataModel


class DiscrepancyStatus(StrEnum):
    OPEN = "open"
    ACCEPTED = "accepted"
    RESOLVED = "resolved"


class PartitionStatus(StrEnum):
    CANDIDATE = "candidate"
    PASSED = "passed"
    QUARANTINED = "quarantined"


class EvidenceMode(StrEnum):
    FIXTURE = "fixture"
    LIVE = "live"


class ProviderTransportIdentity(DataModel):
    """Sanitized identity for the exact transport/source used by a live Raw object."""

    schema_version: Literal["1.0.0"] = "1.0.0"
    host_pool_contract_id: str = Field(min_length=1)
    host_pool_contract_version: str = Field(min_length=1)
    host_pool_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    configured_candidates: int = Field(ge=1)
    max_host_attempts: int = Field(ge=1)
    attempted_source_fingerprints: tuple[str, ...]
    successful_source_fingerprint: str | None = Field(
        default=None, pattern=r"^[a-f0-9]{64}$"
    )
    failure_classes: tuple[str, ...] = ()
    close_failure_classes: tuple[str, ...] = ()
    protocol_identity: str = Field(min_length=1)
    client_package: str = Field(min_length=1)
    client_version: str = Field(min_length=1)
    transport_security: str = Field(min_length=1)
    server_authenticated: bool
    encrypted: bool
    source_terms_status: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_attempt_identity(self) -> ProviderTransportIdentity:
        if self.max_host_attempts > self.configured_candidates:
            raise ValueError("transport host-attempt bound exceeds the configured pool")
        if len(self.attempted_source_fingerprints) > self.max_host_attempts:
            raise ValueError("transport attempts exceed the bounded host contract")
        if any(
            len(value) != 64 or any(character not in "0123456789abcdef" for character in value)
            for value in self.attempted_source_fingerprints
        ):
            raise ValueError("transport source fingerprints must be SHA-256 values")
        if self.successful_source_fingerprint is not None and (
            self.successful_source_fingerprint not in self.attempted_source_fingerprints
        ):
            raise ValueError("successful source must be present in the attempted order")
        if not self.host_pool_contract_id.endswith(self.host_pool_fingerprint[:16]):
            raise ValueError("host pool changes must alter the transport contract identity")
        return self


class ProviderSourceIdentity(DataModel):
    provider: str = Field(min_length=1)
    endpoint: str = Field(min_length=1)
    provider_version: str = Field(min_length=1)
    adapter_version: str = Field(min_length=1)
    native_schema_version: str = Field(min_length=1)
    raw_uri: str = Field(pattern=r"^quant-data://")
    raw_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    transport_identity: ProviderTransportIdentity | None = None

    @model_validator(mode="after")
    def require_tdx_transport_identity(self) -> ProviderSourceIdentity:
        if self.provider == "tdx_reference" and self.transport_identity is None:
            raise ValueError("TDX source identity requires exact transport evidence")
        return self


class ComparisonFailure(DataModel):
    error_class: str = Field(pattern=r"^[A-Za-z_][A-Za-z0-9_.]*$")
    stage: Literal["provider_fetch", "schema_validation", "normalization"]


class CNDiscrepancyRecord(DataModel):
    schema_version: Literal["1.0.0"] = "1.0.0"
    dataset_partition: str = Field(min_length=1)
    instrument_id: str = Field(min_length=3)
    trade_date: date
    primary_provider: str = Field(min_length=1)
    comparison_provider: str = Field(min_length=1)
    primary_raw_uri: str = Field(pattern=r"^quant-data://")
    comparison_raw_uri: str = Field(pattern=r"^quant-data://")
    differing_fields: tuple[str, ...] = Field(min_length=1)
    tolerance_policy: str = Field(min_length=1)
    discrepancy_status: DiscrepancyStatus = DiscrepancyStatus.OPEN
    canonical_source_provider: str | None = None
    canonical_selection_reason: str | None = None
    created_at: datetime

    @model_validator(mode="after")
    def validate_discrepancy(self) -> CNDiscrepancyRecord:
        if self.created_at.tzinfo is None:
            raise ValueError("created_at must be timezone-aware")
        selected = self.canonical_source_provider is not None
        reasoned = self.canonical_selection_reason is not None
        if selected != reasoned:
            raise ValueError("canonical source and selection reason must be recorded together")
        return self


class CNPartitionPromotion(DataModel):
    dataset_partition: str = Field(min_length=1)
    dataset_version: str = Field(min_length=1)
    status: PartitionStatus
    discrepancy_uris: tuple[str, ...] = ()
    catalog_eligible: bool

    @model_validator(mode="after")
    def quarantined_partitions_cannot_be_promoted(self) -> CNPartitionPromotion:
        if self.status is PartitionStatus.QUARANTINED and self.catalog_eligible:
            raise ValueError("quarantined partitions cannot enter the catalog")
        return self


class CNProviderDiscrepancyReport(DataModel):
    """Immutable, sanitized evidence envelope for fixture or controlled live overlap."""

    schema_version: Literal["1.0.0", "1.1.0", "1.2.0"] = "1.2.0"
    report_id: str = Field(pattern=r"^[a-zA-Z0-9._-]+$")
    generated_at: datetime
    evidence_mode: EvidenceMode = EvidenceMode.FIXTURE
    fixture_only: bool = True
    primary_raw_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    comparison_raw_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    primary_identity: ProviderSourceIdentity | None = None
    comparison_identity: ProviderSourceIdentity | None = None
    status: Literal["passed", "unavailable", "quarantined"] = "passed"
    comparison_required: bool = False
    failure: ComparisonFailure | None = None
    canonical_selection_reason: str = Field(min_length=1)
    records: tuple[CNDiscrepancyRecord, ...] = ()

    @model_validator(mode="after")
    def validate_report(self) -> CNProviderDiscrepancyReport:
        if self.generated_at.tzinfo is None:
            raise ValueError("generated_at must be timezone-aware")
        if self.fixture_only != (self.evidence_mode is EvidenceMode.FIXTURE):
            raise ValueError("fixture_only must match evidence_mode")
        if self.evidence_mode is EvidenceMode.LIVE and (
            self.primary_raw_sha256 is None or self.comparison_raw_sha256 is None
        ):
            raise ValueError("live discrepancy reports require both Raw hashes")
        if self.evidence_mode is EvidenceMode.LIVE and (
            self.primary_identity is None or self.comparison_identity is None
        ):
            raise ValueError("live discrepancy reports require exact source identities")
        for identity, raw_sha256 in (
            (self.primary_identity, self.primary_raw_sha256),
            (self.comparison_identity, self.comparison_raw_sha256),
        ):
            if identity is not None and identity.raw_sha256 != raw_sha256:
                raise ValueError("source identity Raw hash must match report evidence")
        if self.status == "quarantined" and not self.records:
            raise ValueError("quarantined discrepancy reports require records")
        if self.status == "unavailable" and self.failure is None:
            raise ValueError("unavailable comparison requires sanitized failure evidence")
        if self.status != "unavailable" and self.failure is not None:
            raise ValueError("failure evidence is valid only for unavailable comparison")
        if self.status == "unavailable" and self.records:
            raise ValueError("unavailable comparison cannot fabricate discrepancy records")
        if (
            self.status == "passed"
            and self.comparison_identity is not None
            and self.comparison_identity.provider == "tdx_reference"
            and (
                self.comparison_identity.transport_identity is None
                or self.comparison_identity.transport_identity.successful_source_fingerprint is None
            )
        ):
            raise ValueError("passed TDX comparison requires a successful transport source")
        return self
