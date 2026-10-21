from __future__ import annotations

import fcntl
import hashlib
import json
import platform
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from pydantic import Field, computed_field, model_validator

from .catalog import DuckDBCatalog
from .catalog.duckdb_catalog import manifest_sha256
from .ingestion import IngestionResult
from .models import DataModel, DatasetLayer, DatasetManifest
from .providers.cn.reconciliation import (
    CNProviderDiscrepancyReport,
    ProviderSourceIdentity,
    ProviderTransportIdentity,
)
from .storage import LocalFileSystemStorage
from .storage.atomic import atomic_replace_bytes, atomic_write_immutable_bytes


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_write(path: Path, payload: bytes) -> None:
    atomic_replace_bytes(path, payload)


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


ComparisonStatus = Literal[
    "not_required",
    "passed",
    "unavailable",
    "serious_discrepancy",
    "source_identity_drift",
]
PublicationGate = Literal["eligible", "incomplete", "blocked", "quarantined"]


class WorkerImportEvidenceRef(DataModel):
    """Hash-bound reference to NAS-persisted, authenticated worker import evidence."""

    receipt_uri: str = Field(pattern=r"^quant-data://")
    receipt_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    worker_bundle_id: str = Field(pattern=r"^worker-[a-f0-9]{32}$")
    envelope_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    archive_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    source_commit: str = Field(pattern=r"^[a-f0-9]{40}$")
    worker_environment_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    provider_contract_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")


class PublicationArtifact(DataModel):
    uri: str = Field(pattern=r"^quant-data://")
    kind: Literal[
        "raw",
        "primary_raw",
        "comparison_raw",
        "instruments",
        "bars",
        "quality",
        "discrepancy",
        "proposed_manifest",
    ]
    size_bytes: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    dataset_id: str | None = None
    dataset_version: str | None = None

    @model_validator(mode="after")
    def bind_dataset_version_together(self) -> PublicationArtifact:
        if (self.dataset_id is None) != (self.dataset_version is None):
            raise ValueError("artifact dataset ID and version must be recorded together")
        return self


class ConsumerEligibilityPolicy(DataModel):
    route_class: Literal["canonical", "reference", "optional"] = "canonical"
    shadow_allowed: bool = True
    candidate_allowed: bool = True
    reason: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_route_class(self) -> ConsumerEligibilityPolicy:
        if self.route_class in {"reference", "optional"} and self.candidate_allowed:
            raise ValueError("reference and optional routes cannot be SS candidates")
        if self.candidate_allowed and not self.shadow_allowed:
            raise ValueError("candidate policy requires shadow access")
        return self


def canonical_consumer_eligibility() -> ConsumerEligibilityPolicy:
    return ConsumerEligibilityPolicy(
        route_class="canonical",
        shadow_allowed=True,
        candidate_allowed=True,
        reason="explicit canonical route may progress through controlled consumer gates",
    )


def optional_consumer_eligibility() -> ConsumerEligibilityPolicy:
    return ConsumerEligibilityPolicy(
        route_class="optional",
        shadow_allowed=True,
        candidate_allowed=False,
        reason="optional/reference provider evidence is shadow-only and never an SS candidate",
    )


class PublicationState(DataModel):
    """Consumer-specific state for one immutable dataset version."""

    quality_passed: bool
    catalog_published: bool
    comparison_complete: bool
    shadow_eligible: bool
    candidate_eligible: bool
    cutover_eligible: bool = False

    @model_validator(mode="after")
    def validate_eligibility_order(self) -> PublicationState:
        if self.candidate_eligible and not self.shadow_eligible:
            raise ValueError("candidate eligibility requires shadow eligibility")
        if self.cutover_eligible and not self.candidate_eligible:
            raise ValueError("cutover eligibility requires candidate eligibility")
        if (self.shadow_eligible or self.candidate_eligible or self.cutover_eligible) and not (
            self.quality_passed and self.catalog_published
        ):
            raise ValueError("consumer eligibility requires quality and Catalog publication")
        return self


def publication_state_for(
    gate: PublicationGate,
    *,
    quality_passed: bool,
    comparison_status: ComparisonStatus,
    consumer_eligibility: ConsumerEligibilityPolicy | None = None,
) -> PublicationState:
    policy = consumer_eligibility or canonical_consumer_eligibility()
    comparison_complete = comparison_status in {
        "not_required",
        "passed",
        "serious_discrepancy",
        "source_identity_drift",
    }
    catalog_published = gate in {"eligible", "incomplete"} and quality_passed
    shadow_eligible = catalog_published and policy.shadow_allowed
    candidate_eligible = (
        gate == "eligible"
        and quality_passed
        and comparison_complete
        and policy.candidate_allowed
    )
    return PublicationState(
        quality_passed=quality_passed,
        catalog_published=catalog_published,
        comparison_complete=comparison_complete,
        shadow_eligible=shadow_eligible,
        candidate_eligible=candidate_eligible,
        # Phase 2.4 has not started. No Phase 2.3 decision can assert cutover.
        cutover_eligible=False,
    )


class ProducerEnvironment(DataModel):
    environment_id: str
    hostname: str
    system: str
    machine: str
    python_version: str

    @classmethod
    def current(cls, environment_id: str = "local") -> ProducerEnvironment:
        return cls(
            environment_id=environment_id,
            hostname=platform.node() or "unknown",
            system=platform.system(),
            machine=platform.machine(),
            python_version=platform.python_version(),
        )


class PublicationBundle(DataModel):
    schema_version: Literal["1.0.0", "1.1.0", "1.2.0", "1.3.0"] = "1.3.0"
    task_id: str
    attempt_id: str
    dataset_id: str
    dataset_version: str
    provider: str
    endpoint: str
    artifacts: tuple[PublicationArtifact, ...] = Field(min_length=1)
    proposed_manifests: tuple[DatasetManifest, ...] = Field(min_length=1)
    created_at: datetime
    producer_environment: ProducerEnvironment
    # Deprecated compatibility projections. Publisher decisions use publication_state.
    canonical_eligible: bool
    quarantined: bool = False
    discrepancy_report_uri: str | None = Field(default=None, pattern=r"^quant-data://")
    discrepancy_report_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    comparison_status: ComparisonStatus = "not_required"
    comparison_required: bool = False
    publication_gate: PublicationGate = "eligible"
    publication_state: PublicationState | None = None
    consumer_eligibility: ConsumerEligibilityPolicy = Field(
        default_factory=canonical_consumer_eligibility
    )
    primary_source_identity: ProviderSourceIdentity | None = None
    comparison_source_identity: ProviderSourceIdentity | None = None
    canonical_source_provider: str = Field(min_length=1)
    canonical_source_endpoint: str = Field(min_length=1)
    canonical_selection_reason: str = Field(min_length=1)
    selection_rule_id: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_publication_gate(self) -> PublicationBundle:
        if self.created_at.tzinfo is None:
            raise ValueError("created_at must be timezone-aware")
        if self.quarantined and self.canonical_eligible:
            raise ValueError("quarantined bundles cannot be catalog eligible")
        if any(manifest.quality_status == "failed" for manifest in self.proposed_manifests):
            if self.canonical_eligible:
                raise ValueError("failed quality manifests cannot be catalog eligible")
        manifest_versions = {item.version for item in self.proposed_manifests}
        if manifest_versions != {self.dataset_version}:
            raise ValueError("all proposed manifests must use the bundle dataset version")
        artifact_uris = [item.uri for item in self.artifacts]
        if len(artifact_uris) != len(set(artifact_uris)):
            raise ValueError("publication artifact URIs must be unique")
        required_kinds = {
            "raw",
            "instruments",
            "bars",
            "quality",
            "proposed_manifest",
        }
        if not required_kinds.issubset({item.kind for item in self.artifacts}):
            raise ValueError("publication bundle is missing a required artifact kind")
        manifest_keys = [
            (manifest.dataset_id, manifest.version) for manifest in self.proposed_manifests
        ]
        if len(manifest_keys) != len(set(manifest_keys)):
            raise ValueError("proposed manifest keys must be unique")
        proposed_artifact_count = sum(
            artifact.kind == "proposed_manifest" for artifact in self.artifacts
        )
        if proposed_artifact_count != len(self.proposed_manifests):
            raise ValueError(
                "proposed manifest artifacts must map one-to-one to proposed manifests"
            )
        if self.discrepancy_report_uri is not None and not any(
            item.kind == "discrepancy" and item.uri == self.discrepancy_report_uri
            for item in self.artifacts
        ):
            raise ValueError("discrepancy report must be included in artifact inventory")
        if (self.discrepancy_report_uri is None) != (self.discrepancy_report_sha256 is None):
            raise ValueError("discrepancy report URI and hash must be recorded together")
        if self.discrepancy_report_uri is not None:
            discrepancy = next(
                item
                for item in self.artifacts
                if item.kind == "discrepancy" and item.uri == self.discrepancy_report_uri
            )
            if discrepancy.sha256 != self.discrepancy_report_sha256:
                raise ValueError("discrepancy report hash conflicts with artifact inventory")
        if self.comparison_status != "not_required" and self.discrepancy_report_uri is None:
            raise ValueError("comparison result requires discrepancy report evidence")
        if (
            self.schema_version in {"1.2.0", "1.3.0"}
            and self.comparison_required
            and (self.comparison_status == "not_required")
        ):
            raise ValueError("required comparison cannot be downgraded to not_required")
        if self.schema_version in {"1.2.0", "1.3.0"}:
            for artifact in self.artifacts:
                if (artifact.dataset_id, artifact.dataset_version) != (
                    self.dataset_id,
                    self.dataset_version,
                ):
                    raise ValueError("publication artifact dataset version conflict")
            if self.primary_source_identity is None:
                raise ValueError("publication bundle requires exact primary source identity")
            primary_raw = next((item for item in self.artifacts if item.kind == "raw"), None)
            if primary_raw is None or (
                primary_raw.uri,
                primary_raw.sha256,
            ) != (
                self.primary_source_identity.raw_uri,
                self.primary_source_identity.raw_sha256,
            ):
                raise ValueError("primary Raw artifact conflicts with source identity")
            # An unavailable comparison records the attempted gate and its
            # failure evidence, but does not fabricate a source identity for a
            # provider that was never reached.  A real comparison outcome still
            # requires the exact comparison Raw/source identity.
            comparison_required_evidence = self.comparison_status not in {
                "not_required",
                "unavailable",
            }
            if comparison_required_evidence:
                if self.comparison_source_identity is None:
                    raise ValueError("comparison result requires exact comparison source identity")
                comparison_raw = next(
                    (item for item in self.artifacts if item.kind == "comparison_raw"),
                    None,
                )
                if comparison_raw is None or (
                    comparison_raw.uri,
                    comparison_raw.sha256,
                ) != (
                    self.comparison_source_identity.raw_uri,
                    self.comparison_source_identity.raw_sha256,
                ):
                    raise ValueError("comparison Raw artifact conflicts with source identity")
                if (
                    self.comparison_source_identity.provider,
                    self.comparison_source_identity.endpoint,
                ) == (
                    self.canonical_source_provider,
                    self.canonical_source_endpoint,
                ):
                    raise ValueError("comparison source cannot replace canonical source")
                if (
                    self.comparison_status == "passed"
                    and self.comparison_source_identity.provider == "tdx_reference"
                    and (
                        self.comparison_source_identity.transport_identity is None
                        or (
                            self.comparison_source_identity.transport_identity
                            .successful_source_fingerprint
                            is None
                        )
                    )
                ):
                    raise ValueError("passed TDX comparison requires successful source evidence")
        expected_gate = {
            "not_required": "eligible",
            "passed": "eligible",
            "unavailable": "blocked" if self.comparison_required else "incomplete",
            "serious_discrepancy": "quarantined",
            "source_identity_drift": "quarantined",
        }[self.comparison_status]
        quality_eligible = all(
            item.layer is not DatasetLayer.NORMALIZED
            or item.quality_status in {"passed", "warning"}
            for item in self.proposed_manifests
        )
        if not quality_eligible and expected_gate != "quarantined":
            expected_gate = "blocked"
        if self.publication_gate != expected_gate:
            raise ValueError("publication gate conflicts with comparison outcome")
        expected_eligible = self.publication_gate in {"eligible", "incomplete"}
        if self.canonical_eligible != expected_eligible:
            raise ValueError("canonical eligibility conflicts with publication gate")
        if self.quarantined != (self.publication_gate == "quarantined"):
            raise ValueError("quarantine flag conflicts with publication gate")
        if (
            self.canonical_source_provider != self.provider
            or self.canonical_source_endpoint != self.endpoint
        ):
            raise ValueError("canonical source must match the publication payload source")
        quality_passed = all(
            item.layer is not DatasetLayer.NORMALIZED or item.quality_status == "passed"
            for item in self.proposed_manifests
        )
        expected_state = publication_state_for(
            self.publication_gate,
            quality_passed=quality_passed,
            comparison_status=self.comparison_status,
            consumer_eligibility=self.consumer_eligibility,
        )
        if self.schema_version in {"1.2.0", "1.3.0"} and self.publication_state != expected_state:
            raise ValueError("publication state conflicts with gate and comparison outcome")
        return self

    def content_payload(self) -> dict[str, object]:
        """Canonical immutable semantics used for an attempt-scoped bundle identity."""

        return {
            "schema_version": self.schema_version,
            "task_id": self.task_id,
            "attempt_id": self.attempt_id,
            "dataset_id": self.dataset_id,
            "dataset_version": self.dataset_version,
            "provider": self.provider,
            "endpoint": self.endpoint,
            "artifacts": [
                item.model_dump(mode="json")
                for item in sorted(self.artifacts, key=lambda value: (value.kind, value.uri))
            ],
            "proposed_manifests": [
                {
                    "dataset_id": manifest.dataset_id,
                    "version": manifest.version,
                    "manifest_sha256": manifest_sha256(manifest),
                }
                for manifest in sorted(
                    self.proposed_manifests,
                    key=lambda value: (value.dataset_id, value.version),
                )
            ],
            "created_at": self.created_at.isoformat(),
            "producer_environment": self.producer_environment.model_dump(mode="json"),
            "canonical_eligible": self.canonical_eligible,
            "quarantined": self.quarantined,
            "discrepancy_report_uri": self.discrepancy_report_uri,
            "discrepancy_report_sha256": self.discrepancy_report_sha256,
            "comparison_status": self.comparison_status,
            "comparison_required": self.comparison_required,
            "publication_gate": self.publication_gate,
            "publication_state": (
                self.publication_state.model_dump(mode="json")
                if self.publication_state is not None
                else None
            ),
            "consumer_eligibility": self.consumer_eligibility.model_dump(mode="json"),
            "primary_source_identity": (
                self.primary_source_identity.model_dump(mode="json")
                if self.primary_source_identity is not None
                else None
            ),
            "comparison_source_identity": (
                self.comparison_source_identity.model_dump(mode="json")
                if self.comparison_source_identity is not None
                else None
            ),
            "canonical_source_provider": self.canonical_source_provider,
            "canonical_source_endpoint": self.canonical_source_endpoint,
            "canonical_selection_reason": self.canonical_selection_reason,
            "selection_rule_id": self.selection_rule_id,
        }

    def decision_payload(self) -> dict[str, object]:
        payload = self.content_payload()
        for transient in (
            "task_id",
            "attempt_id",
            "created_at",
            "producer_environment",
        ):
            payload.pop(transient, None)
        return payload

    @computed_field
    @property
    def decision_fingerprint(self) -> str:
        return hashlib.sha256(_canonical_json(self.decision_payload()).encode()).hexdigest()

    @computed_field
    @property
    def bundle_id(self) -> str:
        payload = _canonical_json(self.content_payload())
        return f"bundle-{hashlib.sha256(payload.encode()).hexdigest()[:32]}"


class PublicationReceipt(DataModel):
    schema_version: Literal["1.0.0", "1.1.0", "1.2.0", "1.3.0"] = "1.3.0"
    evidence_status: Literal["verified", "legacy_unknown"] = "verified"
    receipt_id: str | None = Field(default=None, min_length=1)
    bundle_id: str
    bundle_uri: str | None = Field(default=None, pattern=r"^quant-data://")
    bundle_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    decision_uri: str | None = Field(default=None, pattern=r"^quant-data://")
    decision_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    published_at: datetime
    manifest_uris: tuple[str, ...] = Field(min_length=1)
    dataset_id: str | None = None
    dataset_version: str | None = None
    comparison_status: ComparisonStatus | None = None
    comparison_required: bool | None = None
    publication_gate: PublicationGate | None = None
    publication_state: PublicationState | None = None
    consumer_eligibility: ConsumerEligibilityPolicy = Field(
        default_factory=canonical_consumer_eligibility
    )
    discrepancy_report_uri: str | None = Field(default=None, pattern=r"^quant-data://")
    discrepancy_report_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    primary_source_identity: ProviderSourceIdentity | None = None
    comparison_source_identity: ProviderSourceIdentity | None = None
    canonical_source_provider: str | None = None
    canonical_source_endpoint: str | None = None
    canonical_selection_reason: str | None = None
    selection_rule_id: str | None = None
    publication_evidence_fingerprint: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    worker_import_evidence: WorkerImportEvidenceRef | None = None
    idempotent_replay: bool = False

    @model_validator(mode="before")
    @classmethod
    def mark_legacy_receipts_unknown(cls, value: object) -> object:
        if isinstance(value, dict) and value.get("schema_version", "1.3.0") not in {
            "1.2.0",
            "1.3.0",
        }:
            return {**value, "evidence_status": "legacy_unknown"}
        return value

    @model_validator(mode="after")
    def validate_receipt_evidence(self) -> PublicationReceipt:
        if self.published_at.tzinfo is None:
            raise ValueError("published_at must be timezone-aware")
        if self.schema_version not in {"1.2.0", "1.3.0"}:
            if self.evidence_status != "legacy_unknown":
                raise ValueError("legacy receipts must remain legacy/unknown")
            return self
        if self.evidence_status != "verified":
            raise ValueError("current receipts require verified evidence status")
        required = {
            "receipt_id": self.receipt_id,
            "bundle_uri": self.bundle_uri,
            "bundle_sha256": self.bundle_sha256,
            "decision_uri": self.decision_uri,
            "decision_sha256": self.decision_sha256,
            "dataset_id": self.dataset_id,
            "dataset_version": self.dataset_version,
            "comparison_status": self.comparison_status,
            "publication_gate": self.publication_gate,
            "publication_state": self.publication_state,
            "canonical_source_provider": self.canonical_source_provider,
            "canonical_source_endpoint": self.canonical_source_endpoint,
            "canonical_selection_reason": self.canonical_selection_reason,
            "selection_rule_id": self.selection_rule_id,
            "publication_evidence_fingerprint": self.publication_evidence_fingerprint,
        }
        missing = sorted(name for name, item in required.items() if item is None)
        if missing:
            raise ValueError("verified receipt is missing evidence: " + ", ".join(missing))
        if self.comparison_required is None:
            raise ValueError("verified receipt must bind comparison_required")
        if self.comparison_status not in {"not_required", "unavailable"}:
            if self.primary_source_identity is None:
                raise ValueError("verified comparison receipt requires primary source identity")
            if self.comparison_source_identity is None:
                raise ValueError("verified comparison receipt requires comparison source identity")
        if (self.discrepancy_report_uri is None) != (self.discrepancy_report_sha256 is None):
            raise ValueError("receipt discrepancy URI and hash must be recorded together")
        if self.comparison_status != "not_required" and self.discrepancy_report_uri is None:
            raise ValueError("verified comparison receipt requires discrepancy evidence")
        if self.comparison_required and self.comparison_status == "not_required":
            raise ValueError("required comparison cannot be downgraded to not_required")
        assert self.publication_gate is not None
        assert self.publication_state is not None
        assert self.comparison_status is not None
        expected_state = publication_state_for(
            self.publication_gate,
            quality_passed=self.publication_state.quality_passed,
            comparison_status=self.comparison_status,
            consumer_eligibility=self.consumer_eligibility,
        )
        if self.publication_state != expected_state or not self.publication_state.catalog_published:
            raise ValueError("verified receipt publication state is inconsistent")
        return self


class PublicationDecision(DataModel):
    schema_version: Literal["1.0.0", "1.1.0"] = "1.1.0"
    bundle_id: str
    bundle_uri: str = Field(pattern=r"^quant-data://")
    bundle_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    dataset_id: str
    dataset_version: str
    comparison_status: ComparisonStatus
    comparison_required: bool
    publication_gate: PublicationGate
    publication_state: PublicationState
    consumer_eligibility: ConsumerEligibilityPolicy = Field(
        default_factory=canonical_consumer_eligibility
    )
    discrepancy_report_uri: str | None = Field(default=None, pattern=r"^quant-data://")
    discrepancy_report_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    primary_source_identity: ProviderSourceIdentity | None = None
    comparison_source_identity: ProviderSourceIdentity | None = None
    canonical_source_provider: str
    canonical_source_endpoint: str
    canonical_selection_reason: str
    selection_rule_id: str
    manifest_uris: tuple[str, ...]
    worker_import_evidence: WorkerImportEvidenceRef | None = None
    decided_at: datetime

    @model_validator(mode="after")
    def validate_decision(self) -> PublicationDecision:
        if self.decided_at.tzinfo is None:
            raise ValueError("decided_at must be timezone-aware")
        if not self.publication_state.catalog_published:
            raise ValueError("persisted publication decision must represent Catalog state")
        if (self.discrepancy_report_uri is None) != (self.discrepancy_report_sha256 is None):
            raise ValueError("decision discrepancy URI and hash must be recorded together")
        if self.comparison_status not in {"not_required", "unavailable"}:
            if self.primary_source_identity is None:
                raise ValueError("comparison decision requires primary source identity")
            if self.comparison_source_identity is None:
                raise ValueError("comparison decision requires comparison source identity")
        expected_state = publication_state_for(
            self.publication_gate,
            quality_passed=self.publication_state.quality_passed,
            comparison_status=self.comparison_status,
            consumer_eligibility=self.consumer_eligibility,
        )
        if self.publication_state != expected_state:
            raise ValueError("persisted publication decision state is inconsistent")
        return self

    def content_payload(self) -> dict[str, object]:
        return self.model_dump(mode="json", exclude_computed_fields=True)

    @computed_field
    @property
    def fingerprint(self) -> str:
        return hashlib.sha256(_canonical_json(self.content_payload()).encode()).hexdigest()


class ReceiptReplayVerification(DataModel):
    receipt_id: str | None
    evidence_status: Literal["verified", "legacy_unknown"]
    verified: bool
    bundle_verified: bool = False
    manifests_verified: bool = False
    pointer_verified: bool = False
    duckdb_verified: bool = False
    discrepancy_verified: bool = False
    primary_raw_verified: bool = False
    comparison_raw_verified: bool = False
    worker_import_verified: bool = False


def build_publication_bundle(
    *,
    result: IngestionResult,
    task_id: str,
    attempt_id: str,
    endpoint: str,
    created_at: datetime,
    environment_id: str = "local",
    discrepancy_report_uri: str | None = None,
    comparison_status: ComparisonStatus = "not_required",
    comparison_required: bool = False,
    primary_source_identity: ProviderSourceIdentity | None = None,
    comparison_source_identity: ProviderSourceIdentity | None = None,
    canonical_selection_reason: str = "explicit single-source canonical selection",
    selection_rule_id: str = "single-source-v1",
    consumer_eligibility: ConsumerEligibilityPolicy | None = None,
) -> PublicationBundle:
    data_root = result.raw_artifact.path
    for _ in Path(result.raw_artifact.uri.removeprefix("quant-data://")).parts:
        data_root = data_root.parent
    paths_and_kinds = (
        (result.raw_artifact.path, result.raw_artifact.uri, "raw"),
        (result.instrument_artifact.path, result.instrument_artifact.uri, "instruments"),
        (result.bar_artifact.path, result.bar_artifact.uri, "bars"),
        (result.quality_artifact.path, result.quality_artifact.uri, "quality"),
    )
    artifacts = [
        PublicationArtifact(
            uri=uri,
            kind=kind,
            size_bytes=path.stat().st_size,
            sha256=_sha256(path),
            dataset_id=result.bar_manifest.dataset_id,
            dataset_version=result.bar_manifest.version,
        )
        for path, uri, kind in paths_and_kinds
    ]
    discrepancy_report_sha256 = None
    discrepancy_report: CNProviderDiscrepancyReport | None = None
    if discrepancy_report_uri is not None:
        discrepancy_path = data_root / discrepancy_report_uri.removeprefix("quant-data://")
        discrepancy_report_sha256 = _sha256(discrepancy_path)
        discrepancy_report = CNProviderDiscrepancyReport.model_validate_json(
            discrepancy_path.read_text(encoding="utf-8")
        )
        primary_source_identity = primary_source_identity or discrepancy_report.primary_identity
        comparison_source_identity = (
            comparison_source_identity or discrepancy_report.comparison_identity
        )
        artifacts.append(
            PublicationArtifact(
                uri=discrepancy_report_uri,
                kind="discrepancy",
                size_bytes=discrepancy_path.stat().st_size,
                sha256=discrepancy_report_sha256,
                dataset_id=result.bar_manifest.dataset_id,
                dataset_version=result.bar_manifest.version,
            )
        )
    primary_source_identity = primary_source_identity or ProviderSourceIdentity(
        provider=result.bar_manifest.provider,
        endpoint=endpoint,
        provider_version=result.bar_manifest.provider_version or "unknown",
        adapter_version=result.bar_manifest.adapter_version or "unknown",
        native_schema_version=result.bar_manifest.native_schema_version or "unknown",
        raw_uri=result.raw_artifact.uri,
        raw_sha256=result.raw_artifact.sha256,
    )
    if comparison_source_identity is not None:
        comparison_path = data_root / comparison_source_identity.raw_uri.removeprefix(
            "quant-data://"
        )
        artifacts.append(
            PublicationArtifact(
                uri=comparison_source_identity.raw_uri,
                kind="comparison_raw",
                size_bytes=comparison_path.stat().st_size,
                sha256=_sha256(comparison_path),
                dataset_id=result.bar_manifest.dataset_id,
                dataset_version=result.bar_manifest.version,
            )
        )
    for path, manifest in zip(
        result.manifest_paths,
        (result.raw_manifest, result.instrument_manifest, result.bar_manifest),
        strict=True,
    ):
        payload = path.read_bytes()
        proposed_sha256 = hashlib.sha256(payload).hexdigest()
        durable_path = (
            data_root
            / "catalog/publication_evidence/proposed_manifests"
            / f"{manifest.dataset_id}@{manifest.version}@{proposed_sha256[:16]}.json"
        )
        atomic_write_immutable_bytes(
            durable_path,
            payload,
            artifact_kind=(
                f"publication evidence Manifest {manifest.dataset_id}@{manifest.version}"
            ),
            validate=lambda temporary: DatasetManifest.model_validate_json(
                temporary.read_text(encoding="utf-8")
            ),
        )
        artifacts.append(
            PublicationArtifact(
                uri=f"quant-data://{durable_path.relative_to(data_root).as_posix()}",
                kind="proposed_manifest",
                size_bytes=durable_path.stat().st_size,
                sha256=proposed_sha256,
                dataset_id=result.bar_manifest.dataset_id,
                dataset_version=result.bar_manifest.version,
            )
        )
    publication_gate: PublicationGate
    if comparison_status in {"serious_discrepancy", "source_identity_drift"}:
        publication_gate = "quarantined"
    elif comparison_status == "unavailable":
        publication_gate = "blocked" if comparison_required else "incomplete"
    else:
        publication_gate = "eligible"
    quality_eligible = result.quality_report.quality_status in {"passed", "warning"}
    canonical_eligible = quality_eligible and publication_gate in {"eligible", "incomplete"}
    if not quality_eligible and publication_gate != "quarantined":
        publication_gate = "blocked"
        canonical_eligible = False
    effective_consumer_eligibility = consumer_eligibility
    if effective_consumer_eligibility is None:
        effective_consumer_eligibility = (
            optional_consumer_eligibility()
            if result.bar_manifest.provider.startswith("akshare")
            else canonical_consumer_eligibility()
        )
    publication_state = publication_state_for(
        publication_gate,
        quality_passed=result.quality_report.quality_status == "passed",
        comparison_status=comparison_status,
        consumer_eligibility=effective_consumer_eligibility,
    )
    return PublicationBundle(
        task_id=task_id,
        attempt_id=attempt_id,
        dataset_id=result.bar_manifest.dataset_id,
        dataset_version=result.bar_manifest.version,
        provider=result.bar_manifest.provider,
        endpoint=endpoint,
        artifacts=tuple(artifacts),
        proposed_manifests=(
            result.raw_manifest,
            result.instrument_manifest,
            result.bar_manifest,
        ),
        created_at=created_at,
        producer_environment=ProducerEnvironment.current(environment_id),
        canonical_eligible=canonical_eligible,
        quarantined=publication_gate == "quarantined",
        discrepancy_report_uri=discrepancy_report_uri,
        discrepancy_report_sha256=discrepancy_report_sha256,
        comparison_status=comparison_status,
        comparison_required=comparison_required,
        publication_gate=publication_gate,
        publication_state=publication_state,
        consumer_eligibility=effective_consumer_eligibility,
        primary_source_identity=primary_source_identity,
        comparison_source_identity=comparison_source_identity,
        canonical_source_provider=result.bar_manifest.provider,
        canonical_source_endpoint=endpoint,
        canonical_selection_reason=canonical_selection_reason,
        selection_rule_id=selection_rule_id,
    )


def write_publication_bundle(storage: LocalFileSystemStorage, bundle: PublicationBundle) -> Path:
    # Catalog evidence is durable. It deliberately does not live under staging,
    # which validation and worker cleanup are allowed to remove.
    path = storage.data_root / "catalog" / "publication_bundles" / f"{bundle.bundle_id}.json"
    payload = (
        json.dumps(bundle.model_dump(mode="json", exclude_computed_fields=True), indent=2) + "\n"
    ).encode()
    atomic_write_immutable_bytes(
        path,
        payload,
        artifact_kind=f"publication bundle {bundle.bundle_id}",
        validate=lambda temporary: PublicationBundle.model_validate_json(
            temporary.read_text(encoding="utf-8")
        ),
    )
    return path


class CatalogPublisher:
    """The sole serialized writer for manifest files and DuckDB catalog state."""

    def __init__(self, data_root: str | Path) -> None:
        self.storage = LocalFileSystemStorage(data_root)
        self.catalog_path = self.storage.data_root / "catalog" / "quant.duckdb"
        self.lock_path = self.storage.data_root / "catalog" / ".publisher.lock"

    @contextmanager
    def _locked(self):
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        with self.lock_path.open("a+b") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)

    def _verify_artifacts(self, bundle: PublicationBundle) -> CNProviderDiscrepancyReport | None:
        proposed_from_artifacts: list[DatasetManifest] = []
        discrepancy_report: CNProviderDiscrepancyReport | None = None
        for artifact in bundle.artifacts:
            path = self.storage.resolve_uri(artifact.uri)
            if not path.is_file():
                raise FileNotFoundError(path)
            if path.stat().st_size != artifact.size_bytes or _sha256(path) != artifact.sha256:
                raise ValueError(f"Publication artifact hash conflict: {artifact.uri}")
            if artifact.kind == "proposed_manifest":
                proposed_from_artifacts.append(
                    DatasetManifest.model_validate_json(path.read_text(encoding="utf-8"))
                )
            elif artifact.kind == "discrepancy":
                discrepancy_report = CNProviderDiscrepancyReport.model_validate_json(
                    path.read_text(encoding="utf-8")
                )
        proposed_keyed = {(item.dataset_id, item.version): item for item in proposed_from_artifacts}
        expected_keyed = {
            (item.dataset_id, item.version): item for item in bundle.proposed_manifests
        }
        if (
            len(proposed_from_artifacts) != len(bundle.proposed_manifests)
            or proposed_keyed != expected_keyed
        ):
            raise ValueError("Proposed manifest content conflict")
        artifact_hashes = {item.uri: item.sha256 for item in bundle.artifacts}
        for manifest in bundle.proposed_manifests:
            if artifact_hashes.get(manifest.storage_uri) != manifest.sha256:
                raise ValueError(
                    f"Manifest payload hash is not present in bundle: {manifest.storage_uri}"
                )
        primary_identity = bundle.primary_source_identity
        if bundle.schema_version in {"1.2.0", "1.3.0"}:
            if primary_identity is None:
                raise ValueError("Publication primary identity is missing")
            primary_raw = next(item for item in bundle.artifacts if item.kind == "raw")
            if (
                primary_raw.uri,
                primary_raw.sha256,
            ) != (primary_identity.raw_uri, primary_identity.raw_sha256):
                raise ValueError("Publication primary Raw evidence conflicts")
            bar_manifest = next(
                (
                    item
                    for item in bundle.proposed_manifests
                    if item.layer is DatasetLayer.NORMALIZED
                    and item.dataset_id == bundle.dataset_id
                ),
                None,
            )
            if bar_manifest is None:
                raise ValueError("Publication bar Manifest is missing")
            primary_fields = (
                bar_manifest.provider,
                bar_manifest.provider_version,
                bar_manifest.adapter_version,
                bar_manifest.native_schema_version,
                bar_manifest.raw_storage_uri,
            )
            expected_primary_fields = (
                primary_identity.provider,
                primary_identity.provider_version,
                primary_identity.adapter_version,
                primary_identity.native_schema_version,
                primary_identity.raw_uri,
            )
            if primary_fields != expected_primary_fields:
                raise ValueError("primary Raw identity conflicts with Manifest lineage")
            if (
                primary_identity.provider,
                primary_identity.endpoint,
            ) != (bundle.provider, bundle.endpoint):
                raise ValueError("primary identity conflicts with canonical route")
            self._verify_bar_lineage(bar_manifest, primary_identity)
        if bundle.discrepancy_report_uri is not None:
            if discrepancy_report is None:
                raise ValueError("Publication discrepancy evidence is missing")
            expected_status = discrepancy_report.status
            if expected_status == "quarantined":
                expected_status = (
                    "source_identity_drift"
                    if any(
                        "endpoint_source_drift" in item.differing_fields
                        for item in discrepancy_report.records
                    )
                    else "serious_discrepancy"
                )
            if bundle.comparison_status != expected_status:
                raise ValueError("Publication gate conflicts with discrepancy report")
            if discrepancy_report.comparison_required != bundle.comparison_required:
                raise ValueError("comparison_required conflicts with discrepancy report")
            if discrepancy_report.primary_identity != bundle.primary_source_identity:
                raise ValueError("Discrepancy primary identity conflicts with Bundle")
            if discrepancy_report.comparison_identity != bundle.comparison_source_identity:
                raise ValueError("Discrepancy comparison identity conflicts with Bundle")
            report_primary = discrepancy_report.primary_identity
            if report_primary is not None and (
                report_primary.provider != bundle.canonical_source_provider
                or report_primary.endpoint != bundle.canonical_source_endpoint
            ):
                raise ValueError("Discrepancy primary identity conflicts with canonical source")
            report_comparison = discrepancy_report.comparison_identity
            if report_comparison is not None:
                comparison_raw = next(
                    (item for item in bundle.artifacts if item.kind == "comparison_raw"),
                    None,
                )
                if comparison_raw is None or (
                    comparison_raw.uri,
                    comparison_raw.sha256,
                ) != (report_comparison.raw_uri, report_comparison.raw_sha256):
                    raise ValueError("comparison Raw evidence conflicts with report identity")
                if (
                    report_comparison.provider,
                    report_comparison.endpoint,
                ) == (
                    bundle.canonical_source_provider,
                    bundle.canonical_source_endpoint,
                ):
                    raise ValueError("comparison source cannot overwrite canonical source")
                if report_comparison.transport_identity is not None:
                    comparison_path = self.storage.resolve_uri(report_comparison.raw_uri)
                    raw_payload = json.loads(comparison_path.read_text(encoding="utf-8"))
                    raw_transport = ProviderTransportIdentity.model_validate(
                        raw_payload.get("transport_audit")
                    )
                    if raw_transport != report_comparison.transport_identity:
                        raise ValueError(
                            "comparison Raw transport evidence conflicts with source identity"
                        )
            for record in discrepancy_report.records:
                if (
                    report_primary is None
                    or report_comparison is None
                    or (
                        record.primary_provider != report_primary.provider
                        or record.comparison_provider != report_comparison.provider
                        or record.primary_raw_uri != report_primary.raw_uri
                        or record.comparison_raw_uri != report_comparison.raw_uri
                    )
                ):
                    raise ValueError("discrepancy record source identity conflicts")
        return discrepancy_report

    def _verify_bar_lineage(
        self, manifest: DatasetManifest, identity: ProviderSourceIdentity
    ) -> None:
        import duckdb

        path = self.storage.resolve_uri(manifest.storage_uri)
        connection = duckdb.connect()
        try:
            rows = connection.execute(
                """SELECT DISTINCT lineage.provider, lineage.endpoint,
                          lineage.provider_version, lineage.adapter_version,
                          lineage.native_schema_version, lineage.raw_uri,
                          lineage.raw_sha256
                   FROM read_parquet(?)""",
                [str(path)],
            ).fetchall()
        finally:
            connection.close()
        expected = (
            identity.provider,
            identity.endpoint,
            identity.provider_version,
            identity.adapter_version,
            identity.native_schema_version,
            identity.raw_uri,
            identity.raw_sha256,
        )
        if rows != [expected]:
            raise ValueError("normalized bar lineage conflicts with primary Raw identity")

    def ensure_catalog(self) -> None:
        """Create the catalog schema under the publisher lock."""

        with self._locked():
            catalog = DuckDBCatalog(self.catalog_path)
            catalog.close()

    def _bundle_path(self, bundle: PublicationBundle) -> Path:
        return self.storage.data_root / "catalog/publication_bundles" / f"{bundle.bundle_id}.json"

    def _logical_uri(self, path: Path) -> str:
        return f"quant-data://{path.relative_to(self.storage.data_root).as_posix()}"

    def _decision_path(self, bundle: PublicationBundle) -> Path:
        key = hashlib.sha256(f"{bundle.dataset_id}@{bundle.dataset_version}".encode()).hexdigest()[
            :32
        ]
        return (
            self.storage.data_root
            / "catalog/publication_decisions"
            / f"{bundle.dataset_id}@{key}.json"
        )

    def _enforce_version_bundle_decision(self, bundle: PublicationBundle) -> None:
        bundles_root = self.storage.data_root / "catalog/publication_bundles"
        if not bundles_root.is_dir():
            return
        for path in bundles_root.glob("*.json"):
            prior = load_publication_bundle(path)
            if (prior.dataset_id, prior.dataset_version) != (
                bundle.dataset_id,
                bundle.dataset_version,
            ):
                continue
            if prior.decision_fingerprint != bundle.decision_fingerprint:
                raise ValueError(
                    "dataset version must change for a different persisted publication decision"
                )

    def _enforce_version_selection_identity(self, bundle: PublicationBundle) -> None:
        receipts_root = self.storage.data_root / "catalog/publication_receipts"
        if not receipts_root.is_dir():
            return
        expected = (
            bundle.canonical_source_provider,
            bundle.canonical_source_endpoint,
            bundle.canonical_selection_reason,
            bundle.selection_rule_id,
        )
        for path in receipts_root.glob("*.json"):
            receipt = PublicationReceipt.model_validate_json(path.read_text(encoding="utf-8"))
            if (
                receipt.dataset_id,
                receipt.dataset_version,
            ) != (bundle.dataset_id, bundle.dataset_version):
                continue
            prior = (
                receipt.canonical_source_provider,
                receipt.canonical_source_endpoint,
                receipt.canonical_selection_reason,
                receipt.selection_rule_id,
            )
            if prior != expected:
                raise ValueError(
                    "dataset version must change when publication rule or source changes"
                )

    def replay_receipt(
        self, receipt_or_path: PublicationReceipt | str | Path
    ) -> ReceiptReplayVerification:
        receipt = (
            receipt_or_path
            if isinstance(receipt_or_path, PublicationReceipt)
            else load_publication_receipt(receipt_or_path)
        )
        if receipt.evidence_status == "legacy_unknown":
            return ReceiptReplayVerification(
                receipt_id=receipt.receipt_id,
                evidence_status="legacy_unknown",
                verified=False,
            )
        assert receipt.bundle_uri is not None
        assert receipt.bundle_sha256 is not None
        assert receipt.decision_uri is not None
        assert receipt.decision_sha256 is not None
        bundle_path = self.storage.resolve_uri(receipt.bundle_uri)
        if not bundle_path.is_file() or _sha256(bundle_path) != receipt.bundle_sha256:
            raise ValueError("Receipt Bundle hash verification failed")
        bundle = load_publication_bundle(bundle_path)
        if bundle.bundle_id != receipt.bundle_id:
            raise ValueError("Receipt Bundle identity verification failed")
        if (
            receipt.primary_source_identity != bundle.primary_source_identity
            or receipt.comparison_source_identity != bundle.comparison_source_identity
        ):
            raise ValueError("Receipt source identity conflicts with Bundle")
        discrepancy = self._verify_artifacts(bundle)

        decision_path = self.storage.resolve_uri(receipt.decision_uri)
        if not decision_path.is_file() or _sha256(decision_path) != receipt.decision_sha256:
            raise ValueError("Receipt decision hash verification failed")
        decision = load_publication_decision(decision_path)
        receipt_decision_fields = {
            "bundle_id": receipt.bundle_id,
            "bundle_uri": receipt.bundle_uri,
            "bundle_sha256": receipt.bundle_sha256,
            "dataset_id": receipt.dataset_id,
            "dataset_version": receipt.dataset_version,
            "comparison_status": receipt.comparison_status,
            "comparison_required": receipt.comparison_required,
            "publication_gate": receipt.publication_gate,
            "publication_state": receipt.publication_state,
            "consumer_eligibility": receipt.consumer_eligibility,
            "discrepancy_report_uri": receipt.discrepancy_report_uri,
            "discrepancy_report_sha256": receipt.discrepancy_report_sha256,
            "primary_source_identity": receipt.primary_source_identity,
            "comparison_source_identity": receipt.comparison_source_identity,
            "canonical_source_provider": receipt.canonical_source_provider,
            "canonical_source_endpoint": receipt.canonical_source_endpoint,
            "canonical_selection_reason": receipt.canonical_selection_reason,
            "selection_rule_id": receipt.selection_rule_id,
            "manifest_uris": receipt.manifest_uris,
            "worker_import_evidence": receipt.worker_import_evidence,
        }
        if decision.fingerprint != receipt.publication_evidence_fingerprint or any(
            getattr(decision, key) != value for key, value in receipt_decision_fields.items()
        ):
            raise ValueError("Receipt decision content verification failed")

        worker_import_verified = False
        if receipt.worker_import_evidence is not None:
            worker_ref = receipt.worker_import_evidence
            worker_path = self.storage.resolve_uri(worker_ref.receipt_uri)
            if not worker_path.is_file() or _sha256(worker_path) != worker_ref.receipt_sha256:
                raise ValueError("Receipt worker import evidence hash verification failed")
            worker_payload = json.loads(worker_path.read_text(encoding="utf-8"))
            expected_worker_fields = {
                "bundle_id": worker_ref.worker_bundle_id,
                "envelope_sha256": worker_ref.envelope_sha256,
                "archive_sha256": worker_ref.archive_sha256,
                "source_commit": worker_ref.source_commit,
                "worker_environment_sha256": worker_ref.worker_environment_sha256,
                "provider_contract_sha256": worker_ref.provider_contract_sha256,
            }
            if any(
                worker_payload.get(key) != value
                for key, value in expected_worker_fields.items()
            ):
                raise ValueError("Receipt worker import evidence content verification failed")
            worker_import_verified = True

        pointer_path = self.storage.data_root / "catalog/current-manifest-set.json"
        if not pointer_path.is_file():
            raise ValueError("Receipt manifest pointer is missing")
        pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
        pointer_uris = {f"quant-data://{relative}" for relative in pointer.get("manifests", [])}
        if not set(receipt.manifest_uris).issubset(pointer_uris):
            raise ValueError("Receipt Manifests are absent from current pointer")

        published_manifests: list[DatasetManifest] = []
        for uri in receipt.manifest_uris:
            path = self.storage.resolve_uri(uri)
            if not path.is_file():
                raise ValueError("Receipt Manifest is missing")
            published_manifests.append(
                DatasetManifest.model_validate_json(path.read_text(encoding="utf-8"))
            )
        expected = {
            (item.dataset_id, item.version): manifest_sha256(item) for item in published_manifests
        }
        catalog = DuckDBCatalog(self.catalog_path)
        try:
            actual = {
                (row[0], row[1]): row[2]
                for row in catalog.connection.execute(
                    "SELECT dataset_id, version, manifest_sha256 FROM datasets"
                ).fetchall()
                if (row[0], row[1]) in expected
            }
        finally:
            catalog.close()
        if actual != expected:
            raise ValueError("Receipt DuckDB projection verification failed")
        return ReceiptReplayVerification(
            receipt_id=receipt.receipt_id,
            evidence_status="verified",
            verified=True,
            bundle_verified=True,
            manifests_verified=True,
            pointer_verified=True,
            duckdb_verified=True,
            discrepancy_verified=(
                discrepancy is not None or bundle.comparison_status == "not_required"
            ),
            primary_raw_verified=True,
            comparison_raw_verified=(
                bundle.comparison_status == "not_required"
                or bundle.comparison_source_identity is not None
            ),
            worker_import_verified=worker_import_verified,
        )

    def publish(
        self,
        bundle: PublicationBundle,
        *,
        worker_import_evidence: WorkerImportEvidenceRef | None = None,
    ) -> PublicationReceipt:
        bundle = PublicationBundle.model_validate(
            bundle.model_dump(mode="python", exclude_computed_fields=True)
        )
        with self._locked():
            self._enforce_version_bundle_decision(bundle)
            bundle_path = write_publication_bundle(self.storage, bundle)
            if (
                bundle.publication_state is None
                or not bundle.publication_state.catalog_published
                or bundle.publication_gate in {"blocked", "quarantined"}
            ):
                raise ValueError("Bundle is not eligible for catalog publication")
            self._enforce_version_selection_identity(bundle)
            self._verify_artifacts(bundle)
            receipt_path = (
                self.storage.data_root
                / "catalog"
                / "publication_receipts"
                / f"{bundle.bundle_id}.json"
            )
            prior_receipt = None
            if receipt_path.exists():
                prior_receipt = PublicationReceipt.model_validate_json(
                    receipt_path.read_text(encoding="utf-8")
                )
            pointer_path = self.storage.data_root / "catalog/current-manifest-set.json"
            pointer_existed = pointer_path.exists()
            if pointer_existed:
                pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
                current_relatives = list(pointer["manifests"])
            else:
                legacy_root = self.storage.data_root / "catalog/manifests"
                current_relatives = (
                    [
                        path.relative_to(self.storage.data_root).as_posix()
                        for path in sorted(legacy_root.glob("*.json"))
                    ]
                    if legacy_root.is_dir()
                    else []
                )
            current_by_key: dict[tuple[str, str], tuple[DatasetManifest, str]] = {}
            for relative in current_relatives:
                path = self.storage.resolve_uri(f"quant-data://{relative}")
                current = DatasetManifest.model_validate_json(path.read_text(encoding="utf-8"))
                current_by_key[(current.dataset_id, current.version)] = (current, relative)
            authoritative_manifests = {key: value[0] for key, value in current_by_key.items()}

            generation_root = self.storage.data_root / "catalog/manifest_sets" / bundle.bundle_id
            published_relatives = list(current_relatives)
            for manifest in bundle.proposed_manifests:
                key = (manifest.dataset_id, manifest.version)
                if key in current_by_key:
                    existing, _ = current_by_key[key]
                    if existing != manifest:
                        raise ValueError(
                            f"Immutable manifest conflict: {manifest.dataset_id}@{manifest.version}"
                        )
                    continue
                path = generation_root / f"{manifest.dataset_id}@{manifest.version}.json"
                payload = (json.dumps(manifest.model_dump(mode="json"), indent=2) + "\n").encode()
                atomic_write_immutable_bytes(
                    path,
                    payload,
                    artifact_kind=(f"manifest generation {manifest.dataset_id}@{manifest.version}"),
                    validate=lambda temporary: DatasetManifest.model_validate_json(
                        temporary.read_text(encoding="utf-8")
                    ),
                )
                relative = path.relative_to(self.storage.data_root).as_posix()
                published_relatives.append(relative)
                current_by_key[key] = (manifest, relative)

            catalog = DuckDBCatalog(self.catalog_path)
            try:
                catalog.register_many_atomic(
                    list(bundle.proposed_manifests),
                    authoritative_manifests=authoritative_manifests,
                )
            finally:
                catalog.close()
            _atomic_write(
                pointer_path,
                (
                    json.dumps(
                        {
                            "schema_version": "1.0.0",
                            "bundle_id": bundle.bundle_id,
                            "manifests": published_relatives,
                        },
                        indent=2,
                    )
                    + "\n"
                ).encode(),
            )
            manifest_uris = tuple(
                f"quant-data://{current_by_key[(manifest.dataset_id, manifest.version)][1]}"
                for manifest in bundle.proposed_manifests
            )
            bundle_uri = self._logical_uri(bundle_path)
            bundle_sha256 = _sha256(bundle_path)
            decision_path = self._decision_path(bundle)
            if decision_path.exists():
                decision = load_publication_decision(decision_path)
            else:
                decision = PublicationDecision(
                    bundle_id=bundle.bundle_id,
                    bundle_uri=bundle_uri,
                    bundle_sha256=bundle_sha256,
                    dataset_id=bundle.dataset_id,
                    dataset_version=bundle.dataset_version,
                    comparison_status=bundle.comparison_status,
                    comparison_required=bundle.comparison_required,
                    publication_gate=bundle.publication_gate,
                    publication_state=bundle.publication_state,
                    consumer_eligibility=bundle.consumer_eligibility,
                    discrepancy_report_uri=bundle.discrepancy_report_uri,
                    discrepancy_report_sha256=bundle.discrepancy_report_sha256,
                    primary_source_identity=bundle.primary_source_identity,
                    comparison_source_identity=bundle.comparison_source_identity,
                    canonical_source_provider=bundle.canonical_source_provider,
                    canonical_source_endpoint=bundle.canonical_source_endpoint,
                    canonical_selection_reason=bundle.canonical_selection_reason,
                    selection_rule_id=bundle.selection_rule_id,
                    manifest_uris=manifest_uris,
                    worker_import_evidence=worker_import_evidence,
                    decided_at=datetime.now(UTC),
                )
                atomic_write_immutable_bytes(
                    decision_path,
                    (
                        json.dumps(
                            decision.model_dump(mode="json", exclude_computed_fields=True),
                            indent=2,
                        )
                        + "\n"
                    ).encode(),
                    artifact_kind=(
                        f"publication decision {bundle.dataset_id}@{bundle.dataset_version}"
                    ),
                    validate=lambda temporary: PublicationDecision.model_validate_json(
                        temporary.read_text(encoding="utf-8")
                    ),
                )
            expected_decision = {
                "bundle_id": bundle.bundle_id,
                "bundle_uri": bundle_uri,
                "bundle_sha256": bundle_sha256,
                "dataset_id": bundle.dataset_id,
                "dataset_version": bundle.dataset_version,
                "comparison_status": bundle.comparison_status,
                "comparison_required": bundle.comparison_required,
                "publication_gate": bundle.publication_gate,
                "publication_state": bundle.publication_state,
                "consumer_eligibility": bundle.consumer_eligibility,
                "discrepancy_report_uri": bundle.discrepancy_report_uri,
                "discrepancy_report_sha256": bundle.discrepancy_report_sha256,
                "primary_source_identity": bundle.primary_source_identity,
                "comparison_source_identity": bundle.comparison_source_identity,
                "canonical_source_provider": bundle.canonical_source_provider,
                "canonical_source_endpoint": bundle.canonical_source_endpoint,
                "canonical_selection_reason": bundle.canonical_selection_reason,
                "selection_rule_id": bundle.selection_rule_id,
                "manifest_uris": manifest_uris,
                "worker_import_evidence": worker_import_evidence,
            }
            if any(getattr(decision, key) != value for key, value in expected_decision.items()):
                raise ValueError("dataset version has a conflicting persisted decision")
            if prior_receipt is not None:
                verification = self.replay_receipt(prior_receipt)
                if not verification.verified:
                    raise ValueError("legacy receipt cannot verify current publication")
                return prior_receipt.model_copy(update={"idempotent_replay": True})
            decision_uri = self._logical_uri(decision_path)
            decision_sha256 = _sha256(decision_path)
            receipt = PublicationReceipt(
                receipt_id=f"receipt-{bundle.bundle_id.removeprefix('bundle-')}",
                bundle_id=bundle.bundle_id,
                bundle_uri=bundle_uri,
                bundle_sha256=bundle_sha256,
                decision_uri=decision_uri,
                decision_sha256=decision_sha256,
                published_at=datetime.now(UTC),
                manifest_uris=manifest_uris,
                dataset_id=bundle.dataset_id,
                dataset_version=bundle.dataset_version,
                comparison_status=bundle.comparison_status,
                comparison_required=bundle.comparison_required,
                publication_gate=bundle.publication_gate,
                publication_state=bundle.publication_state,
                consumer_eligibility=bundle.consumer_eligibility,
                discrepancy_report_uri=bundle.discrepancy_report_uri,
                discrepancy_report_sha256=bundle.discrepancy_report_sha256,
                primary_source_identity=bundle.primary_source_identity,
                comparison_source_identity=bundle.comparison_source_identity,
                canonical_source_provider=bundle.canonical_source_provider,
                canonical_source_endpoint=bundle.canonical_source_endpoint,
                canonical_selection_reason=bundle.canonical_selection_reason,
                selection_rule_id=bundle.selection_rule_id,
                publication_evidence_fingerprint=decision.fingerprint,
                worker_import_evidence=worker_import_evidence,
            )
            atomic_write_immutable_bytes(
                receipt_path,
                (json.dumps(receipt.model_dump(mode="json"), indent=2) + "\n").encode(),
                artifact_kind=f"publication receipt {bundle.bundle_id}",
                validate=lambda temporary: PublicationReceipt.model_validate_json(
                    temporary.read_text(encoding="utf-8")
                ),
            )
            return receipt


def load_publication_bundle(path: str | Path) -> PublicationBundle:
    return PublicationBundle.model_validate_json(Path(path).read_text(encoding="utf-8"))


def load_publication_receipt(path: str | Path) -> PublicationReceipt:
    return PublicationReceipt.model_validate_json(Path(path).read_text(encoding="utf-8"))


def load_publication_decision(path: str | Path) -> PublicationDecision:
    return PublicationDecision.model_validate_json(Path(path).read_text(encoding="utf-8"))


def find_publication_receipt_for_manifest(
    data_root: str | Path,
    manifest: DatasetManifest,
    *,
    manifest_relative: str | None = None,
) -> PublicationReceipt | None:
    """Resolve one verified version decision without treating legacy data as verified."""

    root = Path(data_root).expanduser().resolve()
    receipts_root = root / "catalog/publication_receipts"
    if not receipts_root.is_dir():
        return None
    if manifest_relative is not None:
        bundle_id = Path(manifest_relative).parent.name
        if bundle_id.startswith("bundle-"):
            direct_path = receipts_root / f"{bundle_id}.json"
            try:
                direct = load_publication_receipt(direct_path)
            except (FileNotFoundError, PermissionError):
                direct = None
            if (
                direct is not None
                and direct.evidence_status == "verified"
                and (
                    (direct.dataset_id, direct.dataset_version)
                    == (manifest.dataset_id, manifest.version)
                    or f"quant-data://{manifest_relative}" in direct.manifest_uris
                )
            ):
                return direct
    verified: list[PublicationReceipt] = []
    for path in receipts_root.glob("*.json"):
        try:
            receipt = load_publication_receipt(path)
        except PermissionError:
            # One unreadable historical receipt must not make every unrelated
            # canonical dataset unavailable.  A dataset whose own receipt is
            # unreadable still fails closed because it produces no match.
            continue
        if receipt.evidence_status != "verified":
            continue
        verified.append(receipt)

    matches = [
        receipt
        for receipt in verified
        if (receipt.dataset_id, receipt.dataset_version)
        == (manifest.dataset_id, manifest.version)
    ]
    if not matches:
        for receipt in verified:
            included = False
            for uri in receipt.manifest_uris:
                candidate = root / uri.removeprefix("quant-data://")
                if not candidate.is_file():
                    continue
                published = DatasetManifest.model_validate_json(
                    candidate.read_text(encoding="utf-8")
                )
                if (published.dataset_id, published.version) == (
                    manifest.dataset_id,
                    manifest.version,
                ):
                    included = True
                    break
            if included:
                matches.append(receipt)
    if not matches:
        return None
    fingerprints = {item.publication_evidence_fingerprint for item in matches}
    if len(fingerprints) != 1:
        raise ValueError("dataset version has multiple publication decisions")
    return max(matches, key=lambda item: item.published_at)
