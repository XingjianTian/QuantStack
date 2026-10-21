from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

from ..models import DatasetKind, DatasetLayer, DatasetManifest, DatasetQualityMetadata
from ..providers.cn.live import BAOSTOCK_LIVE_RUNTIME_CONTRACT
from ..providers.cn.reconciliation import (
    CNProviderDiscrepancyReport,
    ComparisonFailure,
    EvidenceMode,
    ProviderSourceIdentity,
)
from ..publication import (
    ProducerEnvironment,
    PublicationArtifact,
    PublicationBundle,
    optional_consumer_eligibility,
    publication_state_for,
)
from ..storage import StagingFileSystemStorage
from ..storage.atomic import atomic_write_immutable_bytes
from ..worker_bundle import WorkerArchive, build_worker_envelope, create_worker_archive
from .models import BackfillShard, BackfillShardResult, BackfillUniverse


def _json_bytes(payload: object) -> bytes:
    return (json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n").encode()


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _write_immutable_json(path: Path, payload: object, *, artifact_kind: str) -> bytes:
    encoded = _json_bytes(payload)
    atomic_write_immutable_bytes(
        path,
        encoded,
        artifact_kind=artifact_kind,
        validate=lambda temporary: json.loads(temporary.read_text(encoding="utf-8")),
    )
    return encoded


def _artifact(
    *,
    root: Path,
    relative_path: Path,
    kind: str,
    dataset_id: str,
    dataset_version: str,
) -> PublicationArtifact:
    path = (root / relative_path).resolve()
    if root not in path.parents or not path.is_file():
        raise FileNotFoundError(path)
    payload = path.read_bytes()
    return PublicationArtifact(
        uri=f"quant-data://{relative_path.as_posix()}",
        kind=kind,
        size_bytes=len(payload),
        sha256=_sha256(payload),
        dataset_id=dataset_id,
        dataset_version=dataset_version,
    )


def build_backfill_worker_archive(
    *,
    data_root: Path,
    shard: BackfillShard,
    result: BackfillShardResult,
    universe: BackfillUniverse,
    source_commit: str,
    environment_id: str,
    provider_transport_audit: dict[str, object],
    hmac_key: bytes,
    output_path: Path,
    created_at: datetime | None = None,
) -> WorkerArchive:
    """Bridge one completed backfill shard into the existing signed worker protocol.

    The bridge deliberately creates a blocked, comparison-unavailable publication
    proposal. It never writes a Catalog pointer; NAS import remains staging-only.
    """

    if result.shard_id != shard.shard_id:
        raise ValueError("backfill result does not match the selected shard")
    if result.task_id == "" or result.attempts < 1:
        raise ValueError("backfill result must contain a completed task attempt")
    if result.status not in {"succeeded", "replayed"} or result.row_count <= 0:
        raise ValueError("worker bridge requires a non-empty succeeded backfill shard")
    if tuple(result.instrument_ids) != tuple(shard.instrument_ids):
        raise ValueError("backfill result instrument scope conflicts with the selected shard")
    if (result.start_date, result.end_date) != (shard.start_date, shard.end_date):
        raise ValueError("backfill result date range conflicts with the selected shard")
    if result.raw_uri is None or result.raw_sha256 is None:
        raise ValueError("backfill result is missing immutable Raw evidence")
    if result.parquet_uri is None or result.parquet_sha256 is None:
        raise ValueError("backfill result is missing normalized Parquet evidence")
    if result.quality_uri is None:
        raise ValueError("backfill result is missing quality evidence")

    root = data_root.expanduser().resolve()
    storage = StagingFileSystemStorage(root)
    raw_path = storage.resolve_uri(result.raw_uri)
    parquet_path = storage.resolve_uri(result.parquet_uri)
    quality_path = storage.resolve_uri(result.quality_uri)
    raw_payload = raw_path.read_bytes()
    parquet_payload = parquet_path.read_bytes()
    quality_payload = quality_path.read_bytes()
    if _sha256(raw_payload) != result.raw_sha256:
        raise ValueError("backfill Raw evidence hash conflicts with the result")
    if _sha256(parquet_payload) != result.parquet_sha256:
        raise ValueError("backfill Parquet evidence hash conflicts with the result")
    quality = json.loads(quality_payload)
    if not isinstance(quality, dict) or quality.get("status") not in {"passed", "warning"}:
        raise ValueError("worker bridge requires a passed or warning quality report")
    native = json.loads(raw_payload)
    if not isinstance(native, dict) or not isinstance(native.get("records"), list):
        raise ValueError("backfill Raw evidence must contain the native records list")

    completed_requests = provider_transport_audit.get("completed_requests")
    request_successes = provider_transport_audit.get("request_successful_source_fingerprints")
    if not isinstance(completed_requests, int) or completed_requests < len(shard.instrument_ids):
        raise ValueError("transport audit does not cover the selected shard request batch")
    if not isinstance(request_successes, list) or len(request_successes) != completed_requests:
        raise ValueError("transport audit request success history is incomplete")
    if any(value is None for value in request_successes[-len(shard.instrument_ids) :]):
        raise ValueError("transport audit does not prove the selected shard request batch")

    created = (created_at or datetime.now(UTC)).astimezone(UTC)
    bridge_relative = (
        Path("pilot/worker_bridge")
        / f"version={shard.dataset_version}"
        / f"shard={shard.shard_id}"
        / f"attempt={result.attempts}"
    )
    raw_relative = bridge_relative / "raw.json"
    instruments_relative = bridge_relative / "instruments.json"
    quality_relative = bridge_relative / "quality.json"
    discrepancy_relative = bridge_relative / "comparison-unavailable.json"
    manifest_relative = bridge_relative / "manifest.json"
    bundle_relative = bridge_relative / "publication-bundle.json"

    bridge_raw = {
        **native,
        "source_raw_uri": result.raw_uri,
        "source_raw_sha256": result.raw_sha256,
        "transport_audit": provider_transport_audit,
        "job": {
            "task_id": result.task_id,
            "shard_id": shard.shard_id,
            "symbols": list(shard.instrument_ids),
            "start_date": shard.start_date.isoformat(),
            "end_date": shard.end_date.isoformat(),
        },
    }
    raw_encoded = _write_immutable_json(
        root / raw_relative,
        bridge_raw,
        artifact_kind="backfill worker bridge Raw evidence",
    )
    _write_immutable_json(
        root / instruments_relative,
        {
            "universe_version": universe.version,
            "instruments": [
                item.model_dump(mode="json")
                for item in universe.instruments
                if item.instrument_id in shard.instrument_ids
            ],
        },
        artifact_kind="backfill worker bridge instrument evidence",
    )
    quality_encoded = _write_immutable_json(
        root / quality_relative,
        quality,
        artifact_kind="backfill worker bridge quality evidence",
    )

    quality_status = str(quality["status"])
    quality_metadata = DatasetQualityMetadata(
        status=quality_status,
        checks=[str(item) for item in quality.get("checks", [])],
        failed_checks=[str(item) for item in quality.get("anomalies", [])],
        warnings=(
            ["provider returned explicit empty or suspended observations"]
            if quality_status == "warning"
            else []
        ),
        report_uri=f"quant-data://{quality_relative.as_posix()}",
    )
    manifest = DatasetManifest(
        dataset_id=shard.dataset_id,
        version=shard.dataset_version,
        layer=DatasetLayer.NORMALIZED,
        kind=DatasetKind.MARKET_BARS,
        market="CN",
        provider=BAOSTOCK_LIVE_RUNTIME_CONTRACT.provider,
        provider_version=BAOSTOCK_LIVE_RUNTIME_CONTRACT.provider_version,
        adapter_version=BAOSTOCK_LIVE_RUNTIME_CONTRACT.adapter_version,
        native_schema_version=BAOSTOCK_LIVE_RUNTIME_CONTRACT.native_schema_version,
        frequency="1d",
        schema_ref="quant-stack.cn-equity-daily.backfill.v1",
        storage_uri=result.parquet_uri,
        created_at=created,
        available_at_policy="provider_retrieval_time_after_complete_batch",
        revision_policy="immutable",
        row_count=result.row_count,
        sha256=result.parquet_sha256,
        quality_status=quality_status,
        quality=quality_metadata,
        license_scope="controlled research evidence; verify BaoStock terms before production use",
        source_url="https://baostock.com",
        raw_storage_uri=f"quant-data://{raw_relative.as_posix()}",
        job_id=result.task_id,
        available_at=created,
    )
    manifest_encoded = _write_immutable_json(
        root / manifest_relative,
        manifest.model_dump(mode="json", exclude_computed_fields=True),
        artifact_kind="backfill worker bridge proposed manifest",
    )

    primary_identity = ProviderSourceIdentity(
        provider=BAOSTOCK_LIVE_RUNTIME_CONTRACT.provider,
        endpoint=BAOSTOCK_LIVE_RUNTIME_CONTRACT.endpoint,
        provider_version=BAOSTOCK_LIVE_RUNTIME_CONTRACT.provider_version,
        adapter_version=BAOSTOCK_LIVE_RUNTIME_CONTRACT.adapter_version,
        native_schema_version=BAOSTOCK_LIVE_RUNTIME_CONTRACT.native_schema_version,
        raw_uri=f"quant-data://{raw_relative.as_posix()}",
        raw_sha256=_sha256(raw_encoded),
    )
    comparison = CNProviderDiscrepancyReport(
        report_id=f"backfill-{shard.shard_id}-comparison-unavailable",
        generated_at=created,
        evidence_mode=EvidenceMode.FIXTURE,
        fixture_only=True,
        primary_raw_sha256=primary_identity.raw_sha256,
        primary_identity=primary_identity,
        status="unavailable",
        comparison_required=True,
        failure=ComparisonFailure(
            error_class="ComparisonUnavailable",
            stage="provider_fetch",
        ),
        canonical_selection_reason="阿里云单 Provider 受控回填，待后续独立来源复核",
    )
    comparison_encoded = _write_immutable_json(
        root / discrepancy_relative,
        comparison.model_dump(mode="json", exclude_computed_fields=True),
        artifact_kind="backfill worker bridge comparison evidence",
    )

    dataset_id = shard.dataset_id
    dataset_version = shard.dataset_version
    artifacts = (
        _artifact(
            root=root,
            relative_path=raw_relative,
            kind="raw",
            dataset_id=dataset_id,
            dataset_version=dataset_version,
        ),
        _artifact(
            root=root,
            relative_path=instruments_relative,
            kind="instruments",
            dataset_id=dataset_id,
            dataset_version=dataset_version,
        ),
        PublicationArtifact(
            uri=result.parquet_uri,
            kind="bars",
            size_bytes=len(parquet_payload),
            sha256=result.parquet_sha256,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
        ),
        PublicationArtifact(
            uri=f"quant-data://{quality_relative.as_posix()}",
            kind="quality",
            size_bytes=len(quality_encoded),
            sha256=_sha256(quality_encoded),
            dataset_id=dataset_id,
            dataset_version=dataset_version,
        ),
        PublicationArtifact(
            uri=f"quant-data://{discrepancy_relative.as_posix()}",
            kind="discrepancy",
            size_bytes=len(comparison_encoded),
            sha256=_sha256(comparison_encoded),
            dataset_id=dataset_id,
            dataset_version=dataset_version,
        ),
        PublicationArtifact(
            uri=f"quant-data://{manifest_relative.as_posix()}",
            kind="proposed_manifest",
            size_bytes=len(manifest_encoded),
            sha256=_sha256(manifest_encoded),
            dataset_id=dataset_id,
            dataset_version=dataset_version,
        ),
    )
    bundle = PublicationBundle(
        task_id=result.task_id,
        attempt_id=f"backfill-{result.task_id}-{result.attempts}",
        dataset_id=dataset_id,
        dataset_version=dataset_version,
        provider=BAOSTOCK_LIVE_RUNTIME_CONTRACT.provider,
        endpoint=BAOSTOCK_LIVE_RUNTIME_CONTRACT.endpoint,
        artifacts=artifacts,
        proposed_manifests=(manifest,),
        created_at=created,
        producer_environment=ProducerEnvironment.current(environment_id),
        canonical_eligible=False,
        quarantined=False,
        discrepancy_report_uri=f"quant-data://{discrepancy_relative.as_posix()}",
        discrepancy_report_sha256=_sha256(comparison_encoded),
        comparison_status="unavailable",
        comparison_required=True,
        publication_gate="blocked",
        publication_state=publication_state_for(
            "blocked",
            quality_passed=quality_status == "passed",
            comparison_status="unavailable",
            consumer_eligibility=optional_consumer_eligibility(),
        ),
        consumer_eligibility=optional_consumer_eligibility(),
        primary_source_identity=primary_identity,
        canonical_source_provider=BAOSTOCK_LIVE_RUNTIME_CONTRACT.provider,
        canonical_source_endpoint=BAOSTOCK_LIVE_RUNTIME_CONTRACT.endpoint,
        canonical_selection_reason="阿里云单 Provider 受控回填，待后续独立来源复核",
        selection_rule_id="backfill-worker-bridge-v1",
    )
    bundle_encoded = _write_immutable_json(
        root / bundle_relative,
        bundle.model_dump(mode="json", exclude_computed_fields=True),
        artifact_kind="backfill worker bridge publication bundle",
    )
    if PublicationBundle.model_validate_json(bundle_encoded) != bundle:
        raise ValueError("backfill worker bridge publication bundle readback mismatch")

    envelope = build_worker_envelope(
        data_root=root,
        publication_bundle_path=root / bundle_relative,
        source_commit=source_commit,
        environment_id=environment_id,
        provider_contract=BAOSTOCK_LIVE_RUNTIME_CONTRACT,
        provider_transport_audit=dict(provider_transport_audit),
        created_at=created,
    )
    return create_worker_archive(
        data_root=root,
        envelope=envelope,
        output_path=output_path,
        hmac_key=hmac_key,
    )
