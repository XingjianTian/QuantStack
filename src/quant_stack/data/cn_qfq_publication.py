from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path

from .models import DatasetKind, DatasetLayer, DatasetManifest, DatasetQualityMetadata
from .publication import (
    CatalogPublisher,
    ProducerEnvironment,
    PublicationArtifact,
    PublicationBundle,
    canonical_consumer_eligibility,
    publication_state_for,
)
from .storage.atomic import atomic_write_immutable_bytes


def _digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def publish_cn_qfq_session_dataset(
    *,
    data_root: Path,
    dataset_version: str,
    descriptor_path: Path,
    session_manifest_path: Path,
    instruments_source_path: Path,
    row_count: int,
    instrument_count: int,
    first_date: date,
    last_date: date,
    environment_id: str = "cn-qfq-session-publisher",
) -> tuple[object, dict[str, object]]:
    """Publish a lightweight qfq descriptor without hashing historical/session Parquets."""

    data_root = data_root.expanduser().resolve()
    descriptor_path = descriptor_path.expanduser().resolve()
    target_root = data_root / "normalized/bars_1d/market=CN" / f"version={dataset_version}"
    if descriptor_path.parent != target_root:
        raise ValueError("CN qfq descriptor must be built inside its immutable Catalog version")
    evidence_root = target_root / "evidence"
    quality_path = evidence_root / "quality.json"
    source_path = evidence_root / "session-manifest.json"
    instruments_path = evidence_root / "instruments.json"
    atomic_write_immutable_bytes(
        source_path,
        session_manifest_path.read_bytes(),
        artifact_kind="CN qfq session source manifest",
    )
    instruments = {
        "schema_version": "quantstack.cn-instruments-reference.v1",
        "instrument_count": instrument_count,
        "source_file": instruments_source_path.name,
        "source_size_bytes": instruments_source_path.stat().st_size,
    }
    atomic_write_immutable_bytes(
        instruments_path,
        (json.dumps(instruments, indent=2, sort_keys=True) + "\n").encode(),
        artifact_kind="CN instruments reference",
    )
    quality = {
        "schema_version": "1.0.0",
        "status": "passed",
        "checks": [
            "session_parquet_readback",
            "instrument_date_unique",
            "ohlcv_shape",
            "factor_ledger_continuity",
        ],
        "warnings": [],
        "first_date": first_date.isoformat(),
        "last_date": last_date.isoformat(),
        "history_rewritten": False,
    }
    atomic_write_immutable_bytes(
        quality_path,
        (json.dumps(quality, indent=2, sort_keys=True) + "\n").encode(),
        artifact_kind="CN qfq session quality evidence",
    )
    proposed_root = data_root / "staging/proposed_manifests"
    bars_proposed = proposed_root / f"cn_equity_daily@{dataset_version}.json"
    created_at = datetime.now(UTC)
    bar_manifest = DatasetManifest(
        dataset_id="cn_equity_daily",
        version=dataset_version,
        layer=DatasetLayer.NORMALIZED,
        kind=DatasetKind.MARKET_BARS,
        market="CN",
        provider="baostock",
        provider_version="0.8.9",
        adapter_version="phase-2.17",
        native_schema_version="daily-session-parquet-v1",
        frequency="1d",
        schema_ref="quant-stack://schemas/cn-equity-daily-qfq-v1",
        storage_uri=f"quant-data://{descriptor_path.relative_to(data_root).as_posix()}",
        created_at=created_at,
        available_at_policy="after session readback and factor delta acceptance",
        revision_policy="immutable",
        row_count=row_count,
        sha256=_digest(descriptor_path),
        quality_status="passed",
        quality=DatasetQualityMetadata(
            status="passed",
            checks=quality["checks"],
            warnings=[],
            report_uri=f"quant-data://{quality_path.relative_to(data_root).as_posix()}",
        ),
        license_scope="research",
        source_url="http://baostock.com/",
        raw_storage_uri=f"quant-data://{source_path.relative_to(data_root).as_posix()}",
        job_id=f"cn-qfq-session-{dataset_version}",
    )
    atomic_write_immutable_bytes(
        bars_proposed,
        (json.dumps(bar_manifest.model_dump(mode="json"), indent=2) + "\n").encode(),
        artifact_kind="CN qfq proposed manifest",
    )

    def artifact(
        path: Path, kind: str, *, dataset_id: str = "cn_equity_daily"
    ) -> PublicationArtifact:
        return PublicationArtifact(
            uri=f"quant-data://{path.relative_to(data_root).as_posix()}",
            kind=kind,
            size_bytes=path.stat().st_size,
            sha256=_digest(path),
            dataset_id=dataset_id,
            dataset_version=dataset_version,
        )

    policy = canonical_consumer_eligibility()
    bundle = PublicationBundle(
        schema_version="1.1.0",
        task_id=f"cn-qfq-session-{dataset_version}",
        attempt_id="attempt-1",
        dataset_id="cn_equity_daily",
        dataset_version=dataset_version,
        provider="baostock",
        endpoint="query_history_k_data_plus/query_adjust_factor",
        artifacts=(
            artifact(source_path, "raw"),
            artifact(instruments_path, "instruments"),
            artifact(descriptor_path, "bars"),
            artifact(quality_path, "quality"),
            artifact(bars_proposed, "proposed_manifest"),
        ),
        proposed_manifests=(bar_manifest,),
        created_at=created_at,
        producer_environment=ProducerEnvironment.current(environment_id),
        canonical_eligible=True,
        comparison_status="not_required",
        comparison_required=False,
        publication_gate="eligible",
        publication_state=publication_state_for(
            "eligible",
            quality_passed=True,
            comparison_status="not_required",
            consumer_eligibility=policy,
        ),
        consumer_eligibility=policy,
        canonical_source_provider="baostock",
        canonical_source_endpoint="query_history_k_data_plus/query_adjust_factor",
        canonical_selection_reason="session readback and incremental qfq factor acceptance",
        selection_rule_id="cn-qfq-session-v1",
    )
    receipt = CatalogPublisher(data_root).publish(bundle)
    return receipt, {
        "dataset_version": dataset_version,
        "row_count": row_count,
        "instrument_count": instrument_count,
        "first_date": first_date.isoformat(),
        "last_date": last_date.isoformat(),
        "history_rewritten": False,
    }
