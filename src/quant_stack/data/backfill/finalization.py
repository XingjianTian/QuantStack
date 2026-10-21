from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path

from ..models import DatasetKind, DatasetLayer, DatasetManifest, DatasetQualityMetadata
from ..publication import (
    CatalogPublisher,
    ConsumerEligibilityPolicy,
    ProducerEnvironment,
    PublicationArtifact,
    PublicationBundle,
    PublicationReceipt,
    publication_state_for,
)
from ..storage.atomic import atomic_write_immutable_bytes, atomic_write_immutable_file
from .campaign import BackfillCampaignPlan, BackfillCampaignProgress
from .models import BackfillUniverse

CANONICAL_DATASET_ID = "cn_equity_daily"
_BAR_COLUMNS = (
    "instrument_id",
    "symbol",
    "venue",
    "trading_date",
    "ts_event",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "currency",
    "provider",
    "provider_symbol",
    "adjustment",
    "volume_unit",
    "amount_unit",
    "calendar_basis",
    "retrieval_timestamp",
    "raw_uri",
    "raw_sha256",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_bytes(value: object) -> bytes:
    return (json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n").encode()


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _write_json(path: Path, value: object) -> None:
    atomic_write_immutable_bytes(
        path,
        _json_bytes(value),
        artifact_kind="backfill finalization JSON",
    )


def _artifact(
    data_root: Path,
    path: Path,
    kind: str,
    *,
    dataset_version: str,
) -> PublicationArtifact:
    return PublicationArtifact(
        uri=f"quant-data://{path.relative_to(data_root).as_posix()}",
        kind=kind,
        size_bytes=path.stat().st_size,
        sha256=_sha256(path),
        dataset_id=CANONICAL_DATASET_ID,
        dataset_version=dataset_version,
    )


def _load_contracts(
    plan_path: Path,
    universe_path: Path,
    progress_path: Path,
) -> tuple[BackfillCampaignPlan, BackfillUniverse, BackfillCampaignProgress]:
    plan = BackfillCampaignPlan.model_validate_json(plan_path.read_bytes())
    universe = BackfillUniverse.model_validate_json(universe_path.read_bytes())
    progress_payload = json.loads(progress_path.read_bytes())
    for shard in progress_payload.get("shards", ()):
        if shard.get("status") == "empty" and shard.get("row_count") is None:
            # Early campaign snapshots used null for a terminal empty result.  It is
            # losslessly equivalent to zero rows; null remains invalid elsewhere.
            shard["row_count"] = 0
    terminal = progress_payload.get("succeeded_shards", 0) + progress_payload.get(
        "empty_shards", 0
    )
    no_open_shards = not any(
        progress_payload.get(field, 0)
        for field in (
            "pending_shards",
            "running_shards",
            "retryable_shards",
            "failed_shards",
        )
    )
    if terminal == progress_payload.get("planned_shards") and no_open_shards:
        # Old snapshots retained the pre-authorization gate value.  The shard
        # states and counts are authoritative; the caller supplies authorization.
        progress_payload["finalizable"] = True
    progress = BackfillCampaignProgress.model_validate(progress_payload)
    if (
        progress.campaign_id,
        progress.dataset_version,
        progress.universe_version,
    ) != (plan.campaign_id, plan.dataset_version, plan.universe_version):
        raise ValueError("campaign progress is not bound to the requested plan")
    if universe.version != plan.universe_version:
        raise ValueError("universe version is not bound to the requested plan")
    if universe.content_sha256 != plan.universe_content_sha256:
        raise ValueError("universe content hash is not bound to the requested plan")
    terminal = progress.succeeded_shards + progress.empty_shards
    if terminal != progress.planned_shards or any(
        (
            progress.pending_shards,
            progress.running_shards,
            progress.retryable_shards,
            progress.failed_shards,
        )
    ):
        raise ValueError("campaign is not fully terminal and cannot be finalized")
    if progress.planned_shards != len(plan.shards):
        raise ValueError("campaign progress does not cover the complete plan")
    return plan, universe, progress


def _select_shards(
    progress: BackfillCampaignProgress,
    parquet_roots: Sequence[Path],
) -> tuple[Path, ...]:
    expected = {
        item.shard_id for item in progress.shards if item.status == "succeeded"
    }
    discovered: dict[str, Path] = {}
    duplicates: set[str] = set()
    for root in parquet_roots:
        for path in root.rglob("*.parquet"):
            if path.name.startswith("._"):
                continue
            shard_id = path.stem
            if shard_id not in expected:
                continue
            if shard_id in discovered and discovered[shard_id].resolve() != path.resolve():
                duplicates.add(shard_id)
            else:
                discovered[shard_id] = path
    if duplicates:
        raise ValueError(f"duplicate authoritative parquet shards: {sorted(duplicates)[:5]}")
    missing = expected - discovered.keys()
    if missing:
        raise ValueError(f"missing authoritative parquet shards: {sorted(missing)[:5]}")
    if len(discovered) != progress.succeeded_shards:
        raise ValueError("authoritative parquet count conflicts with campaign progress")
    return tuple(discovered[key] for key in sorted(discovered))


def finalize_and_publish_backfill(
    *,
    data_root: Path,
    plan_path: Path,
    universe_path: Path,
    progress_path: Path,
    parquet_roots: Sequence[Path],
    environment_id: str = "nas-catalog-publisher",
    created_at: datetime | None = None,
) -> tuple[PublicationReceipt, dict[str, object]]:
    """Create one immutable full-market Parquet and register it in Catalog.

    This is deliberately replay-only: it consumes existing shard Parquet files and
    never constructs or calls a market-data provider.
    """

    import duckdb

    data_root = data_root.expanduser().resolve()
    plan, universe, progress = _load_contracts(plan_path, universe_path, progress_path)
    parquet_paths = _select_shards(progress, parquet_roots)
    generated_at = created_at or progress.observed_at
    version = plan.dataset_version
    final_root = data_root / "canonical" / CANONICAL_DATASET_ID / f"version={version}"
    evidence_root = final_root / "evidence"
    bars_path = final_root / "bars.parquet"
    inventory_path = evidence_root / "finalization-inventory.json"
    instruments_path = evidence_root / "universe.json"
    quality_path = evidence_root / "quality.json"
    proposed_path = (
        data_root
        / "staging/proposed_manifests"
        / f"{CANONICAL_DATASET_ID}@{version}.json"
    )
    final_root.mkdir(parents=True, exist_ok=True)
    evidence_root.mkdir(parents=True, exist_ok=True)
    proposed_path.parent.mkdir(parents=True, exist_ok=True)

    inventory = {
        "schema_version": "1.0.0",
        "campaign_id": plan.campaign_id,
        "dataset_id": CANONICAL_DATASET_ID,
        "dataset_version": version,
        "universe_version": universe.version,
        "planned_shards": progress.planned_shards,
        "succeeded_shards": progress.succeeded_shards,
        "empty_shards": progress.empty_shards,
        "expected_rows": progress.total_rows,
        "source_shards": [
            {"shard_id": path.stem, "sha256": _sha256(path), "bytes": path.stat().st_size}
            for path in parquet_paths
        ],
    }
    _write_json(inventory_path, inventory)
    _write_json(
        instruments_path,
        universe.model_dump(mode="json", exclude_computed_fields=True),
    )

    proof: list[tuple[object, ...]] = []

    def write_bars(temporary: Path) -> None:
        descriptor, local_name = tempfile.mkstemp(
            prefix="quantstack-full-market-",
            suffix=".parquet",
        )
        local_temporary = Path(local_name)
        os.close(descriptor)
        local_temporary.unlink()
        try:
            connection = duckdb.connect()
            try:
                quoted_columns = ", ".join(f'"{column}"' for column in _BAR_COLUMNS)
                connection.execute(
                    f"COPY (SELECT {quoted_columns} FROM read_parquet(?) "
                    "ORDER BY instrument_id, trading_date) TO "
                    f"{_sql_string(str(local_temporary))} "
                    "(FORMAT PARQUET, COMPRESSION ZSTD)",
                    [[str(path) for path in parquet_paths]],
                )
            finally:
                connection.close()
            shutil.copyfile(local_temporary, temporary)
        finally:
            local_temporary.unlink(missing_ok=True)

    def validate_bars(candidate: Path) -> None:
        connection = duckdb.connect()
        try:
            row = connection.execute(
                "SELECT count(*) AS rows, "
                "count(DISTINCT (instrument_id, trading_date)) AS distinct_keys, "
                "count(DISTINCT instrument_id) AS instruments, "
                "min(trading_date) AS first_date, max(trading_date) AS last_date, "
                "count(DISTINCT provider) AS providers, min(provider) AS provider, "
                "count(DISTINCT adjustment) AS adjustments, min(adjustment) AS adjustment "
                "FROM read_parquet(?)",
                [str(candidate)],
            ).fetchone()
        finally:
            connection.close()
        if row is None:
            raise ValueError("finalized bars validation returned no proof row")
        proof[:] = [row]

    atomic_write_immutable_file(
        bars_path,
        write=write_bars,
        validate=validate_bars,
        equivalent=lambda current, candidate: _sha256(current) == _sha256(candidate),
        artifact_kind="canonical full-market Parquet",
    )
    if not proof:
        validate_bars(bars_path)
    (
        row_count,
        distinct_keys,
        instrument_count,
        first_date,
        last_date,
        provider_count,
        provider,
        adjustment_count,
        adjustment,
    ) = proof[0]
    if row_count != progress.total_rows:
        raise ValueError(
            f"final row count conflict: expected {progress.total_rows}, got {row_count}"
        )
    if distinct_keys != row_count:
        raise ValueError("finalized bars contain duplicate instrument/date keys")
    if (provider_count, provider, adjustment_count, adjustment) != (1, "baostock", 1, "none"):
        raise ValueError("finalized bars are not a single-source unadjusted BaoStock baseline")

    quality = {
        "schema_version": "1.0.0",
        "status": "passed",
        "checks": [
            "complete_terminal_campaign",
            "authoritative_shard_set",
            "expected_row_count",
            "instrument_date_unique",
            "single_baostock_provider",
            "unadjusted_only",
        ],
        "warnings": [
            f"{progress.empty_shards} terminal empty shards remain explicit coverage gaps",
            "provider-observed coverage is not an official point-in-time universe",
            "qfq/hfq and Stock Screener production eligibility are outside this publication",
        ],
        "row_count": row_count,
        "distinct_keys": distinct_keys,
        "instrument_count": instrument_count,
        "first_date": first_date.isoformat(),
        "last_date": last_date.isoformat(),
        "duplicate_keys": row_count - distinct_keys,
        "succeeded_shards": progress.succeeded_shards,
        "empty_shards": progress.empty_shards,
    }
    _write_json(quality_path, quality)
    bars_sha256 = _sha256(bars_path)
    manifest = DatasetManifest(
        manifest_schema_version="1.3.0",
        dataset_id=CANONICAL_DATASET_ID,
        version=version,
        layer=DatasetLayer.NORMALIZED,
        kind=DatasetKind.MARKET_BARS,
        market="CN",
        provider="baostock",
        provider_version="0.9.3",
        adapter_version="cn-backfill-canonical-finalize-v1",
        native_schema_version="baostock-query-history-k-data-plus-v1",
        frequency="1d",
        schema_ref="quant-stack://schemas/cn-equity-daily-unadjusted-v1",
        storage_uri=f"quant-data://{bars_path.relative_to(data_root).as_posix()}",
        created_at=generated_at,
        available_at_policy="provider retrieval timestamp; no PIT membership claim",
        revision_policy="immutable",
        row_count=row_count,
        sha256=bars_sha256,
        quality_status="passed",
        quality=DatasetQualityMetadata(
            status="passed",
            checks=list(quality["checks"]),
            warnings=list(quality["warnings"]),
            report_uri=f"quant-data://{quality_path.relative_to(data_root).as_posix()}",
        ),
        license_scope="BaoStock terms; internal research and controlled consumers",
        source_url="http://baostock.com/baostock/index.php/Python_API%E6%96%87%E6%A1%A3",
        input_dataset_versions=[
            f"cn_equity_daily_backfill_pilot@{plan.dataset_version}"
        ],
        raw_storage_uri=f"quant-data://{inventory_path.relative_to(data_root).as_posix()}",
        job_id=plan.campaign_id,
    )
    _write_json(proposed_path, manifest.model_dump(mode="json"))
    artifacts = (
        _artifact(data_root, inventory_path, "raw", dataset_version=version),
        _artifact(data_root, instruments_path, "instruments", dataset_version=version),
        _artifact(data_root, bars_path, "bars", dataset_version=version),
        _artifact(data_root, quality_path, "quality", dataset_version=version),
        _artifact(data_root, proposed_path, "proposed_manifest", dataset_version=version),
    )
    policy = ConsumerEligibilityPolicy(
        route_class="canonical",
        shadow_allowed=False,
        candidate_allowed=False,
        reason="historical unadjusted baseline is cataloged but not an SS or production candidate",
    )
    bundle = PublicationBundle(
        schema_version="1.1.0",
        task_id=f"finalize-{plan.campaign_id}",
        attempt_id="attempt-1",
        dataset_id=CANONICAL_DATASET_ID,
        dataset_version=version,
        provider="baostock",
        endpoint="query_history_k_data_plus",
        artifacts=artifacts,
        proposed_manifests=(manifest,),
        created_at=generated_at,
        producer_environment=ProducerEnvironment.current(environment_id),
        canonical_eligible=True,
        publication_gate="eligible",
        publication_state=publication_state_for(
            "eligible",
            quality_passed=True,
            comparison_status="not_required",
            consumer_eligibility=policy,
        ),
        consumer_eligibility=policy,
        canonical_source_provider="baostock",
        canonical_source_endpoint="query_history_k_data_plus",
        canonical_selection_reason="explicit finalized full-market unadjusted historical baseline",
        selection_rule_id="cn-full-history-unadjusted-finalize-v1",
    )
    publisher = CatalogPublisher(data_root)
    receipt = publisher.publish(bundle)
    replay = publisher.replay_receipt(receipt)
    if not replay.verified:
        raise ValueError("catalog publication receipt replay did not verify")
    return receipt, {
        **quality,
        "manifest_uri": receipt.manifest_uris[0],
        "receipt_id": receipt.receipt_id,
        "receipt_replay_verified": replay.verified,
        "ss_candidate": False,
        "production_pointer_updated": False,
    }
