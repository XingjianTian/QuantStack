from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

from .models import DatasetKind, DatasetLayer, DatasetManifest, DatasetQualityMetadata
from .publication import (
    CatalogPublisher,
    ConsumerEligibilityPolicy,
    ProducerEnvironment,
    PublicationArtifact,
    PublicationBundle,
    PublicationReceipt,
    publication_state_for,
)
from .storage.atomic import atomic_write_immutable_bytes, atomic_write_immutable_file


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
        artifact_kind="SS shadow publication evidence",
    )


def _artifact(
    data_root: Path,
    path: Path,
    kind: str,
    *,
    dataset_id: str,
    dataset_version: str,
) -> PublicationArtifact:
    return PublicationArtifact(
        uri=f"quant-data://{path.relative_to(data_root).as_posix()}",
        kind=kind,
        size_bytes=path.stat().st_size,
        sha256=_sha256(path),
        dataset_id=dataset_id,
        dataset_version=dataset_version,
    )


def publish_unadjusted_ss_shadow_copy(
    *,
    data_root: Path,
    source_data_root: Path,
    source_manifest_path: Path,
    target_version: str,
    environment_id: str = "local-shadow-publisher",
) -> tuple[PublicationReceipt, dict[str, object]]:
    """Publish an immutable Batch-readable projection of raw-price bars for shadow only.

    This function performs no provider calls and never modifies the source manifest or
    Parquet. The new version deliberately remains adjustment=none and candidate-ineligible.
    """

    import duckdb

    data_root = data_root.expanduser().resolve()
    source_data_root = source_data_root.expanduser().resolve()
    source_manifest_path = source_manifest_path.expanduser().resolve()
    if (
        data_root == source_data_root
        or data_root in source_data_root.parents
        or source_data_root in data_root.parents
    ):
        raise ValueError("source and target data roots must be separate non-nested paths")
    if (
        source_manifest_path != source_data_root
        and source_data_root not in source_manifest_path.parents
    ):
        raise ValueError("source manifest escapes the source data root")
    source_manifest = DatasetManifest.model_validate_json(
        source_manifest_path.read_text(encoding="utf-8")
    )
    if source_manifest.dataset_id != "cn_equity_daily":
        raise ValueError("SS shadow copy only accepts cn_equity_daily")
    if source_manifest.kind is not DatasetKind.MARKET_BARS or source_manifest.market != "CN":
        raise ValueError("SS shadow copy requires CN market bars")
    if source_manifest.frequency != "1d" or source_manifest.quality_status != "passed":
        raise ValueError("SS shadow copy requires passed daily bars")
    if target_version == source_manifest.version:
        raise ValueError("shadow publication requires a new immutable dataset version")
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", target_version) is None:
        raise ValueError("shadow dataset version contains unsafe path characters")

    source_bars = (
        source_data_root / source_manifest.storage_uri.removeprefix("quant-data://")
    ).resolve()
    if source_data_root not in source_bars.parents:
        raise ValueError("source manifest storage URI escapes the source data root")
    if not source_bars.is_file() or _sha256(source_bars) != source_manifest.sha256:
        raise ValueError("source manifest Parquet hash verification failed")
    source_manifest_sha256 = _sha256(source_manifest_path)

    target_root = (
        data_root
        / "normalized/bars_1d/market=CN"
        / f"version={target_version}"
    )
    evidence_root = target_root / "evidence"
    bars_path = target_root / "bars.parquet"
    source_evidence_path = evidence_root / "source-manifest.json"
    coverage_path = evidence_root / "coverage.json"
    quality_path = evidence_root / "quality.json"
    proposed_path = (
        data_root / "staging/proposed_manifests" / f"cn_equity_daily@{target_version}.json"
    )

    def copy_bars(temporary: Path) -> None:
        connection = duckdb.connect()
        try:
            connection.execute(
                "COPY (SELECT instrument_id, symbol, venue, trading_date, ts_event, "
                "open, high, low, close, volume, amount, currency, "
                "struct_pack(adjustment := adjustment) AS lineage "
                "FROM read_parquet(?) ORDER BY instrument_id, trading_date) "
                f"TO {_sql_string(str(temporary))} (FORMAT PARQUET, COMPRESSION ZSTD)",
                [str(source_bars)],
            )
        finally:
            connection.close()

    proof: list[tuple[object, ...]] = []

    def validate_bars(candidate: Path) -> None:
        connection = duckdb.connect()
        try:
            row = connection.execute(
                "SELECT count(*), count(DISTINCT (instrument_id, trading_date)), "
                "count(DISTINCT instrument_id), min(trading_date), max(trading_date), "
                "count(DISTINCT lineage.adjustment), min(lineage.adjustment), "
                "sum(CAST(open AS DECIMAL(38, 8))), "
                "sum(CAST(high AS DECIMAL(38, 8))), "
                "sum(CAST(low AS DECIMAL(38, 8))), "
                "sum(CAST(close AS DECIMAL(38, 8))), "
                "sum(CAST(volume AS DECIMAL(38, 8))), "
                "sum(CAST(coalesce(amount, 0) AS DECIMAL(38, 8))) "
                "FROM read_parquet(?)",
                [str(candidate)],
            ).fetchone()
            source_row = connection.execute(
                "SELECT count(*), count(DISTINCT (instrument_id, trading_date)), "
                "count(DISTINCT instrument_id), min(trading_date), max(trading_date), "
                "count(DISTINCT adjustment), min(adjustment), "
                "sum(CAST(open AS DECIMAL(38, 8))), "
                "sum(CAST(high AS DECIMAL(38, 8))), "
                "sum(CAST(low AS DECIMAL(38, 8))), "
                "sum(CAST(close AS DECIMAL(38, 8))), "
                "sum(CAST(volume AS DECIMAL(38, 8))), "
                "sum(CAST(coalesce(amount, 0) AS DECIMAL(38, 8))) "
                "FROM read_parquet(?)",
                [str(source_bars)],
            ).fetchone()
        finally:
            connection.close()
        if row is None:
            raise ValueError("shadow Parquet validation returned no proof")
        if row[0] != source_manifest.row_count or row[0] != row[1]:
            raise ValueError("shadow Parquet row count or key uniqueness conflict")
        if (row[5], row[6]) != (1, "none"):
            raise ValueError("shadow publication must remain explicitly unadjusted")
        if source_row is None or row != source_row:
            raise ValueError("shadow projection changed source price or coverage aggregates")
        proof[:] = [row]

    created = atomic_write_immutable_file(
        bars_path,
        write=copy_bars,
        validate=validate_bars,
        equivalent=lambda current, candidate: _sha256(current) == _sha256(candidate),
        artifact_kind="SS shadow Parquet copy",
    )
    if not proof:
        validate_bars(bars_path)
    row_count, distinct_keys, instrument_count, first_date, last_date = proof[0][:5]

    _write_json(
        source_evidence_path,
        {
            "schema_version": "1.0.0",
            "source_dataset_id": source_manifest.dataset_id,
            "source_dataset_version": source_manifest.version,
            "source_manifest_sha256": source_manifest_sha256,
            "source_storage_uri": source_manifest.storage_uri,
            "source_parquet_sha256": source_manifest.sha256,
            "copy_mode": "immutable_batch_schema_projection",
            "projection": "flat adjustment moved to lineage.adjustment; price values unchanged",
        },
    )
    _write_json(
        coverage_path,
        {
            "schema_version": "1.0.0",
            "row_count": row_count,
            "distinct_instrument_date_keys": distinct_keys,
            "instrument_count": instrument_count,
            "first_date": first_date.isoformat(),
            "last_date": last_date.isoformat(),
        },
    )
    quality = {
        "schema_version": "1.0.0",
        "status": "passed",
        "checks": [
            "source_manifest_hash_verified",
            "batch_schema_projection",
            "price_and_coverage_aggregates_preserved",
            "order_independent_decimal_aggregate_proof",
            "source_row_count_preserved",
            "instrument_date_unique",
            "unadjusted_only",
        ],
        "warnings": [
            "shadow-only: prices are unadjusted",
            "Adj Close compatibility aliases do not make these bars adjusted",
            "candidate and production require independently verified qfq",
        ],
        "candidate_allowed": False,
        "production_price_basis_required": "qfq",
    }
    _write_json(quality_path, quality)

    source_evidence_uri = f"quant-data://{source_evidence_path.relative_to(data_root).as_posix()}"
    quality_uri = f"quant-data://{quality_path.relative_to(data_root).as_posix()}"
    manifest = source_manifest.model_copy(
        update={
            # The source has no full-range official calendar/market-state evidence.
            # Schema 1.2 keeps that limitation explicit and Batch-readable for shadow;
            # candidate eligibility remains independently hard-disabled below.
            "manifest_schema_version": "1.2.0",
            "version": target_version,
            "layer": DatasetLayer.NORMALIZED,
            "adapter_version": "cn-ss-unadjusted-shadow-copy-v1",
            "storage_uri": f"quant-data://{bars_path.relative_to(data_root).as_posix()}",
            "schema_ref": "quant-stack://schemas/cn-equity-daily-unadjusted-shadow-v1",
            "revision_policy": "immutable",
            "sha256": _sha256(bars_path),
            "quality": DatasetQualityMetadata(
                status="passed",
                checks=list(quality["checks"]),
                warnings=list(quality["warnings"]),
                report_uri=quality_uri,
            ),
            "input_dataset_versions": [
                f"{source_manifest.dataset_id}@{source_manifest.version}"
            ],
            "raw_storage_uri": source_evidence_uri,
            "job_id": f"ss-shadow-copy-{source_manifest.version}",
        }
    )
    _write_json(proposed_path, manifest.model_dump(mode="json"))

    artifacts = (
        _artifact(
            data_root,
            source_evidence_path,
            "raw",
            dataset_id=manifest.dataset_id,
            dataset_version=target_version,
        ),
        _artifact(
            data_root,
            coverage_path,
            "instruments",
            dataset_id=manifest.dataset_id,
            dataset_version=target_version,
        ),
        _artifact(
            data_root,
            bars_path,
            "bars",
            dataset_id=manifest.dataset_id,
            dataset_version=target_version,
        ),
        _artifact(
            data_root,
            quality_path,
            "quality",
            dataset_id=manifest.dataset_id,
            dataset_version=target_version,
        ),
        _artifact(
            data_root,
            proposed_path,
            "proposed_manifest",
            dataset_id=manifest.dataset_id,
            dataset_version=target_version,
        ),
    )
    policy = ConsumerEligibilityPolicy(
        route_class="canonical",
        shadow_allowed=True,
        candidate_allowed=False,
        reason=(
            "unadjusted historical bars are diagnostic shadow input only; "
            "SS candidate requires verified qfq"
        ),
    )
    bundle = PublicationBundle(
        schema_version="1.1.0",
        task_id=f"ss-shadow-copy-{source_manifest.version}",
        attempt_id="attempt-1",
        dataset_id=manifest.dataset_id,
        dataset_version=target_version,
        provider=manifest.provider,
        endpoint="immutable_shadow_copy",
        artifacts=artifacts,
        proposed_manifests=(manifest,),
        created_at=source_manifest.created_at,
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
        canonical_source_provider=manifest.provider,
        canonical_source_endpoint="immutable_shadow_copy",
        canonical_selection_reason=(
            "value-preserving unadjusted Batch projection for adjustment-aware "
            "SS shadow diagnosis"
        ),
        selection_rule_id="cn-ss-unadjusted-shadow-only-v1",
    )
    publisher = CatalogPublisher(data_root)
    receipt = publisher.publish(bundle)
    replay = publisher.replay_receipt(receipt)
    if not replay.verified:
        raise ValueError("shadow publication receipt replay did not verify")
    return receipt, {
        "source_dataset_version": source_manifest.version,
        "dataset_version": target_version,
        "created": created,
        "row_count": row_count,
        "instrument_count": instrument_count,
        "first_date": first_date.isoformat(),
        "last_date": last_date.isoformat(),
        "source_parquet_unchanged": _sha256(source_bars) == source_manifest.sha256,
        "copy_sha256": _sha256(bars_path),
        "shadow_eligible": receipt.publication_state.shadow_eligible,
        "candidate_eligible": receipt.publication_state.candidate_eligible,
        "cutover_eligible": receipt.publication_state.cutover_eligible,
        "receipt_replay_verified": replay.verified,
    }


def publish_qfq_ss_shadow_derivative(
    *,
    data_root: Path,
    source_manifest_path: Path,
    source_bars_path: Path,
    qfq_bars_path: Path,
    target_version: str,
    qualification_evidence_path: Path | None = None,
    environment_id: str = "local-qfq-shadow-publisher",
) -> tuple[PublicationReceipt, dict[str, object]]:
    """Publish a prevalidated qfq Parquet for SS shadow or qualified candidate reads."""

    import shutil

    import duckdb

    data_root = data_root.expanduser().resolve()
    source_manifest_path = source_manifest_path.expanduser().resolve()
    source_bars_path = source_bars_path.expanduser().resolve()
    qfq_bars_path = qfq_bars_path.expanduser().resolve()
    if qualification_evidence_path is not None:
        qualification_evidence_path = qualification_evidence_path.expanduser().resolve()
    source = DatasetManifest.model_validate_json(
        source_manifest_path.read_text(encoding="utf-8")
    )
    if source.dataset_id != "cn_equity_daily" or source.market != "CN":
        raise ValueError("qfq shadow publication requires CN equity daily bars")
    if source.frequency != "1d" or source.quality_status != "passed":
        raise ValueError("qfq shadow publication requires passed daily source bars")
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", target_version) is None:
        raise ValueError("qfq dataset version contains unsafe path characters")
    if not qfq_bars_path.is_file():
        raise ValueError("qfq derivative Parquet is missing")
    if not source_bars_path.is_file():
        raise ValueError("qfq authoritative source Parquet is missing")

    target_root = data_root / "normalized/bars_1d/market=CN" / f"version={target_version}"
    evidence_root = target_root / "evidence"
    bars_path = target_root / "bars.parquet"
    source_evidence_path = evidence_root / "source.json"
    instruments_path = evidence_root / "instruments.json"
    quality_path = evidence_root / "quality.json"
    qualification_path = evidence_root / "ss-qualification.json"
    proposed_path = (
        data_root / "staging/proposed_manifests" / f"cn_equity_daily@{target_version}.json"
    )
    proof: dict[str, object] = {}
    candidate_qualified = False
    qualification: dict[str, object] | None = None
    if qualification_evidence_path is not None:
        qualification = json.loads(qualification_evidence_path.read_text(encoding="utf-8"))
        runtime = qualification.get("ss_runtime", {})
        comparison = qualification.get("price_comparison", {})
        screening = qualification.get("indicator_and_screener_comparison", {})
        candidate_qualified = bool(
            qualification.get("result") == "PASS_QFQ_SHADOW_READY_FOR_CANDIDATE"
            and runtime.get("adjustment_compatible_reports") == runtime.get("reports")
            and all(
                runtime.get(field) == 0
                for field in (
                    "candidate_failures",
                    "comparison_failures",
                    "report_failures",
                    "worker_failures",
                    "dropped",
                )
            )
            and comparison.get("price_ratio_regime_changes") == 0
            and float(comparison.get("close_return_drift_p95_max", 1.0)) <= 0.00001
            and screening.get("scanner_categorical_change_symbols") == 0
            and screening.get("ma_score_change_symbols") == 0
            and screening.get("vcp_detection_change_symbols") == 0
        )
        if not candidate_qualified:
            raise ValueError("qfq SS qualification evidence did not pass candidate gates")

    def copy_bars(temporary: Path) -> None:
        shutil.copyfile(qfq_bars_path, temporary)

    def validate_bars(candidate: Path) -> None:
        connection = duckdb.connect()
        try:
            row = connection.execute(
                "SELECT count(*), count(DISTINCT (instrument_id, trading_date)), "
                "count(DISTINCT instrument_id), min(trading_date), max(trading_date), "
                "count(DISTINCT lineage.adjustment), min(lineage.adjustment), "
                "count(*) FILTER (WHERE NOT (high >= greatest(open, close) "
                "AND low <= least(open, close) AND low <= high)) "
                "FROM read_parquet(?)",
                [str(candidate)],
            ).fetchone()
            source_row = connection.execute(
                "SELECT count(*), count(DISTINCT instrument_id), min(trading_date), "
                "max(trading_date) FROM read_parquet(?)",
                [str(source_bars_path)],
            ).fetchone()
            coverage_diff = connection.execute(
                "SELECT count(*) FROM ((SELECT instrument_id, trading_date "
                "FROM read_parquet(?) EXCEPT SELECT instrument_id, trading_date "
                "FROM read_parquet(?)) UNION ALL (SELECT instrument_id, trading_date "
                "FROM read_parquet(?) EXCEPT SELECT instrument_id, trading_date "
                "FROM read_parquet(?)))",
                [
                    str(source_bars_path),
                    str(candidate),
                    str(candidate),
                    str(source_bars_path),
                ],
            ).fetchone()[0]
        finally:
            connection.close()
        if row is None or row[0] != row[1] or row[2] < 1:
            raise ValueError("qfq derivative coverage is invalid")
        if row[5:] != (1, "qfq", 0):
            raise ValueError("qfq derivative semantics or OHLC shape is invalid")
        candidate_coverage = (row[0], row[2], row[3], row[4])
        if source_row is None or candidate_coverage != source_row or coverage_diff != 0:
            raise ValueError("qfq derivative does not exactly cover the source universe")
        proof.update(
            row_count=int(row[0]),
            instrument_count=int(row[2]),
            first_date=row[3].isoformat(),
            last_date=row[4].isoformat(),
        )

    created = atomic_write_immutable_file(
        bars_path,
        write=copy_bars,
        validate=validate_bars,
        equivalent=lambda current, candidate: _sha256(current) == _sha256(candidate),
        artifact_kind="SS qfq shadow Parquet",
    )
    if not proof:
        validate_bars(bars_path)

    _write_json(
        source_evidence_path,
        {
            "schema_version": "1.0.0",
            "source_dataset_id": source.dataset_id,
            "source_dataset_version": source.version,
            "source_manifest": str(source_manifest_path),
            "adjustment": "qfq",
            "formula": "raw_price * back_adjust_factor_t / latest_back_adjust_factor",
            "latest_price_anchor": "raw",
            "raw_bars_unchanged": True,
        },
    )
    quality = {
        "schema_version": "1.0.0",
        "status": "passed",
        "checks": [
            "instrument_date_unique",
            "qfq_only",
            "ohlc_shape_valid",
            "latest_price_anchored_to_raw",
        ],
        "warnings": (
            []
            if candidate_qualified
            else ["shadow-only until SS indicator and screener comparison passes"]
        ),
        "candidate_allowed": candidate_qualified,
    }
    connection = duckdb.connect()
    try:
        instruments = [
            {"instrument_id": row[0], "symbol": row[1], "venue": row[2]}
            for row in connection.execute(
                "SELECT DISTINCT instrument_id, symbol, venue FROM read_parquet(?) "
                "ORDER BY instrument_id",
                [str(bars_path)],
            ).fetchall()
        ]
    finally:
        connection.close()
    _write_json(instruments_path, {"schema_version": "1.0.0", "rows": instruments})
    _write_json(quality_path, quality)
    if qualification is not None:
        _write_json(qualification_path, qualification)
    quality_uri = f"quant-data://{quality_path.relative_to(data_root).as_posix()}"
    source_uri = f"quant-data://{source_evidence_path.relative_to(data_root).as_posix()}"
    manifest = source.model_copy(
        update={
            "version": target_version,
            "layer": DatasetLayer.NORMALIZED,
            "adapter_version": "cn-ss-qfq-derivative-v1",
            "storage_uri": f"quant-data://{bars_path.relative_to(data_root).as_posix()}",
            "schema_ref": "quant-stack://schemas/cn-equity-daily-qfq-v1",
            "revision_policy": "immutable",
            "row_count": proof["row_count"],
            "sha256": _sha256(bars_path),
            "quality_status": "passed",
            "quality": DatasetQualityMetadata(
                status="passed",
                checks=list(quality["checks"]),
                warnings=list(quality["warnings"]),
                report_uri=quality_uri,
            ),
            "input_dataset_versions": [f"{source.dataset_id}@{source.version}"],
            "raw_storage_uri": source_uri,
            "job_id": f"ss-qfq-derivative-{target_version}",
        }
    )
    _write_json(proposed_path, manifest.model_dump(mode="json"))
    artifact_inputs = [
        (source_evidence_path, "raw"),
        (instruments_path, "instruments"),
        (bars_path, "bars"),
        (quality_path, "quality"),
        (proposed_path, "proposed_manifest"),
    ]
    if qualification is not None:
        artifact_inputs.append((qualification_path, "comparison_raw"))
    artifacts = tuple(
        _artifact(
            data_root,
            path,
            kind,
            dataset_id=manifest.dataset_id,
            dataset_version=target_version,
        )
        for path, kind in artifact_inputs
    )
    policy = ConsumerEligibilityPolicy(
        route_class="canonical",
        shadow_allowed=True,
        candidate_allowed=candidate_qualified,
        reason=(
            "qfq passed SS price, indicator and screener qualification"
            if candidate_qualified
            else "qfq is Batch-readable for SS shadow; indicator/screener gates are pending"
        ),
    )
    bundle = PublicationBundle(
        schema_version="1.1.0",
        task_id=f"ss-qfq-derivative-{target_version}",
        attempt_id="attempt-1",
        dataset_id=manifest.dataset_id,
        dataset_version=target_version,
        provider=manifest.provider,
        endpoint="qfq_derivative",
        artifacts=artifacts,
        proposed_manifests=(manifest,),
        created_at=source.created_at,
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
        canonical_source_provider=manifest.provider,
        canonical_source_endpoint="qfq_derivative",
        canonical_selection_reason=(
            "qfq passed SS price, indicator and screener shadow qualification"
            if candidate_qualified
            else "independently qualified qfq direction for SS shadow"
        ),
        selection_rule_id=(
            "cn-ss-qfq-candidate-v1" if candidate_qualified else "cn-ss-qfq-shadow-v1"
        ),
    )
    publisher = CatalogPublisher(data_root)
    receipt = publisher.publish(bundle)
    replay = publisher.replay_receipt(receipt)
    if not replay.verified:
        raise ValueError("qfq publication receipt replay did not verify")
    return receipt, {
        **proof,
        "created": created,
        "dataset_version": target_version,
        "shadow_eligible": receipt.publication_state.shadow_eligible,
        "candidate_eligible": receipt.publication_state.candidate_eligible,
        "cutover_eligible": receipt.publication_state.cutover_eligible,
        "receipt_replay_verified": replay.verified,
    }
