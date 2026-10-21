#!/usr/bin/env python3
"""Package one immutable qfq candidate in the existing signed worker protocol."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import UTC, datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb

from quant_stack.data.ingestion import ProviderRuntimeContract
from quant_stack.data.models import (
    DatasetKind,
    DatasetLayer,
    DatasetManifest,
    DatasetQualityMetadata,
)
from quant_stack.data.providers.cn.live import BAOSTOCK_LIVE_RUNTIME_CONTRACT
from quant_stack.data.providers.cn.reconciliation import ProviderSourceIdentity
from quant_stack.data.publication import (
    ConsumerEligibilityPolicy,
    ProducerEnvironment,
    PublicationArtifact,
    PublicationBundle,
    publication_state_for,
)
from quant_stack.data.storage.atomic import (
    atomic_write_immutable_bytes,
    atomic_write_immutable_file,
)
from quant_stack.data.worker_bundle import build_worker_envelope, create_worker_archive


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json(path: Path, payload: object) -> None:
    atomic_write_immutable_bytes(
        path,
        (json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n").encode(),
        artifact_kind="qfq worker evidence",
    )


def _artifact(root: Path, path: Path, kind: str, version: str) -> PublicationArtifact:
    return PublicationArtifact(
        uri=f"quant-data://{path.relative_to(root).as_posix()}",
        kind=kind,
        size_bytes=path.stat().st_size,
        sha256=_sha256(path),
        dataset_id="cn_equity_daily",
        dataset_version=version,
    )


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _write_publication_bars(
    *,
    source: Path,
    destination: Path,
    runtime_contract: ProviderRuntimeContract,
    raw_uri: str,
    raw_sha256: str,
) -> None:
    connection = duckdb.connect()
    try:
        connection.execute(
            "COPY (SELECT * EXCLUDE (lineage), struct_pack("
            "adjustment := 'qfq', "
            f"provider := {_sql_literal(runtime_contract.provider)}, "
            f"endpoint := {_sql_literal(runtime_contract.endpoint)}, "
            f"provider_version := {_sql_literal(runtime_contract.provider_version)}, "
            f"adapter_version := {_sql_literal(runtime_contract.adapter_version)}, "
            f"native_schema_version := {_sql_literal(runtime_contract.native_schema_version)}, "
            f"raw_uri := {_sql_literal(raw_uri)}, "
            f"raw_sha256 := {_sql_literal(raw_sha256)}) AS lineage "
            f"FROM read_parquet({_sql_literal(str(source))})) "
            f"TO {_sql_literal(str(destination))} (FORMAT PARQUET)"
        )
    finally:
        connection.close()


def _validate_publication_bars(
    candidate: Path,
    *,
    expected_rows: int,
    runtime_contract: ProviderRuntimeContract,
    raw_uri: str,
    raw_sha256: str,
) -> None:
    connection = duckdb.connect()
    try:
        row = connection.execute(
            "SELECT count(*), count(DISTINCT (instrument_id, trading_date)), "
            "count(*) FILTER (WHERE lineage.adjustment != 'qfq'), "
            "list(DISTINCT [lineage.provider, lineage.endpoint, lineage.provider_version, "
            "lineage.adapter_version, lineage.native_schema_version, lineage.raw_uri, "
            "lineage.raw_sha256]) FROM read_parquet(?)",
            [str(candidate)],
        ).fetchone()
    finally:
        connection.close()
    expected_lineage = [
        runtime_contract.provider,
        runtime_contract.endpoint,
        runtime_contract.provider_version,
        runtime_contract.adapter_version,
        runtime_contract.native_schema_version,
        raw_uri,
        raw_sha256,
    ]
    if row != (expected_rows, expected_rows, 0, [expected_lineage]):
        raise ValueError("qfq publication projection failed coverage or lineage validation")


def build_archive(
    *,
    qfq_path: Path,
    factor_path: Path,
    staging_root: Path,
    dataset_version: str,
    source_commit: str,
    hmac_key: bytes,
    output_path: Path | None,
) -> dict[str, object]:
    factor_payload = json.loads(factor_path.read_text(encoding="utf-8"))
    requests = factor_payload.get("requests", [])
    if factor_payload.get("complete") is not True or len(requests) != factor_payload.get(
        "universe_count"
    ):
        raise ValueError("qfq worker archive requires complete factor evidence")
    expected_policy = {
        "concurrency": 1,
        "request_interval_ms": 50,
        "batch_size": 500,
        "batch_interval_seconds": 5,
    }
    if factor_payload.get("request_policy") != expected_policy:
        raise ValueError("qfq worker archive requires the serial factor request policy")
    request_ids = [str(item.get("instrument_id", "")) for item in requests]
    if (
        not all(request_ids)
        or len(request_ids) != len(set(request_ids))
        or any(
            item.get("status") not in {"ok", "ok_no_adjustments"}
            for item in requests
        )
    ):
        raise ValueError("qfq worker archive requires unique successful factor requests")
    connection = duckdb.connect()
    try:
        row = connection.execute(
            "SELECT count(*), count(DISTINCT (instrument_id, trading_date)), "
            "count(DISTINCT instrument_id), min(trading_date), max(trading_date), "
            "list(DISTINCT lineage.adjustment) FROM read_parquet(?)",
            [str(qfq_path)],
        ).fetchone()
        qfq_ids = {
            str(item[0])
            for item in connection.execute(
                "SELECT DISTINCT instrument_id FROM read_parquet(?)", [str(qfq_path)]
            ).fetchall()
        }
    finally:
        connection.close()
    if row is None or row[0] != row[1] or row[5] != ["qfq"]:
        raise ValueError("qfq worker artifact failed key or adjustment validation")
    if factor_payload.get("request_end") != row[4].isoformat():
        raise ValueError("qfq worker factor watermark does not match qfq data")
    if set(request_ids) != qfq_ids:
        raise ValueError("qfq worker factor instruments do not match qfq data")
    release_time = datetime.combine(
        row[4], time(15, 0), tzinfo=ZoneInfo("Asia/Shanghai")
    ).astimezone(UTC)

    root = staging_root.expanduser().resolve()
    candidate_root = (
        root / "normalized/bars_1d/market=CN" / f"version={dataset_version}"
    )
    bars_path = candidate_root / "bars.parquet"
    evidence_root = candidate_root / "evidence"
    raw_path = evidence_root / "factor-acquisition.json"
    instruments_path = evidence_root / "coverage.json"
    quality_path = evidence_root / "quality.json"
    manifest_path = root / "staging/proposed_manifests" / f"cn_equity_daily@{dataset_version}.json"
    bundle_path = root / "staging/publication_bundles" / f"cn_equity_daily@{dataset_version}.json"

    factor_sha = _sha256(factor_path)
    request_markers = [
        f"baostock:query_adjust_factor:{instrument_id}" for instrument_id in request_ids
    ]
    transport_audit = {
        "completed_requests": len(requests),
        "request_successful_source_fingerprints": request_markers,
        "successful_source_fingerprint": factor_sha,
        "request_policy": expected_policy,
    }
    symbols = request_ids
    runtime_contract = ProviderRuntimeContract(
        **BAOSTOCK_LIVE_RUNTIME_CONTRACT.model_dump(exclude={"endpoint"}),
        endpoint="query_adjust_factor",
    )
    raw_uri = f"quant-data://{raw_path.relative_to(root).as_posix()}"
    _json(
        raw_path,
        {
            "provider": "baostock",
            "endpoint": "query_adjust_factor",
            "request_end": factor_payload["request_end"],
            "factor_artifact_sha256": factor_sha,
            "transport_audit": transport_audit,
            "job": {"symbols": symbols},
        },
    )
    raw_sha = _sha256(raw_path)
    atomic_write_immutable_file(
        bars_path,
        write=lambda temporary: _write_publication_bars(
            source=qfq_path,
            destination=temporary,
            runtime_contract=runtime_contract,
            raw_uri=raw_uri,
            raw_sha256=raw_sha,
        ),
        validate=lambda candidate: _validate_publication_bars(
            candidate,
            expected_rows=int(row[0]),
            runtime_contract=runtime_contract,
            raw_uri=raw_uri,
            raw_sha256=raw_sha,
        ),
        equivalent=lambda current, candidate: _sha256(current) == _sha256(candidate),
        artifact_kind="qfq worker publication bars",
    )
    _json(
        instruments_path,
        {
            "row_count": row[0],
            "distinct_keys": row[1],
            "instrument_count": row[2],
            "first_date": row[3].isoformat(),
            "last_date": row[4].isoformat(),
        },
    )
    checks = [
        "complete_factor_universe",
        "unique_instrument_date_keys",
        "qfq_adjustment_only",
        "immutable_import_only",
    ]
    _json(quality_path, {"status": "passed", "checks": checks})
    quality_uri = f"quant-data://{quality_path.relative_to(root).as_posix()}"
    manifest = DatasetManifest(
        dataset_id="cn_equity_daily",
        version=dataset_version,
        layer=DatasetLayer.NORMALIZED,
        kind=DatasetKind.MARKET_BARS,
        market="CN",
        provider=runtime_contract.provider,
        provider_version=runtime_contract.provider_version,
        adapter_version=runtime_contract.adapter_version,
        native_schema_version=runtime_contract.native_schema_version,
        frequency="1d",
        schema_ref="quant-stack://schemas/cn-equity-daily-qfq-v1",
        storage_uri=f"quant-data://{bars_path.relative_to(root).as_posix()}",
        created_at=release_time,
        available_at_policy="after complete factor acquisition and immutable qfq build",
        revision_policy="immutable",
        row_count=int(row[0]),
        sha256=_sha256(bars_path),
        quality_status="passed",
        quality=DatasetQualityMetadata(status="passed", checks=checks, report_uri=quality_uri),
        license_scope="controlled BaoStock research data",
        source_url="https://baostock.com",
        raw_storage_uri=f"quant-data://{raw_path.relative_to(root).as_posix()}",
        job_id=f"qfq-isolated-{dataset_version}",
        available_at=release_time,
    )
    _json(manifest_path, manifest.model_dump(mode="json"))
    artifacts = tuple(
        _artifact(root, path, kind, dataset_version)
        for path, kind in (
            (raw_path, "raw"),
            (instruments_path, "instruments"),
            (bars_path, "bars"),
            (quality_path, "quality"),
            (manifest_path, "proposed_manifest"),
        )
    )
    policy = ConsumerEligibilityPolicy(
        shadow_allowed=True,
        candidate_allowed=True,
        reason="immutable qfq candidate remains isolated until an explicit cutover gate",
    )
    source_identity = ProviderSourceIdentity(
        provider=runtime_contract.provider,
        endpoint=runtime_contract.endpoint,
        provider_version=runtime_contract.provider_version,
        adapter_version=runtime_contract.adapter_version,
        native_schema_version=runtime_contract.native_schema_version,
        raw_uri=artifacts[0].uri,
        raw_sha256=artifacts[0].sha256,
    )
    bundle = PublicationBundle(
        task_id=f"qfq-isolated-{dataset_version}",
        attempt_id="attempt-1",
        dataset_id="cn_equity_daily",
        dataset_version=dataset_version,
        provider=runtime_contract.provider,
        endpoint=runtime_contract.endpoint,
        artifacts=artifacts,
        proposed_manifests=(manifest,),
        created_at=release_time,
        producer_environment=ProducerEnvironment.current("aliyun-daily-worker"),
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
        primary_source_identity=source_identity,
        canonical_source_provider=runtime_contract.provider,
        canonical_source_endpoint=runtime_contract.endpoint,
        canonical_selection_reason="complete factor evidence and deterministic qfq transform",
        selection_rule_id="cn-qfq-isolated-v1",
    )
    _json(bundle_path, bundle.model_dump(mode="json", exclude_computed_fields=True))
    report: dict[str, object] = {
        "dataset_version": dataset_version,
        "row_count": int(row[0]),
        "instrument_count": int(row[2]),
        "first_date": row[3].isoformat(),
        "last_date": row[4].isoformat(),
        "publication_bundle_id": bundle.bundle_id,
        "prepared": True,
        "catalog_published": False,
    }
    if output_path is None:
        return report
    envelope = build_worker_envelope(
        data_root=root,
        publication_bundle_path=bundle_path,
        source_commit=source_commit,
        environment_id="aliyun-daily-worker",
        provider_contract=runtime_contract,
        provider_transport_audit=transport_audit,
    )
    archive = create_worker_archive(
        data_root=root,
        envelope=envelope,
        output_path=output_path,
        hmac_key=hmac_key,
    )
    return {
        **report,
        "bundle_id": archive.envelope.bundle_id,
        "archive_size_bytes": archive.size_bytes,
        "created": archive.created,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--qfq", type=Path, required=True)
    parser.add_argument("--factors", type=Path, required=True)
    parser.add_argument("--staging-root", type=Path, required=True)
    parser.add_argument("--dataset-version", required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--hmac-key-file", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--prepare-only", action="store_true")
    args = parser.parse_args()
    if args.prepare_only == (args.output is not None):
        parser.error("provide exactly one of --prepare-only or --output")
    report = build_archive(
        qfq_path=args.qfq.expanduser().resolve(),
        factor_path=args.factors.expanduser().resolve(),
        staging_root=args.staging_root.expanduser().resolve(),
        dataset_version=args.dataset_version,
        source_commit=args.source_commit,
        hmac_key=args.hmac_key_file.read_bytes(),
        output_path=(args.output.expanduser().resolve() if args.output is not None else None),
    )
    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    main()
