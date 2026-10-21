#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from quant_stack.data.ingestion import ProviderRuntimeContract
from quant_stack.data.providers.cn.live import BAOSTOCK_LIVE_RUNTIME_CONTRACT
from quant_stack.data.publication import load_publication_bundle
from quant_stack.data.worker_bundle import build_worker_envelope, create_worker_archive


def package_existing_release(
    *,
    staging_root: Path,
    dataset_version: str,
    source_commit: str,
    hmac_key: bytes,
    output_path: Path,
) -> dict[str, object]:
    root = staging_root.expanduser().resolve()
    bundle_path = (
        root / "staging/publication_bundles" / f"cn_equity_daily@{dataset_version}.json"
    )
    bundle = load_publication_bundle(bundle_path)
    if (bundle.dataset_id, bundle.dataset_version) != ("cn_equity_daily", dataset_version):
        raise ValueError("qfq package dataset identity conflict")
    raw = next(item for item in bundle.artifacts if item.kind == "raw")
    raw_path = root / raw.uri.removeprefix("quant-data://")
    raw_payload = json.loads(raw_path.read_text(encoding="utf-8"))
    transport_audit = raw_payload.get("transport_audit")
    if not isinstance(transport_audit, dict):
        raise ValueError("qfq package is missing factor transport evidence")
    runtime_contract = ProviderRuntimeContract(
        **BAOSTOCK_LIVE_RUNTIME_CONTRACT.model_dump(exclude={"endpoint"}),
        endpoint="query_adjust_factor",
    )
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
        output_path=output_path.expanduser().resolve(),
        hmac_key=hmac_key,
    )
    return {
        "schema_version": "quantstack.qfq-release-package.v1",
        "dataset_id": envelope.dataset_id,
        "dataset_version": envelope.dataset_version,
        "worker_bundle_id": envelope.bundle_id,
        "source_commit": envelope.source_commit,
        "archive_size_bytes": archive.size_bytes,
        "created": archive.created,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Package a prepared qfq release in a clean low-memory process"
    )
    parser.add_argument("--staging-root", type=Path, required=True)
    parser.add_argument("--dataset-version", required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--hmac-key-file", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = package_existing_release(
        staging_root=args.staging_root,
        dataset_version=args.dataset_version,
        source_commit=args.source_commit,
        hmac_key=args.hmac_key_file.read_bytes(),
        output_path=args.output,
    )
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
