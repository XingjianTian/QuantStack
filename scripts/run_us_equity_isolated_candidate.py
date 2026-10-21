from __future__ import annotations

import argparse
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from quant_stack.data.providers.yfinance import (
    ReplayYFinanceTransport,
    YFinanceProvider,
    run_yfinance_daily_task,
)
from quant_stack.data.storage.atomic import atomic_replace_bytes
from quant_stack.data.tasks import TaskState
from quant_stack.data.worker_bundle import build_worker_envelope, create_worker_archive

ISOLATED_SAMPLE_SYMBOLS = ("AAPL", "IBM")


def _key(path: Path) -> bytes:
    payload = path.read_bytes()
    if len(payload) < 32:
        raise ValueError("worker HMAC key file must contain at least 32 bytes")
    return payload


def run(args: argparse.Namespace) -> dict[str, object]:
    transport = ReplayYFinanceTransport(args.fixture) if args.fixture else None
    execution_time = (
        datetime.combine(args.end + timedelta(days=1), datetime.min.time(), tzinfo=UTC)
        if args.fixture
        else datetime.now(UTC)
    )
    execution = run_yfinance_daily_task(
        args.staging_root,
        args.task_state_root,
        symbols=list(ISOLATED_SAMPLE_SYMBOLS),
        start=args.start,
        end=args.end,
        dataset_version=args.dataset_version,
        execution_time=execution_time,
        transport=transport,
    )
    if (
        execution.task.state is not TaskState.SUCCEEDED
        or execution.bundle is None
        or execution.result is None
        or execution.attempt is None
        or execution.attempt.publication_bundle_uri is None
    ):
        raise RuntimeError(f"US isolated candidate failed: state={execution.task.state}")

    root = args.staging_root.resolve()
    raw_path = root / execution.result.raw_artifact.uri.removeprefix("quant-data://")
    raw_payload = json.loads(raw_path.read_text(encoding="utf-8"))
    bundle_path = root / execution.attempt.publication_bundle_uri.removeprefix("quant-data://")
    envelope = build_worker_envelope(
        data_root=root,
        publication_bundle_path=bundle_path,
        source_commit=args.commit_sha,
        environment_id=args.environment_id,
        provider_contract=YFinanceProvider().runtime_contract,
        provider_transport_audit=raw_payload["transport_audit"],
    )
    archive = create_worker_archive(
        data_root=root,
        envelope=envelope,
        output_path=args.archive,
        hmac_key=_key(args.hmac_key_file),
    )
    return {
        "schema_version": "1.0.0",
        "generated_at": datetime.now(UTC).isoformat(),
        "mode": "replay" if args.fixture else "live",
        "dataset_id": envelope.dataset_id,
        "dataset_version": envelope.dataset_version,
        "symbols": list(ISOLATED_SAMPLE_SYMBOLS),
        "start": args.start.isoformat(),
        "end": args.end.isoformat(),
        "license_scope": YFinanceProvider.license_scope,
        "price_semantics": "unadjusted_ohlcv; adjusted_close_raw_only",
        "parallel_requests": False,
        "completed_requests": raw_payload["transport_audit"]["completed_requests"],
        "records_received": execution.result.bar_manifest.row_count,
        "quality_status": execution.result.quality_report.quality_status,
        "task_id": execution.task.task_id,
        "task_state": execution.task.state,
        "bundle_id": envelope.bundle_id,
        "archive_sha256": archive.sha256,
        "archive_size_bytes": archive.size_bytes,
        "archive_created": archive.created,
        "publication_authority": envelope.publication_authority,
        "catalog_published": False,
        "production_pointer_updated": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build the fixed two-symbol US equity isolated candidate archive"
    )
    parser.add_argument("--staging-root", type=Path, required=True)
    parser.add_argument("--task-state-root", type=Path, required=True)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--hmac-key-file", type=Path, required=True)
    parser.add_argument("--commit-sha", required=True)
    parser.add_argument("--environment-id", required=True)
    parser.add_argument("--dataset-version", required=True)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--fixture", type=Path)
    args = parser.parse_args()
    report = run(args)
    atomic_replace_bytes(
        args.report,
        (json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(),
    )
    print(
        json.dumps(
            {
                "bundle_id": report["bundle_id"],
                "task_state": report["task_state"],
                "records_received": report["records_received"],
                "catalog_published": False,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
