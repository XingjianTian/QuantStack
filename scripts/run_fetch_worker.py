from __future__ import annotations

import argparse
import hashlib
import json
import time
from datetime import UTC, date, datetime
from pathlib import Path

from quant_stack.data.calendars import ObservedSessionCalendar
from quant_stack.data.evidence import (
    load_market_state_sample,
    load_official_calendar_evidence,
)
from quant_stack.data.ingestion import IngestionService
from quant_stack.data.providers.cn.live import (
    AKSHARE_LIVE_RUNTIME_CONTRACT,
    AKShareLiveProvider,
    normalize_akshare_live_bars,
)
from quant_stack.data.providers.cn.live.akshare_transport import AKShareEastmoneyTransport
from quant_stack.data.publication import ProducerEnvironment, write_publication_bundle
from quant_stack.data.storage import StagingFileSystemStorage
from quant_stack.data.storage.atomic import atomic_replace_bytes
from quant_stack.data.tasks import Task, TaskRuntime, TaskState
from quant_stack.data.worker_bundle import build_worker_envelope, create_worker_archive
from scripts.diagnose_akshare_network import INSTRUMENTS


def _key(path: Path) -> bytes:
    payload = path.read_bytes()
    if len(payload) < 32:
        raise ValueError("worker HMAC key file must contain at least 32 bytes")
    return payload


def run(args: argparse.Namespace) -> dict[str, object]:
    calendar = ObservedSessionCalendar(
        load_official_calendar_evidence(
            raw_path=args.calendar_raw,
            normalized_path=args.calendar_normalized,
            observed_at=datetime.now(UTC),
            raw_uri=args.calendar_raw_uri,
            normalized_uri=args.calendar_normalized_uri,
        )
    )
    # This also rejects unknown dates, holidays, and sessions that have not closed.
    list(calendar.sessions("CN", args.start, args.end))
    market_state_sample = load_market_state_sample(
        raw_path=args.market_state_raw,
        raw_uri=args.market_state_raw_uri,
    )
    transport = AKShareEastmoneyTransport(
        max_edge_attempts=args.max_edge_attempts,
        max_resolvers=args.max_resolvers,
    )
    provider = AKShareLiveProvider(INSTRUMENTS, request_transport=transport)
    storage = StagingFileSystemStorage(args.staging_root)
    service = IngestionService(
        providers={provider.name: provider},
        storage=storage,
        bar_normalizers={provider.name: normalize_akshare_live_bars},
        provider_schemas={provider.name: provider.bar_schema},
        calendar=calendar,
        market_state_sample=market_state_sample,
    )
    task = Task(
        dataset_id="cn_equity_daily_akshare_reference",
        dataset_version=args.dataset_version,
        provider="akshare",
        provider_version="1.18.81",
        endpoint="stock_zh_a_hist",
        adapter_version="real-cn-v2",
        native_schema_version="live-observed-2026-08-03-v1",
        market="CN",
        instrument_scope=tuple(item.instrument_id for item in INSTRUMENTS),
        start_date=args.start,
        end_date=args.end,
        frequency="1d",
        max_attempts=args.task_attempts,
        retry_backoff_seconds=args.retry_backoff_seconds,
        created_at=datetime.now(UTC),
    )
    runtime = TaskRuntime()
    execution = None
    for attempt_number in range(1, args.task_attempts + 1):
        execution = runtime.execute(task, service)
        if execution.task.state is TaskState.SUCCEEDED:
            break
        if execution.task.state is not TaskState.RETRYABLE_FAILED:
            break
        if attempt_number < args.task_attempts:
            time.sleep(args.retry_backoff_seconds)
    if execution is None or execution.bundle is None or execution.result is None:
        state = execution.task.state if execution is not None else "not_started"
        error_type = (
            execution.attempt.retry.error_type
            if execution is not None
            and execution.attempt is not None
            and execution.attempt.retry is not None
            else "unknown"
        )
        failure_stage = (
            execution.attempt.failure_context.failure_stage
            if execution is not None
            and execution.attempt is not None
            and execution.attempt.failure_context is not None
            else "unknown"
        )
        raise RuntimeError(
            "worker task did not produce a bundle: "
            f"state={state}; error_type={error_type}; stage={failure_stage}"
        )

    producer = execution.bundle.producer_environment
    sanitized_producer = ProducerEnvironment(
        environment_id=args.environment_id,
        hostname=(
            "sha256:"
            + hashlib.sha256(producer.hostname.encode()).hexdigest()
        ),
        system=producer.system,
        machine=producer.machine,
        python_version=producer.python_version,
    )
    bundle = execution.bundle.model_copy(
        update={
            "producer_environment": sanitized_producer,
            "canonical_selection_reason": (
                "AKShare is staged only as an optional reference/BJSE candidate dataset; "
                "it cannot replace BaoStock or TDX and requires NAS publication"
            ),
            "selection_rule_id": "akshare-reference-worker-poc-v1",
        }
    )
    bundle_path = write_publication_bundle(storage, bundle)
    transport_audit = provider.transport_audit()
    envelope = build_worker_envelope(
        data_root=storage.data_root,
        publication_bundle_path=bundle_path,
        source_commit=args.commit_sha,
        environment_id=args.environment_id,
        provider_contract=AKSHARE_LIVE_RUNTIME_CONTRACT,
        provider_transport_audit=transport_audit,
    )
    archive = create_worker_archive(
        data_root=storage.data_root,
        envelope=envelope,
        output_path=args.archive,
        hmac_key=_key(args.hmac_key_file),
    )
    return {
        "schema_version": "1.0.0",
        "generated_at": datetime.now(UTC).isoformat(),
        "source_commit": args.commit_sha,
        "worker_environment": envelope.worker_environment.model_dump(mode="json"),
        "provider_contract": envelope.provider_contract.model_dump(mode="json"),
        "dataset_id": envelope.dataset_id,
        "dataset_version": envelope.dataset_version,
        "task_id": task.task_id,
        "task_state": execution.task.state,
        "task_attempts": len(runtime.attempts(task.task_id)),
        "rows": execution.result.bar_manifest.row_count,
        "quality_status": execution.result.quality_report.quality_status,
        "successful_source_fingerprint": transport_audit[
            "successful_source_fingerprint"
        ],
        "bundle_id": envelope.bundle_id,
        "archive_sha256": archive.sha256,
        "archive_size_bytes": archive.size_bytes,
        "archive_created": archive.created,
        "artifact_count": len(envelope.artifacts),
        "proposed_manifest_count": len(envelope.proposed_manifests),
        "transfer_protocol": envelope.transfer_protocol,
        "publication_authority": envelope.publication_authority,
        "worker_write_scope": envelope.worker_write_scope,
        "direct_duckdb_write": envelope.direct_duckdb_write,
        "webdav_active_write": envelope.webdav_active_write,
        "catalog_published": False,
        "calendar_evidence_version": calendar.evidence.version,
        "calendar_exchanges": list(calendar.evidence.exchanges),
        "consumer_route_class": bundle.consumer_eligibility.route_class,
        "candidate_eligible": bundle.publication_state.candidate_eligible,
        "market_state_evidence_version": market_state_sample.version,
        "market_state_finite_sample": market_state_sample.finite_sample,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="One-shot optional AKShare fetch worker")
    parser.add_argument("--staging-root", type=Path, required=True)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--hmac-key-file", type=Path, required=True)
    parser.add_argument("--commit-sha", required=True)
    parser.add_argument("--environment-id", required=True)
    parser.add_argument("--dataset-version", required=True)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--calendar-raw", type=Path, required=True)
    parser.add_argument("--calendar-normalized", type=Path, required=True)
    parser.add_argument("--calendar-raw-uri", required=True)
    parser.add_argument("--calendar-normalized-uri", required=True)
    parser.add_argument("--market-state-raw", type=Path, required=True)
    parser.add_argument("--market-state-raw-uri", required=True)
    parser.add_argument("--task-attempts", type=int, default=5, choices=range(1, 11))
    parser.add_argument("--retry-backoff-seconds", type=float, default=2.0)
    parser.add_argument("--max-edge-attempts", type=int, default=8, choices=range(1, 9))
    parser.add_argument("--max-resolvers", type=int, default=4, choices=range(0, 5))
    args = parser.parse_args()
    report = run(args)
    atomic_replace_bytes(
        args.report,
        (json.dumps(report, ensure_ascii=True, indent=2, sort_keys=True) + "\n").encode(),
    )
    print(
        json.dumps(
            {
                "bundle_id": report["bundle_id"],
                "archive_sha256": report["archive_sha256"],
                "task_state": report["task_state"],
                "catalog_published": False,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
