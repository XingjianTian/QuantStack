from __future__ import annotations

import argparse
import json
import time
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from quant_stack.data.calendars import USExchangeCalendar
from quant_stack.data.models import DatasetLayer
from quant_stack.data.providers.yfinance import (
    LiveYFinanceTransport,
    ReplayYFinanceTransport,
    YFinanceProvider,
    YFinanceTransport,
    load_us_instrument_snapshot,
    require_live_yfinance_interval,
    resolve_yfinance_interval,
    run_yfinance_daily_task,
    yfinance_runtime_root,
)
from quant_stack.data.publication import PublicationBundle
from quant_stack.data.storage.atomic import atomic_replace_bytes
from quant_stack.data.tasks import FileTaskStore, TaskState
from quant_stack.data.worker_bundle import build_worker_envelope, create_worker_archive

SNAPSHOT_PATH = (
    Path(__file__).parents[1]
    / "src/quant_stack/data/providers/yfinance/us_instruments_research_20_v1.json"
)
SESSION_COUNT = 20


def completed_sessions(end: date) -> tuple[date, ...]:
    start = end - timedelta(days=45)
    xnys = tuple(
        item.trading_date for item in USExchangeCalendar("XNYS").sessions("US", start, end)
    )
    xnas = tuple(
        item.trading_date for item in USExchangeCalendar("XNAS").sessions("US", start, end)
    )
    if xnys != xnas or len(xnys) < SESSION_COUNT:
        raise ValueError("XNYS/XNAS session window is incomplete or inconsistent")
    return xnys[-SESSION_COUNT:]


def _key(path: Path) -> bytes:
    payload = path.read_bytes()
    if len(payload) < 32:
        raise ValueError("worker HMAC key file must contain at least 32 bytes")
    return payload


def _bundle_evidence(
    staging_root: Path, state_root: Path, task_id: str
) -> tuple[Path, PublicationBundle, dict[str, object]]:
    persisted = FileTaskStore(state_root).load(task_id)
    if persisted is None or persisted.publication_bundle_uri is None:
        raise RuntimeError(f"missing persisted publication bundle for {task_id}")
    bundle_path = staging_root / persisted.publication_bundle_uri.removeprefix("quant-data://")
    bundle = PublicationBundle.model_validate_json(bundle_path.read_text(encoding="utf-8"))
    raw = next(item for item in bundle.artifacts if item.kind == "raw")
    raw_payload = json.loads(
        (staging_root / raw.uri.removeprefix("quant-data://")).read_text(encoding="utf-8")
    )
    return bundle_path, bundle, raw_payload


def run(
    args: argparse.Namespace,
    *,
    transport_factory: Callable[[str], YFinanceTransport] | None = None,
    sleeper: Callable[[float], None] = time.sleep,
) -> dict[str, object]:
    proxy_url = getattr(args, "yfinance_proxy_url", None)
    runtime_root = yfinance_runtime_root(args.yfinance_runtime_root, proxy_url=proxy_url)
    interval = resolve_yfinance_interval(runtime_root, args.request_interval_seconds)
    if args.fixture is None and transport_factory is None and not proxy_url:
        raise ValueError("live isolated scale requires --yfinance-proxy-url")
    if args.fixture is None and transport_factory is None:
        require_live_yfinance_interval(interval)
    snapshot = load_us_instrument_snapshot(SNAPSHOT_PATH)
    venue_counts = {
        venue: sum(item.canonical_venue == venue for item in snapshot.instruments)
        for venue in ("XNAS", "XNYS")
    }
    if len(snapshot.instruments) != 20 or venue_counts != {"XNAS": 10, "XNYS": 10}:
        raise ValueError("scale snapshot must contain exactly 10 XNAS and 10 XNYS equities")
    sessions = completed_sessions(args.end)
    full_symbol_order = [item.provider_symbol for item in snapshot.instruments]
    symbols = full_symbol_order[:1] if args.canary_only else full_symbol_order
    execution_time = (
        datetime.combine(args.end + timedelta(days=1), datetime.min.time(), tzinfo=UTC)
        if args.fixture
        else datetime.now(UTC)
    )
    key = _key(args.hmac_key_file)
    entries: list[dict[str, object]] = []
    actual_requests = 0
    args.archive_root.mkdir(parents=True, exist_ok=True)

    for index, symbol in enumerate(symbols):
        transport = (
            transport_factory(symbol)
            if transport_factory
            else ReplayYFinanceTransport(args.fixture)
            if args.fixture
            else LiveYFinanceTransport(
                runtime_root=args.yfinance_runtime_root,
                proxy_url=proxy_url,
                minimum_interval_seconds=interval,
            )
        )
        execution = run_yfinance_daily_task(
            args.staging_root,
            args.task_state_root,
            symbols=[symbol],
            start=sessions[0],
            end=sessions[-1],
            dataset_version=f"{args.dataset_version}.{symbol.lower()}",
            execution_time=execution_time,
            transport=transport,
            instrument_snapshot=snapshot,
        )
        if execution.task.state is not TaskState.SUCCEEDED:
            raise RuntimeError(f"US isolated scale failed for {symbol}: {execution.task.state}")
        bundle_path, bundle, raw_payload = _bundle_evidence(
            args.staging_root.resolve(), args.task_state_root, execution.task.task_id
        )
        normalized_manifest = next(
            item
            for item in bundle.proposed_manifests
            if item.layer is DatasetLayer.NORMALIZED
            and item.dataset_id == "yfinance_us_equity_daily"
        )
        row_dates = sorted({str(item["Date"])[:10] for item in raw_payload["records"]})
        expected_dates = [item.isoformat() for item in sessions]
        if normalized_manifest.row_count != SESSION_COUNT or row_dates != expected_dates:
            raise RuntimeError(f"US isolated scale session coverage failed for {symbol}")
        envelope = build_worker_envelope(
            data_root=args.staging_root.resolve(),
            publication_bundle_path=bundle_path,
            source_commit=args.commit_sha,
            environment_id=args.environment_id,
            provider_contract=YFinanceProvider(instrument_snapshot=snapshot).runtime_contract,
            provider_transport_audit=raw_payload["transport_audit"],
            created_at=bundle.created_at,
        )
        archive = create_worker_archive(
            data_root=args.staging_root.resolve(),
            envelope=envelope,
            output_path=args.archive_root / f"{index + 1:02d}-{symbol}.tar",
            hmac_key=key,
        )
        if not execution.deduplicated:
            actual_requests += 1
        entries.append(
            {
                "ordinal": index + 1,
                "symbol": symbol,
                "venue": snapshot.instruments[index].canonical_venue,
                "task_id": execution.task.task_id,
                "task_state": execution.task.state,
                "deduplicated": execution.deduplicated,
                "records_received": normalized_manifest.row_count,
                "completed_requests": raw_payload["transport_audit"]["completed_requests"],
                "parallel_requests": raw_payload["transport_audit"]["parallel_requests"],
                "bundle_id": envelope.bundle_id,
                "archive_name": archive.path.name,
                "archive_size_bytes": archive.size_bytes,
                "archive_created": archive.created,
            }
        )
        if (
            not execution.deduplicated
            and index + 1 < len(symbols)
            and (args.fixture or transport_factory)
        ):
            sleeper(interval)

    report = {
        "schema_version": "1.0.0",
        "generated_at": datetime.now(UTC).isoformat(),
        "mode": "replay" if args.fixture else "live",
        "run_scope": "canary" if args.canary_only else "full_campaign",
        "dataset_id": "yfinance_us_equity_daily",
        "dataset_version": args.dataset_version,
        "instrument_snapshot_id": snapshot.snapshot_id,
        "symbols": symbols,
        "venue_counts": venue_counts,
        "session_dates": [item.isoformat() for item in sessions],
        "start": sessions[0].isoformat(),
        "end": sessions[-1].isoformat(),
        "request_interval_seconds": interval,
        "network_route": "explicit_proxy" if proxy_url else "fixture",
        "parallel_requests": False,
        "actual_requests": actual_requests,
        "deduplicated_tasks": sum(bool(item["deduplicated"]) for item in entries),
        "records_received": sum(int(item["records_received"]) for item in entries),
        "license_scope": YFinanceProvider.license_scope,
        "price_semantics": "unadjusted_ohlcv; adjusted_close_raw_only",
        "entries": entries,
        "all_succeeded": len(entries) == len(symbols),
        "full_campaign_complete": not args.canary_only and len(entries) == 20,
        "catalog_published": False,
        "production_pointer_updated": False,
    }
    atomic_replace_bytes(
        args.report,
        (json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(),
    )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build the fixed 20-symbol by 20-session US equity isolated archives"
    )
    parser.add_argument("--staging-root", type=Path, required=True)
    parser.add_argument("--task-state-root", type=Path, required=True)
    parser.add_argument("--archive-root", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--hmac-key-file", type=Path, required=True)
    parser.add_argument("--commit-sha", required=True)
    parser.add_argument("--environment-id", required=True)
    parser.add_argument("--dataset-version", required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--request-interval-seconds", type=float)
    parser.add_argument("--yfinance-runtime-root", type=Path)
    parser.add_argument("--yfinance-proxy-url")
    parser.add_argument("--canary-only", action="store_true")
    parser.add_argument("--fixture", type=Path)
    args = parser.parse_args()
    if args.request_interval_seconds is not None and args.request_interval_seconds < 0:
        parser.error("--request-interval-seconds must be non-negative")
    if args.fixture is None and not args.yfinance_proxy_url:
        parser.error("live isolated scale requires --yfinance-proxy-url")
    if args.fixture is None and args.request_interval_seconds is not None:
        try:
            require_live_yfinance_interval(args.request_interval_seconds)
        except ValueError as exc:
            parser.error(str(exc))
    report = run(args)
    print(
        json.dumps(
            {
                "all_succeeded": report["all_succeeded"],
                "full_campaign_complete": report["full_campaign_complete"],
                "actual_requests": report["actual_requests"],
                "records_received": report["records_received"],
                "catalog_published": False,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
