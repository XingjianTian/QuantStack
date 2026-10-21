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
from quant_stack.data.storage import LocalFileSystemStorage
from quant_stack.data.storage.atomic import atomic_replace_bytes
from quant_stack.data.tasks import TaskState
from quant_stack.data.worker_bundle import (
    build_worker_envelope,
    create_worker_archive,
    import_worker_archive,
)
from scripts.run_us_equity_isolated_scale import SNAPSHOT_PATH, _bundle_evidence, _key

DATASET_ID = "yfinance_us_equity_daily"
RELEASE_DELAY = timedelta(hours=2)


def next_completed_session(
    watermark: date,
    now: datetime,
    *,
    release_delay: timedelta = RELEASE_DELAY,
) -> date | None:
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    end = now.astimezone(UTC).date()
    start = watermark + timedelta(days=1)
    if start > end:
        return None
    calendars = [USExchangeCalendar("XNYS"), USExchangeCalendar("XNAS")]
    session_sets = [
        tuple(item.trading_date for item in calendar.sessions("US", start, end))
        for calendar in calendars
    ]
    if session_sets[0] != session_sets[1]:
        raise ValueError("XNYS/XNAS session windows are inconsistent")
    for trading_date in session_sets[0]:
        closes = [calendar.session_close("US", trading_date) for calendar in calendars]
        if any(close is None for close in closes):
            raise ValueError("session calendar did not provide both venue closes")
        if all(now >= close + release_delay for close in closes if close is not None):
            return trading_date
    return None


def _load_watermark(
    path: Path,
    initial_watermark: date,
    snapshot_id: str,
    dataset_version_prefix: str | None = None,
) -> date:
    if not path.is_file():
        return initial_watermark
    payload = json.loads(path.read_text(encoding="utf-8"))
    expected_schema = "1.1.0" if dataset_version_prefix else "1.0.0"
    if (
        payload.get("schema_version") != expected_schema
        or payload.get("dataset_id") != DATASET_ID
        or payload.get("instrument_snapshot_id") != snapshot_id
        or (
            dataset_version_prefix is not None
            and payload.get("dataset_version_prefix") != dataset_version_prefix
        )
    ):
        raise ValueError("unsupported US daily incremental state")
    return date.fromisoformat(payload["watermark"])


def _write_watermark(
    path: Path,
    trading_date: date,
    snapshot_id: str,
    dataset_version_prefix: str | None = None,
) -> None:
    payload = {
        "schema_version": "1.1.0" if dataset_version_prefix else "1.0.0",
        "dataset_id": DATASET_ID,
        "instrument_snapshot_id": snapshot_id,
        "watermark": trading_date.isoformat(),
    }
    if dataset_version_prefix is not None:
        payload["dataset_version_prefix"] = dataset_version_prefix
    atomic_replace_bytes(
        path,
        (json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n").encode(),
    )


def _validate_imported_session(
    isolated_root: Path,
    imports: list[object],
    trading_date: date,
    expected_instruments: int,
) -> tuple[int, int]:
    storage = LocalFileSystemStorage(isolated_root)
    rows: list[dict[str, object]] = []
    for imported in imports:
        manifest = next(
            item
            for item in imported.envelope.proposed_manifests
            if item.dataset_id == DATASET_ID and item.layer is DatasetLayer.NORMALIZED
        )
        rows.extend(storage.read_dataset(manifest.storage_uri))
    dates = {item["ts_event"].date() for item in rows}
    instruments = {str(item["instrument_id"]) for item in rows}
    if (
        len(rows) != expected_instruments
        or len(instruments) != expected_instruments
        or dates != {trading_date}
    ):
        raise RuntimeError("isolated import readback did not produce the expected daily rows")
    return len(rows), len(instruments)


def run(
    args: argparse.Namespace,
    *,
    transport_factory: Callable[[str], YFinanceTransport] | None = None,
    sleeper: Callable[[float], None] = time.sleep,
) -> dict[str, object]:
    now = args.now or datetime.now(UTC)
    snapshot = load_us_instrument_snapshot(getattr(args, "snapshot", SNAPSHOT_PATH))
    if not snapshot.instruments:
        raise ValueError("daily snapshot must contain at least one equity")
    watermark = _load_watermark(args.state, args.initial_watermark, snapshot.snapshot_id)
    trading_date = next_completed_session(watermark, now)
    if trading_date is None:
        report = {
            "schema_version": "1.0.0",
            "status": "no_completed_session",
            "watermark_before": watermark.isoformat(),
            "watermark_after": watermark.isoformat(),
            "actual_requests": 0,
            "catalog_published": False,
            "production_pointer_updated": False,
        }
        atomic_replace_bytes(
            args.report,
            (json.dumps(report, ensure_ascii=True, indent=2, sort_keys=True) + "\n").encode(),
        )
        return report

    proxy_url = getattr(args, "yfinance_proxy_url", None)
    runtime_root = yfinance_runtime_root(args.yfinance_runtime_root, proxy_url=proxy_url)
    interval = resolve_yfinance_interval(runtime_root, args.request_interval_seconds)
    if args.fixture is None and transport_factory is None and not proxy_url:
        raise ValueError("live isolated daily requires --yfinance-proxy-url")
    if args.fixture is None and transport_factory is None:
        require_live_yfinance_interval(interval)
    symbols = [item.provider_symbol for item in snapshot.instruments]
    key = _key(args.hmac_key_file)
    archive_root = args.archive_root / trading_date.isoformat()
    archive_root.mkdir(parents=True, exist_ok=True)
    entries: list[dict[str, object]] = []
    archives: list[Path] = []
    actual_requests = 0

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
            start=trading_date,
            end=trading_date,
            dataset_version=f"{trading_date.isoformat()}.isolated-daily-v1.{symbol.lower()}",
            execution_time=now,
            transport=transport,
            instrument_snapshot=snapshot,
            max_attempts=1,
        )
        if execution.task.state is not TaskState.SUCCEEDED:
            raise RuntimeError(f"US isolated daily failed for {symbol}: {execution.task.state}")
        bundle_path, bundle, raw_payload = _bundle_evidence(
            args.staging_root.resolve(), args.task_state_root, execution.task.task_id
        )
        normalized = next(
            item
            for item in bundle.proposed_manifests
            if item.dataset_id == DATASET_ID and item.layer is DatasetLayer.NORMALIZED
        )
        raw_dates = {str(item["Date"])[:10] for item in raw_payload["records"]}
        if (
            normalized.row_count != 1
            or raw_dates != {trading_date.isoformat()}
            or any(
                "Close" not in item or "Adj Close" not in item
                for item in raw_payload["records"]
            )
        ):
            raise RuntimeError(f"US isolated daily coverage failed for {symbol}")
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
            output_path=archive_root / f"{index + 1:02d}-{symbol}.tar",
            hmac_key=key,
        )
        archives.append(archive.path)
        if not execution.deduplicated:
            actual_requests += 1
        entries.append(
            {
                "ordinal": index + 1,
                "symbol": symbol,
                "task_id": execution.task.task_id,
                "deduplicated": execution.deduplicated,
                "archive_name": archive.path.name,
            }
        )
        if not execution.deduplicated and index + 1 < len(symbols) and (
            args.fixture or transport_factory
        ):
            sleeper(interval)

    first_imports = [
        import_worker_archive(archive_path=path, data_root=args.isolated_root, hmac_key=key)
        for path in archives
    ]
    replay_imports = [
        import_worker_archive(archive_path=path, data_root=args.isolated_root, hmac_key=key)
        for path in archives
    ]
    if any(not item.idempotent_replay for item in replay_imports):
        raise RuntimeError("isolated import/replay contract failed")
    row_count, instrument_count = _validate_imported_session(
        args.isolated_root, first_imports, trading_date, len(symbols)
    )
    _write_watermark(args.state, trading_date, snapshot.snapshot_id)
    report = {
        "schema_version": "1.0.0",
        "status": "completed",
        "dataset_id": DATASET_ID,
        "instrument_snapshot_id": snapshot.snapshot_id,
        "session_date": trading_date.isoformat(),
        "watermark_before": watermark.isoformat(),
        "watermark_after": trading_date.isoformat(),
        "symbols": symbols,
        "actual_requests": actual_requests,
        "deduplicated_tasks": sum(bool(item["deduplicated"]) for item in entries),
        "archives": len(archives),
        "first_imports": len(first_imports),
        "new_imports": sum(not item.idempotent_replay for item in first_imports),
        "replay_imports": len(replay_imports),
        "receipts": len(
            list((args.isolated_root / "staging/worker_import_receipts").glob("*.json"))
        ),
        "rows": row_count,
        "instruments": instrument_count,
        "parallel_requests": False,
        "request_interval_seconds": interval,
        "retries": 0,
        "duplicate_keys": row_count - instrument_count,
        "price_semantics": "unadjusted_ohlcv; adjusted_close_raw_only",
        "license_scope": YFinanceProvider.license_scope,
        "catalog_published": False,
        "production_pointer_updated": False,
        "entries": entries,
    }
    atomic_replace_bytes(
        args.report,
        (json.dumps(report, ensure_ascii=True, indent=2, sort_keys=True) + "\n").encode(),
    )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Acquire and import at most one completed US equity session"
    )
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--snapshot", type=Path, default=SNAPSHOT_PATH)
    parser.add_argument("--initial-watermark", type=date.fromisoformat, required=True)
    parser.add_argument("--staging-root", type=Path, required=True)
    parser.add_argument("--task-state-root", type=Path, required=True)
    parser.add_argument("--archive-root", type=Path, required=True)
    parser.add_argument("--isolated-root", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--hmac-key-file", type=Path, required=True)
    parser.add_argument("--commit-sha", required=True)
    parser.add_argument("--environment-id", required=True)
    parser.add_argument("--request-interval-seconds", type=float)
    parser.add_argument("--yfinance-runtime-root", type=Path)
    parser.add_argument("--yfinance-proxy-url")
    parser.add_argument("--fixture", type=Path)
    parser.add_argument("--now", type=datetime.fromisoformat)
    args = parser.parse_args()
    if args.request_interval_seconds is not None and args.request_interval_seconds < 0:
        parser.error("--request-interval-seconds must be non-negative")
    if args.fixture is None and not args.yfinance_proxy_url:
        parser.error("live isolated daily requires --yfinance-proxy-url")
    if args.fixture is None and args.request_interval_seconds is not None:
        try:
            require_live_yfinance_interval(args.request_interval_seconds)
        except ValueError as exc:
            parser.error(str(exc))
    print(json.dumps(run(args), sort_keys=True))


if __name__ == "__main__":
    main()
