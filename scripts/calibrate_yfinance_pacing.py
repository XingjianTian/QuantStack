from __future__ import annotations

import argparse
import json
from collections.abc import Callable
from datetime import date
from pathlib import Path

from quant_stack.data.providers.yfinance import (
    CALIBRATION_INTERVAL_SECONDS,
    LEGACY_SAFE_FALLBACK_INTERVAL_SECONDS,
    LiveYFinanceTransport,
    YFinanceEmptyResponseError,
    YFinanceRateLimitError,
    YFinanceTransport,
    load_calibrated_interval,
    load_us_instrument_snapshot,
    require_live_yfinance_interval,
    write_pacing_policy,
    yfinance_runtime_root,
)
from quant_stack.data.storage.atomic import atomic_replace_bytes

TransportFactory = Callable[[float, str], YFinanceTransport]
MAX_CALIBRATION_STAGE_SIZE = 100
MAX_CALIBRATION_REQUESTS = 300


def _validate_calibration_envelope(
    *,
    intervals: tuple[float, ...],
    stage_size: int,
    fallback_interval_seconds: float,
    persisted_interval_seconds: float | None,
    direct_interval_seconds: float | None = None,
) -> None:
    if stage_size <= 0:
        raise ValueError("stage size must be positive")
    if stage_size * len(intervals) > MAX_CALIBRATION_REQUESTS:
        raise ValueError("calibration is limited to 300 new requests")
    if stage_size > MAX_CALIBRATION_STAGE_SIZE:
        raise ValueError("calibration stages are limited to 100 requests")

    if direct_interval_seconds is not None:
        if direct_interval_seconds not in {0.25, 0.1, 0.05}:
            raise ValueError("direct NAS pacing test must use 0.25, 0.1 or 0.05 seconds")
        if intervals != (direct_interval_seconds,):
            raise ValueError("direct NAS pacing test must contain exactly its fixed interval")
        if direct_interval_seconds == 0.1 and persisted_interval_seconds != 0.25:
            raise ValueError(
                "direct 100ms test requires an existing calibrated 0.25 second policy"
            )
        if direct_interval_seconds == 0.05 and persisted_interval_seconds != 0.1:
            raise ValueError(
                "direct 50ms test requires an existing calibrated 0.1 second policy"
            )
        return

    allowed = CALIBRATION_INTERVAL_SECONDS
    allowed_suffixes = tuple(allowed[index:] for index in range(len(allowed)))
    if intervals not in allowed_suffixes:
        raise ValueError(
            "calibration intervals must be an authorized ordered suffix of 1, 0.5, 0.25"
        )

    if intervals != allowed:
        if persisted_interval_seconds is None:
            raise ValueError("replacement calibration requires a persisted safe interval")
        if persisted_interval_seconds not in allowed:
            raise ValueError("replacement calibration must continue from a calibrated stage")
        expected_suffix = allowed[allowed.index(persisted_interval_seconds) + 1 :]
        if intervals != expected_suffix:
            raise ValueError(
                "replacement intervals must be the next authorized suffix after the persisted stage"
            )
        if fallback_interval_seconds != persisted_interval_seconds:
            raise ValueError("replacement fallback must equal the persisted safe interval")
    elif fallback_interval_seconds not in {
        LEGACY_SAFE_FALLBACK_INTERVAL_SECONDS,
        persisted_interval_seconds,
    }:
        raise ValueError("fallback must be the legacy or persisted safe interval")


def calibrate(
    args: argparse.Namespace,
    *,
    transport_factory: TransportFactory | None = None,
) -> dict[str, object]:
    direct_250ms = bool(getattr(args, "direct_250ms", False))
    direct_100ms = bool(getattr(args, "direct_100ms", False))
    direct_50ms = bool(getattr(args, "direct_50ms", False))
    if sum((direct_250ms, direct_100ms, direct_50ms)) > 1:
        raise ValueError("choose only one direct NAS pacing test")
    direct_interval_seconds = (
        0.25 if direct_250ms else 0.1 if direct_100ms else 0.05 if direct_50ms else None
    )
    intervals = (
        (direct_interval_seconds,)
        if direct_interval_seconds is not None
        else tuple(float(value) for value in args.intervals)
    )
    if args.symbol_offset < 0:
        raise ValueError("symbol offset must be non-negative")

    runtime_root = yfinance_runtime_root(
        args.yfinance_runtime_root, proxy_url=args.yfinance_proxy_url
    )
    persisted_interval = load_calibrated_interval(runtime_root)
    _validate_calibration_envelope(
        intervals=intervals,
        stage_size=args.stage_size,
        fallback_interval_seconds=float(args.fallback_interval_seconds),
        persisted_interval_seconds=persisted_interval,
        direct_interval_seconds=direct_interval_seconds,
    )
    for interval in intervals:
        require_live_yfinance_interval(interval)
    require_live_yfinance_interval(float(args.fallback_interval_seconds))

    snapshot = load_us_instrument_snapshot(args.snapshot)
    required = args.symbol_offset + args.stage_size * len(intervals)
    symbols = [item.provider_symbol for item in snapshot.instruments]
    if len(symbols) < required:
        raise ValueError(
            f"pacing calibration needs {required} unique symbols, snapshot has {len(symbols)}"
        )

    stages: list[dict[str, object]] = []
    selected: float | None = (
        persisted_interval
        if direct_interval_seconds is not None
        else float(args.fallback_interval_seconds)
    )
    status = "calibrated"
    cursor = args.symbol_offset

    for interval in intervals:
        stage_symbols = symbols[cursor : cursor + args.stage_size]
        cursor += args.stage_size
        attempted = 0
        succeeded = 0
        empty = 0
        outcome = "clean"
        for symbol in stage_symbols:
            transport = (
                transport_factory(interval, symbol)
                if transport_factory is not None
                else LiveYFinanceTransport(
                    runtime_root=args.yfinance_runtime_root,
                    proxy_url=args.yfinance_proxy_url,
                    minimum_interval_seconds=interval,
                )
            )
            attempted += 1
            try:
                rows = transport.fetch_daily_bars([symbol], args.start, args.end)
            except YFinanceRateLimitError:
                outcome = "rate_limited"
                status = "rate_limited"
                break
            except YFinanceEmptyResponseError:
                empty += 1
                if direct_interval_seconds is not None:
                    outcome = "empty_response"
                    status = "empty_response"
                    break
                continue
            if not rows:
                empty += 1
                if direct_interval_seconds is not None:
                    outcome = "empty_response"
                    status = "empty_response"
                    break
                continue
            succeeded += 1

        stages.append(
            {
                "interval_seconds": interval,
                "stage_size": len(stage_symbols),
                "attempted_requests": attempted,
                "successful_requests": succeeded,
                "empty_responses": empty,
                "outcome": outcome,
            }
        )
        if outcome != "clean":
            break
        selected = interval
        write_pacing_policy(
            runtime_root,
            selected_interval_seconds=selected,
            status="partial" if interval != intervals[-1] else "calibrated",
            stages=stages,
        )

    policy_path = (
        write_pacing_policy(
            runtime_root,
            selected_interval_seconds=selected,
            status=status,
            stages=stages,
        )
        if selected is not None
        and (direct_interval_seconds is None or status == "calibrated")
        else None
    )
    report = {
        "schema_version": "quantstack.yfinance-pacing-calibration.v1",
        "status": status,
        "snapshot_id": snapshot.snapshot_id,
        "start": args.start.isoformat(),
        "end": args.end.isoformat(),
        "strictly_serial": True,
        "provider_retries": 0,
        "symbol_offset": args.symbol_offset,
        "selected_interval_seconds": selected,
        "policy_path": str(policy_path) if policy_path is not None else None,
        "direct_250ms": direct_250ms,
        "direct_100ms": direct_100ms,
        "direct_50ms": direct_50ms,
        "direct_interval_seconds": direct_interval_seconds,
        "stages": stages,
    }
    atomic_replace_bytes(
        args.report,
        (json.dumps(report, indent=2, sort_keys=True) + "\n").encode(),
    )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Calibrate one persistent strictly serial Yahoo request interval"
    )
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--yfinance-runtime-root", type=Path, required=True)
    parser.add_argument("--yfinance-proxy-url", required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--stage-size", type=int, default=100)
    parser.add_argument("--symbol-offset", type=int, default=0)
    parser.add_argument(
        "--intervals",
        type=lambda value: tuple(float(item) for item in value.split(",")),
        default=CALIBRATION_INTERVAL_SECONDS,
    )
    parser.add_argument(
        "--fallback-interval-seconds",
        type=float,
        default=LEGACY_SAFE_FALLBACK_INTERVAL_SECONDS,
    )
    direct_group = parser.add_mutually_exclusive_group()
    direct_group.add_argument(
        "--direct-250ms",
        action="store_true",
        help="run one fixed 0.25 second stage without slower calibration stages",
    )
    direct_group.add_argument(
        "--direct-100ms",
        action="store_true",
        help="run one fixed 0.1 second stage without slower calibration stages",
    )
    direct_group.add_argument(
        "--direct-50ms",
        action="store_true",
        help="run one fixed 0.05 second stage after a clean 100ms calibration",
    )
    args = parser.parse_args()
    print(json.dumps(calibrate(args), sort_keys=True))


if __name__ == "__main__":
    main()
