#!/usr/bin/env python3
"""Acquire BaoStock qfq factors serially for an exact raw-bar universe."""

from __future__ import annotations

import argparse
import json
import signal
import time
from collections.abc import Callable
from contextlib import contextmanager, nullcontext
from pathlib import Path
from typing import Any

import duckdb

from quant_stack.data.providers.cn.live import BaoStockRequestBudget
from quant_stack.data.providers.cn.live.providers import _scoped_baostock_socket_timeout

REQUEST_INTERVAL_SECONDS = 0.05
BATCH_SIZE = 500
BATCH_INTERVAL_SECONDS = 5.0
REQUEST_TIMEOUT_SECONDS = 10.0


@contextmanager
def _request_deadline(seconds: float):
    def expired(_signum: int, _frame: object) -> None:
        raise TimeoutError(f"BaoStock factor request exceeded {seconds:g} seconds")

    previous = signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def _provider_symbol(instrument_id: str) -> str:
    venue, symbol = instrument_id.split(".", maxsplit=1)
    prefixes = {"XSHG": "sh", "XSHE": "sz"}
    try:
        return f"{prefixes[venue]}.{symbol}"
    except KeyError as exc:
        raise ValueError(f"unsupported CN venue: {venue}") from exc


def factor_request_budget(control_root: Path) -> BaoStockRequestBudget:
    return BaoStockRequestBudget(
        control_root / "baostock-request-budget.json",
        min_batch_interval_seconds=BATCH_INTERVAL_SECONDS,
    )


def load_universe(raw_bars_path: Path) -> list[dict[str, str]]:
    connection = duckdb.connect()
    try:
        ids = [
            str(row[0])
            for row in connection.execute(
                "SELECT DISTINCT instrument_id FROM read_parquet(?) ORDER BY instrument_id",
                [str(raw_bars_path)],
            ).fetchall()
        ]
    finally:
        connection.close()
    if not ids:
        raise ValueError("raw-bar universe is empty")
    return [
        {"instrument_id": instrument_id, "provider_symbol": _provider_symbol(instrument_id)}
        for instrument_id in ids
    ]


def load_plan(path: Path) -> list[dict[str, str]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != "quantstack.cn-qfq-factor-plan.v2":
        raise ValueError("unsupported qfq factor request plan")
    requests = payload.get("requests")
    if not isinstance(requests, list) or not requests:
        raise ValueError("qfq factor request plan is empty")
    return [
        {
            "instrument_id": str(item["instrument_id"]),
            "provider_symbol": str(item["provider_symbol"]),
            "start_date": str(item["start_date"]),
            "end_date": str(item["end_date"]),
        }
        for item in requests
    ]


def _rows(result: object, *, allow_empty: bool = False) -> tuple[list[str], list[dict[str, str]]]:
    code = str(getattr(result, "error_code", ""))
    if code != "0":
        raise ConnectionError(str(getattr(result, "error_msg", code)))
    fields = [str(field) for field in getattr(result, "fields", ())]
    rows: list[dict[str, str]] = []
    while result.next():  # type: ignore[attr-defined]
        values = [str(value) for value in result.get_row_data()]  # type: ignore[attr-defined]
        rows.append(dict(zip(fields, values, strict=True)))
    if not rows and not allow_empty:
        raise ValueError("BaoStock returned no adjustment factor rows")
    return fields, rows


def _write_checkpoint(
    output_path: Path,
    *,
    universe_count: int,
    start_date: str,
    end_date: str,
    requests: list[dict[str, Any]],
) -> None:
    payload = {
        "schema_version": "1.0.0",
        "provider": "baostock",
        "factor_endpoint": "query_adjust_factor",
        "universe_count": universe_count,
        "request_policy": {
            "concurrency": 1,
            "request_interval_ms": 50,
            "batch_size": 500,
            "batch_interval_seconds": 5,
        },
        "request_start": start_date,
        "request_end": end_date,
        "complete": len(requests) == universe_count,
        "requests": requests,
    }
    temporary = output_path.with_suffix(output_path.suffix + ".tmp")
    temporary.parent.mkdir(parents=True, exist_ok=True)
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    temporary.replace(output_path)


def acquire_factors(
    *,
    client: object,
    universe: list[dict[str, str]],
    output_path: Path,
    start_date: str,
    end_date: str,
    sleep: Callable[[float], None] = time.sleep,
    request_budget: BaoStockRequestBudget | None = None,
    request_timeout_seconds: float = REQUEST_TIMEOUT_SECONDS,
) -> dict[str, object]:
    existing: list[dict[str, Any]] = []
    if output_path.exists():
        payload = json.loads(output_path.read_text(encoding="utf-8"))
        if (
            payload.get("universe_count") != len(universe)
            or payload.get("request_start") != start_date
            or payload.get("request_end") != end_date
        ):
            raise ValueError("factor checkpoint scope does not match this acquisition")
        existing = list(payload.get("requests", []))
    expected_ids = [item["instrument_id"] for item in universe]
    if [str(item.get("instrument_id")) for item in existing] != expected_ids[: len(existing)]:
        raise ValueError("factor checkpoint does not match the raw-bar universe")

    requests = existing
    lock = request_budget.session_lock() if request_budget is not None else nullcontext()
    with lock:
        if request_budget is not None:
            request_budget.ensure_capacity(1 + len(universe) - len(requests))
            request_budget.reserve(1)
        login = client.login()  # type: ignore[attr-defined]
        if str(login.error_code) != "0":
            raise ConnectionError(f"BaoStock login failed: {login.error_code} {login.error_msg}")
        batch_open = False
        try:
            for index in range(len(requests), len(universe)):
                if index % BATCH_SIZE == 0:
                    if batch_open and request_budget is not None:
                        request_budget.complete_batch()
                    if request_budget is not None:
                        request_budget.wait_for_batch_slot()
                        request_budget.begin_batch(min(BATCH_SIZE, len(universe) - index))
                    elif index:
                        sleep(BATCH_INTERVAL_SECONDS)
                    batch_open = True
                elif index:
                    sleep(REQUEST_INTERVAL_SECONDS)
                instrument = universe[index]
                if request_budget is not None:
                    request_budget.reserve(1)
                with _request_deadline(request_timeout_seconds):
                    result = client.query_adjust_factor(  # type: ignore[attr-defined]
                        instrument["provider_symbol"],
                        start_date=instrument.get("start_date", start_date),
                        end_date=instrument.get("end_date", end_date),
                    )
                    fields, rows = _rows(result, allow_empty=True)
                status = "ok" if rows else "ok_no_adjustments"
                requests.append(
                    {
                        **instrument,
                        "status": status,
                        "fields": fields,
                        "rows": rows,
                        "empty_confirmations": 1 if not rows else 0,
                    }
                )
                if len(requests) % BATCH_SIZE == 0 or len(requests) == len(universe):
                    _write_checkpoint(
                        output_path,
                        universe_count=len(universe),
                        start_date=start_date,
                        end_date=end_date,
                        requests=requests,
                    )
        except Exception:
            _write_checkpoint(
                output_path,
                universe_count=len(universe),
                start_date=start_date,
                end_date=end_date,
                requests=requests,
            )
            raise
        finally:
            if batch_open and request_budget is not None:
                request_budget.complete_batch()
            client.logout()  # type: ignore[attr-defined]
    return {"request_count": len(requests), "complete": len(requests) == len(universe)}


def main() -> None:
    parser = argparse.ArgumentParser()
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--raw-bars", type=Path)
    source.add_argument("--plan", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--provider-control-root", type=Path)
    args = parser.parse_args()

    import baostock

    universe = (
        load_plan(args.plan.expanduser().resolve())
        if args.plan is not None
        else load_universe(args.raw_bars.expanduser().resolve())
    )
    with _scoped_baostock_socket_timeout(baostock, REQUEST_TIMEOUT_SECONDS):
        report = acquire_factors(
            client=baostock,
            universe=universe,
            output_path=args.output.expanduser().resolve(),
            start_date=args.start,
            end_date=args.end,
            request_budget=(
                factor_request_budget(args.provider_control_root.expanduser().resolve())
                if args.provider_control_root is not None
                else None
            ),
        )
    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    main()
