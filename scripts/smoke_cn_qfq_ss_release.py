#!/usr/bin/env python3
"""Read-only smoke test for one exact qfq Catalog and Stock Screener release."""

from __future__ import annotations

import argparse
import json
import os
import urllib.request
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from quant_stack.data.storage.atomic import atomic_replace_bytes

SYMBOLS = {
    "000002.SZ": "XSHE.000002",
    "600519.SS": "XSHG.600519",
    "000938.SZ": "XSHE.000938",
}


def _environment(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if line and not line.lstrip().startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            values[key] = value
    return values


def _json_request(
    url: str,
    *,
    payload: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = 60,
) -> Any:
    request = urllib.request.Request(
        url,
        data=None if payload is None else json.dumps(payload).encode(),
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            **(headers or {}),
        },
        method="GET" if payload is None else "POST",
    )
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(request, timeout=timeout) as response:
        if response.status != 200:
            raise RuntimeError(f"smoke request returned HTTP {response.status}: {url}")
        return json.loads(response.read())


def smoke_release(
    *,
    quant_base_url: str,
    ss_base_url: str,
    ss_environment_path: Path,
    expected_version: str,
    expected_watermark: date,
) -> dict[str, object]:
    environment = _environment(ss_environment_path)
    expected_environment = {
        "SS_QUANTSTACK_READ_MODE": "quantstack_primary",
        "SS_QUANTSTACK_PRIMARY_ENABLED": "true",
        "SS_QUANTSTACK_SHADOW_ENABLED": "false",
        "SS_QUANTSTACK_BARS_DATASET_ID": "cn_equity_daily",
        "SS_QUANTSTACK_DATASET_VERSION": expected_version,
        "SS_QUANTSTACK_ALLOW_CANDIDATE": "false",
    }
    if any(environment.get(key) != value for key, value in expected_environment.items()):
        raise ValueError("Stock Screener environment is not pinned to the exact qfq release")
    api_key = environment.get("SS_QUANTSTACK_API_KEY", "")
    if not api_key:
        raise ValueError("Stock Screener environment has no QuantStack API key")
    headers = {"X-Quant-API-Key": api_key}
    quant = quant_base_url.rstrip("/")
    ss = ss_base_url.rstrip("/")
    metadata = _json_request(
        f"{quant}/market/datasets/cn_equity_daily/metadata?version={expected_version}",
        headers=headers,
    )
    if metadata.get("dataset_version") != expected_version:
        raise ValueError("QuantStack metadata did not return the exact qfq release")

    batch: dict[str, object] = {}
    ss_results: dict[str, object] = {}
    observed_watermarks: list[date] = []
    start = expected_watermark - timedelta(days=45)
    for symbol, instrument_id in SYMBOLS.items():
        bars = _json_request(
            f"{quant}/market/batch/bars",
            headers=headers,
            payload={
                "instrument_ids": [instrument_id],
                "market": "CN",
                "frequency": "1d",
                "start": start.isoformat(),
                "end": expected_watermark.isoformat(),
                "page_size": 100,
                "dataset_id": "cn_equity_daily",
                "dataset_version": expected_version,
            },
        )
        items = bars.get("items", [])
        if not items or bars.get("dataset_version") != expected_version:
            raise ValueError(f"QuantStack Batch returned no exact-version bars for {symbol}")
        batch_last = max(date.fromisoformat(item["ts_event"][:10]) for item in items)
        history = _json_request(f"{ss}/api/v1/stocks/{symbol}/history?period=1mo")
        if not history:
            raise ValueError(f"Stock Screener history is empty for {symbol}")
        history_last = max(date.fromisoformat(item["date"]) for item in history)
        if history_last != batch_last:
            raise ValueError(f"lake/SS watermark mismatch for {symbol}")
        observed_watermarks.append(batch_last)
        technical = _json_request(f"{ss}/api/v1/technical/{symbol}/stage")
        scanner = _json_request(f"{ss}/api/v1/technical/{symbol}/minervini")
        batch[symbol] = {
            "rows": len(items),
            "last_date": batch_last.isoformat(),
            "adjustment": sorted({item.get("adjustment_semantics") for item in items}),
        }
        ss_results[symbol] = {
            "history_rows": len(history),
            "last_date": history_last.isoformat(),
            "technical": bool(technical),
            "scanner": bool(scanner),
        }
    if max(observed_watermarks) != expected_watermark:
        raise ValueError("representative Stock Screener data did not reach the release watermark")
    with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(
        ss + "/", timeout=30
    ) as frontend:
        frontend_status = frontend.status
    if frontend_status != 200:
        raise RuntimeError("Stock Screener frontend smoke failed")
    return {
        "schema_version": "quantstack.cn-qfq-ss-smoke.v1",
        "status": "passed",
        "dataset_version": expected_version,
        "watermark": expected_watermark.isoformat(),
        "read_mode": "quantstack_primary",
        "shadow_enabled": False,
        "batch": batch,
        "stock_screener": ss_results,
        "frontend_http_status": frontend_status,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quant-base-url", required=True)
    parser.add_argument("--ss-base-url", required=True)
    parser.add_argument("--ss-environment", type=Path, required=True)
    parser.add_argument("--expected-version", required=True)
    parser.add_argument("--expected-watermark", type=date.fromisoformat, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--report-gid", type=int)
    args = parser.parse_args()
    report = smoke_release(
        quant_base_url=args.quant_base_url,
        ss_base_url=args.ss_base_url,
        ss_environment_path=args.ss_environment,
        expected_version=args.expected_version,
        expected_watermark=args.expected_watermark,
    )
    atomic_replace_bytes(
        args.report,
        (json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(),
    )
    if args.report_gid is not None:
        os.chown(args.report, -1, args.report_gid)
        os.chmod(args.report, 0o640)
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
