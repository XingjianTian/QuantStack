#!/usr/bin/env python3
"""Publish one idempotent SS daily scan for CN or US Catalog data."""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from datetime import date
from typing import Any

SCREENERS = ["minervini", "canslim", "ipo", "custom", "volume_breakthrough"]
MARKET_CONFIG = {
    "CN": {"dataset": "qfq", "benchmark": "000300.SS"},
    "US": {"dataset": "total-return", "benchmark": "SPY"},
}
CUSTOM_FILTERS = {
    "price_min": 20,
    "price_max": 500,
    "rs_rating_min": 75,
    "volume_min": 1_000_000,
    "market_cap_min": 1_000_000_000,
    "eps_growth_min": 20,
    "sales_growth_min": 15,
    "ma_alignment": True,
    "min_score": 70,
}


def _json_request(
    url: str,
    *,
    payload: dict[str, Any] | None = None,
    timeout: float = 60,
) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        data=None if payload is None else json.dumps(payload).encode(),
        headers={"Accept": "application/json", "Content-Type": "application/json"},
        method="GET" if payload is None else "POST",
    )
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(request, timeout=timeout) as response:
            body = response.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")
        raise RuntimeError(f"Stock Screener returned HTTP {exc.code}: {detail}") from exc
    result = json.loads(body)
    if not isinstance(result, dict):
        raise ValueError("Stock Screener returned a non-object JSON response")
    return result


def _expected_scan_date(status: dict[str, Any], market: str) -> str | None:
    for warning in status.get("warnings") or []:
        if not isinstance(warning, dict):
            continue
        expected = warning.get("expected_dates")
        if isinstance(expected, dict) and expected.get(market):
            return str(expected[market])
    return None


def publish_daily_scan(
    *,
    market: str,
    ss_base_url: str,
    expected_version: str,
    expected_watermark: date,
    timeout_seconds: int = 7_200,
    poll_interval_seconds: float = 30,
    request_json: Callable[..., dict[str, Any]] = _json_request,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    code = market.upper()
    if code not in MARKET_CONFIG:
        raise ValueError(f"unsupported SS market: {market}")
    settings = MARKET_CONFIG[code]
    criteria = {
        "include_vcp": True,
        "custom_filters": dict(CUSTOM_FILTERS),
        "benchmark": settings["benchmark"],
    }
    token = expected_watermark.strftime("%Y%m%d")
    idempotency_key = f"quantstack-{code.lower()}-{settings['dataset']}-daily-{token}"
    ss = ss_base_url.rstrip("/")
    created = request_json(
        f"{ss}/api/v1/scans",
        payload={
            "universe_def": {"type": "market", "market": code},
            "criteria": criteria,
            "screeners": SCREENERS,
            "composite_method": "weighted_average",
            "idempotency_key": idempotency_key,
        },
        timeout=120,
    )
    scan_id = str(created.get("scan_id") or "")
    if not scan_id:
        raise ValueError("Stock Screener scan submission returned no scan_id")

    deadline = time.monotonic() + timeout_seconds
    transient_status_errors = 0
    while True:
        try:
            status = request_json(f"{ss}/api/v1/scans/{scan_id}/status", timeout=120)
            transient_status_errors = 0
        except (OSError, urllib.error.URLError):
            transient_status_errors += 1
            if transient_status_errors >= 3 or time.monotonic() >= deadline:
                raise
            sleep(poll_interval_seconds)
            continue
        state = str(status.get("status") or "").lower()
        if state == "completed":
            break
        if state in {"cancelled", "failed"}:
            raise RuntimeError(f"Stock Screener scan {scan_id} ended as {state}")
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Stock Screener scan {scan_id} did not complete in time")
        sleep(poll_interval_seconds)

    expected = expected_watermark.isoformat()
    scan_date = _expected_scan_date(status, code)
    if scan_date != expected:
        raise ValueError(
            f"Stock Screener scan date mismatch: expected {expected}, observed {scan_date}"
        )
    return {
        "schema_version": "quantstack.ss-daily-scan.v2",
        "status": "completed",
        "market": code,
        "dataset_version": expected_version,
        "watermark": expected,
        "idempotency_key": idempotency_key,
        "scan_id": scan_id,
        "submission_status": created.get("status"),
        "total_stocks": status.get("total_stocks"),
        "completed_stocks": status.get("completed_stocks"),
        "passed_stocks": status.get("passed_stocks"),
    }
