#!/usr/bin/env python3
"""Converge a CN or US SS Daily Snapshot from the matching Catalog release."""

from __future__ import annotations

import time
from collections.abc import Callable
from datetime import date
from typing import Any

from scripts.publish_ss_daily_scan import _json_request, publish_daily_scan
from scripts.verify_ss_daily_snapshot import verify_daily_snapshot

DEFAULT_ANALYTICS_VERSION = "quantstack.scan-analytics.v1"


def sync_daily_snapshot(
    *,
    market: str,
    ss_base_url: str,
    dataset_version: str,
    expected_watermark: date,
    analytics_version: str = DEFAULT_ANALYTICS_VERSION,
    request_json: Callable[..., dict[str, Any]] = _json_request,
    publish_scan: Callable[..., dict[str, Any]] = publish_daily_scan,
    verify_snapshot: Callable[..., dict[str, Any]] = verify_daily_snapshot,
    readiness_timeout_seconds: int = 900,
    readiness_poll_seconds: float = 10,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    code = market.upper()
    snapshot = request_json(
        f"{ss_base_url.rstrip('/')}/api/v1/market-scan/daily-snapshot?market={code}",
        timeout=120,
    )
    freshness = snapshot.get("freshness") or {}
    expected = expected_watermark.isoformat()
    if (
        freshness.get("scan_as_of_date") == expected
        and freshness.get("snapshot_as_of_date") == expected
        and freshness.get("analytics_version") == analytics_version
        and snapshot.get("scan_id")
    ):
        return {
            "schema_version": "quantstack.ss-daily-snapshot-sync.v2",
            "status": "current",
            "market": code,
            "watermark": expected,
            "dataset_version": dataset_version,
            "analytics_version": analytics_version,
            "scan_id": snapshot["scan_id"],
        }

    published = publish_scan(
        market=code,
        ss_base_url=ss_base_url,
        expected_version=dataset_version,
        expected_watermark=expected_watermark,
    )
    scan_id = str(published.get("scan_id") or "")
    if not scan_id:
        raise RuntimeError("Stock Screener daily scan returned no scan_id")
    deadline = time.monotonic() + readiness_timeout_seconds
    while True:
        try:
            verified = verify_snapshot(
                market=code,
                ss_base_url=ss_base_url,
                scan_id=scan_id,
                expected_watermark=expected_watermark,
                analytics_version=analytics_version,
                request_json=request_json,
            )
            break
        except ValueError:
            if time.monotonic() >= deadline:
                raise
            sleep(readiness_poll_seconds)
    return {
        "schema_version": "quantstack.ss-daily-snapshot-sync.v2",
        "status": "published",
        "market": code,
        "watermark": expected,
        "dataset_version": dataset_version,
        "analytics_version": analytics_version,
        "scan_id": scan_id,
        "total_stocks": published.get("total_stocks"),
        "completed_stocks": published.get("completed_stocks"),
        "snapshot": verified,
    }
