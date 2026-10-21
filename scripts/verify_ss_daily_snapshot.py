#!/usr/bin/env python3
"""Verify that Daily Snapshot exposes one analytics-ready completed scan."""

from __future__ import annotations

from datetime import date
from typing import Any

from scripts.publish_ss_daily_scan import _json_request


def verify_daily_snapshot(
    *,
    market: str,
    ss_base_url: str,
    scan_id: str,
    expected_watermark: date,
    analytics_version: str,
    request_json=_json_request,
) -> dict[str, Any]:
    code = market.upper()
    snapshot = request_json(
        f"{ss_base_url.rstrip('/')}/api/v1/market-scan/daily-snapshot?market={code}",
        timeout=120,
    )
    freshness = snapshot.get("freshness")
    if not isinstance(freshness, dict):
        raise ValueError("Daily Snapshot returned no freshness object")
    observed = {
        "scan_id": snapshot.get("scan_id"),
        "scan_as_of_date": freshness.get("scan_as_of_date"),
        "snapshot_as_of_date": freshness.get("snapshot_as_of_date"),
        "analytics_version": freshness.get("analytics_version"),
    }
    expected = expected_watermark.isoformat()
    required = {
        "scan_id": scan_id,
        "scan_as_of_date": expected,
        "snapshot_as_of_date": expected,
        "analytics_version": analytics_version,
    }
    if observed != required:
        raise ValueError(f"Daily Snapshot did not publish the analytics-ready scan: {observed}")
    return {
        "schema_version": "quantstack.ss-daily-snapshot-readback.v2",
        "status": "passed",
        "market": code,
        "watermark": expected,
        **observed,
    }
