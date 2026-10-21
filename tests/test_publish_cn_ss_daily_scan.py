from __future__ import annotations

import urllib.error
from datetime import date

import pytest

from scripts.publish_cn_ss_daily_scan import publish_daily_scan
from scripts.verify_cn_ss_daily_snapshot import verify_daily_snapshot


def test_daily_scan_is_idempotent_and_publishes_expected_snapshot() -> None:
    calls: list[tuple[str, dict | None]] = []

    def request(url: str, *, payload=None, timeout=60):
        del timeout
        calls.append((url, payload))
        if url.endswith("/api/v1/scans"):
            return {"scan_id": "scan-25", "status": "queued"}
        if url.endswith("/api/v1/scans/scan-25/status"):
            return {
                "scan_id": "scan-25",
                "status": "completed",
                "total_stocks": 5206,
                "completed_stocks": 5206,
                "passed_stocks": 31,
                "warnings": [{"expected_dates": {"CN": "2026-08-25"}}],
            }
        raise AssertionError(url)

    report = publish_daily_scan(
        ss_base_url="http://ss",
        expected_version="daily-through-20260825.qfq-release-v1",
        expected_watermark=date(2026, 8, 25),
        request_json=request,
        sleep=lambda _seconds: None,
    )
    assert report["status"] == "completed"
    assert report["scan_id"] == "scan-25"
    assert calls[0][1]["idempotency_key"] == "quantstack-cn-qfq-daily-20260825"
    assert calls[0][1]["universe_def"] == {"type": "market", "market": "CN"}


def test_daily_snapshot_readback_requires_the_exact_completed_scan() -> None:
    def request(url: str, *, timeout=60):
        del url, timeout
        return {
            "scan_id": "scan-25",
            "freshness": {
                "scan_as_of_date": "2026-08-25",
                "snapshot_as_of_date": "2026-08-25",
                "analytics_version": "quantstack.scan-analytics.v1",
            },
        }

    report = verify_daily_snapshot(
        ss_base_url="http://ss",
        scan_id="scan-25",
        expected_watermark=date(2026, 8, 25),
        request_json=request,
    )
    assert report["status"] == "passed"
    assert report["snapshot_as_of_date"] == "2026-08-25"


def test_daily_snapshot_readback_rejects_a_stale_homepage_anchor() -> None:
    def request(url: str, *, timeout=60):
        del url, timeout
        return {
            "scan_id": "scan-24",
            "freshness": {
                "scan_as_of_date": "2026-08-24",
                "snapshot_as_of_date": "2026-08-24",
            },
        }

    with pytest.raises(ValueError, match="did not publish the analytics-ready scan"):
        verify_daily_snapshot(
            ss_base_url="http://ss",
            scan_id="scan-25",
            expected_watermark=date(2026, 8, 25),
            request_json=request,
        )


def test_daily_scan_rejects_a_completed_scan_from_another_watermark() -> None:
    def request(url: str, *, payload=None, timeout=60):
        del payload, timeout
        if url.endswith("/api/v1/scans"):
            return {"scan_id": "scan-old", "status": "completed"}
        return {
            "scan_id": "scan-old",
            "status": "completed",
            "warnings": [{"expected_dates": {"CN": "2026-08-24"}}],
        }

    with pytest.raises(ValueError, match="scan date mismatch"):
        publish_daily_scan(
            ss_base_url="http://ss",
            expected_version="daily-through-20260825.qfq-release-v1",
            expected_watermark=date(2026, 8, 25),
            request_json=request,
            sleep=lambda _seconds: None,
        )


def test_daily_scan_tolerates_a_brief_status_endpoint_restart() -> None:
    responses: list[dict[str, object] | BaseException] = [
        {"scan_id": "scan-25", "status": "running"},
        urllib.error.URLError("connection refused"),
        {
            "scan_id": "scan-25",
            "status": "completed",
            "total_stocks": 1,
            "completed_stocks": 1,
            "passed_stocks": 1,
            "warnings": [{"expected_dates": {"CN": "2026-08-25"}}],
        },
    ]
    sleeps: list[float] = []

    def request(*_args: object, **_kwargs: object) -> dict[str, object]:
        response = responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response

    report = publish_daily_scan(
        ss_base_url="http://ss",
        expected_version="daily-through-20260825.qfq-release-v1",
        expected_watermark=date(2026, 8, 25),
        poll_interval_seconds=0.25,
        request_json=request,
        sleep=sleeps.append,
    )

    assert report["status"] == "completed"
    assert sleeps == [0.25]
