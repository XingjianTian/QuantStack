from __future__ import annotations

from datetime import date

from scripts.sync_cn_ss_daily_snapshot import sync_daily_snapshot


def test_current_snapshot_is_a_zero_scan_noop() -> None:
    calls: list[str] = []

    def request_json(_url: str, **_kwargs):
        return {
            "scan_id": "scan-current",
            "freshness": {
                "scan_as_of_date": "2026-08-28",
                "snapshot_as_of_date": "2026-08-28",
                "analytics_version": "quantstack.scan-analytics.v1",
            },
        }

    def publish_scan(**_kwargs):
        calls.append("publish")
        return {}

    report = sync_daily_snapshot(
        ss_base_url="http://ss",
        dataset_version="daily-through-20260828.qfq-session-v1",
        expected_watermark=date(2026, 8, 28),
        request_json=request_json,
        publish_scan=publish_scan,
    )

    assert report["status"] == "current"
    assert report["scan_id"] == "scan-current"
    assert calls == []


def test_stale_snapshot_publishes_once_then_verifies() -> None:
    calls: list[object] = []

    def request_json(_url: str, **_kwargs):
        return {
            "scan_id": "scan-old",
            "freshness": {
                "scan_as_of_date": "2026-08-27",
                "snapshot_as_of_date": "2026-08-27",
            },
        }

    def publish_scan(**kwargs):
        calls.append(("publish", kwargs["expected_watermark"]))
        return {
            "scan_id": "scan-new",
            "total_stocks": 5_207,
            "completed_stocks": 5_207,
        }

    def verify_snapshot(**kwargs):
        calls.append(("verify", kwargs["scan_id"]))
        return {"status": "passed", "scan_id": kwargs["scan_id"]}

    report = sync_daily_snapshot(
        ss_base_url="http://ss",
        dataset_version="daily-through-20260828.qfq-session-v1",
        expected_watermark=date(2026, 8, 28),
        request_json=request_json,
        publish_scan=publish_scan,
        verify_snapshot=verify_snapshot,
    )

    assert report["status"] == "published"
    assert report["scan_id"] == "scan-new"
    assert calls == [("publish", date(2026, 8, 28)), ("verify", "scan-new")]
