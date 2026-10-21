from __future__ import annotations

import json
import runpy
import shutil
import time
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts.promote_us_equity_daily_total_return import run as run_us_promotion
from scripts.publish_ss_daily_scan import publish_daily_scan
from scripts.sync_ss_daily_snapshot import sync_daily_snapshot
from scripts.verify_ss_runtime_parity import evaluate_runtime_parity

ROOT = Path(__file__).resolve().parents[1]
ANALYTICS_VERSION = "quantstack.scan-analytics.v1"


def test_us_and_cn_use_one_publisher_with_market_specific_identity() -> None:
    submissions: dict[str, dict] = {}

    def request(url: str, *, payload=None, timeout=60):
        del timeout
        if url.endswith("/api/v1/scans"):
            code = payload["universe_def"]["market"]
            submissions[code] = payload
            return {"scan_id": f"scan-{code.lower()}", "status": "queued"}
        code = "US" if url.endswith("scan-us/status") else "CN"
        return {
            "status": "completed",
            "warnings": [{"expected_dates": {code: "2026-09-04"}}],
        }

    for market in ("CN", "US"):
        result = publish_daily_scan(
            market=market,
            ss_base_url="http://ss",
            expected_version=f"{market.lower()}-v1",
            expected_watermark=date(2026, 9, 4),
            request_json=request,
            sleep=lambda _seconds: None,
        )
        assert result["market"] == market

    assert submissions["CN"]["criteria"]["benchmark"] == "000300.SS"
    assert submissions["US"]["criteria"]["benchmark"] == "SPY"
    assert submissions["CN"]["screeners"] == submissions["US"]["screeners"]
    assert submissions["CN"]["criteria"]["custom_filters"] == (
        submissions["US"]["criteria"]["custom_filters"]
    )
    assert submissions["US"]["idempotency_key"] == (
        "quantstack-us-total-return-daily-20260904"
    )


def test_snapshot_sync_waits_for_analytics_ready_instead_of_exposing_partial_scan() -> None:
    attempts = 0
    sleeps: list[float] = []

    def request(_url: str, **_kwargs):
        return {"scan_id": "old", "freshness": {}}

    def publish(**_kwargs):
        return {"scan_id": "scan-us", "total_stocks": 4_721}

    def verify(**kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ValueError("analytics pending")
        return {
            "status": "passed",
            "scan_id": kwargs["scan_id"],
            "analytics_version": kwargs["analytics_version"],
        }

    report = sync_daily_snapshot(
        market="US",
        ss_base_url="http://ss",
        dataset_version="us-v1",
        expected_watermark=date(2026, 9, 4),
        request_json=request,
        publish_scan=publish,
        verify_snapshot=verify,
        readiness_poll_seconds=0.25,
        sleep=sleeps.append,
    )

    assert report["status"] == "published"
    assert attempts == 2
    assert sleeps == [0.25]


@pytest.mark.parametrize("market", ["CN", "US"])
def test_uncached_fixture_snapshot_readback_is_under_one_second(market: str) -> None:
    calls = 0

    def request(_url: str, **_kwargs):
        nonlocal calls
        calls += 1
        return {
            "scan_id": f"scan-{market.lower()}",
            "freshness": {
                "scan_as_of_date": "2026-09-04",
                "snapshot_as_of_date": "2026-09-04",
                "analytics_version": ANALYTICS_VERSION,
            },
        }

    started = time.perf_counter()
    report = sync_daily_snapshot(
        market=market,
        ss_base_url="http://fixture-without-cache",
        dataset_version=f"{market.lower()}-v1",
        expected_watermark=date(2026, 9, 4),
        request_json=request,
        publish_scan=lambda **_kwargs: pytest.fail("current snapshot republished"),
    )
    elapsed_ms = (time.perf_counter() - started) * 1_000

    assert report["status"] == "current"
    assert calls == 1
    assert elapsed_ms < 1_000


def test_us_noop_retries_snapshot_sync_without_provider_or_republication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = tmp_path / "state.json"
    state.write_text(
        json.dumps(
            {
                "snapshot_id": "snapshot-v1",
                "watermark": "2026-09-04",
                "dataset_version": "us-v1",
            }
        ),
        encoding="utf-8",
    )
    config = {
        "snapshot": str(tmp_path / "snapshot.json"),
        "state": str(state),
        "initial_watermark": "2026-09-03",
        "ss_base_url": "http://ss",
        "ss_report_root": str(tmp_path / "reports"),
    }
    monkeypatch.setattr(
        "scripts.promote_us_equity_daily_total_return.promote",
        lambda _config, now=None: {
            "status": "noop",
            "watermark": "2026-09-04",
            "provider_requested": False,
        },
    )
    monkeypatch.setattr(
        "scripts.promote_us_equity_daily_total_return.load_us_instrument_snapshot",
        lambda _path: SimpleNamespace(snapshot_id="snapshot-v1"),
    )
    calls: list[dict] = []
    result = run_us_promotion(
        config,
        sync_snapshot=lambda **kwargs: calls.append(kwargs) or {"status": "current"},
    )

    assert result["provider_requested"] is False
    assert result["ss_daily_snapshot"]["status"] == "current"
    assert calls == [
        {
            "market": "US",
            "ss_base_url": "http://ss",
            "dataset_version": "us-v1",
            "expected_watermark": date(2026, 9, 4),
        }
    ]


def _inspect_record(role: str, *, ref: str, image_id: str, running: bool = True) -> dict:
    return {
        "Config": {"Image": ref, "Labels": {"com.docker.compose.service": role}},
        "Image": image_id,
        "State": {"Running": running},
    }


def test_runtime_parity_requires_backend_and_both_market_workers_on_one_image() -> None:
    ref = "personal/stock-screener-backend-downstream:1.4.0-quant-runtime.16"
    records = [
        _inspect_record(role, ref=ref, image_id="sha256:runtime11")
        for role in (
            "backend",
            "celery-general",
            "celery-userscans-cn",
            "celery-userscans-us",
        )
    ]
    assert evaluate_runtime_parity(records, expected_image=ref)["status"] == "passed"
    records[-1]["Image"] = "sha256:old"
    with pytest.raises(ValueError, match="image drift"):
        evaluate_runtime_parity(records, expected_image=ref)


def test_snapshot_readiness_patcher_applies_to_locked_upstream_and_fails_closed(
    tmp_path: Path,
) -> None:
    upstream = ROOT.parent / "upstream" / "stock-screener" / "backend"
    if not upstream.is_dir():
        pytest.skip("optional pinned Stock Screener source checkout is not installed")
    target = tmp_path / "backend"
    for relative in (
        "app/models/scan_result.py",
        "app/schemas/market_scan.py",
        "app/services/daily_snapshot_service.py",
        "app/api/v1/market_scan.py",
        "app/tasks/scan_tasks.py",
    ):
        destination = target / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(upstream / relative, destination)

    patcher = runpy.run_path(
        str(ROOT / "docker/stock-screener/patches/patch_snapshot_analytics_readiness.py")
    )["main"]
    patcher(target)
    assert "analytics_version = Column" in (
        target / "app/models/scan_result.py"
    ).read_text(encoding="utf-8")
    assert "required_version" in (
        target / "app/services/daily_snapshot_service.py"
    ).read_text(encoding="utf-8")
    patched_schema = (target / "app/schemas/market_scan.py").read_text(encoding="utf-8")
    assert "analytics_version: Optional[str] = None" in patched_schema
    assert "analytics_completed_at: Optional[str] = None" in patched_schema
    assert "autoretry_for=(Exception,)" in (
        target / "app/tasks/scan_tasks.py"
    ).read_text(encoding="utf-8")
    patched_route = (target / "app/api/v1/market_scan.py").read_text(encoding="utf-8")
    assert "load_latest_snapshot_publication_json" in patched_route
    assert patched_route.index("if required_version:") < patched_route.index(
        "scan = latest_completed_scan(db, code)"
    )
    assert "status_code=503" in patched_route
    assert "get_or_build_daily_snapshot_payload" in patched_route
    with pytest.raises(SystemExit, match="anchor drifted"):
        patcher(target)
