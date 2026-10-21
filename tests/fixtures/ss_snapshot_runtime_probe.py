"""Run the patched Snapshot publication path in a fresh real app process."""

from __future__ import annotations

import asyncio
import importlib
import io
import json
import os
import subprocess
import sys
from contextlib import redirect_stdout
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pandas as pd


def _frame(anchor: str) -> pd.DataFrame:
    index = pd.bdate_range(end=anchor, periods=280)
    return pd.DataFrame(
        {
            "Open": [100.0 + offset / 10 for offset in range(len(index))],
            "High": [101.0 + offset / 10 for offset in range(len(index))],
            "Low": [99.0 + offset / 10 for offset in range(len(index))],
            "Close": [100.0 + offset / 10 for offset in range(len(index))],
            "Volume": [2_000_000] * len(index),
        },
        index=index,
    )


class _LocalFrames:
    max_cache_entries = 100

    def __init__(self, anchor: str) -> None:
        self.anchor = anchor

    def get_batch_price_history(self, symbols, *, start, end):
        del start, end
        frame = _frame(self.anchor)
        return {symbol: SimpleNamespace(frame=frame.copy()) for symbol in symbols}


def main(market: str) -> None:
    market = market.upper()
    anchor = "2026-09-04"

    # Enter through the production CLI initializer so the compatibility layer
    # is installed before the locked-upstream service container is constructed.
    import reconcile_scan_analytics as cli

    cli.initialize_runtime_services()
    import app.models  # noqa: F401
    import quantstack_scan_analytics as analytics
    from app.database import Base, SessionLocal, engine
    from app.domain.markets.catalog import get_market_catalog
    from app.models.daily_snapshot_publication import (
        DailySnapshotPublication,
        DailySnapshotPublicationInput,
    )
    from app.models.scan_result import Scan, ScanResult
    from app.models.stock_universe import StockUniverse
    from app.services import daily_snapshot_service
    from app.wiring.bootstrap import clear_runtime_services, get_group_rank_service
    from daily_snapshot_publication import (
        _payload_digest,
        load_latest_snapshot_publication_json,
    )

    assert get_market_catalog().get(market).capabilities.group_rankings is True
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)

    scan_id = f"scan-{market.lower()}-runtime-probe"
    symbols = [f"{market}AAA", f"{market}BBB"]
    db = SessionLocal()
    try:
        db.add(
            Scan(
                scan_id=scan_id,
                status="completed",
                universe_market=market,
                completed_at=datetime(2026, 9, 4, 21, 0, tzinfo=UTC),
                warnings=[{"expected_dates": {market: anchor}}],
                total_stocks=2,
                passed_stocks=2,
            )
        )
        for offset, symbol in enumerate(symbols):
            db.add(
                StockUniverse(
                    symbol=symbol,
                    name=f"{symbol} Company",
                    market=market,
                    exchange="NASDAQ" if market == "US" else "SSE",
                    currency="USD" if market == "US" else "CNY",
                    timezone="America/New_York" if market == "US" else "Asia/Shanghai",
                )
            )
            db.add(
                ScanResult(
                    scan_id=scan_id,
                    symbol=symbol,
                    composite_score=90.0 - offset,
                    volume=250_000_000,
                    price=100.0,
                    details={},
                )
            )
        db.commit()
    finally:
        db.close()

    analytics._scan_analytics_adapter = lambda: _LocalFrames(anchor)
    daily_snapshot_service.build_key_market_entries = lambda *_args, **_kwargs: []

    clear_runtime_services()
    stdout = io.StringIO()
    sys.argv = [
        "reconcile_scan_analytics.py",
        "--scan-id",
        scan_id,
        "--apply",
        "--force",
    ]
    with redirect_stdout(stdout):
        assert cli.main() == 0
    published = json.loads(stdout.getvalue().strip().splitlines()[-1])
    assert published["status"] == "published"

    db = SessionLocal()
    try:
        scan = db.query(Scan).filter_by(scan_id=scan_id).one()
        publication = db.query(DailySnapshotPublication).filter_by(scan_id=scan_id).one()
        payload = json.loads(publication.payload_json)
        assert scan.analytics_version == analytics.SCAN_ANALYTICS_VERSION
        assert payload["market"] == market
        assert payload["scan_id"] == scan_id
        assert len(payload["top_candidates"]["rows"]) == 2
        if market == "US":
            # US production advertises group rankings even when the current
            # database has no industry rows; the real service must return an
            # empty section without bypassing RuntimeServices.
            assert payload["top_groups"] == []
    finally:
        db.close()

    stdout = io.StringIO()
    sys.argv = ["reconcile_scan_analytics.py", "--scan-id", scan_id, "--apply"]
    with redirect_stdout(stdout):
        assert cli.main() == 0
    current = json.loads(stdout.getvalue().strip().splitlines()[-1])
    assert current["status"] == "current"

    # Profile the exact durable read in another fresh process while this
    # isolated database still contains a valid publication.
    profiler = Path(__file__).resolve().parents[2] / "scripts/profile_ss_snapshot_read.py"
    profile_result = subprocess.run(
        [sys.executable, str(profiler), "--market", market],
        cwd=Path.cwd(),
        env=os.environ.copy(),
        text=True,
        capture_output=True,
        timeout=60,
        check=False,
    )
    assert profile_result.returncode == 0, profile_result.stderr
    profile = json.loads(profile_result.stdout.strip().splitlines()[-1])
    assert profile["candidate_count"] == 2
    assert profile["sql"]["statement_count"] == 1
    assert [item["kind"] for item in profile["sql"]["statements"]] == [
        "snapshot_publication"
    ]
    print(json.dumps({"snapshot_read_profile": profile}, sort_keys=True))

    # Exercise the patched route itself so byte identity, ETag generation, and
    # conditional 304 behavior remain covered after replacing its read query.
    from app.api.v1 import market_scan as market_scan_route

    db = SessionLocal()
    try:
        first_response = asyncio.run(
            market_scan_route.get_daily_snapshot(
                SimpleNamespace(headers={}),
                market=market,
                db=db,
                uow=None,
                use_case=None,
            )
        )
        assert first_response.status_code == 200
        assert json.loads(first_response.body)["scan_id"] == scan_id
        etag = first_response.headers["etag"]
        cached_response = asyncio.run(
            market_scan_route.get_daily_snapshot(
                SimpleNamespace(headers={"if-none-match": etag}),
                market=market,
                db=db,
                uow=None,
                use_case=None,
            )
        )
        assert cached_response.status_code == 304
        assert cached_response.headers["etag"] == etag
    finally:
        db.close()

    # A newer analytics-ready scan without its exact publication must make the
    # one-statement read unavailable; it must not select the older publication.
    unpublished_scan_id = f"scan-{market.lower()}-unpublished-newer"
    db = SessionLocal()
    try:
        unpublished_scan = Scan(
            scan_id=unpublished_scan_id,
            status="completed",
            universe_market=market,
            completed_at=datetime(2026, 9, 5, 21, 0, tzinfo=UTC),
            analytics_version="other-analytics-version",
            analytics_completed_at=datetime(2026, 9, 5, 21, 1, tzinfo=UTC),
            warnings=[],
            total_stocks=0,
            passed_stocks=0,
        )
        db.add(unpublished_scan)
        db.commit()
        version_filtered_payload = load_latest_snapshot_publication_json(
            db,
            market=market,
            analytics_version=analytics.SCAN_ANALYTICS_VERSION,
        )
        assert json.loads(version_filtered_payload)["scan_id"] == scan_id
        unpublished_scan.analytics_version = analytics.SCAN_ANALYTICS_VERSION
        db.commit()
    finally:
        db.close()
    unavailable_result = subprocess.run(
        [sys.executable, str(profiler), "--market", market],
        cwd=Path.cwd(),
        env=os.environ.copy(),
        text=True,
        capture_output=True,
        timeout=60,
        check=False,
    )
    assert unavailable_result.returncode != 0
    assert "snapshot publication is unavailable" in unavailable_result.stderr
    db = SessionLocal()
    try:
        db.query(Scan).filter_by(scan_id=unpublished_scan_id).delete()
        db.commit()
    finally:
        db.close()

    # Corrupt a committed publication input and prove the repair stays closed
    # and cannot leave a partially replaced publication behind.
    db = SessionLocal()
    try:
        db.query(DailySnapshotPublication).filter_by(scan_id=scan_id).delete()
        publication_input = (
            db.query(DailySnapshotPublicationInput).filter_by(scan_id=scan_id).one()
        )
        publication_input.key_markets_sha256 = _payload_digest("corrupt")
        db.commit()
    finally:
        db.close()
    try:
        cli.reconcile(market=None, scan_id=scan_id, apply=True)
    except RuntimeError as exc:
        assert "digest does not match" in str(exc)
    else:
        raise AssertionError("corrupt Snapshot input did not fail closed")
    db = SessionLocal()
    try:
        assert db.query(DailySnapshotPublication).filter_by(scan_id=scan_id).count() == 0
    finally:
        db.close()

    # A fresh import of the deployed general-worker wrapper must independently
    # bind the same real upstream service container.
    clear_runtime_services()
    importlib.import_module("quant_workspace_celery")
    assert get_group_rank_service() is not None

    print(
        json.dumps(
            {
                "market": market,
                "published": published["status"],
                "repeat": current["status"],
                "failed_closed": True,
                "general_worker_runtime": True,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main(sys.argv[1])
