"""Lightweight scan analytics derived only from accepted QuantStack bars.

The Stock Screener upstream normally fills RS, breadth, exposure and group
rankings in separate provider-backed jobs.  QuantStack already supplied the
same historical frames to the completed scan, so the production overlay does
one local batch read after publication and persists the derived display data.
"""

from __future__ import annotations

import os
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
from statistics import median
from typing import Any

import pandas as pd
from daily_snapshot_publication import (
    SNAPSHOT_PUBLICATION_VERSION,
    build_and_store_snapshot_publication,
    snapshot_publication_is_current,
)
from quantstack_consumer_adapter import SSQuantStackConsumerAdapter

SCAN_ANALYTICS_VERSION = (
    os.environ.get("SS_REQUIRED_SCAN_ANALYTICS_VERSION", "quantstack.scan-analytics.v1").strip()
    or "quantstack.scan-analytics.v1"
)
LIQUIDITY_FLOORS = {"CN": 7_000_000, "US": 100_000_000}
DEFAULT_SCAN_ANALYTICS_TIMEOUT_SECONDS = 30.0
DEFAULT_SCAN_ANALYTICS_MAX_ATTEMPTS = 1


def _return(closes: pd.Series, sessions: int) -> float | None:
    if len(closes) <= sessions:
        return None
    base = float(closes.iloc[-sessions - 1])
    return None if base == 0 else float(closes.iloc[-1] / base - 1.0)


def _summarize_frame(
    frame: pd.DataFrame,
) -> tuple[dict[str, Any], dict[str, float | None], dict[int, float | None], pd.Series]:
    ordered = frame.sort_index()
    closes = pd.to_numeric(ordered["Close"], errors="coerce").dropna()
    volumes = pd.to_numeric(ordered["Volume"], errors="coerce").dropna()
    change_1d = _return(closes, 1)
    returns = {
        "1m": _return(closes, 21),
        "3m": _return(closes, 63),
        "12m": _return(closes, 252),
    }
    changes = {sessions: _return(closes, sessions) for sessions in (1, 5, 10, 21, 34, 63)}
    ma50 = float(closes.tail(50).mean()) if len(closes) >= 50 else None
    ma200 = float(closes.tail(200).mean()) if len(closes) >= 200 else None
    price = float(closes.iloc[-1])
    # ScanResult.volume is the product's local-currency average dollar-volume
    # field (despite the legacy column name).  Persisting the latest raw share
    # volume here silently changed the Daily Snapshot's USD 100M liquidity
    # floor into a 100M-share floor for QuantStack-backed scans.
    avg_dollar_volume = int(float(volumes.tail(50).mean()) * price) if not volumes.empty else None
    stage = None
    if ma50 is not None and ma200 is not None:
        stage = 2 if price >= ma50 >= ma200 else 4 if price < ma50 < ma200 else 3
    metrics = {
        "price": price,
        "volume": avg_dollar_volume,
        "price_sparkline_data": [round(float(value), 6) for value in closes.tail(30)],
        "price_change_1d": None if change_1d is None else round(change_1d * 100, 4),
        "price_trend": (
            None if change_1d is None else (1 if change_1d > 0 else -1 if change_1d < 0 else 0)
        ),
        "stage": stage,
    }
    return metrics, returns, changes, closes.pct_change().dropna()


def _apply_rs_percentiles(
    metrics: dict[str, dict[str, Any]],
    returns: dict[str, dict[str, float | None]],
) -> None:
    for horizon, field in (
        ("1m", "rs_rating_1m"),
        ("3m", "rs_rating_3m"),
        ("12m", "rs_rating_12m"),
    ):
        values = pd.Series(
            {symbol: item[horizon] for symbol, item in returns.items()},
            dtype="float64",
        ).dropna()
        ranks = values.rank(method="average", pct=True) * 99.0 + 1.0
        for symbol, rank in ranks.items():
            metrics[symbol][field] = round(float(rank), 2)
    for item in metrics.values():
        item["rs_rating"] = (
            item.get("rs_rating_12m") or item.get("rs_rating_3m") or item.get("rs_rating_1m")
        )


def calculate_symbol_metrics(
    frames: dict[str, pd.DataFrame],
) -> dict[str, dict[str, Any]]:
    """Calculate display metrics and cross-sectional RS percentiles."""

    metrics: dict[str, dict[str, Any]] = {}
    returns: dict[str, dict[str, float | None]] = {}
    for symbol, frame in frames.items():
        if frame is None or frame.empty:
            continue
        try:
            item, symbol_returns, _, _ = _summarize_frame(frame)
        except (KeyError, IndexError):
            continue
        metrics[symbol] = item
        returns[symbol] = symbol_returns
    _apply_rs_percentiles(metrics, returns)
    return metrics


def calculate_breadth(frames: dict[str, pd.DataFrame]) -> dict[str, Any]:
    """Calculate one session of cross-sectional breadth from local frames."""

    changes: dict[int, list[float]] = defaultdict(list)
    for frame in frames.values():
        if frame is None or frame.empty:
            continue
        closes = pd.to_numeric(frame.sort_index()["Close"], errors="coerce").dropna()
        for sessions in (1, 5, 10, 21, 34, 63):
            value = _return(closes, sessions)
            if value is not None:
                changes[sessions].append(value)

    def count(sessions: int, threshold: float, *, up: bool) -> int:
        return sum(value >= threshold if up else value <= -threshold for value in changes[sessions])

    up5, down5 = count(5, 0.04, up=True), count(5, 0.04, up=False)
    up10, down10 = count(10, 0.04, up=True), count(10, 0.04, up=False)
    return {
        "stocks_up_4pct": count(1, 0.04, up=True),
        "stocks_down_4pct": count(1, 0.04, up=False),
        "ratio_5day": None if down5 == 0 else round(up5 / down5, 4),
        "ratio_10day": None if down10 == 0 else round(up10 / down10, 4),
        "stocks_up_25pct_quarter": count(63, 0.25, up=True),
        "stocks_down_25pct_quarter": count(63, 0.25, up=False),
        "stocks_up_25pct_month": count(21, 0.25, up=True),
        "stocks_down_25pct_month": count(21, 0.25, up=False),
        "stocks_up_50pct_month": count(21, 0.50, up=True),
        "stocks_down_50pct_month": count(21, 0.50, up=False),
        "stocks_up_13pct_34days": count(34, 0.13, up=True),
        "stocks_down_13pct_34days": count(34, 0.13, up=False),
        "total_stocks_scanned": len(changes[1]),
    }


def calculate_equal_weight_exposure(
    frames: dict[str, pd.DataFrame], breadth: dict[str, Any]
) -> dict[str, Any]:
    """Build a transparent market-health proxy without a provider benchmark."""

    returns = []
    for frame in frames.values():
        if frame is None or frame.empty:
            continue
        closes = pd.to_numeric(frame.sort_index()["Close"], errors="coerce").dropna()
        series = closes.pct_change().rename("return")
        returns.append(series)
    if not returns:
        raise ValueError("no local price frames available for exposure")
    market_returns = pd.concat(returns, axis=1).mean(axis=1, skipna=True).dropna()
    synthetic = 100.0 * (1.0 + market_returns).cumprod()
    return _equal_weight_exposure_values(synthetic, market_returns, breadth)


def _equal_weight_exposure_values(
    synthetic: pd.Series,
    market_returns: pd.Series,
    breadth: dict[str, Any],
) -> dict[str, Any]:
    price = float(synthetic.iloc[-1])
    ma50 = float(synthetic.tail(50).mean()) if len(synthetic) >= 50 else None
    ma200 = float(synthetic.tail(200).mean()) if len(synthetic) >= 200 else None
    net_4pct = int(breadth["stocks_up_4pct"] - breadth["stocks_down_4pct"])
    score = 50.0
    components: dict[str, float] = {"base": 50.0}
    if ma50:
        contribution = max(-15.0, min(15.0, (price / ma50 - 1.0) * 250.0))
        components["equal_weight_50d"] = round(contribution, 1)
        score += contribution
    if ma200:
        contribution = max(-25.0, min(25.0, (price / ma200 - 1.0) * 250.0))
        components["equal_weight_200d"] = round(contribution, 1)
        score += contribution
    breadth_contribution = max(-10.0, min(10.0, net_4pct / 10.0))
    components["net_4pct"] = round(breadth_contribution, 1)
    score = max(0.0, min(100.0, score + breadth_contribution))
    stance = (
        "Power Trend"
        if score >= 85
        else "Confirmed Uptrend"
        if score >= 65
        else "Uptrend Under Pressure"
        if score >= 50
        else "Downtrend/Caution"
        if score >= 30
        else "Correction — In Cash"
    )
    if ma50 and ma200 and price >= ma50 >= ma200:
        trend = "bullish"
    elif ma50 and ma200 and price < ma50 < ma200:
        trend = "bearish"
    else:
        trend = "neutral"
    return {
        "exposure_score": round(score, 2),
        "stance": stance,
        "benchmark_price": price,
        "benchmark_ma50": ma50,
        "benchmark_ma200": ma200,
        "trend": trend,
        "distribution_day_count": int((market_returns.tail(25) <= -0.002).sum()),
        "follow_through_day": False,
        "follow_through_date": None,
        "vix": None,
        "net_4pct": net_4pct,
        "components": components,
    }


def _stream_scan_frames(
    adapter: SSQuantStackConsumerAdapter,
    symbols: list[str],
    *,
    start: date,
    end: date,
) -> tuple[
    dict[str, dict[str, Any]], dict[str, dict[str, float | None]], dict[int, list[float]], pd.Series
]:
    if hasattr(adapter, "max_cache_entries"):
        # This one-pass job never reuses symbol frames; retain only the final
        # adapter result instead of the normal interactive read cache.
        adapter.max_cache_entries = 1
    metrics: dict[str, dict[str, Any]] = {}
    returns: dict[str, dict[str, float | None]] = {}
    breadth_changes: dict[int, list[float]] = defaultdict(list)
    daily_return_sum: pd.Series | None = None
    daily_return_count: pd.Series | None = None
    for offset in range(0, len(symbols), 250):
        batch = adapter.get_batch_price_history(
            symbols[offset : offset + 250], start=start, end=end
        )
        for symbol, result in batch.items():
            frame = result.frame
            if frame is None or frame.empty:
                continue
            try:
                item, symbol_returns, changes, daily_returns = _summarize_frame(frame)
            except (KeyError, IndexError):
                continue
            metrics[symbol] = item
            returns[symbol] = symbol_returns
            for sessions, value in changes.items():
                if value is not None:
                    breadth_changes[sessions].append(value)
            values = daily_returns.astype("float64")
            counts = values.notna().astype("int64")
            daily_return_sum = (
                values if daily_return_sum is None else daily_return_sum.add(values, fill_value=0)
            )
            daily_return_count = (
                counts
                if daily_return_count is None
                else daily_return_count.add(counts, fill_value=0)
            )
    _apply_rs_percentiles(metrics, returns)
    if daily_return_sum is None or daily_return_count is None:
        market_returns = pd.Series(dtype="float64")
    else:
        market_returns = daily_return_sum.div(daily_return_count.replace(0, pd.NA)).dropna()
    return metrics, returns, breadth_changes, market_returns


def _scan_analytics_adapter() -> SSQuantStackConsumerAdapter:
    """Use a cold-batch budget without changing latency-sensitive consumers."""

    timeout_seconds = float(
        os.environ.get(
            "SS_SCAN_ANALYTICS_TIMEOUT_SECONDS",
            str(DEFAULT_SCAN_ANALYTICS_TIMEOUT_SECONDS),
        )
    )
    max_attempts = int(
        os.environ.get(
            "SS_SCAN_ANALYTICS_MAX_ATTEMPTS",
            str(DEFAULT_SCAN_ANALYTICS_MAX_ATTEMPTS),
        )
    )
    if timeout_seconds <= 0:
        raise ValueError("QuantStack adapter timeout must be positive")
    if max_attempts < 1 or max_attempts > 3:
        raise ValueError("QuantStack adapter attempts must be between 1 and 3")
    return SSQuantStackConsumerAdapter.from_environment(
        timeout_seconds=timeout_seconds,
        max_attempts=max_attempts,
    )


def _breadth_from_changes(changes: dict[int, list[float]]) -> dict[str, Any]:
    def count(sessions: int, threshold: float, *, up: bool) -> int:
        return sum(value >= threshold if up else value <= -threshold for value in changes[sessions])

    up5, down5 = count(5, 0.04, up=True), count(5, 0.04, up=False)
    up10, down10 = count(10, 0.04, up=True), count(10, 0.04, up=False)
    return {
        "stocks_up_4pct": count(1, 0.04, up=True),
        "stocks_down_4pct": count(1, 0.04, up=False),
        "ratio_5day": None if down5 == 0 else round(up5 / down5, 4),
        "ratio_10day": None if down10 == 0 else round(up10 / down10, 4),
        "stocks_up_25pct_quarter": count(63, 0.25, up=True),
        "stocks_down_25pct_quarter": count(63, 0.25, up=False),
        "stocks_up_25pct_month": count(21, 0.25, up=True),
        "stocks_down_25pct_month": count(21, 0.25, up=False),
        "stocks_up_50pct_month": count(21, 0.50, up=True),
        "stocks_down_50pct_month": count(21, 0.50, up=False),
        "stocks_up_13pct_34days": count(34, 0.13, up=True),
        "stocks_down_13pct_34days": count(34, 0.13, up=False),
        "total_stocks_scanned": len(changes[1]),
    }


def _exposure_from_market_returns(
    market_returns: pd.Series, breadth: dict[str, Any]
) -> dict[str, Any]:
    if market_returns.empty:
        raise ValueError("no local price frames available for exposure")
    synthetic = 100.0 * (1.0 + market_returns).cumprod()
    return _equal_weight_exposure_values(synthetic, market_returns, breadth)


def _scan_anchor(scan: Any) -> date:
    market = str(scan.universe_market or "").upper()
    for warning in scan.warnings or ():
        value = warning.get("expected_dates", {}).get(market) if isinstance(warning, dict) else None
        if value:
            return date.fromisoformat(value)
    return scan.completed_at.date()


def sync_scan_analytics(
    scan_id: str,
    *,
    apply: bool = True,
    force: bool = False,
) -> dict[str, Any]:
    """Plan or atomically publish local analytics for one completed scan.

    The Daily Snapshot read path only considers a scan publishable after the
    versioned readiness marker is committed in the same transaction as every
    derived value.  Re-running the current version is intentionally a no-op.
    """

    from app.database import SessionLocal
    from app.models.industry import IBDGroupRank, IBDIndustryGroup
    from app.models.market_breadth import MarketBreadth
    from app.models.market_exposure import MarketExposure
    from app.models.scan_result import Scan, ScanResult
    from app.models.stock_universe import StockUniverse

    db = SessionLocal()
    previous_autoflush = db.autoflush
    db.autoflush = False
    try:
        scan = db.query(Scan).filter(Scan.scan_id == scan_id, Scan.status == "completed").one()
        market = str(scan.universe_market).upper()
        anchor = _scan_anchor(scan)
        rows = db.query(ScanResult).filter(ScanResult.scan_id == scan_id).all()
        liquidity_floor = LIQUIDITY_FLOORS.get(market)
        if apply and not force and scan.analytics_version == SCAN_ANALYTICS_VERSION:
            report = {
                "schema_version": "quantstack.ss-scan-analytics-publication.v1",
                "status": "current",
                "scan_id": scan_id,
                "market": market,
                "date": anchor.isoformat(),
                "analytics_version": scan.analytics_version,
                "analytics_completed_at": (
                    scan.analytics_completed_at.isoformat()
                    if scan.analytics_completed_at is not None
                    else None
                ),
                "rows": len(rows),
                "eligible_rows": (
                    sum(
                        row.volume is not None and int(row.volume) >= liquidity_floor
                        for row in rows
                    )
                    if liquidity_floor is not None
                    else None
                ),
                "liquidity_floor": liquidity_floor,
            }
            if snapshot_publication_is_current(
                db,
                scan=scan,
                market=market,
                analytics_version=SCAN_ANALYTICS_VERSION,
            ):
                report["snapshot_publication_version"] = SNAPSHOT_PUBLICATION_VERSION
                return report
            # Repair an interrupted or pre-publication-version scan entirely
            # from committed DB rows. Do not reread thousands of bar frames.
            build_and_store_snapshot_publication(
                db,
                scan=scan,
                market=market,
                analytics_version=SCAN_ANALYTICS_VERSION,
                capture_inputs=False,
            )
            db.commit()
            report["status"] = "publication_repaired"
            report["snapshot_publication_version"] = SNAPSHOT_PUBLICATION_VERSION
            return report
        symbols = [row.symbol for row in rows]
        adapter = _scan_analytics_adapter()
        start = anchor - timedelta(days=420)
        metrics, _, breadth_changes, market_returns = _stream_scan_frames(
            adapter,
            symbols,
            start=start,
            end=anchor,
        )
        universe = {
            row.symbol: row
            for row in db.query(StockUniverse).filter(StockUniverse.symbol.in_(symbols)).all()
        }
        classifications = {
            row.symbol: row.industry_group
            for row in db.query(IBDIndustryGroup)
            .filter(
                IBDIndustryGroup.market == market,
                IBDIndustryGroup.symbol.in_(symbols),
            )
            .all()
        }
        for row in rows:
            item = metrics.get(row.symbol)
            if item:
                for field, value in item.items():
                    setattr(row, field, value)
            classification = classifications.get(row.symbol)
            if classification:
                row.ibd_industry_group = classification
            instrument = universe.get(row.symbol)
            if instrument is not None:
                row.gics_sector = row.gics_sector or instrument.sector
                row.gics_industry = row.gics_industry or instrument.industry
            details = dict(row.details or {})
            for field in (
                "rs_rating",
                "rs_rating_1m",
                "rs_rating_3m",
                "rs_rating_12m",
                "ibd_industry_group",
                "gics_sector",
                "gics_industry",
            ):
                value = getattr(row, field, None)
                if value is not None:
                    details[field] = value
            row.details = details

        grouped: dict[str, list[ScanResult]] = defaultdict(list)
        for row in rows:
            if row.ibd_industry_group and row.rs_rating is not None:
                grouped[row.ibd_industry_group].append(row)
        ranked = sorted(
            grouped.items(),
            key=lambda item: sum(float(row.rs_rating) for row in item[1]) / len(item[1]),
            reverse=True,
        )
        rank_by_group: dict[str, int] = {}
        for rank, (group, members) in enumerate(ranked, 1):
            rank_by_group[group] = rank
            values = [float(row.rs_rating) for row in members]
            top = max(members, key=lambda row: float(row.rs_rating))
            record = (
                db.query(IBDGroupRank)
                .filter_by(market=market, date=anchor, industry_group=group)
                .one_or_none()
            )
            if record is None:
                record = IBDGroupRank(market=market, date=anchor, industry_group=group)
                db.add(record)
            record.rank = rank
            record.avg_rs_rating = sum(values) / len(values)
            record.median_rs_rating = median(values)
            record.rs_std_dev = float(pd.Series(values).std(ddof=0))
            record.num_stocks = len(values)
            record.num_stocks_rs_above_80 = sum(value >= 80 for value in values)
            record.top_symbol = top.symbol
            record.top_rs_rating = float(top.rs_rating)
        for row in rows:
            row.ibd_group_rank = rank_by_group.get(row.ibd_industry_group)
            if row.ibd_group_rank is not None:
                details = dict(row.details or {})
                details["ibd_group_rank"] = row.ibd_group_rank
                details["ibd_group_rank_date"] = anchor.isoformat()
                row.details = details

        breadth_values = _breadth_from_changes(breadth_changes)
        breadth = db.query(MarketBreadth).filter_by(market=market, date=anchor).one_or_none()
        if breadth is None:
            breadth = MarketBreadth(market=market, date=anchor)
            db.add(breadth)
        for field, value in breadth_values.items():
            setattr(breadth, field, value)

        exposure_values = _exposure_from_market_returns(
            market_returns,
            breadth_values,
        )
        exposure = db.query(MarketExposure).filter_by(market=market, date=anchor).one_or_none()
        if exposure is None:
            exposure = MarketExposure(market=market, date=anchor)
            db.add(exposure)
        for field, value in exposure_values.items():
            setattr(exposure, field, value)
        exposure.benchmark_symbol = f"{market} equal-weight"
        completed_at = datetime.now(UTC)
        report = {
            "schema_version": "quantstack.ss-scan-analytics-publication.v1",
            "status": "published" if apply else "planned",
            "scan_id": scan_id,
            "market": market,
            "date": anchor.isoformat(),
            "analytics_version": SCAN_ANALYTICS_VERSION,
            "analytics_completed_at": completed_at.isoformat() if apply else None,
            "rows": len(rows),
            "eligible_rows": (
                sum(
                    row.volume is not None and int(row.volume) >= liquidity_floor
                    for row in rows
                )
                if liquidity_floor is not None
                else None
            ),
            "liquidity_floor": liquidity_floor,
            "rs_rows": sum(row.rs_rating is not None for row in rows),
            "classified_rows": sum(bool(row.ibd_industry_group) for row in rows),
            "groups": len(ranked),
            "breadth_rows": breadth_values["total_stocks_scanned"],
        }
        if apply:
            scan.analytics_version = SCAN_ANALYTICS_VERSION
            scan.analytics_completed_at = completed_at
            build_and_store_snapshot_publication(
                db,
                scan=scan,
                market=market,
                analytics_version=SCAN_ANALYTICS_VERSION,
                capture_inputs=True,
            )
            report["snapshot_publication_version"] = SNAPSHOT_PUBLICATION_VERSION
            db.commit()
        else:
            db.rollback()
        return report
    except Exception:
        db.rollback()
        raise
    finally:
        db.autoflush = previous_autoflush
        db.close()
