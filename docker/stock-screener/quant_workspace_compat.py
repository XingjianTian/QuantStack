"""Narrow runtime compatibility layer for stock-screener v1.4.0.

The upstream CN decision dashboard can return non-finite floats from its live
technical-data fallback. Starlette intentionally rejects NaN/Infinity during
JSON encoding, turning an otherwise usable daily chart into HTTP 500. The same
fallback can persist a combined provider label longer than the v1.4.0 database
column. Keep both compatibility changes outside the upstream checkout.
"""

from __future__ import annotations

import json
import logging
import math
import os
import threading
import time as monotonic_time
import urllib.parse
import urllib.request
import uuid
from contextlib import asynccontextmanager
from contextvars import ContextVar
from datetime import UTC, date, datetime, time
from functools import partial, wraps
from numbers import Real
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from anyio import CapacityLimiter, to_thread
from app.api.v1 import stocks as stocks_api
from app.api.v1 import stocks_rs_line as stocks_rs_line_api
from app.api.v1.technical import convert_numpy_types
from app.database import SessionLocal, engine
from app.main import app
from app.services.price_cache_service import PriceCacheService
from app.services.yfinance_service import YFinanceService
from app.wiring.bootstrap import (
    get_fundamentals_cache,
    get_price_cache,
    get_scan_orchestrator,
)
from daily_snapshot_publication import warm_latest_snapshot_publication_query_plan
from fastapi import Depends, Header, HTTPException, Response
from quantstack_consumer_adapter import (
    AdapterError,
    HTTPQuantStackMarketDataClient,
    SSQuantStackConsumerAdapter,
    capture_read_stage_timings,
    dataset_release_date,
    install_price_cache_read_modes,
    install_yfinance_read_modes,
)
from sqlalchemy import bindparam, text

logger = logging.getLogger(__name__)

_upstream_stock_info_loader = stocks_api._get_stock_info_payload


def _ensure_provider_label_capacity() -> None:
    with engine.begin() as connection:
        connection.execute(
            text("ALTER TABLE stock_fundamentals ALTER COLUMN data_source TYPE VARCHAR(64)")
        )


def _sync_us_quantstack_universe() -> int:
    """Idempotently project the published US instrument set into SS."""
    dataset_id = os.environ.get("SS_QUANTSTACK_US_INSTRUMENTS_DATASET_ID", "").strip()
    dataset_version = os.environ.get(
        "SS_QUANTSTACK_US_INSTRUMENTS_DATASET_VERSION", "latest"
    ).strip()
    if not dataset_id:
        return 0
    client = HTTPQuantStackMarketDataClient.from_environment()
    payload = client.fetch_instruments(
        {
            "market": "US",
            "active": True,
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
            "page_size": 10_000,
        }
    )
    if payload.get("next_cursor") is not None:
        raise RuntimeError("US universe exceeds the bounded one-page projection")
    rows = [
        {
            "symbol": str(item["symbol"]).upper(),
            "market": "US",
            "exchange": str(item["venue"]),
            "currency": str(item["currency"]),
            "timezone": str(item["timezone"]),
            "source": "quantstack",
        }
        for item in payload.get("items", [])
        if str(item.get("instrument_id", "")).startswith(("XNAS.", "XNYS."))
    ]
    if not rows:
        raise RuntimeError("published US universe projection is empty")
    current_statement = text(
        """
        SELECT symbol, exchange, currency, timezone, is_active, status,
               status_reason, source, consecutive_fetch_failures
        FROM stock_universe
        WHERE market = 'US'
        """
    )
    statement = text(
        """
        INSERT INTO stock_universe
            (symbol, name, market, exchange, currency, timezone, is_active,
             status, source, consecutive_fetch_failures, added_at,
             first_seen_at, last_seen_in_source_at, updated_at)
        VALUES
            (:symbol, :symbol, :market, :exchange, :currency, :timezone, true,
             'active', :source, 0, now(), now(), now(), now())
        ON CONFLICT (symbol) DO UPDATE SET
            market = EXCLUDED.market,
            exchange = EXCLUDED.exchange,
            currency = EXCLUDED.currency,
            timezone = EXCLUDED.timezone,
            is_active = true,
            status = 'active',
            status_reason = NULL,
            source = EXCLUDED.source,
            consecutive_fetch_failures = 0,
            last_seen_in_source_at = now(),
            deactivated_at = NULL,
            updated_at = now()
        WHERE stock_universe.market = 'US'
        """
    )
    with engine.begin() as connection:
        current = {
            str(item["symbol"]).upper(): item
            for item in connection.execute(current_statement).mappings()
        }
        changed = [
            item
            for item in rows
            if item["symbol"] not in current
            or current[item["symbol"]]["exchange"] != item["exchange"]
            or current[item["symbol"]]["currency"] != item["currency"]
            or current[item["symbol"]]["timezone"] != item["timezone"]
            or current[item["symbol"]]["is_active"] is not True
            or current[item["symbol"]]["status"] != "active"
            or current[item["symbol"]]["status_reason"] is not None
            or current[item["symbol"]]["source"] != "quantstack"
            or current[item["symbol"]]["consecutive_fetch_failures"] != 0
        ]
        if changed:
            connection.execute(statement, changed)
    return len(changed)


def _warm_snapshot_publication_query_plan() -> None:
    """Warm publication query metadata without reading a snapshot payload."""

    analytics_version = os.environ.get("SS_REQUIRED_SCAN_ANALYTICS_VERSION", "").strip()
    if not analytics_version:
        return
    session = SessionLocal()
    try:
        warm_latest_snapshot_publication_query_plan(
            session,
            analytics_version=analytics_version,
        )
    except Exception:  # pragma: no cover - the rollback image has no publication tables
        logger.warning("Daily Snapshot publication query-plan warmup skipped", exc_info=True)
    finally:
        session.rollback()
        session.close()


def _set_quantstack_market_defaults() -> None:
    if os.environ.get("SS_QUANTSTACK_US_DEFAULT", "false").lower() != "true":
        return
    from app.services.runtime_preferences_service import save_runtime_preferences

    session = SessionLocal()
    try:
        save_runtime_preferences(
            session,
            primary_market="US",
            enabled_markets=["US", "CN"],
            bootstrap_state="ready",
        )
    finally:
        session.close()


def _resolve_us_instruments_from_stock_universe(
    symbols: tuple[str, ...],
) -> dict[str, str]:
    """Reuse the already-published local universe for foreground chart routing."""

    if not symbols:
        return {}
    statement = text(
        """
        SELECT symbol, exchange
        FROM stock_universe
        WHERE market = 'US'
          AND is_active = true
          AND symbol IN :symbols
        """
    ).bindparams(bindparam("symbols", expanding=True))
    with engine.connect() as connection:
        rows = connection.execute(statement, {"symbols": list(symbols)}).mappings()
        return {
            str(row["symbol"]).upper(): (
                str(row["exchange"]).upper() + "." + str(row["symbol"]).upper()
            )
            for row in rows
            if str(row["exchange"]).upper() in {"XNAS", "XNYS"}
        }


def _chart_adapter_factory() -> SSQuantStackConsumerAdapter:
    return SSQuantStackConsumerAdapter.from_environment(
        us_instrument_resolver=_resolve_us_instruments_from_stock_universe,
    )


_upstream_lifespan = app.router.lifespan_context


@asynccontextmanager
async def _quant_workspace_lifespan(application):
    async with _upstream_lifespan(application):
        _ensure_provider_label_capacity()
        _warm_snapshot_publication_query_plan()
        synced = _sync_us_quantstack_universe()
        _set_quantstack_market_defaults()
        if synced:
            logger.info("Projected %s published US instruments into Stock Screener", synced)
        yield


def _finite_json(value: Any) -> Any:
    if isinstance(value, Real) and not math.isfinite(float(value)):
        return None
    if isinstance(value, dict):
        return {key: _finite_json(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_finite_json(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_finite_json(item) for item in value)
    return value


def _install_decision_dashboard_sanitizer() -> None:
    for route in app.routes:
        if getattr(route, "path", None) != "/api/v1/stocks/{symbol}/decision-dashboard":
            continue

        original = route.dependant.call

        @wraps(original)
        async def sanitized_dashboard(*args, _original=original, **kwargs):
            return _finite_json(await _original(*args, **kwargs))

        route.dependant.call = sanitized_dashboard
        return
    raise RuntimeError("stock-screener decision-dashboard route was not found")


def _load_stock_info_from_quantstack(symbol: str) -> dict[str, Any] | None:
    """Serve the basic detail header from the accepted local price release."""

    normalized = symbol.upper()
    try:
        frame = get_price_cache().get_cached_only(normalized, period="2y")
    except AdapterError:
        return _upstream_stock_info_loader(normalized)
    if frame is None or frame.empty or "Close" not in frame.columns:
        return _upstream_stock_info_loader(normalized)

    current_price = frame["Close"].iloc[-1]
    return {
        "symbol": normalized,
        "name": normalized,
        "sector": None,
        "industry": None,
        "current_price": (
            float(current_price)
            if current_price is not None and math.isfinite(float(current_price))
            else None
        ),
        "market_cap": None,
    }


def _install_quantstack_stock_info() -> None:
    """Keep direct stock pages local when QuantStack is the price source."""

    stocks_api._get_stock_info_payload = _load_stock_info_from_quantstack


def _load_fundamentals_cache_only(
    symbol: str, *, force_refresh: bool = False
) -> dict[str, Any] | None:
    """Read persisted fundamentals without making a chart request a fetch job."""

    data, _last_update = get_fundamentals_cache()._get_from_database(symbol.upper())
    if data is None:
        return None
    data.setdefault("symbol", symbol.upper())
    return data


def _install_cache_only_stock_fundamentals() -> None:
    stocks_api._get_stock_fundamentals_payload = _load_fundamentals_cache_only


_chart_primary_read_limiter = CapacityLimiter(2)
_chart_background_read_limiter = CapacityLimiter(1)


async def _run_chart_read(function, *args, limiter=_chart_primary_read_limiter):
    """Keep synchronous QuantStack/NAS reads off the single ASGI event loop."""

    return await to_thread.run_sync(partial(function, *args), limiter=limiter)


def _load_chart_history(symbol: str, period: str) -> list[dict[str, Any]]:
    days = stocks_api.PERIOD_DAYS.get(period)
    if days is None:
        raise HTTPException(status_code=422, detail=f"Unsupported period: {period}")

    frame = get_price_cache().get_cached_only(symbol.upper(), period=period)
    points = stocks_api.dataframe_to_points(frame, days)
    if not points:
        raise HTTPException(
            status_code=404,
            detail=f"Historical data not available for {symbol}",
        )
    return points


def _load_chart_history_range(
    symbol: str, start: date, end: date
) -> tuple[list[dict[str, Any]], dict[str, float]]:
    if start > end or (end - start).days > 3650:
        raise HTTPException(status_code=422, detail="Invalid history range")
    worker_started_at = monotonic_time.perf_counter()
    with capture_read_stage_timings() as timings:
        frame = get_price_cache().get_quantstack_range(symbol.upper(), start, end)
    points_started_at = monotonic_time.perf_counter()
    points = [] if frame is None else [
        {
            "date": timestamp.strftime("%Y-%m-%d"),
            "open": round(float(row["Open"]), 2),
            "high": round(float(row["High"]), 2),
            "low": round(float(row["Low"]), 2),
            "close": round(float(row["Close"]), 2),
            "volume": int(row["Volume"]),
        }
        for timestamp, row in frame.iterrows()
    ]
    timings["point_conversion"] = (
        monotonic_time.perf_counter() - points_started_at
    ) * 1000
    timings["worker_total"] = (
        monotonic_time.perf_counter() - worker_started_at
    ) * 1000
    if not points:
        raise HTTPException(
            status_code=404,
            detail=f"Historical data not available for {symbol}",
        )
    return points, timings


def _load_chart_history_batch(payload) -> dict[str, Any]:
    days = stocks_api.PERIOD_DAYS.get(payload.period)
    if days is None:
        raise HTTPException(
            status_code=422, detail=f"Unsupported period: {payload.period}"
        )

    symbols = stocks_api._normalize_batch_symbols(payload.symbols)
    frames = get_price_cache().get_many_cached_only(symbols, period=payload.period)
    data: dict[str, list[dict[str, Any]]] = {}
    missing: list[str] = []
    for symbol in symbols:
        frame = frames.get(symbol) if isinstance(frames, dict) else None
        points = stocks_api.dataframe_to_points(frame, days)
        if points:
            data[symbol] = points
        else:
            missing.append(symbol)
    return {"data": data, "missing": missing}


def _load_rs_line_with_local_session(symbol: str, period: str):
    """Build the optional RS overlay from published local datasets only."""

    with SessionLocal() as db:
        market = stocks_rs_line_api.resolve_symbol_market(db, symbol) or "US"
    benchmark_service = stocks_rs_line_api.BenchmarkCacheService()
    benchmark_symbol = benchmark_service.get_benchmark_symbol(market)
    empty = stocks_rs_line_api.RSLineResponse(
        symbol=symbol.upper(),
        benchmark_symbol=benchmark_symbol,
        rs_line=[],
        blue_dots=[],
    )
    days = stocks_rs_line_api.PERIOD_DAYS.get(period)
    if days is None:
        raise HTTPException(status_code=422, detail=f"Unsupported period: {period}")

    cache_period = "5y" if period == "5y" else "2y"
    try:
        stock_df = get_price_cache().get_cached_only(symbol.upper(), period=cache_period)
        benchmark_df = get_price_cache().get_cached_only(
            benchmark_symbol, period=cache_period
        )
    except AdapterError:
        return empty
    if (
        stock_df is None
        or stock_df.empty
        or benchmark_df is None
        or benchmark_df.empty
    ):
        return empty

    rs_line_full = stocks_rs_line_api.compute_rs_line(
        stock_df["Close"], benchmark_df["Close"], normalize=True
    )
    blue_full = stocks_rs_line_api.blue_dot_series(
        stock_df["Close"], benchmark_df["Close"]
    )
    cutoff = stocks_rs_line_api.window_cutoff(rs_line_full.index, days)
    rs_window = rs_line_full[rs_line_full.index >= cutoff].dropna()
    blue_window = blue_full[(blue_full.index >= cutoff) & blue_full]
    return stocks_rs_line_api.RSLineResponse(
        symbol=symbol.upper(),
        benchmark_symbol=benchmark_symbol,
        rs_line=[
            stocks_rs_line_api.RSLinePoint(
                time=timestamp.strftime("%Y-%m-%d"), value=round(float(value), 4)
            )
            for timestamp, value in rs_window.items()
        ],
        blue_dots=[timestamp.strftime("%Y-%m-%d") for timestamp in blue_window.index],
    )


def _install_nonblocking_chart_reads() -> None:
    """Prioritize one-symbol history while preserving upstream API contracts."""

    stocks_api.PERIOD_DAYS["10y"] = 3650

    async def history(
        symbol: str,
        period: str = "6mo",
    ):
        return await _run_chart_read(_load_chart_history, symbol, period)

    async def history_range(
        start: date,
        end: date,
        symbol: str = Depends(stocks_api.require_valid_price_history_symbol),
    ):
        request_id = uuid.uuid4().hex
        route_started_at = monotonic_time.perf_counter()
        points, timings = await _run_chart_read(
            _load_chart_history_range,
            symbol,
            start,
            end,
        )
        after_worker_at = monotonic_time.perf_counter()
        adapter_stage_names = (
            "instrument_resolution",
            "metadata",
            "adapter_cache_lookup",
            "bars_http_and_json",
            "dataframe_conversion",
        )
        timings["adapter_other"] = max(
            0.0,
            timings.get("adapter_total", 0.0)
            - sum(timings.get(name, 0.0) for name in adapter_stage_names),
        )
        timings["worker_other"] = max(
            0.0,
            timings["worker_total"]
            - timings.get("adapter_total", 0.0)
            - timings.get("point_conversion", 0.0),
        )
        timings["queue_and_handoff"] = max(
            0.0,
            (after_worker_at - route_started_at) * 1000 - timings["worker_total"],
        )
        serialization_started_at = monotonic_time.perf_counter()
        body = json.dumps(
            points,
            separators=(",", ":"),
            allow_nan=False,
        ).encode()
        timings["json_serialization"] = (
            monotonic_time.perf_counter() - serialization_started_at
        ) * 1000
        timings["route_total"] = (
            monotonic_time.perf_counter() - route_started_at
        ) * 1000
        stage_order = (
            "queue_and_handoff",
            "instrument_resolution",
            "metadata",
            "adapter_cache_lookup",
            "bars_http_and_json",
            "dataframe_conversion",
            "adapter_other",
            "point_conversion",
            "worker_other",
            "json_serialization",
        )
        server_timing = ", ".join(
            f"{name};dur={timings.get(name, 0.0):.3f}" for name in stage_order
        )
        logger.info(
            "chart_history_range request_id=%s symbol=%s start=%s end=%s "
            "rows=%s bytes=%s timings_ms=%s",
            request_id,
            symbol.upper(),
            start.isoformat(),
            end.isoformat(),
            len(points),
            len(body),
            json.dumps(timings, sort_keys=True, separators=(",", ":")),
        )
        return Response(
            content=body,
            media_type="application/json",
            headers={
                "Server-Timing": server_timing,
                "X-Request-ID": request_id,
                "X-Server-Duration-Ms": f'{timings["route_total"]:.3f}',
            },
        )

    async def history_batch(payload):
        return await _run_chart_read(
            _load_chart_history_batch,
            payload,
            limiter=_chart_background_read_limiter,
        )

    async def rs_line(symbol: str, period: str = "6mo", db=None):
        return await _run_chart_read(
            _load_rs_line_with_local_session,
            symbol,
            period,
            limiter=_chart_background_read_limiter,
        )

    replacements = {
        "/api/v1/stocks/{symbol}/history": history,
        "/api/v1/stocks/history/batch": history_batch,
        "/api/v1/stocks/{symbol}/rs-line": rs_line,
    }
    installed: set[str] = set()
    for route in app.routes:
        path = getattr(route, "path", None)
        replacement = replacements.get(path)
        if replacement is None:
            continue
        route.dependant.call = replacement
        installed.add(path)
    missing = set(replacements) - installed
    if missing:
        raise RuntimeError(f"stock-screener chart routes were not found: {sorted(missing)}")

    range_path = "/api/v1/stocks/{symbol}/history-range"
    if not any(getattr(route, "path", None) == range_path for route in app.routes):
        app.add_api_route(
            range_path,
            history_range,
            methods=["GET"],
            response_model=list[stocks_api.StockPriceHistoryPoint],
        )


def _install_quantstack_scan_freshness() -> None:
    """Make scan admission evaluate the same exact qfq release as scan reads."""

    read_mode = os.environ.get("SS_QUANTSTACK_READ_MODE", "legacy").strip().lower()
    if read_mode not in {"quantstack_candidate", "quantstack_primary"}:
        return

    from app.domain.scanning.models import (
        FreshnessDecision,
        FreshnessOmissionWarning,
        ScanFreshnessPolicy,
        ScanWarningCode,
    )
    from app.models.stock_universe import StockUniverse
    from app.services import market_data_freshness as freshness
    from app.wiring.bootstrap import get_market_calendar_service

    if getattr(freshness, "_quantstack_primary_installed", False):
        return

    adapter = SSQuantStackConsumerAdapter.from_environment()
    supported_cn_suffixes = {"SS", "SZ"}

    def freshness_target(calendar, market: str):
        expected = calendar.last_completed_trading_day(market)
        release_date = dataset_release_date(
            adapter.resolved_dataset_version_for_market(market)
        )
        if release_date is None:
            return expected
        previous_session = calendar.last_completed_trading_day(
            market,
            now=datetime.combine(expected, time.min, tzinfo=UTC),
        )
        if release_date == previous_session:
            logger.warning(
                "Using one-session-lagged exact qfq release for scan admission; "
                "market=%s release_date=%s calendar_expected=%s",
                market,
                release_date,
                expected,
            )
            return release_date
        return expected

    def evaluate_quantstack_freshness(symbols, *, policy=None):
        policy = policy or ScanFreshnessPolicy.strict()
        ordered = tuple(dict.fromkeys(str(value).strip().upper() for value in symbols if value))
        if not ordered:
            return FreshnessDecision(symbols_to_scan=())

        session = SessionLocal()
        try:
            universe_rows = (
                session.query(
                    StockUniverse.symbol.label("symbol"),
                    StockUniverse.market.label("market"),
                )
                .filter(StockUniverse.symbol.in_(ordered))
                .all()
            )
        finally:
            session.close()

        resolved = {row.symbol for row in universe_rows}
        unresolved = sorted(set(ordered) - resolved)
        broad_bj_exclusions = tuple(
            symbol
            for symbol in ordered
            if symbol.endswith(".BJ") and policy.allow_stale_tail
        )
        eligible_rows = [
            row
            for row in universe_rows
            if (
                row.market == "US"
                and "." not in row.symbol
            )
            or row.symbol.rsplit(".", 1)[-1] in supported_cn_suffixes
        ]
        eligible_symbols = {row.symbol for row in eligible_rows}
        unsupported_rows = [
            row
            for row in universe_rows
            if row.symbol not in eligible_symbols
            and row.symbol not in broad_bj_exclusions
        ]

        per_market: dict[str, list[Any]] = {}
        for row in eligible_rows:
            per_market.setdefault(row.market, []).append(row)

        assessments = []
        current_session_omissions: list[str] = []
        calendar = get_market_calendar_service()
        for market, rows in sorted(per_market.items()):
            try:
                expected = freshness_target(calendar, market)
                # Read only the promoted target session. This returns one row per
                # covered symbol and keeps explicit session omissions stale without
                # restoring the expensive full-history scan-admission query.
                # The US instrument lookup is a paginated local Batch API, not
                # a Provider request.  Keep scan admission inside its bounded
                # request size instead of sending the entire 5k universe as a
                # single instruments payload.
                last_dates: dict[str, date | None] = {}
                market_symbols = [row.symbol for row in rows]
                for offset in range(0, len(market_symbols), 500):
                    last_dates.update(
                        adapter.get_last_trade_dates(
                            market_symbols[offset : offset + 500],
                            start=expected,
                            end=expected,
                        )
                    )
                assessed_rows = tuple(
                    SimpleNamespace(
                        symbol=row.symbol,
                        market=row.market,
                        last_date=last_dates.get(row.symbol),
                    )
                    for row in rows
                )
                if market == "US":
                    current_session_omissions.extend(
                        row.symbol for row in assessed_rows if row.last_date is None
                    )
                    assessed_rows = tuple(
                        row for row in assessed_rows if row.last_date is not None
                    )
                stale_symbols = tuple(
                    row.symbol for row in assessed_rows if row.last_date != expected
                )
                covered = tuple(
                    row.last_date for row in assessed_rows if row.last_date is not None
                )
                assessments.append(
                    freshness._MarketFreshnessAssessment(
                        market=market,
                        rows=assessed_rows,
                        stale_symbols=stale_symbols,
                        covered_dates=covered,
                        uncovered_symbols=sum(
                            row.last_date is None for row in assessed_rows
                        ),
                        expected_date=expected,
                        oldest_last_cached_date=min(covered) if covered else None,
                    )
                )
            except Exception:
                logger.warning(
                    "QuantStack primary scan freshness failed for market=%s",
                    market,
                    exc_info=True,
                )
                assessed_rows = tuple(
                    SimpleNamespace(symbol=row.symbol, market=row.market, last_date=None)
                    for row in rows
                )
                assessments.append(
                    freshness._MarketFreshnessAssessment(
                        market=market,
                        rows=assessed_rows,
                        stale_symbols=tuple(row.symbol for row in rows),
                        uncovered_symbols=len(assessed_rows),
                        reason="quantstack_primary_unavailable",
                        hard_block=True,
                    )
                )

        if unsupported_rows:
            unsupported_symbols = {row.symbol for row in unsupported_rows}
            assessments.append(
                freshness._MarketFreshnessAssessment(
                    market="UNSUPPORTED",
                    rows=tuple(
                        SimpleNamespace(
                            symbol=row.symbol,
                            market=row.market,
                            last_date=None,
                        )
                        for row in unsupported_rows
                    ),
                    stale_symbols=tuple(
                        symbol for symbol in ordered if symbol in unsupported_symbols
                    ),
                    uncovered_symbols=len(unsupported_rows),
                    reason="quantstack_primary_unsupported",
                    hard_block=True,
                )
            )

        explicit_exclusions = tuple(
            dict.fromkeys((*broad_bj_exclusions, *current_session_omissions))
        )
        eligible_ordered = tuple(symbol for symbol in ordered if symbol not in explicit_exclusions)
        decision = freshness._decide_scan_freshness(
            ordered_symbols=eligible_ordered,
            unresolved_symbols=unresolved,
            assessments=tuple(assessments),
            policy=policy,
        )
        if decision.blocking_detail is not None or not explicit_exclusions:
            return decision

        stale_omissions = tuple(
            symbol
            for warning in decision.warnings
            for symbol in warning.omitted_symbols
        )
        omitted = tuple(dict.fromkeys((*explicit_exclusions, *stale_omissions)))
        scanned = tuple(decision.symbols_to_scan)
        expected_dates = {
            item.market: str(item.expected_date) if item.expected_date else None
            for item in assessments
            if not item.hard_block
        }
        oldest_dates = {
            item.market: (
                str(item.oldest_last_cached_date)
                if item.oldest_last_cached_date
                else None
            )
            for item in assessments
            if not item.hard_block
        }
        warning = FreshnessOmissionWarning(
            code=ScanWarningCode.STALE_TAIL_OMITTED.value,
            message=(
                f"Excluded {len(explicit_exclusions)} symbols without a bar in the "
                "accepted current-session release"
                + (f" and {len(stale_omissions)} other stale symbols." if stale_omissions else ".")
            ),
            markets=tuple(
                market
                for market, symbols in (
                    ("CN", broad_bj_exclusions),
                    ("US", tuple(current_session_omissions)),
                )
                if symbols
            ),
            omitted_symbols=omitted,
            omitted_count=len(omitted),
            total_symbols=len(ordered),
            fresh_count=len(scanned),
            freshness_rate=round(len(scanned) / len(ordered), 6),
            expected_dates=expected_dates,
            oldest_last_cached_dates=oldest_dates,
        )
        return FreshnessDecision(symbols_to_scan=scanned, warnings=(warning,))

    freshness.evaluate_symbol_freshness = evaluate_quantstack_freshness
    freshness._quantstack_primary_installed = True


def _install_stale_fundamentals_activity_recovery() -> None:
    """Let the existing stale guard release abandoned fundamentals work."""
    from app.services import market_activity_service

    market_activity_service.DATA_FETCH_RUNTIME_STAGE_KEYS = frozenset(
        (*market_activity_service.DATA_FETCH_RUNTIME_STAGE_KEYS, "fundamentals")
    )


def _install_quantstack_snapshot_freshness() -> None:
    """Anchor Daily Snapshot to qfq and preserve the legacy fallback contract."""

    read_mode = os.environ.get("SS_QUANTSTACK_READ_MODE", "legacy").strip().lower()
    if read_mode not in {"quantstack_candidate", "quantstack_primary"}:
        return

    from app.services import daily_snapshot_service

    if getattr(daily_snapshot_service, "_quantstack_freshness_installed", False):
        return

    # These values still govern the locked-upstream fallback when the durable
    # publication contract is disabled for a schema-compatible rollback.
    daily_snapshot_service.DAILY_SNAPSHOT_SCHEMA_VERSION = 5
    daily_snapshot_service.DAILY_SNAPSHOT_CACHE_TTL_SECONDS = 86_400

    original_scan_freshness = daily_snapshot_service._scan_freshness
    original_snapshot_anchor_date = daily_snapshot_service._snapshot_anchor_date
    original_key_markets = daily_snapshot_service.build_key_market_entries
    key_markets_override: ContextVar[tuple[dict[str, Any], ...] | None] = ContextVar(
        "quantstack_snapshot_key_markets_override",
        default=None,
    )

    def local_key_markets(db, market: str, *, points: int = 30, as_of_date=None):
        override = key_markets_override.get()
        if override is not None:
            return [dict(item) for item in override]
        if market.upper() != "US":
            return original_key_markets(
                db, market, points=points, as_of_date=as_of_date
            )
        base_url = os.environ.get("SS_QUANTSTACK_BASE_URL", "").rstrip("/")
        api_key = os.environ.get("SS_QUANTSTACK_API_KEY", "")
        query = urllib.parse.urlencode(
            {
                "market": "US",
                "points": points,
                **({"as_of_date": as_of_date.isoformat()} if as_of_date else {}),
            }
        )
        request = urllib.request.Request(
            f"{base_url}/market/index-snapshot?{query}",
            headers={"X-Quant-API-Key": api_key},
        )
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(request, timeout=5) as response:
            payload = json.loads(response.read().decode("utf-8"))
        return list(payload.get("key_markets", []))

    def qfq_as_of_date(scan: Any) -> date | None:
        if scan is None:
            return None
        market = str(getattr(scan, "universe_market", "") or "").strip().upper()
        for warning in getattr(scan, "warnings", None) or ():
            if not isinstance(warning, dict):
                continue
            expected_dates = warning.get("expected_dates")
            if not isinstance(expected_dates, dict):
                continue
            value = expected_dates.get(market)
            if not isinstance(value, str):
                continue
            try:
                return date.fromisoformat(value)
            except ValueError:
                continue
        return None

    def scan_freshness(scan: Any) -> dict[str, Any]:
        payload = original_scan_freshness(scan)
        as_of = qfq_as_of_date(scan)
        if as_of is not None:
            payload["scan_as_of_date"] = as_of.isoformat()
        return payload

    def snapshot_anchor_date(scan: Any) -> date | None:
        return qfq_as_of_date(scan) or original_snapshot_anchor_date(scan)

    daily_snapshot_service._scan_freshness = scan_freshness
    daily_snapshot_service._snapshot_anchor_date = snapshot_anchor_date
    daily_snapshot_service.build_key_market_entries = local_key_markets
    daily_snapshot_service._quantstack_key_markets_override = key_markets_override
    daily_snapshot_service._quantstack_freshness_installed = True


def _install_scan_queue_market_inference() -> None:
    """Route a single-market custom universe to its market scan worker."""
    from app.infra.tasks.dispatcher import CeleryTaskDispatcher

    if getattr(CeleryTaskDispatcher, "_quantstack_market_inference_installed", False):
        return

    original_dispatch = CeleryTaskDispatcher.dispatch_scan
    market_statement = text(
        """
        SELECT DISTINCT market
        FROM stock_universe
        WHERE symbol IN :symbols AND is_active = true
        """
    ).bindparams(bindparam("symbols", expanding=True))

    def dispatch_scan(
        self: Any,
        scan_id: str,
        symbols: list[str],
        criteria: dict[str, Any],
        *,
        market: str | None = None,
    ) -> str:
        resolved_market = market
        if resolved_market is None and symbols:
            with engine.connect() as connection:
                markets = {
                    str(row[0]).strip().upper()
                    for row in connection.execute(
                        market_statement,
                        {"symbols": list(dict.fromkeys(symbols))},
                    )
                    if row[0]
                }
            if len(markets) == 1:
                resolved_market = markets.pop()
        return original_dispatch(
            self,
            scan_id,
            symbols,
            criteria,
            market=resolved_market,
        )

    CeleryTaskDispatcher.dispatch_scan = dispatch_scan
    CeleryTaskDispatcher._quantstack_market_inference_installed = True


def _install_quantstack_post_scan_analytics() -> None:
    """Atomically publish UI analytics and its durable Snapshot response."""

    read_mode = os.environ.get("SS_QUANTSTACK_READ_MODE", "legacy").strip().lower()
    if read_mode not in {"quantstack_candidate", "quantstack_primary"}:
        return
    from app.services import scan_execution
    from app.tasks import scan_tasks
    from quantstack_scan_analytics import sync_scan_analytics

    if getattr(scan_execution, "_quantstack_post_scan_analytics_installed", False):
        return

    def publish_analytics(scan_id: str) -> None:
        result = sync_scan_analytics(scan_id)
        logger.info("Published QuantStack scan analytics: %s", result)

    original_service = scan_execution.run_post_scan_pipeline

    @wraps(original_service)
    def run_service_post_scan_pipeline(scan_id: str, *, session_factory: Any) -> None:
        original_service(scan_id, session_factory=session_factory)
        publish_analytics(scan_id)

    original_task = scan_tasks._run_post_scan_pipeline

    @wraps(original_task)
    def run_task_post_scan_pipeline(scan_id: str, *, warm_chart_cache: bool = True) -> None:
        original_task(scan_id, warm_chart_cache=warm_chart_cache)
        publish_analytics(scan_id)

    scan_execution.run_post_scan_pipeline = run_service_post_scan_pipeline
    scan_tasks._run_post_scan_pipeline = run_task_post_scan_pipeline
    scan_execution._quantstack_post_scan_analytics_installed = True


app.router.lifespan_context = _quant_workspace_lifespan
_install_decision_dashboard_sanitizer()
_install_quantstack_stock_info()
_install_cache_only_stock_fundamentals()
if os.environ.get("T111_LEGACY_FIXTURE_REPLAY_ENABLED", "").lower() == "true":
    from legacy_fixture_replay import install_legacy_fixture_replay

    install_legacy_fixture_replay(YFinanceService)
install_price_cache_read_modes(
    PriceCacheService,
    adapter_factory=_chart_adapter_factory,
)
install_yfinance_read_modes(YFinanceService, PriceCacheService)
_install_nonblocking_chart_reads()
_install_stale_fundamentals_activity_recovery()
_install_quantstack_scan_freshness()
_install_quantstack_snapshot_freshness()
_install_scan_queue_market_inference()
_install_quantstack_post_scan_analytics()


_t078_control_lock = threading.Lock()


def _require_t078_control(x_t078_control_token: str | None) -> None:
    expected = os.environ.get("T078_CONTROL_TOKEN", "")
    if (
        os.environ.get("SS_RUNTIME_ENV") != "isolated-shadow"
        or not expected
        or x_t078_control_token != expected
    ):
        raise HTTPException(status_code=404, detail="not found")


def _t078_worker():
    worker = PriceCacheService._quantstack_shadow_worker
    if worker is None:
        raise HTTPException(status_code=503, detail="shadow worker unavailable")
    return worker


def _t078_report_files() -> list[str]:
    root = Path(os.environ["SS_QUANTSTACK_SHADOW_REPORT_ROOT"]).resolve()
    return sorted(str(path.relative_to(root)) for path in root.glob("*/*.json"))


def _t078_frame_summary(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        frames = {key: frame for key, frame in value.items() if hasattr(frame, "to_json")}
        rows = sum(len(frame) for frame in frames.values())
    elif hasattr(value, "to_json"):
        rows = len(value)
    else:
        rows = 0
    return {"row_count": rows}


@app.get("/__t078/control/health", include_in_schema=False)
def t078_control_health(
    wait_for_idle: bool = False,
    x_t078_control_token: str | None = Header(default=None),
):
    _require_t078_control(x_t078_control_token)
    worker = _t078_worker()
    idle = True
    if wait_for_idle:
        idle = worker.wait_for_idle(timeout_seconds=30)
    return {
        "worker": vars(worker.health_snapshot()),
        "idle": idle,
        "report_files": _t078_report_files(),
    }


@app.post("/__t078/control/read", include_in_schema=False)
def t078_control_read(
    payload: dict[str, Any],
    x_t078_control_token: str | None = Header(default=None),
):
    """Invoke one controlled legacy read in the sole instrumented server process."""

    _require_t078_control(x_t078_control_token)
    if not _t078_control_lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="another T-078 control read is active")
    try:
        method_name = str(payload.get("method", ""))
        symbols = [str(value).strip().upper() for value in payload.get("symbols", [])]
        period = str(payload.get("period", "2y"))
        if not symbols or len(symbols) > 50:
            raise HTTPException(status_code=422, detail="invalid T-078 symbol count")
        if period not in {"6mo", "1y", "2y", "5y"}:
            raise HTTPException(status_code=422, detail="invalid T-078 period")
        allowed = {
            "seed",
            "get_historical_data",
            "get_cached_only",
            "get_cached_only_fresh",
            "get_many",
            "get_many_cached_only",
            "get_many_cached_only_fresh",
        }
        if method_name not in allowed:
            raise HTTPException(status_code=422, detail="invalid T-078 read method")
        prices = PriceCacheService()
        if method_name == "seed":
            if len(symbols) != 1:
                raise HTTPException(status_code=422, detail="seed accepts one symbol")
            value = YFinanceService().get_historical_data(
                symbols[0], period=period, interval="1d", use_cache=False
            )
            if value is None or value.empty:
                raise HTTPException(status_code=502, detail="legacy seed returned no rows")
            prices._store_in_database(symbols[0], value)
        elif method_name.startswith("get_many"):
            value = getattr(prices, method_name)(symbols, period=period)
        else:
            if len(symbols) != 1:
                raise HTTPException(status_code=422, detail="single read accepts one symbol")
            value = getattr(prices, method_name)(symbols[0], period=period)
        return {"method": method_name, "symbols": symbols, **_t078_frame_summary(value)}
    finally:
        _t078_control_lock.release()


@app.post("/__t078/control/scan", include_in_schema=False)
def t078_control_scan(
    payload: dict[str, Any],
    x_t078_control_token: str | None = Header(default=None),
):
    """Run the pinned synchronous scanner over one frozen-corpus symbol."""

    _require_t078_control(x_t078_control_token)
    if not _t078_control_lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="another T-078 control read is active")
    try:
        symbols = [str(value).strip().upper() for value in payload.get("symbols", [])]
        screeners = [str(value) for value in payload.get("screeners", [])]
        if payload.get("universe") != "custom" or len(symbols) != 1:
            raise HTTPException(status_code=422, detail="invalid T-078 scan universe")
        if screeners != ["minervini"]:
            raise HTTPException(status_code=422, detail="invalid T-078 scanner set")
        if payload.get("composite_method") != "weighted_average":
            raise HTTPException(status_code=422, detail="invalid T-078 composite method")
        allowlist = {
            value.strip().upper()
            for value in os.environ.get("SS_QUANTSTACK_SHADOW_ALLOWLIST", "").split(",")
            if value.strip()
        }
        if symbols[0] not in allowlist:
            raise HTTPException(status_code=422, detail="symbol outside T-078 corpus")
        result = get_scan_orchestrator().scan_stock_multi(
            symbol=symbols[0],
            screener_names=screeners,
            criteria={"include_vcp": False},
            composite_method="weighted_average",
        )
        return {
            "surface": "scan_orchestrator.scan_stock_multi",
            "universe": "custom",
            "symbols": symbols,
            "result": convert_numpy_types(result),
        }
    finally:
        _t078_control_lock.release()
