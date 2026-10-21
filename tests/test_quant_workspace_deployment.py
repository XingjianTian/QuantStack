import json
import re
import runpy
from pathlib import Path

import yaml
from yaml.constructor import SafeConstructor

ROOT = Path(__file__).parents[1]


class ComposeLoader(yaml.SafeLoader):
    pass


ComposeLoader.add_constructor("!override", SafeConstructor.construct_sequence)


def test_stock_screener_upstream_is_reproducibly_locked() -> None:
    lock = json.loads((ROOT / "VERSION_LOCK.json").read_text(encoding="utf-8"))
    upstream = lock["upstreams"]["stock_screener"]

    assert upstream["source_tag"] == "v1.4.0"
    assert len(upstream["tag_object"]) == 40
    assert len(upstream["commit"]) == 40
    assert upstream["tag_object"] != upstream["commit"]
    assert "@sha256:" in lock["container_images"]["stock_screener_backend"]
    assert "@sha256:" in lock["container_images"]["stock_screener_frontend"]
    assert lock["applications"]["stock_screener_frontend_patch"] == (
        "1.4.0-name-first.35"
    )
    adapter_source = (
        ROOT / "docker" / "stock-screener" / "quantstack_consumer_adapter.py"
    ).read_text(encoding="utf-8")
    adapter_version_match = re.search(
        r'^ADAPTER_VERSION = "([^"]+)"$', adapter_source, re.MULTILINE
    )
    assert adapter_version_match is not None
    assert lock["applications"]["stock_screener_quantstack_consumer_adapter"] == (
        adapter_version_match.group(1)
    )
    assert upstream["commit"] == "da4d01a83e420ac9c148ff4fe0260ee1e4228885"
    assert "patches 0001-0034" in lock["container_images"]["stock_screener_frontend_downstream"]


def test_stock_screener_downstream_patch_is_versioned_and_rebuildable() -> None:
    ticker_patch = ROOT / "docker" / "stock-screener" / "patches" / (
        "0001-ticker-cell-company-name-first.patch"
    )
    snapshot_patch = ROOT / "docker" / "stock-screener" / "patches" / (
        "0002-daily-snapshot-name-first-and-compact-health.patch"
    )
    chart_patch = ROOT / "docker" / "stock-screener" / "patches" / (
        "0003-chart-viewer-fit-available-height.patch"
    )
    performance_patch = ROOT / "docker" / "stock-screener" / "patches" / (
        "0004-kline-priority-and-timeframes.patch"
    )
    closure_patch = ROOT / "docker" / "stock-screener" / "patches" / (
        "0005-dual-market-production-closure.patch"
    )
    peer_patch = ROOT / "docker" / "stock-screener" / "patches" / (
        "0006-peer-modal-pagination.patch"
    )
    acceptance_patch = ROOT / "docker" / "stock-screener" / "patches" / (
        "0007-production-acceptance-corrections.patch"
    )
    history_patch = ROOT / "docker" / "stock-screener" / "patches" / (
        "0008-complete-history-timeframes.patch"
    )
    followup_patch = ROOT / "docker" / "stock-screener" / "patches" / (
        "0009-production-acceptance-followups.patch"
    )
    browser_patch = ROOT / "docker" / "stock-screener" / "patches" / (
        "0010-browser-acceptance-corrections.patch"
    )
    market_switch_patch = ROOT / "docker" / "stock-screener" / "patches" / (
        "0011-market-switch-daily-tab.patch"
    )
    incremental_patch = ROOT / "docker" / "stock-screener" / "patches" / (
        "0012-ss-home-and-incremental-history.patch"
    )
    priority_patch = ROOT / "docker" / "stock-screener" / "patches" / (
        "0013-cn-chart-primary-priority.patch"
    )
    rs_on_demand_patch = ROOT / "docker" / "stock-screener" / "patches" / (
        "0014-cn-rs-on-demand.patch"
    )
    expansion_gesture_patch = ROOT / "docker" / "stock-screener" / "patches" / (
        "0015-history-expansion-gesture.patch"
    )
    history_range_patch = ROOT / "docker" / "stock-screener" / "patches" / (
        "0016-history-range-route.patch"
    )
    symbol_scope_patch = ROOT / "docker" / "stock-screener" / "patches" / (
        "0017-symbol-scoped-chart-instance.patch"
    )
    streaming_history_patch = ROOT / "docker" / "stock-screener" / "patches" / (
        "0018-detail-chart-streaming-history.patch"
    )
    interaction_closure_patch = ROOT / "docker" / "stock-screener" / "patches" / (
        "0019-kline-tradingview-interaction-closure.patch"
    )
    cold_first_read_patch = ROOT / "docker" / "stock-screener" / "patches" / (
        "0020-kline-cold-first-read-prefetch.patch"
    )
    eager_history_patch = ROOT / "docker" / "stock-screener" / "patches" / (
        "0021-kline-eager-history-prefetch.patch"
    )
    deep_first_request_patch = ROOT / "docker" / "stock-screener" / "patches" / (
        "0022-kline-deep-first-request.patch"
    )
    parallel_dashboard_patch = ROOT / "docker" / "stock-screener" / "patches" / (
        "0023-stock-chart-parallel-dashboard.patch"
    )
    empty_cold_range_patch = ROOT / "docker" / "stock-screener" / "patches" / (
        "0024-kline-empty-cold-range-guard.patch"
    )
    foreground_priority_patch = ROOT / "docker" / "stock-screener" / "patches" / (
        "0025-stock-chart-foreground-priority.patch"
    )
    eager_chart_modal_patch = ROOT / "docker" / "stock-screener" / "patches" / (
        "0026-chart-modal-eager-interaction.patch"
    )
    initial_chart_symbol_patch = ROOT / "docker" / "stock-screener" / "patches" / (
        "0027-chart-navigation-initial-symbol.patch"
    )
    progressive_first_paint_patch = ROOT / "docker" / "stock-screener" / "patches" / (
        "0028-kline-progressive-first-paint.patch"
    )
    multitimeframe_history_patch = ROOT / "docker" / "stock-screener" / "patches" / (
        "0029-multitimeframe-shared-history.patch"
    )
    assert (
        ROOT / "docker" / "stock-screener" / "patches" /
        "0030-kline-history-foreground-priority.patch"
    ).exists()
    viewport_paging_patch = ROOT / "docker" / "stock-screener" / "patches" / (
        "0031-kline-viewport-paging.patch"
    )
    background_preemption_patch = ROOT / "docker" / "stock-screener" / "patches" / (
        "0032-kline-background-preemption.patch"
    )
    range_first_paint_patch = ROOT / "docker" / "stock-screener" / "patches" / (
        "0033-kline-range-first-paint.patch"
    )
    dual_market_loading_patch = ROOT / "docker" / "stock-screener" / "patches" / (
        "0034-kline-dual-market-loading-policy.patch"
    )
    dockerfile = (
        ROOT / "docker" / "stock-screener" / "Dockerfile.frontend-downstream"
    ).read_text(encoding="utf-8")

    ticker_patch_text = ticker_patch.read_text(encoding="utf-8")
    snapshot_patch_text = snapshot_patch.read_text(encoding="utf-8")
    chart_patch_text = chart_patch.read_text(encoding="utf-8")
    performance_patch_text = performance_patch.read_text(encoding="utf-8")
    closure_patch_text = closure_patch.read_text(encoding="utf-8")
    peer_patch_text = peer_patch.read_text(encoding="utf-8")
    acceptance_patch_text = acceptance_patch.read_text(encoding="utf-8")
    history_patch_text = history_patch.read_text(encoding="utf-8")
    followup_patch_text = followup_patch.read_text(encoding="utf-8")
    browser_patch_text = browser_patch.read_text(encoding="utf-8")
    market_switch_patch_text = market_switch_patch.read_text(encoding="utf-8")
    incremental_patch_text = incremental_patch.read_text(encoding="utf-8")
    priority_patch_text = priority_patch.read_text(encoding="utf-8")
    rs_on_demand_patch_text = rs_on_demand_patch.read_text(encoding="utf-8")
    expansion_gesture_patch_text = expansion_gesture_patch.read_text(encoding="utf-8")
    history_range_patch_text = history_range_patch.read_text(encoding="utf-8")
    symbol_scope_patch_text = symbol_scope_patch.read_text(encoding="utf-8")
    streaming_history_patch_text = streaming_history_patch.read_text(encoding="utf-8")
    interaction_closure_patch_text = interaction_closure_patch.read_text(encoding="utf-8")
    cold_first_read_patch_text = cold_first_read_patch.read_text(encoding="utf-8")
    assert "TickerCell.jsx" in ticker_patch_text
    assert "TickerCell.test.jsx" in ticker_patch_text
    assert "{displayName}" in ticker_patch_text
    assert "DailyMarketSnapshotTab.jsx" in snapshot_patch_text
    assert "MarketHealthExposure.jsx" in snapshot_patch_text
    assert 'data-testid="market-health-summary"' in snapshot_patch_text
    assert "ChartViewerModal.jsx" in chart_patch_text
    assert 'height="100%"' in chart_patch_text
    assert "flexShrink: 0" in chart_patch_text
    assert "CHART_TIMEFRAMES" in performance_patch_text
    assert "Monthly K" in performance_patch_text
    assert "Quarterly K" in performance_patch_text
    assert "Yearly K" in performance_patch_text
    assert "aggregateByTimeframe" in performance_patch_text
    assert "chartZoomDirection" in performance_patch_text
    assert "setVisibleLogicalRange" in performance_patch_text
    assert "chartReadySymbol === currentSymbol" in performance_patch_text
    assert "useLayoutEffect" in performance_patch_text
    assert "ChartViewerModal.readiness.test.jsx" in performance_patch_text
    assert "requires fresh readiness after closing and reopening" in performance_patch_text
    assert "currentIndex + 3" in performance_patch_text
    assert "useNavigate" in closure_patch_text
    assert "No group rankings have been published" in closure_patch_text
    assert "universeSyncedMarketRef" in closure_patch_text
    assert "formatLocalCurrency" in closure_patch_text
    assert "TablePagination" in peer_patch_text
    assert "paginatePeerRows" in peer_patch_text
    assert "pannedLogicalRange" in acceptance_patch_text
    assert "filterRowsForMarket" in acceptance_patch_text
    assert "usesBundledPriceData" in history_patch_text
    assert "CHART_TIMEFRAMES.map" in history_patch_text
    assert "marketFromRow" in followup_patch_text
    assert "marginThreshold: 4" in followup_patch_text
    assert "chartWheelIntent" in followup_patch_text
    assert "MARKET_STORAGE_KEY" in browser_patch_text
    assert "errorRankings?.status" in browser_patch_text
    assert "setSelectedTab(0)" in market_switch_patch_text
    assert "fetchPriceHistoryRange" in incremental_patch_text
    assert "priceRequestSettled" in incremental_patch_text
    assert "Warming adjacent CN symbols competes" in priority_patch_text
    assert "does not warm adjacent CN charts" in priority_patch_text
    assert "defaultShowRsLine" in rs_on_demand_patch_text
    assert "retaining the US default" in rs_on_demand_patch_text
    assert "collapses a wheel burst" in expansion_gesture_patch_text
    assert "/history-range" in history_range_patch_text
    assert "key={currentSymbol}" in symbol_scope_patch_text
    assert "remounts chart state when the selected symbol changes" in symbol_scope_patch_text
    assert "STREAMING_HISTORY_PREFETCH_BARS = 50" in streaming_history_patch_text
    assert "STREAMING_HISTORY_PAGE_DAYS = 400" in streaming_history_patch_text
    assert "subscribeVisibleLogicalRangeChange" in streaming_history_patch_text
    assert "keeps only one older-page request in flight" in streaming_history_patch_text
    assert "shiftedLogicalRangeAfterPrepend" in interaction_closure_patch_text
    assert "chartPinchZoomDirection" in interaction_closure_patch_text
    assert "keeps pan width fixed while prepending history" in interaction_closure_patch_text
    assert "STREAMING_HISTORY_PAGE_DAYS = 3650" in cold_first_read_patch_text
    assert "historyPrefetchBarsForRange" in cold_first_read_patch_text
    assert "historyEagerStartedRef" in eager_history_patch.read_text(encoding="utf-8")
    deep_first_request_patch_text = deep_first_request_patch.read_text(encoding="utf-8")
    assert "INTERACTIVE_DAILY_HISTORY_PERIOD = '10y'" in deep_first_request_patch_text
    assert "INTERACTIVE_DAILY_VISIBLE_BARS = 130" in deep_first_request_patch_text
    parallel_dashboard_patch_text = parallel_dashboard_patch.read_text(encoding="utf-8")
    assert (
        "mounts the deep-history chart before the decision dashboard resolves"
        in parallel_dashboard_patch_text
    )
    assert "<StockChartWorkspace symbol={symbol} />" in parallel_dashboard_patch_text
    empty_cold_range_patch_text = empty_cold_range_patch.read_text(encoding="utf-8")
    assert "if (candleCount === 0) return" in empty_cold_range_patch_text
    foreground_priority_patch_text = foreground_priority_patch.read_text(encoding="utf-8")
    assert (
        "enabled: Boolean(symbol && requestProfile && chartReady)"
        in foreground_priority_patch_text
    )
    eager_chart_modal_patch_text = eager_chart_modal_patch.read_text(encoding="utf-8")
    assert "import ChartViewerModal from './ChartViewerModal'" in eager_chart_modal_patch_text
    assert "lazy(() => import('./ChartViewerModal'))" in eager_chart_modal_patch_text
    initial_chart_symbol_patch_text = initial_chart_symbol_patch.read_text(encoding="utf-8")
    assert "symbols.indexOf(initialSymbol)" in initial_chart_symbol_patch_text
    assert "never exposes the first row" in initial_chart_symbol_patch_text
    progressive_first_paint_patch_text = progressive_first_paint_patch.read_text(encoding="utf-8")
    assert "INTERACTIVE_DAILY_HISTORY_PERIOD = '2y'" in progressive_first_paint_patch_text
    assert "starts one predecessor read" in progressive_first_paint_patch_text
    multitimeframe_history_patch_text = multitimeframe_history_patch.read_text(encoding="utf-8")
    assert "usesSharedInteractiveHistory" in multitimeframe_history_patch_text
    assert "historyPageIsExhausted" in multitimeframe_history_patch_text
    assert "visibleBarsForTimeframe" in multitimeframe_history_patch_text
    viewport_paging_patch_text = viewport_paging_patch.read_text(encoding="utf-8")
    background_preemption_patch_text = background_preemption_patch.read_text(encoding="utf-8")
    range_first_paint_patch_text = range_first_paint_patch.read_text(encoding="utf-8")
    dual_market_loading_patch_text = dual_market_loading_patch.read_text(encoding="utf-8")
    assert "STREAMING_HISTORY_PAGE_DAYS = 400" in viewport_paging_patch_text
    assert "historyDemandArmedRef.current = false" in viewport_paging_patch_text
    assert "BACKGROUND_IDLE_DELAY_MS = 1500" in viewport_paging_patch_text
    assert "queryClient.cancelQueries" in viewport_paging_patch_text
    assert "paints two years without starting a predecessor read" in viewport_paging_patch_text
    assert "onForegroundHistoryStart" in background_preemption_patch_text
    assert "backgroundBatchAbortRef" in background_preemption_patch_text
    assert "revokes and cancels latched background work" in background_preemption_patch_text
    assert "currentHistoryPage" in range_first_paint_patch_text
    assert "fetchPriceHistoryRange" in range_first_paint_patch_text
    assert "fetchPriceHistory(symbol, historyPeriod" in range_first_paint_patch_text
    assert "uses the same abortable adjacent prefetch for CN" in dual_market_loading_patch_text
    assert "-    if (/\\.(SS|SZ)$/i.test(currentSymbol)) return;" in dual_market_loading_patch_text
    assert 'LABEL org.opencontainers.image.version="1.4.0-name-first.35"' in dockerfile
    assert "0005-dual-market-production-closure.patch" in dockerfile
    assert "0006-peer-modal-pagination.patch" in dockerfile
    assert "0007-production-acceptance-corrections.patch" in dockerfile
    assert "0008-complete-history-timeframes.patch" in dockerfile
    assert "0009-production-acceptance-followups.patch" in dockerfile
    assert "0010-browser-acceptance-corrections.patch" in dockerfile
    assert "0011-market-switch-daily-tab.patch" in dockerfile
    assert "0012-ss-home-and-incremental-history.patch" in dockerfile
    assert "0013-cn-chart-primary-priority.patch" in dockerfile
    assert "0014-cn-rs-on-demand.patch" in dockerfile
    assert "0015-history-expansion-gesture.patch" in dockerfile
    assert "0016-history-range-route.patch" in dockerfile
    assert "0017-symbol-scoped-chart-instance.patch" in dockerfile
    assert "0018-detail-chart-streaming-history.patch" in dockerfile
    assert "0019-kline-tradingview-interaction-closure.patch" in dockerfile
    assert "0020-kline-cold-first-read-prefetch.patch" in dockerfile
    assert "0021-kline-eager-history-prefetch.patch" in dockerfile
    assert "0022-kline-deep-first-request.patch" in dockerfile
    assert "0023-stock-chart-parallel-dashboard.patch" in dockerfile
    assert "0024-kline-empty-cold-range-guard.patch" in dockerfile
    assert "0025-stock-chart-foreground-priority.patch" in dockerfile
    assert "0026-chart-modal-eager-interaction.patch" in dockerfile
    assert "0027-chart-navigation-initial-symbol.patch" in dockerfile
    assert "0028-kline-progressive-first-paint.patch" in dockerfile
    assert "0029-multitimeframe-shared-history.patch" in dockerfile
    assert "0030-kline-history-foreground-priority.patch" in dockerfile
    assert "0031-kline-viewport-paging.patch" in dockerfile
    assert "0032-kline-background-preemption.patch" in dockerfile
    assert "0033-kline-range-first-paint.patch" in dockerfile
    assert "0034-kline-dual-market-loading-policy.patch" in dockerfile
    assert "node:22-alpine@sha256:" in dockerfile
    assert "ARG VITE_API_URL=/api" in dockerfile
    assert "COPY --from=builder /app/dist/ /usr/share/nginx/html/" in dockerfile
    assert "resolver 127.0.0.11 valid=10s ipv6=off" in dockerfile
    assert "http://$backend_upstream$request_uri" in dockerfile


def test_stock_screener_celery_policy_is_versioned_and_rebuildable() -> None:
    lock = json.loads((ROOT / "VERSION_LOCK.json").read_text(encoding="utf-8"))
    dockerfile = (
        ROOT / "docker" / "stock-screener" / "Dockerfile.backend-downstream"
    ).read_text(encoding="utf-8")
    patcher = (
        ROOT
        / "docker"
        / "stock-screener"
        / "patches"
        / "patch_celery_schedule.py"
    ).read_text(encoding="utf-8")

    assert lock["applications"]["stock_screener_celery_slimming"] == (
        "1.4.0-celery-slim.2"
    )
    assert "stock-screener-backend-downstream:1.4.0-quant-runtime.16" in (
        lock["container_images"]["stock_screener_backend_downstream"]
    )
    assert "ARG UPSTREAM_IMAGE" in dockerfile
    assert "/app/app/celery_app.py" in dockerfile
    assert "settings.enabled_markets_list" in patcher
    assert "SS_MARKET_JOBS_ENABLED" in patcher
    assert "if not settings.feature_themes" in patcher
    assert 'org.opencontainers.image.version="1.4.0-quant-runtime.16"' in dockerfile
    assert "quantstack_scan_analytics.py" in dockerfile
    assert "reconcile_scan_analytics.py" in dockerfile
    assert "20260905_0026_add_scan_analytics_publication.py" in dockerfile
    assert "20260905_0027_add_daily_snapshot_publications.py" in dockerfile
    assert "daily_snapshot_publication_model.py" in dockerfile
    assert "daily_snapshot_publication.py" in dockerfile
    assert "patch_snapshot_analytics_readiness.py" in dockerfile
    assert "patch_runtime_recovery.py" in dockerfile

    publication_migration = (
        ROOT
        / "docker"
        / "stock-screener"
        / "migrations"
        / "20260905_0027_add_daily_snapshot_publications.py"
    ).read_text(encoding="utf-8")
    assert 'down_revision = "20260905_0026"' in publication_migration
    assert 'ondelete="CASCADE"' in publication_migration
    assert "payload_json" in publication_migration
    assert "payload_sha256" in publication_migration
    assert "daily_snapshot_publication_inputs" in publication_migration
    assert "key_markets_json" in publication_migration
    assert "key_markets_sha256" in publication_migration

    compat = (
        ROOT / "docker" / "stock-screener" / "quant_workspace_compat.py"
    ).read_text(encoding="utf-8")
    assert "_prewarm_daily_snapshot" not in compat
    assert "_quantstack_key_markets_override" in compat

    migration = (
        ROOT
        / "docker"
        / "stock-screener"
        / "migrations"
        / "20260826_0025_add_daily_snapshot_filter_indexes.py"
    ).read_text(encoding="utf-8")
    assert "20260701_0024" in migration
    assert "avg_dollar_volume" in migration
    assert "ibd_group_rank" in migration
    assert "CREATE INDEX IF NOT EXISTS" in migration
    assert "20260826_0025_add_daily_snapshot_filter_indexes.py" in dockerfile

    runtime_patcher = (
        ROOT
        / "docker"
        / "stock-screener"
        / "patches"
        / "patch_runtime_recovery.py"
    ).read_text(encoding="utf-8")
    assert "runtime recovery anchor drifted" in runtime_patcher
    assert "readiness_active = bootstrap_state" in runtime_patcher
    assert "_clear_orphaned_runtime_activity" in runtime_patcher
    assert 'status="idle"' in runtime_patcher


def test_stock_screener_celery_patcher_fails_closed_on_anchor_drift() -> None:
    patcher_path = (
        ROOT
        / "docker"
        / "stock-screener"
        / "patches"
        / "patch_celery_schedule.py"
    )
    patch_source = runpy.run_path(str(patcher_path))["patch_source"]
    locked_source_shape = """    _enabled_markets = list(SUPPORTED_MARKETS)
    # --- Market-agnostic / shared beat entries below ---
    # Merge shared entries into the fanned-out schedule and install.
"""

    patched = patch_source(locked_source_shape)
    assert "settings.enabled_markets_list" in patched
    assert "SS_MARKET_JOBS_ENABLED" in patched
    assert "if not settings.feature_themes:" in patched

    for anchor in (
        "    _enabled_markets = list(SUPPORTED_MARKETS)\n",
        "    # --- Market-agnostic / shared beat entries below ---\n",
        "    # Merge shared entries into the fanned-out schedule and install.\n",
    ):
        drifted = locked_source_shape.replace(anchor, "", 1)
        try:
            patch_source(drifted)
        except ValueError as exc:
            assert "expected exactly one" in str(exc)
        else:
            raise AssertionError(f"patcher accepted missing anchor: {anchor!r}")


def test_quant_workspace_defaults_are_us_first_and_private() -> None:
    env_text = (
        ROOT / "docker" / "stock-screener" / "nas.env.example"
    ).read_text(encoding="utf-8")

    assert "ENABLED_MARKETS=US,CN" in env_text
    assert "FRONTEND_PORT=127.0.0.1:18910" in env_text
    assert "WEB_CONCURRENCY=1" in env_text
    assert "QUANT_WORKSPACE_BACKEND_IMAGE_REF=" in env_text
    assert (
        "QUANT_WORKSPACE_BACKEND_IMAGE_REF="
        "personal/stock-screener-backend-downstream:1.4.0-quant-runtime.16"
        in env_text
    )
    assert "QUANT_WORKSPACE_BACKEND_IMAGE_REF=ghcr.io/xang1234/" not in env_text
    assert "QUANT_WORKSPACE_FRONTEND_IMAGE_REF=" in env_text
    assert "LLM_FALLBACK_ENABLED=false" in env_text
    assert "GROQ_API_KEY=\n" in env_text
    assert "TAVILY_API_KEY=\n" in env_text
    assert "SS_QUANTSTACK_READ_MODE=quantstack_primary" in env_text
    assert "SS_QUANTSTACK_PRIMARY_ENABLED=true" in env_text
    assert "SS_QUANTSTACK_SHADOW_ENABLED=false" in env_text
    assert "SS_QUANTSTACK_SHADOW_SAMPLE_RATE=0" in env_text
    assert "SS_QUANTSTACK_ALLOW_CANDIDATE=false" in env_text
    assert "SS_REQUIRED_SCAN_ANALYTICS_VERSION=quantstack.scan-analytics.v1" in env_text
    assert "SS_RUNTIME_ENV=production" in env_text
    assert "SS_MARKET_JOBS_ENABLED=false" in env_text
    assert (
        "SS_QUANTSTACK_DATASET_VERSION="
        "daily-through-20260831.qfq-session-v1.materialized-range8-rg8192-v2" in env_text
    )
    assert "SS_QUANTSTACK_SHADOW_REPORT_ROOT=/app/data/quantstack-shadow-reports" in env_text
    assert "SS_QUANTSTACK_SHADOW_QUEUE_SIZE=128" in env_text
    assert "SS_QUANTSTACK_CACHE_MAX_ENTRIES=2048" in env_text
    assert "SS_QUANTSTACK_US_BARS_DATASET_ID=us_equity_total_return" in env_text
    assert "SS_QUANTSTACK_US_INSTRUMENTS_DATASET_ID=instruments_us" in env_text
    assert "SS_QUANTSTACK_US_DEFAULT=true" in env_text


def test_stock_screener_shell_version_is_consistent_across_entry_and_deploy() -> None:
    lock = json.loads((ROOT / "VERSION_LOCK.json").read_text(encoding="utf-8"))
    shell_version = lock["applications"]["stock_screener_localization"]
    center_version = lock["applications"]["quant_center_web"]
    env_text = (
        ROOT / "docker" / "stock-screener" / "nas.env.example"
    ).read_text(encoding="utf-8")
    center_source = (ROOT / "apps" / "quant-center" / "src" / "App.tsx").read_text(
        encoding="utf-8"
    )
    compose = (ROOT / "docker" / "compose.nas.yaml").read_text(encoding="utf-8")

    assert f"stock-screener-frontend-zh:{shell_version}" in env_text
    assert f'const stockScreenerShellVersion = "{shell_version}"' in center_source
    assert f"personal/quant-center-web:{center_version}" in compose


def test_quant_workspace_overlay_limits_cn_workers_without_replacing_upstream() -> None:
    overlay_text = (
        ROOT / "docker" / "stock-screener" / "compose.quant.yaml"
    ).read_text(encoding="utf-8")
    overlay = yaml.load(
        overlay_text,
        Loader=ComposeLoader,
    )

    assert overlay["name"] == "quant-workspace"
    services = overlay["services"]
    assert "build" not in services["backend"]
    assert services["backend"]["image"].startswith("${QUANT_WORKSPACE_BACKEND_IMAGE_REF:")
    assert services["frontend"]["image"].startswith("${QUANT_WORKSPACE_FRONTEND_IMAGE_REF:")
    assert "ports" not in services["frontend"]
    assert services["backend"]["environment"]["WEB_CONCURRENCY"] == "1"
    assert services["backend"]["environment"]["FEATURE_CHATBOT"] == "false"
    assert services["backend"]["environment"]["FEATURE_THEMES"] == "false"
    assert services["backend"]["command"].startswith(
        "uvicorn quant_workspace_compat:app"
    )
    assert (
        "./quant_workspace_compat.py:/app/quant_workspace_compat.py:ro"
        in services["backend"]["volumes"]
    )
    assert (
        "./quantstack_consumer_adapter.py:/app/quantstack_consumer_adapter.py:ro"
        in services["backend"]["volumes"]
    )
    assert services["backend"]["environment"]["SS_QUANTSTACK_READ_MODE"].endswith(
        ":-quantstack_primary}"
    )
    assert services["backend"]["environment"]["SS_QUANTSTACK_PRIMARY_ENABLED"].endswith(
        ":-true}"
    )
    assert services["backend"]["environment"]["SS_QUANTSTACK_SHADOW_ENABLED"].endswith(
        ":-false}"
    )
    assert services["backend"]["environment"]["SS_SCAN_ANALYTICS_TIMEOUT_SECONDS"].endswith(
        ":-30}"
    )
    assert services["backend"]["environment"]["SS_SCAN_ANALYTICS_MAX_ATTEMPTS"].endswith(
        ":-1}"
    )
    assert services["backend"]["environment"]["SS_QUANTSTACK_ALLOW_CANDIDATE"].endswith(
        ":-false}"
    )
    assert services["backend"]["environment"]["SS_RUNTIME_ENV"].endswith(
        ":-production}"
    )
    assert services["backend"]["environment"]["SS_QUANTSTACK_SHADOW_REPORT_ROOT"].endswith(
        ":-/app/data/quantstack-shadow-reports}"
    )
    assert services["proxy-relay"]["network_mode"] == "host"
    assert (
        services["proxy-relay"]["environment"]["PROXY_RELAY_BIND_ADDRESS"]
        == "${PROXY_RELAY_BIND_ADDRESS:-127.0.0.1}"
    )
    assert "ports" not in services["proxy-relay"]
    assert services["celery-datafetch"]["environment"]["HTTPS_PROXY"] == (
        "${QUANT_WORKSPACE_HTTP_PROXY:-}"
    )
    assert services["celery-general"]["environment"]["SS_MARKET_JOBS_ENABLED"] == (
        "${SS_MARKET_JOBS_ENABLED:-false}"
    )
    assert services["celery-userscans"]["profiles"] == [
        "ss-shared-scan-fallback"
    ]
    assert services["celery-marketjobs-cn"]["profiles"] == [
        "ss-market-maintenance"
    ]
    assert 'profiles: !override ["ss-market-maintenance"]' in overlay_text
    assert (
        services["celery-datafetch"]["deploy"]["resources"]["limits"]["memory"]
        == "768M"
    )
    assert (
        services["celery-marketjobs-cn"]["deploy"]["resources"]["limits"]["memory"]
        == "768M"
    )
    assert (
        services["celery-userscans-cn"]["deploy"]["resources"]["limits"]["memory"]
        == "1G"
    )
    cn_scan_worker = services["celery-userscans-cn"]
    assert cn_scan_worker["command"].startswith(
        "celery -A quant_workspace_celery:celery_app worker "
    )
    assert cn_scan_worker["environment"]["SS_QUANTSTACK_READ_MODE"].endswith(
        ":-quantstack_primary}"
    )
    assert cn_scan_worker["environment"]["SS_QUANTSTACK_DATASET_VERSION"].endswith(
        ":-latest}"
    )
    assert cn_scan_worker["environment"]["SS_QUANTSTACK_CACHE_MAX_ENTRIES"].endswith(
        ":-100}"
    )
    assert (
        "./quant_workspace_celery.py:/app/quant_workspace_celery.py:ro"
        in cn_scan_worker["volumes"]
    )
    assert (
        "./quant_workspace_compat.py:/app/quant_workspace_compat.py:ro"
        in cn_scan_worker["volumes"]
    )
    assert (
        "./quantstack_consumer_adapter.py:/app/quantstack_consumer_adapter.py:ro"
        in cn_scan_worker["volumes"]
    )
    us_scan_worker = services["celery-userscans-us"]
    assert us_scan_worker["profiles"] == []
    assert "-Q user_scans_us" in us_scan_worker["command"]
    assert us_scan_worker["environment"]["SS_QUANTSTACK_US_BARS_DATASET_ID"].endswith(
        ":-}"
    )
    assert (
        "./quant_workspace_compat.py:/app/quant_workspace_compat.py:ro"
        in us_scan_worker["volumes"]
    )


def test_quant_workspace_projects_us_universe_without_provider_bootstrap() -> None:
    compat = (ROOT / "docker/stock-screener/quant_workspace_compat.py").read_text(
        encoding="utf-8"
    )

    assert "def _sync_us_quantstack_universe()" in compat
    assert '"dataset_id": dataset_id' in compat
    assert 'source = EXCLUDED.source' in compat
    assert "current_statement = text(" in compat
    assert "if changed:" in compat
    assert "connection.execute(statement, changed)" in compat
    assert "save_runtime_preferences(" in compat
    assert 'primary_market="US"' in compat
    assert 'enabled_markets=["US", "CN"]' in compat


def test_quant_workspace_reuses_projected_us_universe_for_chart_routing() -> None:
    compat = (ROOT / "docker/stock-screener/quant_workspace_compat.py").read_text(
        encoding="utf-8"
    )

    assert "def _resolve_us_instruments_from_stock_universe(" in compat
    assert "SELECT symbol, exchange" in compat
    assert "AND symbol IN :symbols" in compat
    assert 'in {"XNAS", "XNYS"}' in compat
    assert "adapter_factory=_chart_adapter_factory" in compat


def test_cn_scan_worker_installs_quantstack_overlay_before_celery_tasks() -> None:
    entrypoint = (
        ROOT / "docker" / "stock-screener" / "quant_workspace_celery.py"
    ).read_text(encoding="utf-8")

    compat_import = "import quant_workspace_compat as _quant_workspace_compat"
    celery_import = "from app.celery_app import _ensure_worker_runtime_services, celery_app"
    runtime_init = "_runtime_services = _ensure_worker_runtime_services()"
    assert compat_import in entrypoint
    assert celery_import in entrypoint
    assert runtime_init in entrypoint
    assert entrypoint.index(compat_import) < entrypoint.index(celery_import)
    assert entrypoint.index(celery_import) < entrypoint.index(runtime_init)


def test_snapshot_reconciliation_initializes_runtime_before_sync() -> None:
    entrypoint = (
        ROOT / "docker" / "stock-screener" / "reconcile_scan_analytics.py"
    ).read_text(encoding="utf-8")

    runtime_init = "initialize_runtime_services()"
    sync_import = "from quantstack_scan_analytics import sync_scan_analytics"
    compat_import = "import quant_workspace_compat as _quant_workspace_compat"
    assert compat_import in entrypoint
    assert "initialize_process_runtime_services(session_factory=SessionLocal)" in entrypoint
    assert runtime_init in entrypoint
    assert sync_import in entrypoint
    reconcile_body = entrypoint[entrypoint.index("def reconcile(") :]
    assert reconcile_body.index(runtime_init) < reconcile_body.index(sync_import)
    initializer_body = entrypoint[entrypoint.index("def initialize_runtime_services(") :]
    assert initializer_body.index(compat_import) < initializer_body.index(
        "initialize_process_runtime_services"
    )


def test_quant_workspace_runtime_installs_price_cache_before_direct_yfinance_hook() -> None:
    compat = (ROOT / "docker/stock-screener/quant_workspace_compat.py").read_text(
        encoding="utf-8"
    )

    assert "from app.services.yfinance_service import YFinanceService" in compat
    assert "install_yfinance_read_modes" in compat
    price_install = "install_price_cache_read_modes(\n    PriceCacheService,"
    yfinance_install = "install_yfinance_read_modes(YFinanceService, PriceCacheService)"
    assert price_install in compat
    assert yfinance_install in compat
    assert compat.index(price_install) < compat.index(yfinance_install)


def test_quant_workspace_chart_reads_are_bounded_and_nonblocking() -> None:
    compat = (ROOT / "docker/stock-screener/quant_workspace_compat.py").read_text(
        encoding="utf-8"
    )

    assert "_chart_primary_read_limiter = CapacityLimiter(2)" in compat
    assert "_chart_background_read_limiter = CapacityLimiter(1)" in compat
    assert "limiter=_chart_background_read_limiter" in compat
    assert "await to_thread.run_sync(" in compat
    assert 'stocks_api.PERIOD_DAYS["10y"] = 3650' in compat
    assert "get_cached_only(symbol.upper(), period=period)" in compat
    assert "get_many_cached_only(symbols, period=payload.period)" in compat
    assert '"/api/v1/stocks/{symbol}/history": history' in compat
    assert 'range_path = "/api/v1/stocks/{symbol}/history-range"' in compat
    assert "get_quantstack_range(symbol.upper(), start, end)" in compat
    assert "for timestamp, row in frame.iterrows()" in compat
    assert "symbol: str = Depends(stocks_api.require_valid_price_history_symbol)" in compat
    assert '"/api/v1/stocks/history/batch": history_batch' in compat
    assert '"/api/v1/stocks/{symbol}/rs-line": rs_line' in compat
    assert "_load_rs_line_with_local_session" in compat
    assert "_install_nonblocking_chart_reads()" in compat
    assert "_install_quantstack_stock_info()" in compat
    assert 'get_cached_only(normalized, period="2y")' in compat
    assert "_install_cache_only_stock_fundamentals()" in compat
    assert "._get_from_database(symbol.upper())" in compat


def test_quant_workspace_history_range_exposes_correlated_stage_timing() -> None:
    compat = (ROOT / "docker/stock-screener/quant_workspace_compat.py").read_text(
        encoding="utf-8"
    )

    assert "with capture_read_stage_timings() as timings:" in compat
    assert 'request_id = uuid.uuid4().hex' in compat
    assert '"Server-Timing": server_timing' in compat
    assert '"X-Request-ID": request_id' in compat
    assert '"X-Server-Duration-Ms"' in compat
    assert '"bars_http_and_json"' in compat
    assert '"dataframe_conversion"' in compat
    assert '"point_conversion"' in compat
    assert '"json_serialization"' in compat


def test_quant_workspace_scan_freshness_uses_quantstack_primary_overlay() -> None:
    compat = (ROOT / "docker/stock-screener/quant_workspace_compat.py").read_text(
        encoding="utf-8"
    )

    assert "def _install_quantstack_scan_freshness()" in compat
    assert "adapter.get_last_trade_dates" in compat
    assert "range(0, len(market_symbols), 500)" in compat
    assert "market_symbols[offset : offset + 500]" in compat
    assert "dataset_release_date(" in compat
    assert "start=expected" in compat
    assert "end=expected" in compat
    assert "last_date=last_dates.get(row.symbol)" in compat
    assert 'if market == "US"' in compat
    assert "current_session_omissions" in compat
    assert "accepted current-session release" in compat
    assert "reading thousands of symbol histories" not in compat
    assert "adapter.resolved_dataset_version_for_market(market)" in compat
    assert "release_date == previous_session" in compat
    assert "from app.models.stock_price import StockPrice" not in compat
    assert compat.index("install_price_cache_read_modes(\n    PriceCacheService,") < compat.rindex(
        "_install_quantstack_scan_freshness()"
    )


def test_us_instrument_projection_has_an_independent_dataset_version() -> None:
    compat = (ROOT / "docker/stock-screener/quant_workspace_compat.py").read_text(
        encoding="utf-8"
    )
    compose = (ROOT / "docker/stock-screener/compose.quant.yaml").read_text(
        encoding="utf-8"
    )
    sync_source = compat[
        compat.index("def _sync_us_quantstack_universe") : compat.index(
            "def _set_quantstack_market_defaults"
        )
    ]

    assert "SS_QUANTSTACK_US_INSTRUMENTS_DATASET_VERSION" in sync_source
    assert "SS_QUANTSTACK_US_DATASET_VERSION" not in sync_source
    assert compose.count("SS_QUANTSTACK_US_INSTRUMENTS_DATASET_VERSION") == 8


def test_quant_workspace_releases_abandoned_fundamentals_activity() -> None:
    compat = (ROOT / "docker/stock-screener/quant_workspace_compat.py").read_text(
        encoding="utf-8"
    )

    assert "def _install_stale_fundamentals_activity_recovery()" in compat
    assert 'DATA_FETCH_RUNTIME_STAGE_KEYS, "fundamentals"' in compat
    assert "_install_stale_fundamentals_activity_recovery()" in compat


def test_quant_workspace_daily_snapshot_uses_scan_qfq_as_of_date() -> None:
    compat = (ROOT / "docker/stock-screener/quant_workspace_compat.py").read_text(
        encoding="utf-8"
    )

    assert "def _install_quantstack_snapshot_freshness()" in compat
    assert "DAILY_SNAPSHOT_SCHEMA_VERSION = 5" in compat
    assert "/market/index-snapshot" in compat
    assert "DAILY_SNAPSHOT_CACHE_TTL_SECONDS = 86_400" in compat
    assert 'expected_dates = warning.get("expected_dates")' in compat
    assert 'payload["scan_as_of_date"] = as_of.isoformat()' in compat
    assert "daily_snapshot_service._snapshot_anchor_date = snapshot_anchor_date" in compat
    assert "_install_quantstack_snapshot_freshness()" in compat


def test_quant_workspace_routes_single_market_custom_scans_to_market_worker() -> None:
    compat = (ROOT / "docker/stock-screener/quant_workspace_compat.py").read_text(
        encoding="utf-8"
    )

    assert "def _install_scan_queue_market_inference()" in compat
    assert "SELECT DISTINCT market" in compat
    assert 'WHERE symbol IN :symbols AND is_active = true' in compat
    assert "if len(markets) == 1:" in compat
    assert "market=resolved_market" in compat
    assert "_install_scan_queue_market_inference()" in compat


def test_quant_workspace_defers_provider_label_widening_until_after_migration() -> None:
    compat = (ROOT / "docker/stock-screener/quant_workspace_compat.py").read_text(
        encoding="utf-8"
    )

    assert "\n_ensure_provider_label_capacity()\n" not in compat
    assert "_upstream_lifespan = app.router.lifespan_context" in compat
    wrapper_start = compat.index("async def _quant_workspace_lifespan(application):")
    wrapper_end = compat.index("\n\n", wrapper_start)
    wrapper = compat[wrapper_start:wrapper_end]
    assert wrapper.index("async with _upstream_lifespan(application):") < wrapper.index(
        "_ensure_provider_label_capacity()"
    )
    assert wrapper.index("_ensure_provider_label_capacity()") < wrapper.index("yield")
    assert "app.router.lifespan_context = _quant_workspace_lifespan" in compat


def test_schwab_workbench_uses_protected_center_route_and_loopback_sidecar() -> None:
    compose = yaml.safe_load((ROOT / "docker/compose.nas.yaml").read_text())
    sample = compose["services"]["brokerage-sample"]
    assert sample["network_mode"] == "service:center-web"
    assert "ports" not in sample
    assert sample["volumes"] == ["brokerage-sample-state:/sample-state"]
    assert "secrets" not in sample and "env_file" not in sample
    assert sample["read_only"] is True
    assert sample["cap_drop"] == ["ALL"]
    nginx = (ROOT / "docker/quant-center/nginx.conf").read_text()
    assert "location /brokerage-sample-api/" in nginx
    assert "proxy_pass http://127.0.0.1:8917/;" in nginx
    assert "proxy_set_header Host 127.0.0.1:8917;" in nginx
    api = (ROOT / "apps/quant-center/src/brokerageSampleApi.ts").read_text()
    assert "/brokerage-sample-api/v1/brokerage-sample/" in api
    dockerfile = (ROOT / "docker/Dockerfile.brokerage-sample").read_text()
    assert "--frozen --no-dev --extra brokerage" in dockerfile
    assert "COPY tests/fixtures/brokerage" in dockerfile
    assert "USER quant" in dockerfile
