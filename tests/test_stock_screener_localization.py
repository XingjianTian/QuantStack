from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LOCALIZATION = ROOT / "docker" / "stock-screener" / "localization"


def test_localized_image_keeps_upstream_bundle_immutable() -> None:
    dockerfile = (LOCALIZATION / "Dockerfile").read_text(encoding="utf-8")

    assert "v1.4.0@sha256:" in dockerfile
    assert (
        "COPY quant-center.svg quant-theme-init.js quant-theme-init.html quant-zh-cn.js"
        in dockerfile
    )
    assert "quant-theme-init.html" in dockerfile
    assert "awk '/<\\/head>/" in dockerfile
    assert "sha256-tuvzR0DlMF8dx6Aj8dNOeZHF1JpWOnBWwYeEVCNbiw8=" in dockerfile
    assert "location = /index.html" in dockerfile
    assert "location = /quant-zh-cn.js" in dockerfile
    assert "location = / {" in dockerfile
    assert dockerfile.count('Cache-Control "no-store, no-cache, must-revalidate"') == 3
    assert "npm" not in dockerfile


def test_theme_bootstrap_prevents_an_incorrect_first_paint() -> None:
    bootstrap = (LOCALIZATION / "quant-theme-init.js").read_text(encoding="utf-8")
    inline_bootstrap = (LOCALIZATION / "quant-theme-init.html").read_text(encoding="utf-8")
    localizer = (LOCALIZATION / "quant-zh-cn.js").read_text(encoding="utf-8")

    assert "quant-theme-pending" in bootstrap
    assert "html.quant-theme-pending #root { visibility: hidden; }" in bootstrap
    assert "themeBootstrapMaxDelayMs = 500" in bootstrap
    assert "themeBootstrapMaxDelayMs), 2500" not in bootstrap
    assert "dataset.quantPlatformTheme" in bootstrap
    assert "quant-theme-pending" in inline_bootstrap
    assert "themeBootstrapMaxDelayMs = 500" in inline_bootstrap
    assert "themeBootstrapMaxDelayMs), 2500" not in inline_bootstrap
    assert "dataset.quantPlatformTheme" in inline_bootstrap
    assert "revealPlatformTheme" in localizer
    assert "settleInitialPlatformTheme" in localizer
    assert "initialThemeStableFrames >= 12" in localizer
    assert "applyingPlatformTheme || isPlatformThemePending()" in localizer


def test_localizer_covers_primary_routes_and_dynamic_content() -> None:
    script = (LOCALIZATION / "quant-zh-cn.js").read_text(encoding="utf-8")

    for english, chinese in {
        "Daily": "每日概览",
        "5 Days": "5日",
        "Daily K": "日K",
        "Weekly K": "周K",
        "Monthly K": "月K",
        "Quarterly K": "季K",
        "Yearly K": "年K",
        "Scan": "选股扫描",
        "Breadth": "市场宽度",
        "Groups": "行业组",
        "Backtest": "回测验证",
        "Operations": "运行监控",
        "Stock Details": "股票详情",
        "Scheduled Tasks": "定时任务",
        "Previous Scans": "历史扫描",
        "Export to CSV": "导出为 CSV",
        "Rows per page:": "每页行数：",
    }.items():
        assert f"'{english}': '{chinese}'" in script

    assert "MutationObserver" in script
    assert "characterData: true" in script
    assert "attributeFilter: translatedAttributes" in script
    assert "document.documentElement.lang = 'zh-CN'" in script
    assert "Results:\\s*([\\d,]+)\\s+stocks?" in script
    assert "fields? unavailable or computed" in script


def test_localizer_preserves_professional_terms() -> None:
    script = (LOCALIZATION / "quant-zh-cn.js").read_text(encoding="utf-8")

    assert "'RRG': '相对轮动图（RRG）'" in script
    assert "'Minervini': '米涅尔维尼（Minervini）'" in script
    assert "data-no-translate" in script


def test_localizer_keeps_stock_screener_brand_and_shared_navigation() -> None:
    script = (LOCALIZATION / "quant-zh-cn.js").read_text(encoding="utf-8")

    assert "document.title = 'Quant Center'" in script
    assert "'STOCK SCANNER': 'Stock Screener'" in script
    assert ".quant-platform-nav{height:52px" in script
    assert "font-family:-apple-system,BlinkMacSystemFont" in script
    assert "font-size:14px;font-weight:400;line-height:21px" in script
    assert "行情选股(SS)" in script
    assert "策略实施(NT)" in script
    assert "--quant-platform-nav-offset:64px" in script
    assert '.quant-platform-nav[data-surface="dark"]' in script
    assert '.quant-platform-nav[data-surface="light"]' in script
    assert "quant-platform-theme" in script
    assert "quant-platform-theme-toggle" in script
    assert "quant-platform-nav-links" in script
    assert "QuantStack 量子栈" in script
    assert "QuantStack 量子栈统一导航" in script
    assert "quant-platform-brand" in script
    assert "返回 QuantStack 量子栈主页" in script
    assert "href=\"${originFor('quant')}/\"" in script
    assert "Domain=." in script
    assert "getNativeThemeToggle" in script
    assert "lightToggleTitles.includes(nativeTitle)" in script
    assert "darkToggleTitles.includes(nativeTitle)" in script
    assert ".quant-platform-active .MuiModal-root>.MuiBox-root" in script
    assert "data-quant-ss-home" in script
    assert "window.history.pushState" in script
    assert "quant:ss-home" in script
    assert "selectedMarket" in script
    assert "Market selector" in script


def test_localizer_covers_live_dashboard_and_operations_copy() -> None:
    script = (LOCALIZATION / "quant-zh-cn.js").read_text(encoding="utf-8")

    for english, chinese in {
        "Why this score": "评分依据",
        "Distribution days": "派发日数量",
        "Top 10 Groups": "排名前十的行业组",
        "Recent Events": "近期事件",
        "External Fetch Lease": "外部数据抓取租约",
        "Market Workload Leases": "各市场工作负载租约",
    }.items():
        assert f"'{english}': '{chinese}'" in script

    assert "Market Breadth \\(4%\\+ Movers\\) with" in script
    assert "Dollar volume >=" in script


def test_localizer_covers_source_and_runtime_generated_copy() -> None:
    script = (LOCALIZATION / "quant-zh-cn.js").read_text(encoding="utf-8")

    for english, chinese in {
        "Stocks up 4%+ (daily count)": "单日上涨 4% 以上的股票数量",
        "Stock Count": "股票数量",
        "Price refresh": "价格数据刷新",
        "Workspace is partially degraded:": "工作台部分数据异常：",
    }.items():
        assert english in script
        assert chinese in script

    assert "Actionable untranslated literals" not in script
    assert "test(translated) ? ''" in script
