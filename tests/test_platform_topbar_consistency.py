from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _find_sibling_checkout(start: Path, relative_path: Path) -> Path:
    """Find a sibling checkout from a primary checkout or a nested worktree."""

    searched: list[Path] = []
    for ancestor in (start, *start.parents):
        candidate = ancestor / relative_path
        searched.append(candidate)
        if candidate.is_dir():
            return candidate
    locations = ", ".join(str(path) for path in searched)
    raise FileNotFoundError(f"checkout {relative_path} not found; searched: {locations}")


@pytest.fixture
def vibe_root() -> Path:
    try:
        return _find_sibling_checkout(ROOT, Path("upstream/Vibe-Trading"))
    except FileNotFoundError:
        pytest.skip("optional Vibe-Trading sibling checkout is not installed")


def test_sibling_checkout_discovery_supports_checkout_and_nested_worktree(tmp_path) -> None:
    checkout = tmp_path / "QuantStack"
    worktree = checkout / "QuantStack-daily-worker"
    vibe = tmp_path / "upstream" / "Vibe-Trading"
    worktree.mkdir(parents=True)
    vibe.mkdir(parents=True)

    assert _find_sibling_checkout(checkout, Path("upstream/Vibe-Trading")) == vibe
    assert _find_sibling_checkout(worktree, Path("upstream/Vibe-Trading")) == vibe


def test_all_owned_topbars_share_the_same_typography_and_breakpoint(vibe_root: Path) -> None:
    styles = [
        (ROOT / "apps/quant-center/src/styles.css").read_text(encoding="utf-8"),
        (ROOT / "apps/nautilus-lab/src/platform-nav.css").read_text(encoding="utf-8"),
        (vibe_root / "frontend/src/index.css").read_text(encoding="utf-8"),
        (ROOT / "docker/stock-screener/localization/quant-zh-cn.js").read_text(
            encoding="utf-8"
        ),
    ]

    for style in styles:
        compact = "".join(style.split())
        assert "-apple-system,BlinkMacSystemFont" in compact
        assert '"SegoeUI","PingFangSC","MicrosoftYaHei",sans-serif' in compact
        assert "font-size:14px" in compact
        assert "font-weight:700" in compact
        assert "line-height:21px" in compact
        assert "max-width:700px" in compact or "max-width:700px" in style

    assert ".platform-navstrong{display:block;" in "".join(styles[1].split())


def test_all_owned_topbars_share_the_quantstack_brand(vibe_root: Path) -> None:
    sources = (
        (ROOT / "apps/quant-center/src/App.tsx").read_text(encoding="utf-8"),
        (ROOT / "apps/nautilus-lab/src/App.tsx").read_text(encoding="utf-8"),
        (vibe_root / "frontend/src/components/layout/Layout.tsx").read_text(encoding="utf-8"),
        (ROOT / "docker/stock-screener/localization/quant-zh-cn.js").read_text(encoding="utf-8"),
    )

    for source in sources:
        assert "QuantStack 量子栈" in source
        assert "QuantStack 量子栈统一导航" in source


def test_vibe_theme_is_applied_before_the_application_mounts(vibe_root: Path) -> None:
    html = (vibe_root / "frontend/index.html").read_text(encoding="utf-8")

    assert html.index("quant-platform-theme=") < html.index("/src/main.tsx")
    assert "classList.toggle('dark'" in html
    assert "dataset.theme = theme" in html
    assert '<script src="/quant-theme-init' not in html


def test_browser_tabs_keep_expected_titles_and_shared_brand_icon(vibe_root: Path) -> None:
    center_html = (ROOT / "apps/quant-center/index.html").read_text(encoding="utf-8")
    nautilus_html = (ROOT / "apps/nautilus-lab/index.html").read_text(encoding="utf-8")
    vibe_html = (vibe_root / "frontend/index.html").read_text(encoding="utf-8-sig")
    stock_dockerfile = (
        ROOT / "docker/stock-screener/localization/Dockerfile"
    ).read_text(encoding="utf-8")
    stock_localizer = (
        ROOT / "docker/stock-screener/localization/quant-zh-cn.js"
    ).read_text(encoding="utf-8")

    assert "<title>QuantStack量子栈</title>" in center_html

    for html in (nautilus_html, vibe_html):
        assert "<title>Quant Center</title>" in html
        assert 'rel="icon" type="image/svg+xml" href="/quant-center.svg"' in html

    assert 'rel="icon" type="image/svg+xml" href="/quant-center.svg"' in center_html

    assert "<title>Quant Center</title>" in stock_dockerfile
    assert 'href="/quant-center.svg"' in stock_dockerfile
    assert "document.title = 'Quant Center'" in stock_localizer

    shared_satellite_icon = (ROOT / "assets/brand/quant-center-favicon.svg").read_bytes()
    satellite_icon_paths = (
        ROOT / "apps/nautilus-lab/public/quant-center.svg",
        vibe_root / "frontend/public/quant-center.svg",
        ROOT / "docker/stock-screener/localization/quant-center.svg",
    )
    assert all(path.read_bytes() == shared_satellite_icon for path in satellite_icon_paths)

    center_icon = (ROOT / "apps/quant-center/public/quant-center.svg").read_text(
        encoding="utf-8"
    )
    assert 'stroke="#35f27c"' in center_icon
    assert 'fill="#ffffff"' in center_icon
