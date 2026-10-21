from __future__ import annotations

import importlib.util
import shutil
import sys
from pathlib import Path

import pandas as pd
import pytest
import yaml

ROOT = Path(__file__).parents[1]
REPLAY = ROOT / "docker/stock-screener/legacy_fixture_replay.py"
COMPAT = ROOT / "docker/stock-screener/quant_workspace_compat.py"
COMPOSE = ROOT / "docker/stock-screener/compose.shadow-t111.yaml"
DRIVER = ROOT / "docker/stock-screener/shadow_soak_driver.py"
FIXTURE = ROOT / "tests/fixtures/stock_screener/legacy_yfinance_cn_t110"
SYMBOLS = (
    "600000.SS",
    "600519.SS",
    "601318.SS",
    "601398.SS",
    "000001.SZ",
    "000333.SZ",
    "000858.SZ",
    "002594.SZ",
)
EXPECTED_ROWS = {"6mo": 119, "1y": 243, "2y": 484, "5y": 1211}


def _module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_replay_reads_all_fixed_symbols_and_windows() -> None:
    replay_module = _module(REPLAY, "t111_legacy_fixture_replay_all")
    replay = replay_module.LegacyFixtureReplay(FIXTURE)

    for symbol in SYMBOLS:
        for period, expected_rows in EXPECTED_ROWS.items():
            frame = replay.load(symbol, period, "1d")
            assert len(frame) == expected_rows
            assert list(frame.columns) == replay_module.EXPECTED_COLUMNS
            assert frame.index.name == "Date"
            assert str(frame.index.tz) == "Asia/Shanghai"
            assert frame.attrs == {"volume_unit": "shares", "adjustment": "qfq"}


def test_enabled_wrapper_returns_fixture_without_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    replay_module = _module(REPLAY, "t111_legacy_fixture_replay_enabled")
    provider_calls: list[tuple[object, ...]] = []

    class Service:
        def get_historical_data(self, *args, **kwargs):
            provider_calls.append((args, kwargs))
            return "provider"

    replay_module.install_legacy_fixture_replay(Service, expected_root=FIXTURE)
    monkeypatch.setenv(replay_module.ENABLE_ENV, "true")
    monkeypatch.setenv(replay_module.RUNTIME_ENV, "isolated-shadow")
    monkeypatch.setenv(replay_module.ROOT_ENV, str(FIXTURE.resolve()))

    frame = Service().get_historical_data(
        "600000.SS", period="2y", interval="1d", use_cache=False
    )

    assert isinstance(frame, pd.DataFrame)
    assert len(frame) == 484
    assert provider_calls == []


def test_enabled_wrapper_represents_allowlisted_missing_legacy_as_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    replay_module = _module(REPLAY, "t111_legacy_fixture_replay_missing")

    class Service:
        def get_historical_data(self, *_args, **_kwargs):
            raise AssertionError("provider fallback is forbidden")

    replay_module.install_legacy_fixture_replay(Service, expected_root=FIXTURE)
    monkeypatch.setenv(replay_module.ENABLE_ENV, "true")
    monkeypatch.setenv(replay_module.MISSING_AS_EMPTY_ENV, "true")
    monkeypatch.setenv(replay_module.RUNTIME_ENV, "isolated-shadow")
    monkeypatch.setenv(replay_module.ROOT_ENV, str(FIXTURE.resolve()))
    monkeypatch.setenv("SS_QUANTSTACK_SHADOW_ALLOWLIST", "600000.SS,600036.SS")

    frame = Service().get_historical_data("600036.SS", period="2y", interval="1d")

    assert frame.empty
    assert list(frame.columns) == replay_module.EXPECTED_COLUMNS
    assert str(frame.index.tz) == "Asia/Shanghai"


def test_replay_uses_portable_fixture_when_backend_has_no_parquet_engine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    replay_module = _module(REPLAY, "t111_legacy_fixture_replay_portable")

    def missing_engine(*_args, **_kwargs):
        raise ImportError("backend image has no parquet engine")

    monkeypatch.setattr(replay_module.pd, "read_parquet", missing_engine)
    frame = replay_module.LegacyFixtureReplay(FIXTURE).load("600000.SS", "2y", "1d")

    assert len(frame) == 484
    assert list(frame.columns) == replay_module.EXPECTED_COLUMNS
    assert str(frame.index.tz) == "Asia/Shanghai"


def test_disabled_wrapper_delegates_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    replay_module = _module(REPLAY, "t111_legacy_fixture_replay_disabled")
    sentinel = object()
    calls: list[tuple[object, ...]] = []

    class Service:
        def get_historical_data(self, symbol, period="1y", *args, **kwargs):
            calls.append((symbol, period, args, kwargs))
            return sentinel

    replay_module.install_legacy_fixture_replay(Service, expected_root=FIXTURE)
    monkeypatch.delenv(replay_module.ENABLE_ENV, raising=False)

    result = Service().get_historical_data(
        "600000.SS", "2y", "1d", use_cache=False, custom="preserved"
    )

    assert result is sentinel
    assert calls == [
        ("600000.SS", "2y", ("1d",), {"use_cache": False, "custom": "preserved"})
    ]


@pytest.mark.parametrize(
    ("symbol", "period", "interval"),
    [
        ("AAPL", "2y", "1d"),
        ("600000.SS", "max", "1d"),
        ("600000.SS", "2y", "1h"),
    ],
)
def test_enabled_wrapper_rejects_unexpected_inputs_without_provider(
    monkeypatch: pytest.MonkeyPatch,
    symbol: str,
    period: str,
    interval: str,
) -> None:
    replay_module = _module(REPLAY, f"t111_bad_{symbol}_{period}_{interval}")
    provider_calls = 0

    class Service:
        def get_historical_data(self, *args, **kwargs):
            nonlocal provider_calls
            provider_calls += 1

    replay_module.install_legacy_fixture_replay(Service, expected_root=FIXTURE)
    monkeypatch.setenv(replay_module.ENABLE_ENV, "true")
    monkeypatch.setenv(replay_module.RUNTIME_ENV, "isolated-shadow")
    monkeypatch.setenv(replay_module.ROOT_ENV, str(FIXTURE.resolve()))

    with pytest.raises(replay_module.LegacyFixtureError):
        Service().get_historical_data(symbol, period=period, interval=interval, use_cache=False)
    assert provider_calls == 0


def test_integrity_drift_fails_closed_without_provider(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    replay_module = _module(REPLAY, "t111_legacy_fixture_replay_integrity")
    copied = tmp_path / "legacy_yfinance_cn_t110"
    shutil.copytree(FIXTURE, copied)
    artifact = copied / "raw/600000_SS.parquet"
    with artifact.open("ab") as stream:
        stream.write(b"drift")
    provider_calls = 0

    class Service:
        def get_historical_data(self, *args, **kwargs):
            nonlocal provider_calls
            provider_calls += 1

    replay_module.install_legacy_fixture_replay(Service, expected_root=copied)
    monkeypatch.setenv(replay_module.ENABLE_ENV, "true")
    monkeypatch.setenv(replay_module.RUNTIME_ENV, "isolated-shadow")
    monkeypatch.setenv(replay_module.ROOT_ENV, str(copied.resolve()))

    with pytest.raises(replay_module.LegacyFixtureError, match="integrity drift"):
        Service().get_historical_data(
            "600000.SS", period="2y", interval="1d", use_cache=False
        )
    assert provider_calls == 0


def test_runtime_and_root_guards_fail_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    replay_module = _module(REPLAY, "t111_legacy_fixture_replay_guards")

    class Service:
        def get_historical_data(self, *args, **kwargs):
            raise AssertionError("provider fallback is forbidden")

    replay_module.install_legacy_fixture_replay(Service, expected_root=FIXTURE)
    monkeypatch.setenv(replay_module.ENABLE_ENV, "true")
    monkeypatch.setenv(replay_module.ROOT_ENV, str(FIXTURE.resolve()))
    monkeypatch.setenv(replay_module.RUNTIME_ENV, "production")
    with pytest.raises(replay_module.LegacyFixtureError, match="isolated-shadow"):
        Service().get_historical_data("600000.SS", period="2y")

    monkeypatch.setenv(replay_module.RUNTIME_ENV, "isolated-shadow")
    monkeypatch.setenv(replay_module.ROOT_ENV, str(FIXTURE.parent.resolve()))
    with pytest.raises(replay_module.LegacyFixtureError, match="fixed read-only root"):
        Service().get_historical_data("600000.SS", period="2y")


def test_t111_compose_enforces_internal_only_fixture_replay() -> None:
    raw = COMPOSE.read_text(encoding="utf-8")
    parsed = yaml.safe_load(raw)
    services = parsed["services"]
    backend = services["backend"]
    environment = backend["environment"]

    assert parsed["name"] == "quant-workspace-shadow-t111"
    assert set(services) == {"postgres", "redis", "batch-api", "backend"}
    assert parsed["networks"] == {"core": {"internal": True}}
    assert all(service["networks"] == ["core"] for service in services.values())
    assert backend["ports"] == ["127.0.0.1:29110:8000"]
    assert services["batch-api"]["ports"] == ["127.0.0.1:29120:8900"]
    assert services["batch-api"]["group_add"] == [
        "${T111_QUANT_DATA_READ_GID:?set the stable quant-data-read GID}",
        "${T111_SHADOW_WRITE_GID:?set the stable quant-shadow-write GID}",
    ]
    assert environment["SS_RUNTIME_ENV"] == "${T111_SS_RUNTIME_ENV:-isolated-shadow}"
    assert environment["T111_LEGACY_FIXTURE_REPLAY_ENABLED"] == "true"
    assert environment["T111_LEGACY_FIXTURE_MISSING_AS_EMPTY"] == "true"
    assert environment["T111_LEGACY_FIXTURE_ROOT"] == "/fixtures/legacy_yfinance_cn_t110"
    assert environment["SS_QUANTSTACK_TIMEOUT_SECONDS"] == "15"
    assert environment["SS_QUANTSTACK_MAX_ATTEMPTS"] == "1"
    assert not any("PROXY" in key for key in environment)
    assert set(backend["depends_on"]) == {"postgres", "redis", "batch-api"}
    assert any(
        mount.endswith(":/fixtures/legacy_yfinance_cn_t110:ro")
        for mount in backend["volumes"]
    )
    assert "./legacy_fixture_replay.py:/app/legacy_fixture_replay.py:ro" in backend["volumes"]
    assert "./shadow-corpus-t133.json:/opt/t078/shadow-corpus-t133.json:ro" in backend[
        "volumes"
    ]
    assert "./shadow-corpus-t134-qfq.json:/opt/t078/shadow-corpus-t134-qfq.json:ro" in backend[
        "volumes"
    ]
    assert environment["SS_QUANTSTACK_DATASET_VERSION"] == (
        "${T111_DATASET_VERSION:-2015-2026.full-market-v1.ss-shadow-v1}"
    )
    assert "egress" not in raw.lower()
    assert "network_mode:" not in raw


def test_compat_installs_replay_only_behind_explicit_flag() -> None:
    source = COMPAT.read_text(encoding="utf-8")
    install_modes = source.index("install_yfinance_read_modes(YFinanceService, PriceCacheService)")
    gate = source.index('if os.environ.get("T111_LEGACY_FIXTURE_REPLAY_ENABLED"')
    replay_install = source.index("install_legacy_fixture_replay(YFinanceService)")

    assert gate < replay_install < install_modes


def test_s1_seed_plan_remains_eight_controlled_misses_and_eight_seeds() -> None:
    driver = _module(DRIVER, "t111_shadow_soak_driver")
    operations: list[dict[str, object]] = []

    class Guard:
        client = object()

        def call(self, **kwargs):
            operations.append(kwargs)
            return {"row_count": 0} if "controlled_miss" in kwargs["read_path"] else {}

    seeded = driver._seed_or_verify(
        stage="S1",
        symbols=list(SYMBOLS),
        predecessor_symbols=set(),
        guard=Guard(),
    )

    assert seeded == list(SYMBOLS)
    assert [operation["read_path"] for operation in operations].count(
        "get_cached_only_controlled_miss"
    ) == 8
    assert [operation["read_path"] for operation in operations].count("seed") == 8
    assert len(operations) == 16


def test_s2_preserves_symbols_without_legacy_fixtures_as_empty(monkeypatch) -> None:
    driver = _module(DRIVER, "t133_shadow_soak_driver")
    monkeypatch.setenv("T111_LEGACY_FIXTURE_MISSING_AS_EMPTY", "true")
    symbols = [*SYMBOLS, "600005.SS", "000022.SZ"]
    operations: list[dict[str, object]] = []

    class Guard:
        client = object()

        def call(self, **kwargs):
            operations.append(kwargs)
            return {
                "row_count": 1 if kwargs["read_path"] == "get_cached_only" else 0
            }

    seeded = driver._seed_or_verify(
        stage="S2",
        symbols=symbols,
        predecessor_symbols=set(SYMBOLS),
        guard=Guard(),
    )

    assert seeded == symbols
    assert [operation["read_path"] for operation in operations[:8]] == [
        "get_cached_only"
    ] * 8
    assert [operation["read_path"] for operation in operations[8:]] == [
        "get_cached_only_missing_legacy"
    ] * 2
    assert all(operation["read_path"] != "seed" for operation in operations)


def test_expanded_traffic_uses_legacy_routes_only_for_fixture_symbols() -> None:
    driver = _module(DRIVER, "t133_shadow_traffic")
    symbols = [*SYMBOLS, "600005.SS", "000022.SZ"]
    operations = driver._traffic_operations(object(), symbols)

    routed = {
        symbol
        for family, _, selected, _, _ in operations
        if family in {"single_history", "technical_indicators", "scanner"}
        for symbol in selected
    }
    expanded = {
        symbol
        for family, _, selected, _, _ in operations
        if family in {"batch_history", "price_cache_methods"}
        for symbol in selected
    }

    assert routed == set(SYMBOLS)
    assert set(symbols).issubset(expanded)
