from __future__ import annotations

import ast
import json
import os
import runpy
import shutil
import sys
import urllib.request
from importlib import import_module
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pandas as pd
import pytest

MODULE_PATH = (
    Path(__file__).parents[1] / "docker" / "stock-screener" / "quantstack_scan_analytics.py"
)
sys.path.insert(0, str(MODULE_PATH.parent))
snapshot_publication = import_module("daily_snapshot_publication")

SPEC = spec_from_file_location("quantstack_scan_analytics", MODULE_PATH)
assert SPEC and SPEC.loader
analytics = module_from_spec(SPEC)
SPEC.loader.exec_module(analytics)


def _frame(multiplier: float, *, rows: int = 280) -> pd.DataFrame:
    index = pd.date_range("2025-01-01", periods=rows, freq="B", tz="UTC")
    closes = pd.Series(range(1, rows + 1), index=index, dtype="float64") * multiplier
    return pd.DataFrame({"Close": closes, "Volume": 1_000_000}, index=index)


def test_scan_analytics_builds_cross_sectional_rs_and_sparklines() -> None:
    frames = {
        "FAST": _frame(1.0),
        "SLOW": pd.DataFrame(
            {
                "Close": pd.Series(
                    range(280, 0, -1),
                    index=pd.date_range("2025-01-01", periods=280, freq="B", tz="UTC"),
                    dtype="float64",
                ),
                "Volume": 500_000,
            }
        ),
    }

    result = analytics.calculate_symbol_metrics(frames)

    assert result["FAST"]["rs_rating"] > result["SLOW"]["rs_rating"]
    assert len(result["FAST"]["price_sparkline_data"]) == 30
    assert result["FAST"]["price"] == 280.0
    assert result["FAST"]["volume"] == 280_000_000
    assert result["SLOW"]["volume"] == 500_000
    assert result["FAST"]["stage"] == 2


def test_scan_analytics_volume_preserves_average_dollar_volume_contract() -> None:
    frame = pd.DataFrame(
        {
            "Close": [20.0, 25.0, 30.0],
            "Volume": [1_000_000, 2_000_000, 3_000_000],
        },
        index=pd.date_range("2026-08-24", periods=3, freq="B", tz="UTC"),
    )

    result = analytics.calculate_symbol_metrics({"AAA": frame})

    # Average shares (2M) multiplied by the latest close ($30).
    assert result["AAA"]["volume"] == 60_000_000


def test_scan_analytics_builds_breadth_and_equal_weight_exposure() -> None:
    frames = {"A": _frame(1.0), "B": _frame(2.0)}

    breadth = analytics.calculate_breadth(frames)
    exposure = analytics.calculate_equal_weight_exposure(frames, breadth)

    assert breadth["total_stocks_scanned"] == 2
    assert breadth["stocks_up_4pct"] == 0
    assert 0 <= exposure["exposure_score"] <= 100
    assert exposure["benchmark_ma200"] is not None
    assert exposure["components"]["base"] == 50.0


def test_scan_analytics_streams_bounded_batches_with_equivalent_results() -> None:
    frames = {f"S{index}": _frame(index + 1.0) for index in range(251)}

    class Adapter:
        calls: list[list[str]] = []
        max_cache_entries = 2048

        def get_batch_price_history(self, symbols, *, start, end):
            self.calls.append(list(symbols))
            return {symbol: SimpleNamespace(frame=frames[symbol]) for symbol in symbols}

    adapter = Adapter()
    metrics, _, changes, market_returns = analytics._stream_scan_frames(
        adapter,
        list(frames),
        start=pd.Timestamp("2025-01-01").date(),
        end=pd.Timestamp("2026-01-01").date(),
    )
    breadth = analytics._breadth_from_changes(changes)
    exposure = analytics._exposure_from_market_returns(market_returns, breadth)

    assert [len(batch) for batch in adapter.calls] == [250, 1]
    assert adapter.max_cache_entries == 1
    assert len(metrics) == 251
    assert breadth == analytics.calculate_breadth(frames)
    expected = analytics.calculate_equal_weight_exposure(frames, breadth)
    assert exposure["exposure_score"] == expected["exposure_score"]
    assert exposure["benchmark_price"] == pytest.approx(expected["benchmark_price"])
    assert exposure["benchmark_ma50"] == pytest.approx(expected["benchmark_ma50"])
    assert exposure["benchmark_ma200"] == pytest.approx(expected["benchmark_ma200"])


class _Field:
    def __eq__(self, _other):
        return self

    def in_(self, _values):
        return self

    def desc(self):
        return self


class _Record:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class _ScanModel(_Record):
    scan_id = _Field()
    status = _Field()


class _ScanResultModel(_Record):
    scan_id = _Field()


class _IndustryModel(_Record):
    market = _Field()
    symbol = _Field()


class _UniverseModel(_Record):
    symbol = _Field()


class _GroupRankModel(_Record):
    pass


class _BreadthModel(_Record):
    date = _Field()
    market = _Field()


class _ExposureModel(_Record):
    pass


class _Query:
    def __init__(self, model, scan, rows, records):
        self.model = model
        self.scan = scan
        self.rows = rows
        self.records = records
        self.filter_values: dict[str, object] = {}

    def filter(self, *_args):
        return self

    def filter_by(self, **kwargs):
        self.filter_values.update(kwargs)
        return self

    def order_by(self, *_args):
        return self

    def one(self):
        return self.scan

    def all(self):
        if self.model is _ScanResultModel:
            return self.rows
        return []

    def one_or_none(self):
        record = self.records.get(self.model)
        if record is None:
            return None
        if any(
            getattr(record, key, object()) != value
            for key, value in self.filter_values.items()
        ):
            return None
        return record

    def first(self):
        return None


class _Session:
    def __init__(self, scan, rows):
        self.scan = scan
        self.rows = rows
        self.autoflush = True
        self.commits = 0
        self.rollbacks = 0
        self.records: dict[type, object] = {}

    def query(self, model):
        return _Query(model, self.scan, self.rows, self.records)

    def add(self, record):
        self.records[type(record)] = record

    def flush(self):
        pass

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        pass


def _install_fake_app(monkeypatch: pytest.MonkeyPatch, session: _Session) -> None:
    modules = {
        "app": ModuleType("app"),
        "app.models": ModuleType("app.models"),
        "app.database": ModuleType("app.database"),
        "app.models.industry": ModuleType("app.models.industry"),
        "app.models.market_breadth": ModuleType("app.models.market_breadth"),
        "app.models.market_exposure": ModuleType("app.models.market_exposure"),
        "app.models.scan_result": ModuleType("app.models.scan_result"),
        "app.models.stock_universe": ModuleType("app.models.stock_universe"),
    }
    modules["app.database"].SessionLocal = lambda: session
    modules["app.models.industry"].IBDGroupRank = _GroupRankModel
    modules["app.models.industry"].IBDIndustryGroup = _IndustryModel
    modules["app.models.market_breadth"].MarketBreadth = _BreadthModel
    modules["app.models.market_exposure"].MarketExposure = _ExposureModel
    modules["app.models.scan_result"].Scan = _ScanModel
    modules["app.models.scan_result"].ScanResult = _ScanResultModel
    modules["app.models.stock_universe"].StockUniverse = _UniverseModel
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)


def _analytics_session() -> tuple[_Session, _ScanModel]:
    scan = _ScanModel(
        scan_id="scan-us",
        status="completed",
        universe_market="US",
        warnings=[{"expected_dates": {"US": "2026-09-04"}}],
        completed_at=pd.Timestamp("2026-09-04", tz="UTC").to_pydatetime(),
        analytics_version=None,
        analytics_completed_at=None,
    )
    row = _ScanResultModel(
        symbol="AAA",
        details={},
        ibd_industry_group=None,
        rs_rating=None,
        gics_sector=None,
        gics_industry=None,
        volume=1,
    )
    return _Session(scan, [row]), scan


def _install_actual_snapshot_integration(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    session: _Session,
) -> tuple[type[_Record], type[_Record], list[str]]:
    """Load the patched upstream builder and the real compat key-markets binding."""

    root = Path(__file__).parents[1]
    upstream = root.parent / "upstream" / "stock-screener" / "backend"
    if not upstream.is_dir():
        pytest.skip("optional pinned Stock Screener source checkout is not installed")
    patched = tmp_path / "backend"
    for relative in (
        "app/models/scan_result.py",
        "app/schemas/market_scan.py",
        "app/services/daily_snapshot_service.py",
        "app/api/v1/market_scan.py",
        "app/tasks/scan_tasks.py",
    ):
        destination = patched / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(upstream / relative, destination)
    patcher = runpy.run_path(
        str(root / "docker/stock-screener/patches/patch_snapshot_analytics_readiness.py")
    )["main"]
    patcher(patched)

    external_reads: list[str] = []

    class PublicationRecord(_Record):
        pass

    class PublicationInputRecord(_Record):
        pass

    class _Value:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

        def add_range(self, *_args):
            return None

    class _ScanResultItem:
        @classmethod
        def from_domain(cls, item, *, include_setup_payload):
            del include_setup_payload
            return SimpleNamespace(model_dump=lambda **_kwargs: item)

    class _GetScanResultsUseCase:
        def execute(self, _uow, _query):
            return SimpleNamespace(page=SimpleNamespace(items=[]))

    class _Repository:
        def __init__(self, owner_session):
            self.session = owner_session

    class _SnapshotSectionDates:
        @classmethod
        def from_raw(cls, **kwargs):
            return kwargs

    catalog = SimpleNamespace(
        get=lambda _market: SimpleNamespace(
            label="United States",
            display_timezone="America/New_York",
            capabilities=SimpleNamespace(group_rankings=False),
        )
    )

    def fail_index_file(*_args, **_kwargs):
        external_reads.append("index-file")
        pytest.fail("US repair accessed the upstream index-file leaf")

    def build_snapshot_freshness(*, base_freshness, anchor, **_kwargs):
        result = dict(base_freshness)
        result["snapshot_as_of_date"] = anchor.isoformat() if anchor else None
        return result

    services = ModuleType("app.services")
    services.__path__ = []
    modules: dict[str, ModuleType] = {
        "sqlalchemy": ModuleType("sqlalchemy"),
        "sqlalchemy.orm": ModuleType("sqlalchemy.orm"),
        "app.domain": ModuleType("app.domain"),
        "app.domain.common": ModuleType("app.domain.common"),
        "app.domain.common.query": ModuleType("app.domain.common.query"),
        "app.domain.markets": ModuleType("app.domain.markets"),
        "app.domain.markets.catalog": ModuleType("app.domain.markets.catalog"),
        "app.domain.scanning": ModuleType("app.domain.scanning"),
        "app.domain.scanning.default_filters": ModuleType(
            "app.domain.scanning.default_filters"
        ),
        "app.infra": ModuleType("app.infra"),
        "app.infra.db": ModuleType("app.infra.db"),
        "app.infra.db.repositories": ModuleType("app.infra.db.repositories"),
        "app.infra.serialization": ModuleType("app.infra.serialization"),
        "app.models.daily_snapshot_publication": ModuleType(
            "app.models.daily_snapshot_publication"
        ),
        "app.schemas": ModuleType("app.schemas"),
        "app.schemas.market_scan": ModuleType("app.schemas.market_scan"),
        "app.schemas.scanning": ModuleType("app.schemas.scanning"),
        "app.services": services,
        "app.services.key_market_history": ModuleType(
            "app.services.key_market_history"
        ),
        "app.services.market_exposure_service": ModuleType(
            "app.services.market_exposure_service"
        ),
        "app.services.redis_pool": ModuleType("app.services.redis_pool"),
        "app.services.snapshot_date_coherence": ModuleType(
            "app.services.snapshot_date_coherence"
        ),
        "app.use_cases": ModuleType("app.use_cases"),
        "app.use_cases.scanning": ModuleType("app.use_cases.scanning"),
        "app.use_cases.scanning.get_scan_results": ModuleType(
            "app.use_cases.scanning.get_scan_results"
        ),
        "app.wiring": ModuleType("app.wiring"),
        "app.wiring.bootstrap": ModuleType("app.wiring.bootstrap"),
    }
    for package_name in (
        "app.domain",
        "app.domain.common",
        "app.domain.markets",
        "app.domain.scanning",
        "app.infra",
        "app.infra.db",
        "app.infra.db.repositories",
        "app.schemas",
        "app.use_cases",
        "app.use_cases.scanning",
        "app.wiring",
    ):
        modules[package_name].__path__ = []
    modules["sqlalchemy.orm"].Session = object
    query_module = modules["app.domain.common.query"]
    query_module.FilterSpec = _Value
    query_module.PageSpec = _Value
    query_module.QuerySpec = _Value
    query_module.SortOrder = SimpleNamespace(DESC="desc")
    query_module.SortSpec = _Value
    modules["app.domain.markets.catalog"].get_market_catalog = lambda: catalog
    modules[
        "app.domain.scanning.default_filters"
    ].resolve_default_scan_filters = lambda _market: {"minVolume": 100_000_000}
    modules["app.infra.serialization"].json_safe = lambda payload: payload
    modules["app.models.daily_snapshot_publication"].DailySnapshotPublication = (
        PublicationRecord
    )
    modules[
        "app.models.daily_snapshot_publication"
    ].DailySnapshotPublicationInput = PublicationInputRecord
    modules["app.schemas.market_scan"].DailySnapshotResponse = SimpleNamespace(
        model_validate=lambda payload: payload
    )
    modules["app.schemas.scanning"].ScanResultItem = _ScanResultItem
    modules["app.services.key_market_history"].build_key_market_entries = fail_index_file
    modules[
        "app.services.market_exposure_service"
    ].build_exposure_payload = lambda *_args, **_kwargs: {"date": "2026-09-04"}
    modules["app.services.redis_pool"].get_redis_client = lambda: None
    coherence = modules["app.services.snapshot_date_coherence"]
    coherence.SnapshotSectionDates = _SnapshotSectionDates
    coherence.build_snapshot_freshness = build_snapshot_freshness
    scan_results = modules["app.use_cases.scanning.get_scan_results"]
    scan_results.GetScanResultsQuery = _Value
    scan_results.GetScanResultsUseCase = _GetScanResultsUseCase
    modules["app.wiring.bootstrap"].get_group_rank_service = lambda: pytest.fail(
        "US snapshot unexpectedly requested group rankings"
    )
    for module_name, class_name in (
        ("feature_run_repo", "SqlFeatureRunRepository"),
        ("feature_store_repo", "SqlFeatureStoreRepository"),
        ("scan_repo", "SqlScanRepository"),
        ("scan_result_repo", "SqlScanResultRepository"),
        ("universe_repo", "SqlUniverseRepository"),
    ):
        qualified = f"app.infra.db.repositories.{module_name}"
        module = ModuleType(qualified)
        setattr(module, class_name, _Repository)
        modules[qualified] = module
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)

    service_path = patched / "app/services/daily_snapshot_service.py"
    service_spec = spec_from_file_location(
        "app.services.daily_snapshot_service", service_path
    )
    assert service_spec and service_spec.loader
    daily_snapshot_service = module_from_spec(service_spec)
    monkeypatch.setitem(
        sys.modules, "app.services.daily_snapshot_service", daily_snapshot_service
    )
    services.daily_snapshot_service = daily_snapshot_service
    service_spec.loader.exec_module(daily_snapshot_service)

    compat_path = root / "docker/stock-screener/quant_workspace_compat.py"
    compat_tree = ast.parse(compat_path.read_text(encoding="utf-8"), compat_path)
    installer = next(
        node
        for node in compat_tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "_install_quantstack_snapshot_freshness"
    )
    future = ast.ImportFrom(
        module="__future__", names=[ast.alias(name="annotations")], level=0
    )
    installer_module = ast.fix_missing_locations(
        ast.Module(body=[future, installer], type_ignores=[])
    )
    compat_globals: dict[str, Any] = {
        "Any": Any,
        "ContextVar": __import__("contextvars").ContextVar,
        "date": __import__("datetime").date,
        "json": json,
        "os": os,
        "urllib": __import__("urllib"),
    }
    exec(compile(installer_module, str(compat_path), "exec"), compat_globals)
    monkeypatch.setenv("SS_QUANTSTACK_READ_MODE", "quantstack_primary")

    def fail_http(*_args, **_kwargs):
        external_reads.append("http")
        pytest.fail("US repair accessed /market/index-snapshot")

    monkeypatch.setattr(urllib.request, "build_opener", fail_http)
    compat_globals["_install_quantstack_snapshot_freshness"]()
    assert daily_snapshot_service.build_daily_snapshot_payload.__code__.co_filename == str(
        service_path
    )
    assert daily_snapshot_service.build_key_market_entries.__code__.co_filename == str(
        compat_path
    )
    monkeypatch.setattr(
        analytics.SSQuantStackConsumerAdapter,
        "from_environment",
        lambda **_kwargs: external_reads.append("accepted-bars")
        or pytest.fail("publication repair reread accepted scan bars"),
    )
    return PublicationRecord, PublicationInputRecord, external_reads


def test_scan_analytics_dry_run_rolls_back_and_apply_marks_same_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    breadth_changes = {item: [0.01] for item in (1, 5, 10, 21, 34, 63)}
    market_returns = pd.Series(
        [0.01] * 220,
        index=pd.date_range("2025-01-01", periods=220, freq="B", tz="UTC"),
    )
    monkeypatch.setattr(
        analytics,
        "_stream_scan_frames",
        lambda *_args, **_kwargs: (
            {"AAA": {"volume": 200_000_000, "rs_rating": 90}},
            {},
            breadth_changes,
            market_returns,
        ),
    )
    monkeypatch.setattr(
        analytics.SSQuantStackConsumerAdapter,
        "from_environment",
        lambda **_kwargs: object(),
    )
    publications: list[tuple[str, str, bool]] = []
    monkeypatch.setattr(
        analytics,
        "build_and_store_snapshot_publication",
        lambda _db, *, scan, market, analytics_version, capture_inputs: publications.append(
            (scan.scan_id, f"{market}:{analytics_version}", capture_inputs)
        ),
    )

    dry_session, dry_scan = _analytics_session()
    _install_fake_app(monkeypatch, dry_session)
    dry = analytics.sync_scan_analytics("scan-us", apply=False)
    assert dry["status"] == "planned"
    assert dry["eligible_rows"] == 1
    assert dry_session.commits == 0
    assert dry_session.rollbacks == 1
    assert dry_scan.analytics_version is None

    apply_session, apply_scan = _analytics_session()
    _install_fake_app(monkeypatch, apply_session)
    applied = analytics.sync_scan_analytics("scan-us", apply=True)
    assert applied["status"] == "published"
    assert apply_session.commits == 1
    assert apply_session.rollbacks == 0
    assert apply_scan.analytics_version == analytics.SCAN_ANALYTICS_VERSION
    assert apply_scan.analytics_completed_at is not None
    assert applied["snapshot_publication_version"] == analytics.SNAPSHOT_PUBLICATION_VERSION
    assert publications == [
        ("scan-us", f"US:{analytics.SCAN_ANALYTICS_VERSION}", True)
    ]


def test_scan_analytics_failure_rolls_back_without_readiness(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session, scan = _analytics_session()
    _install_fake_app(monkeypatch, session)
    monkeypatch.setattr(
        analytics.SSQuantStackConsumerAdapter,
        "from_environment",
        lambda **_kwargs: object(),
    )
    monkeypatch.setattr(
        analytics,
        "_stream_scan_frames",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("broken analytics")),
    )

    with pytest.raises(RuntimeError, match="broken analytics"):
        analytics.sync_scan_analytics("scan-us", apply=True)
    assert session.commits == 0
    assert session.rollbacks == 1
    assert scan.analytics_version is None


def test_scan_analytics_current_version_is_an_idempotent_noop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session, scan = _analytics_session()
    scan.analytics_version = analytics.SCAN_ANALYTICS_VERSION
    scan.analytics_completed_at = pd.Timestamp("2026-09-04T12:00:00Z").to_pydatetime()
    _install_fake_app(monkeypatch, session)
    monkeypatch.setattr(
        analytics.SSQuantStackConsumerAdapter,
        "from_environment",
        lambda **_kwargs: pytest.fail("current analytics reread bars"),
    )
    monkeypatch.setattr(
        analytics,
        "snapshot_publication_is_current",
        lambda *_args, **_kwargs: True,
    )

    report = analytics.sync_scan_analytics("scan-us", apply=True)

    assert report["status"] == "current"
    assert report["snapshot_publication_version"] == analytics.SNAPSHOT_PUBLICATION_VERSION
    assert session.commits == 0
    assert session.rollbacks == 0


def test_scan_analytics_repairs_missing_publication_without_rereading_bars(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session, scan = _analytics_session()
    scan.analytics_version = analytics.SCAN_ANALYTICS_VERSION
    scan.analytics_completed_at = pd.Timestamp("2026-09-04T12:00:00Z").to_pydatetime()
    _install_fake_app(monkeypatch, session)
    monkeypatch.setattr(
        analytics.SSQuantStackConsumerAdapter,
        "from_environment",
        lambda **_kwargs: pytest.fail("publication repair reread bars"),
    )
    monkeypatch.setattr(
        analytics,
        "snapshot_publication_is_current",
        lambda *_args, **_kwargs: False,
    )
    repaired: list[tuple[str, bool]] = []
    monkeypatch.setattr(
        analytics,
        "build_and_store_snapshot_publication",
        lambda _db, **_kwargs: repaired.append(
            (_kwargs["market"], _kwargs["capture_inputs"])
        ),
    )

    report = analytics.sync_scan_analytics("scan-us", apply=True)

    assert report["status"] == "publication_repaired"
    assert repaired == [("US", False)]
    assert session.commits == 1
    assert session.rollbacks == 0


def test_real_us_publication_repair_uses_committed_database_inputs_only(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    session, scan = _analytics_session()
    scan.analytics_version = analytics.SCAN_ANALYTICS_VERSION
    scan.analytics_completed_at = pd.Timestamp("2026-09-04T12:00:00Z").to_pydatetime()
    scan.feature_run = None
    _install_fake_app(monkeypatch, session)
    PublicationRecord, PublicationInputRecord, external_reads = (
        _install_actual_snapshot_integration(monkeypatch, tmp_path, session)
    )

    entries = [{"symbol": "SPY", "name": "S&P 500", "change_pct": 0.5}]
    serialized = snapshot_publication._serialize_key_markets(entries)
    session.records[PublicationInputRecord] = PublicationInputRecord(
        market="US",
        scan_id=scan.scan_id,
        analytics_version=analytics.SCAN_ANALYTICS_VERSION,
        snapshot_schema_version=5,
        input_version=snapshot_publication.SNAPSHOT_PUBLICATION_INPUT_VERSION,
        key_markets_json=serialized,
        key_markets_sha256=snapshot_publication._payload_digest(serialized),
    )

    repaired = analytics.sync_scan_analytics("scan-us", apply=True)
    current = analytics.sync_scan_analytics("scan-us", apply=True)

    assert repaired["status"] == "publication_repaired"
    assert current["status"] == "current"
    assert external_reads == []
    assert session.commits == 1
    publication_record = session.records[PublicationRecord]
    assert publication_record.payload_sha256 == snapshot_publication._payload_digest(
        publication_record.payload_json
    )


@pytest.mark.parametrize(
    ("case", "message"),
    [
        ("missing", "input is unavailable"),
        ("wrong-version", "input is unavailable"),
        ("wrong-identity", "input is unavailable"),
        ("corrupt-digest", "digest does not match"),
    ],
)
def test_us_publication_repair_fails_closed_for_invalid_committed_input(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    case: str,
    message: str,
) -> None:
    session, scan = _analytics_session()
    scan.analytics_version = analytics.SCAN_ANALYTICS_VERSION
    scan.analytics_completed_at = pd.Timestamp("2026-09-04T12:00:00Z").to_pydatetime()
    scan.feature_run = None
    _install_fake_app(monkeypatch, session)
    PublicationRecord, PublicationInputRecord, external_reads = (
        _install_actual_snapshot_integration(monkeypatch, tmp_path, session)
    )
    entries = [{"symbol": "SPY", "name": "S&P 500", "change_pct": 0.5}]
    serialized = snapshot_publication._serialize_key_markets(entries)
    if case != "missing":
        session.records[PublicationInputRecord] = PublicationInputRecord(
            market="CN" if case == "wrong-identity" else "US",
            scan_id=scan.scan_id,
            analytics_version=analytics.SCAN_ANALYTICS_VERSION,
            snapshot_schema_version=5,
            input_version=(
                "wrong-version"
                if case == "wrong-version"
                else snapshot_publication.SNAPSHOT_PUBLICATION_INPUT_VERSION
            ),
            key_markets_json=serialized,
            key_markets_sha256=(
                "corrupt"
                if case == "corrupt-digest"
                else snapshot_publication._payload_digest(serialized)
            ),
        )

    with pytest.raises(snapshot_publication.SnapshotPublicationUnavailable, match=message):
        analytics.sync_scan_analytics("scan-us", apply=True)

    assert session.commits == 0
    assert session.rollbacks == 1
    assert PublicationRecord not in session.records
    assert external_reads == []


def test_snapshot_publication_failure_rolls_back_analytics_readiness(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    breadth_changes = {item: [0.01] for item in (1, 5, 10, 21, 34, 63)}
    market_returns = pd.Series(
        [0.01] * 220,
        index=pd.date_range("2025-01-01", periods=220, freq="B", tz="UTC"),
    )
    session, scan = _analytics_session()
    _install_fake_app(monkeypatch, session)
    monkeypatch.setattr(
        analytics.SSQuantStackConsumerAdapter,
        "from_environment",
        lambda **_kwargs: object(),
    )
    monkeypatch.setattr(
        analytics,
        "_stream_scan_frames",
        lambda *_args, **_kwargs: (
            {"AAA": {"volume": 200_000_000, "rs_rating": 90}},
            {},
            breadth_changes,
            market_returns,
        ),
    )
    monkeypatch.setattr(
        analytics,
        "build_and_store_snapshot_publication",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("broken publication")),
    )

    with pytest.raises(RuntimeError, match="broken publication"):
        analytics.sync_scan_analytics("scan-us", apply=True)

    assert session.commits == 0
    assert session.rollbacks == 1


def test_scan_analytics_uses_one_attempt_with_a_dedicated_cold_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, float | int] = {}
    sentinel = object()

    def from_environment(**kwargs):
        observed.update(kwargs)
        return sentinel

    monkeypatch.delenv("SS_SCAN_ANALYTICS_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("SS_SCAN_ANALYTICS_MAX_ATTEMPTS", raising=False)
    monkeypatch.setattr(
        analytics.SSQuantStackConsumerAdapter,
        "from_environment",
        from_environment,
    )

    assert analytics._scan_analytics_adapter() is sentinel
    assert observed == {"timeout_seconds": 30.0, "max_attempts": 1}


@pytest.mark.parametrize(
    ("name", "value", "message"),
    [
        ("SS_SCAN_ANALYTICS_TIMEOUT_SECONDS", "0", "timeout must be positive"),
        ("SS_SCAN_ANALYTICS_MAX_ATTEMPTS", "4", "attempts must be between 1 and 3"),
    ],
)
def test_scan_analytics_rejects_invalid_transport_budget(
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    value: str,
    message: str,
) -> None:
    monkeypatch.setenv(name, value)
    with pytest.raises(ValueError, match=message):
        analytics._scan_analytics_adapter()
