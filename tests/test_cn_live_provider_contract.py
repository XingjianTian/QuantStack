import socket
import threading
from datetime import UTC, date, datetime, time
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from quant_stack.data.calendars import (
    ObservedSessionCalendar,
    PartialMarketSessionError,
    UnknownMarketSessionError,
)
from quant_stack.data.ingestion import DataJob, IngestionService, RetryPolicy
from quant_stack.data.models import CalendarDayEvidence, CalendarEvidence, Instrument
from quant_stack.data.providers.cn import (
    CNProviderDiscrepancyReport,
    EvidenceMode,
    ProviderSourceIdentity,
    ProviderTransportIdentity,
)
from quant_stack.data.providers.cn.live import (
    AKSHARE_LIVE_DAILY_SCHEMA,
    AKSHARE_LIVE_RUNTIME_CONTRACT,
    BAOSTOCK_LIVE_DAILY_SCHEMA,
    TDX_REFERENCE_LIVE_DAILY_SCHEMA,
    TDX_REFERENCE_LIVE_RUNTIME_CONTRACT,
    AKShareLiveProvider,
    BaoStockLiveProvider,
    BaoStockRequestBudget,
    TDXReferenceLiveProvider,
    normalize_akshare_live_bars,
    normalize_baostock_live_bars,
    normalize_tdx_reference_live_bars,
)
from quant_stack.data.providers.cn.replay import compare_replay_bars
from quant_stack.data.quality import ProviderNativeSchema, ProviderSchemaDriftError
from quant_stack.data.quality.provider_schema import ProviderSchemaValidator
from quant_stack.data.storage import StagingFileSystemStorage


def _instrument() -> Instrument:
    return Instrument(
        instrument_id="XSHG.600000",
        symbol="600000",
        market="CN",
        venue="XSHG",
        provider_symbol="600000",
        provider_exchange_alias="SH",
        currency="CNY",
        timezone="Asia/Shanghai",
    )


def _xshe_instrument() -> Instrument:
    return Instrument(
        instrument_id="XSHE.000001",
        symbol="000001",
        market="CN",
        venue="XSHE",
        provider_symbol="000001",
        provider_exchange_alias="SZ",
        currency="CNY",
        timezone="Asia/Shanghai",
    )


def _calendar(
    *,
    sessions: tuple[date, ...] = (date(2026, 7, 31),),
    non_sessions: tuple[date, ...] = (),
    observed_at: datetime = datetime(2026, 8, 3, tzinfo=UTC),
    official: bool = True,
) -> ObservedSessionCalendar:
    dates = (*sessions, *non_sessions)
    if not official:
        return ObservedSessionCalendar(
            CalendarEvidence(
                source="controlled provider observation",
                source_kind="provider_observed",
                version="test-provider-v1",
                market="CN",
                timezone="Asia/Shanghai",
                open_time=time(9, 30),
                close_time=time(15),
                supported_start=min(dates),
                supported_end=max(dates),
                confirmed_sessions=sessions,
                confirmed_non_sessions=non_sessions,
                observed_at=observed_at,
            )
        )
    day_evidence = tuple(
        CalendarDayEvidence(
            trading_date=day,
            state=(
                "session"
                if day in sessions
                else "weekend"
                if day.weekday() >= 5
                else "holiday"
            ),
            source_refs=("https://exchange.example/official-notice",),
        )
        for day in sorted(dates)
    )
    return ObservedSessionCalendar(
        CalendarEvidence(
            schema_version="1.1.0",
            source="controlled official exchange fixture",
            source_kind="exchange_announcement",
            version="test-official-v1",
            market="CN",
            timezone="Asia/Shanghai",
            open_time=time(9, 30),
            close_time=time(15),
            supported_start=min(dates),
            supported_end=max(dates),
            confirmed_sessions=sessions,
            confirmed_non_sessions=non_sessions,
            observed_at=observed_at,
            exchanges=("XSHG", "XSHE"),
            day_evidence=day_evidence,
            raw_evidence_uri="quant-data://reference/calendar-raw.json",
            raw_evidence_sha256="a" * 64,
            normalized_evidence_uri="quant-data://reference/calendar-normalized.json",
            normalized_evidence_sha256="b" * 64,
        )
    )


def _identity(provider: str, raw_uri: str, raw_hash: str) -> ProviderSourceIdentity:
    return ProviderSourceIdentity(
        provider=provider,
        endpoint=(
            "query_history_k_data_plus" if provider == "baostock" else "stock_zh_a_hist"
        ),
        provider_version="0.9.3" if provider == "baostock" else "1.18.81",
        adapter_version="real-cn-v1" if provider == "baostock" else "real-cn-v2",
        native_schema_version=(
            "live-observed-2026-08-03-v1"
            if provider == "baostock"
            else "live-observed-2026-08-03-v1"
        ),
        raw_uri=raw_uri,
        raw_sha256=raw_hash,
    )


def test_live_schema_drift_blocks_before_normalization() -> None:
    record = {
        "日期": "2026-07-31",
        "股票代码": "600000",
        "开盘": 10.0,
        "最高": 10.2,
        "最低": 9.9,
        "收盘": 10.1,
        "成交量": "unknown-unit-value",
        "成交额": 1_000_000.0,
        "复权": "none",
        "_retrieval_timestamp": datetime(2026, 8, 3, tzinfo=UTC).isoformat(),
    }
    with pytest.raises(ProviderSchemaDriftError, match="成交量"):
        ProviderSchemaValidator().validate(
            provider="akshare",
            records=[record],
            schema=AKSHARE_LIVE_DAILY_SCHEMA,
        )


def test_live_normalizers_preserve_explicit_unit_and_unadjusted_lineage() -> None:
    instrument = _instrument()
    lookup = {instrument.symbol: instrument}
    retrieved = datetime(2026, 8, 3, tzinfo=UTC).isoformat()
    akshare = normalize_akshare_live_bars(
        [{
            "日期": "2026-07-31", "股票代码": "600000", "开盘": 10.0,
            "最高": 10.2, "最低": 9.9, "收盘": 10.1, "成交量": 123.0,
            "成交额": 124_000.0, "复权": "none",
            "_retrieval_timestamp": retrieved,
        }],
        lookup,
        _calendar(),
    )[0]
    baostock = normalize_baostock_live_bars(
        [{
            "date": "2026-07-31", "code": "sh.600000", "open": "10.0",
            "high": "10.2", "low": "9.9", "close": "10.1", "volume": "12300",
            "amount": "124000", "adjustflag": "3",
            "_retrieval_timestamp": retrieved,
        }],
        lookup,
        _calendar(),
    )[0]

    assert akshare["volume"] == baostock["volume"] == 12_300
    assert akshare["lineage"]["original_volume_unit"] == "lot"
    assert akshare["lineage"]["applied_volume_multiplier"] == 100
    assert baostock["lineage"]["original_volume_unit"] == "share"
    assert baostock["lineage"]["applied_volume_multiplier"] == 1
    assert akshare["lineage"]["adjustment"] == "none"


def test_akshare_provider_scopes_and_restores_history_transport(monkeypatch) -> None:
    original_requests = object()
    stock_hist_module = SimpleNamespace(requests=original_requests)
    observed: dict[str, object] = {}
    injected_transport = object()

    class _Frame:
        def to_dict(self, *, orient: str) -> list[dict[str, object]]:
            assert orient == "records"
            return [{
                "日期": date(2026, 7, 31),
                "开盘": 10.0,
                "最高": 10.2,
                "最低": 9.9,
                "收盘": 10.1,
                "成交量": 123.0,
                "成交额": 124_000.0,
            }]

    def stock_zh_a_hist(**kwargs: object) -> _Frame:
        observed.update(kwargs)
        assert stock_hist_module.requests is injected_transport
        return _Frame()

    akshare_module = SimpleNamespace(stock_zh_a_hist=stock_zh_a_hist)

    def fake_import(name: str) -> object:
        return (
            stock_hist_module
            if name == "akshare.stock_feature.stock_hist_em"
            else akshare_module
        )

    monkeypatch.setattr(
        "quant_stack.data.providers.cn.live.providers._require_runtime_version",
        lambda *_: None,
    )
    monkeypatch.setattr(
        "quant_stack.data.providers.cn.live.providers.import_module", fake_import
    )
    provider = AKShareLiveProvider(
        (_instrument(),), request_transport=injected_transport  # type: ignore[arg-type]
    )

    rows = list(
        provider.fetch_bars(
            "CN", ["XSHG.600000"], date(2026, 7, 31), date(2026, 7, 31)
        )
    )

    assert stock_hist_module.requests is original_requests
    assert observed == {
        "symbol": "600000",
        "period": "daily",
        "start_date": "20260731",
        "end_date": "20260731",
        "adjust": "",
        "timeout": 15,
    }
    assert rows[0]["股票代码"] == "600000"


def test_adjusted_live_raw_is_rejected() -> None:
    instrument = _instrument()
    with pytest.raises(ValueError, match="unadjusted"):
        normalize_baostock_live_bars(
            [{
                "date": date(2026, 7, 31).isoformat(), "code": "sh.600000",
                "open": "10", "high": "10", "low": "10", "close": "10",
                "volume": "0", "amount": "0", "adjustflag": "2",
                "_retrieval_timestamp": datetime(2026, 8, 3, tzinfo=UTC).isoformat(),
            }],
            {instrument.symbol: instrument},
            _calendar(),
        )


def test_live_discrepancy_evidence_requires_both_raw_hashes() -> None:
    with pytest.raises(ValidationError, match="both Raw hashes"):
        CNProviderDiscrepancyReport(
            report_id="live-overlap",
            generated_at=datetime(2026, 8, 3, tzinfo=UTC),
            evidence_mode="live",
            fixture_only=False,
            status="passed",
            canonical_selection_reason="within explicit tolerance",
        )


def test_serious_live_overlap_difference_is_quarantined() -> None:
    primary_identity = _identity(
        "baostock", "quant-data://raw/baostock.json", "a" * 64
    )
    comparison_identity = _identity(
        "akshare", "quant-data://raw/akshare.json", "b" * 64
    )
    base = {
        "instrument_id": "XSHG.600000",
        "ts_event": datetime(2026, 7, 31, 7, tzinfo=UTC),
        "open": 10.0,
        "high": 10.2,
        "low": 9.9,
        "close": 10.1,
        "volume": 12_300,
        "amount": 124_000,
        "lineage": primary_identity.model_dump(mode="python"),
    }
    report = compare_replay_bars(
        [base],
        [{
            **base,
            "close": 11.0,
            "lineage": comparison_identity.model_dump(mode="python"),
        }],
        primary_provider="baostock",
        comparison_provider="akshare",
        primary_raw_uri="quant-data://raw/baostock.json",
        comparison_raw_uri="quant-data://raw/akshare.json",
        evidence_mode=EvidenceMode.LIVE,
        primary_raw_sha256="a" * 64,
        comparison_raw_sha256="b" * 64,
        primary_identity=primary_identity,
        comparison_identity=comparison_identity,
    )

    assert report.status == "quarantined"
    assert report.records[0].differing_fields == ("close",)


def test_live_identity_matching_is_exact_not_prefix_based() -> None:
    primary_identity = _identity(
        "baostock", "quant-data://raw/baostock.json", "a" * 64
    )
    comparison_identity = _identity(
        "akshare", "quant-data://raw/akshare.json", "b" * 64
    )
    row = {
        "instrument_id": "XSHG.600000",
        "ts_event": datetime(2026, 7, 31, 7, tzinfo=UTC),
        "open": 10.0,
        "high": 10.2,
        "low": 9.9,
        "close": 10.1,
        "volume": 12_300,
        "amount": 124_000,
        "lineage": primary_identity.model_dump(mode="python"),
    }
    comparison = {
        **row,
        "lineage": {
            **comparison_identity.model_dump(mode="python"),
            "provider": "akshare-prefix",
        },
    }
    report = compare_replay_bars(
        [row],
        [comparison],
        primary_provider="baostock",
        comparison_provider="akshare",
        primary_raw_uri=primary_identity.raw_uri,
        comparison_raw_uri=comparison_identity.raw_uri,
        evidence_mode=EvidenceMode.LIVE,
        primary_raw_sha256=primary_identity.raw_sha256,
        comparison_raw_sha256=comparison_identity.raw_sha256,
        primary_identity=primary_identity,
        comparison_identity=comparison_identity,
    )
    assert report.status == "quarantined"
    assert report.records[0].differing_fields == ("endpoint_source_drift",)


def test_live_comparison_rejects_empty_source_as_unavailable_not_passed() -> None:
    primary_identity = _identity(
        "baostock", "quant-data://raw/baostock.json", "a" * 64
    )
    comparison_identity = _identity(
        "akshare", "quant-data://raw/akshare.json", "b" * 64
    )
    with pytest.raises(ValueError, match="use unavailable evidence"):
        compare_replay_bars(
            [],
            [],
            primary_provider="baostock",
            comparison_provider="akshare",
            primary_raw_uri=primary_identity.raw_uri,
            comparison_raw_uri=comparison_identity.raw_uri,
            evidence_mode=EvidenceMode.LIVE,
            primary_raw_sha256=primary_identity.raw_sha256,
            comparison_raw_sha256=comparison_identity.raw_sha256,
            primary_identity=primary_identity,
            comparison_identity=comparison_identity,
        )


def test_observed_calendar_fails_closed_for_unknown_holiday_weekend_and_partial() -> None:
    known = _calendar(
        sessions=(date(2026, 7, 31),),
        non_sessions=(date(2026, 8, 1), date(2026, 10, 1)),
        official=False,
    )
    assert known.session_close("CN", date(2026, 7, 31)) is not None
    assert known.session_close("CN", date(2026, 8, 1)) is None
    assert known.session_close("CN", date(2026, 10, 1)) is None
    with pytest.raises(UnknownMarketSessionError):
        known.session_close("CN", date(2026, 8, 3))

    partial = _calendar(
        observed_at=datetime(2026, 7, 31, 6, tzinfo=UTC)
    )
    with pytest.raises(PartialMarketSessionError):
        partial.session_close("CN", date(2026, 7, 31))


def test_live_provider_ingestion_rejects_synthetic_calendar_before_io(tmp_path) -> None:
    provider = BaoStockLiveProvider((_instrument(),))
    service = IngestionService(
        providers={provider.name: provider},
        storage=StagingFileSystemStorage(tmp_path),
        bar_normalizers={provider.name: normalize_baostock_live_bars},
    )
    with pytest.raises(ValueError, match="verified calendar evidence"):
        service.run(
            DataJob(
                dataset="cn_equity_daily",
                dataset_version="calendar-required-v1",
                provider="baostock",
                provider_version="0.9.3",
                provider_endpoint="query_history_k_data_plus",
                adapter_version="real-cn-v1",
                native_schema_version="live-observed-2026-08-03-v1",
                market="CN",
                frequency="1d",
                retry_policy=RetryPolicy(max_attempts=1, backoff_seconds=0),
                symbols=["XSHG.600000"],
                start=date(2026, 7, 31),
                end=date(2026, 7, 31),
            )
        )


def test_live_schema_contracts_include_retrieval_evidence() -> None:
    assert isinstance(BAOSTOCK_LIVE_DAILY_SCHEMA, ProviderNativeSchema)
    assert "_retrieval_timestamp" in BAOSTOCK_LIVE_DAILY_SCHEMA.required_fields
    assert (
        AKSHARE_LIVE_RUNTIME_CONTRACT.native_schema_version
        == "live-observed-2026-08-03-v1"
    )
    assert TDX_REFERENCE_LIVE_RUNTIME_CONTRACT.provider_version == "0.2.7"
    assert "vol" in TDX_REFERENCE_LIVE_DAILY_SCHEMA.required_fields


def test_baostock_live_provider_bounds_and_restores_socket_timeout(monkeypatch, tmp_path) -> None:
    observed_defaults: list[float | None] = []
    created_socket_timeouts: list[float | None] = []
    worker_defaults: list[float | None] = []
    queried_fields: list[str] = []
    call_started = threading.Event()
    worker_done = threading.Event()

    class Result:
        error_code = "0"
        fields = [
            "date",
            "code",
            "open",
            "high",
            "low",
            "close",
            "preclose",
            "volume",
            "amount",
            "adjustflag",
        ]

        @staticmethod
        def next() -> bool:
            return False

    class Login:
        error_code = "0"

    class Client:
        util = SimpleNamespace(socketutil=SimpleNamespace(socket=socket))

        @classmethod
        def login(cls):
            observed_defaults.append(socket.getdefaulttimeout())
            call_started.set()
            created = cls.util.socketutil.socket.socket(
                socket.AF_INET, socket.SOCK_STREAM
            )
            try:
                created_socket_timeouts.append(created.gettimeout())
            finally:
                created.close()
            assert worker_done.wait(timeout=2)
            return Login()

        @classmethod
        def query_history_k_data_plus(cls, *_args, **_kwargs):
            observed_defaults.append(socket.getdefaulttimeout())
            queried_fields.append(str(_args[1]))
            return Result()

        @staticmethod
        def logout() -> None:
            observed_defaults.append(socket.getdefaulttimeout())

    monkeypatch.setattr(
        "quant_stack.data.providers.cn.live.providers.import_module",
        lambda _name: Client,
    )
    instrument = _instrument().model_copy(
        update={"provider_symbol": "sh.600000", "provider_exchange_alias": "sh"}
    )
    provider = BaoStockLiveProvider(
        (instrument,),
        request_budget=BaoStockRequestBudget(
            tmp_path / "control" / "baostock-request-budget.json",
            min_batch_interval_seconds=0,
        ),
    )
    previous = socket.getdefaulttimeout()
    worker = threading.Thread(
        target=lambda: (
            call_started.wait(timeout=2),
            worker_defaults.append(socket.getdefaulttimeout()),
            worker_done.set(),
        ),
        daemon=True,
    )
    worker.start()

    assert list(
        provider.fetch_bars(
            "CN", [instrument.instrument_id], date(2026, 7, 31), date(2026, 7, 31)
        )
    ) == []
    worker.join(timeout=2)
    assert queried_fields and "preclose" in queried_fields[0].split(",")
    assert not worker.is_alive()
    assert observed_defaults == [previous, previous, previous]
    assert worker_defaults == [previous]
    assert created_socket_timeouts == [30.0]
    assert Client.util.socketutil.socket is socket
    assert socket.getdefaulttimeout() == previous


def test_tdx_reference_rotates_bounded_candidates_and_normalizes_lots() -> None:
    class FakeClient:
        def __init__(self, *, connect_error: bool = False) -> None:
            self.connect_error = connect_error
            self.closed = False

        def connect(self, host: str, port: int, *, time_out: int) -> None:
            assert host and port == 7709 and time_out == 5
            if self.connect_error:
                raise ConnectionError("controlled candidate failure")

        def get_security_bars(
            self, category: int, market: int, code: str, start: int, count: int
        ) -> list[dict[str, object]]:
            assert (category, start, count) == (9, 0, 800)
            assert (market, code) in ((1, "600000"), (0, "000001"))
            return [{
                "open": 10.0,
                "high": 10.2,
                "low": 9.9,
                "close": 10.1,
                "vol": 123.95,
                "amount": 124_000.0,
                "datetime": "2026-07-31 15:00",
            }]

        def close(self) -> None:
            self.closed = True

    clients = iter((FakeClient(connect_error=True), FakeClient()))
    instruments = (_instrument(), _xshe_instrument())
    provider = TDXReferenceLiveProvider(
        instruments,
        verified_sessions=(date(2026, 7, 31),),
        hosts=(("8.8.8.8", 7709), ("1.1.1.1", 7709)),
        max_host_attempts=2,
        client_factory=lambda: next(clients),
    )
    records = list(
        provider.fetch_bars(
            "CN",
            [item.instrument_id for item in instruments],
            date(2026, 7, 31),
            date(2026, 7, 31),
        )
    )
    ProviderSchemaValidator().validate(
        provider=provider.name,
        records=records,
        schema=provider.bar_schema,
    )
    normalized = normalize_tdx_reference_live_bars(
        records,
        {item.symbol: item for item in instruments},
        _calendar(),
    )

    assert len(normalized) == 2
    assert {item["volume"] for item in normalized} == {12_395}
    assert {item["lineage"]["original_volume_unit"] for item in normalized} == {"lot"}
    assert {item["lineage"]["applied_volume_multiplier"] for item in normalized} == {100}
    audit = provider.transport_audit()
    assert audit["schema_version"] == "1.0.0"
    assert audit["configured_candidates"] == 2
    assert audit["max_host_attempts"] == 2
    assert len(audit["attempted_source_fingerprints"]) == 2
    assert audit["successful_source_fingerprint"] == audit[
        "attempted_source_fingerprints"
    ][1]
    assert audit["failure_classes"] == ("ConnectionError",)
    assert audit["close_failure_classes"] == ()
    assert audit["transport_security"] == "unauthenticated_unencrypted_tcp"
    assert audit["server_authenticated"] is False
    assert audit["encrypted"] is False
    assert audit["host_pool_contract_id"].endswith(audit["host_pool_fingerprint"][:16])


def test_tdx_reference_rejects_bjse_scope() -> None:
    bjse = Instrument(
        instrument_id="BJSE.920047",
        symbol="920047",
        market="CN",
        venue="BJSE",
        provider_symbol="920047",
        provider_exchange_alias="BJ",
        currency="CNY",
        timezone="Asia/Shanghai",
    )
    with pytest.raises(ValueError, match="only XSHG and XSHE"):
        TDXReferenceLiveProvider((bjse,), verified_sessions=(date(2026, 7, 31),))


@pytest.mark.parametrize(
    "host",
    ("not-an-ip", "127.0.0.1", "10.0.0.1", "169.254.1.1", "224.0.0.1", "::1"),
)
def test_tdx_reference_rejects_non_public_or_invalid_hosts(host: str) -> None:
    with pytest.raises(ValueError, match="public|IPv4"):
        TDXReferenceLiveProvider(
            (_instrument(),),
            verified_sessions=(date(2026, 7, 31),),
            hosts=((host, 7709),),
            max_host_attempts=1,
        )


def test_tdx_reference_rotates_on_incomplete_duplicate_and_empty_candidates() -> None:
    class FakeClient:
        def __init__(self, mode: str) -> None:
            self.mode = mode

        def connect(self, *_: object, **__: object) -> bool:
            return True

        def get_security_bars(
            self, category: int, market: int, code: str, start: int, count: int
        ) -> list[dict[str, object]]:
            del category, market, start, count
            row = {
                "open": 10.0,
                "high": 10.2,
                "low": 9.9,
                "close": 10.1,
                "vol": 123.95,
                "amount": 124_000.0,
                "datetime": "2026-07-31 15:00",
            }
            if self.mode == "empty":
                return []
            if self.mode == "missing" and code == "000001":
                return []
            if self.mode == "duplicate":
                return [row, row]
            return [row]

        def close(self) -> None:
            return None

    instruments = (_instrument(), _xshe_instrument())
    for invalid_mode in ("empty", "missing", "duplicate"):
        clients = iter((FakeClient(invalid_mode), FakeClient("valid")))
        provider = TDXReferenceLiveProvider(
            instruments,
            verified_sessions=(date(2026, 7, 31),),
            hosts=(("8.8.8.8", 7709), ("1.1.1.1", 7709)),
            max_host_attempts=2,
            client_factory=lambda clients=clients: next(clients),
        )
        records = list(
            provider.fetch_bars(
                "CN",
                [item.instrument_id for item in instruments],
                date(2026, 7, 31),
                date(2026, 7, 31),
            )
        )
        assert len(records) == 2
        assert len(provider.transport_audit()["attempted_source_fingerprints"]) == 2


def test_tdx_reference_close_failure_does_not_mask_request_failure_or_stop_rotation() -> None:
    class FakeClient:
        def __init__(self, *, fail_request: bool, fail_close: bool) -> None:
            self.fail_request = fail_request
            self.fail_close = fail_close

        def connect(self, *_: object, **__: object) -> bool:
            if self.fail_request:
                raise ConnectionError("controlled request failure")
            return True

        def get_security_bars(
            self, category: int, market: int, code: str, start: int, count: int
        ) -> list[dict[str, object]]:
            del category, market, code, start, count
            return [{
                "open": 10.0,
                "high": 10.2,
                "low": 9.9,
                "close": 10.1,
                "vol": 123.95,
                "amount": 124_000.0,
                "datetime": "2026-07-31 15:00",
            }]

        def close(self) -> None:
            if self.fail_close:
                raise RuntimeError("controlled close failure")

    clients = iter(
        (
            FakeClient(fail_request=True, fail_close=True),
            FakeClient(fail_request=False, fail_close=False),
        )
    )
    provider = TDXReferenceLiveProvider(
        (_instrument(),),
        verified_sessions=(date(2026, 7, 31),),
        hosts=(("8.8.8.8", 7709), ("1.1.1.1", 7709)),
        max_host_attempts=2,
        client_factory=lambda: next(clients),
    )
    assert len(
        list(
            provider.fetch_bars(
                "CN", ["XSHG.600000"], date(2026, 7, 31), date(2026, 7, 31)
            )
        )
    ) == 1
    audit = provider.transport_audit()
    assert audit["failure_classes"] == ("ConnectionError",)
    assert audit["close_failure_classes"] == ("RuntimeError",)


def test_tdx_reference_host_pool_change_changes_contract_identity() -> None:
    common = {
        "instruments": (_instrument(),),
        "verified_sessions": (date(2026, 7, 31),),
        "max_host_attempts": 1,
    }
    first = TDXReferenceLiveProvider(hosts=(("8.8.8.8", 7709),), **common)
    second = TDXReferenceLiveProvider(hosts=(("1.1.1.1", 7709),), **common)
    assert first.transport_audit()["host_pool_fingerprint"] != second.transport_audit()[
        "host_pool_fingerprint"
    ]
    assert first.transport_audit()["host_pool_contract_id"] != second.transport_audit()[
        "host_pool_contract_id"
    ]


def test_tdx_transport_identity_rejects_successful_host_drift() -> None:
    with pytest.raises(ValidationError, match="successful source"):
        ProviderTransportIdentity(
            host_pool_contract_id="tdx-public-host-pool-v1:" + "a" * 16,
            host_pool_contract_version="tdx-public-host-pool-v1",
            host_pool_fingerprint="a" * 64,
            configured_candidates=2,
            max_host_attempts=2,
            attempted_source_fingerprints=("b" * 64,),
            successful_source_fingerprint="c" * 64,
            protocol_identity="tdx-standard-market-binary/category-9/daily-bars",
            client_package="tdxpy",
            client_version="0.2.7",
            transport_security="unauthenticated_unencrypted_tcp",
            server_authenticated=False,
            encrypted=False,
            source_terms_status="unverified",
        )


def test_tdx_volume_rejects_fractional_canonical_share() -> None:
    retrieved = datetime(2026, 8, 4, tzinfo=UTC).isoformat()
    with pytest.raises(ValueError, match="fractional canonical share"):
        normalize_tdx_reference_live_bars(
            [{
                "datetime": "2026-07-31 15:00",
                "code": "600000",
                "market": 1,
                "open": 10.0,
                "high": 10.2,
                "low": 9.9,
                "close": 10.1,
                "vol": 123.951,
                "amount": 124_000.0,
                "adjustment": "none",
                "_retrieval_timestamp": retrieved,
            }],
            {"600000": _instrument()},
            _calendar(),
        )


@pytest.mark.parametrize("port", (0, 65536, -1, True, "7709"))
def test_tdx_reference_rejects_invalid_ports(port: object) -> None:
    with pytest.raises(ValueError, match="ports"):
        TDXReferenceLiveProvider(
            (_instrument(),),
            verified_sessions=(date(2026, 7, 31),),
            hosts=(("8.8.8.8", port),),  # type: ignore[arg-type]
            max_host_attempts=1,
        )
