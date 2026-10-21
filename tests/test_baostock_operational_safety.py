from __future__ import annotations

from datetime import date
from multiprocessing import get_context
from pathlib import Path
from types import SimpleNamespace

import pytest

from quant_stack.data.models import Instrument
from quant_stack.data.providers.cn.live import (
    BAOSTOCK_HISTORY_REQUEST_INTERVAL_SECONDS,
    BaoStockAccessRejected,
    BaoStockDailyBatchLimitExceeded,
    BaoStockDailyQuotaExceeded,
    BaoStockLiveProvider,
    BaoStockRequestBudget,
)


def _instrument() -> Instrument:
    return Instrument(
        instrument_id="XSHG.600000",
        symbol="600000",
        market="CN",
        venue="XSHG",
        provider_symbol="sh.600000",
        provider_exchange_alias="sh",
        currency="CNY",
        timezone="Asia/Shanghai",
    )


def _instruments(count: int) -> tuple[Instrument, ...]:
    return tuple(
        Instrument(
            instrument_id=f"XSHG.{600000 + index:06d}",
            symbol=f"{600000 + index:06d}",
            market="CN",
            venue="XSHG",
            provider_symbol=f"sh.{600000 + index:06d}",
            provider_exchange_alias="sh",
            currency="CNY",
            timezone="Asia/Shanghai",
        )
        for index in range(count)
    )


def _acquire_session_lock(
    path: Path, attempting: object, entered: object, release: object
) -> None:
    attempting.set()
    budget = BaoStockRequestBudget(path, min_batch_interval_seconds=0)
    with budget.session_lock():
        entered.set()
        release.wait(timeout=5)


def test_request_budget_persists_and_stops_before_daily_limit(tmp_path) -> None:
    budget = BaoStockRequestBudget(
        tmp_path / "control" / "baostock-request-budget.json",
        daily_limit=3,
        min_batch_interval_seconds=0,
    )
    budget.reserve(2)
    assert budget.snapshot()["days"]
    with pytest.raises(BaoStockDailyQuotaExceeded, match="used=2 requested=2 limit=3"):
        budget.ensure_capacity(2)
    budget.reserve(1)
    with pytest.raises(BaoStockDailyQuotaExceeded):
        budget.reserve(1)


def test_shared_control_root_accumulates_across_session_data_roots(tmp_path) -> None:
    control_root = tmp_path / "worker" / "control"
    first = BaoStockRequestBudget.for_control_root(control_root)
    second = BaoStockRequestBudget.for_control_root(control_root)

    assert first.path == second.path
    assert first.session_lock_path == second.session_lock_path
    first.reserve(24_500)
    second.reserve(24_500)
    with pytest.raises(BaoStockDailyQuotaExceeded):
        first.reserve(1)


def test_blacklist_marks_local_cooldown_and_never_retries(monkeypatch, tmp_path) -> None:
    calls = {"login": 0, "query": 0}

    class Client:
        util = SimpleNamespace(socketutil=SimpleNamespace(socket=__import__("socket")))

        @classmethod
        def login(cls):
            calls["login"] += 1
            return SimpleNamespace(error_code="10001011", error_msg="黑名单用户，请与管理员联系")

        @classmethod
        def query_history_k_data_plus(cls, *_args, **_kwargs):
            calls["query"] += 1
            raise AssertionError("blacklist must stop before a history query")

    monkeypatch.setattr(
        "quant_stack.data.providers.cn.live.providers._require_runtime_version",
        lambda *_: None,
    )
    monkeypatch.setattr(
        "quant_stack.data.providers.cn.live.providers.import_module",
        lambda _name: Client,
    )
    budget = BaoStockRequestBudget(
        tmp_path / "control" / "baostock-request-budget.json",
        daily_limit=10,
        min_batch_interval_seconds=0,
    )
    provider = BaoStockLiveProvider((_instrument(),), request_budget=budget)
    with pytest.raises(BaoStockAccessRejected, match="10001011"):
        list(provider.fetch_bars("CN", ["XSHG.600000"], date(2026, 7, 31), date(2026, 7, 31)))
    with pytest.raises(BaoStockAccessRejected, match="local cooldown"):
        list(provider.fetch_bars("CN", ["XSHG.600000"], date(2026, 7, 31), date(2026, 7, 31)))
    assert calls == {"login": 1, "query": 0}
    blocked = budget.snapshot()["blocked"]
    assert isinstance(blocked, dict)
    assert blocked["error_code"] == "10001011"


def test_batch_control_records_single_flight_and_completion(tmp_path) -> None:
    budget = BaoStockRequestBudget(
        tmp_path / "control" / "baostock-request-budget.json",
        daily_limit=10,
        min_batch_interval_seconds=0,
    )
    with budget.session_lock():
        budget.wait_for_batch_slot()
        budget.ensure_capacity(2)
        budget.begin_batch(1)
        budget.reserve(2)
        budget.complete_batch()
    snapshot = budget.snapshot()
    day_record = next(iter(snapshot["days"].values()))
    assert day_record["used"] == 2
    assert day_record["batches"] == 1
    assert snapshot["last_batch_completed_at"]


def test_session_lock_serializes_independent_processes(tmp_path) -> None:
    context = get_context("spawn")
    attempting = context.Event()
    entered = context.Event()
    release = context.Event()
    path = tmp_path / "control" / "baostock-request-budget.json"
    budget = BaoStockRequestBudget(path, min_batch_interval_seconds=0)

    with budget.session_lock():
        process = context.Process(
            target=_acquire_session_lock,
            args=(path, attempting, entered, release),
        )
        process.start()
        assert attempting.wait(timeout=2)
        assert not entered.wait(timeout=0.2)

    assert entered.wait(timeout=2)
    release.set()
    process.join(timeout=2)
    assert process.exitcode == 0


def test_live_provider_accepts_500_instruments_and_queries_each_serially(
    monkeypatch, tmp_path
) -> None:
    calls = {"login": 0, "query": 0, "logout": 0, "sleep": []}

    class Result:
        error_code = "0"
        error_msg = "success"
        fields: tuple[str, ...] = ()

        @staticmethod
        def next() -> bool:
            return False

    class Client:
        util = SimpleNamespace(socketutil=SimpleNamespace(socket=__import__("socket")))

        @classmethod
        def login(cls):
            calls["login"] += 1
            return SimpleNamespace(error_code="0", error_msg="success")

        @classmethod
        def query_history_k_data_plus(cls, *_args, **_kwargs):
            calls["query"] += 1
            return Result()

        @classmethod
        def logout(cls) -> None:
            calls["logout"] += 1

    monkeypatch.setattr(
        "quant_stack.data.providers.cn.live.providers._require_runtime_version",
        lambda *_: None,
    )
    monkeypatch.setattr(
        "quant_stack.data.providers.cn.live.providers.import_module",
        lambda _name: Client,
    )
    monkeypatch.setattr(
        "quant_stack.data.providers.cn.live.providers.time.sleep",
        lambda seconds: calls["sleep"].append(seconds),
    )
    instruments = _instruments(500)
    provider = BaoStockLiveProvider(
        instruments,
        request_budget=BaoStockRequestBudget(
            tmp_path / "control" / "baostock-request-budget.json",
            daily_limit=1_000,
            min_batch_interval_seconds=0,
        ),
    )

    assert list(
        provider.fetch_bars(
            "CN",
            [item.instrument_id for item in instruments],
            date(2026, 7, 31),
            date(2026, 7, 31),
        )
    ) == []
    assert calls["login"] == 1
    assert calls["query"] == 500
    assert calls["logout"] == 1
    assert calls["sleep"] == [BAOSTOCK_HISTORY_REQUEST_INTERVAL_SECONDS] * 500


def test_live_provider_paces_failed_history_requests(monkeypatch, tmp_path) -> None:
    sleeps: list[float] = []

    class Client:
        util = SimpleNamespace(socketutil=SimpleNamespace(socket=__import__("socket")))

        @staticmethod
        def login():
            return SimpleNamespace(error_code="0", error_msg="success")

        @staticmethod
        def query_history_k_data_plus(*_args, **_kwargs):
            raise OSError("controlled receive failure")

        @staticmethod
        def logout() -> None:
            return None

    monkeypatch.setattr(
        "quant_stack.data.providers.cn.live.providers._require_runtime_version",
        lambda *_: None,
    )
    monkeypatch.setattr(
        "quant_stack.data.providers.cn.live.providers.import_module",
        lambda _name: Client,
    )
    monkeypatch.setattr(
        "quant_stack.data.providers.cn.live.providers.time.sleep",
        sleeps.append,
    )
    provider = BaoStockLiveProvider(
        (_instrument(),),
        request_budget=BaoStockRequestBudget(
            tmp_path / "control" / "baostock-request-budget.json",
            daily_limit=10,
            min_batch_interval_seconds=0,
        ),
    )

    with pytest.raises(OSError, match="controlled receive failure"):
        list(
            provider.fetch_bars(
                "CN", ["XSHG.600000"], date(2026, 7, 31), date(2026, 7, 31)
            )
        )
    assert sleeps == [BAOSTOCK_HISTORY_REQUEST_INTERVAL_SECONDS]


def test_live_provider_reauthenticates_once_for_expired_session(monkeypatch, tmp_path) -> None:
    calls = {"login": 0, "query": 0, "logout": 0, "sleep": []}

    class Result:
        fields: tuple[str, ...] = ()

        def __init__(self, error_code: str, error_msg: str) -> None:
            self.error_code = error_code
            self.error_msg = error_msg

        @staticmethod
        def next() -> bool:
            return False

    class Client:
        util = SimpleNamespace(socketutil=SimpleNamespace(socket=__import__("socket")))

        @classmethod
        def login(cls):
            calls["login"] += 1
            return SimpleNamespace(error_code="0", error_msg="success")

        @classmethod
        def query_history_k_data_plus(cls, *_args, **_kwargs):
            calls["query"] += 1
            if calls["query"] == 2:
                return Result("10001001", "用户未登录")
            return Result("0", "success")

        @classmethod
        def logout(cls) -> None:
            calls["logout"] += 1

    monkeypatch.setattr(
        "quant_stack.data.providers.cn.live.providers._require_runtime_version",
        lambda *_: None,
    )
    monkeypatch.setattr(
        "quant_stack.data.providers.cn.live.providers.import_module",
        lambda _name: Client,
    )
    monkeypatch.setattr(
        "quant_stack.data.providers.cn.live.providers.time.sleep",
        lambda seconds: calls["sleep"].append(seconds),
    )
    instruments = _instruments(3)
    provider = BaoStockLiveProvider(
        instruments,
        request_budget=BaoStockRequestBudget(
            tmp_path / "control" / "baostock-request-budget.json",
            daily_limit=10,
            min_batch_interval_seconds=0,
        ),
    )

    assert list(
        provider.fetch_bars(
            "CN",
            [item.instrument_id for item in instruments],
            date(2026, 7, 31),
            date(2026, 7, 31),
        )
    ) == []
    assert calls == {
        "login": 2,
        "query": 4,
        "logout": 2,
        "sleep": [
            BAOSTOCK_HISTORY_REQUEST_INTERVAL_SECONDS,
            BAOSTOCK_HISTORY_REQUEST_INTERVAL_SECONDS,
            5.0,
            BAOSTOCK_HISTORY_REQUEST_INTERVAL_SECONDS,
            BAOSTOCK_HISTORY_REQUEST_INTERVAL_SECONDS,
        ],
    }


def test_live_provider_can_fail_closed_without_10001001_reauth_replay(
    monkeypatch, tmp_path
) -> None:
    calls = {"login": 0, "query": 0, "logout": 0, "sleep": []}

    class Result:
        error_code = "10001001"
        error_msg = "用户未登录"
        fields: tuple[str, ...] = ()

        @staticmethod
        def next() -> bool:
            raise AssertionError("expired-session response must fail before row iteration")

    class Client:
        util = SimpleNamespace(socketutil=SimpleNamespace(socket=__import__("socket")))

        @classmethod
        def login(cls):
            calls["login"] += 1
            return SimpleNamespace(error_code="0", error_msg="success")

        @classmethod
        def query_history_k_data_plus(cls, *_args, **_kwargs):
            calls["query"] += 1
            return Result()

        @classmethod
        def logout(cls) -> None:
            calls["logout"] += 1

    monkeypatch.setattr(
        "quant_stack.data.providers.cn.live.providers._require_runtime_version",
        lambda *_: None,
    )
    monkeypatch.setattr(
        "quant_stack.data.providers.cn.live.providers.import_module",
        lambda _name: Client,
    )
    monkeypatch.setattr(
        "quant_stack.data.providers.cn.live.providers.time.sleep",
        lambda seconds: calls["sleep"].append(seconds),
    )
    budget = BaoStockRequestBudget(
        tmp_path / "control" / "baostock-request-budget.json",
        daily_limit=2,
        min_batch_interval_seconds=0,
    )
    provider = BaoStockLiveProvider(
        (_instrument(),),
        request_budget=budget,
        allow_expired_session_reauth_replay=False,
    )

    with pytest.raises(ConnectionError, match="10001001"):
        list(
            provider.fetch_bars(
                "CN", ["XSHG.600000"], date(2026, 7, 31), date(2026, 7, 31)
            )
        )

    assert calls == {
        "login": 1,
        "query": 1,
        "logout": 0,
        "sleep": [BAOSTOCK_HISTORY_REQUEST_INTERVAL_SECONDS],
    }
    day_record = next(iter(budget.snapshot()["days"].values()))
    assert day_record["used"] == 2
    assert provider.transport_audit()["request_failure_classes"] == (("ConnectionError",),)


def test_live_provider_rejects_more_than_500_instruments(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(
        "quant_stack.data.providers.cn.live.providers._require_runtime_version",
        lambda *_: None,
    )
    monkeypatch.setattr(
        "quant_stack.data.providers.cn.live.providers.import_module",
        lambda _name: SimpleNamespace(),
    )
    instruments = _instruments(501)
    provider = BaoStockLiveProvider(
        instruments,
        request_budget=BaoStockRequestBudget(
            tmp_path / "control" / "baostock-request-budget.json",
            daily_limit=1_000,
            min_batch_interval_seconds=0,
        ),
    )

    with pytest.raises(ValueError, match="at most 500"):
        list(
            provider.fetch_bars(
                "CN",
                [item.instrument_id for item in instruments],
                date(2026, 7, 31),
                date(2026, 7, 31),
            )
        )


def test_batch_control_enforces_daily_serial_batch_ceiling(tmp_path) -> None:
    budget = BaoStockRequestBudget(
        tmp_path / "control" / "baostock-request-budget.json",
        daily_limit=49_000,
        min_batch_interval_seconds=0,
    )
    with budget.session_lock():
        for _ in range(144):
            budget.begin_batch(25)
            budget.complete_batch()
        with pytest.raises(BaoStockDailyBatchLimitExceeded, match="used=144 limit=144"):
            budget.begin_batch(25)


def test_batch_interval_env_override_scales_serial_batch_ceiling(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("QUANTSTACK_BAOSTOCK_MIN_BATCH_INTERVAL_SECONDS", "30")
    budget = BaoStockRequestBudget.for_data_root(tmp_path / "staging")
    assert budget.min_batch_interval_seconds == 30.0
    assert budget.max_batches_per_day == 2880
    with budget.session_lock():
        for _ in range(144):
            budget.begin_batch(25)
            budget.complete_batch()
        budget.begin_batch(25)
        budget.complete_batch()
    snapshot = budget.snapshot()
    day_record = next(iter(snapshot["days"].values()))
    assert day_record["batches"] == 145


def test_batch_interval_env_defaults_to_ten_minutes(monkeypatch, tmp_path) -> None:
    monkeypatch.delenv("QUANTSTACK_BAOSTOCK_MIN_BATCH_INTERVAL_SECONDS", raising=False)
    budget = BaoStockRequestBudget.for_data_root(tmp_path / "staging")
    assert budget.min_batch_interval_seconds == 600.0
    assert budget.max_batches_per_day == 144


def test_batch_interval_env_rejects_invalid_values(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("QUANTSTACK_BAOSTOCK_MIN_BATCH_INTERVAL_SECONDS", "not-a-number")
    with pytest.raises(ValueError, match="must be a number"):
        BaoStockRequestBudget.for_data_root(tmp_path / "staging")
    monkeypatch.setenv("QUANTSTACK_BAOSTOCK_MIN_BATCH_INTERVAL_SECONDS", "-5")
    with pytest.raises(ValueError, match="between 0 and 86400"):
        BaoStockRequestBudget.for_data_root(tmp_path / "staging")
