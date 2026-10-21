from __future__ import annotations

import json
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from quant_stack.data.providers.cn.live import BaoStockRequestBudget
from scripts.acquire_cn_qfq_factors import acquire_factors, factor_request_budget


class _Result:
    error_code = "0"
    error_msg = "success"
    fields = ["dividOperateDate", "backAdjustFactor"]

    def __init__(self) -> None:
        self._remaining = [["2025-01-01", "1.0"]]

    def next(self) -> bool:
        return bool(self._remaining)

    def get_row_data(self) -> list[str]:
        return self._remaining.pop(0)


class _Client:
    def __init__(self) -> None:
        self.symbols: list[str] = []
        self.queries: list[dict[str, str]] = []

    def login(self) -> SimpleNamespace:
        return SimpleNamespace(error_code="0", error_msg="success")

    def logout(self) -> None:
        return None

    def query_adjust_factor(self, symbol: str, **_kwargs: str) -> _Result:
        self.symbols.append(symbol)
        self.queries.append(_kwargs)
        return _Result()


class _HangingClient(_Client):
    def query_adjust_factor(self, symbol: str, **_kwargs: str) -> _Result:
        time.sleep(1)
        return super().query_adjust_factor(symbol)


class _EmptyResult(_Result):
    def __init__(self) -> None:
        self._remaining = []


class _EmptyClient(_Client):
    def query_adjust_factor(self, symbol: str, **_kwargs: str) -> _Result:
        self.symbols.append(symbol)
        return _EmptyResult()


def test_factor_acquisition_is_serial_and_uses_fixed_pacing(tmp_path: Path) -> None:
    universe = [
        {"instrument_id": f"XSHG.{index:06d}", "provider_symbol": f"sh.{index:06d}"}
        for index in range(502)
    ]
    sleeps: list[float] = []
    output = tmp_path / "factors.json"
    client = _Client()

    report = acquire_factors(
        client=client,
        universe=universe,
        output_path=output,
        start_date="2015-01-01",
        end_date="2026-08-06",
        sleep=sleeps.append,
    )

    assert report == {"request_count": 502, "complete": True}
    assert len(client.symbols) == 502
    assert sleeps.count(5.0) == 1
    assert sleeps.count(0.05) == 500
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["complete"] is True
    assert payload["request_policy"]["concurrency"] == 1


def test_factor_acquisition_uses_shared_budget_and_session_lock(tmp_path: Path) -> None:
    universe = [
        {"instrument_id": f"XSHG.{index:06d}", "provider_symbol": f"sh.{index:06d}"}
        for index in range(502)
    ]
    budget = BaoStockRequestBudget(
        tmp_path / "control" / "baostock-request-budget.json",
        min_batch_interval_seconds=0,
    )

    report = acquire_factors(
        client=_Client(),
        universe=universe,
        output_path=tmp_path / "factors.json",
        start_date="1990-01-01",
        end_date="2026-08-11",
        sleep=lambda _seconds: None,
        request_budget=budget,
    )

    assert report == {"request_count": 502, "complete": True}
    day = next(iter(budget.snapshot()["days"].values()))
    assert day["used"] == 503
    assert day["batches"] == 2


def test_factor_acquisition_hard_timeout_preserves_checkpoint(tmp_path: Path) -> None:
    output = tmp_path / "factors.json"

    with pytest.raises(TimeoutError, match="exceeded"):
        acquire_factors(
            client=_HangingClient(),
            universe=[{"instrument_id": "XSHG.600000", "provider_symbol": "sh.600000"}],
            output_path=output,
            start_date="1990-01-01",
            end_date="2026-08-14",
            request_timeout_seconds=0.01,
        )

    checkpoint = json.loads(output.read_text(encoding="utf-8"))
    assert checkpoint["complete"] is False
    assert checkpoint["requests"] == []


def test_factor_cli_budget_uses_five_second_batch_interval(tmp_path: Path) -> None:
    budget = factor_request_budget(tmp_path / "control")

    assert budget.min_batch_interval_seconds == 5.0


def test_successful_empty_factor_response_is_not_retried(tmp_path: Path) -> None:
    client = _EmptyClient()
    output = tmp_path / "factors.json"

    report = acquire_factors(
        client=client,
        universe=[{"instrument_id": "XSHG.600000", "provider_symbol": "sh.600000"}],
        output_path=output,
        start_date="2026-08-27",
        end_date="2026-08-27",
        sleep=lambda _seconds: None,
    )

    assert report == {"request_count": 1, "complete": True}
    assert client.symbols == ["sh.600000"]
    request = json.loads(output.read_text(encoding="utf-8"))["requests"][0]
    assert request["status"] == "ok_no_adjustments"
    assert request["empty_confirmations"] == 1


def test_factor_plan_uses_each_instruments_last_checked_range(tmp_path: Path) -> None:
    client = _Client()

    acquire_factors(
        client=client,
        universe=[
            {
                "instrument_id": "XSHG.600000",
                "provider_symbol": "sh.600000",
                "start_date": "2026-08-23",
                "end_date": "2026-08-27",
            }
        ],
        output_path=tmp_path / "factors.json",
        start_date="2026-08-27",
        end_date="2026-08-27",
        sleep=lambda _seconds: None,
    )

    assert client.queries == [{"start_date": "2026-08-23", "end_date": "2026-08-27"}]
