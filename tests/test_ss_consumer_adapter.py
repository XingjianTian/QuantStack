from __future__ import annotations

import importlib.util
import json
import math
import os
import sys
import threading
import time
from dataclasses import replace
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

MODULE_PATH = (
    Path(__file__).parents[1] / "docker" / "stock-screener" / "quantstack_consumer_adapter.py"
)
SPEC = importlib.util.spec_from_file_location("quantstack_consumer_adapter_test", MODULE_PATH)
assert SPEC and SPEC.loader
adapter_module = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = adapter_module
SPEC.loader.exec_module(adapter_module)


@pytest.mark.parametrize(
    ("version", "expected"),
    [
        ("daily-through-20260831.qfq-session-v1", date(2026, 8, 31)),
        (
            "2016-2026.full-market-v2.total-return-ss-daily-v1.20260828.recovery-8",
            date(2026, 8, 28),
        ),
        ("latest", None),
    ],
)
def test_dataset_release_date_accepts_immutable_release_suffixes(
    version: str, expected: date | None
) -> None:
    assert adapter_module.dataset_release_date(version) == expected


def _row(instrument_id: str, day: str = "2026-07-30") -> dict[str, object]:
    return {
        "instrument_id": instrument_id,
        "ts_event": f"{day}T07:00:00Z",
        "open": 10.0,
        "high": 11.0,
        "low": 9.0,
        "close": 10.5,
        "volume": 1234,
        "volume_unit": "shares",
        "adjustment_semantics": "none",
        "amount": 12957.0,
    }


def test_interactive_environment_transport_defaults_remain_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SS_QUANTSTACK_BASE_URL", "http://quant-stack-api:8900")
    monkeypatch.setenv("SS_QUANTSTACK_API_KEY", "test-only")
    monkeypatch.delenv("SS_QUANTSTACK_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("SS_QUANTSTACK_MAX_ATTEMPTS", raising=False)

    client = adapter_module.HTTPQuantStackMarketDataClient.from_environment()

    assert client.timeout_seconds == 5.0
    assert client.max_attempts == 2


def test_batch_adapter_accepts_an_explicit_background_transport_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SS_QUANTSTACK_BASE_URL", "http://quant-stack-api:8900")
    monkeypatch.setenv("SS_QUANTSTACK_API_KEY", "test-only")
    monkeypatch.setenv("SS_QUANTSTACK_BARS_DATASET_ID", "cn-bars")

    adapter = adapter_module.SSQuantStackConsumerAdapter.from_environment(
        timeout_seconds=30.0,
        max_attempts=1,
    )

    assert adapter.client.timeout_seconds == 30.0
    assert adapter.client.max_attempts == 1


class FakeClient:
    def __init__(self) -> None:
        self.snapshot_token = "snapshot-a"
        self.bars_calls: list[dict[str, object]] = []
        self.metadata_calls = 0

    def get_dataset_metadata(self, dataset_id: str, version: str = "latest"):
        self.metadata_calls += 1
        return {
            "dataset_id": dataset_id,
            "dataset_version": "2026.07.30",
            "manifest_fingerprint": "manifest-a",
            "snapshot_token": self.snapshot_token,
            "quality_status": "passed",
            "etag": '"metadata-a"',
            "publication_gate": "eligible",
            "publication_state": {
                "quality_passed": True,
                "catalog_published": True,
                "comparison_complete": True,
                "shadow_eligible": True,
                "candidate_eligible": True,
                "cutover_eligible": False,
            },
            "comparison_status": "passed",
            "comparison_required": True,
            "comparison_complete": True,
            "canonical_source_provider": "akshare",
            "canonical_source_endpoint": "stock_zh_a_hist",
            "selection_rule_id": "cn-route-v1",
            "publication_evidence_fingerprint": "a" * 64,
        }

    def fetch_instruments(self, payload):
        raise AssertionError("not used")

    def fetch_bars(self, payload):
        self.bars_calls.append(payload)
        return {
            "dataset_id": "cn-bars",
            "dataset_version": "2026.07.30",
            "manifest_fingerprint": "manifest-a",
            "snapshot_token": self.snapshot_token,
            "quality_status": "passed",
            "etag": '"bars-page-a"',
            "items": [_row(item) for item in payload["instrument_ids"]],
            "next_cursor": None,
        }


@pytest.mark.parametrize(
    ("ss_symbol", "instrument_id"),
    [
        ("600000.SS", "XSHG.600000"),
        ("000001.SZ", "XSHE.000001"),
        ("430047.BJ", "BJSE.430047"),
    ],
)
def test_explicit_cn_symbol_mapping_round_trips(ss_symbol, instrument_id):
    assert adapter_module.ss_symbol_to_quantstack_instrument(ss_symbol) == instrument_id
    assert adapter_module.quantstack_instrument_to_ss_symbol(instrument_id) == ss_symbol


@pytest.mark.parametrize("symbol", ["600000", "600000.SH", "US.AAPL", "XSHG.60000"])
def test_symbol_mapping_fails_closed_without_supported_explicit_exchange(symbol):
    with pytest.raises(adapter_module.MappingError):
        if symbol.startswith(("XSHG", "US")):
            adapter_module.quantstack_instrument_to_ss_symbol(symbol)
        else:
            adapter_module.ss_symbol_to_quantstack_instrument(symbol)


def test_batch_adapter_maps_schema_timezone_units_and_uses_one_http_batch():
    client = FakeClient()
    adapter = adapter_module.SSQuantStackConsumerAdapter(client, bars_dataset_id="cn-bars")
    result = adapter.get_batch_price_history(
        ["600000.SS", "000001.SZ"],
        start=date(2026, 7, 1),
        end=date(2026, 7, 30),
    )

    assert len(client.bars_calls) == 1
    assert client.bars_calls[0]["instrument_ids"] == [
        "XSHG.600000",
        "XSHE.000001",
    ]
    frame = result["600000.SS"].frame
    assert list(frame.columns) == ["Open", "High", "Low", "Close", "Volume", "Adj Close"]
    assert str(frame.index.tz) == "Asia/Shanghai"
    assert frame.iloc[0]["Volume"] == 1234
    assert frame.iloc[0]["Adj Close"] == frame.iloc[0]["Close"]
    assert frame.attrs["adj_close_is_compatibility_alias"] is True
    metadata = result["600000.SS"].metadata
    assert metadata.amount_by_date["2026-07-30"] == 12957.0
    assert metadata.manifest_fingerprint == "manifest-a"
    assert metadata.snapshot_token == "snapshot-a"


def test_resolved_market_version_expands_latest_alias_from_metadata():
    client = FakeClient()
    adapter = adapter_module.SSQuantStackConsumerAdapter(
        client,
        bars_dataset_id="cn-bars",
        dataset_version="latest",
    )

    assert adapter.dataset_version_for_market("CN") == "latest"
    assert adapter.resolved_dataset_version_for_market("CN") == "2026.07.30"
    assert client.metadata_calls == 1


def test_us_batch_adapter_resolves_plain_symbols_and_uses_us_candidate() -> None:
    class USClient(FakeClient):
        def fetch_instruments(self, payload):
            assert payload["market"] == "US"
            assert payload["dataset_id"] == "instruments_us"
            assert payload["dataset_version"] == "2026.08.27.ss-v1"
            return {
                "items": [
                    {"symbol": symbol, "instrument_id": f"XNAS.{symbol}"}
                    for symbol in payload["symbols"]
                ],
                "next_cursor": None,
            }

        def fetch_bars(self, payload):
            self.bars_calls.append(payload)
            rows = []
            for instrument_id in payload["instrument_ids"]:
                row = _row(instrument_id)
                row["ts_event"] = "2026-07-30T20:00:00Z"
                row["adjustment_semantics"] = "total_return"
                rows.append(row)
            return {
                "dataset_id": "us_equity_total_return",
                "dataset_version": "2026.08.27.ss-v1",
                "manifest_fingerprint": "manifest-a",
                "snapshot_token": self.snapshot_token,
                "quality_status": "passed",
                "etag": '"bars-us"',
                "items": rows,
                "next_cursor": None,
            }

        def get_dataset_metadata(self, dataset_id, version="latest"):
            payload = super().get_dataset_metadata(dataset_id, version)
            payload["dataset_version"] = "2026.08.27.ss-v1"
            return payload

    client = USClient()
    adapter = adapter_module.SSQuantStackConsumerAdapter(
        client,
        bars_dataset_id="cn-bars",
        consumer_access="candidate",
        us_bars_dataset_id="us_equity_total_return",
        us_instruments_dataset_id="instruments_us",
        us_dataset_version="2026.08.27.ss-v1",
    )

    result = adapter.get_batch_price_history(
        ["AAPL", "NVDA"],
        start=date(2026, 7, 1),
        end=date(2026, 7, 30),
    )
    assert set(result) == {"AAPL", "NVDA"}
    assert str(result["AAPL"].frame.index.tz) == "America/New_York"
    assert result["AAPL"].metadata.adjustment == "total_return"
    assert client.bars_calls[0]["dataset_id"] == "us_equity_total_return"
    assert client.bars_calls[0]["dataset_version"] == "2026.08.27.ss-v1"


def test_us_batch_adapter_uses_local_universe_mapping_before_instrument_api() -> None:
    class LocalUniverseClient(FakeClient):
        def fetch_instruments(self, payload):
            raise AssertionError("published local universe should satisfy the mapping")

        def fetch_bars(self, payload):
            self.bars_calls.append(payload)
            rows = []
            for instrument_id in payload["instrument_ids"]:
                row = _row(instrument_id)
                row["ts_event"] = "2026-07-30T20:00:00Z"
                row["adjustment_semantics"] = "total_return"
                rows.append(row)
            return {
                "dataset_id": "us_equity_total_return",
                "dataset_version": "2026.08.27.ss-v1",
                "manifest_fingerprint": "manifest-a",
                "snapshot_token": self.snapshot_token,
                "quality_status": "passed",
                "etag": '"bars-us"',
                "items": rows,
                "next_cursor": None,
            }

        def get_dataset_metadata(self, dataset_id, version="latest"):
            payload = super().get_dataset_metadata(dataset_id, version)
            payload["dataset_version"] = "2026.08.27.ss-v1"
            return payload

    client = LocalUniverseClient()
    adapter = adapter_module.SSQuantStackConsumerAdapter(
        client,
        bars_dataset_id="cn-bars",
        consumer_access="candidate",
        us_bars_dataset_id="us_equity_total_return",
        us_dataset_version="2026.08.27.ss-v1",
        us_instrument_resolver=lambda symbols: {
            symbol: f"XNAS.{symbol}" for symbol in symbols
        },
    )

    result = adapter.get_batch_price_history(
        ["INTC", "NVDA"],
        start=date(2026, 7, 1),
        end=date(2026, 7, 30),
    )

    assert set(result) == {"INTC", "NVDA"}
    assert client.bars_calls[0]["instrument_ids"] == ["XNAS.INTC", "XNAS.NVDA"]
    assert client.metadata_calls == 0


def test_adapter_captures_request_local_stage_timings() -> None:
    client = FakeClient()
    adapter = adapter_module.SSQuantStackConsumerAdapter(
        client,
        bars_dataset_id="cn-bars",
        dataset_version="2026.07.30",
        consumer_access="candidate",
    )

    with adapter_module.capture_read_stage_timings() as timings:
        adapter.get_price_history(
            "600000.SS",
            start=date(2026, 7, 1),
            end=date(2026, 7, 30),
        )

    assert {
        "instrument_resolution",
        "metadata",
        "adapter_cache_lookup",
        "bars_http_and_json",
        "dataframe_conversion",
        "adapter_total",
    }.issubset(timings)
    assert timings["metadata"] < timings["adapter_total"]
    assert all(value >= 0 for value in timings.values())


def test_us_batch_adapter_falls_back_only_for_missing_local_mapping() -> None:
    class PartialUniverseClient(FakeClient):
        def __init__(self):
            super().__init__()
            self.instrument_calls = []

        def fetch_instruments(self, payload):
            self.instrument_calls.append(payload)
            return {
                "items": [{"symbol": "NVDA", "instrument_id": "XNAS.NVDA"}],
                "next_cursor": None,
            }

        def fetch_bars(self, payload):
            self.bars_calls.append(payload)
            rows = []
            for instrument_id in payload["instrument_ids"]:
                row = _row(instrument_id)
                row["ts_event"] = "2026-07-30T20:00:00Z"
                row["adjustment_semantics"] = "total_return"
                rows.append(row)
            return {
                "dataset_id": "us_equity_total_return",
                "dataset_version": "2026.08.27.ss-v1",
                "manifest_fingerprint": "manifest-a",
                "snapshot_token": self.snapshot_token,
                "quality_status": "passed",
                "etag": '"bars-us"',
                "items": rows,
                "next_cursor": None,
            }

        def get_dataset_metadata(self, dataset_id, version="latest"):
            payload = super().get_dataset_metadata(dataset_id, version)
            payload["dataset_version"] = "2026.08.27.ss-v1"
            return payload

    client = PartialUniverseClient()
    adapter = adapter_module.SSQuantStackConsumerAdapter(
        client,
        bars_dataset_id="cn-bars",
        consumer_access="candidate",
        us_bars_dataset_id="us_equity_total_return",
        us_instruments_dataset_id="instruments_us",
        us_dataset_version="2026.08.27.ss-v1",
        us_instrument_resolver=lambda _symbols: {"INTC": "XNAS.INTC"},
    )

    adapter.get_batch_price_history(
        ["INTC", "NVDA"],
        start=date(2026, 7, 1),
        end=date(2026, 7, 30),
    )

    assert client.instrument_calls[0]["symbols"] == ["NVDA"]
    assert client.bars_calls[0]["instrument_ids"] == ["XNAS.INTC", "XNAS.NVDA"]


def test_us_last_trade_dates_marks_one_session_omission_without_history_read() -> None:
    class CoverageClient(FakeClient):
        def fetch_instruments(self, payload):
            return {
                "items": [
                    {"symbol": symbol, "instrument_id": f"XNAS.{symbol}"}
                    for symbol in payload["symbols"]
                ],
                "next_cursor": None,
            }

        def fetch_bars(self, payload):
            self.bars_calls.append(payload)
            return {
                "dataset_id": "us_equity_total_return",
                "dataset_version": "2026.08.28.ss-v1",
                "manifest_fingerprint": "manifest-a",
                "snapshot_token": self.snapshot_token,
                "quality_status": "passed",
                "etag": '"bars-us"',
                "items": [
                    {
                        **_row("XNAS.AAPL", "2026-08-28"),
                        "ts_event": "2026-08-28T20:00:00Z",
                        "adjustment_semantics": "total_return",
                    }
                ],
                "next_cursor": None,
            }

        def get_dataset_metadata(self, dataset_id, version="latest"):
            payload = super().get_dataset_metadata(dataset_id, version)
            payload["dataset_version"] = "2026.08.28.ss-v1"
            return payload

    client = CoverageClient()
    adapter = adapter_module.SSQuantStackConsumerAdapter(
        client,
        bars_dataset_id="cn-bars",
        consumer_access="candidate",
        us_bars_dataset_id="us_equity_total_return",
        us_instruments_dataset_id="instruments_us",
        us_dataset_version="2026.08.28.ss-v1",
    )
    expected = date(2026, 8, 28)
    assert adapter.get_last_trade_dates(
        ["AAPL", "NVDA"], start=expected, end=expected
    ) == {"AAPL": expected, "NVDA": None}
    assert client.bars_calls[0]["start"] == "2026-08-28"
    assert client.bars_calls[0]["end"] == "2026-08-28"


def test_adapter_reuses_validated_metadata_within_short_ttl():
    client = FakeClient()
    adapter = adapter_module.SSQuantStackConsumerAdapter(
        client,
        bars_dataset_id="cn-bars",
        metadata_ttl_seconds=30,
    )

    adapter.get_price_history(
        "600000.SS", start=date(2026, 7, 1), end=date(2026, 7, 30)
    )
    adapter.get_price_history(
        "000001.SZ", start=date(2026, 7, 1), end=date(2026, 7, 30)
    )

    assert client.metadata_calls == 1


def test_zero_metadata_ttl_observes_the_next_snapshot_immediately():
    client = FakeClient()
    adapter = adapter_module.SSQuantStackConsumerAdapter(
        client,
        bars_dataset_id="cn-bars",
        metadata_ttl_seconds=0,
    )

    adapter.get_price_history(
        "600000.SS", start=date(2026, 7, 1), end=date(2026, 7, 30)
    )
    adapter.get_price_history(
        "000001.SZ", start=date(2026, 7, 1), end=date(2026, 7, 30)
    )

    assert client.metadata_calls == 2

def test_scan_freshness_watermark_comes_from_exact_version_batch_rows():
    class SparseClient(FakeClient):
        def fetch_bars(self, payload):
            self.bars_calls.append(payload)
            return {
                "dataset_id": "cn-bars",
                "dataset_version": "daily-through-20260824.qfq-release-v1",
                "manifest_fingerprint": "manifest-a",
                "snapshot_token": self.snapshot_token,
                "quality_status": "passed",
                "etag": '"bars-page-a"',
                "items": [_row(payload["instrument_ids"][0], "2026-08-24")],
                "next_cursor": None,
            }

    client = SparseClient()
    adapter = adapter_module.SSQuantStackConsumerAdapter(
        client,
        bars_dataset_id="cn-bars",
        dataset_version="daily-through-20260824.qfq-release-v1",
        consumer_access="candidate",
    )

    dates = adapter.get_last_trade_dates(
        ["600519.SS", "000938.SZ"],
        start=date(2026, 7, 10),
        end=date(2026, 8, 24),
    )

    assert dates == {"600519.SS": date(2026, 8, 24), "000938.SZ": None}
    assert client.metadata_calls == 0
    assert client.bars_calls[0]["dataset_version"] == (
        "daily-through-20260824.qfq-release-v1"
    )


def test_adapter_lru_cache_key_uses_snapshot_token_and_is_bounded():
    client = FakeClient()
    adapter = adapter_module.SSQuantStackConsumerAdapter(
        client,
        bars_dataset_id="cn-bars",
        max_cache_entries=1,
        metadata_ttl_seconds=0,
    )
    kwargs = {"start": date(2026, 7, 1), "end": date(2026, 7, 30)}
    adapter.get_price_history("600000.SS", **kwargs)
    adapter.get_price_history("600000.SS", **kwargs)
    assert len(client.bars_calls) == 1

    client.snapshot_token = "snapshot-b"
    adapter.get_price_history("600000.SS", **kwargs)
    assert len(client.bars_calls) == 2
    assert len(adapter._cache) == 1


def test_adapter_allows_incomplete_only_for_shadow_access():
    class IncompleteClient(FakeClient):
        def get_dataset_metadata(self, dataset_id: str, version: str = "latest"):
            metadata = super().get_dataset_metadata(dataset_id, version)
            metadata["publication_gate"] = "incomplete"
            metadata["comparison_status"] = "unavailable"
            metadata["comparison_complete"] = False
            metadata["publication_state"] = {
                **metadata["publication_state"],
                "comparison_complete": False,
                "candidate_eligible": False,
            }
            return metadata

    client = IncompleteClient()
    shadow = adapter_module.SSQuantStackConsumerAdapter(
        client, bars_dataset_id="cn-bars", consumer_access="shadow"
    )
    assert shadow._metadata()["publication_gate"] == "incomplete"

    candidate = adapter_module.SSQuantStackConsumerAdapter(
        client, bars_dataset_id="cn-bars", consumer_access="candidate"
    )
    with pytest.raises(adapter_module.AdapterError, match="candidate eligible"):
        candidate._metadata()


def test_shadow_adapter_consumes_published_batch_metadata_when_candidate_is_disabled():
    class ShadowPublishedClient(FakeClient):
        def get_dataset_metadata(self, dataset_id: str, version: str = "latest"):
            metadata = super().get_dataset_metadata(dataset_id, version)
            metadata.update(
                {
                    "publication_gate": "eligible",
                    "comparison_status": "unavailable",
                    "comparison_complete": False,
                    "consumer_eligibility": {
                        "shadow_allowed": True,
                        "candidate_allowed": False,
                        "cutover_allowed": False,
                    },
                    "shadow_eligible": True,
                    "candidate_eligible": False,
                    "cutover_eligible": False,
                }
            )
            metadata["publication_state"] = {
                **metadata["publication_state"],
                "comparison_complete": False,
                "shadow_eligible": True,
                "candidate_eligible": False,
                "cutover_eligible": False,
            }
            return metadata

    client = ShadowPublishedClient()
    adapter = adapter_module.SSQuantStackConsumerAdapter(
        client,
        bars_dataset_id="cn-bars",
        dataset_version="2015-2026.full-market-v1.ss-shadow-v1",
        consumer_access="shadow",
    )

    result = adapter.get_price_history(
        "600000.SS",
        start=date(2024, 8, 11),
        end=date(2026, 8, 11),
    )

    assert len(result.frame) == 1
    assert result.metadata.snapshot_token == "snapshot-a"
    assert client.bars_calls[0]["dataset_version"] == "2026.07.30"


def test_adapter_rejects_inconsistent_or_unbound_publication_state():
    class InvalidClient(FakeClient):
        def get_dataset_metadata(self, dataset_id: str, version: str = "latest"):
            metadata = super().get_dataset_metadata(dataset_id, version)
            metadata["comparison_complete"] = False
            return metadata

    with pytest.raises(adapter_module.AdapterError, match="inconsistent"):
        adapter_module.SSQuantStackConsumerAdapter(
            InvalidClient(), bars_dataset_id="cn-bars"
        )._metadata()


def test_adapter_accepts_page_specific_etags_but_requires_stable_snapshot():
    class PaginatedClient(FakeClient):
        def fetch_bars(self, payload):
            self.bars_calls.append(payload)
            page_two = "cursor" in payload
            return {
                "dataset_id": "cn-bars",
                "dataset_version": "2026.07.30",
                "manifest_fingerprint": "manifest-a",
                "snapshot_token": self.snapshot_token,
                "quality_status": "passed",
                "etag": '"bars-page-b"' if page_two else '"bars-page-a"',
                "items": [_row(payload["instrument_ids"][int(page_two)])],
                "next_cursor": None if page_two else "next-page",
            }

    client = PaginatedClient()
    adapter = adapter_module.SSQuantStackConsumerAdapter(client, bars_dataset_id="cn-bars")
    result = adapter.get_batch_price_history(
        ["600000.SS", "000001.SZ"],
        start=date(2026, 7, 1),
        end=date(2026, 7, 30),
    )
    assert len(client.bars_calls) == 2
    assert all(len(item.frame) == 1 for item in result.values())

    class ChangedSnapshotClient(PaginatedClient):
        def fetch_bars(self, payload):
            response = super().fetch_bars(payload)
            if "cursor" in payload:
                response["snapshot_token"] = "snapshot-changed"
            return response

    with pytest.raises(adapter_module.AdapterError, match="changed during pagination"):
        adapter_module.SSQuantStackConsumerAdapter(
            ChangedSnapshotClient(), bars_dataset_id="cn-bars"
        ).get_batch_price_history(
            ["600000.SS", "000001.SZ"],
            start=date(2026, 7, 1),
            end=date(2026, 7, 30),
        )


def test_adapter_rejects_non_share_volume():
    row = _row("XSHG.600000")
    row["volume_unit"] = "unknown_or_mixed"
    with pytest.raises(adapter_module.IncompatibleUnitError):
        adapter_module.SSQuantStackConsumerAdapter._frame([row])


@pytest.mark.parametrize("adjustment", [None, "unknown"])
def test_adapter_rejects_missing_or_unsupported_adjustment_semantics(adjustment):
    row = _row("XSHG.600000")
    if adjustment is None:
        row.pop("adjustment_semantics")
    else:
        row["adjustment_semantics"] = adjustment
    with pytest.raises(adapter_module.AdapterError, match="homogeneous none or qfq"):
        adapter_module.SSQuantStackConsumerAdapter._frame([row])


def test_adapter_rejects_mixed_adjustment_semantics():
    raw = _row("XSHG.600000")
    adjusted = _row("XSHE.000001")
    adjusted["adjustment_semantics"] = "qfq"
    with pytest.raises(adapter_module.AdapterError, match="homogeneous none or qfq"):
        adapter_module.SSQuantStackConsumerAdapter._frame([raw, adjusted])


def test_adapter_preserves_qfq_adjustment_semantics():
    row = _row("XSHG.600000")
    row["adjustment_semantics"] = "qfq"
    frame, _amounts = adapter_module.SSQuantStackConsumerAdapter._frame([row])

    assert frame.attrs["adjustment"] == "qfq"


def test_http_client_disables_environment_proxy_for_internal_service():
    client = adapter_module.HTTPQuantStackMarketDataClient(
        "http://quant-stack-api:8900", "service-key"
    )
    proxy_handlers = [
        handler
        for handler in client._opener.handlers
        if isinstance(handler, adapter_module.urllib.request.ProxyHandler)
    ]
    assert proxy_handlers == []


def test_legacy_mode_starts_no_worker():
    legacy = pd.DataFrame({"Close": [1.0]})

    class PriceCache:
        def get_historical_data(self, symbol, period="2y", force_refresh=False, market=None):
            return legacy

        def get_many_cached_only(self, symbols, period="2y"):
            return {symbol: legacy for symbol in symbols}

    def forbidden_factory():
        raise AssertionError("default legacy path must not construct candidate adapter")

    adapter_module.install_price_cache_read_modes(PriceCache, adapter_factory=forbidden_factory)
    service = PriceCache()
    assert service.get_historical_data("600000.SS") is legacy
    assert service.get_many_cached_only(["600000.SS"])["600000.SS"] is legacy
    assert PriceCache._quantstack_shadow_worker is None


def test_shadow_wraps_every_price_cache_history_read_path_and_preserves_results(tmp_path):
    legacy = pd.DataFrame({"Close": [1.0]})
    original_calls: list[tuple[str, tuple, dict]] = []
    candidate_calls: list[tuple[list[str], dict]] = []

    class PriceCache:
        def _single(self, method_name, symbol, period, *args, **kwargs):
            original_calls.append((method_name, (symbol, period, *args), kwargs))
            return legacy

        def _bulk(self, method_name, symbols, period, *args, **kwargs):
            original_calls.append((method_name, (tuple(symbols), period, *args), kwargs))
            return {symbol: legacy for symbol in symbols}

        def get_historical_data(self, symbol, period="2y", *args, **kwargs):
            return self._single("get_historical_data", symbol, period, *args, **kwargs)

        def get_cached_only(self, symbol, period="2y", *args, **kwargs):
            return self._single("get_cached_only", symbol, period, *args, **kwargs)

        def get_cached_only_fresh(self, symbol, period="2y", *args, **kwargs):
            return self._single("get_cached_only_fresh", symbol, period, *args, **kwargs)

        def get_many(self, symbols, period="2y", *args, **kwargs):
            return self._bulk("get_many", symbols, period, *args, **kwargs)

        def get_many_cached_only(self, symbols, period="2y", *args, **kwargs):
            return self._bulk("get_many_cached_only", symbols, period, *args, **kwargs)

        def get_many_cached_only_fresh(self, symbols, period="2y", *args, **kwargs):
            return self._bulk("get_many_cached_only_fresh", symbols, period, *args, **kwargs)

    class Adapter:
        def get_batch_price_history(self, symbols, **kwargs):
            candidate_calls.append((list(symbols), kwargs))
            return {}

    config = adapter_module.SSReadModeConfig(
        mode="shadow",
        shadow_enabled=True,
        sample_rate=1.0,
        max_window_days=3650,
        report_root=str(tmp_path / "reports"),
    )
    adapter_module.install_price_cache_read_modes(
        PriceCache, adapter_factory=Adapter, config=config
    )
    service = PriceCache()

    assert service.get_historical_data("600000.SS", "max", True, market="CN") is legacy
    assert service.get_cached_only("600000.SS", "max") is legacy
    assert service.get_cached_only_fresh("600000.SS", "max") is legacy
    assert service.get_many(
        ["600000.SS"],
        "max",
        market="CN",
        market_by_symbol={"600000.SS": "CN"},
    )["600000.SS"] is legacy
    assert service.get_many_cached_only(["600000.SS"], "max")["600000.SS"] is legacy
    assert service.get_many_cached_only_fresh(["600000.SS"], "max")["600000.SS"] is legacy

    expected_paths = {
        "get_historical_data",
        "get_cached_only",
        "get_cached_only_fresh",
        "get_many",
        "get_many_cached_only",
        "get_many_cached_only_fresh",
    }
    assert set(PriceCache._quantstack_wrapped_read_methods) == expected_paths
    assert {name for name, _, _ in original_calls} == expected_paths
    get_many_call = next(call for call in original_calls if call[0] == "get_many")
    assert get_many_call[2] == {
        "market": "CN",
        "market_by_symbol": {"600000.SS": "CN"},
    }

    worker = PriceCache._quantstack_shadow_worker
    assert worker.wait_for_idle()
    assert len(candidate_calls) == 6
    assert all((call[1]["end"] - call[1]["start"]).days == 3650 for call in candidate_calls)
    reports = [json.loads(path.read_text()) for path in (tmp_path / "reports").rglob("*.json")]
    assert {report["read_path"] for report in reports} == expected_paths
    assert all(report["cutover_compatible"] is False for report in reports)
    assert worker.shutdown()


def test_shadow_does_not_route_unsupported_non_cn_symbol_to_cn_dataset(tmp_path):
    legacy = pd.DataFrame({"Close": [1.0]})

    class PriceCache:
        def get_historical_data(self, *args, **kwargs):
            return legacy

    class Adapter:
        def get_batch_price_history(self, *args, **kwargs):
            raise AssertionError("non-CN symbol must not reach the CN dataset")

    config = adapter_module.SSReadModeConfig(
        mode="shadow",
        shadow_enabled=True,
        sample_rate=1.0,
        report_root=str(tmp_path / "reports"),
    )
    adapter_module.install_price_cache_read_modes(
        PriceCache, adapter_factory=Adapter, config=config
    )
    assert PriceCache().get_historical_data("AAPL") is legacy
    worker = PriceCache._quantstack_shadow_worker
    assert worker.wait_for_idle()
    assert worker.health_snapshot().submitted == 0
    assert not list((tmp_path / "reports").rglob("*.json"))
    assert worker.shutdown()


def test_direct_yfinance_cn_daily_read_preserves_call_and_records_shadow(tmp_path):
    legacy = pd.DataFrame({"Close": [1.0]})
    original_calls: list[tuple[str, str, str, bool]] = []
    candidate_calls: list[tuple[list[str], dict]] = []

    class PriceCache:
        def get_historical_data(self, symbol, period="2y"):
            raise AssertionError("use_cache=False direct read must not enter PriceCache")

    class YFinance:
        def get_historical_data(
            self,
            symbol,
            period="1y",
            interval="1d",
            use_cache=True,
        ):
            original_calls.append((symbol, period, interval, use_cache))
            return legacy

    class Adapter:
        def get_batch_price_history(self, symbols, **kwargs):
            candidate_calls.append((list(symbols), kwargs))
            return {}

    config = adapter_module.SSReadModeConfig(
        mode="shadow",
        shadow_enabled=True,
        sample_rate=1.0,
        max_window_days=3650,
        report_root=str(tmp_path / "reports"),
    )
    adapter_module.install_price_cache_read_modes(
        PriceCache,
        adapter_factory=Adapter,
        config=config,
    )
    adapter_module.install_yfinance_read_modes(YFinance, PriceCache, config=config)

    result = YFinance().get_historical_data("600000.SS", "6mo", "1d", False)
    assert result is legacy
    assert original_calls == [("600000.SS", "6mo", "1d", False)]

    worker = PriceCache._quantstack_shadow_worker
    assert worker.wait_for_idle()
    assert len(candidate_calls) == 1
    assert candidate_calls[0][0] == ["600000.SS"]
    assert (candidate_calls[0][1]["end"] - candidate_calls[0][1]["start"]).days == 183
    reports = [json.loads(path.read_text()) for path in (tmp_path / "reports").rglob("*.json")]
    assert [report["read_path"] for report in reports] == ["yfinance_direct"]
    assert worker.shutdown()


def test_direct_yfinance_cache_delegate_has_one_price_cache_observation(tmp_path):
    legacy = pd.DataFrame({"Close": [1.0]})
    candidate_calls: list[list[str]] = []
    original_yfinance_calls: list[bool] = []

    class YFinance:
        def get_historical_data(
            self,
            symbol,
            period="1y",
            interval="1d",
            use_cache=True,
        ):
            original_yfinance_calls.append(use_cache)
            if use_cache:
                return PriceCache().get_historical_data(symbol, period)
            return legacy

    class PriceCache:
        def get_historical_data(self, symbol, period="2y"):
            return YFinance().get_historical_data(
                symbol,
                period=period,
                use_cache=False,
            )

    class Adapter:
        def get_batch_price_history(self, symbols, **kwargs):
            candidate_calls.append(list(symbols))
            return {}

    config = adapter_module.SSReadModeConfig(
        mode="shadow",
        shadow_enabled=True,
        sample_rate=1.0,
        report_root=str(tmp_path / "reports"),
    )
    adapter_module.install_price_cache_read_modes(
        PriceCache,
        adapter_factory=Adapter,
        config=config,
    )
    adapter_module.install_yfinance_read_modes(YFinance, PriceCache, config=config)
    adapter_module.install_yfinance_read_modes(YFinance, PriceCache, config=config)

    assert YFinance().get_historical_data("600000.SS", period="2y") is legacy
    worker = PriceCache._quantstack_shadow_worker
    assert worker.wait_for_idle()
    assert original_yfinance_calls == [True, False]
    assert candidate_calls == [["600000.SS"]]
    reports = [json.loads(path.read_text()) for path in (tmp_path / "reports").rglob("*.json")]
    assert [report["read_path"] for report in reports] == ["get_historical_data"]
    assert worker.shutdown()


def test_direct_yfinance_cache_miss_fallback_has_one_direct_observation(tmp_path):
    legacy = pd.DataFrame({"Close": [1.0]})
    original_yfinance_calls: list[bool] = []

    class PriceCache:
        def get_historical_data(self, symbol, period="2y"):
            return None

    class YFinance:
        def get_historical_data(
            self,
            symbol,
            period="1y",
            interval="1d",
            use_cache=True,
        ):
            original_yfinance_calls.append(use_cache)
            if use_cache:
                cached = PriceCache().get_historical_data(symbol, period)
                if cached is not None:
                    return cached
                return self.get_historical_data(
                    symbol,
                    period=period,
                    interval=interval,
                    use_cache=False,
                )
            return legacy

    class Adapter:
        def get_batch_price_history(self, symbols, **kwargs):
            return {}

    config = adapter_module.SSReadModeConfig(
        mode="shadow",
        shadow_enabled=True,
        sample_rate=1.0,
        report_root=str(tmp_path / "reports"),
    )
    adapter_module.install_price_cache_read_modes(
        PriceCache,
        adapter_factory=Adapter,
        config=config,
    )
    adapter_module.install_yfinance_read_modes(YFinance, PriceCache, config=config)

    assert YFinance().get_historical_data("600000.SS", period="2y") is legacy
    worker = PriceCache._quantstack_shadow_worker
    assert worker.wait_for_idle()
    assert original_yfinance_calls == [True, False]
    reports = [json.loads(path.read_text()) for path in (tmp_path / "reports").rglob("*.json")]
    assert [report["read_path"] for report in reports].count("yfinance_direct") == 1
    assert worker.shutdown()


@pytest.mark.parametrize(
    ("submit_results", "expected_accepted"),
    [
        ([False, True], 1),
        ([False, False], 0),
    ],
)
def test_direct_yfinance_rejected_nested_submit_has_one_bounded_outer_retry(
    monkeypatch,
    submit_results,
    expected_accepted,
):
    legacy = pd.DataFrame({"Close": [1.0]})

    class PriceCache:
        def get_historical_data(self, symbol, period="2y"):
            return None

    class YFinance:
        def get_historical_data(
            self,
            symbol,
            period="1y",
            interval="1d",
            use_cache=True,
        ):
            if use_cache:
                return self.get_historical_data(
                    symbol,
                    period=period,
                    interval=interval,
                    use_cache=False,
                )
            return legacy

    class Adapter:
        def get_batch_price_history(self, symbols, **kwargs):
            return {}

    config = adapter_module.SSReadModeConfig(
        mode="shadow",
        shadow_enabled=True,
        sample_rate=1.0,
    )
    adapter_module.install_price_cache_read_modes(
        PriceCache,
        adapter_factory=Adapter,
        config=config,
    )
    adapter_module.install_yfinance_read_modes(YFinance, PriceCache, config=config)
    worker = PriceCache._quantstack_shadow_worker
    submitted: list[bool] = []
    outcomes = iter(submit_results)

    def controlled_submit(**kwargs):
        accepted = next(outcomes)
        submitted.append(accepted)
        return accepted

    monkeypatch.setattr(worker, "submit_observation", controlled_submit)

    assert YFinance().get_historical_data("600000.SS", period="2y") is legacy
    assert submitted == submit_results
    assert sum(submitted) == expected_accepted
    assert worker.shutdown()


def test_direct_yfinance_non_cn_and_intraday_reads_fail_closed(tmp_path):
    legacy = pd.DataFrame({"Close": [1.0]})

    class PriceCache:
        def get_historical_data(self, symbol, period="2y"):
            return legacy

    class YFinance:
        def get_historical_data(
            self,
            symbol,
            period="1y",
            interval="1d",
            use_cache=True,
        ):
            return legacy

    class Adapter:
        def get_batch_price_history(self, *args, **kwargs):
            raise AssertionError("unsupported direct read must not reach CN dataset")

    config = adapter_module.SSReadModeConfig(
        mode="shadow",
        shadow_enabled=True,
        sample_rate=1.0,
        report_root=str(tmp_path / "reports"),
    )
    adapter_module.install_price_cache_read_modes(
        PriceCache,
        adapter_factory=Adapter,
        config=config,
    )
    adapter_module.install_yfinance_read_modes(YFinance, PriceCache, config=config)

    service = YFinance()
    assert service.get_historical_data("AAPL", use_cache=False) is legacy
    assert service.get_historical_data("SPY", use_cache=False) is legacy
    assert service.get_historical_data(
        "600000.SS",
        interval="1h",
        use_cache=False,
    ) is legacy
    worker = PriceCache._quantstack_shadow_worker
    assert worker.wait_for_idle()
    assert worker.health_snapshot().submitted == 0
    assert not list((tmp_path / "reports").rglob("*.json"))
    assert worker.shutdown()


def test_direct_yfinance_shadow_candidate_never_blocks_legacy_return():
    legacy = pd.DataFrame({"Close": [1.0]})
    candidate_started = threading.Event()
    release_candidate = threading.Event()

    class PriceCache:
        def get_historical_data(self, symbol, period="2y"):
            return legacy

    class YFinance:
        def get_historical_data(
            self,
            symbol,
            period="1y",
            interval="1d",
            use_cache=True,
        ):
            return legacy

    class Adapter:
        def get_batch_price_history(self, *args, **kwargs):
            candidate_started.set()
            assert release_candidate.wait(timeout=2)
            return {}

    config = adapter_module.SSReadModeConfig(
        mode="shadow",
        shadow_enabled=True,
        sample_rate=1.0,
    )
    adapter_module.install_price_cache_read_modes(
        PriceCache,
        adapter_factory=Adapter,
        config=config,
    )
    adapter_module.install_yfinance_read_modes(YFinance, PriceCache, config=config)

    started = time.perf_counter()
    assert YFinance().get_historical_data("600000.SS", use_cache=False) is legacy
    elapsed = time.perf_counter() - started
    assert elapsed < 0.2
    assert candidate_started.wait(timeout=1)
    release_candidate.set()
    worker = PriceCache._quantstack_shadow_worker
    assert worker.wait_for_idle()
    assert worker.shutdown()


def test_direct_yfinance_wrapper_preserves_original_exception():
    class PriceCache:
        def get_historical_data(self, symbol, period="2y"):
            return None

    class YFinance:
        def get_historical_data(self, symbol, period="1y", *args, **kwargs):
            raise RuntimeError("legacy failure")

    config = adapter_module.SSReadModeConfig(
        mode="shadow",
        shadow_enabled=True,
        sample_rate=1.0,
    )
    adapter_module.install_price_cache_read_modes(PriceCache, config=config)
    adapter_module.install_yfinance_read_modes(YFinance, PriceCache, config=config)
    with pytest.raises(RuntimeError, match="legacy failure"):
        YFinance().get_historical_data("600000.SS", use_cache=False)
    worker = PriceCache._quantstack_shadow_worker
    assert worker.health_snapshot().submitted == 0
    assert worker.shutdown()


def test_direct_yfinance_context_resets_after_original_exception(tmp_path):
    legacy = pd.DataFrame({"Close": [1.0]})
    calls = 0

    class PriceCache:
        def get_historical_data(self, symbol, period="2y"):
            return None

    class YFinance:
        def get_historical_data(self, symbol, period="1y", *args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("legacy failure")
            return legacy

    class Adapter:
        def get_batch_price_history(self, symbols, **kwargs):
            return {}

    config = adapter_module.SSReadModeConfig(
        mode="shadow",
        shadow_enabled=True,
        sample_rate=1.0,
        report_root=str(tmp_path / "reports"),
    )
    adapter_module.install_price_cache_read_modes(
        PriceCache,
        adapter_factory=Adapter,
        config=config,
    )
    adapter_module.install_yfinance_read_modes(YFinance, PriceCache, config=config)

    service = YFinance()
    with pytest.raises(RuntimeError, match="legacy failure"):
        service.get_historical_data("600000.SS", use_cache=False)
    assert service.get_historical_data("600000.SS", use_cache=False) is legacy
    worker = PriceCache._quantstack_shadow_worker
    assert worker.wait_for_idle()
    reports = [json.loads(path.read_text()) for path in (tmp_path / "reports").rglob("*.json")]
    assert [report["read_path"] for report in reports] == ["yfinance_direct"]
    assert worker.shutdown()


def test_single_shadow_none_legacy_still_returns_none():
    class PriceCache:
        def get_historical_data(self, *args, **kwargs):
            return None

    class Adapter:
        def get_batch_price_history(self, *args, **kwargs):
            return {}

    config = adapter_module.SSReadModeConfig(mode="shadow", shadow_enabled=True, sample_rate=1.0)
    adapter_module.install_price_cache_read_modes(
        PriceCache, adapter_factory=Adapter, config=config
    )
    assert PriceCache().get_historical_data("600000.SS") is None
    worker = PriceCache._quantstack_shadow_worker
    assert worker.wait_for_idle()
    assert worker.shutdown()


def test_bulk_shadow_missing_symbol_does_not_change_legacy_result():
    legacy = pd.DataFrame({"Close": [1.0]})
    candidate_calls: list[list[str]] = []

    class PriceCache:
        def get_historical_data(self, *args, **kwargs):
            raise AssertionError("not used")

        def get_many_cached_only(self, symbols, period="2y"):
            return {symbols[0]: legacy}

    class Adapter:
        def get_batch_price_history(self, symbols, **kwargs):
            candidate_calls.append(list(symbols))
            return {}

    config = adapter_module.SSReadModeConfig(mode="shadow", shadow_enabled=True, sample_rate=1.0)
    adapter_module.install_price_cache_read_modes(
        PriceCache, adapter_factory=Adapter, config=config
    )
    result = PriceCache().get_many_cached_only(["600000.SS", "000001.SZ"])
    assert result == {"600000.SS": legacy}
    worker = PriceCache._quantstack_shadow_worker
    assert worker.wait_for_idle()
    assert candidate_calls == [["600000.SS", "000001.SZ"]]
    assert worker.shutdown()


def test_bulk_shadow_none_frame_does_not_raise():
    class PriceCache:
        def get_historical_data(self, *args, **kwargs):
            raise AssertionError("not used")

        def get_many_cached_only(self, symbols, period="2y"):
            return {symbols[0]: None}

    class Adapter:
        def get_batch_price_history(self, *args, **kwargs):
            return {}

    config = adapter_module.SSReadModeConfig(mode="shadow", shadow_enabled=True, sample_rate=1.0)
    adapter_module.install_price_cache_read_modes(
        PriceCache, adapter_factory=Adapter, config=config
    )
    result = PriceCache().get_many_cached_only(["600000.SS"])
    assert result == {"600000.SS": None}
    worker = PriceCache._quantstack_shadow_worker
    assert worker.wait_for_idle()
    assert worker.shutdown()


def test_shadow_snapshot_applies_declared_legacy_semantics(monkeypatch):
    frame = pd.DataFrame({"Close": [10.0]})
    monkeypatch.setenv("SS_LEGACY_ADJUSTMENT_SEMANTICS", "qfq")
    monkeypatch.setenv("SS_LEGACY_VOLUME_UNIT", "shares")

    snapshot = adapter_module._safe_frame_snapshot(frame)

    assert snapshot is not frame
    assert snapshot.attrs == {"adjustment": "qfq", "volume_unit": "shares"}
    assert frame.attrs == {}


def test_shadow_returns_before_candidate_and_writes_immutable_sanitized_report(tmp_path):
    index = pd.DatetimeIndex(["2026-07-30"], tz="Asia/Shanghai", name="Date")
    legacy = pd.DataFrame(
        {"Open": [1.0], "High": [1.0], "Low": [1.0], "Close": [1.0]},
        index=index,
    )
    entered = threading.Event()
    release = threading.Event()

    class PriceCache:
        def get_historical_data(self, symbol, period="2y", force_refresh=False, market=None):
            return legacy

    class BrokenAdapter:
        def get_batch_price_history(self, *args, **kwargs):
            entered.set()
            release.wait(timeout=2)
            raise TimeoutError("secret-token-must-not-be-reported")

    config = adapter_module.SSReadModeConfig(
        mode="shadow",
        shadow_enabled=True,
        sample_rate=1.0,
        report_root=str(tmp_path / "reports"),
    )
    adapter_module.install_price_cache_read_modes(
        PriceCache, adapter_factory=BrokenAdapter, config=config
    )
    started = time.perf_counter()
    result = PriceCache().get_historical_data("600000.SS")
    elapsed = time.perf_counter() - started
    assert result is legacy
    assert elapsed < 0.2
    assert entered.wait(timeout=1)
    assert not list((tmp_path / "reports").rglob("*.json"))

    release.set()
    worker = PriceCache._quantstack_shadow_worker
    assert worker.wait_for_idle()
    reports = list((tmp_path / "reports").rglob("*.json"))
    assert len(reports) == 1
    report_text = reports[0].read_text(encoding="utf-8")
    assert "secret-token" not in report_text
    report = json.loads(report_text)
    assert report["status"] == "candidate_error"
    assert report["ss_symbol"] == "600000.SS"
    assert report["read_path"] == "get_historical_data"
    assert report["cutover_compatible"] is False
    assert os.stat(reports[0]).st_mode & 0o777 == 0o600
    assert os.stat(reports[0].parent).st_mode & 0o777 == 0o700
    assert worker.shutdown()


def test_bulk_shadow_uses_one_background_batch_singleton_and_exact_legacy_objects():
    legacy_a = pd.DataFrame({"Close": [1.0]})
    legacy_b = pd.DataFrame({"Close": [2.0]})
    candidate_calls: list[list[str]] = []
    factory_calls = 0

    class PriceCache:
        def get_historical_data(self, *args, **kwargs):
            raise AssertionError("not used")

        def get_many_cached_only(self, symbols, period="2y"):
            return {symbols[0]: legacy_a, symbols[1]: legacy_b}

    class Adapter:
        def get_batch_price_history(self, symbols, **kwargs):
            candidate_calls.append(list(symbols))
            return {}

    def factory():
        nonlocal factory_calls
        factory_calls += 1
        return Adapter()

    config = adapter_module.SSReadModeConfig(mode="shadow", shadow_enabled=True, sample_rate=1.0)
    adapter_module.install_price_cache_read_modes(
        PriceCache, adapter_factory=factory, config=config
    )
    service = PriceCache()
    result = service.get_many_cached_only(["600000.SS", "000001.SZ"])
    assert result["600000.SS"] is legacy_a
    assert result["000001.SZ"] is legacy_b
    worker = PriceCache._quantstack_shadow_worker
    assert worker.wait_for_idle()
    service.get_many_cached_only(["600000.SS", "000001.SZ"])
    assert worker.wait_for_idle()
    assert candidate_calls == [
        ["600000.SS", "000001.SZ"],
        ["600000.SS", "000001.SZ"],
    ]
    assert factory_calls == 1
    assert worker._thread.daemon is True
    assert worker._thread.name == "quantstack-shadow-worker-0"
    assert worker.shutdown()


def test_shadow_queue_full_fails_open_without_candidate_on_request_thread():
    entered = threading.Event()
    release = threading.Event()
    legacy = pd.DataFrame({"Close": [1.0]})

    class PriceCache:
        def get_historical_data(self, *args, **kwargs):
            return legacy

    class Adapter:
        def get_batch_price_history(self, *args, **kwargs):
            entered.set()
            release.wait(timeout=2)
            return {}

    config = adapter_module.SSReadModeConfig(
        mode="shadow",
        shadow_enabled=True,
        sample_rate=1.0,
        shadow_queue_size=1,
    )
    adapter_module.install_price_cache_read_modes(
        PriceCache, adapter_factory=Adapter, config=config
    )
    service = PriceCache()
    assert service.get_historical_data("600000.SS") is legacy
    assert entered.wait(timeout=1)
    assert service.get_historical_data("600001.SS") is legacy
    started = time.perf_counter()
    assert service.get_historical_data("600002.SS") is legacy
    assert time.perf_counter() - started < 0.2
    release.set()
    worker = PriceCache._quantstack_shadow_worker
    assert worker.wait_for_idle()
    assert worker.shutdown()


def test_candidate_mode_is_disabled_in_production_before_adapter_construction():
    class PriceCache:
        def get_historical_data(self, *args, **kwargs):
            raise AssertionError("legacy not used")

    config = adapter_module.SSReadModeConfig(
        mode="quantstack_candidate",
        runtime_environment="production",
        allow_candidate_mode=True,
    )
    adapter_module.install_price_cache_read_modes(
        PriceCache, adapter_factory=lambda: None, config=config
    )
    with pytest.raises(adapter_module.CandidateModeDisabledError):
        PriceCache().get_historical_data("600000.SS")


def _candidate(frame: pd.DataFrame) -> object:
    return adapter_module.SSAdapterResult(
        frame=frame,
        metadata=adapter_module.SSAdapterMetadata(
            dataset_id="cn-bars",
            dataset_version="2026.07.30",
            manifest_fingerprint="manifest-a",
            snapshot_token="snapshot-a",
            etag='"bars-a"',
            instrument_id="XSHG.600000",
            frequency="1d",
            amount_by_date={"2026-07-30": 12957.0},
        ),
    )


def test_primary_mode_price_cache_returns_quantstack_frame():
    candidate = pd.DataFrame({"Close": [10.0]})
    factory_calls = 0

    class PriceCache:
        def get_historical_data(self, *args, **kwargs):
            raise AssertionError("legacy not used")

    class Adapter:
        def get_price_history(self, *args, **kwargs):
            return _candidate(candidate)

    def factory():
        nonlocal factory_calls
        factory_calls += 1
        return Adapter()

    config = adapter_module.SSReadModeConfig(
        mode="quantstack_primary",
        runtime_environment="production",
        primary_enabled=True,
    )
    adapter_module.install_price_cache_read_modes(
        PriceCache, adapter_factory=factory, config=config
    )

    assert PriceCache().get_historical_data("600000.SS") is candidate
    assert factory_calls == 1


def test_primary_mode_disabled_fails_before_adapter_construction():
    class PriceCache:
        def get_historical_data(self, *args, **kwargs):
            raise AssertionError("legacy not used")

    config = adapter_module.SSReadModeConfig(
        mode="quantstack_primary",
        runtime_environment="production",
        primary_enabled=False,
    )
    adapter_module.install_price_cache_read_modes(
        PriceCache,
        adapter_factory=lambda: (_ for _ in ()).throw(
            AssertionError("adapter must not be constructed")
        ),
        config=config,
    )

    with pytest.raises(adapter_module.CandidateModeDisabledError):
        PriceCache().get_historical_data("600000.SS")


def test_direct_yfinance_primary_routes_cn_and_us_daily_and_preserves_intraday_reads():
    legacy = pd.DataFrame({"Close": [1.0]})
    candidate = pd.DataFrame({"Close": [10.0]})
    original_calls: list[tuple[str, str]] = []
    candidate_calls: list[tuple[str, dict]] = []

    class PriceCache:
        def get_historical_data(self, *args, **kwargs):
            return legacy

    class YFinance:
        def get_historical_data(
            self, symbol, period="1y", interval="1d", use_cache=True
        ):
            original_calls.append((symbol, interval))
            return legacy

    class Adapter:
        def get_price_history(self, symbol, **kwargs):
            candidate_calls.append((symbol, kwargs))
            return _candidate(candidate)

    config = adapter_module.SSReadModeConfig(
        mode="quantstack_primary",
        runtime_environment="production",
        primary_enabled=True,
        max_window_days=3650,
    )
    adapter_module.install_price_cache_read_modes(
        PriceCache, adapter_factory=Adapter, config=config
    )
    adapter_module.install_yfinance_read_modes(YFinance, PriceCache, config=config)

    service = YFinance()
    assert service.get_historical_data("600000.SS", period="10y") is candidate
    assert service.get_historical_data("AAPL", period="1y") is candidate
    assert service.get_historical_data("600000.SS", interval="1h") is legacy
    assert [call[0] for call in candidate_calls] == ["600000.SS", "AAPL"]
    assert (candidate_calls[0][1]["end"] - candidate_calls[0][1]["start"]).days == 3650
    assert (candidate_calls[1][1]["end"] - candidate_calls[1][1]["start"]).days == 365
    assert original_calls == [("600000.SS", "1h")]


def test_dynamic_trading_day_period_adds_calendar_margin():
    end = date(2026, 8, 29)

    assert (end - adapter_module._period_start("230d", end=end)).days == 368


def test_direct_yfinance_primary_disabled_fails_before_adapter_construction():
    class PriceCache:
        def get_historical_data(self, *args, **kwargs):
            raise AssertionError("legacy not used")

    class YFinance:
        def get_historical_data(self, *args, **kwargs):
            raise AssertionError("legacy not used")

    config = adapter_module.SSReadModeConfig(
        mode="quantstack_primary",
        runtime_environment="production",
        primary_enabled=False,
    )
    adapter_module.install_price_cache_read_modes(
        PriceCache,
        adapter_factory=lambda: (_ for _ in ()).throw(
            AssertionError("adapter must not be constructed")
        ),
        config=config,
    )
    adapter_module.install_yfinance_read_modes(YFinance, PriceCache, config=config)

    with pytest.raises(adapter_module.CandidateModeDisabledError):
        YFinance().get_historical_data("600000.SS")


def _shadow_task(*symbols: str) -> object:
    return adapter_module._ShadowTask(
        legacy_results={symbol: pd.DataFrame() for symbol in symbols},
        symbols=tuple(symbols),
        start=date(2026, 7, 1),
        end=date(2026, 7, 30),
        frequency="1d",
        period="2y",
        created_at="2026-08-03T00:00:00Z",
    )


def _shadow_worker(adapter_factory, tmp_path=None, **kwargs):
    return adapter_module.SSShadowWorker(
        adapter_factory=adapter_factory,
        report_root=None if tmp_path is None else str(tmp_path / "reports"),
        queue_size=kwargs.pop("queue_size", 8),
        shutdown_timeout_seconds=1.0,
        **kwargs,
    )


def test_comparison_failure_does_not_kill_worker(tmp_path, monkeypatch):
    frame = pd.DataFrame()

    class Adapter:
        def get_batch_price_history(self, symbols, **kwargs):
            return {symbol: _candidate(frame) for symbol in symbols}

    original = adapter_module.compare_shadow_frames

    def fail_one(*, symbol, **kwargs):
        if symbol == "600000.SS":
            raise ValueError("private comparison detail")
        return original(symbol=symbol, **kwargs)

    monkeypatch.setattr(adapter_module, "compare_shadow_frames", fail_one)
    worker = _shadow_worker(Adapter, tmp_path)
    assert worker.submit(_shadow_task("600000.SS", "000001.SZ"))
    assert worker.wait_for_idle()
    health = worker.health_snapshot()
    assert health.thread_alive is True
    assert health.comparison_failures == 1
    assert health.last_failure_class == "ValueError"
    assert len(list((tmp_path / "reports").rglob("*.json"))) == 1
    assert worker.shutdown()


def test_report_validation_failure_does_not_kill_worker(tmp_path, monkeypatch):
    frame = pd.DataFrame()

    class Adapter:
        def get_batch_price_history(self, symbols, **kwargs):
            return {symbol: _candidate(frame) for symbol in symbols}

    original = adapter_module.write_immutable_shadow_report
    calls = 0

    def fail_first(root, report):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ValueError("private report detail")
        return original(root, report)

    monkeypatch.setattr(adapter_module, "write_immutable_shadow_report", fail_first)
    worker = _shadow_worker(Adapter, tmp_path)
    assert worker.submit(_shadow_task("600000.SS"))
    assert worker.submit(_shadow_task("000001.SZ"))
    assert worker.wait_for_idle()
    health = worker.health_snapshot()
    assert health.thread_alive is True
    assert health.report_failures == 1
    assert health.completed == 2
    assert len(list((tmp_path / "reports").rglob("*.json"))) == 1
    assert worker.shutdown()


def test_worker_processes_task_after_previous_failure(monkeypatch):
    class Adapter:
        def get_batch_price_history(self, *args, **kwargs):
            return {}

    worker = _shadow_worker(Adapter)
    processed: list[tuple[str, ...]] = []

    def fail_first(task):
        processed.append(task.symbols)
        if len(processed) == 1:
            raise RuntimeError("private worker detail")

    monkeypatch.setattr(worker, "_process", fail_first)
    assert worker.submit(_shadow_task("600000.SS"))
    assert worker.submit(_shadow_task("000001.SZ"))
    assert worker.wait_for_idle()
    assert processed == [("600000.SS",), ("000001.SZ",)]
    health = worker.health_snapshot()
    assert health.worker_failures == 1
    assert health.completed == 2
    assert health.thread_alive is True
    assert worker.shutdown()


def test_submit_to_dead_worker_fails_open():
    worker = _shadow_worker(lambda: None)
    assert worker.shutdown()
    assert worker.submit(_shadow_task("600000.SS")) is False
    health = worker.health_snapshot()
    assert health.thread_alive is False
    assert health.dropped == 1
    assert health.submitted == 0


def test_shadow_task_symbol_and_row_limits():
    worker = _shadow_worker(
        lambda: None,
        max_symbols_per_task=1,
        max_legacy_rows_per_task=1,
    )
    assert (
        worker.submit_observation(
            legacy_results={
                "600000.SS": pd.DataFrame({"Close": [1.0]}),
                "000001.SZ": pd.DataFrame({"Close": [1.0]}),
            },
            symbols=("600000.SS", "000001.SZ"),
            start=date(2026, 7, 1),
            end=date(2026, 7, 30),
            frequency="1d",
            period="2y",
            created_at="2026-08-03T00:00:00Z",
        )
        is False
    )
    assert (
        worker.submit_observation(
            legacy_results={"600000.SS": pd.DataFrame({"Close": [1.0, 2.0]})},
            symbols=("600000.SS",),
            start=date(2026, 7, 1),
            end=date(2026, 7, 30),
            frequency="1d",
            period="2y",
            created_at="2026-08-03T00:00:00Z",
        )
        is False
    )
    health = worker.health_snapshot()
    assert health.dropped == 2
    assert health.submitted == 0
    assert health.queue_size == 0
    assert worker.shutdown()


def test_shadow_health_counters():
    class BrokenAdapter:
        def get_batch_price_history(self, *args, **kwargs):
            raise TimeoutError("private candidate detail")

    worker = _shadow_worker(BrokenAdapter)
    assert worker.submit(_shadow_task("600000.SS"))
    assert worker.wait_for_idle()
    health = worker.health_snapshot()
    assert health.thread_alive is True
    assert health.queue_capacity == 8
    assert health.queue_size == 0
    assert health.submitted == 1
    assert health.completed == 1
    assert health.dropped == 0
    assert health.candidate_failures == 1
    assert health.comparison_failures == 0
    assert health.report_failures == 0
    assert health.last_failure_class == "TimeoutError"
    assert health.last_candidate_failure_stage == "unknown"
    assert health.last_candidate_failure_class == "TimeoutError"
    assert worker.shutdown()


def test_shadow_health_records_metadata_fetch_failure_identity():
    class MetadataTimeoutClient(FakeClient):
        def get_dataset_metadata(self, dataset_id: str, version: str = "latest"):
            raise TimeoutError("private metadata detail")

    adapter = adapter_module.SSQuantStackConsumerAdapter(
        MetadataTimeoutClient(), bars_dataset_id="cn-bars"
    )
    worker = _shadow_worker(lambda: adapter)
    assert worker.submit(_shadow_task("600000.SS"))
    assert worker.wait_for_idle()
    health = worker.health_snapshot()
    assert health.candidate_failures == 1
    assert health.last_candidate_failure_stage == "metadata_fetch"
    assert health.last_candidate_failure_class == "TimeoutError"
    assert worker.shutdown()


def test_later_comparison_failure_preserves_candidate_failure_identity(monkeypatch):
    class BrokenAdapter:
        candidate_failure_stage = "bars_fetch"

        def get_batch_price_history(self, *args, **kwargs):
            raise TimeoutError("private candidate detail")

    def fail_comparison(**kwargs):
        raise ValueError("private comparison detail")

    monkeypatch.setattr(adapter_module, "compare_shadow_frames", fail_comparison)
    worker = _shadow_worker(BrokenAdapter)
    assert worker.submit(_shadow_task("600000.SS"))
    assert worker.wait_for_idle()
    health = worker.health_snapshot()
    assert health.candidate_failures == 1
    assert health.comparison_failures == 1
    assert health.last_failure_class == "ValueError"
    assert health.last_candidate_failure_stage == "bars_fetch"
    assert health.last_candidate_failure_class == "TimeoutError"
    assert worker.shutdown()


def _comparison(**kwargs):
    return adapter_module.compare_shadow_frames(
        start=date(2026, 7, 1),
        end=date(2026, 7, 30),
        frequency="1d",
        period="2y",
        **kwargs,
    )


def test_comparison_splits_shape_consumer_and_cutover_compatibility():
    index = pd.DatetimeIndex(["2026-07-30"], tz="Asia/Shanghai", name="Date")
    frame = pd.DataFrame(
        {
            "Open": [10.0],
            "High": [11.0],
            "Low": [9.0],
            "Close": [10.5],
            "Volume": [1234],
            "Adj Close": [10.5],
        },
        index=index,
    )
    frame.attrs["volume_unit"] = "shares"
    report = _comparison(
        symbol="600000.SS", legacy=frame.copy(), candidate=_candidate(frame), latency_ms=1.0
    )
    assert report.status == adapter_module.ShadowComparisonStatus.BLOCKED_BY_ADJUSTMENT
    assert report.ohlc_shape_compatible is True
    assert report.consumer_contract_compatible is True
    assert report.cutover_compatible is False
    assert report.amount_coverage == 1
    assert report.dataset_id == "cn-bars"
    assert report.manifest_fingerprint == "manifest-a"
    assert report.snapshot_token == "snapshot-a"


def test_comparison_marks_explicit_none_to_none_adjustment_compatible():
    index = pd.DatetimeIndex(["2026-07-30"], tz="Asia/Shanghai", name="Date")
    frame = pd.DataFrame(
        {
            "Open": [10.0],
            "High": [11.0],
            "Low": [9.0],
            "Close": [10.5],
            "Volume": [1234],
            "Adj Close": [10.5],
        },
        index=index,
    )
    frame.attrs.update({"volume_unit": "shares", "adjustment": "none"})
    report = _comparison(
        symbol="600000.SS", legacy=frame.copy(), candidate=_candidate(frame), latency_ms=1.0
    )

    assert report.status == adapter_module.ShadowComparisonStatus.PASSED
    assert report.adjustment_compatible is True
    assert report.cutover_compatible is False


def test_comparison_marks_qfq_to_qfq_adjustment_compatible():
    index = pd.DatetimeIndex(["2026-07-30"], tz="Asia/Shanghai", name="Date")
    frame = pd.DataFrame(
        {
            "Open": [10.0],
            "High": [11.0],
            "Low": [9.0],
            "Close": [10.5],
            "Volume": [1234],
            "Adj Close": [10.5],
        },
        index=index,
    )
    frame.attrs.update({"volume_unit": "shares", "adjustment": "qfq"})
    candidate = _candidate(frame)
    candidate = replace(candidate, metadata=replace(candidate.metadata, adjustment="qfq"))
    report = _comparison(
        symbol="600000.SS", legacy=frame.copy(), candidate=candidate, latency_ms=1.0
    )

    assert report.status == adapter_module.ShadowComparisonStatus.PASSED
    assert report.adjustment_compatible is True
    assert report.adjustment_semantics == "qfq"


@pytest.mark.parametrize("legacy_adjustment", ["qfq", "unknown"])
def test_comparison_blocks_none_against_adjusted_or_unknown(legacy_adjustment):
    index = pd.DatetimeIndex(["2026-07-30"], tz="Asia/Shanghai", name="Date")
    frame = pd.DataFrame(
        {
            "Open": [10.0],
            "High": [11.0],
            "Low": [9.0],
            "Close": [10.5],
            "Volume": [1234],
            "Adj Close": [10.5],
        },
        index=index,
    )
    frame.attrs.update({"volume_unit": "shares", "adjustment": legacy_adjustment})
    report = _comparison(
        symbol="600000.SS", legacy=frame.copy(), candidate=_candidate(frame), latency_ms=1.0
    )

    assert report.status == adapter_module.ShadowComparisonStatus.BLOCKED_BY_ADJUSTMENT
    assert report.adjustment_compatible is False
    assert report.cutover_compatible is False


def test_comparison_diagnoses_adjustment_ratio_regime_and_return_drift():
    index = pd.DatetimeIndex(
        ["2026-07-27", "2026-07-28", "2026-07-29", "2026-07-30"],
        tz="Asia/Shanghai",
        name="Date",
    )
    candidate_close = pd.Series([100.0, 102.0, 51.0, 52.0], index=index)
    legacy_close = pd.Series([50.0, 51.0, 51.0, 52.0], index=index)

    def frame(close: pd.Series) -> pd.DataFrame:
        result = pd.DataFrame(
            {
                "Open": close * 0.99,
                "High": close * 1.01,
                "Low": close * 0.98,
                "Close": close,
                "Volume": [1000, 1100, 1200, 1300],
                "Adj Close": close,
            },
            index=index,
        )
        result.attrs["volume_unit"] = "shares"
        return result

    legacy = frame(legacy_close)
    legacy.attrs["adjustment"] = "qfq"
    report = _comparison(
        symbol="600000.SS",
        legacy=legacy,
        candidate=_candidate(frame(candidate_close)),
        latency_ms=1.0,
    )

    assert report.status == adapter_module.ShadowComparisonStatus.BLOCKED_BY_ADJUSTMENT
    assert report.legacy_adjustment_semantics == "qfq"
    assert report.adjustment_semantics == "none"
    assert report.adjustment_compatible is False
    assert report.production_price_basis_required == "qfq"
    assert report.shared_dates == 4
    assert report.price_ratio_median == 0.75
    assert report.price_ratio_regime_changes == 1
    assert report.close_return_overlap == 3
    assert report.close_return_drift_max is not None
    assert report.close_return_drift_max > 0.4
    assert report.normalized_ohlc_mismatches == 0
    assert report.cutover_compatible is False


def test_comparison_sanitizes_zero_nan_and_infinity_diagnostics():
    index = pd.DatetimeIndex(
        ["2026-07-26", "2026-07-27", "2026-07-28", "2026-07-29", "2026-07-30"],
        tz="Asia/Shanghai",
        name="Date",
    )

    def frame(close, *, bad_open=False):
        values = pd.Series(close, index=index, dtype=float)
        result = pd.DataFrame(
            {
                "Open": values.copy(),
                "High": values * 1.01,
                "Low": values * 0.99,
                "Close": values,
                "Volume": [1000, 1100, 1200, 1300, 1400],
                "Adj Close": values,
            },
            index=index,
        )
        if bad_open:
            result.loc[index[1], "Open"] = float("inf")
        result.attrs.update({"volume_unit": "shares", "adjustment": "none"})
        return result

    legacy = frame([10.0, 11.0, 0.0, 12.0, float("inf")])
    candidate_frame = frame([10.0, 12.0, 6.0, float("nan"), 13.0], bad_open=True)
    report = _comparison(
        symbol="600000.SS",
        legacy=legacy,
        candidate=_candidate(candidate_frame),
        latency_ms=1.0,
    )

    assert report.adjustment_compatible is True
    assert report.close_return_overlap == 1
    assert report.close_return_drift_p95 is not None
    assert math.isfinite(report.close_return_drift_p95)
    assert report.close_return_drift_max is not None
    assert math.isfinite(report.close_return_drift_max)
    assert report.price_ratio_median is not None
    assert math.isfinite(report.price_ratio_median)
    assert report.normalized_ohlc_mismatches >= 10
    json.dumps(report.sanitized(), allow_nan=False)


def test_comparison_rejects_unknown_volume_for_consumer_contract():
    index = pd.DatetimeIndex(["2026-07-30"], name="Date")
    values = {
        "Open": [10.0],
        "High": [11.0],
        "Low": [9.0],
        "Close": [10.5],
        "Volume": [1234],
        "Adj Close": [10.5],
    }
    legacy = pd.DataFrame(values, index=index)
    candidate_frame = pd.DataFrame(
        values,
        index=pd.DatetimeIndex(["2026-07-30"], tz="Asia/Shanghai", name="Date"),
    )
    report = _comparison(
        symbol="600000.SS",
        legacy=legacy,
        candidate=_candidate(candidate_frame),
        latency_ms=1.0,
    )
    assert report.ohlc_shape_compatible is True
    assert report.consumer_contract_compatible is False
    assert report.cutover_compatible is False


def test_report_records_window_path_and_snapshot_directly():
    frame = pd.DataFrame()
    base = _comparison(
        symbol="600000.SS", legacy=frame, candidate=_candidate(frame), latency_ms=1.0
    )
    changed_snapshot = _candidate(frame)
    changed_snapshot = adapter_module.SSAdapterResult(
        frame=frame,
        metadata=replace(changed_snapshot.metadata, snapshot_token="snapshot-b"),
    )
    other = _comparison(
        symbol="600000.SS", legacy=frame, candidate=changed_snapshot, latency_ms=1.0
    )
    assert base.report_id != other.report_id
    assert base.snapshot_token == "snapshot-a"
    assert other.snapshot_token == "snapshot-b"

    other_path = _comparison(
        symbol="600000.SS",
        legacy=frame,
        candidate=_candidate(frame),
        latency_ms=1.0,
        read_path="get_many_cached_only",
    )
    assert base.read_path == "get_historical_data"
    assert other_path.read_path == "get_many_cached_only"


def test_immutable_report_refuses_conflict_and_never_overwrites(tmp_path):
    report = _comparison(
        symbol="600000.SS",
        legacy=pd.DataFrame(),
        candidate=None,
        latency_ms=1.0,
        error=TimeoutError("private"),
    )
    path = adapter_module.write_immutable_shadow_report(tmp_path, report)
    original = path.read_bytes()
    with pytest.raises(FileExistsError):
        adapter_module.write_immutable_shadow_report(tmp_path, report)
    assert path.read_bytes() == original
    assert not list(path.parent.glob("*.tmp"))
