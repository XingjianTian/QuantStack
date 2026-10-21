import gzip
import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

import scripts.run_us_equity_core_daily as daily
import scripts.run_us_equity_core_periodic_replay as periodic
from quant_stack.data.providers.yfinance import (
    ReplayYFinanceTransport,
    YFinanceEmptyResponseError,
)


def _inputs(tmp_path: Path):
    symbols = ["AAA", "BBB", "CCC"]
    snapshot = {
        "schema_version": "1.0.0",
        "snapshot_id": "us-core-daily-test.v1",
        "as_of": "2026-08-25T00:00:00Z",
        "source": "curated_research_snapshot",
        "update_policy": "new_version",
        "membership_basis": "current_sp500_constituents_not_point_in_time",
        "source_refs": ["https://example.test"],
        "instruments": [
            {
                "instrument_id": f"{'XNAS' if index < 2 else 'XNYS'}.{symbol}",
                "symbol": symbol,
                "canonical_venue": "XNAS" if index < 2 else "XNYS",
                "provider_symbol": symbol,
                "source_symbol": symbol,
                "name": symbol,
                "asset_class": "equity",
                "listed_from": None,
                "listed_to": None,
                "status": "listed",
                "currency": "USD",
                "timezone": "America/New_York",
            }
            for index, symbol in enumerate(symbols)
        ],
    }
    snapshot_path = tmp_path / "snapshot.json"
    snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
    records = []
    for index, symbol in enumerate(symbols):
        close = 100.0 + index
        records.append(
            {
                "ticker": symbol,
                "Date": "2026-08-25",
                "Open": close - 1,
                "High": close + 1,
                "Low": close - 2,
                "Close": close,
                "Adj Close": close - 0.5,
                "Volume": 1000 + index,
            }
        )
    fixture = tmp_path / "fixture.json"
    fixture.write_text(json.dumps({"provider": "yfinance", "records": records}), encoding="utf-8")
    key = tmp_path / "worker.key"
    key.write_bytes(b"k" * 32)
    collect_args = type(
        "CollectArgs",
        (),
        {
            "snapshot": snapshot_path,
            "session": date(2026, 8, 25),
            "staging_root": tmp_path / "producer",
            "task_state_root": tmp_path / "tasks",
            "archive_root": tmp_path / "archives",
            "transfer": tmp_path / "outbound" / "us-core-daily-2026-08-25.tar",
            "report": tmp_path / "collect.json",
            "hmac_key_file": key,
            "commit_sha": "a" * 40,
            "environment_id": "mac-test",
            "request_interval_seconds": 0.1,
            "yfinance_runtime_root": tmp_path / "runtime",
            "yfinance_proxy_url": None,
            "fixture": fixture,
            "now": datetime(2026, 8, 26, 16, tzinfo=UTC),
        },
    )()
    accept_args = type(
        "AcceptArgs",
        (),
        {
            "snapshot": snapshot_path,
            "state": tmp_path / "nas-state.json",
            "initial_watermark": date(2026, 8, 24),
            "transfer": collect_args.transfer,
            "scratch_parent": tmp_path / "scratch",
            "isolated_root": tmp_path / "nas-isolated",
            "hmac_key_file": key,
            "report": tmp_path / "accept.json",
            "verification_mode": "full_replay",
        },
    )()
    return symbols, collect_args, accept_args


def test_one_aggregate_transfer_is_accepted_before_watermark(tmp_path: Path) -> None:
    symbols, collect_args, accept_args = _inputs(tmp_path)
    sleeps: list[float] = []

    collected = daily.collect(collect_args, sleeper=sleeps.append)
    assert collected["actual_requests"] == 3
    assert collected["aggregate_transfers"] == 1
    assert collected["corporate_actions_in_raw"] is False
    assert collected["eligible_instruments"] == 3
    assert collected["quarantined_instruments"] == 0
    assert sleeps == [0.1, 0.1]
    assert [item["symbol"] for item in collected["entries"]] == symbols
    raw_rows = [
        json.loads(path.read_text())["records"][0]
        for path in collect_args.staging_root.glob("raw/US/**/*.json")
    ]
    assert raw_rows
    assert all("Dividends" not in row and "Stock Splits" not in row for row in raw_rows)
    assert len(list(collect_args.transfer.parent.glob("*.tar"))) == 1
    assert not accept_args.state.exists()

    accepted = daily.accept(accept_args)
    assert accepted["status"] == "passed"
    assert accepted["rows"] == 3
    assert accepted["instruments"] == 3
    assert accepted["new_imports"] == 3
    assert accepted["replay_imports"] == 3
    assert json.loads(accept_args.state.read_text())["watermark"] == "2026-08-25"

    repeated = daily.accept(accept_args)
    assert repeated["new_imports"] == 0
    assert repeated["resumed_imports"] == 3
    assert repeated["watermark_after"] == "2026-08-25"

    recovered = daily.collect(collect_args, sleeper=lambda _: None)
    assert recovered["actual_requests"] == 0
    assert recovered["deduplicated_tasks"] == 3


def test_live_collection_prefetches_fixed_batches_without_provider_retry(
    monkeypatch, tmp_path: Path
) -> None:
    symbols, collect_args, _ = _inputs(tmp_path)
    records = json.loads(collect_args.fixture.read_text())["records"]
    calls: list[tuple[list[str], int]] = []

    class BatchTransport:
        def __init__(self, **kwargs) -> None:
            pass

        def fetch_daily_bars_batch(
            self, requested, start, end, *, include_actions, threads
        ):
            calls.append((list(requested), threads))
            return [row for row in records if row["ticker"] in requested]

    monkeypatch.setattr(daily, "LiveYFinanceTransport", BatchTransport)
    collect_args.fixture = None
    collect_args.yfinance_proxy_url = "http://127.0.0.1:7890"
    collect_args.provider_threads = 2
    collect_args.provider_batch_size = 2

    collected = daily.collect(collect_args, sleeper=lambda _: None)

    assert calls == [(["AAA", "BBB"], 2), (["CCC"], 2)]
    assert collected["actual_requests"] == len(symbols)
    assert collected["parallel_requests"] is True
    assert collected["provider_threads"] == 2
    assert collected["provider_batch_size"] == 2
    assert collected["provider_batch_count"] == 2
    assert collected["provider_retries"] == 0
    assert collected["aggregate_transfers"] == 1
    assert len(list((tmp_path / "provider-batches").rglob("batch-*.json.gz"))) == 2


def test_action_only_batch_miss_is_recorded_after_all_symbols(monkeypatch, tmp_path: Path) -> None:
    symbols, collect_args, _ = _inputs(tmp_path)
    daily.collect(collect_args, sleeper=lambda _: None)
    records = json.loads(collect_args.fixture.read_text())["records"]

    calls: list[tuple[date, date, list[str]]] = []

    class PartialBatchTransport:
        def __init__(self, **kwargs) -> None:
            pass

        def fetch_daily_bars_batch(
            self, requested, start, end, *, include_actions, threads
        ):
            calls.append((start, end, list(requested)))
            return [
                {
                    **row,
                    "Date": start.isoformat(),
                    "Dividends": 0.0,
                    "Stock Splits": 0.0,
                }
                for row in records
                if row["ticker"] in requested and row["ticker"] != symbols[1]
            ]

    monkeypatch.setattr(daily, "LiveYFinanceTransport", PartialBatchTransport)
    collect_args.fixture = None
    collect_args.yfinance_proxy_url = "http://127.0.0.1:7890"
    collect_args.provider_threads = 2
    collect_args.provider_batch_size = 3
    collect_args.action_evidence_root = tmp_path / "action-evidence"
    collect_args.action_sessions = (date(2026, 8, 24),)

    report = daily.collect(collect_args, sleeper=lambda _: None)

    assert report["status"] == "collected"
    assert report["action_evidence_quarantines"][0]["symbol"] == symbols[1]
    assert report["action_evidence_quarantined_instruments"] == 1
    assert report["provider_retries"] == 0
    assert report["aggregate_transfers"] == 1
    assert calls[0][:2] == (date(2026, 8, 25), date(2026, 8, 25))
    assert calls[1][:2] == (date(2026, 8, 24), date(2026, 8, 24))
    assert all(
        (collect_args.action_evidence_root / "2026-08-24" / f"{symbol}.json.gz").is_file()
        for symbol in (symbols[0], symbols[2])
    )


def test_stale_session_row_does_not_require_action_evidence(
    monkeypatch, tmp_path: Path
) -> None:
    symbols, collect_args, _ = _inputs(tmp_path)
    payload = json.loads(collect_args.fixture.read_text())
    payload["records"][0]["Date"] = "2026-08-24"
    collect_args.fixture.write_text(json.dumps(payload), encoding="utf-8")

    first = daily.collect(collect_args, sleeper=lambda _: None)
    assert first["eligible_instruments"] == 2
    assert first["quarantined_instruments"] == 1

    class CachedBatchTransport:
        def __init__(self, **kwargs) -> None:
            pass

        def fetch_daily_bars_batch(
            self, requested, start, end, *, include_actions, threads
        ):
            return [
                {**row, "Dividends": 0.0, "Stock Splits": 0.0}
                for row in payload["records"]
                if row["ticker"] in requested
            ]

    monkeypatch.setattr(daily, "LiveYFinanceTransport", CachedBatchTransport)
    collect_args.fixture = None
    collect_args.yfinance_proxy_url = "http://127.0.0.1:7890"
    collect_args.provider_threads = 2
    collect_args.provider_batch_size = 3
    collect_args.action_evidence_root = tmp_path / "action-evidence"
    collect_args.action_sessions = (date(2026, 8, 25),)

    recovered = daily.collect(collect_args, sleeper=lambda _: None)

    assert recovered["eligible_instruments"] == 2
    assert recovered["quarantined_instruments"] == 1
    assert not (
        collect_args.action_evidence_root / "2026-08-25" / f"{symbols[0]}.json.gz"
    ).exists()
    assert all(
        (collect_args.action_evidence_root / "2026-08-25" / f"{symbol}.json.gz").is_file()
        for symbol in symbols[1:]
    )


def test_batch_cache_prevents_provider_retry_for_missing_symbols(
    monkeypatch, tmp_path: Path
) -> None:
    symbols, collect_args, _ = _inputs(tmp_path)
    records = json.loads(collect_args.fixture.read_text())["records"]
    cache = (
        tmp_path
        / "provider-batches/2026-08-25/core-daily-v2/batch-0001.json.gz"
    )
    cache.parent.mkdir(parents=True)
    cache.write_bytes(
        gzip.compress(
            json.dumps(
                {
                    "schema_version": "quantstack.yfinance-daily-batch-cache.v1",
                    "session_date": "2026-08-25",
                    "requested_symbols": symbols,
                    "records": [records[0]],
                }
            ).encode(),
            mtime=0,
        )
    )

    class NoProviderRetry:
        def __init__(self, **kwargs) -> None:
            pass

        def fetch_daily_bars_batch(self, *args, **kwargs):
            raise AssertionError("cached attempted symbols must not be requested again")

    monkeypatch.setattr(daily, "LiveYFinanceTransport", NoProviderRetry)
    collect_args.fixture = None
    collect_args.yfinance_proxy_url = "http://127.0.0.1:7890"
    collect_args.provider_threads = 2
    collect_args.provider_batch_size = 3

    collected = daily.collect(collect_args, sleeper=lambda _: None)

    assert collected["actual_requests"] == 0
    assert collected["provider_cached_symbols"] == 3
    assert set(collected["provider_missing_symbols"]) == set(symbols[1:])
    assert collected["eligible_instruments"] == 1
    assert collected["quarantined_instruments"] == 2


def test_explicit_recovery_uses_a_new_batch_cache_generation(
    monkeypatch, tmp_path: Path
) -> None:
    symbols, collect_args, _ = _inputs(tmp_path)
    records = json.loads(collect_args.fixture.read_text())["records"]
    original_cache = (
        tmp_path / "provider-batches/2026-08-25/core-daily-v2/batch-0001.json.gz"
    )
    original_cache.parent.mkdir(parents=True)
    original_cache.write_bytes(
        gzip.compress(
            json.dumps(
                {
                    "schema_version": "quantstack.yfinance-daily-batch-cache.v1",
                    "session_date": "2026-08-25",
                    "requested_symbols": symbols,
                    "records": [records[0]],
                }
            ).encode(),
            mtime=0,
        )
    )
    calls: list[list[str]] = []

    class RecoveryBatchTransport:
        def __init__(self, **kwargs) -> None:
            pass

        def fetch_daily_bars_batch(
            self, requested, start, end, *, include_actions, threads
        ):
            calls.append(list(requested))
            return [row for row in records if row["ticker"] in requested]

    monkeypatch.setattr(daily, "LiveYFinanceTransport", RecoveryBatchTransport)
    collect_args.fixture = None
    collect_args.yfinance_proxy_url = "http://127.0.0.1:7890"
    collect_args.provider_threads = 2
    collect_args.provider_batch_size = 3
    collect_args.task_recovery_generation = 1

    recovered = daily.collect(collect_args, sleeper=lambda _: None)

    assert calls == [symbols]
    assert recovered["eligible_instruments"] == 3
    assert recovered["provider_cached_symbols"] == 0
    assert len(
        list(
            (
                tmp_path
                / "provider-batches/2026-08-25/core-daily-v2/recovery-1"
            ).glob("batch-*.json.gz")
        )
    ) == 1


def test_action_backfill_reuses_accepted_price_tasks(tmp_path: Path) -> None:
    symbols, collect_args, _ = _inputs(tmp_path)
    first = daily.collect(collect_args, sleeper=lambda _: None)
    assert first["price_requests"] == len(symbols)

    collect_args.action_evidence_root = tmp_path / "action-evidence"
    collect_args.action_sessions = (date(2026, 8, 25),)
    second = daily.collect(collect_args, sleeper=lambda _: None)

    assert second["price_requests"] == 0
    assert second["reused_price_tasks"] == len(symbols)
    assert second["action_evidence_requests"] == len(symbols)
    assert second["actual_requests"] == len(symbols)
    assert all(
        (collect_args.action_evidence_root / "2026-08-25" / f"{symbol}.json.gz").is_file()
        for symbol in symbols
    )

    third = daily.collect(collect_args, sleeper=lambda _: None)
    assert third["actual_requests"] == 0
    assert third["price_requests"] == 0
    assert third["action_evidence_requests"] == 0


def test_collection_failure_does_not_create_transfer(tmp_path: Path) -> None:
    symbols, collect_args, _ = _inputs(tmp_path)

    class BrokenTransport(ReplayYFinanceTransport):
        def __init__(self, symbol: str) -> None:
            super().__init__(collect_args.fixture)
            self.symbol = symbol

        def fetch_daily_bars(self, requested, start, end):
            if self.symbol == symbols[1]:
                raise ConnectionError("injected failure")
            return super().fetch_daily_bars(requested, start, end)

    with pytest.raises(RuntimeError, match=symbols[1]):
        daily.collect(
            collect_args,
            transport_factory=BrokenTransport,
            sleeper=lambda _: None,
        )
    assert not collect_args.transfer.exists()


def test_daily_quality_quarantine_is_explicit_and_does_not_deadlock(tmp_path: Path) -> None:
    symbols, collect_args, accept_args = _inputs(tmp_path)
    payload = json.loads(collect_args.fixture.read_text())
    bad = next(item for item in payload["records"] if item["ticker"] == symbols[1])
    bad["Low"] = bad["High"] + 1
    collect_args.fixture.write_text(json.dumps(payload))

    collected = daily.collect(collect_args, sleeper=lambda _: None)
    assert collected["eligible_instruments"] == 2
    assert collected["quarantined_instruments"] == 1
    assert collected["quarantines"][0]["symbol"] == symbols[1]

    accepted = daily.accept(accept_args)
    assert accepted["rows"] == 2
    assert accepted["quarantines"][0]["symbol"] == symbols[1]
    assert accepted["watermark_after"] == "2026-08-25"

    repeated = daily.collect(collect_args, sleeper=lambda _: None)
    assert repeated["actual_requests"] == 0
    assert repeated["deduplicated_tasks"] == 2
    assert repeated["quarantines"][0]["symbol"] == symbols[1]


def test_daily_empty_session_row_is_explicit_and_watermark_can_advance(tmp_path: Path) -> None:
    symbols, collect_args, accept_args = _inputs(tmp_path)

    class EmptyRowTransport(ReplayYFinanceTransport):
        def __init__(self, symbol: str) -> None:
            super().__init__(collect_args.fixture)
            self.symbol = symbol

        def fetch_daily_bars(self, requested, start, end):
            if self.symbol == symbols[1]:
                raise YFinanceEmptyResponseError("no row for completed session")
            return super().fetch_daily_bars(requested, start, end)

    collected = daily.collect(
        collect_args,
        transport_factory=EmptyRowTransport,
        sleeper=lambda _: None,
    )

    assert collected["eligible_instruments"] == 2
    assert collected["quarantined_instruments"] == 1
    assert collected["quarantines"][0]["state"] == "no_session_row"
    assert collected["quarantines"][0]["symbol"] == symbols[1]

    accepted = daily.accept(accept_args)
    assert accepted["rows"] == 2
    assert accepted["watermark_after"] == "2026-08-25"

    repeated = daily.collect(collect_args, sleeper=lambda _: None)
    assert repeated["actual_requests"] == 0
    assert repeated["deduplicated_tasks"] == 2
    assert repeated["quarantines"][0]["state"] == "no_session_row"
    assert repeated["quarantines"][0]["symbol"] == symbols[1]


def test_daily_stale_session_row_is_explicit_and_watermark_can_advance(
    tmp_path: Path,
) -> None:
    symbols, collect_args, accept_args = _inputs(tmp_path)

    class StaleRowTransport(ReplayYFinanceTransport):
        def __init__(self, symbol: str) -> None:
            super().__init__(collect_args.fixture)
            self.symbol = symbol

        def fetch_daily_bars(self, requested, start, end):
            records = super().fetch_daily_bars(requested, start, end)
            if self.symbol == symbols[1]:
                records[0]["Date"] = "2026-08-24"
            return records

    collected = daily.collect(
        collect_args,
        transport_factory=StaleRowTransport,
        sleeper=lambda _: None,
    )

    assert collected["eligible_instruments"] == 2
    assert collected["quarantined_instruments"] == 1
    stale = collected["quarantines"][0]
    assert stale["state"] == "no_session_row"
    assert stale["symbol"] == symbols[1]
    assert stale["failure_stage"] == "provider_coverage"
    assert stale["error_type"] == "YFinanceMissingRequestedSessionError"
    assert stale["observed_dates"] == ["2026-08-24"]

    accepted = daily.accept(accept_args)
    assert accepted["rows"] == 2
    assert accepted["watermark_after"] == "2026-08-25"


def test_full_market_accept_rejects_low_session_coverage_before_watermark(
    tmp_path: Path,
) -> None:
    _, collect_args, accept_args = _inputs(tmp_path)
    payload = json.loads(collect_args.fixture.read_text())
    payload["records"] = payload["records"][:1]
    collect_args.fixture.write_text(json.dumps(payload), encoding="utf-8")

    collected = daily.collect(collect_args, sleeper=lambda _: None)
    assert collected["eligible_instruments"] == 1
    accept_args.minimum_session_coverage = 0.99
    accept_args.maximum_session_omissions = 100

    with pytest.raises(RuntimeError, match="session coverage is insufficient: 1/3"):
        daily.accept(accept_args)

    assert not accept_args.state.exists()


def test_daily_clustered_empty_responses_stop_before_transfer(tmp_path: Path) -> None:
    symbols, collect_args, _ = _inputs(tmp_path)
    collect_args.provider_failure_window = 3
    collect_args.provider_failure_limit = 2

    class ClusteredEmptyTransport(ReplayYFinanceTransport):
        def __init__(self, symbol: str) -> None:
            super().__init__(collect_args.fixture)
            self.symbol = symbol

        def fetch_daily_bars(self, requested, start, end):
            if self.symbol != symbols[1]:
                raise YFinanceEmptyResponseError("provider returned an empty response")
            return super().fetch_daily_bars(requested, start, end)

    with pytest.raises(RuntimeError, match="daily provider failure budget reached: 2/3"):
        daily.collect(
            collect_args,
            transport_factory=ClusteredEmptyTransport,
            sleeper=lambda _: None,
        )

    assert not collect_args.transfer.exists()


def test_daily_quality_empty_responses_share_the_provider_failure_budget(
    tmp_path: Path,
) -> None:
    symbols, collect_args, _ = _inputs(tmp_path)
    collect_args.provider_failure_window = 3
    collect_args.provider_failure_limit = 2

    class EmptyQualityTransport(ReplayYFinanceTransport):
        def __init__(self, symbol: str) -> None:
            super().__init__(collect_args.fixture)
            self.symbol = symbol

        def fetch_daily_bars(self, requested, start, end):
            if self.symbol != symbols[1]:
                return []
            return super().fetch_daily_bars(requested, start, end)

    with pytest.raises(RuntimeError, match="daily provider failure budget reached: 2/3"):
        daily.collect(
            collect_args,
            transport_factory=EmptyQualityTransport,
            sleeper=lambda _: None,
        )
    assert not collect_args.transfer.exists()


def test_session_unavailable_quarantine_can_use_explicit_recovery_generation(
    tmp_path: Path,
) -> None:
    symbols, collect_args, _ = _inputs(tmp_path)
    original = json.loads(collect_args.fixture.read_text())
    collect_args.fixture.write_text(json.dumps({"provider": "yfinance", "records": []}))

    with pytest.raises(RuntimeError, match="produced no eligible archives"):
        daily.collect(collect_args, sleeper=lambda _: None)

    collect_args.fixture.write_text(json.dumps(original))
    collect_args.task_recovery_generation = 1
    recovered = daily.collect(collect_args, sleeper=lambda _: None)

    assert recovered["eligible_instruments"] == len(symbols)
    assert recovered["quarantined_instruments"] == 0
    assert recovered["actual_requests"] == len(symbols)
    task_states = [
        json.loads(path.read_text(encoding="utf-8"))["task"]
        for path in collect_args.task_state_root.glob("task-*.json")
    ]
    recovered_versions = {
        task["instrument_scope"][0]: task["dataset_version"]
        for task in task_states
        if task["state"] == "succeeded"
    }
    assert set(recovered_versions) == set(symbols)
    assert all(version.endswith(".recovery-1") for version in recovered_versions.values())


def test_explicit_recovery_generation_retries_permanent_empty_response_once(
    tmp_path: Path,
) -> None:
    symbols, collect_args, _ = _inputs(tmp_path)

    class OneEmptyResponse(ReplayYFinanceTransport):
        def __init__(self, symbol: str) -> None:
            super().__init__(collect_args.fixture)
            self.symbol = symbol

        def fetch_daily_bars(self, requested, start, end):
            if self.symbol == symbols[1]:
                raise YFinanceEmptyResponseError("session not published")
            return super().fetch_daily_bars(requested, start, end)

    first = daily.collect(
        collect_args,
        transport_factory=OneEmptyResponse,
        sleeper=lambda _: None,
    )
    assert first["eligible_instruments"] == 2
    assert first["quarantines"][0]["error_type"] == "YFinanceEmptyResponseError"

    collect_args.task_recovery_generation = 1
    recovered = daily.collect(collect_args, sleeper=lambda _: None)

    assert recovered["eligible_instruments"] == 3
    assert recovered["actual_requests"] == 1
    assert recovered["quarantined_instruments"] == 0


def test_explicit_recovery_generation_replaces_stale_succeeded_task(
    tmp_path: Path,
) -> None:
    symbols, collect_args, _ = _inputs(tmp_path)

    class OneStaleResponse(ReplayYFinanceTransport):
        def __init__(self, symbol: str) -> None:
            super().__init__(collect_args.fixture)
            self.symbol = symbol

        def fetch_daily_bars(self, requested, start, end):
            records = super().fetch_daily_bars(requested, start, end)
            if self.symbol == symbols[1]:
                records[0]["Date"] = "2026-08-24"
            return records

    first = daily.collect(
        collect_args,
        transport_factory=OneStaleResponse,
        sleeper=lambda _: None,
    )
    assert first["eligible_instruments"] == 2
    assert first["quarantines"][0]["error_type"] == "YFinanceMissingRequestedSessionError"

    collect_args.task_recovery_generation = 1
    recovered = daily.collect(collect_args, sleeper=lambda _: None)

    assert recovered["eligible_instruments"] == 3
    assert recovered["actual_requests"] == 1
    assert recovered["quarantined_instruments"] == 0


def test_failed_readback_keeps_authoritative_watermark(tmp_path: Path, monkeypatch) -> None:
    _, collect_args, accept_args = _inputs(tmp_path)
    daily.collect(collect_args, sleeper=lambda _: None)
    monkeypatch.setattr(daily.LocalFileSystemStorage, "read_dataset", lambda *_: [])

    with pytest.raises(RuntimeError, match="readback audit"):
        daily.accept(accept_args)
    assert not accept_args.state.exists()


def test_readback_only_defers_second_import(tmp_path: Path) -> None:
    _, collect_args, accept_args = _inputs(tmp_path)
    daily.collect(collect_args, sleeper=lambda _: None)
    accept_args.verification_mode = "readback_only"

    accepted = daily.accept(accept_args)

    assert accepted["status"] == "passed"
    assert accepted["verification_mode"] == "readback_only"
    assert accepted["new_imports"] == 3
    assert accepted["replay_imports"] == 0
    assert accepted["idempotent_replay_deferred"] is True
    assert json.loads(accept_args.state.read_text())["watermark"] == "2026-08-25"

    replay_args = type(
        "ReplayArgs",
        (),
        {
            "snapshot": accept_args.snapshot,
            "session": date(2026, 8, 25),
            "state": accept_args.state,
            "initial_watermark": accept_args.initial_watermark,
            "transfer": accept_args.transfer,
            "scratch_parent": tmp_path / "periodic-scratch",
            "isolated_root": accept_args.isolated_root,
            "hmac_key_file": accept_args.hmac_key_file,
            "report": tmp_path / "periodic.json",
        },
    )()
    replayed = periodic.replay(replay_args)

    assert replayed["idempotent_replays"] == 3
    assert replayed["provider_requested"] is False
    assert replayed["history_rebuilt"] is False
    assert replayed["watermark_before"] == replayed["watermark_after"] == "2026-08-25"
