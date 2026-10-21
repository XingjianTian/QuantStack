import gzip
import json
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from quant_stack.data.providers.yfinance import (
    ReplayYFinanceTransport,
    YFinanceRateLimitError,
    load_us_instrument_snapshot,
)
from quant_stack.data.tasks import TaskStateFile
from quant_stack.data.us_corporate_actions import (
    build_us_corporate_action_evidence_from_raw,
)
from scripts import run_us_equity_backfill as backfill
from scripts import run_us_equity_daily as daily
from scripts import run_us_equity_periodic_replay as periodic
from scripts.build_us_equity_recovery_snapshot import build as build_recovery_snapshot
from scripts.import_us_equity_shards import run as import_shards
from scripts.run_us_equity_core_daily_nas import FULL_MARKET_CONFIG_SCHEMA
from scripts.run_us_equity_daily_nas import load_config, plan_daily, run_daily

ROOT = Path(__file__).parents[1]
FULL_MARKET_SNAPSHOT = (
    ROOT / "src/quant_stack/data/providers/yfinance/us_equity_xnas_xnys_2026_08_27_v1.json"
)


def _inputs(tmp_path: Path) -> tuple[Path, Path, Path, list[str]]:
    symbols = ["AAA", "BBB", "CCC"]
    snapshot = {
        "schema_version": "1.0.0",
        "snapshot_id": "us-full-market-test.v1",
        "as_of": "2026-08-27T18:02:00Z",
        "source": "curated_research_snapshot",
        "update_policy": "new_version",
        "membership_basis": "current_xnas_xnys_listings_not_point_in_time",
        "source_refs": ["https://example.test/nasdaq-trader"],
        "instruments": [
            {
                "instrument_id": f"{'XNAS' if index < 2 else 'XNYS'}.{symbol}",
                "symbol": symbol,
                "canonical_venue": "XNAS" if index < 2 else "XNYS",
                "provider_symbol": symbol,
                "source_symbol": symbol,
                "name": f"{symbol} Common Stock",
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
    fixture = tmp_path / "fixture.json"
    fixture.write_text(
        json.dumps(
            {
                "provider": "yfinance",
                "records": [
                    {
                        "ticker": symbol,
                        "Date": "2026-08-25",
                        "Open": 99.0 + index,
                        "High": 101.0 + index,
                        "Low": 98.0 + index,
                        "Close": 100.0 + index,
                        "Adj Close": 99.5 + index,
                        "Volume": 1000 + index,
                    }
                    for index, symbol in enumerate(symbols)
                ],
            }
        ),
        encoding="utf-8",
    )
    key = tmp_path / "worker.key"
    key.write_bytes(b"f" * 32)
    return snapshot_path, fixture, key, symbols


def test_full_market_backfill_defaults_cover_the_versioned_universe() -> None:
    args = backfill._configure(SimpleNamespace())
    snapshot = load_us_instrument_snapshot(FULL_MARKET_SNAPSHOT)

    assert args.minimum_instruments == 1
    assert args.maximum_instruments == 10_000
    assert args.minimum_instruments <= len(snapshot.instruments) <= args.maximum_instruments
    assert len(snapshot.instruments) == 5079


def test_generic_backfill_is_serial_recoverable_and_uses_generic_shards(
    tmp_path: Path,
) -> None:
    snapshot, fixture, key, symbols = _inputs(tmp_path)
    args = SimpleNamespace(
        snapshot=snapshot,
        staging_root=tmp_path / "producer",
        task_state_root=tmp_path / "tasks",
        archive_root=tmp_path / "archives",
        transfer_shard_root=tmp_path / "shards",
        report=tmp_path / "report.json",
        hmac_key_file=key,
        commit_sha="a" * 40,
        environment_id="full-market-test",
        dataset_version="2016-2026.full-market-v1",
        start=date(2026, 8, 25),
        end=date(2026, 8, 25),
        execution_time=datetime(2026, 8, 26, tzinfo=UTC),
        request_interval_seconds=0.1,
        max_archives_per_shard=2,
        yfinance_runtime_root=tmp_path / "runtime",
        yfinance_proxy_url=None,
        fixture=fixture,
    )
    sleeps: list[float] = []

    first = backfill.run(args, sleeper=sleeps.append)
    repeated = backfill.run(args, sleeper=lambda _: None)

    assert first["instrument_count"] == len(symbols)
    assert first["actual_requests"] == len(symbols)
    assert first["provider_retries"] == 0
    assert first["parallel_requests"] is False
    assert first["corporate_actions_in_raw"] is True
    assert sleeps == [0.1, 0.1]
    assert [item["name"] for item in first["transfer_shards"]] == [
        "us-equity-archives-01.tar",
        "us-equity-archives-02.tar",
    ]
    assert repeated["actual_requests"] == 0
    assert repeated["deduplicated_tasks"] == len(symbols)

    isolated = tmp_path / "isolated"
    import_shards(
        SimpleNamespace(
            shard_root=args.transfer_shard_root,
            scratch_parent=tmp_path / "scratch",
            isolated_root=isolated,
            hmac_key_file=key,
            report=tmp_path / "import.json",
        )
    )
    action_package = tmp_path / "actions.json.gz"
    action_report = build_us_corporate_action_evidence_from_raw(
        data_root=isolated,
        dataset_version_prefix=args.dataset_version,
        snapshot=load_us_instrument_snapshot(snapshot),
        start=args.start,
        end=args.end,
        package_path=action_package,
        report_path=tmp_path / "actions-report.json",
    )
    package = json.loads(gzip.decompress(action_package.read_bytes()))
    assert action_report["provider_requested"] is False
    assert action_report["instrument_count"] == len(symbols)
    assert package["network_route"] == "existing_authenticated_raw"
    assert all(
        set(row) == {"Date", "Dividends", "Stock Splits"}
        for entry in package["entries"]
        for row in entry["actions"]
    )


def test_generic_import_reads_local_archive_roots_without_transfer_repacking(
    tmp_path: Path,
) -> None:
    snapshot, fixture, key, symbols = _inputs(tmp_path)
    args = SimpleNamespace(
        snapshot=snapshot,
        staging_root=tmp_path / "producer",
        task_state_root=tmp_path / "tasks",
        archive_root=tmp_path / "archives",
        transfer_shard_root=tmp_path / "shards",
        report=tmp_path / "backfill.json",
        hmac_key_file=key,
        commit_sha="a" * 40,
        environment_id="full-market-test",
        dataset_version="2016-2026.full-market-v1",
        start=date(2026, 8, 25),
        end=date(2026, 8, 25),
        execution_time=datetime(2026, 8, 26, tzinfo=UTC),
        request_interval_seconds=0.1,
        max_archives_per_shard=2,
        yfinance_runtime_root=tmp_path / "runtime",
        yfinance_proxy_url=None,
        fixture=fixture,
        skip_transfer_shards=True,
    )
    backfill_report = backfill.run(args, sleeper=lambda _: None)

    assert backfill_report["archive_transport"] == "nas_local_direct"
    assert backfill_report["transfer_shards"] == []
    assert not args.transfer_shard_root.exists()

    report = import_shards(
        SimpleNamespace(
            shard_root=None,
            scratch_parent=None,
            archive_root=[args.archive_root],
            isolated_root=tmp_path / "isolated",
            hmac_key_file=key,
            report=tmp_path / "import.json",
            local_import_workers=2,
            verification_mode="readback_only",
        )
    )

    assert report["status"] == "passed"
    assert report["source_mode"] == "archive_roots"
    assert report["archives"] == len(symbols)
    assert report["transfer_shards"] == 0
    assert report["new_imports"] == len(symbols)
    assert report["replay_imports"] == 0
    assert report["local_import_workers"] == 2
    assert report["idempotent_replay_deferred"] is True


def test_generic_backfill_records_one_provider_failure_and_continues(tmp_path: Path) -> None:
    snapshot, fixture, key, symbols = _inputs(tmp_path)
    args = SimpleNamespace(
        snapshot=snapshot,
        staging_root=tmp_path / "producer",
        task_state_root=tmp_path / "tasks",
        archive_root=tmp_path / "archives",
        transfer_shard_root=tmp_path / "shards",
        report=tmp_path / "report.json",
        hmac_key_file=key,
        commit_sha="a" * 40,
        environment_id="full-market-test",
        dataset_version="2016-2026.full-market-v1",
        start=date(2026, 8, 25),
        end=date(2026, 8, 25),
        execution_time=datetime(2026, 8, 26, tzinfo=UTC),
        request_interval_seconds=0.1,
        max_archives_per_shard=2,
        yfinance_runtime_root=tmp_path / "runtime",
        yfinance_proxy_url=None,
        fixture=fixture,
    )
    failed_symbol = symbols[1]
    fetched: list[str] = []

    class TrackingTransport(ReplayYFinanceTransport):
        def __init__(self, symbol: str) -> None:
            super().__init__(fixture)
            self.symbol = symbol

        def fetch_daily_bars(self, requested, start, end):
            fetched.append(self.symbol)
            if self.symbol == failed_symbol:
                raise ConnectionError("injected provider failure")
            return super().fetch_daily_bars(requested, start, end)

    first = backfill.run(args, transport_factory=TrackingTransport, sleeper=lambda _: None)
    fetched.clear()
    repeated = backfill.run(args, transport_factory=TrackingTransport, sleeper=lambda _: None)

    assert first["successful_instruments"] == 2
    assert first["failed_instruments"] == 1
    assert first["quarantined_instruments"] == 0
    assert first["actual_requests"] == 3
    assert first["terminal_failures"][0]["symbol"] == failed_symbol
    assert repeated["actual_requests"] == 0
    assert repeated["failed_instruments"] == 1
    assert fetched == []

    recovery_path = tmp_path / "recovery.json"
    recovery_report = build_recovery_snapshot(
        SimpleNamespace(
            snapshot=snapshot,
            task_state_root=args.task_state_root,
            dataset_version_prefix=args.dataset_version,
            start=args.start,
            end=args.end,
            recovery_snapshot_id="us-full-market-test.recovery1",
            output=recovery_path,
            report=tmp_path / "recovery-report.json",
        )
    )
    recovery = load_us_instrument_snapshot(recovery_path)
    assert recovery_report["recovery_instruments"] == 1
    assert [item.provider_symbol for item in recovery.instruments] == [failed_symbol]


def test_generic_backfill_stops_on_rate_limit(tmp_path: Path) -> None:
    snapshot, fixture, key, symbols = _inputs(tmp_path)
    args = SimpleNamespace(
        snapshot=snapshot,
        staging_root=tmp_path / "producer",
        task_state_root=tmp_path / "tasks",
        archive_root=tmp_path / "archives",
        transfer_shard_root=tmp_path / "shards",
        report=tmp_path / "report.json",
        hmac_key_file=key,
        commit_sha="a" * 40,
        environment_id="full-market-test",
        dataset_version="2016-2026.full-market-v1",
        start=date(2026, 8, 25),
        end=date(2026, 8, 25),
        execution_time=datetime(2026, 8, 26, tzinfo=UTC),
        request_interval_seconds=0.1,
        max_archives_per_shard=2,
        yfinance_runtime_root=tmp_path / "runtime",
        yfinance_proxy_url=None,
        fixture=fixture,
    )
    fetched: list[str] = []

    class RateLimitedTransport(ReplayYFinanceTransport):
        def __init__(self, symbol: str) -> None:
            super().__init__(fixture)
            self.symbol = symbol

        def fetch_daily_bars(self, requested, start, end):
            fetched.append(self.symbol)
            if self.symbol == symbols[1]:
                raise YFinanceRateLimitError("injected rate limit")
            return super().fetch_daily_bars(requested, start, end)

    with pytest.raises(RuntimeError, match="backfill stopped"):
        backfill.run(
            args,
            transport_factory=RateLimitedTransport,
            sleeper=lambda _: None,
        )

    assert fetched == symbols[:2]


def test_generic_backfill_stops_when_provider_failures_cluster(tmp_path: Path) -> None:
    snapshot, fixture, key, symbols = _inputs(tmp_path)
    args = SimpleNamespace(
        snapshot=snapshot,
        staging_root=tmp_path / "producer",
        task_state_root=tmp_path / "tasks",
        archive_root=tmp_path / "archives",
        transfer_shard_root=tmp_path / "shards",
        report=tmp_path / "report.json",
        hmac_key_file=key,
        commit_sha="a" * 40,
        environment_id="full-market-test",
        dataset_version="2016-2026.full-market-v1",
        start=date(2026, 8, 25),
        end=date(2026, 8, 25),
        execution_time=datetime(2026, 8, 26, tzinfo=UTC),
        request_interval_seconds=0.1,
        max_archives_per_shard=2,
        yfinance_runtime_root=tmp_path / "runtime",
        yfinance_proxy_url=None,
        fixture=fixture,
        provider_failure_window=3,
        provider_failure_limit=2,
    )
    fetched: list[str] = []

    class ClusteredFailureTransport(ReplayYFinanceTransport):
        def __init__(self, symbol: str) -> None:
            super().__init__(fixture)
            self.symbol = symbol

        def fetch_daily_bars(self, requested, start, end):
            fetched.append(self.symbol)
            if self.symbol != symbols[1]:
                raise ConnectionError("injected provider failure")
            return super().fetch_daily_bars(requested, start, end)

    with pytest.raises(RuntimeError, match="provider failure budget reached: 2/3"):
        backfill.run(
            args,
            transport_factory=ClusteredFailureTransport,
            sleeper=lambda _: None,
        )

    assert fetched == symbols


def test_generic_daily_collect_accept_and_periodic_replay(tmp_path: Path) -> None:
    snapshot, fixture, key, symbols = _inputs(tmp_path)
    transfer = tmp_path / "outbound" / "us-equity-daily-2026-08-25.tar"
    legacy_archive = (
        tmp_path / "daily-archives/2026-08-25/full-market-daily-v0" / f"0001-{symbols[0]}.tar"
    )
    legacy_archive.parent.mkdir(parents=True)
    legacy_archive.write_bytes(b"retained-v1")
    collect_args = SimpleNamespace(
        snapshot=snapshot,
        session=date(2026, 8, 25),
        staging_root=tmp_path / "daily-producer",
        task_state_root=tmp_path / "daily-tasks",
        archive_root=tmp_path / "daily-archives",
        transfer=transfer,
        report=tmp_path / "collect.json",
        hmac_key_file=key,
        commit_sha="b" * 40,
        environment_id="nas-us-equity-daily-test",
        request_interval_seconds=0.1,
        yfinance_runtime_root=tmp_path / "daily-runtime",
        yfinance_proxy_url=None,
        fixture=fixture,
        now=datetime(2026, 8, 26, 16, tzinfo=UTC),
    )
    collected = daily.collect(collect_args, sleeper=lambda _: None)
    task_versions = {
        TaskStateFile.model_validate_json(path.read_text()).task.dataset_version
        for path in collect_args.task_state_root.glob("task-*.json")
    }
    assert collected["schema_version"] == daily.TRANSFER_SCHEMA
    assert collected["aggregate_transfers"] == 1
    assert collected["actual_requests"] == len(symbols)
    assert all(".full-market-daily-v1." in item for item in task_versions)
    assert legacy_archive.read_bytes() == b"retained-v1"
    assert len(
        list((collect_args.archive_root / "2026-08-25/full-market-daily-v1").glob("*.tar"))
    ) == len(symbols)

    state = tmp_path / "watermark.json"
    accept_args = SimpleNamespace(
        snapshot=snapshot,
        state=state,
        initial_watermark=date(2026, 8, 24),
        transfer=transfer,
        scratch_parent=tmp_path / "scratch",
        isolated_root=tmp_path / "isolated",
        hmac_key_file=key,
        report=tmp_path / "accept.json",
        verification_mode="readback_only",
        local_import_workers=2,
    )
    accepted = daily.accept(accept_args)
    assert accepted["rows"] == len(symbols)
    assert accepted["watermark_after"] == "2026-08-25"
    assert accepted["idempotent_replay_deferred"] is True
    assert accepted["local_import_workers"] == 2

    replayed = periodic.replay(
        SimpleNamespace(
            snapshot=snapshot,
            session=date(2026, 8, 25),
            state=state,
            initial_watermark=date(2026, 8, 24),
            transfer=transfer,
            scratch_parent=tmp_path / "periodic-scratch",
            isolated_root=accept_args.isolated_root,
            hmac_key_file=key,
            report=tmp_path / "periodic.json",
            local_import_workers=2,
        )
    )
    assert replayed["provider_requested"] is False
    assert replayed["idempotent_replays"] == len(symbols)
    assert replayed["local_import_workers"] == 2
    assert replayed["watermark_before"] == replayed["watermark_after"]


def test_daily_plan_proves_accepted_prices_are_not_requested(tmp_path: Path) -> None:
    snapshot, fixture, key, symbols = _inputs(tmp_path)
    work = tmp_path / "work"
    daily.collect(
        SimpleNamespace(
            snapshot=snapshot,
            session=date(2026, 8, 25),
            staging_root=work / "producer",
            task_state_root=work / "tasks",
            archive_root=work / "archives",
            transfer=work / "outbound/session.tar",
            report=work / "reports/collect.json",
            hmac_key_file=key,
            commit_sha="b" * 40,
            environment_id="test",
            request_interval_seconds=0.1,
            yfinance_runtime_root=work / "runtime",
            yfinance_proxy_url=None,
            fixture=fixture,
            now=datetime(2026, 8, 26, 16, tzinfo=UTC),
        ),
        sleeper=lambda _: None,
    )
    config = {
        "schema_version": FULL_MARKET_CONFIG_SCHEMA,
        "snapshot": str(snapshot),
        "state": str(work / "state.json"),
        "initial_watermark": "2026-08-24",
        "work_root": str(work),
        "action_evidence_root": str(work / "action-evidence"),
        "action_initial_watermark": "2026-08-24",
    }

    plan = plan_daily(config, now=datetime(2026, 8, 26, 16, tzinfo=UTC))

    assert plan["reused_price_tasks"] == len(symbols)
    assert plan["price_requests"] == 0
    assert plan["action_evidence_requests"] == len(symbols)
    assert plan["duplicate_price_requests"] == 0
    assert plan["provider_requested"] is False


def test_daily_plan_counts_explicit_session_recovery_as_missing_price_work(
    tmp_path: Path,
) -> None:
    snapshot, fixture, key, symbols = _inputs(tmp_path)
    fixture.write_text(json.dumps({"provider": "yfinance", "records": []}))
    work = tmp_path / "work"
    with pytest.raises(RuntimeError, match="produced no eligible archives"):
        daily.collect(
            SimpleNamespace(
                snapshot=snapshot,
                session=date(2026, 8, 25),
                staging_root=work / "producer",
                task_state_root=work / "tasks",
                archive_root=work / "archives",
                transfer=work / "outbound/session.tar",
                report=work / "reports/collect.json",
                hmac_key_file=key,
                commit_sha="b" * 40,
                environment_id="test",
                scope_label="US full-market",
                dataset_version_prefix="full-market-daily-v1",
                request_interval_seconds=0.1,
                yfinance_runtime_root=work / "runtime",
                yfinance_proxy_url=None,
                fixture=fixture,
                now=datetime(2026, 8, 26, 16, tzinfo=UTC),
            ),
            sleeper=lambda _: None,
        )
    config = {
        "schema_version": FULL_MARKET_CONFIG_SCHEMA,
        "snapshot": str(snapshot),
        "state": str(work / "state.json"),
        "initial_watermark": "2026-08-24",
        "work_root": str(work),
        "action_evidence_root": str(work / "action-evidence"),
        "action_initial_watermark": "2026-08-24",
        "task_recovery_generations": {"2026-08-25": 1},
    }

    plan = plan_daily(config, now=datetime(2026, 8, 26, 16, tzinfo=UTC))

    assert plan["recoverable_price_tasks"] == len(symbols)
    assert plan["terminal_price_tasks"] == 0
    assert plan["price_requests"] == len(symbols)
    assert plan["total_provider_requests"] == len(symbols)


def test_daily_plan_recovers_an_explicit_incomplete_watermarked_session(
    tmp_path: Path,
) -> None:
    snapshot, _, _, symbols = _inputs(tmp_path)
    snapshot_id = json.loads(snapshot.read_text())["snapshot_id"]
    work = tmp_path / "work"
    state = work / "state.json"
    state.parent.mkdir(parents=True)
    state.write_text(
        json.dumps(
            {
                "schema_version": "1.0.0",
                "dataset_id": "yfinance_us_equity_daily",
                "instrument_snapshot_id": snapshot_id,
                "watermark": "2026-08-25",
            }
        )
    )
    reports = work / "reports"
    reports.mkdir()
    (reports / "accept-2026-08-25.json").write_text(
        json.dumps({"status": "passed", "instruments": 1})
    )
    config = {
        "schema_version": FULL_MARKET_CONFIG_SCHEMA,
        "snapshot": str(snapshot),
        "state": str(state),
        "initial_watermark": "2026-08-24",
        "work_root": str(work),
        "action_evidence_root": str(work / "action-evidence"),
        "action_initial_watermark": "2026-08-24",
        "task_recovery_generations": {"2026-08-25": 1},
    }

    plan = plan_daily(config, now=datetime(2026, 8, 26, 16, tzinfo=UTC))

    assert plan["session_date"] == "2026-08-25"
    assert plan["universe_instruments"] == len(symbols)
    assert plan["price_requests"] == len(symbols)


def test_full_market_nas_runner_propagates_generic_daily_contract(tmp_path: Path) -> None:
    snapshot, _, key, _ = _inputs(tmp_path)
    config = {
        "schema_version": FULL_MARKET_CONFIG_SCHEMA,
        "repo_root": str(ROOT),
        "snapshot": str(snapshot),
        "state": str(tmp_path / "state.json"),
        "initial_watermark": "2026-08-24",
        "work_root": str(tmp_path / "work"),
        "isolated_root": str(tmp_path / "isolated"),
        "hmac_key_file": str(key),
        "yfinance_runtime_root": str(tmp_path / "runtime"),
        "yfinance_proxy_url": "http://127.0.0.1:7890",
        "action_evidence_root": str(tmp_path / "action-evidence"),
        "action_initial_watermark": "2026-08-24",
        "verification_mode": "readback_only",
        "local_import_workers": 4,
        "source_commit": "c" * 40,
        "provider_threads": 24,
        "provider_batch_size": 128,
    }
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    loaded = load_config(config_path)
    observed: list[SimpleNamespace] = []

    def collect(args: SimpleNamespace) -> dict[str, object]:
        observed.append(args)
        return {
            "actual_requests": 3,
            "request_interval_seconds": 0.1,
            "aggregate_transfers": 1,
        }

    def accept(args: SimpleNamespace) -> dict[str, object]:
        observed.append(args)
        return {
            "rows": 3,
            "instruments": 3,
            "duplicate_keys": 0,
            "verification_mode": "readback_only",
            "watermark_after": "2026-08-25",
        }

    result = run_daily(
        loaded,
        now=datetime(2026, 8, 26, 16, tzinfo=UTC),
        collect_fn=collect,
        accept_fn=accept,
        readiness_fn=lambda config, **kwargs: {
            "status": "ready",
            "session_date": kwargs["session"].isoformat(),
        },
    )

    assert observed[0].dataset_version_prefix == "full-market-daily-v1"
    assert observed[0].commit_sha == "c" * 40
    assert observed[0].transfer_schema == daily.TRANSFER_SCHEMA
    assert observed[0].transfer.name == "us-equity-daily-2026-08-25.tar"
    assert observed[0].provider_threads == 24
    assert observed[0].provider_batch_size == 128
    assert observed[1].verification_mode == "readback_only"
    assert observed[1].local_import_workers == 4
    assert observed[1].minimum_session_coverage == 0.99
    assert observed[1].maximum_session_omissions == 100
    assert result["schema_version"] == "quantstack.us-equity-daily-nas-run.v1"


def test_full_market_runner_reuses_accepted_v1_state(tmp_path: Path) -> None:
    snapshot, _, key, _ = _inputs(tmp_path)
    snapshot_id = json.loads(snapshot.read_text())["snapshot_id"]
    state = tmp_path / "legacy-state.json"
    state.write_text(
        json.dumps(
            {
                "schema_version": "1.0.0",
                "dataset_id": "yfinance_us_equity_daily",
                "instrument_snapshot_id": snapshot_id,
                "watermark": "2026-08-27",
            }
        )
    )
    config = {
        "schema_version": FULL_MARKET_CONFIG_SCHEMA,
        "repo_root": str(ROOT),
        "snapshot": str(snapshot),
        "state": str(state),
        "initial_watermark": "2026-08-26",
        "work_root": str(tmp_path / "work-v2"),
        "isolated_root": str(tmp_path / "isolated"),
        "hmac_key_file": str(key),
        "yfinance_runtime_root": str(tmp_path / "runtime"),
        "yfinance_proxy_url": "http://127.0.0.1:7890",
        "action_evidence_root": str(tmp_path / "action-evidence"),
        "action_initial_watermark": "2026-08-26",
        "verification_mode": "readback_only",
        "source_commit": "c" * 40,
    }

    plan = plan_daily(config, now=datetime(2026, 8, 29, 16, tzinfo=UTC))
    assert plan["session_date"] == "2026-08-28"
    assert plan["provider_requested"] is False
