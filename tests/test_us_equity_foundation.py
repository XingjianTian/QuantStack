import json
import logging
import sys
import types
from datetime import UTC, date, datetime, time
from pathlib import Path

import duckdb
import pytest
from fastapi.testclient import TestClient
from jsonschema import Draft202012Validator

import quant_stack.data.providers.yfinance.transport as yfinance_transport_module
from quant_stack.api import create_app
from quant_stack.data.calendars import USExchangeCalendar
from quant_stack.data.models import DatasetLayer
from quant_stack.data.providers.yfinance import (
    LiveYFinanceTransport,
    ReplayYFinanceTransport,
    USInstrumentSnapshot,
    YFinanceCircuitOpenError,
    YFinanceEmptyResponseError,
    YFinanceProvider,
    YFinanceRateLimitError,
    load_us_instrument_snapshot,
    run_yfinance_daily_pipeline,
    run_yfinance_daily_task,
)
from quant_stack.data.storage import LocalFileSystemStorage, StagingFileSystemStorage
from quant_stack.data.tasks import FileTaskStore, TaskState
from quant_stack.data.worker_bundle import import_worker_archive
from quant_stack.platform import PlatformRepository
from scripts.run_us_equity_isolated_candidate import run as run_isolated_candidate

ROOT = Path(__file__).parents[1]
FIXTURE = (
    ROOT / "tests/fixtures/providers/yfinance/us_daily_bars_xnas_xnys_2026-11-25_2026-11-27.json"
)


@pytest.mark.parametrize("venue", ["XNYS", "XNAS"])
def test_us_exchange_calendar_models_holiday_early_close_and_dst(venue: str) -> None:
    calendar = USExchangeCalendar(venue)

    assert calendar.is_trading_day("US", date(2026, 11, 26)) is False
    assert calendar.session_close("US", date(2026, 11, 27)).time() == time(13)
    assert calendar.session_close("US", date(2026, 3, 6)).astimezone(UTC).hour == 21
    assert calendar.session_close("US", date(2026, 3, 9)).astimezone(UTC).hour == 20


def test_us_exchange_calendar_rejects_other_markets() -> None:
    with pytest.raises(ValueError, match="only the US market"):
        USExchangeCalendar("XNYS").is_trading_day("CN", date(2026, 11, 27))


def test_versioned_instrument_snapshot_drives_both_us_venues() -> None:
    snapshot = load_us_instrument_snapshot()
    provider = YFinanceProvider(ReplayYFinanceTransport(FIXTURE), snapshot)
    instruments = list(provider.list_instruments("US", ["AAPL", "IBM"]))

    assert snapshot.snapshot_id == "us-equity-research-2026-08-16.v1"
    assert [(item.instrument_id, item.venue) for item in instruments] == [
        ("XNAS.AAPL", "XNAS"),
        ("XNYS.IBM", "XNYS"),
    ]
    assert {item.instrument_snapshot_id for item in instruments} == {snapshot.snapshot_id}
    with pytest.raises(ValueError, match="absent from instrument snapshot"):
        list(provider.list_instruments("US", ["MISSING"]))


def test_versioned_instrument_snapshot_matches_json_schema() -> None:
    schema = json.loads((ROOT / "schemas/data/us_instrument_snapshot.schema.json").read_text())
    payload = json.loads(
        (
            ROOT / "src/quant_stack/data/providers/yfinance/us_instruments_research_v1.json"
        ).read_text()
    )
    Draft202012Validator(schema, format_checker=Draft202012Validator.FORMAT_CHECKER).validate(
        payload
    )
    assert all(item["currency"] == "USD" for item in payload["instruments"])
    assert all(item["timezone"] == "America/New_York" for item in payload["instruments"])


def test_instrument_snapshot_rejects_duplicate_provider_symbols() -> None:
    payload = load_us_instrument_snapshot().model_dump(mode="json")
    payload["instruments"].append(dict(payload["instruments"][0]))
    with pytest.raises(ValueError, match="instrument_id must be unique"):
        USInstrumentSnapshot.model_validate(payload)


def test_recoverable_us_task_builds_staging_bundle_and_preserves_price_semantics(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    state_root = tmp_path / "state"
    execution = run_yfinance_daily_task(
        data_root,
        state_root,
        symbols=["AAPL", "IBM"],
        start=date(2026, 11, 25),
        end=date(2026, 11, 27),
        dataset_version="2026-11-27.replay-v1",
        execution_time=datetime(2026, 11, 28, 3, tzinfo=UTC),
        transport=ReplayYFinanceTransport(FIXTURE),
        checkpoint={"next_instrument_index": 2},
        cursor="IBM",
    )

    assert execution.task.state is TaskState.SUCCEEDED
    assert execution.attempt is not None
    assert execution.attempt.checkpoint == {"next_instrument_index": 2}
    assert execution.attempt.cursor == "IBM"
    assert execution.bundle is not None
    assert execution.result is not None
    assert execution.result.bar_manifest.row_count == 4
    storage = StagingFileSystemStorage(data_root)
    raw_payload = json.loads(
        (data_root / execution.result.raw_artifact.uri.removeprefix("quant-data://")).read_text()
    )
    assert raw_payload["records"][0]["Close"] == 280.8
    assert raw_payload["records"][0]["Adj Close"] == 280.55
    audit = raw_payload["transport_audit"]
    assert {
        key: audit[key]
        for key in (
            "adjusted_close_semantics",
            "auto_adjust",
            "frequency",
            "instrument_snapshot_as_of",
            "instrument_snapshot_id",
            "normalized_price_semantics",
            "parallel_requests",
        )
    } == {
        "adjusted_close_semantics": "provider_native_raw_only",
        "auto_adjust": False,
        "frequency": "1d",
        "instrument_snapshot_as_of": "2026-08-16T00:00:00+00:00",
        "instrument_snapshot_id": "us-equity-research-2026-08-16.v1",
        "normalized_price_semantics": "unadjusted_ohlcv",
        "parallel_requests": False,
    }
    assert audit["completed_requests"] == 2
    assert len(audit["request_successful_source_fingerprints"]) == 2
    assert audit["all_requests_succeeded"] is True
    bars = storage.read_dataset(execution.result.bar_artifact.uri)
    assert {row["lineage"]["adjustment"] for row in bars} == {
        "unadjusted_ohlcv; adjusted_close_raw_only"
    }
    assert {row["lineage"]["canonical_mic"] for row in bars} == {"XNAS", "XNYS"}
    assert {row["lineage"]["instrument_snapshot_id"] for row in bars} == {
        "us-equity-research-2026-08-16.v1"
    }
    assert {row["ts_event"].hour for row in bars if row["ts_event"].day == 27} == {18}
    assert all(
        manifest.layer in {DatasetLayer.RAW, DatasetLayer.NORMALIZED}
        for manifest in storage.list_manifests()
    )
    persisted = FileTaskStore(state_root).load(execution.task.task_id)
    assert persisted is not None
    assert persisted.publication_bundle_uri == execution.attempt.publication_bundle_uri

    repeated = run_yfinance_daily_task(
        data_root,
        state_root,
        symbols=["AAPL", "IBM"],
        start=date(2026, 11, 25),
        end=date(2026, 11, 27),
        dataset_version="2026-11-27.replay-v1",
        execution_time=datetime(2026, 11, 29, 3, tzinfo=UTC),
        transport=ReplayYFinanceTransport(FIXTURE),
    )
    assert repeated.deduplicated is True
    assert repeated.attempt is None


def test_dual_venue_replay_runs_catalog_and_api_closed_loop(tmp_path: Path) -> None:
    storage = LocalFileSystemStorage(tmp_path)
    result = run_yfinance_daily_pipeline(
        tmp_path,
        symbols=["AAPL", "IBM"],
        start=date(2026, 11, 25),
        end=date(2026, 11, 27),
        execution_time=datetime(2026, 11, 28, 3, tzinfo=UTC),
        transport=ReplayYFinanceTransport(FIXTURE),
    )

    connection = duckdb.connect(str(tmp_path / "catalog/quant.duckdb"), read_only=True)
    catalog_row = connection.execute(
        """SELECT provider, row_count, quality_status
           FROM datasets
           WHERE dataset_id = 'yfinance_us_equity_daily'"""
    ).fetchone()
    connection.close()
    assert catalog_row == ("yfinance", 4, "passed")
    assert result.bar_manifest.row_count == 4

    repository = PlatformRepository(ROOT, tmp_path / "experiments", tmp_path)
    client = TestClient(create_app(repository, data_storage=storage))
    for symbol, instrument_id in (("AAPL", "XNAS.AAPL"), ("IBM", "XNYS.IBM")):
        response = client.get(
            f"/market/{symbol}/bars",
            params={"market": "US", "frequency": "1d"},
        )
        assert response.status_code == 200
        assert len(response.json()) == 2
        assert {row["instrument_id"] for row in response.json()} == {instrument_id}


def test_empty_replay_task_fails_closed_with_raw_evidence(tmp_path: Path) -> None:
    execution = run_yfinance_daily_task(
        tmp_path / "data",
        tmp_path / "state",
        symbols=["AAPL"],
        start=date(2026, 12, 1),
        end=date(2026, 12, 2),
        dataset_version="empty-v1",
        execution_time=datetime(2026, 12, 3, tzinfo=UTC),
        transport=ReplayYFinanceTransport(FIXTURE),
    )

    assert execution.task.state is TaskState.QUARANTINED
    assert execution.attempt is not None
    assert execution.attempt.failure_context is not None
    assert execution.attempt.failure_context.failure_stage == "quality_gate"
    assert execution.attempt.raw_uri is not None
    assert execution.bundle is None


def test_live_transport_rejects_unpinned_yfinance_before_request(monkeypatch) -> None:
    monkeypatch.setattr(yfinance_transport_module, "version", lambda _: "1.5.3")
    with pytest.raises(RuntimeError, match="expected 1.5.2, got 1.5.3"):
        LiveYFinanceTransport().fetch_daily_bars(["AAPL"], date(2026, 11, 27), date(2026, 11, 27))


def test_live_transport_fails_closed_on_first_empty_symbol(monkeypatch, tmp_path: Path) -> None:
    import pandas as pd

    calls: list[tuple[list[str], bool]] = []

    def download(**kwargs):
        calls.append((kwargs["tickers"], kwargs["threads"]))
        return pd.DataFrame()

    monkeypatch.setattr(yfinance_transport_module, "version", lambda _: "1.5.2")
    monkeypatch.setitem(
        sys.modules,
        "yfinance",
        types.SimpleNamespace(download=download, set_tz_cache_location=lambda _: None),
    )
    transport = LiveYFinanceTransport(runtime_root=tmp_path, minimum_interval_seconds=0.25)

    with pytest.raises(YFinanceEmptyResponseError):
        transport.fetch_daily_bars(["AAPL", "IBM"], date(2026, 11, 27), date(2026, 11, 27))
    assert calls == [(["AAPL"], False)]
    assert transport.audit_snapshot()["completed_requests"] == 1
    assert transport.audit_snapshot()["all_requests_succeeded"] is False


def test_live_transport_uses_isolated_cache_and_shared_request_pacing(
    monkeypatch, tmp_path: Path
) -> None:
    import pandas as pd

    cache_locations: list[str] = []
    calls: list[bool] = []
    sleeps: list[float] = []
    now = [100.0]

    def download(**kwargs):
        calls.append(kwargs["threads"])
        return pd.DataFrame(
            {
                "Open": [100.0],
                "High": [101.0],
                "Low": [99.0],
                "Close": [100.5],
                "Adj Close": [100.4],
                "Volume": [1_000_000.0],
            },
            index=pd.to_datetime(["2026-11-27"]),
        )

    def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        now[0] += seconds

    monkeypatch.setattr(yfinance_transport_module, "version", lambda _: "1.5.2")
    monkeypatch.setitem(
        sys.modules,
        "yfinance",
        types.SimpleNamespace(
            download=download,
            set_tz_cache_location=cache_locations.append,
        ),
    )
    first = LiveYFinanceTransport(
        runtime_root=tmp_path,
        minimum_interval_seconds=5,
        sleeper=sleep,
        clock=lambda: now[0],
    )
    second = LiveYFinanceTransport(
        runtime_root=tmp_path,
        minimum_interval_seconds=5,
        sleeper=sleep,
        clock=lambda: now[0],
    )

    assert len(first.fetch_daily_bars(["AAPL"], date(2026, 11, 27), date(2026, 11, 27))) == 1
    assert len(second.fetch_daily_bars(["IBM"], date(2026, 11, 27), date(2026, 11, 27))) == 1
    assert calls == [False, False]
    assert sleeps == [5.0]
    assert cache_locations == [str(tmp_path / "cache-v1")] * 2


def test_live_transport_can_retain_corporate_actions(monkeypatch, tmp_path: Path) -> None:
    import pandas as pd

    calls = []

    def download(**kwargs):
        calls.append(kwargs["actions"])
        return pd.DataFrame(
            {
                "Open": [100.0],
                "High": [101.0],
                "Low": [99.0],
                "Close": [100.0],
                "Adj Close": [99.0],
                "Volume": [1000.0],
                "Dividends": [1.0],
                "Stock Splits": [0.0],
            },
            index=pd.to_datetime(["2026-11-27"]),
        )

    monkeypatch.setattr(yfinance_transport_module, "version", lambda _: "1.5.2")
    monkeypatch.setitem(
        sys.modules,
        "yfinance",
        types.SimpleNamespace(download=download, set_tz_cache_location=lambda _: None),
    )
    rows = LiveYFinanceTransport(
        runtime_root=tmp_path, minimum_interval_seconds=0.25
    ).fetch_daily_bars_with_actions(["AAPL"], date(2026, 11, 27), date(2026, 11, 27))

    assert calls == [True]
    assert rows[0]["Dividends"] == 1
    assert rows[0]["Stock Splits"] == 0


def test_live_transport_batches_symbols_with_bounded_threads(monkeypatch, tmp_path: Path) -> None:
    import pandas as pd

    calls: list[dict[str, object]] = []

    def download(**kwargs):
        calls.append(kwargs)
        row = pd.DataFrame(
            {
                "Open": [100.0],
                "High": [101.0],
                "Low": [99.0],
                "Close": [100.5],
                "Adj Close": [100.4],
                "Volume": [1_000_000.0],
                "Dividends": [0.0],
                "Stock Splits": [0.0],
            },
            index=pd.to_datetime(["2026-11-27"]),
        )
        return pd.concat({"AAPL": row, "IBM": row}, axis=1)

    monkeypatch.setattr(yfinance_transport_module, "version", lambda _: "1.5.2")
    monkeypatch.setitem(
        sys.modules,
        "yfinance",
        types.SimpleNamespace(download=download, set_tz_cache_location=lambda _: None),
    )
    transport = LiveYFinanceTransport(runtime_root=tmp_path, minimum_interval_seconds=0.05)

    rows = transport.fetch_daily_bars_batch(
        ["AAPL", "IBM"],
        date(2026, 11, 27),
        date(2026, 11, 27),
        include_actions=True,
        threads=32,
    )

    assert calls[0]["tickers"] == ["AAPL", "IBM"]
    assert calls[0]["threads"] == 2
    assert {row["ticker"] for row in rows} == {"AAPL", "IBM"}
    assert transport.audit_snapshot()["completed_requests"] == 2
    assert transport.audit_snapshot()["all_requests_succeeded"] is True


def test_live_transport_isolates_explicit_proxy_route(monkeypatch, tmp_path: Path) -> None:
    import pandas as pd

    cache_locations: list[str] = []
    network = types.SimpleNamespace(proxy=None, retries=4)

    def download(**kwargs):
        return pd.DataFrame(
            {
                "Open": [100.0],
                "High": [101.0],
                "Low": [99.0],
                "Close": [100.5],
                "Adj Close": [100.4],
                "Volume": [1_000_000.0],
            },
            index=pd.to_datetime(["2026-11-27"]),
        )

    monkeypatch.setattr(yfinance_transport_module, "version", lambda _: "1.5.2")
    monkeypatch.setitem(
        sys.modules,
        "yfinance",
        types.SimpleNamespace(
            config=types.SimpleNamespace(network=network),
            download=download,
            set_tz_cache_location=cache_locations.append,
        ),
    )
    transport = LiveYFinanceTransport(
        runtime_root=tmp_path,
        proxy_url="http://127.0.0.1:7890",
        minimum_interval_seconds=0.25,
    )

    assert len(transport.fetch_daily_bars(["AAPL"], date(2026, 11, 27), date(2026, 11, 27))) == 1
    assert network.proxy == "http://127.0.0.1:7890"
    assert network.retries == 0
    assert transport.runtime_root == tmp_path / "routes/explicit-proxy-v1"
    assert cache_locations == [str(transport.runtime_root / "cache-v1")]


def test_live_transport_classifies_rate_limit_without_retry(monkeypatch, tmp_path: Path) -> None:
    import pandas as pd

    calls = 0

    def download(**kwargs):
        nonlocal calls
        calls += 1
        logging.getLogger("yfinance").error("YFRateLimitError: Too Many Requests")
        return pd.DataFrame()

    monkeypatch.setattr(yfinance_transport_module, "version", lambda _: "1.5.2")
    monkeypatch.setitem(
        sys.modules,
        "yfinance",
        types.SimpleNamespace(
            download=download,
            set_tz_cache_location=lambda _: None,
        ),
    )

    transport = LiveYFinanceTransport(
        runtime_root=tmp_path,
        minimum_interval_seconds=0.25,
        rate_limit_cooldown_seconds=86_400,
        clock=lambda: 100.0,
    )
    with pytest.raises(YFinanceRateLimitError):
        transport.fetch_daily_bars(["AAPL"], date(2026, 11, 27), date(2026, 11, 27))
    with pytest.raises(YFinanceCircuitOpenError):
        transport.fetch_daily_bars(["AAPL"], date(2026, 11, 27), date(2026, 11, 27))
    assert calls == 1


def test_live_transport_rejects_nonempty_frame_without_usable_ohlcv(
    monkeypatch, tmp_path: Path
) -> None:
    import pandas as pd

    def download(**kwargs):
        return pd.DataFrame(
            {
                "Open": [float("nan")],
                "High": [float("nan")],
                "Low": [float("nan")],
                "Close": [float("nan")],
                "Adj Close": [float("nan")],
                "Volume": [float("nan")],
            },
            index=pd.to_datetime(["2026-11-27"]),
        )

    monkeypatch.setattr(yfinance_transport_module, "version", lambda _: "1.5.2")
    monkeypatch.setitem(
        sys.modules,
        "yfinance",
        types.SimpleNamespace(download=download, set_tz_cache_location=lambda _: None),
    )
    transport = LiveYFinanceTransport(runtime_root=tmp_path, minimum_interval_seconds=0.25)

    with pytest.raises(YFinanceEmptyResponseError, match="no usable daily rows"):
        transport.fetch_daily_bars(["AAPL"], date(2026, 11, 27), date(2026, 11, 27))
    assert transport.audit_snapshot()["all_requests_succeeded"] is False


def test_us_isolated_candidate_archive_import_is_idempotent_and_import_only(
    tmp_path: Path,
) -> None:
    key = b"u" * 32
    key_path = tmp_path / "worker.key"
    key_path.write_bytes(key)
    args = type(
        "Args",
        (),
        {
            "staging_root": tmp_path / "producer",
            "task_state_root": tmp_path / "state",
            "archive": tmp_path / "us-isolated.tar",
            "fixture": FIXTURE,
            "dataset_version": "2026-11-27.isolated-v1",
            "start": date(2026, 11, 25),
            "end": date(2026, 11, 27),
            "commit_sha": "d" * 40,
            "environment_id": "us-isolated-test",
            "hmac_key_file": key_path,
        },
    )()

    report = run_isolated_candidate(args)

    assert report["symbols"] == ["AAPL", "IBM"]
    assert report["parallel_requests"] is False
    assert report["completed_requests"] == 2
    assert report["records_received"] == 4
    assert report["quality_status"] == "passed"
    assert report["license_scope"] == "research_only_review_yahoo_terms"
    assert report["catalog_published"] is False
    nas_root = tmp_path / "nas"
    first = import_worker_archive(
        archive_path=args.archive,
        data_root=nas_root,
        hmac_key=key,
    )
    replay = import_worker_archive(
        archive_path=args.archive,
        data_root=nas_root,
        hmac_key=key,
    )
    assert first.idempotent_replay is False
    assert replay.idempotent_replay is True
    assert not (nas_root / "catalog/quant.duckdb").exists()
    assert not (nas_root / "catalog/current-manifest-set.json").exists()
