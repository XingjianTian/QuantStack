import gzip
import json
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from quant_stack.data.batch import BatchBarsQuery, CanonicalBatchRepository
from quant_stack.data.session_parquet import install_session_package
from quant_stack.data.us_adjustment import (
    materialize_us_adjustment_shadow,
    publish_us_adjustment_shadow,
)
from scripts.promote_us_equity_daily_total_return import (
    _daily_records,
    _require_session_coverage,
    _session_parquet_records,
    promote,
)
from scripts.run_us_equity_daily import accept, collect


def test_daily_records_quarantines_missing_action_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    snapshot = tmp_path / "snapshot.json"
    snapshot.write_text(
        json.dumps(
            {
                "schema_version": "1.0.0",
                "snapshot_id": "us-action-quarantine-test.v1",
                "as_of": "2026-08-26T00:00:00Z",
                "source": "curated_research_snapshot",
                "update_policy": "new_version",
                "membership_basis": "current_xnas_xnys_listings_not_point_in_time",
                "source_refs": ["https://example.test"],
                "instruments": [
                    {
                        "instrument_id": f"XNAS.{symbol}",
                        "symbol": symbol,
                        "canonical_venue": "XNAS",
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
                    for symbol in ("AAA", "BBB")
                ],
            }
        ),
        encoding="utf-8",
    )
    session = date(2026, 8, 26)
    evidence_root = tmp_path / "action-evidence"
    evidence_path = evidence_root / session.isoformat() / "AAA.json.gz"
    evidence_path.parent.mkdir(parents=True)
    evidence_path.write_bytes(
            gzip.compress(
                json.dumps(
                    {
                        "schema_version": "quantstack.us-daily-action-evidence.v1",
                        "instrument_id": "XNAS.AAA",
                        "symbol": "AAA",
                        "session": session.isoformat(),
                        "Dividends": 0.0,
                        "Stock Splits": 0.0,
                    }
                ).encode()
        )
    )
    monkeypatch.setattr(
        "scripts.promote_us_equity_daily_total_return.iter_us_raw_series",
        lambda *_args, **_kwargs: iter(
            (
                symbol,
                [
                    {
                        "Date": session,
                        "Open": 10.0,
                        "High": 11.0,
                        "Low": 9.0,
                        "Close": 10.5,
                        "Adj Close": 10.5,
                        "Volume": 100.0,
                    }
                ],
            )
            for symbol in ("AAA", "BBB")
        ),
    )

    records, quarantines = _daily_records(
        isolated_root=tmp_path / "isolated",
        action_evidence_root=evidence_root,
        snapshot_path=snapshot,
        session=session,
    )

    assert [record["instrument_id"] for record in records] == ["XNAS.AAA"]
    assert quarantines == ["BBB"]


def test_total_return_rejects_partial_session_coverage() -> None:
    with pytest.raises(ValueError, match="264/5055"):
        _require_session_coverage(
            session=date(2026, 8, 28),
            observed_instruments=264,
            expected_instruments=5055,
            label="action-complete",
        )


def test_total_return_allows_bounded_terminal_omissions() -> None:
    assert _require_session_coverage(
        session=date(2026, 8, 28),
        observed_instruments=5010,
        expected_instruments=5055,
        label="action-complete",
    ) == pytest.approx(5010 / 5055)


def test_session_parquet_reuses_existing_action_cache_and_sidecar(tmp_path: Path) -> None:
    session = date(2026, 8, 28)
    package_root = tmp_path / "sessions"
    install_session_package(
        records=[
            {
                "instrument_id": f"XNAS.{symbol}",
                "open": 10.0,
                "high": 11.0,
                "low": 9.0,
                "close": 10.5,
                "volume": 100.0,
            }
            for symbol in ("AAA", "BBB")
        ],
        output_root=package_root,
        market="US",
        session=session,
        dataset_id="us_equity_daily",
        version="daily-v1",
        adjustment="raw",
        requested_instruments=3,
        omissions=("XNAS.CCC",),
    )
    cache_root = tmp_path / "provider-batches"
    cache_root.mkdir()
    (cache_root / "batch-0001.json.gz").write_bytes(
        gzip.compress(
            json.dumps(
                {
                    "session_date": session.isoformat(),
                    "records": [
                        {
                            "ticker": "AAA",
                            "Date": session.isoformat(),
                            "Dividends": 0.25,
                            "Stock Splits": 0.0,
                        }
                    ],
                }
            ).encode()
        )
    )
    evidence_root = tmp_path / "action-evidence"
    evidence = evidence_root / session.isoformat() / "BBB.json.gz"
    evidence.parent.mkdir(parents=True)
    evidence.write_bytes(
        gzip.compress(json.dumps({"Dividends": 0.0, "Stock Splits": 2.0}).encode())
    )

    records, omissions, quarantines = _session_parquet_records(
        package_root=package_root,
        action_evidence_root=evidence_root,
        action_cache_roots=(cache_root,),
        session=session,
    )

    by_id = {str(row["instrument_id"]): row for row in records}
    assert by_id["XNAS.AAA"]["Dividends"] == 0.25
    assert by_id["XNAS.BBB"]["Stock Splits"] == 2.0
    assert omissions == ["XNAS.CCC"]
    assert quarantines == []


def test_session_parquet_keeps_missing_action_separate_from_formal_omission(
    tmp_path: Path,
) -> None:
    session = date(2026, 8, 28)
    package_root = tmp_path / "sessions"
    install_session_package(
        records=[
            {
                "instrument_id": "XNAS.AAA",
                "open": 10.0,
                "high": 11.0,
                "low": 9.0,
                "close": 10.5,
                "volume": 100.0,
            }
        ],
        output_root=package_root,
        market="US",
        session=session,
        dataset_id="us_equity_daily",
        version="daily-v1",
        adjustment="raw",
        requested_instruments=2,
        omissions=("XNAS.BBB",),
    )

    records, omissions, quarantines = _session_parquet_records(
        package_root=package_root,
        action_evidence_root=tmp_path / "action-evidence",
        action_cache_roots=(),
        session=session,
    )

    assert records == []
    assert omissions == ["XNAS.BBB"]
    assert quarantines == ["AAA"]


def test_action_complete_daily_promotes_without_another_provider_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    snapshot = tmp_path / "snapshot.json"
    snapshot.write_text(
        json.dumps(
            {
                "schema_version": "1.0.0",
                "snapshot_id": "us-promotion-test.v1",
                "as_of": "2026-08-26T00:00:00Z",
                "source": "curated_research_snapshot",
                "update_policy": "new_version",
                "membership_basis": "current_xnas_xnys_listings_not_point_in_time",
                "source_refs": ["https://example.test"],
                "instruments": [
                    {
                        "instrument_id": "XNAS.AAA",
                        "symbol": "AAA",
                        "canonical_venue": "XNAS",
                        "provider_symbol": "AAA",
                        "source_symbol": "AAA",
                        "name": "AAA",
                        "asset_class": "equity",
                        "listed_from": None,
                        "listed_to": None,
                        "status": "listed",
                        "currency": "USD",
                        "timezone": "America/New_York",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    fixture = tmp_path / "fixture.json"
    fixture.write_text(
        json.dumps(
            {
                "provider": "yfinance",
                "records": [
                    {
                        "ticker": "AAA",
                        "Date": "2026-08-26",
                        "Open": 10.0,
                        "High": 12.0,
                        "Low": 9.0,
                        "Close": 11.0,
                        "Adj Close": 11.0,
                        "Volume": 120.0,
                        "Dividends": 1.0,
                        "Stock Splits": 0.0,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    key = tmp_path / "worker.key"
    key.write_bytes(b"p" * 32)
    work = tmp_path / "daily"
    transfer = work / "outbound/us-equity-daily-2026-08-26.tar"
    collected = collect(
        SimpleNamespace(
            snapshot=snapshot,
            session=date(2026, 8, 26),
            staging_root=work / "producer",
            task_state_root=work / "tasks",
            archive_root=work / "archives",
            transfer=transfer,
            report=work / "collect.json",
            hmac_key_file=key,
            commit_sha="a" * 40,
            environment_id="test",
            request_interval_seconds=0.1,
            yfinance_runtime_root=work / "runtime",
            yfinance_proxy_url=None,
            fixture=fixture,
            now=datetime(2026, 8, 27, 16, tzinfo=UTC),
            action_evidence_root=work / "action-evidence",
            action_sessions=(date(2026, 8, 26),),
        ),
        sleeper=lambda _: None,
    )
    assert collected["actual_requests"] == 1
    assert collected["price_requests"] == 1
    assert collected["action_evidence_requests"] == 1
    isolated = tmp_path / "isolated"
    accept(
        SimpleNamespace(
            snapshot=snapshot,
            state=work / "raw-state.json",
            initial_watermark=date(2026, 8, 25),
            transfer=transfer,
            scratch_parent=work / "scratch",
            isolated_root=isolated,
            hmac_key_file=key,
            report=work / "accept.json",
            verification_mode="readback_only",
            local_import_workers=1,
        )
    )

    data_root = tmp_path / "production"
    shadow = tmp_path / "base-shadow"
    materialize_us_adjustment_shadow(
        {
            "XNAS.AAA": [
                {
                    "Date": "2026-08-25",
                    "Open": 9.0,
                    "High": 11.0,
                    "Low": 8.0,
                    "Close": 10.0,
                    "Adj Close": 10.0,
                    "Volume": 100.0,
                    "Dividends": 0.0,
                    "Stock Splits": 0.0,
                }
            ]
        },
        shadow,
    )
    publish_us_adjustment_shadow(
        shadow_root=shadow,
        data_root=data_root,
        dataset_id="us_equity_total_return",
        dataset_version="base-v1",
        environment_id="test",
    )
    config = {
        "snapshot": str(snapshot),
        "isolated_root": str(isolated),
        "action_evidence_root": str(work / "action-evidence"),
        "data_root": str(data_root),
        "work_root": str(tmp_path / "promotion"),
        "state": str(tmp_path / "promotion/state.json"),
        "initial_watermark": "2026-08-25",
        "dataset_version_prefix": "us-total-return-daily-v1",
    }

    result = promote(config)
    assert result["status"] == "completed"
    assert result["provider_requested"] is False
    assert result["action_instruments"] == 1
    repository = CanonicalBatchRepository(data_root, consumer_access="candidate")
    query = BatchBarsQuery(
        instrument_ids=("XNAS.AAA",),
        start=date(2026, 8, 25),
        end=date(2026, 8, 26),
        market="US",
        dataset_id="us_equity_total_return",
        dataset_version="us-total-return-daily-v1.20260826",
    )
    plan = repository.prepare_bars(query)
    try:
        response = repository.execute_bars(plan, query)
    finally:
        plan.connection.close()
    assert [item.close for item in response.items] == [9.0, 11.0]
    assert promote(config)["status"] == "noop"

    recovery_version = "us-total-return-daily-v1.20260826.recovery-1"
    config["recovery_sessions"] = {
        "2026-08-26": {
            "base_dataset_version": "base-v1",
            "dataset_version": recovery_version,
        }
    }
    recovered = promote(config)
    assert recovered["status"] == "completed"
    assert recovered["recovery"] is True
    assert recovered["base_dataset_version"] == "base-v1"
    assert recovered["dataset_version"] == recovery_version
    assert promote(config)["status"] == "noop"

    monkeypatch.setattr(
        "scripts.promote_us_equity_daily_total_return._available_sessions",
        lambda _root: [date(2026, 8, 28)],
    )
    with pytest.raises(ValueError, match="would skip a market session"):
        promote(config, now=datetime(2026, 8, 29, 16, tzinfo=UTC))
