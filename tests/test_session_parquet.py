import gzip
import json
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from quant_stack.data.session_parquet import (
    inspect_session_package,
    install_session_package,
    replay_session_package,
)
from scripts.run_daily_session_parquet import _us_cache_records

SESSION = date(2026, 8, 28)


def _rows() -> list[dict[str, object]]:
    return [
        {
            "instrument_id": "XNAS.AAPL",
            "market": "US",
            "trading_date": SESSION,
            "open": 100.0,
            "high": 104.0,
            "low": 99.0,
            "close": 103.0,
            "volume": 10.0,
        },
        {
            "instrument_id": "XNAS.NVDA",
            "market": "US",
            "trading_date": SESSION,
            "open": 200.0,
            "high": 205.0,
            "low": 198.0,
            "close": 201.0,
            "volume": 20.0,
        },
    ]


def _snapshot(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "schema_version": "1.0.0",
                "snapshot_id": "session-test-v1",
                "as_of": "2026-08-28T00:00:00Z",
                "source": "curated_research_snapshot",
                "update_policy": "new_version",
                "instruments": [
                    {
                        "instrument_id": f"XNAS.{symbol}",
                        "symbol": symbol,
                        "canonical_venue": "XNAS",
                        "provider_symbol": symbol,
                        "status": "listed",
                    }
                    for symbol in ("AAPL", "NVDA")
                ],
            }
        ),
        encoding="utf-8",
    )


def _batch_cache(path: Path, *, missing_status: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": "quantstack.yfinance-daily-batch-cache.v2",
        "session_date": SESSION.isoformat(),
        "requested_symbols": ["AAPL", "NVDA"],
        "records": [
            {
                "ticker": "AAPL",
                "Date": f"{SESSION.isoformat()}T00:00:00",
                "Open": 100.0,
                "High": 104.0,
                "Low": 99.0,
                "Close": 103.0,
                "Volume": 10.0,
            }
        ],
        "symbol_outcomes": [
            {"symbol": "AAPL", "status": "returned"},
            {"symbol": "NVDA", "status": missing_status},
        ],
    }
    path.write_bytes(gzip.compress(json.dumps(payload).encode(), mtime=0))


def test_session_package_is_one_parquet_one_manifest_and_idempotent(tmp_path: Path) -> None:
    first = install_session_package(
        records=_rows(),
        output_root=tmp_path,
        market="US",
        session=SESSION,
        dataset_id="us_equity_daily",
        version="2026-08-28.daily-v1",
        adjustment="raw",
        requested_instruments=3,
        omissions=["XNYS.HALT"],
    )
    assert first.installed is True
    assert sorted(path.name for path in first.path.iterdir()) == ["bars.parquet", "manifest.json"]
    assert inspect_session_package(first.path).omission_count == 1

    second = install_session_package(
        records=_rows(),
        output_root=tmp_path,
        market="US",
        session=SESSION,
        dataset_id="us_equity_daily",
        version="2026-08-28.daily-v1",
        adjustment="raw",
        requested_instruments=3,
        omissions=["XNYS.HALT"],
    )
    assert second.installed is False
    assert replay_session_package(first.path, first.path / "bars.parquet") is True


def test_session_package_rejects_duplicate_and_unclosed_coverage(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="requested instruments"):
        install_session_package(
            records=_rows(),
            output_root=tmp_path,
            market="US",
            session=SESSION,
            dataset_id="us_equity_daily",
            version="v1",
            adjustment="raw",
            requested_instruments=4,
            omissions=["XNYS.HALT"],
        )
    with pytest.raises(ValueError, match="duplicate"):
        install_session_package(
            records=_rows() + [_rows()[0]],
            output_root=tmp_path,
            market="US",
            session=SESSION,
            dataset_id="us_equity_daily",
            version="v1",
            adjustment="raw",
            requested_instruments=2,
        )


def test_session_package_reinstall_rejects_equal_count_different_identity(
    tmp_path: Path,
) -> None:
    install_session_package(
        records=_rows(),
        output_root=tmp_path,
        market="US",
        session=SESSION,
        dataset_id="us_equity_daily",
        version="v1",
        adjustment="raw",
        requested_instruments=3,
        omissions=["XNYS.HALT"],
    )
    with pytest.raises(ValueError, match="conflicts"):
        install_session_package(
            records=_rows(),
            output_root=tmp_path,
            market="US",
            session=SESSION,
            dataset_id="us_equity_daily",
            version="v1",
            adjustment="raw",
            requested_instruments=3,
            omissions=["XNYS.OTHER"],
        )


def test_us_cache_distinguishes_legal_no_bar_from_provider_error(tmp_path: Path) -> None:
    snapshot = tmp_path / "snapshot.json"
    _snapshot(snapshot)
    legal = tmp_path / "legal" / "batch-0001.json.gz"
    _batch_cache(legal, missing_status="no_bar")
    records, omissions, requested, errors = _us_cache_records(
        legal.parent, snapshot, SESSION
    )
    assert requested == 2
    assert [row["instrument_id"] for row in records] == ["XNAS.AAPL"]
    assert omissions == ["XNAS.NVDA"]
    assert errors == []

    failed = tmp_path / "failed" / "batch-0001.json.gz"
    _batch_cache(failed, missing_status="provider_error")
    records, omissions, requested, errors = _us_cache_records(
        failed.parent, snapshot, SESSION
    )
    assert omissions == []
    assert errors == [
        {"symbol": "NVDA", "status": "provider_error", "batch": "batch-0001.json.gz"}
    ]
    with pytest.raises(ValueError, match="provider errors"):
        install_session_package(
            records=records,
            output_root=tmp_path / "installed",
            market="US",
            session=SESSION,
            dataset_id="us_equity_daily",
            version="v1",
            adjustment="raw",
            requested_instruments=requested,
            provider_errors=errors,
        )


def test_weekly_replay_detects_semantic_change(tmp_path: Path) -> None:
    package = install_session_package(
        records=_rows(),
        output_root=tmp_path / "installed",
        market="US",
        session=SESSION,
        dataset_id="us_equity_daily",
        version="v1",
        adjustment="raw",
    ).path
    changed = tmp_path / "changed.parquet"
    table = pq.ParquetFile(package / "bars.parquet").read()
    rows = table.to_pylist()
    rows[0]["close"] += 1
    rows[0]["high"] = max(rows[0]["high"], rows[0]["close"])
    import pyarrow as pa

    pq.write_table(pa.Table.from_pylist(rows), changed)
    assert replay_session_package(package, changed) is False
