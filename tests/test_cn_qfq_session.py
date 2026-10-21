from __future__ import annotations

import json
from datetime import UTC, date, datetime, time
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from quant_stack.data.batch import BatchBarsQuery, CanonicalBatchRepository
from quant_stack.data.cn_qfq_dataset import cn_qfq_relation_sql
from quant_stack.data.cn_qfq_publication import publish_cn_qfq_session_dataset
from quant_stack.data.cn_qfq_session import build_cn_qfq_session_release


def _bars(path: Path, day: date, close: float) -> None:
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "instrument_id": "XSHG.600000",
                    "symbol": "600000",
                    "venue": "XSHG",
                    "trading_date": day,
                    "ts_event": datetime.combine(day, time(7), UTC),
                    "open": close,
                    "high": close,
                    "low": close,
                    "close": close,
                    "volume": 100.0,
                    "amount": 1000.0,
                    "currency": "CNY",
                }
            ]
        ),
        path,
    )


def _ledger(path: Path, watermark: date, factor: float) -> None:
    path.write_text(
        json.dumps(
            {
                "schema_version": "quantstack.cn-qfq-factor-ledger.v2",
                "watermark": watermark.isoformat(),
                "factors": [
                    {
                        "instrument_id": "XSHG.600000",
                        "effective_date": watermark.isoformat(),
                        "factor": factor,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )


def test_cn_qfq_session_appends_small_shards_and_leaves_base_immutable(tmp_path: Path) -> None:
    base = tmp_path / "base.parquet"
    raw = tmp_path / "raw.parquet"
    current_ledger = tmp_path / "current.json"
    next_ledger = tmp_path / "next.json"
    _bars(base, date(2026, 8, 26), 10.0)
    _bars(raw, date(2026, 8, 27), 5.0)
    _ledger(current_ledger, date(2026, 8, 26), 1.0)
    _ledger(next_ledger, date(2026, 8, 27), 2.0)
    before = base.stat().st_mtime_ns

    report = build_cn_qfq_session_release(
        session=date(2026, 8, 27),
        raw_session_path=raw,
        current_dataset_path=base,
        current_ledger_path=current_ledger,
        next_ledger_path=next_ledger,
        output_root=tmp_path / "release",
    )

    assert report["history_rewritten"] is False
    assert "large_payload_hashes" not in report
    assert base.stat().st_mtime_ns == before
    relation = cn_qfq_relation_sql(Path(str(report["descriptor_path"])))
    assert relation is not None
    connection = duckdb.connect()
    try:
        closes = connection.execute(
            f"SELECT close FROM {relation} ORDER BY trading_date"
        ).fetchall()
    finally:
        connection.close()
    assert closes == [(5.0,), (5.0,)]


def test_cn_qfq_session_derives_symbol_and_venue_for_legacy_raw(tmp_path: Path) -> None:
    base = tmp_path / "base.parquet"
    raw = tmp_path / "raw.parquet"
    current_ledger = tmp_path / "current.json"
    next_ledger = tmp_path / "next.json"
    _bars(base, date(2026, 8, 26), 10.0)
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "instrument_id": "XSHG.600000",
                    "trading_date": date(2026, 8, 27),
                    "ts_event": datetime.combine(date(2026, 8, 27), time(7), UTC),
                    "open": 10.0,
                    "high": 10.0,
                    "low": 10.0,
                    "close": 10.0,
                    "volume": 100.0,
                    "amount": 1000.0,
                    "currency": "CNY",
                }
            ]
        ),
        raw,
    )
    _ledger(current_ledger, date(2026, 8, 26), 1.0)
    _ledger(next_ledger, date(2026, 8, 27), 1.0)

    report = build_cn_qfq_session_release(
        session=date(2026, 8, 27),
        raw_session_path=raw,
        current_dataset_path=base,
        current_ledger_path=current_ledger,
        next_ledger_path=next_ledger,
        output_root=tmp_path / "release",
    )

    row = duckdb.connect().execute(
        "SELECT symbol,venue FROM read_parquet(?)", [report["daily_path"]]
    ).fetchone()
    assert row == ("600000", "XSHG")


def test_cn_qfq_session_descriptor_publishes_without_hashing_bar_shards(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    version = "daily-through-20260827.qfq-session-v1"
    target = data_root / "normalized/bars_1d/market=CN" / f"version={version}"
    target.mkdir(parents=True)
    base = tmp_path / "base.parquet"
    raw = tmp_path / "raw.parquet"
    current_ledger = tmp_path / "current.json"
    next_ledger = tmp_path / "next.json"
    _bars(base, date(2026, 8, 26), 10.0)
    _bars(raw, date(2026, 8, 27), 10.5)
    _ledger(current_ledger, date(2026, 8, 26), 1.0)
    _ledger(next_ledger, date(2026, 8, 27), 1.0)
    report = build_cn_qfq_session_release(
        session=date(2026, 8, 27),
        raw_session_path=raw,
        current_dataset_path=base,
        current_ledger_path=current_ledger,
        next_ledger_path=next_ledger,
        output_root=target,
    )
    session_manifest = tmp_path / "manifest.json"
    session_manifest.write_text('{"status":"passed"}\n', encoding="utf-8")
    instruments = tmp_path / "instruments.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "instrument_id": "XSHG.600000",
                    "symbol": "600000",
                    "venue": "XSHG",
                    "currency": "CNY",
                    "timezone": "Asia/Shanghai",
                    "active": True,
                }
            ]
        ),
        instruments,
    )
    receipt, proof = publish_cn_qfq_session_dataset(
        data_root=data_root,
        dataset_version=version,
        descriptor_path=Path(str(report["descriptor_path"])),
        session_manifest_path=session_manifest,
        instruments_source_path=instruments,
        row_count=2,
        instrument_count=1,
        first_date=date(2026, 8, 26),
        last_date=date(2026, 8, 27),
    )
    assert receipt.dataset_version == version
    assert "large_payload_hashes" not in proof
    instruments_reference = json.loads(
        (target / "evidence/instruments.json").read_text(encoding="utf-8")
    )
    assert instruments_reference["instrument_count"] == 1
    assert instruments_reference["source_file"] == "instruments.parquet"
    assert not (target / "evidence/instruments.parquet").exists()
    repository = CanonicalBatchRepository(data_root, consumer_access="candidate")
    query = BatchBarsQuery(
        instrument_ids=("XSHG.600000",),
        start=date(2026, 8, 26),
        end=date(2026, 8, 27),
        market="CN",
        dataset_id="cn_equity_daily",
        dataset_version=version,
    )
    plan = repository.prepare_bars(query)
    try:
        response = repository.execute_bars(plan, query)
    finally:
        plan.connection.close()
    assert [item.close for item in response.items] == [10.0, 10.5]
