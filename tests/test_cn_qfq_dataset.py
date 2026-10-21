from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from quant_stack.data.cn_qfq_dataset import cn_qfq_relation_sql, relative_dataset_payload


def _write(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)


def test_cn_qfq_descriptor_projects_future_factor_events_without_rewriting_history(
    tmp_path: Path,
) -> None:
    base = tmp_path / "base.parquet"
    daily = tmp_path / "daily.parquet"
    events = tmp_path / "events.parquet"
    descriptor = tmp_path / "bars.json"
    common = {
        "instrument_id": "XSHG.600000",
        "symbol": "600000",
        "venue": "XSHG",
        "amount": 1000.0,
        "currency": "CNY",
        "volume": 100.0,
    }
    _write(
        base,
        [
            {
                **common,
                "trading_date": "2026-08-26",
                "ts_event": "2026-08-26T07:00:00Z",
                "open": 10.0,
                "high": 11.0,
                "low": 9.0,
                "close": 10.0,
            }
        ],
    )
    _write(
        daily,
        [
            {
                **common,
                "trading_date": "2026-08-27",
                "ts_event": "2026-08-27T07:00:00Z",
                "open": 5.0,
                "high": 5.5,
                "low": 4.5,
                "close": 5.0,
            }
        ],
    )
    _write(
        events,
        [{"instrument_id": "XSHG.600000", "session": "2026-08-27", "price_multiplier": 0.5}],
    )
    descriptor.write_text(
        json.dumps(
            relative_dataset_payload(
                descriptor_path=descriptor, bars=(base, daily), events=(events,)
            )
        ),
        encoding="utf-8",
    )

    relation = cn_qfq_relation_sql(descriptor)
    assert relation is not None
    assert relation.count("product(e.price_multiplier)") == 1
    connection = duckdb.connect()
    try:
        rows = connection.execute(
            f"SELECT trading_date,close FROM {relation} ORDER BY trading_date"
        ).fetchall()
    finally:
        connection.close()
    assert [(str(day), close) for day, close in rows] == [
        ("2026-08-26", 5.0),
        ("2026-08-27", 5.0),
    ]
