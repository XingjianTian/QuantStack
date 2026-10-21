from __future__ import annotations

import json
from pathlib import Path

import duckdb

from quant_stack.data.qfq_derivative import build_qfq_derivative


def test_qfq_derivative_adjusts_ohlc_and_anchors_latest_raw(tmp_path: Path) -> None:
    raw = tmp_path / "raw.parquet"
    output = tmp_path / "qfq.parquet"
    symbols = [f"{index:06d}.SS" for index in range(50)]
    ids = [f"XSHG.{index:06d}" for index in range(50)]
    corpus = tmp_path / "corpus.json"
    corpus.write_text(json.dumps({"symbols": symbols}), encoding="utf-8")
    factors = tmp_path / "factors.json"
    factors.write_text(
        json.dumps(
            {
                "requests": [
                    {
                        "instrument_id": instrument_id,
                        "status": (
                            "ok_no_adjustments" if instrument_id == ids[-1] else "ok"
                        ),
                        "empty_confirmations": 2 if instrument_id == ids[-1] else 0,
                        "rows": []
                        if instrument_id == ids[-1]
                        else [
                            {
                                "dividOperateDate": "2025-01-01",
                                "backAdjustFactor": "1.0",
                            },
                            {
                                "dividOperateDate": "2026-01-02",
                                "backAdjustFactor": "2.0",
                            },
                        ]
                        + (
                            [
                                {
                                    "dividOperateDate": "2026-01-03",
                                    "backAdjustFactor": "4.0",
                                }
                            ]
                            if instrument_id == ids[0]
                            else []
                        ),
                    }
                    for instrument_id in ids
                ]
            }
        ),
        encoding="utf-8",
    )
    connection = duckdb.connect()
    raw_sql = "'" + str(raw).replace("'", "''") + "'"
    try:
        connection.execute(
            "COPY (SELECT instrument_id, split_part(instrument_id, '.', 2) symbol, "
            "'XSHG' venue, trading_date, "
            "CAST(trading_date AS TIMESTAMP) AT TIME ZONE 'Asia/Shanghai' ts_event, "
            "10.0 AS open, 12.0 AS high, 9.0 AS low, 11.0 AS close, "
            "100.0 AS volume, 1100.0 AS amount, 'CNY' AS currency, "
            "struct_pack(adjustment := 'none') AS lineage "
            "FROM (SELECT unnest(?) instrument_id) i CROSS JOIN "
            "(VALUES (DATE '2024-12-31'), (DATE '2025-12-31'), "
            "(DATE '2026-01-02')) d(trading_date)) "
            f"TO {raw_sql} (FORMAT PARQUET)",
            [ids],
        )
    finally:
        connection.close()

    report = build_qfq_derivative(
        raw_bars_path=raw,
        factor_path=factors,
        corpus_path=corpus,
        output_path=output,
    )

    assert report["row_count"] == 150
    assert report["instrument_count"] == 50
    assert report["latest_anchor_max_close_drift"] == 0
    connection = duckdb.connect()
    try:
        rows = connection.execute(
            "SELECT trading_date, close, volume, lineage.adjustment FROM read_parquet(?) "
            "WHERE instrument_id='XSHG.000000' ORDER BY trading_date",
            [str(output)],
        ).fetchall()
    finally:
        connection.close()
    assert rows == [
        (rows[0][0], 11.0, 100.0, "qfq"),
        (rows[1][0], 5.5, 100.0, "qfq"),
        (rows[2][0], 11.0, 100.0, "qfq"),
    ]
