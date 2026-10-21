from __future__ import annotations

import os
import tempfile
from collections.abc import Iterable, Mapping
from datetime import date
from pathlib import Path

import duckdb
import pyarrow as pa

US_MARKET_INDEXES = {
    "^GSPC": ("S&P 500", "USD"),
    "^IXIC": ("NASDAQ Composite", "USD"),
    "^DJI": ("Dow Jones", "USD"),
    "^RUT": ("Russell 2000", "USD"),
}


def normalize_index_records(
    records: Iterable[Mapping[str, object]],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for raw in records:
        symbol = str(raw.get("ticker", "")).upper()
        if symbol not in US_MARKET_INDEXES:
            continue
        trading_date = date.fromisoformat(str(raw["Date"])[:10])
        rows.append(
            {
                "instrument_id": f"INDEX.{symbol}",
                "symbol": symbol,
                "market": "US",
                "trading_date": trading_date,
                "open": float(raw["Open"]),
                "high": float(raw["High"]),
                "low": float(raw["Low"]),
                "close": float(raw["Close"]),
                "volume": float(raw["Volume"]),
                "currency": "USD",
            }
        )
    return rows


def install_index_history(records: Iterable[Mapping[str, object]], root: Path) -> Path:
    rows = normalize_index_records(records)
    if not rows:
        raise ValueError("US index bootstrap requires usable rows")
    target = root / "history.parquet"
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        prefix=".history-", suffix=".parquet", dir=target.parent, delete=False
    ) as handle:
        candidate = Path(handle.name)
    try:
        connection = duckdb.connect()
        try:
            connection.register("index_rows", pa.Table.from_pylist(rows))
            connection.execute(
                "COPY (SELECT * FROM index_rows ORDER BY symbol,trading_date) "
                "TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
                [str(candidate)],
            )
        finally:
            connection.close()
        os.replace(candidate, target)
    finally:
        candidate.unlink(missing_ok=True)
    return target


def read_index_snapshot(
    root: Path, *, as_of_date: date | None = None, points: int = 30
) -> list[dict[str, object]]:
    paths = [root / "history.parquet"]
    paths.extend(sorted(root.glob("sessions/market=US/session=*/bars.parquet")))
    readable = [str(path) for path in paths if path.is_file()]
    if not readable:
        return []
    end = as_of_date or date.today()
    connection = duckdb.connect()
    try:
        rows = connection.execute(
            """
            SELECT symbol,trading_date,close FROM (
              SELECT symbol,trading_date,close,
                     row_number() OVER (
                       PARTITION BY symbol,trading_date ORDER BY source_order DESC
                     ) AS duplicate_rank
              FROM (
                SELECT symbol,trading_date,close,
                       row_number() OVER () AS source_order
                FROM read_parquet(?, union_by_name=true, hive_partitioning=false)
                WHERE trading_date <= ?
              )
            ) WHERE duplicate_rank=1
            ORDER BY symbol,trading_date
            """,
            [readable, end],
        ).fetchall()
    finally:
        connection.close()
    histories: dict[str, list[dict[str, object]]] = {symbol: [] for symbol in US_MARKET_INDEXES}
    for symbol, trading_date, close in rows:
        histories.setdefault(str(symbol), []).append(
            {"date": trading_date.isoformat(), "close": float(close)}
        )
    entries: list[dict[str, object]] = []
    for symbol, (display_name, currency) in US_MARKET_INDEXES.items():
        history = histories.get(symbol, [])[-points:]
        latest = history[-1] if history else None
        previous = history[-2] if len(history) > 1 else None
        change = None
        if latest and previous and previous["close"]:
            change = round(
                (float(latest["close"]) / float(previous["close"]) - 1) * 100, 2
            )
        entries.append(
            {
                "symbol": symbol,
                "display_name": display_name,
                "currency": currency,
                "latest_close": latest["close"] if latest else None,
                "latest_date": latest["date"] if latest else None,
                "change_1d": change,
                "history": history,
            }
        )
    return entries
