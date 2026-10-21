from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

PARTITIONED_MARKET_BARS_SCHEMA_VERSION = "quantstack.partitioned-market-bars-dataset.v1"


@dataclass(frozen=True)
class MarketBarsPartition:
    first_instrument_id: str
    last_instrument_id: str
    bars: tuple[Path, ...]


def resolve_dataset_paths(
    descriptor_path: Path,
    values: object,
    *,
    required: bool,
    label: str,
) -> tuple[Path, ...]:
    if not isinstance(values, list) or (required and not values):
        raise ValueError(f"{label} paths are missing")
    resolved: list[Path] = []
    for value in values:
        candidate = Path(os.path.abspath(descriptor_path.parent / str(value)))
        if not candidate.is_file():
            raise ValueError(f"{label} source is missing: {candidate}")
        resolved.append(candidate)
    return tuple(resolved)


def parse_range_partitions(
    descriptor_path: Path,
    values: object,
    *,
    instrument_ids: tuple[str, ...] | None,
    label: str,
) -> tuple[MarketBarsPartition, ...]:
    if not isinstance(values, list) or not values:
        raise ValueError(f"{label} partition metadata is missing")
    partitions: list[MarketBarsPartition] = []
    previous_last = ""
    selected_partition_found = False
    for raw in values:
        if not isinstance(raw, dict):
            raise ValueError(f"{label} partition metadata is invalid")
        first = str(raw.get("first_instrument_id", ""))
        last = str(raw.get("last_instrument_id", ""))
        bars = raw.get("bars")
        if not first or first > last or first <= previous_last or not isinstance(bars, list):
            raise ValueError(f"{label} partition ranges are invalid")
        selected = instrument_ids is None or any(
            first <= instrument_id <= last for instrument_id in instrument_ids
        )
        selected_partition_found = selected_partition_found or selected
        resolved: list[Path] = []
        for value in bars:
            candidate = Path(os.path.abspath(descriptor_path.parent / str(value)))
            if selected and not candidate.is_file():
                raise ValueError(f"{label} partition source is missing: {candidate}")
            resolved.append(candidate)
        if not resolved:
            raise ValueError(f"{label} partition bars are missing")
        partitions.append(MarketBarsPartition(first, last, tuple(resolved)))
        previous_last = last
    if instrument_ids is not None and not selected_partition_found:
        schema_source = partitions[0].bars[0]
        if not schema_source.is_file():
            raise ValueError(f"{label} partition source is missing: {schema_source}")
    return tuple(partitions)


def select_partition_bars(
    partitions: tuple[MarketBarsPartition, ...],
    *,
    instrument_ids: tuple[str, ...] | None,
) -> tuple[Path, ...]:
    if instrument_ids is None:
        return tuple(bar for partition in partitions for bar in partition.bars)
    return tuple(
        bar
        for partition in partitions
        if any(
            partition.first_instrument_id <= instrument_id <= partition.last_instrument_id
            for instrument_id in instrument_ids
        )
        for bar in partition.bars
    )


def partition_payload(
    *,
    descriptor_path: Path,
    partitions: tuple[MarketBarsPartition, ...],
) -> list[dict[str, Any]]:
    return [
        {
            "first_instrument_id": partition.first_instrument_id,
            "last_instrument_id": partition.last_instrument_id,
            "bars": [os.path.relpath(path, descriptor_path.parent) for path in partition.bars],
        }
        for partition in partitions
    ]


def read_partitioned_bar_rows(
    sources: tuple[Path, ...],
    *,
    instrument_id: str,
    start_ts: datetime,
    end_ts: datetime,
    after: tuple[str, str] | None,
    limit: int,
    adjustment: str,
) -> list[tuple]:
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    tables = [
        pq.read_table(
            str(source),
            columns=[
                "instrument_id",
                "ts_event",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "amount",
                "currency",
            ],
            filters=[
                ("instrument_id", "=", instrument_id),
                ("ts_event", ">=", start_ts),
                ("ts_event", "<=", end_ts),
            ],
        )
        for source in sources
    ]
    table = pa.concat_tables(tables, promote_options="permissive")
    table = table.take(
        pc.sort_indices(
            table, sort_keys=[("instrument_id", "ascending"), ("ts_event", "ascending")]
        )
    )
    rows = table.to_pylist()
    if after is not None:
        after_instrument, after_timestamp = after
        after_ts = datetime.fromisoformat(after_timestamp)
        rows = [
            row
            for row in rows
            if row["instrument_id"] > after_instrument
            or (row["instrument_id"] == after_instrument and row["ts_event"] > after_ts)
        ]
    return [
        (
            row["instrument_id"],
            row["ts_event"],
            row["open"],
            row["high"],
            row["low"],
            row["close"],
            row["volume"],
            row["amount"],
            row["currency"],
            adjustment,
        )
        for row in rows[:limit]
    ]
