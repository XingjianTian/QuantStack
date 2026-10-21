from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from .partitioned_bars_dataset import (
    PARTITIONED_MARKET_BARS_SCHEMA_VERSION,
    MarketBarsPartition,
    parse_range_partitions,
    partition_payload,
    read_partitioned_bar_rows,
    resolve_dataset_paths,
    select_partition_bars,
)

SCHEMA_VERSION = "quantstack.us-total-return-dataset.v1"
PARTITIONED_SCHEMA_VERSION = "quantstack.us-total-return-partitioned-dataset.v1"


@dataclass(frozen=True)
class USTotalReturnDataset:
    bars: tuple[Path, ...]
    events: tuple[Path, ...]
    shared_bars: tuple[Path, ...] = ()
    partitions: tuple[MarketBarsPartition, ...] = ()
    latest: Path | None = None
    row_count: int | None = None
    instrument_count: int | None = None
    watermark: str | None = None


def _sql_string(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _paths_sql(paths: tuple[Path, ...]) -> str:
    return "[" + ",".join(_sql_string(path) for path in paths) + "]"


def load_us_total_return_dataset(
    path: Path, *, instrument_ids: tuple[str, ...] | None = None
) -> USTotalReturnDataset:
    path = path.expanduser().resolve()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return USTotalReturnDataset((path,), (), shared_bars=(path,))
    schema_version = payload.get("schema_version")
    if schema_version not in {
        SCHEMA_VERSION,
        PARTITIONED_SCHEMA_VERSION,
        PARTITIONED_MARKET_BARS_SCHEMA_VERSION,
    }:
        return USTotalReturnDataset((path,), (), shared_bars=(path,))
    if schema_version == PARTITIONED_MARKET_BARS_SCHEMA_VERSION and (
        payload.get("market") != "US" or payload.get("adjustment") != "total_return"
    ):
        raise ValueError("partitioned market bars semantics are not US total-return")

    def resolve(values: object, *, required: bool, label: str) -> tuple[Path, ...]:
        return resolve_dataset_paths(
            path,
            values,
            required=required,
            label=f"US total-return {label}",
        )

    shared_bars = resolve(
        payload.get("bars", []), required=schema_version == SCHEMA_VERSION, label="bars"
    )
    raw_events = payload.get("events", [])
    events = () if raw_events == [] else resolve(raw_events, required=True, label="events")
    partitions: tuple[MarketBarsPartition, ...] = ()
    if schema_version in {
        PARTITIONED_SCHEMA_VERSION,
        PARTITIONED_MARKET_BARS_SCHEMA_VERSION,
    }:
        partitions = parse_range_partitions(
            path,
            payload.get("partitions"),
            instrument_ids=instrument_ids,
            label="US total-return",
        )
    selected = select_partition_bars(partitions, instrument_ids=instrument_ids)
    bars = (*selected, *shared_bars) if partitions else shared_bars
    latest_value = payload.get("latest")
    latest = (path.parent / str(latest_value)).resolve() if latest_value else None
    if latest is not None and not latest.is_file():
        raise ValueError(f"US total-return latest snapshot is missing: {latest}")
    return USTotalReturnDataset(
        bars=bars,
        events=events,
        shared_bars=shared_bars,
        partitions=partitions,
        latest=latest,
        row_count=int(payload["row_count"]) if payload.get("row_count") is not None else None,
        instrument_count=(
            int(payload["instrument_count"])
            if payload.get("instrument_count") is not None
            else None
        ),
        watermark=str(payload["watermark"]) if payload.get("watermark") is not None else None,
    )


def us_total_return_relation_sql(
    path: Path, *, instrument_ids: tuple[str, ...] | None = None
) -> str:
    dataset = load_us_total_return_dataset(path, instrument_ids=instrument_ids)
    if dataset.bars:
        bars_sql = (
            f"read_parquet({_paths_sql(dataset.bars)}, union_by_name=true, hive_partitioning=false)"
        )
    else:
        first = dataset.partitions[0].bars[0]
        bars_sql = (
            f"(SELECT * FROM read_parquet({_sql_string(first)}, "
            "hive_partitioning=false) WHERE false)"
        )
    if not dataset.events:
        return bars_sql
    events_sql = (
        f"read_parquet({_paths_sql(dataset.events)}, union_by_name=true, hive_partitioning=false)"
    )
    return f"""
    (
      SELECT
        b.instrument_id, b.symbol, b.venue, b.trading_date, b.ts_event,
        b.amount, b.currency,
        b.open * factor.price_multiplier AS open,
        b.high * factor.price_multiplier AS high,
        b.low * factor.price_multiplier AS low,
        b.close * factor.price_multiplier AS close,
        b.volume * factor.volume_multiplier AS volume,
        b.price_factor * factor.price_multiplier AS price_factor,
        b.semantics, b.lineage
      FROM {bars_sql} b
      LEFT JOIN LATERAL (
        SELECT
          coalesce(product(e.price_multiplier), 1.0) AS price_multiplier,
          coalesce(product(e.volume_multiplier), 1.0) AS volume_multiplier
        FROM {events_sql} e
        WHERE e.instrument_id = b.instrument_id AND e.session > b.trading_date
      ) factor ON TRUE
    )
    """


def read_materialized_us_total_return_rows(
    path: Path,
    *,
    instrument_ids: tuple[str, ...],
    start_ts: datetime,
    end_ts: datetime,
    after: tuple[str, str] | None,
    limit: int,
) -> list[tuple] | None:
    if len(instrument_ids) != 1:
        return None
    dataset = load_us_total_return_dataset(path, instrument_ids=instrument_ids)
    if not dataset.partitions:
        return None
    instrument_id = instrument_ids[0]
    if dataset.events:
        import pyarrow.parquet as pq

        if any(
            pq.read_table(
                str(source),
                columns=["instrument_id"],
                filters=[("instrument_id", "=", instrument_id)],
            ).num_rows
            for source in dataset.events
        ):
            return None
    if not dataset.bars:
        return []
    return read_partitioned_bar_rows(
        dataset.bars,
        instrument_id=instrument_id,
        start_ts=start_ts,
        end_ts=end_ts,
        after=after,
        limit=limit,
        adjustment="total_return",
    )


def relative_dataset_payload(
    *,
    descriptor_path: Path,
    bars: tuple[Path, ...],
    events: tuple[Path, ...],
    latest: Path | None = None,
    row_count: int | None = None,
    instrument_count: int | None = None,
    watermark: str | None = None,
) -> dict[str, object]:
    import os

    payload: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "bars": [os.path.relpath(path, descriptor_path.parent) for path in bars],
        "events": [os.path.relpath(path, descriptor_path.parent) for path in events],
    }
    if row_count is not None:
        payload["row_count"] = row_count
    if latest is not None:
        payload["latest"] = os.path.relpath(latest, descriptor_path.parent)
    if instrument_count is not None:
        payload["instrument_count"] = instrument_count
    if watermark is not None:
        payload["watermark"] = watermark
    return payload


def relative_partitioned_dataset_payload(
    *,
    descriptor_path: Path,
    partitions: tuple[MarketBarsPartition, ...],
    shared_bars: tuple[Path, ...],
    events: tuple[Path, ...],
    latest: Path | None = None,
    row_count: int | None = None,
    instrument_count: int | None = None,
    watermark: str | None = None,
) -> dict[str, object]:
    payload = relative_dataset_payload(
        descriptor_path=descriptor_path,
        bars=shared_bars,
        events=events,
        latest=latest,
        row_count=row_count,
        instrument_count=instrument_count,
        watermark=watermark,
    )
    payload["schema_version"] = PARTITIONED_MARKET_BARS_SCHEMA_VERSION
    payload["market"] = "US"
    payload["adjustment"] = "total_return"
    payload["partitions"] = partition_payload(
        descriptor_path=descriptor_path, partitions=partitions
    )
    return payload
