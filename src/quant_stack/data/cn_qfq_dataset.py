from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from pathlib import Path

from .partitioned_bars_dataset import (
    PARTITIONED_MARKET_BARS_SCHEMA_VERSION,
    MarketBarsPartition,
    parse_range_partitions,
    read_partitioned_bar_rows,
    resolve_dataset_paths,
    select_partition_bars,
)

SCHEMA_VERSION = "quantstack.cn-qfq-dataset.v1"
PARTITIONED_SCHEMA_VERSION = "quantstack.cn-qfq-partitioned-dataset.v1"


CNQFQPartition = MarketBarsPartition


@dataclass(frozen=True)
class CNQFQDataset:
    bars: tuple[Path, ...]
    events: tuple[Path, ...]
    partitions: tuple[CNQFQPartition, ...] = ()


@lru_cache(maxsize=1)
def warm_materialized_cn_qfq_runtime() -> None:
    """Initialize Arrow/Parquet in memory before the API reports ready."""
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    table = pa.table({"instrument_id": ["warmup"], "ordinal": [0]})
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    restored = pq.read_table(pa.BufferReader(sink.getvalue()))
    restored.take(pc.sort_indices(restored, sort_keys=[("instrument_id", "ascending")]))


def _sql_string(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _paths_sql(paths: tuple[Path, ...]) -> str:
    return "[" + ",".join(_sql_string(path) for path in paths) + "]"


def load_cn_qfq_dataset(
    path: Path, *, instrument_ids: tuple[str, ...] | None = None
) -> CNQFQDataset | None:
    path = path.expanduser().resolve()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    schema_version = payload.get("schema_version")
    if schema_version not in {
        SCHEMA_VERSION,
        PARTITIONED_SCHEMA_VERSION,
        PARTITIONED_MARKET_BARS_SCHEMA_VERSION,
    }:
        return None
    if schema_version == PARTITIONED_MARKET_BARS_SCHEMA_VERSION and (
        payload.get("market") != "CN" or payload.get("adjustment") != "qfq"
    ):
        return None

    def resolve(key: str, *, required: bool) -> tuple[Path, ...]:
        return resolve_dataset_paths(
            path,
            payload.get(key, []),
            required=required,
            label=f"CN qfq dataset {key}",
        )

    events = resolve("events", required=False)
    if schema_version == SCHEMA_VERSION:
        return CNQFQDataset(bars=resolve("bars", required=True), events=events)
    partitions = parse_range_partitions(
        path,
        payload.get("partitions"),
        instrument_ids=instrument_ids,
        label="CN qfq",
    )
    return CNQFQDataset(
        bars=tuple(bar for partition in partitions for bar in partition.bars),
        events=events,
        partitions=partitions,
    )


def cn_qfq_relation_sql(
    path: Path, *, instrument_ids: tuple[str, ...] | None = None
) -> str | None:
    dataset = load_cn_qfq_dataset(path, instrument_ids=instrument_ids)
    if dataset is None:
        return None
    selected_bars = select_cn_qfq_bars(dataset, instrument_ids=instrument_ids)
    if selected_bars:
        bars = (
            f"read_parquet({_paths_sql(selected_bars)}, union_by_name=true, "
            "hive_partitioning=false)"
        )
    else:
        first = dataset.bars[0]
        bars = (
            f"(SELECT * FROM read_parquet({_sql_string(first)}, "
            "hive_partitioning=false) WHERE false)"
        )
    if not dataset.events:
        return bars
    events = (
        f"read_parquet({_paths_sql(dataset.events)}, union_by_name=true, hive_partitioning=false)"
    )
    return f"""
    (
      SELECT
        b.instrument_id,b.symbol,b.venue,b.trading_date,b.ts_event,
        b.open*factor.price_multiplier AS open,
        b.high*factor.price_multiplier AS high,
        b.low*factor.price_multiplier AS low,
        b.close*factor.price_multiplier AS close,
        b.volume,b.amount,b.currency,
        struct_pack(adjustment := 'qfq') AS lineage
      FROM {bars} b
      LEFT JOIN LATERAL (
        SELECT coalesce(product(e.price_multiplier),1.0) AS price_multiplier
        FROM {events} e
        WHERE e.instrument_id=b.instrument_id AND e.session>b.trading_date
      ) factor ON TRUE
    )
    """


def select_cn_qfq_bars(
    dataset: CNQFQDataset, *, instrument_ids: tuple[str, ...] | None = None
) -> tuple[Path, ...]:
    if dataset.partitions:
        return select_partition_bars(dataset.partitions, instrument_ids=instrument_ids)
    return dataset.bars


def read_materialized_cn_qfq_rows(
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
    dataset = load_cn_qfq_dataset(path, instrument_ids=instrument_ids)
    if dataset is None or not dataset.partitions or dataset.events:
        return None
    selected_bars = select_cn_qfq_bars(dataset, instrument_ids=instrument_ids)
    if not selected_bars:
        return []

    return read_partitioned_bar_rows(
        selected_bars,
        instrument_id=instrument_ids[0],
        start_ts=start_ts,
        end_ts=end_ts,
        after=after,
        limit=limit,
        adjustment="qfq",
    )


def relative_dataset_payload(
    *, descriptor_path: Path, bars: tuple[Path, ...], events: tuple[Path, ...]
) -> dict[str, object]:
    return {
        "schema_version": SCHEMA_VERSION,
        "bars": [os.path.relpath(path, descriptor_path.parent) for path in bars],
        "events": [os.path.relpath(path, descriptor_path.parent) for path in events],
    }
