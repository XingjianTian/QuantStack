from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from quant_stack.data.batch import (
    BatchBarsQuery,
    BatchQueryMetrics,
    CanonicalBatchRepository,
    CanonicalDatasetSnapshot,
    PreparedBatchQuery,
    _decode_cursor,
)
from quant_stack.data.clustered_parquet import (
    build_range_partitioned_us_total_return_candidate,
)
from quant_stack.data.models import DatasetKind, DatasetLayer, DatasetManifest
from quant_stack.data.partitioned_bars_dataset import (
    PARTITIONED_MARKET_BARS_SCHEMA_VERSION,
)
from quant_stack.data.us_adjustment_incremental import materialize_us_total_return_incremental
from quant_stack.data.us_total_return_dataset import (
    load_us_total_return_dataset,
    read_materialized_us_total_return_rows,
    us_total_return_relation_sql,
)


class _NoPayloadDuckDB:
    def execute(self, *_args: object, **_kwargs: object) -> None:
        raise AssertionError("partitioned single-instrument path used DuckDB payload SQL")

    def close(self) -> None:
        pass


def _source(tmp_path: Path) -> Path:
    path = tmp_path / "source/bars.parquet"
    path.parent.mkdir(parents=True)
    rows: list[dict[str, object]] = []
    for offset in range(4):
        session = date(2026, 8, 20) + timedelta(days=offset)
        for instrument_id, close in (
            ("XNAS.AAA", 10.0),
            ("XNAS.BBB", 20.0),
            ("XNYS.CCC", 30.0),
            ("XNYS.DDD", 40.0),
        ):
            venue, symbol = instrument_id.split(".", maxsplit=1)
            rows.append(
                {
                    "instrument_id": instrument_id,
                    "symbol": symbol,
                    "venue": venue,
                    "trading_date": session,
                    "ts_event": datetime(session.year, session.month, session.day, 20, tzinfo=UTC),
                    "amount": 1000.0,
                    "currency": "USD",
                    "open": close - 1,
                    "high": close + 1,
                    "low": close - 2,
                    "close": close,
                    "volume": 100.0,
                    "price_factor": 1.0,
                    "semantics": "provider_split_basis_plus_cash_dividend_total_return",
                    "lineage": {"adjustment": "total_return"},
                }
            )
    pq.write_table(pa.Table.from_pylist(list(reversed(rows))), path, compression="zstd")
    return path


def _candidate(tmp_path: Path):
    return build_range_partitioned_us_total_return_candidate(
        source_descriptor=_source(tmp_path),
        output_root=tmp_path / "partitioned",
        version="candidate-v1",
        instruments_per_partition=2,
        row_group_size=2048,
    )


def _plan(
    descriptor: Path,
    *,
    after: tuple[str, ...] | None = None,
    connection: Any | None = None,
) -> PreparedBatchQuery:
    manifest = DatasetManifest(
        dataset_id="us_equity_total_return",
        version="candidate-v1",
        layer=DatasetLayer.NORMALIZED,
        kind=DatasetKind.MARKET_BARS,
        market="US",
        provider="fixture",
        frequency="1d",
        schema_ref="fixture",
        storage_uri="quant-data://normalized/candidate/bars.json",
        available_at_policy="fixture",
        revision_policy="immutable",
        row_count=16,
        sha256="f" * 64,
        quality_status="passed",
        license_scope="test",
    )
    return PreparedBatchQuery(
        operation="batch_bars",
        snapshot=CanonicalDatasetSnapshot(manifest, "e" * 64),
        snapshot_token="snapshot-token",
        etag='"fixture-etag"',
        query_hash="query-hash",
        after=after,
        payload_path=descriptor,
        connection=connection or _NoPayloadDuckDB(),  # type: ignore[arg-type]
        metrics=BatchQueryMetrics(catalog_queries=1, duckdb_connections=1),
        publication_receipt=None,  # type: ignore[arg-type]
    )


def test_us_range_candidate_selects_one_partition_and_preserves_relation(
    tmp_path: Path,
) -> None:
    source = _source(tmp_path)
    result = build_range_partitioned_us_total_return_candidate(
        source_descriptor=source,
        output_root=tmp_path / "partitioned",
        version="candidate-v1",
        instruments_per_partition=2,
        row_group_size=2048,
    )
    descriptor = json.loads(result.descriptor.read_text(encoding="utf-8"))
    assert descriptor["schema_version"] == PARTITIONED_MARKET_BARS_SCHEMA_VERSION
    assert descriptor["market"] == "US"
    assert descriptor["adjustment"] == "total_return"
    assert result.partition_count == 2
    assert descriptor["bars"] == []
    assert descriptor["events"] == []

    selected_relation = us_total_return_relation_sql(
        result.descriptor, instrument_ids=("XNAS.BBB",)
    )
    selected = next(
        item
        for item in descriptor["partitions"]
        if item["first_instrument_id"] <= "XNAS.BBB" <= item["last_instrument_id"]
    )
    assert selected["bars"][0] in selected_relation
    assert sum(item["bars"][0] in selected_relation for item in descriptor["partitions"]) == 1

    connection = duckdb.connect()
    try:
        source_rows = connection.execute(
            f"SELECT * FROM {us_total_return_relation_sql(source)} ORDER BY instrument_id,ts_event"
        ).fetchall()
        candidate_rows = connection.execute(
            f"SELECT * FROM {us_total_return_relation_sql(result.descriptor)} "
            "ORDER BY instrument_id,ts_event"
        ).fetchall()
    finally:
        connection.close()
    assert candidate_rows == source_rows


def test_us_partition_validation_and_shared_reader_match_cn_contract(tmp_path: Path) -> None:
    result = _candidate(tmp_path)
    descriptor = json.loads(result.descriptor.read_text(encoding="utf-8"))
    selected = next(
        item
        for item in descriptor["partitions"]
        if item["first_instrument_id"] <= "XNAS.BBB" <= item["last_instrument_id"]
    )
    unrelated = next(item for item in descriptor["partitions"] if item is not selected)
    (result.descriptor.parent / unrelated["bars"][0]).unlink()

    dataset = load_us_total_return_dataset(result.descriptor, instrument_ids=("XNAS.BBB",))
    assert len(dataset.bars) == 1
    rows = read_materialized_us_total_return_rows(
        result.descriptor,
        instrument_ids=("XNAS.BBB",),
        start_ts=datetime(2026, 8, 20, tzinfo=UTC),
        end_ts=datetime(2026, 8, 30, 23, 59, tzinfo=UTC),
        after=None,
        limit=100,
    )
    assert rows is not None
    assert len(rows) == 4
    assert {row[-1] for row in rows} == {"total_return"}


def test_us_partitioned_batch_preserves_paging_cursor_and_metrics(tmp_path: Path) -> None:
    result = _candidate(tmp_path)
    repository = CanonicalBatchRepository(tmp_path)
    query = BatchBarsQuery(
        instrument_ids=("XNAS.BBB",),
        market="US",
        start=date(2026, 8, 20),
        end=date(2026, 8, 30),
        page_size=2,
    )
    first_plan = _plan(result.descriptor)
    first = repository.execute_bars(first_plan, query)
    assert len(first.items) == 2
    assert first.next_cursor is not None
    decoded: dict[str, Any] = _decode_cursor(first.next_cursor)
    assert decoded["after"] == [
        first.items[-1].instrument_id,
        first.items[-1].ts_event.isoformat(),
    ]
    assert {item.adjustment_semantics for item in first.items} == {"total_return"}
    assert first_plan.metrics.payload_queries == 1
    assert first_plan.metrics.parquet_scans == 1

    second_plan = _plan(result.descriptor, after=tuple(str(value) for value in decoded["after"]))
    second = repository.execute_bars(second_plan, query)
    assert len(second.items) == 2
    assert second.items[0].ts_event > first.items[-1].ts_event


def test_us_incremental_keeps_partitions_and_appends_shared_shards(tmp_path: Path) -> None:
    base = _candidate(tmp_path)
    output = tmp_path / "incremental/bars.json"
    report = tmp_path / "incremental/evidence.json"
    result = materialize_us_total_return_incremental(
        base_bars_path=base.descriptor,
        daily_records=[
            {
                "instrument_id": "XNAS.BBB",
                "Date": "2026-08-24",
                "Open": 20.0,
                "High": 22.0,
                "Low": 19.0,
                "Close": 21.0,
                "Adj Close": 21.0,
                "Volume": 120.0,
                "Dividends": 0.0,
                "Stock Splits": 0.0,
            }
        ],
        session=date(2026, 8, 24),
        output_path=output,
        report_path=report,
        base_dataset_version="candidate-v1",
    )
    assert result.row_count == 17
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["schema_version"] == PARTITIONED_MARKET_BARS_SCHEMA_VERSION
    base_dataset = load_us_total_return_dataset(base.descriptor)
    incremental_dataset = load_us_total_return_dataset(output)
    assert [
        (item.first_instrument_id, item.last_instrument_id, item.bars)
        for item in incremental_dataset.partitions
    ] == [
        (item.first_instrument_id, item.last_instrument_id, item.bars)
        for item in base_dataset.partitions
    ]
    assert len(payload["bars"]) == 1
    assert payload["events"] == []

    rows = read_materialized_us_total_return_rows(
        output,
        instrument_ids=("XNAS.BBB",),
        start_ts=datetime(2026, 8, 20, tzinfo=UTC),
        end_ts=datetime(2026, 8, 24, 23, 59, tzinfo=UTC),
        after=None,
        limit=100,
    )
    assert rows is not None
    assert len(rows) == 5
    assert rows[-1][5] == 21.0


def test_us_event_series_falls_back_while_unaffected_series_stays_fast(
    tmp_path: Path,
) -> None:
    base = _candidate(tmp_path)
    output = tmp_path / "event-incremental/bars.json"
    materialize_us_total_return_incremental(
        base_bars_path=base.descriptor,
        daily_records=[
            {
                "instrument_id": "XNAS.BBB",
                "Date": "2026-08-24",
                "Open": 19.0,
                "High": 21.0,
                "Low": 18.0,
                "Close": 20.0,
                "Adj Close": 20.0,
                "Volume": 120.0,
                "Dividends": 1.0,
                "Stock Splits": 0.0,
            }
        ],
        session=date(2026, 8, 24),
        output_path=output,
        report_path=tmp_path / "event-incremental/evidence.json",
        base_dataset_version="candidate-v1",
    )
    bounds = {
        "start_ts": datetime(2026, 8, 20, tzinfo=UTC),
        "end_ts": datetime(2026, 8, 24, 23, 59, tzinfo=UTC),
        "after": None,
        "limit": 100,
    }
    assert (
        read_materialized_us_total_return_rows(output, instrument_ids=("XNAS.BBB",), **bounds)
        is None
    )
    unaffected = read_materialized_us_total_return_rows(
        output, instrument_ids=("XNAS.AAA",), **bounds
    )
    assert unaffected is not None
    assert len(unaffected) == 4

    repository = CanonicalBatchRepository(tmp_path)
    query = BatchBarsQuery(
        instrument_ids=("XNAS.BBB",),
        market="US",
        start=date(2026, 8, 20),
        end=date(2026, 8, 24),
        page_size=3,
    )
    connection = duckdb.connect()
    plan = _plan(output, connection=connection)
    try:
        first = repository.execute_bars(plan, query)
    finally:
        plan.close()
    assert len(first.items) == 3
    assert first.next_cursor is not None
    assert {item.adjustment_semantics for item in first.items} == {"total_return"}
    assert plan.metrics.payload_queries == 1
    assert plan.metrics.parquet_scans == 1
    assert plan.metrics.duckdb_connections == 1
