from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from quant_stack.data.batch import (
    BatchBarsQuery,
    BatchQueryMetrics,
    CanonicalBatchRepository,
    CanonicalDatasetSnapshot,
    PreparedBatchQuery,
    _decode_cursor,
)
from quant_stack.data.clustered_parquet import (
    build_clustered_cn_qfq_candidate,
    build_range_partitioned_cn_qfq_candidate,
)
from quant_stack.data.cn_qfq_dataset import (
    PARTITIONED_SCHEMA_VERSION,
    cn_qfq_relation_sql,
    load_cn_qfq_dataset,
    read_materialized_cn_qfq_rows,
    relative_dataset_payload,
)
from quant_stack.data.models import DatasetKind, DatasetLayer, DatasetManifest


class _NoPayloadDuckDB:
    def execute(self, *_args: object, **_kwargs: object) -> None:
        raise AssertionError("materialized single-instrument path used DuckDB payload SQL")

    def close(self) -> None:
        pass


def _prepared_batch_plan(
    descriptor: Path,
    *,
    after: tuple[str, ...] | None = None,
    connection: Any | None = None,
) -> PreparedBatchQuery:
    manifest = DatasetManifest(
        dataset_id="cn_equity_daily",
        version="candidate-v1",
        layer=DatasetLayer.NORMALIZED,
        kind=DatasetKind.MARKET_BARS,
        market="CN",
        provider="fixture",
        frequency="1d",
        schema_ref="fixture",
        storage_uri="quant-data://normalized/candidate/bars.json",
        available_at_policy="fixture",
        revision_policy="immutable",
        row_count=36,
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
        connection=connection or _NoPayloadDuckDB(),
        metrics=BatchQueryMetrics(catalog_queries=1, duckdb_connections=1),
        publication_receipt=None,  # type: ignore[arg-type]
    )


def _fixture(tmp_path: Path) -> Path:
    bars = tmp_path / "source/bars.parquet"
    events = tmp_path / "source/events.parquet"
    descriptor = tmp_path / "source/bars.json"
    bars.parent.mkdir(parents=True)
    rows: list[dict[str, object]] = []
    for offset in range(12):
        session = date(2026, 8, 1) + timedelta(days=offset)
        for instrument_id, close in (
            ("XSHE.002963", 20.0),
            ("XSHG.600000", 10.0),
            ("XSHE.000001", 15.0),
        ):
            rows.append(
                {
                    "instrument_id": instrument_id,
                    "symbol": instrument_id.split(".", 1)[1],
                    "venue": instrument_id.split(".", 1)[0],
                    "trading_date": session,
                    "ts_event": datetime(session.year, session.month, session.day, 7, tzinfo=UTC),
                    "open": close,
                    "high": close + 1,
                    "low": close - 1,
                    "close": close,
                    "volume": 100.0,
                    "amount": 1000.0,
                    "currency": "CNY",
                    "lineage": {"adjustment": "qfq"},
                }
            )
    pq.write_table(pa.Table.from_pylist(list(reversed(rows))), bars, compression="zstd")
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "instrument_id": "XSHG.600000",
                    "session": date(2026, 8, 12),
                    "price_multiplier": 0.5,
                }
            ]
        ),
        events,
        compression="zstd",
    )
    descriptor.write_text(
        json.dumps(
            relative_dataset_payload(
                descriptor_path=descriptor,
                bars=(bars,),
                events=(events,),
            )
        ),
        encoding="utf-8",
    )
    return descriptor


def test_clustered_candidate_is_immutable_equivalent_and_self_contained(tmp_path: Path) -> None:
    source = _fixture(tmp_path)
    result = build_clustered_cn_qfq_candidate(
        source_descriptor=source,
        output_root=tmp_path / "isolated",
        version="candidate-v1",
        row_group_size=2048,
    )

    assert result.created is True
    assert result.row_count == 36
    assert result.instrument_count == 3
    assert result.row_group_count == 1
    evidence = json.loads(result.report.read_text(encoding="utf-8"))
    assert evidence["rows_equal"] is True
    assert evidence["schema_equal"] is True
    assert evidence["baseline_isolated_copy"] is True
    clustered = json.loads(result.clustered_descriptor.read_text(encoding="utf-8"))
    baseline = json.loads(result.baseline_descriptor.read_text(encoding="utf-8"))
    assert clustered["events"] == ["events/part-0000.parquet"]
    assert baseline["bars"] == ["bars/part-0000.parquet"]

    connection = duckdb.connect()
    try:
        source_rows = connection.execute(
            f"SELECT * FROM {cn_qfq_relation_sql(source)} ORDER BY instrument_id,ts_event"
        ).fetchall()
        candidate_rows = connection.execute(
            f"SELECT * FROM {cn_qfq_relation_sql(result.clustered_descriptor)} "
            "ORDER BY instrument_id,ts_event"
        ).fetchall()
    finally:
        connection.close()
    assert candidate_rows == source_rows

    replay = build_clustered_cn_qfq_candidate(
        source_descriptor=source,
        output_root=tmp_path / "isolated",
        version="candidate-v1",
        row_group_size=2048,
    )
    assert replay.created is False


def test_clustered_candidate_rejects_invalid_row_group_size(tmp_path: Path) -> None:
    source = _fixture(tmp_path)
    try:
        build_clustered_cn_qfq_candidate(
            source_descriptor=source,
            output_root=tmp_path / "isolated",
            version="candidate-v1",
            row_group_size=3000,
        )
    except ValueError as exc:
        assert "multiple of 2048" in str(exc)
    else:
        raise AssertionError("invalid row-group size was accepted")


def test_range_partitioned_candidate_selects_only_the_requested_instrument_range(
    tmp_path: Path,
) -> None:
    source = _fixture(tmp_path)
    result = build_range_partitioned_cn_qfq_candidate(
        source_descriptor=source,
        output_root=tmp_path / "partitioned",
        version="candidate-v1",
        instruments_per_partition=2,
        row_group_size=2048,
    )

    assert result.created is True
    assert result.partition_count == 2
    descriptor = json.loads(result.descriptor.read_text(encoding="utf-8"))
    assert descriptor["schema_version"] == "quantstack.partitioned-market-bars-dataset.v1"
    assert descriptor["market"] == "CN"
    assert descriptor["adjustment"] == "qfq"
    relation = cn_qfq_relation_sql(result.descriptor, instrument_ids=("XSHE.002963",))
    assert relation is not None
    selected = next(
        item
        for item in descriptor["partitions"]
        if item["first_instrument_id"] <= "XSHE.002963" <= item["last_instrument_id"]
    )
    assert selected["bars"][0] in relation
    assert sum(item["bars"][0] in relation for item in descriptor["partitions"]) == 1
    connection = duckdb.connect()
    try:
        rows = connection.execute(
            f"SELECT instrument_id,count(*) FROM {relation} WHERE instrument_id=? "
            "GROUP BY instrument_id",
            ["XSHE.002963"],
        ).fetchall()
    finally:
        connection.close()
    assert rows == [("XSHE.002963", 12)]

    legacy = result.descriptor.with_name("legacy-partitioned.json")
    legacy.write_text(
        json.dumps(
            {
                **descriptor,
                "schema_version": PARTITIONED_SCHEMA_VERSION,
                "market": None,
                "adjustment": None,
            }
        ),
        encoding="utf-8",
    )
    assert load_cn_qfq_dataset(legacy, instrument_ids=("XSHE.002963",)) is not None


def test_partitioned_request_validates_only_its_selected_range(tmp_path: Path) -> None:
    source = _fixture(tmp_path)
    result = build_range_partitioned_cn_qfq_candidate(
        source_descriptor=source,
        output_root=tmp_path / "partitioned",
        version="candidate-v1",
        instruments_per_partition=2,
        row_group_size=2048,
    )
    descriptor = json.loads(result.descriptor.read_text(encoding="utf-8"))
    selected = next(
        item
        for item in descriptor["partitions"]
        if item["first_instrument_id"] <= "XSHE.002963" <= item["last_instrument_id"]
    )
    unrelated = next(item for item in descriptor["partitions"] if item is not selected)
    unrelated_path = (result.descriptor.parent / unrelated["bars"][0]).resolve()
    unrelated_path.unlink()

    relation = cn_qfq_relation_sql(result.descriptor, instrument_ids=("XSHE.002963",))
    assert relation is not None
    assert selected["bars"][0] in relation
    with pytest.raises(ValueError, match="partition source is missing"):
        load_cn_qfq_dataset(result.descriptor)


def test_materialized_partitioned_candidate_removes_read_time_event_join(
    tmp_path: Path,
) -> None:
    source = _fixture(tmp_path)
    result = build_range_partitioned_cn_qfq_candidate(
        source_descriptor=source,
        output_root=tmp_path / "materialized",
        version="candidate-v1",
        instruments_per_partition=2,
        row_group_size=2048,
        materialize_adjustments=True,
    )
    payload = json.loads(result.descriptor.read_text(encoding="utf-8"))
    evidence = json.loads(result.report.read_text(encoding="utf-8"))
    assert payload["events"] == []
    assert evidence["materialized_adjustments"] is True

    source_relation = cn_qfq_relation_sql(source, instrument_ids=("XSHG.600000",))
    candidate_relation = cn_qfq_relation_sql(result.descriptor, instrument_ids=("XSHG.600000",))
    assert source_relation is not None
    assert candidate_relation is not None
    connection = duckdb.connect()
    try:
        source_rows = connection.execute(
            f"SELECT * FROM {source_relation} WHERE instrument_id=? ORDER BY ts_event",
            ["XSHG.600000"],
        ).fetchall()
        candidate_rows = connection.execute(
            f"SELECT * FROM {candidate_relation} WHERE instrument_id=? ORDER BY ts_event",
            ["XSHG.600000"],
        ).fetchall()
    finally:
        connection.close()
    assert candidate_rows == source_rows

    fast_rows = read_materialized_cn_qfq_rows(
        result.descriptor,
        instrument_ids=("XSHG.600000",),
        start_ts=datetime(2026, 8, 1, 0, tzinfo=UTC),
        end_ts=datetime(2026, 8, 12, 23, 59, tzinfo=UTC),
        after=None,
        limit=100,
    )
    assert fast_rows is not None
    assert len(fast_rows) == 12
    assert fast_rows[0][0] == "XSHG.600000"
    assert fast_rows[0][-1] == "qfq"
    paged_rows = read_materialized_cn_qfq_rows(
        result.descriptor,
        instrument_ids=("XSHG.600000",),
        start_ts=datetime(2026, 8, 1, 0, tzinfo=UTC),
        end_ts=datetime(2026, 8, 12, 23, 59, tzinfo=UTC),
        after=("XSHG.600000", fast_rows[0][1].isoformat()),
        limit=2,
    )
    assert paged_rows == fast_rows[1:3]
    assert (
        read_materialized_cn_qfq_rows(
            result.descriptor,
            instrument_ids=("XSHG.600000", "XSHE.002963"),
            start_ts=datetime(2026, 8, 1, 0, tzinfo=UTC),
            end_ts=datetime(2026, 8, 12, 23, 59, tzinfo=UTC),
            after=None,
            limit=100,
        )
        is None
    )


def test_materialized_cn_fast_path_preserves_batch_paging_cursor_and_metrics(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = _fixture(tmp_path)
    result = build_range_partitioned_cn_qfq_candidate(
        source_descriptor=source,
        output_root=tmp_path / "materialized",
        version="candidate-v1",
        instruments_per_partition=2,
        row_group_size=2048,
        materialize_adjustments=True,
    )
    repository = CanonicalBatchRepository(tmp_path)

    def reject_duckdb_payload(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("fast path fell back to DuckDB payload SQL")

    monkeypatch.setattr(repository, "_payload_rows", reject_duckdb_payload)
    query = BatchBarsQuery(
        instrument_ids=("XSHG.600000",),
        market="CN",
        start=date(2026, 8, 1),
        end=date(2026, 8, 12),
        page_size=3,
    )
    first_plan = _prepared_batch_plan(result.descriptor)
    first = repository.execute_bars(first_plan, query)

    assert [item.ts_event for item in first.items] == sorted(item.ts_event for item in first.items)
    assert len(first.items) == 3
    assert first.next_cursor is not None
    decoded = _decode_cursor(first.next_cursor)
    assert decoded == {
        "v": 1,
        "snapshot_token": first_plan.snapshot_token,
        "query_sha256": first_plan.query_hash,
        "after": [first.items[-1].instrument_id, first.items[-1].ts_event.isoformat()],
    }
    assert first.dataset_id == "cn_equity_daily"
    assert first.dataset_version == "candidate-v1"
    assert first.quality_status == "passed"
    assert first.manifest_fingerprint == "e" * 64
    assert first_plan.metrics.catalog_queries == 1
    assert first_plan.metrics.payload_queries == 1
    assert first_plan.metrics.duckdb_connections == 1
    assert first_plan.metrics.parquet_scans == 1

    second_plan = _prepared_batch_plan(
        result.descriptor,
        after=tuple(str(value) for value in decoded["after"]),
    )
    second = repository.execute_bars(second_plan, query)
    assert len(second.items) == 3
    assert second.items[0].ts_event > first.items[-1].ts_event
    assert second.next_cursor is not None
    assert second_plan.metrics.payload_queries == 1
    assert second_plan.metrics.duckdb_connections == 1
    assert second_plan.metrics.parquet_scans == 1


def test_materialized_cn_batch_falls_back_for_multi_legacy_and_event_descriptors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = _fixture(tmp_path)
    result = build_range_partitioned_cn_qfq_candidate(
        source_descriptor=source,
        output_root=tmp_path / "materialized",
        version="candidate-v1",
        instruments_per_partition=2,
        row_group_size=2048,
        materialize_adjustments=True,
    )
    source_payload = json.loads(source.read_text(encoding="utf-8"))
    legacy = source.with_name("legacy-bars.json")
    legacy.write_text(
        json.dumps({**source_payload, "events": []}),
        encoding="utf-8",
    )
    repository = CanonicalBatchRepository(tmp_path)
    original_payload_rows = repository._payload_rows  # noqa: SLF001
    fallback_calls: list[Path] = []

    def track_fallback(plan, sql: str, parameters: list[object]):
        fallback_calls.append(plan.payload_path)
        return original_payload_rows(plan, sql, parameters)

    monkeypatch.setattr(repository, "_payload_rows", track_fallback)
    cases = (
        (result.descriptor, ("XSHG.600000", "XSHE.002963")),
        (legacy, ("XSHG.600000",)),
        (source, ("XSHG.600000",)),
    )
    for descriptor, instrument_ids in cases:
        connection = duckdb.connect()
        plan = _prepared_batch_plan(descriptor, connection=connection)
        query = BatchBarsQuery(
            instrument_ids=instrument_ids,
            market="CN",
            start=date(2026, 8, 1),
            end=date(2026, 8, 12),
            page_size=100,
        )
        try:
            response = repository.execute_bars(plan, query)
        finally:
            plan.close()
        assert response.items
        assert plan.metrics.payload_queries == 1
        assert plan.metrics.parquet_scans == 1
        assert plan.metrics.duckdb_connections == 1

    assert fallback_calls == [case[0] for case in cases]


def test_partitioned_cn_event_descriptor_falls_back_with_paging_and_metrics(
    tmp_path: Path,
) -> None:
    source = _fixture(tmp_path)
    result = build_range_partitioned_cn_qfq_candidate(
        source_descriptor=source,
        output_root=tmp_path / "partitioned-events",
        version="candidate-v1",
        instruments_per_partition=2,
        row_group_size=2048,
    )
    descriptor = json.loads(result.descriptor.read_text(encoding="utf-8"))
    assert descriptor["schema_version"] == "quantstack.partitioned-market-bars-dataset.v1"
    assert descriptor["market"] == "CN"
    assert descriptor["events"]

    repository = CanonicalBatchRepository(tmp_path)
    query = BatchBarsQuery(
        instrument_ids=("XSHG.600000",),
        market="CN",
        start=date(2026, 8, 1),
        end=date(2026, 8, 12),
        page_size=5,
    )
    first_connection = duckdb.connect()
    first_plan = _prepared_batch_plan(result.descriptor, connection=first_connection)
    try:
        first = repository.execute_bars(first_plan, query)
    finally:
        first_plan.close()
    assert len(first.items) == 5
    assert first.next_cursor is not None
    assert {item.adjustment_semantics for item in first.items} == {"qfq"}
    assert first_plan.metrics.payload_queries == 1
    assert first_plan.metrics.parquet_scans == 1
    assert first_plan.metrics.duckdb_connections == 1

    decoded: dict[str, Any] = _decode_cursor(first.next_cursor)
    second_connection = duckdb.connect()
    second_plan = _prepared_batch_plan(
        result.descriptor,
        after=tuple(str(value) for value in decoded["after"]),
        connection=second_connection,
    )
    try:
        second = repository.execute_bars(second_plan, query)
    finally:
        second_plan.close()
    assert len(second.items) == 5
    assert second.items[0].ts_event > first.items[-1].ts_event
    assert second.next_cursor is not None
    assert second_plan.metrics.payload_queries == 1
    assert second_plan.metrics.parquet_scans == 1
    assert second_plan.metrics.duckdb_connections == 1
