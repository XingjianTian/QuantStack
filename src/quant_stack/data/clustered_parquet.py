from __future__ import annotations

import json
import os
import shutil
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from .cn_qfq_dataset import (
    cn_qfq_relation_sql,
    load_cn_qfq_dataset,
    relative_dataset_payload,
)
from .partitioned_bars_dataset import (
    PARTITIONED_MARKET_BARS_SCHEMA_VERSION,
    MarketBarsPartition,
)
from .us_total_return_dataset import (
    load_us_total_return_dataset,
    relative_partitioned_dataset_payload,
    us_total_return_relation_sql,
)

REPORT_SCHEMA = "quantstack.clustered-cn-qfq-candidate.v1"


def _sql_string(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


@dataclass(frozen=True)
class ClusteredCandidateResult:
    root: Path
    clustered_descriptor: Path
    baseline_descriptor: Path
    report: Path
    created: bool
    row_count: int
    instrument_count: int
    row_group_count: int


@dataclass(frozen=True)
class RangePartitionedCandidateResult:
    root: Path
    descriptor: Path
    report: Path
    created: bool
    row_count: int
    instrument_count: int
    partition_count: int


@contextmanager
def _bounded_connection(scratch_parent: Path, *, memory_limit: str = "512MB"):
    with tempfile.TemporaryDirectory(
        prefix=".clustered-qfq-duckdb-", dir=scratch_parent
    ) as temporary:
        connection = duckdb.connect()
        try:
            connection.execute("SET memory_limit=?", [memory_limit])
            connection.execute("SET threads=2")
            connection.execute("SET preserve_insertion_order=false")
            connection.execute("SET temp_directory=?", [temporary])
            yield connection
        finally:
            connection.close()


def _dataset_profile(connection, source: str | list[str]) -> tuple:
    return connection.execute(
        "SELECT count(*),count(DISTINCT instrument_id),"
        "count(DISTINCT (instrument_id,trading_date)),min(trading_date),max(trading_date),"
        "count(*) FILTER (WHERE instrument_id IS NULL OR ts_event IS NULL) "
        "FROM read_parquet(?,union_by_name=true,hive_partitioning=false)",
        [source],
    ).fetchone()


def _schema(connection, source: str | list[str]) -> list[tuple]:
    return connection.execute(
        "DESCRIBE SELECT * FROM read_parquet(?,union_by_name=true,hive_partitioning=false)",
        [source],
    ).fetchall()


def _assert_relation_equivalent(
    connection, source_relation: str, candidate: Path
) -> None:
    candidate_sql = (
        f"read_parquet({_sql_string(candidate)},union_by_name=true,"
        "hive_partitioning=false)"
    )
    source_schema = connection.execute(
        f"DESCRIBE SELECT * FROM {source_relation}"
    ).fetchall()
    candidate_schema = connection.execute(
        f"DESCRIBE SELECT * FROM {candidate_sql}"
    ).fetchall()
    if source_schema != candidate_schema:
        raise ValueError("materialized candidate schema differs from qfq relation")
    instruments = [
        row[0]
        for row in connection.execute(
            f"SELECT DISTINCT instrument_id FROM {candidate_sql} ORDER BY instrument_id"
        ).fetchall()
    ]
    for offset in range(0, len(instruments), 64):
        lower = _sql_string(instruments[offset])
        upper = _sql_string(instruments[min(offset + 63, len(instruments) - 1)])
        changed = connection.execute(
            "SELECT EXISTS((SELECT * FROM "
            f"{source_relation} WHERE instrument_id BETWEEN {lower} AND {upper} EXCEPT ALL "
            f"SELECT * FROM {candidate_sql} WHERE instrument_id BETWEEN {lower} AND {upper}) "
            "UNION ALL (SELECT * FROM "
            f"{candidate_sql} WHERE instrument_id BETWEEN {lower} AND {upper} EXCEPT ALL "
            f"SELECT * FROM {source_relation} WHERE instrument_id BETWEEN {lower} AND {upper}))"
        ).fetchone()
        if changed != (False,):
            raise ValueError(
                "materialized candidate rows differ from qfq relation: "
                f"{instruments[offset]} through "
                f"{instruments[min(offset + 63, len(instruments) - 1)]}"
            )


def _assert_equivalent(
    connection, source_paths: list[str], candidate: Path | list[str]
) -> None:
    candidate_source: str | list[str] = (
        str(candidate) if isinstance(candidate, Path) else candidate
    )
    if _schema(connection, source_paths) != _schema(connection, candidate_source):
        raise ValueError("clustered candidate schema differs from source")
    instruments = [
        row[0]
        for row in connection.execute(
            "SELECT instrument_id FROM read_parquet(?,union_by_name=true,hive_partitioning=false) "
            "UNION SELECT instrument_id FROM read_parquet(?,hive_partitioning=false) "
            "ORDER BY instrument_id",
            [source_paths, candidate_source],
        ).fetchall()
    ]
    for offset in range(0, len(instruments), 64):
        lower = instruments[offset]
        upper = instruments[min(offset + 63, len(instruments) - 1)]
        changed = connection.execute(
            "SELECT EXISTS((SELECT * FROM read_parquet(?,union_by_name=true,"
            "hive_partitioning=false) WHERE instrument_id BETWEEN ? AND ? EXCEPT ALL "
            "SELECT * FROM read_parquet(?,hive_partitioning=false) WHERE instrument_id "
            "BETWEEN ? AND ?) UNION ALL (SELECT * FROM read_parquet(?,hive_partitioning=false) "
            "WHERE instrument_id BETWEEN ? AND ? EXCEPT ALL SELECT * FROM read_parquet(?,"
            "union_by_name=true,hive_partitioning=false) WHERE instrument_id BETWEEN ? AND ?))",
            [
                source_paths,
                lower,
                upper,
                candidate_source,
                lower,
                upper,
                candidate_source,
                lower,
                upper,
                source_paths,
                lower,
                upper,
            ],
        ).fetchone()
        if changed != (False,):
            raise ValueError(
                f"clustered candidate rows differ from source: {lower} through {upper}"
            )


def _copy_dataset_files(paths: tuple[Path, ...], destination: Path) -> tuple[Path, ...]:
    destination.mkdir(parents=True)
    copied: list[Path] = []
    for index, source in enumerate(paths):
        target = destination / f"part-{index:04d}.parquet"
        shutil.copyfile(source, target)
        copied.append(target)
    return tuple(copied)


def _descriptor(
    path: Path, *, bars: tuple[Path, ...], events: tuple[Path, ...]
) -> None:
    path.write_text(
        json.dumps(
            relative_dataset_payload(descriptor_path=path, bars=bars, events=events),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def _row_group_proof(path: Path) -> dict[str, object]:
    parquet = pq.ParquetFile(path)
    instrument_index = parquet.schema_arrow.names.index("instrument_id")
    ranges: list[tuple[str, str]] = []
    for index in range(parquet.metadata.num_row_groups):
        stats = parquet.metadata.row_group(index).column(instrument_index).statistics
        if stats is None or stats.min is None or stats.max is None:
            raise ValueError("clustered candidate lacks instrument_id row-group statistics")
        ranges.append((str(stats.min), str(stats.max)))
    if ranges != sorted(ranges):
        raise ValueError("clustered candidate row groups are not ordered by instrument_id")
    return {
        "row_group_count": len(ranges),
        "first_instrument": ranges[0][0],
        "last_instrument": ranges[-1][1],
    }


def build_clustered_cn_qfq_candidate(
    *, source_descriptor: Path, output_root: Path, version: str, row_group_size: int = 32768
) -> ClusteredCandidateResult:
    source_descriptor = source_descriptor.expanduser().resolve()
    output_root = output_root.expanduser().resolve()
    if row_group_size < 2048 or row_group_size % 2048:
        raise ValueError("row_group_size must be a multiple of 2048")
    dataset = load_cn_qfq_dataset(source_descriptor)
    if dataset is None:
        raise ValueError("source is not a CN qfq descriptor")
    target = output_root / f"version={version}"
    report_path = target / "evidence.json"
    if target.exists():
        payload = json.loads(report_path.read_text(encoding="utf-8"))
        if payload.get("schema_version") != REPORT_SCHEMA:
            raise ValueError("existing clustered candidate has incompatible evidence")
        return ClusteredCandidateResult(
            root=target,
            clustered_descriptor=target / "clustered/bars.json",
            baseline_descriptor=target / "baseline/bars.json",
            report=report_path,
            created=False,
            row_count=int(payload["row_count"]),
            instrument_count=int(payload["instrument_count"]),
            row_group_count=int(payload["row_group_count"]),
        )

    output_root.mkdir(parents=True, exist_ok=True)
    source_paths = [str(path) for path in dataset.bars]
    source_facts = [(path.stat().st_size, path.stat().st_mtime_ns) for path in dataset.bars]
    with tempfile.TemporaryDirectory(prefix=f".{target.name}.", dir=output_root) as temporary:
        candidate_root = Path(temporary) / "candidate"
        clustered_root = candidate_root / "clustered"
        baseline_root = candidate_root / "baseline"
        clustered_root.mkdir(parents=True)
        baseline_root.mkdir(parents=True)
        clustered_bars = clustered_root / "bars.parquet"
        with _bounded_connection(candidate_root) as connection:
            connection.execute(
                "COPY (SELECT * FROM read_parquet(?,union_by_name=true,hive_partitioning=false) "
                f"ORDER BY instrument_id,ts_event) TO {_sql_string(clustered_bars)} "
                f"(FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE {row_group_size})",
                [source_paths],
            )
            source_profile = _dataset_profile(connection, source_paths)
            candidate_profile = _dataset_profile(connection, str(clustered_bars))
            if source_profile != candidate_profile:
                raise ValueError("clustered candidate profile differs from source")
            if source_profile[0] != source_profile[2] or source_profile[5] != 0:
                raise ValueError("source CN qfq descriptor has invalid keys")
            _assert_equivalent(connection, source_paths, clustered_bars)
        if source_facts != [
            (path.stat().st_size, path.stat().st_mtime_ns) for path in dataset.bars
        ]:
            raise ValueError("source CN qfq files changed during candidate generation")

        clustered_events = _copy_dataset_files(dataset.events, clustered_root / "events")
        baseline_bars = _copy_dataset_files(dataset.bars, baseline_root / "bars")
        baseline_events = _copy_dataset_files(dataset.events, baseline_root / "events")
        clustered_descriptor = clustered_root / "bars.json"
        baseline_descriptor = baseline_root / "bars.json"
        _descriptor(
            clustered_descriptor,
            bars=(clustered_bars,),
            events=clustered_events,
        )
        _descriptor(baseline_descriptor, bars=baseline_bars, events=baseline_events)
        row_group_proof = _row_group_proof(clustered_bars)
        evidence = {
            "schema_version": REPORT_SCHEMA,
            "version": version,
            "row_group_size": row_group_size,
            "row_count": int(source_profile[0]),
            "instrument_count": int(source_profile[1]),
            "duplicate_keys": 0,
            "first_date": source_profile[3].isoformat(),
            "last_date": source_profile[4].isoformat(),
            "schema_equal": True,
            "rows_equal": True,
            "source_files_unchanged": True,
            "baseline_isolated_copy": True,
            **row_group_proof,
        }
        (candidate_root / "evidence.json").write_text(
            json.dumps(evidence, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(candidate_root, target)

    return ClusteredCandidateResult(
        root=target,
        clustered_descriptor=target / "clustered/bars.json",
        baseline_descriptor=target / "baseline/bars.json",
        report=report_path,
        created=True,
        row_count=int(source_profile[0]),
        instrument_count=int(source_profile[1]),
        row_group_count=int(row_group_proof["row_group_count"]),
    )


def build_range_partitioned_cn_qfq_candidate(
    *,
    source_descriptor: Path,
    output_root: Path,
    version: str,
    instruments_per_partition: int = 32,
    row_group_size: int = 8192,
    materialize_adjustments: bool = False,
) -> RangePartitionedCandidateResult:
    source_descriptor = source_descriptor.expanduser().resolve()
    output_root = output_root.expanduser().resolve()
    if instruments_per_partition < 1:
        raise ValueError("instruments_per_partition must be positive")
    if row_group_size < 2048 or row_group_size % 2048:
        raise ValueError("row_group_size must be a multiple of 2048")
    dataset = load_cn_qfq_dataset(source_descriptor)
    if dataset is None:
        raise ValueError("source is not a CN qfq descriptor")
    target = output_root / f"version={version}"
    report_path = target / "evidence.json"
    if target.exists():
        payload = json.loads(report_path.read_text(encoding="utf-8"))
        return RangePartitionedCandidateResult(
            root=target,
            descriptor=target / "bars.json",
            report=report_path,
            created=False,
            row_count=int(payload["row_count"]),
            instrument_count=int(payload["instrument_count"]),
            partition_count=int(payload["partition_count"]),
        )
    output_root.mkdir(parents=True, exist_ok=True)
    source_paths = [str(path) for path in dataset.bars]
    with tempfile.TemporaryDirectory(prefix=f".{target.name}.", dir=output_root) as temporary:
        candidate_root = Path(temporary) / "candidate"
        bars_root = candidate_root / "bars"
        bars_root.mkdir(parents=True)
        with _bounded_connection(candidate_root) as connection:
            partition_source_paths = source_paths
            if materialize_adjustments:
                source_relation = cn_qfq_relation_sql(source_descriptor)
                if source_relation is None:
                    raise ValueError("source is not a CN qfq descriptor")
                materialized = candidate_root / ".materialized-qfq.parquet"
                connection.execute(
                    f"COPY (SELECT * FROM {source_relation} ORDER BY instrument_id,ts_event) "
                    f"TO {_sql_string(materialized)} "
                    f"(FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE {row_group_size})"
                )
                if _dataset_profile(connection, source_paths) != _dataset_profile(
                    connection, str(materialized)
                ):
                    raise ValueError("materialized qfq profile differs from source relation")
                _assert_relation_equivalent(connection, source_relation, materialized)
                partition_source_paths = [str(materialized)]
            instruments = [
                row[0]
                for row in connection.execute(
                    "SELECT DISTINCT instrument_id FROM read_parquet(?,union_by_name=true,"
                    "hive_partitioning=false) ORDER BY instrument_id",
                    [partition_source_paths],
                ).fetchall()
            ]
            partitions: list[dict[str, object]] = []
            partition_paths: list[Path] = []
            for offset in range(0, len(instruments), instruments_per_partition):
                first = instruments[offset]
                last = instruments[
                    min(offset + instruments_per_partition - 1, len(instruments) - 1)
                ]
                destination = bars_root / f"range-{len(partitions):04d}.parquet"
                connection.execute(
                    "COPY (SELECT * FROM read_parquet(?,union_by_name=true,"
                    "hive_partitioning=false) WHERE instrument_id BETWEEN ? AND ? "
                    f"ORDER BY instrument_id,ts_event) TO {_sql_string(destination)} "
                    f"(FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE {row_group_size})",
                    [partition_source_paths, first, last],
                )
                partition_paths.append(destination)
                partitions.append(
                    {
                        "first_instrument_id": first,
                        "last_instrument_id": last,
                        "bars": [os.path.relpath(destination, candidate_root)],
                    }
                )
            source_profile = _dataset_profile(connection, partition_source_paths)
            candidate_profile = _dataset_profile(
                connection, [str(path) for path in partition_paths]
            )
            if source_profile != candidate_profile:
                raise ValueError("range-partitioned candidate profile differs from source")
            _assert_equivalent(
                connection,
                partition_source_paths,
                [str(path) for path in partition_paths],
            )
            if materialize_adjustments:
                materialized.unlink()
        events = (
            ()
            if materialize_adjustments
            else _copy_dataset_files(dataset.events, candidate_root / "events")
        )
        descriptor = candidate_root / "bars.json"
        descriptor.write_text(
            json.dumps(
                {
                    "schema_version": PARTITIONED_MARKET_BARS_SCHEMA_VERSION,
                    "market": "CN",
                    "adjustment": "qfq",
                    "partitions": partitions,
                    "events": [os.path.relpath(path, candidate_root) for path in events],
                },
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        evidence = {
            "schema_version": "quantstack.range-partitioned-cn-qfq-candidate.v1",
            "version": version,
            "row_count": int(source_profile[0]),
            "instrument_count": int(source_profile[1]),
            "partition_count": len(partitions),
            "instruments_per_partition": instruments_per_partition,
            "row_group_size": row_group_size,
            "materialized_adjustments": materialize_adjustments,
            "rows_equal": True,
            "schema_equal": True,
        }
        (candidate_root / "evidence.json").write_text(
            json.dumps(evidence, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(candidate_root, target)
    return RangePartitionedCandidateResult(
        root=target,
        descriptor=target / "bars.json",
        report=report_path,
        created=True,
        row_count=int(source_profile[0]),
        instrument_count=int(source_profile[1]),
        partition_count=len(partitions),
    )


def build_range_partitioned_us_total_return_candidate(
    *,
    source_descriptor: Path,
    output_root: Path,
    version: str,
    instruments_per_partition: int = 32,
    row_group_size: int = 8192,
) -> RangePartitionedCandidateResult:
    source_descriptor = source_descriptor.expanduser().resolve()
    output_root = output_root.expanduser().resolve()
    if instruments_per_partition < 1:
        raise ValueError("instruments_per_partition must be positive")
    if row_group_size < 2048 or row_group_size % 2048:
        raise ValueError("row_group_size must be a multiple of 2048")
    source_dataset = load_us_total_return_dataset(source_descriptor)
    source_relation = us_total_return_relation_sql(source_descriptor)
    target = output_root / f"version={version}"
    report_path = target / "evidence.json"
    if target.exists():
        payload = json.loads(report_path.read_text(encoding="utf-8"))
        return RangePartitionedCandidateResult(
            root=target,
            descriptor=target / "bars.json",
            report=report_path,
            created=False,
            row_count=int(payload["row_count"]),
            instrument_count=int(payload["instrument_count"]),
            partition_count=int(payload["partition_count"]),
        )
    output_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".us-range-", dir=output_root) as temporary:
        candidate_root = Path(temporary) / "candidate"
        bars_root = candidate_root / "bars"
        bars_root.mkdir(parents=True)
        materialized = candidate_root / ".materialized-total-return.parquet"
        with _bounded_connection(candidate_root, memory_limit="2GB") as connection:
            connection.execute(
                f"COPY (SELECT * FROM {source_relation}) "
                f"TO {_sql_string(materialized)} "
                f"(FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE {row_group_size})"
            )
            source_profile = connection.execute(
                f"SELECT count(*),count(DISTINCT instrument_id),"
                "count(DISTINCT (instrument_id,trading_date)),min(trading_date),"
                "max(trading_date),count(*) FILTER "
                f"(WHERE instrument_id IS NULL OR ts_event IS NULL) FROM {source_relation}"
            ).fetchone()
            candidate_profile = _dataset_profile(connection, str(materialized))
            if source_profile != candidate_profile:
                raise ValueError("US range candidate profile differs from source relation")
            _assert_relation_equivalent(connection, source_relation, materialized)
            instruments = [
                row[0]
                for row in connection.execute(
                    "SELECT DISTINCT instrument_id FROM read_parquet(?) ORDER BY instrument_id",
                    [str(materialized)],
                ).fetchall()
            ]
            partition_paths: list[Path] = []
            partitions: list[MarketBarsPartition] = []
            for offset in range(0, len(instruments), instruments_per_partition):
                first = instruments[offset]
                last = instruments[
                    min(offset + instruments_per_partition - 1, len(instruments) - 1)
                ]
                destination = bars_root / f"range-{len(partitions):04d}.parquet"
                connection.execute(
                    "COPY (SELECT * FROM read_parquet(?) WHERE instrument_id BETWEEN ? AND ? "
                    f"ORDER BY instrument_id,ts_event) TO {_sql_string(destination)} "
                    f"(FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE {row_group_size})",
                    [str(materialized), first, last],
                )
                partition_paths.append(destination)
                partitions.append(MarketBarsPartition(first, last, (destination,)))
            if candidate_profile != _dataset_profile(
                connection, [str(path) for path in partition_paths]
            ):
                raise ValueError("US range partitions differ from materialized relation")
            _assert_equivalent(
                connection, [str(materialized)], [str(path) for path in partition_paths]
            )
        materialized.unlink()

        latest: Path | None = None
        if source_dataset.latest is not None:
            latest = candidate_root / "latest.parquet"
            shutil.copyfile(source_dataset.latest, latest)
        descriptor = candidate_root / "bars.json"
        descriptor.write_text(
            json.dumps(
                relative_partitioned_dataset_payload(
                    descriptor_path=descriptor,
                    partitions=tuple(partitions),
                    shared_bars=(),
                    events=(),
                    latest=latest,
                    row_count=int(source_profile[0]),
                    instrument_count=int(source_profile[1]),
                    watermark=source_profile[4].isoformat(),
                ),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        evidence = {
            "schema_version": "quantstack.range-partitioned-us-total-return-candidate.v1",
            "version": version,
            "row_count": int(source_profile[0]),
            "instrument_count": int(source_profile[1]),
            "partition_count": len(partitions),
            "instruments_per_partition": instruments_per_partition,
            "row_group_size": row_group_size,
            "materialized_adjustments": True,
            "rows_equal": True,
            "schema_equal": True,
        }
        (candidate_root / "evidence.json").write_text(
            json.dumps(evidence, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(candidate_root, target)
    return RangePartitionedCandidateResult(
        root=target,
        descriptor=target / "bars.json",
        report=report_path,
        created=True,
        row_count=int(source_profile[0]),
        instrument_count=int(source_profile[1]),
        partition_count=len(partitions),
    )
