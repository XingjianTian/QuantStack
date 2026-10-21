from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from .cn_qfq_dataset import CNQFQDataset, load_cn_qfq_dataset, relative_dataset_payload
from .storage.atomic import atomic_write_immutable_bytes, atomic_write_immutable_file


def _sql_string(path: Path) -> str:
    return "'" + str(path).replace("'", "''") + "'"


def _ledger_latest(path: Path) -> dict[str, float]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != "quantstack.cn-qfq-factor-ledger.v2":
        raise ValueError("unsupported qfq factor ledger")
    latest: dict[str, tuple[str, float]] = {}
    for row in payload.get("factors", []):
        instrument_id = str(row["instrument_id"])
        effective_date = str(row["effective_date"])
        factor = float(row["factor"])
        if instrument_id not in latest or effective_date > latest[instrument_id][0]:
            latest[instrument_id] = (effective_date, factor)
    if not latest:
        raise ValueError("qfq factor ledger is empty")
    return {instrument_id: value[1] for instrument_id, value in latest.items()}


def _equivalent(left: Path, right: Path) -> bool:
    connection = duckdb.connect()
    try:
        return not bool(
            connection.execute(
                "SELECT EXISTS((SELECT * FROM read_parquet(?) EXCEPT ALL "
                "SELECT * FROM read_parquet(?)) UNION ALL "
                "(SELECT * FROM read_parquet(?) EXCEPT ALL SELECT * FROM read_parquet(?)))",
                [str(left), str(right), str(right), str(left)],
            ).fetchone()[0]
        )
    finally:
        connection.close()


def build_cn_qfq_session_release(
    *,
    session: date,
    raw_session_path: Path,
    current_dataset_path: Path,
    current_ledger_path: Path,
    next_ledger_path: Path,
    output_root: Path,
) -> dict[str, object]:
    """Append one qfq session and factor deltas without rewriting prior bars."""

    output_root = output_root.expanduser().resolve()
    current = load_cn_qfq_dataset(current_dataset_path)
    dataset = current or CNQFQDataset((current_dataset_path.expanduser().resolve(),), ())
    old_latest = _ledger_latest(current_ledger_path)
    new_latest = _ledger_latest(next_ledger_path)

    event_rows = []
    for instrument_id in sorted(new_latest):
        old_factor = old_latest.get(instrument_id, new_latest[instrument_id])
        new_factor = new_latest[instrument_id]
        multiplier = old_factor / new_factor
        if abs(multiplier - 1.0) > 1e-12:
            event_rows.append(
                {
                    "instrument_id": instrument_id,
                    "session": session,
                    "price_multiplier": multiplier,
                }
            )

    daily_path = output_root / f"qfq-session-{session.isoformat()}.parquet"

    def write_daily(candidate: Path) -> None:
        connection = duckdb.connect()
        try:
            connection.execute(
                "COPY (SELECT instrument_id,split_part(instrument_id,'.',2) symbol,"
                "split_part(instrument_id,'.',1) venue,trading_date,ts_event,"
                "open,high,low,close,volume,amount,currency,"
                "struct_pack(adjustment := 'qfq') lineage FROM read_parquet(?)) "
                f"TO {_sql_string(candidate)} (FORMAT PARQUET,COMPRESSION ZSTD)",
                [str(raw_session_path)],
            )
        finally:
            connection.close()

    def validate_daily(candidate: Path) -> None:
        connection = duckdb.connect()
        try:
            row = connection.execute(
                "SELECT count(*),count(DISTINCT (instrument_id,trading_date)),"
                "min(trading_date),max(trading_date) FROM read_parquet(?)",
                [str(candidate)],
            ).fetchone()
        finally:
            connection.close()
        if row is None or row[0] < 1 or row[0] != row[1] or row[2] != session or row[3] != session:
            raise ValueError("CN qfq daily session validation failed")

    atomic_write_immutable_file(
        daily_path,
        write=write_daily,
        validate=validate_daily,
        equivalent=_equivalent,
        artifact_kind="CN qfq daily session Parquet",
    )

    event_paths = list(dataset.events)
    if event_rows:
        event_path = output_root / f"qfq-events-{session.isoformat()}.parquet"

        def write_events(candidate: Path) -> None:
            pq.write_table(pa.Table.from_pylist(event_rows), candidate, compression="zstd")

        atomic_write_immutable_file(
            event_path,
            write=write_events,
            validate=lambda candidate: pq.read_metadata(candidate),
            equivalent=_equivalent,
            artifact_kind="CN qfq factor event Parquet",
        )
        event_paths.append(event_path)

    descriptor_path = output_root / "bars.parquet"
    payload = relative_dataset_payload(
        descriptor_path=descriptor_path,
        bars=(*dataset.bars, daily_path),
        events=tuple(event_paths),
    )
    atomic_write_immutable_bytes(
        descriptor_path,
        (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(),
        artifact_kind="CN qfq dataset descriptor",
    )
    return {
        "status": "completed",
        "session": session.isoformat(),
        "descriptor_path": str(descriptor_path),
        "daily_path": str(daily_path),
        "event_count": len(event_rows),
        "history_rewritten": False,
    }
