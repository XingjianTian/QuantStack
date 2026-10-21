from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import pyarrow as pa

from .storage.atomic import atomic_write_immutable_bytes, atomic_write_immutable_file
from .us_adjustment import publish_us_adjustment_shadow
from .us_total_return_dataset import (
    load_us_total_return_dataset,
    relative_dataset_payload,
    relative_partitioned_dataset_payload,
    us_total_return_relation_sql,
)


@dataclass(frozen=True)
class USAdjustmentIncrementalResult:
    bars_path: Path
    report_path: Path
    created: bool
    row_count: int
    instrument_count: int
    session: date
    action_instruments: int


def publish_us_total_return_incremental(
    *,
    base_bars_path: Path,
    daily_records: Iterable[Mapping[str, object]],
    session: date,
    work_root: Path,
    data_root: Path,
    base_dataset_version: str,
    dataset_version: str,
    environment_id: str = "us-total-return-incremental",
):
    """Materialize and publish one immutable daily candidate on the same filesystem."""

    del work_root
    shadow_root = (
        data_root.expanduser().resolve()
        / "normalized/bars_1d/market=US"
        / f"version={dataset_version}"
    )
    result = materialize_us_total_return_incremental(
        base_bars_path=base_bars_path,
        daily_records=daily_records,
        session=session,
        output_path=shadow_root / "total-return-action-derived.parquet",
        report_path=shadow_root / "adjustment-shadow-report.json",
        base_dataset_version=base_dataset_version,
    )
    receipt, proof = publish_us_adjustment_shadow(
        shadow_root=shadow_root,
        data_root=data_root,
        dataset_id="us_equity_total_return",
        dataset_version=dataset_version,
        environment_id=environment_id,
    )
    return result, receipt, proof


def _number(row: Mapping[str, object], field: str) -> float:
    value = row.get(field)
    if not isinstance(value, (int, float)):
        raise ValueError(f"US daily action field {field} must be numeric")
    return float(value)


def _sql_string(path: Path) -> str:
    return "'" + str(path).replace("'", "''") + "'"


def _equivalent(left: Path, right: Path) -> bool:
    connection = duckdb.connect()
    try:
        changed = connection.execute(
            "SELECT EXISTS((SELECT * FROM read_parquet(?) EXCEPT ALL "
            "SELECT * FROM read_parquet(?)) UNION ALL "
            "(SELECT * FROM read_parquet(?) EXCEPT ALL SELECT * FROM read_parquet(?)))",
            [str(left), str(right), str(right), str(left)],
        ).fetchone()
    finally:
        connection.close()
    return changed == (False,)


def materialize_us_total_return_incremental(
    *,
    base_bars_path: Path,
    daily_records: Iterable[Mapping[str, object]],
    session: date,
    output_path: Path,
    report_path: Path,
    base_dataset_version: str,
) -> USAdjustmentIncrementalResult:
    """Append one action-complete US session without recomputing unaffected series."""

    base_bars_path = base_bars_path.expanduser().resolve()
    output_path = output_path.expanduser().resolve()
    report_path = report_path.expanduser().resolve()
    if not base_bars_path.is_file():
        raise ValueError("US incremental base bars are missing")
    if output_path == base_bars_path:
        raise ValueError("US incremental output must be a new immutable version")
    base_relation = us_total_return_relation_sql(base_bars_path)
    base_dataset = load_us_total_return_dataset(base_bars_path)

    records = [dict(item) for item in daily_records]
    if not records:
        raise ValueError("US incremental materialization requires daily records")
    required = {
        "instrument_id",
        "Date",
        "Open",
        "High",
        "Low",
        "Close",
        "Adj Close",
        "Volume",
        "Dividends",
        "Stock Splits",
    }
    identities: set[str] = set()
    for row in records:
        missing = sorted(required.difference(row))
        if missing:
            raise ValueError(f"US daily action evidence is incomplete: {', '.join(missing)}")
        instrument_id = str(row["instrument_id"]).upper()
        if instrument_id in identities:
            raise ValueError("US incremental session contains duplicate instruments")
        identities.add(instrument_id)
        if not instrument_id.startswith(("XNAS.", "XNYS.")):
            raise ValueError("US incremental instrument identity is unsupported")
        if date.fromisoformat(str(row["Date"])[:10]) != session:
            raise ValueError("US incremental row is outside the requested session")
        for field in (
            "Open",
            "High",
            "Low",
            "Close",
            "Adj Close",
            "Volume",
            "Dividends",
            "Stock Splits",
        ):
            _number(row, field)

    connection = duckdb.connect()
    try:
        if base_dataset.latest is not None:
            base_rows = connection.execute(
                "SELECT instrument_id,close,trading_date FROM read_parquet(?)",
                [str(base_dataset.latest)],
            ).fetchall()
        else:
            base_rows = connection.execute(
                "SELECT instrument_id,arg_max(close,trading_date),max(trading_date) "
                f"FROM {base_relation} GROUP BY instrument_id"
            ).fetchall()
        if (
            base_dataset.row_count is None
            or base_dataset.instrument_count is None
            or base_dataset.watermark is None
        ):
            base_profile = connection.execute(
                "SELECT count(*),count(DISTINCT instrument_id),max(trading_date) "
                f"FROM {base_relation}"
            ).fetchone()
            if base_profile is None or base_profile[2] is None:
                raise ValueError("US incremental base profile is unavailable")
            base_count = int(base_profile[0])
            base_instrument_count = int(base_profile[1])
            base_watermark = base_profile[2]
        else:
            base_count = base_dataset.row_count
            base_instrument_count = base_dataset.instrument_count
            base_watermark = date.fromisoformat(base_dataset.watermark)
    finally:
        connection.close()
    if base_watermark >= session:
        raise ValueError("US incremental session does not advance the base watermark")
    latest = {str(item[0]): (float(item[1]), item[2]) for item in base_rows}
    event_rows: list[dict[str, object]] = []
    canonical_daily: list[dict[str, object]] = []
    new_instruments: list[str] = []
    for row in sorted(records, key=lambda item: str(item["instrument_id"])):
        instrument_id = str(row["instrument_id"]).upper()
        dividend = _number(row, "Dividends")
        split = _number(row, "Stock Splits")
        if dividend < 0 or split < 0:
            raise ValueError("US corporate actions must be non-negative")
        split_ratio = split if split not in (0.0, 1.0) else 1.0
        previous = latest.get(instrument_id)
        if previous is None:
            if dividend or split_ratio != 1.0:
                raise ValueError(
                    "US incremental new instrument cannot begin with a corporate action"
                )
            new_instruments.append(instrument_id)
            price_multiplier = 1.0
        else:
            previous_close, previous_date = previous
            if previous_date >= session:
                raise ValueError("US incremental session does not advance the base watermark")
            previous_close_current_basis = previous_close / split_ratio
            if not 0 <= dividend < previous_close_current_basis:
                raise ValueError("US dividend is inconsistent with the preceding close")
            dividend_factor = (
                (previous_close_current_basis - dividend) / previous_close_current_basis
                if dividend
                else 1.0
            )
            price_multiplier = dividend_factor / split_ratio
        volume_multiplier = split_ratio
        if dividend or split_ratio != 1.0:
            event_rows.append(
                {
                    "instrument_id": instrument_id,
                    "session": session,
                    "price_multiplier": price_multiplier,
                    "volume_multiplier": volume_multiplier,
                    "cash_dividend": dividend,
                    "split_ratio": split_ratio,
                }
            )
        venue, symbol = instrument_id.split(".", maxsplit=1)
        canonical_daily.append(
            {
                "instrument_id": instrument_id,
                "symbol": symbol,
                "venue": venue,
                "trading_date": session,
                "ts_event": datetime.combine(
                    session, time(16), ZoneInfo("America/New_York")
                ).astimezone(UTC),
                "amount": None,
                "currency": "USD",
                "open": _number(row, "Open"),
                "high": _number(row, "High"),
                "low": _number(row, "Low"),
                "close": _number(row, "Close"),
                "volume": _number(row, "Volume"),
                "price_factor": 1.0,
                "semantics": "provider_split_basis_plus_cash_dividend_total_return",
                "lineage": {"adjustment": "total_return"},
            }
        )

    event_schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("session", pa.date32()),
            ("price_multiplier", pa.float64()),
            ("volume_multiplier", pa.float64()),
            ("cash_dividend", pa.float64()),
            ("split_ratio", pa.float64()),
        ]
    )
    event_table = pa.Table.from_pylist(event_rows, schema=event_schema)
    daily_table = pa.Table.from_pylist(canonical_daily)
    proof: dict[str, object] = {}
    parts_root = output_path.parent / "parts"
    daily_path = parts_root / f"session={session.isoformat()}.parquet"
    events_path = parts_root / f"events={session.isoformat()}.parquet"
    latest_path = parts_root / f"latest={session.isoformat()}.parquet"

    def write_table(path: Path, table: pa.Table, *, kind: str) -> None:
        def write(candidate: Path) -> None:
            writer = duckdb.connect()
            try:
                writer.register("rows", table)
                writer.execute(
                    "COPY (SELECT * FROM rows ORDER BY instrument_id) TO ? "
                    "(FORMAT PARQUET, COMPRESSION ZSTD)",
                    [str(candidate)],
                )
            finally:
                writer.close()

        atomic_write_immutable_file(
            path,
            write=write,
            validate=lambda candidate: duckdb.read_parquet(str(candidate)).count("*").fetchone(),
            equivalent=_equivalent,
            artifact_kind=kind,
        )

    write_table(daily_path, daily_table, kind="US total-return daily session")
    if event_rows:
        write_table(events_path, event_table, kind="US total-return event session")
    next_latest = {
        instrument_id: {
            "instrument_id": instrument_id,
            "close": close,
            "trading_date": trading_date,
        }
        for instrument_id, (close, trading_date) in latest.items()
    }
    for row in canonical_daily:
        next_latest[str(row["instrument_id"])] = {
            "instrument_id": row["instrument_id"],
            "close": row["close"],
            "trading_date": session,
        }
    write_table(
        latest_path,
        pa.Table.from_pylist(list(next_latest.values())),
        kind="US total-return latest close snapshot",
    )

    descriptor_bars = (*base_dataset.bars, daily_path)
    descriptor_shared_bars = (*base_dataset.shared_bars, daily_path)
    descriptor_events = (*base_dataset.events, *((events_path,) if event_rows else ()))

    def write(candidate: Path) -> None:
        payload = (
            relative_partitioned_dataset_payload(
                descriptor_path=output_path,
                partitions=base_dataset.partitions,
                shared_bars=descriptor_shared_bars,
                events=descriptor_events,
                latest=latest_path,
                row_count=base_count + len(records),
                instrument_count=base_instrument_count + len(new_instruments),
                watermark=session.isoformat(),
            )
            if base_dataset.partitions
            else relative_dataset_payload(
                descriptor_path=output_path,
                bars=descriptor_bars,
                events=descriptor_events,
                latest=latest_path,
                row_count=base_count + len(records),
                instrument_count=base_instrument_count + len(new_instruments),
                watermark=session.isoformat(),
            )
        )
        candidate.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    def validate(candidate: Path) -> None:
        candidate_dataset = load_us_total_return_dataset(candidate)
        if candidate_dataset.row_count != base_count + len(records):
            raise ValueError("US incremental row coverage is invalid")
        if candidate_dataset.watermark != session.isoformat():
            raise ValueError("US incremental watermark is invalid")
        proof.update(
            row_count=base_count + len(records),
            instrument_count=base_instrument_count + len(new_instruments),
        )

    created = atomic_write_immutable_file(
        output_path,
        write=write,
        validate=validate,
        equivalent=lambda current, candidate: current.read_bytes() == candidate.read_bytes(),
        artifact_kind="US total-return incremental dataset descriptor",
    )
    if not proof:
        validate(output_path)
    report = {
        "schema_version": "quantstack.us-total-return-incremental.v1",
        "status": "passed",
        "base_dataset_version": base_dataset_version,
        "session": session.isoformat(),
        "rows": proof["row_count"],
        "instruments": proof["instrument_count"],
        "daily_rows": len(records),
        "action_instruments": len(event_rows),
        "new_instruments": new_instruments,
        "unchanged_series_factor_recomputed": 0,
        "candidate_projection_rewritten_locally": False,
        "daily_session_files_written": 2 + int(bool(event_rows)),
        "provider_requested": False,
        "full_history_provider_rebuilt": False,
        "action_evidence_complete": True,
        "events": event_rows,
    }
    atomic_write_immutable_bytes(
        report_path,
        (json.dumps(report, indent=2, sort_keys=True, default=str) + "\n").encode(),
        artifact_kind="US total-return incremental report",
    )
    return USAdjustmentIncrementalResult(
        bars_path=output_path,
        report_path=report_path,
        created=created,
        row_count=int(proof["row_count"]),
        instrument_count=int(proof["instrument_count"]),
        session=session,
        action_instruments=len(event_rows),
    )
