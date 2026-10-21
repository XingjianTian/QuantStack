from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path

import duckdb
import pyarrow as pa

SESSION_SCHEMA = "quantstack.daily-session-parquet.v1"
_KEY_COLUMNS = ("instrument_id", "trading_date")


@dataclass(frozen=True)
class SessionParquetResult:
    market: str
    session: date
    path: Path
    row_count: int
    instrument_count: int
    omission_count: int
    installed: bool


def _validate(path: Path, *, market: str, session: date) -> tuple[int, int]:
    connection = duckdb.connect()
    try:
        row = connection.execute(
            "SELECT count(*), count(DISTINCT instrument_id), "
            "count(DISTINCT (instrument_id,trading_date)), "
            "min(trading_date), max(trading_date), "
            "count(*) FILTER (WHERE instrument_id IS NULL OR open IS NULL OR high IS NULL "
            "OR low IS NULL OR close IS NULL OR volume IS NULL OR volume < 0 "
            "OR high < greatest(open,close) OR low > least(open,close) OR low > high), "
            "count(*) FILTER (WHERE market != ?) FROM read_parquet(?, hive_partitioning=false)",
            [market, str(path)],
        ).fetchone()
    finally:
        connection.close()
    if row is None or row[0] == 0:
        raise ValueError("session Parquet must contain at least one row")
    if row[0] != row[2]:
        raise ValueError("session Parquet contains duplicate instrument/date keys")
    if row[3] != session or row[4] != session:
        raise ValueError("session Parquet contains rows outside its declared session")
    if row[5] or row[6]:
        raise ValueError(
            "session Parquet failed validation: "
            f"ohlcv={int(row[5])}, market={int(row[6])}"
        )
    return int(row[0]), int(row[1])


def inspect_session_package(package: Path) -> SessionParquetResult:
    manifest_path = package / "manifest.json"
    bars_path = package / "bars.parquet"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != SESSION_SCHEMA:
        raise ValueError("unsupported session Parquet manifest")
    market = str(payload["market"])
    session = date.fromisoformat(str(payload["session"]))
    rows, instruments = _validate(bars_path, market=market, session=session)
    if rows != int(payload["row_count"]) or instruments != int(payload["instrument_count"]):
        raise ValueError("session Parquet manifest counts do not match readback")
    omissions = list(payload.get("omissions", []))
    requested = int(payload.get("requested_instruments", instruments + len(omissions)))
    if requested != instruments + len(omissions):
        raise ValueError("session requested instruments do not close against rows and omissions")
    return SessionParquetResult(
        market=market,
        session=session,
        path=package,
        row_count=rows,
        instrument_count=instruments,
        omission_count=len(omissions),
        installed=False,
    )


def install_session_package(
    *,
    records: Iterable[Mapping[str, object]],
    output_root: Path,
    market: str,
    session: date,
    dataset_id: str,
    version: str,
    adjustment: str,
    requested_instruments: int | None = None,
    omissions: Iterable[str] = (),
    provider_errors: Iterable[Mapping[str, object]] = (),
) -> SessionParquetResult:
    """Validate and atomically install one market/session Parquet package.

    This local/NAS contract intentionally uses no content hash or HMAC. The package is
    checked once through DuckDB and the completed directory is installed with one rename.
    """

    rows = [dict(item) for item in records]
    omission_list = sorted({str(item) for item in omissions})
    errors = [dict(item) for item in provider_errors]
    if errors:
        raise ValueError("provider errors prevent session installation")
    if not rows:
        raise ValueError("session installation requires records")
    for row in rows:
        row.setdefault("market", market)
        row.setdefault("trading_date", session)
    identities = {str(row["instrument_id"]) for row in rows}
    requested = (
        requested_instruments
        if requested_instruments is not None
        else len(identities) + len(omission_list)
    )
    if requested != len(identities) + len(omission_list):
        raise ValueError("requested instruments must equal returned instruments plus omissions")
    if identities.intersection(omission_list):
        raise ValueError("an instrument cannot be both returned and omitted")

    target = output_root / f"market={market}" / f"session={session.isoformat()}"
    if target.exists():
        existing = inspect_session_package(target)
        existing_manifest = json.loads(
            (target / "manifest.json").read_text(encoding="utf-8")
        )
        expected_identity = {
            "dataset_id": dataset_id,
            "version": version,
            "market": market,
            "session": session.isoformat(),
            "adjustment": adjustment,
            "row_count": len(rows),
            "instrument_count": len(identities),
            "requested_instruments": requested,
            "omissions": omission_list,
        }
        if any(existing_manifest.get(key) != value for key, value in expected_identity.items()):
            raise ValueError("existing session package conflicts with requested installation")
        return existing

    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=f".{target.name}.", dir=target.parent) as temporary:
        candidate = Path(temporary) / "package"
        candidate.mkdir()
        bars_path = candidate / "bars.parquet"
        connection = duckdb.connect()
        try:
            connection.register("session_rows", pa.Table.from_pylist(rows))
            connection.execute(
                "COPY (SELECT * FROM session_rows ORDER BY instrument_id,trading_date) "
                "TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
                [str(bars_path)],
            )
        finally:
            connection.close()
        row_count, instrument_count = _validate(bars_path, market=market, session=session)
        manifest = {
            "schema_version": SESSION_SCHEMA,
            "dataset_id": dataset_id,
            "version": version,
            "market": market,
            "session": session.isoformat(),
            "adjustment": adjustment,
            "row_count": row_count,
            "instrument_count": instrument_count,
            "requested_instruments": requested,
            "omissions": omission_list,
            "files": ["bars.parquet"],
            "validation": [
                "single_session",
                "unique_instrument_date",
                "ohlcv",
                "requested_equals_rows_plus_omissions",
            ],
        }
        (candidate / "manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(candidate, target)

    result = inspect_session_package(target)
    return SessionParquetResult(**{**asdict(result), "installed": True})


def replay_session_package(package: Path, candidate_parquet: Path) -> bool:
    """Weekly, zero-provider semantic replay; routine daily does not call this."""

    current = inspect_session_package(package)
    _validate(candidate_parquet, market=current.market, session=current.session)
    connection = duckdb.connect()
    try:
        changed = connection.execute(
            "SELECT EXISTS((SELECT * FROM read_parquet(?, hive_partitioning=false) "
            "EXCEPT ALL SELECT * FROM read_parquet(?, hive_partitioning=false)) "
            "UNION ALL (SELECT * FROM read_parquet(?, hive_partitioning=false) "
            "EXCEPT ALL SELECT * FROM read_parquet(?, hive_partitioning=false)))",
            [
                str(package / "bars.parquet"),
                str(candidate_parquet),
                str(candidate_parquet),
                str(package / "bars.parquet"),
            ],
        ).fetchone()[0]
    finally:
        connection.close()
    return not bool(changed)
