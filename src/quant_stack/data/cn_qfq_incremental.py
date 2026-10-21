from __future__ import annotations

import json
import math
import tempfile
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from datetime import date, timedelta
from pathlib import Path

import duckdb

from .market_bars_dataset import market_bars_relation_sql
from .storage.atomic import atomic_replace_bytes, atomic_write_immutable_file

ROTATION_SLICES = 5
_PRICE_COLUMNS = ("open", "high", "low", "close")


@contextmanager
def _connection(scratch_parent: Path) -> Iterator[duckdb.DuckDBPyConnection]:
    with tempfile.TemporaryDirectory(
        dir=scratch_parent,
        prefix=".cn-qfq-incremental-",
    ) as temporary_directory:
        connection = duckdb.connect()
        try:
            connection.execute("SET memory_limit='384MB'")
            connection.execute("SET threads=1")
            connection.execute("SET preserve_insertion_order=false")
            connection.execute("SET temp_directory=?", [temporary_directory])
            yield connection
        finally:
            connection.close()


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _equivalent(left: Path, right: Path) -> bool:
    with _connection(left.resolve().parent) as connection:
        changed = connection.execute(
            "SELECT EXISTS((SELECT * FROM read_parquet(?) EXCEPT ALL "
            "SELECT * FROM read_parquet(?)) UNION ALL "
            "(SELECT * FROM read_parquet(?) EXCEPT ALL SELECT * FROM read_parquet(?)))",
            [str(left), str(right), str(right), str(left)],
        ).fetchone()
    return changed == (False,)


def canonical_instrument_id(provider_code: str) -> str:
    prefix, symbol = provider_code.split(".", maxsplit=1)
    venues = {"sh": "XSHG", "sz": "XSHE"}
    try:
        return f"{venues[prefix.lower()]}.{symbol}"
    except KeyError as exc:
        raise ValueError(f"unsupported BaoStock provider code: {provider_code}") from exc


def load_daily_native_records(paths: Sequence[Path]) -> dict[str, dict[str, object]]:
    """Read immutable BaoStock Raw sidecars and retain the existing preclose evidence."""

    records: dict[str, dict[str, object]] = {}
    for path in paths:
        payload = json.loads(path.read_text(encoding="utf-8"))
        native = payload.get("records")
        if not isinstance(native, list):
            raise ValueError(f"daily Raw has no records: {path}")
        for item in native:
            if not isinstance(item, dict) or "code" not in item or "preclose" not in item:
                raise ValueError("daily BaoStock Raw must contain code and preclose")
            instrument_id = canonical_instrument_id(str(item["code"]))
            if instrument_id in records:
                raise ValueError(f"duplicate daily Raw instrument: {instrument_id}")
            records[instrument_id] = dict(item)
    if not records:
        raise ValueError("daily BaoStock Raw is empty")
    return records


def load_daily_session_records(path: Path) -> dict[str, dict[str, object]]:
    """Read complete BaoStock preclose evidence from one CN session Parquet.

    A session without ``provider_preclose`` is a legacy migration artifact, not
    valid input for the routine factor worker.  Reject it here so missing
    evidence can never fan out into a full-universe Provider request plan.
    """

    connection = duckdb.connect()
    try:
        columns = {
            str(item[0])
            for item in connection.execute(
                "DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]
            ).fetchall()
        }
        if "provider_preclose" not in columns:
            raise ValueError(
                "CN session Parquet has no provider_preclose; "
                "routine factor Provider requests are blocked"
            )
        rows = connection.execute(
            "SELECT instrument_id,provider_preclose FROM read_parquet(?) "
            "ORDER BY instrument_id",
            [str(path)],
        ).fetchall()
    finally:
        connection.close()
    prefixes = {"XSHG": "sh", "XSHE": "sz"}
    records: dict[str, dict[str, object]] = {}
    for instrument_id, preclose in rows:
        if preclose is None:
            raise ValueError(
                f"CN session Parquet has null provider_preclose for {instrument_id}; "
                "routine factor Provider requests are blocked"
            )
        venue, symbol = str(instrument_id).split(".", maxsplit=1)
        records[str(instrument_id)] = {
            "code": f"{prefixes[venue]}.{symbol}",
            "preclose": float(preclose),
        }
    if not records:
        raise ValueError("CN session Parquet is empty")
    return records


def previous_close_by_instrument(raw_bars_path: Path, *, before: date) -> dict[str, float]:
    with _connection(raw_bars_path.resolve().parent) as connection:
        relation = market_bars_relation_sql(raw_bars_path)
        rows = connection.execute(
            "SELECT instrument_id, arg_max(close, trading_date) "
            f"FROM {relation} WHERE trading_date < ? GROUP BY instrument_id",
            [before],
        ).fetchall()
    return {str(instrument_id): float(close) for instrument_id, close in rows}


def factor_universe_from_raw(raw_bars_path: Path) -> list[dict[str, str]]:
    with _connection(raw_bars_path.resolve().parent) as connection:
        relation = market_bars_relation_sql(raw_bars_path)
        ids = [
            str(row[0])
            for row in connection.execute(
                f"SELECT DISTINCT instrument_id FROM {relation} ORDER BY instrument_id",
            ).fetchall()
        ]
    if not ids:
        raise ValueError("factor universe Raw is empty")
    prefixes = {"XSHG": "sh", "XSHE": "sz"}
    result: list[dict[str, str]] = []
    for instrument_id in ids:
        venue, symbol = instrument_id.split(".", maxsplit=1)
        try:
            provider_symbol = f"{prefixes[venue]}.{symbol}"
        except KeyError as exc:
            raise ValueError(f"unsupported CN venue: {venue}") from exc
        result.append({"instrument_id": instrument_id, "provider_symbol": provider_symbol})
    return result


def factor_last_checked(path: Path) -> dict[str, date]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != "quantstack.cn-qfq-factor-ledger.v2":
        raise ValueError("unsupported qfq factor ledger")
    checked = payload.get("last_checked")
    if not isinstance(checked, dict):
        raise ValueError("factor ledger has no last-checked map")
    return {str(key): date.fromisoformat(str(value)) for key, value in checked.items()}


def build_factor_query_plan(
    *,
    session: date,
    session_ordinal: int,
    universe: Sequence[Mapping[str, str]],
    daily_native_records: Mapping[str, Mapping[str, object]],
    previous_closes: Mapping[str, float],
    last_factor_checks: Mapping[str, date] | None = None,
    rotation_slices: int = ROTATION_SLICES,
) -> dict[str, object]:
    """Plan event-driven queries plus one deterministic trading-session rotation slice."""

    if session_ordinal < 0:
        raise ValueError("session_ordinal must be non-negative")
    if rotation_slices < 2:
        raise ValueError("factor rotation must have at least two slices")
    ordered = sorted(universe, key=lambda item: item["instrument_id"])
    ids = [item["instrument_id"] for item in ordered]
    if not ids or len(ids) != len(set(ids)):
        raise ValueError("factor universe must be non-empty and unique")
    if set(daily_native_records).difference(ids):
        raise ValueError("daily Raw contains instruments outside the factor universe")

    slot = session_ordinal % rotation_slices
    rotation_ids = {
        instrument_id for index, instrument_id in enumerate(ids) if index % rotation_slices == slot
    }
    reasons: dict[str, list[str]] = {}
    for instrument_id in ids:
        native = daily_native_records.get(instrument_id)
        if native is not None:
            if native.get("preclose") is None:
                reasons.setdefault(instrument_id, []).append("legacy_session_missing_preclose")
            else:
                preclose = float(str(native["preclose"]))
                if not math.isfinite(preclose) or preclose <= 0:
                    raise ValueError(f"invalid BaoStock preclose for {instrument_id}")
                prior = previous_closes.get(instrument_id)
                if prior is None:
                    reasons.setdefault(instrument_id, []).append(
                        "new_listing_or_first_observation"
                    )
                elif not math.isclose(preclose, prior, rel_tol=1e-10, abs_tol=1e-8):
                    reasons.setdefault(instrument_id, []).append(
                        "preclose_changed_from_previous_raw_close"
                    )
        if instrument_id in rotation_ids:
            reasons.setdefault(instrument_id, []).append(f"rotation_slice_{slot}")

    by_id = {item["instrument_id"]: dict(item) for item in ordered}
    checked = last_factor_checks or {}
    requests = []
    for instrument_id in sorted(reasons):
        previous_check = checked.get(instrument_id)
        start = min(session, previous_check + timedelta(days=1)) if previous_check else session
        requests.append(
            {
                **by_id[instrument_id],
                "start_date": start.isoformat(),
                "end_date": session.isoformat(),
                "reasons": reasons[instrument_id],
            }
        )
    event_count = sum(
        any(reason != f"rotation_slice_{slot}" for reason in item["reasons"]) for item in requests
    )
    return {
        "schema_version": "quantstack.cn-qfq-factor-plan.v2",
        "session": session.isoformat(),
        "session_ordinal": session_ordinal,
        "rotation_slices": rotation_slices,
        "rotation_slot": slot,
        "universe_count": len(ids),
        "rotation_request_count": len(rotation_ids),
        "event_candidate_count": event_count,
        "request_count": len(requests),
        "request_reduction_ratio": 1 - len(requests) / len(ids),
        "requests": requests,
    }


def factor_rows_from_artifact(path: Path) -> list[dict[str, object]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    requests = payload.get("requests")
    if not isinstance(requests, list) or not requests:
        raise ValueError("factor artifact has no requests")
    rows: list[dict[str, object]] = []
    seen: set[str] = set()
    for request in requests:
        instrument_id = str(request.get("instrument_id"))
        if instrument_id in seen:
            raise ValueError("factor artifact contains duplicate requests")
        seen.add(instrument_id)
        status = request.get("status")
        native_rows = request.get("rows")
        if status == "ok_no_adjustments":
            if native_rows or int(request.get("empty_confirmations", 0)) < 1:
                raise ValueError(f"invalid empty factor evidence for {instrument_id}")
            rows.append(
                {
                    "instrument_id": instrument_id,
                    "effective_date": "0001-01-01",
                    "factor": 1.0,
                }
            )
            continue
        if status != "ok" or not isinstance(native_rows, list) or not native_rows:
            raise ValueError(f"factor evidence is unavailable for {instrument_id}")
        for native in native_rows:
            factor = float(native["backAdjustFactor"])
            if not math.isfinite(factor) or factor <= 0:
                raise ValueError("back adjustment factors must be finite and positive")
            rows.append(
                {
                    "instrument_id": instrument_id,
                    "effective_date": str(native["dividOperateDate"]),
                    "factor": factor,
                }
            )
    return rows


def build_factor_ledger(
    *, factor_path: Path, output_path: Path, watermark: date
) -> dict[str, object]:
    rows = factor_rows_from_artifact(factor_path)
    payload = {
        "schema_version": "quantstack.cn-qfq-factor-ledger.v2",
        "watermark": watermark.isoformat(),
        "instrument_count": len({str(item["instrument_id"]) for item in rows}),
        "last_checked": {
            str(instrument_id): watermark.isoformat()
            for instrument_id in sorted({item["instrument_id"] for item in rows})
        },
        "factors": sorted(
            rows,
            key=lambda item: (str(item["instrument_id"]), str(item["effective_date"])),
        ),
    }
    atomic_replace_bytes(
        output_path,
        (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(),
    )
    return payload


def merge_factor_delta(
    *,
    current_ledger_path: Path,
    factor_delta_path: Path,
    factor_plan_path: Path,
    output_path: Path,
) -> dict[str, object]:
    """Merge one selected factor delta without treating an empty range as factor=1."""

    current = json.loads(current_ledger_path.read_text(encoding="utf-8"))
    if current.get("schema_version") != "quantstack.cn-qfq-factor-ledger.v2":
        raise ValueError("unsupported current factor ledger")
    plan = json.loads(factor_plan_path.read_text(encoding="utf-8"))
    delta = json.loads(factor_delta_path.read_text(encoding="utf-8"))
    planned = [str(item["instrument_id"]) for item in plan.get("requests", [])]
    requests = delta.get("requests")
    if (
        not isinstance(requests, list)
        or [str(item.get("instrument_id")) for item in requests] != planned
    ):
        raise ValueError("factor delta does not exactly match its request plan")

    merged = {
        (str(item["instrument_id"]), str(item["effective_date"])): dict(item)
        for item in current.get("factors", [])
    }
    known_ids = {instrument_id for instrument_id, _effective_date in merged}
    for request in requests:
        instrument_id = str(request["instrument_id"])
        native_rows = request.get("rows")
        if request.get("status") == "ok_no_adjustments":
            if native_rows or int(request.get("empty_confirmations", 0)) < 1:
                raise ValueError(f"invalid empty factor delta for {instrument_id}")
            if instrument_id not in known_ids:
                merged[(instrument_id, "0001-01-01")] = {
                    "instrument_id": instrument_id,
                    "effective_date": "0001-01-01",
                    "factor": 1.0,
                }
                known_ids.add(instrument_id)
            continue
        if request.get("status") != "ok" or not isinstance(native_rows, list) or not native_rows:
            raise ValueError(f"factor delta is incomplete for {instrument_id}")
        for native in native_rows:
            factor = float(native["backAdjustFactor"])
            if not math.isfinite(factor) or factor <= 0:
                raise ValueError("back adjustment factors must be finite and positive")
            effective_date = str(native["dividOperateDate"])
            merged[(instrument_id, effective_date)] = {
                "instrument_id": instrument_id,
                "effective_date": effective_date,
                "factor": factor,
            }
            known_ids.add(instrument_id)

    progress = dict(current.get("rotation_progress", {}))
    progress[str(plan["rotation_slot"])] = str(plan["session"])
    last_checked = dict(current.get("last_checked", {}))
    for instrument_id in planned:
        last_checked[instrument_id] = str(plan["session"])
    payload = {
        **current,
        "watermark": str(plan["session"]),
        "instrument_count": len(known_ids),
        "rotation_slices": int(plan["rotation_slices"]),
        "rotation_progress": progress,
        "last_checked": last_checked,
        "last_request_count": len(requests),
        "factors": sorted(
            merged.values(), key=lambda item: (item["instrument_id"], item["effective_date"])
        ),
    }
    atomic_replace_bytes(
        output_path,
        (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(),
    )
    return payload


def _ledger_rows(path: Path) -> list[dict[str, object]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != "quantstack.cn-qfq-factor-ledger.v2":
        raise ValueError("unsupported qfq factor ledger")
    rows = payload.get("factors")
    if not isinstance(rows, list) or not rows:
        raise ValueError("qfq factor ledger is empty")
    return [dict(item) for item in rows]


def _write_stable_partition(
    *, raw_bars_path: Path, factor_ledger_path: Path, output_path: Path
) -> dict[str, object]:
    import pyarrow as pa

    rows = _ledger_rows(factor_ledger_path)
    proof: dict[str, object] = {}

    def write(candidate: Path) -> None:
        with _connection(candidate.parent) as connection:
            connection.register("factor_ledger", pa.Table.from_pylist(rows))
            connection.execute(
                "COPY (WITH factors AS (SELECT instrument_id, "
                "CAST(effective_date AS DATE) effective_date, CAST(factor AS DOUBLE) factor "
                "FROM factor_ledger), joined AS (SELECT b.*, f.factor FROM read_parquet(?) b "
                "ASOF LEFT JOIN factors f ON b.instrument_id=f.instrument_id "
                "AND b.trading_date>=f.effective_date) "
                "SELECT instrument_id, symbol, venue, trading_date, ts_event, "
                "CASE WHEN factor IS NULL THEN open ELSE open*factor END stable_open, "
                "CASE WHEN factor IS NULL THEN high ELSE high*factor END stable_high, "
                "CASE WHEN factor IS NULL THEN low ELSE low*factor END stable_low, "
                "CASE WHEN factor IS NULL THEN close ELSE close*factor END stable_close, "
                "volume, amount, currency, factor back_adjust_factor, "
                "struct_pack(adjustment := 'qfq_stable_numerator_v1') lineage "
                "FROM joined) "
                f"TO {_sql_string(str(candidate))} (FORMAT PARQUET, COMPRESSION ZSTD)",
                [str(raw_bars_path)],
            )

    def validate(candidate: Path) -> None:
        with _connection(candidate.parent) as connection:
            row = connection.execute(
                "SELECT count(*), count(DISTINCT (instrument_id,trading_date)), "
                "count(DISTINCT instrument_id), count(*) FILTER (WHERE stable_open IS NULL "
                "OR stable_high IS NULL OR stable_low IS NULL OR stable_close IS NULL "
                "OR back_adjust_factor<=0) FROM read_parquet(?)",
                [str(candidate)],
            ).fetchone()
            raw_count = connection.execute(
                "SELECT count(*) FROM read_parquet(?)", [str(raw_bars_path)]
            ).fetchone()[0]
        if row is None or row[0] != raw_count or row[1] != raw_count or row[3] != 0:
            raise ValueError("stable qfq numerator validation failed")
        proof.update(row_count=int(row[0]), instrument_count=int(row[2]))

    created = atomic_write_immutable_file(
        output_path,
        write=write,
        validate=validate,
        equivalent=_equivalent,
        artifact_kind="CN qfq stable numerator Parquet",
    )
    if not proof:
        validate(output_path)
    return {**proof, "created": created, "history_rewritten": False}


def build_stable_base(
    *, raw_bars_path: Path, factor_ledger_path: Path, output_path: Path
) -> dict[str, object]:
    """One-time migration of existing history into the stable numerator representation."""

    return _write_stable_partition(
        raw_bars_path=raw_bars_path,
        factor_ledger_path=factor_ledger_path,
        output_path=output_path,
    )


def append_stable_session(
    *, daily_raw_path: Path, factor_ledger_path: Path, output_path: Path
) -> dict[str, object]:
    """Materialize only one daily Raw partition; prior stable files remain immutable."""

    return _write_stable_partition(
        raw_bars_path=daily_raw_path,
        factor_ledger_path=factor_ledger_path,
        output_path=output_path,
    )


def project_qfq(
    *, stable_paths: Sequence[Path], factor_ledger_path: Path, output_path: Path
) -> dict[str, object]:
    """Project qfq from stable numerators without rewriting any stable history."""

    import pyarrow as pa

    if not stable_paths:
        raise ValueError("qfq projection requires stable numerator paths")
    rows = _ledger_rows(factor_ledger_path)
    proof: dict[str, object] = {}

    def write(candidate: Path) -> None:
        with _connection(candidate.parent) as connection:
            connection.register("factor_ledger", pa.Table.from_pylist(rows))
            connection.execute(
                "COPY (WITH latest AS (SELECT instrument_id, arg_max(CAST(factor AS DOUBLE), "
                "CAST(effective_date AS DATE)) latest_factor FROM factor_ledger "
                "GROUP BY instrument_id) SELECT s.instrument_id,s.symbol,s.venue,s.trading_date,"
                "s.ts_event,CASE WHEN s.back_adjust_factor IS NULL THEN s.stable_open "
                "ELSE s.stable_open/l.latest_factor END AS open,"
                "CASE WHEN s.back_adjust_factor IS NULL THEN s.stable_high "
                "ELSE s.stable_high/l.latest_factor END AS high,"
                "CASE WHEN s.back_adjust_factor IS NULL THEN s.stable_low "
                "ELSE s.stable_low/l.latest_factor END AS low,"
                "CASE WHEN s.back_adjust_factor IS NULL THEN s.stable_close "
                "ELSE s.stable_close/l.latest_factor END AS close,"
                "s.volume,s.amount,s.currency,"
                "struct_pack(adjustment := 'qfq') lineage FROM read_parquet(?) s "
                "JOIN latest l USING(instrument_id)) "
                f"TO {_sql_string(str(candidate))} (FORMAT PARQUET, COMPRESSION ZSTD)",
                [[str(path) for path in stable_paths]],
            )

    def validate(candidate: Path) -> None:
        with _connection(candidate.parent) as connection:
            row = connection.execute(
                "SELECT count(*),count(DISTINCT (instrument_id,trading_date)),"
                "count(DISTINCT instrument_id),count(*) FILTER (WHERE open IS NULL OR "
                "NOT(high>=greatest(open,close) AND low<=least(open,close) AND low<=high)) "
                "FROM read_parquet(?)",
                [str(candidate)],
            ).fetchone()
        if row is None or row[0] != row[1] or row[3] != 0:
            raise ValueError("incremental qfq projection validation failed")
        proof.update(row_count=int(row[0]), instrument_count=int(row[2]))

    created = atomic_write_immutable_file(
        output_path,
        write=write,
        validate=validate,
        equivalent=_equivalent,
        artifact_kind="CN incremental qfq projection Parquet",
    )
    if not proof:
        validate(output_path)
    return {**proof, "created": created, "stable_history_rewritten": False}


def write_incremental_manifest(
    *,
    output_path: Path,
    watermark: date,
    factor_ledger_path: Path,
    stable_base_path: Path,
    stable_daily_paths: Iterable[Path],
) -> dict[str, object]:
    daily = sorted(str(path) for path in stable_daily_paths)
    payload = {
        "schema_version": "quantstack.cn-qfq-incremental-release.v2",
        "watermark": watermark.isoformat(),
        "factor_ledger": str(factor_ledger_path),
        "stable_base": str(stable_base_path),
        "stable_daily": daily,
        "stable_partition_count": 1 + len(daily),
        "projection_formula": "stable_price/latest_back_adjust_factor",
        "history_rewritten": False,
    }
    atomic_replace_bytes(
        output_path,
        (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(),
    )
    return payload
