from __future__ import annotations

import hashlib
import json
import re
import resource
import time
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Literal

import duckdb

from ..models import Instrument
from ..providers.cn.live import (
    BAOSTOCK_LIVE_DAILY_SCHEMA,
    BAOSTOCK_LIVE_RUNTIME_CONTRACT,
    BaoStockDailyBatchLimitExceeded,
    BaoStockDailyQuotaExceeded,
)
from ..quality import ProviderSchemaValidator
from ..storage import StagingFileSystemStorage
from ..storage.atomic import atomic_replace_file
from ..tasks import (
    FileTaskStore,
    RetryMetadata,
    Task,
    TaskAttempt,
    TaskFailureContext,
    TaskState,
)
from .models import (
    BACKFILL_DATASET_ID,
    BackfillArtifactEvidence,
    BackfillPilotReport,
    BackfillRunInventory,
    BackfillShard,
    BackfillShardInventoryEntry,
    BackfillShardResult,
    BackfillUniverse,
    BackfillUniverseInstrument,
)

FetchBars = Callable[[Sequence[str], date, date], Sequence[Mapping[str, object]]]


def _json_bytes(payload: object) -> bytes:
    return (json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n").encode()


def _sanitized_error_message(error: Exception) -> str:
    message = str(error).replace("\n", " ")[:500]
    message = re.sub(
        r"(?i)(token|password|secret|api[_-]?key)\s*[:=]\s*['\"]?([^&,'\"\s]+)",
        r"\1=<redacted>",
        message,
    )
    return re.sub(r"(://[^:/\s]+:)[^@/\s]+@", r"\1<redacted>@", message)


def _is_retryable_error(error: Exception) -> bool:
    """Retry only bounded transport or recoverable IO failures, including wrapped causes."""

    current: BaseException | None = error
    visited: set[int] = set()
    deterministic_os_errors = (
        FileNotFoundError,
        FileExistsError,
        IsADirectoryError,
        NotADirectoryError,
        PermissionError,
    )
    while current is not None and id(current) not in visited:
        visited.add(id(current))
        if isinstance(current, (TimeoutError, ConnectionError)):
            return True
        if isinstance(current, OSError) and not isinstance(current, deterministic_os_errors):
            return True
        current = current.__cause__ or current.__context__
    return False


def _is_baostock_budget_pause(error: Exception) -> bool:
    """Quota or serial-batch ceiling pauses without consuming retry budget."""

    return isinstance(error, (BaoStockDailyQuotaExceeded, BaoStockDailyBatchLimitExceeded))


def _shard_id(
    dataset_version: str, universe_version: str, ids: Sequence[str], start: date, end: date
) -> str:
    payload = json.dumps(
        [dataset_version, universe_version, sorted(ids), start.isoformat(), end.isoformat()],
        separators=(",", ":"),
    )
    return f"shard-{hashlib.sha256(payload.encode()).hexdigest()[:24]}"


def build_annual_batch_shards(
    universe: BackfillUniverse,
    *,
    dataset_version: str,
    start: date,
    end: date,
    batch_size: int = 10,
) -> tuple[BackfillShard, ...]:
    if start > end:
        raise ValueError("backfill start cannot exceed end")
    if not 1 <= batch_size <= 50:
        raise ValueError("batch_size must be between 1 and 50")
    ids = [item.instrument_id for item in universe.instruments]
    shards: list[BackfillShard] = []
    for year in range(start.year, end.year + 1):
        year_start = max(start, date(year, 1, 1))
        year_end = min(end, date(year, 12, 31))
        for offset in range(0, len(ids), batch_size):
            batch = tuple(ids[offset : offset + batch_size])
            shards.append(
                BackfillShard(
                    shard_id=_shard_id(
                        dataset_version, universe.version, batch, year_start, year_end
                    ),
                    dataset_version=dataset_version,
                    universe_version=universe.version,
                    instrument_ids=batch,
                    start_date=year_start,
                    end_date=year_end,
                )
            )
    return tuple(shards)


def _instruments(universe: BackfillUniverse) -> dict[str, Instrument]:
    return {
        item.instrument_id: Instrument(
            instrument_id=item.instrument_id,
            symbol=item.symbol,
            market="CN",
            venue=item.venue,
            provider_symbol=item.provider_symbol,
            provider_exchange_alias=item.provider_symbol.split(".", 1)[0],
            currency="CNY",
            timezone="Asia/Shanghai",
        )
        for item in universe.instruments
    }


def _normalize(
    records: Sequence[Mapping[str, object]],
    instruments: Mapping[str, Instrument],
    raw_uri: str,
    raw_sha256: str,
) -> tuple[list[dict[str, object]], list[dict[str, str]]]:
    aliases = {str(item.provider_symbol): item for item in instruments.values()}
    rows: list[dict[str, object]] = []
    empty_observations: list[dict[str, str]] = []
    for native in records:
        if native["adjustflag"] != "3":
            raise ValueError("pilot accepts only unadjusted BaoStock rows")
        provider_symbol = str(native["code"])
        instrument = aliases.get(provider_symbol)
        if instrument is None:
            raise ValueError("provider returned an instrument outside the shard")
        trading_date = date.fromisoformat(str(native["date"]))
        prices = [str(native[field]).strip() for field in ("open", "high", "low", "close")]
        volume_amount = [str(native[field]).strip() for field in ("volume", "amount")]
        fully_empty = all(not value for value in prices) and all(
            value in {"", "0", "0.0", "0.00"} for value in volume_amount
        )
        suspended_no_trade = (
            all(prices) and len(set(prices)) == 1 and all(not value for value in volume_amount)
        )
        if fully_empty or suspended_no_trade:
            empty_observations.append(
                {
                    "instrument_id": instrument.instrument_id,
                    "trading_date": trading_date.isoformat(),
                    "classification": (
                        "provider_suspended_no_trade_observation"
                        if suspended_no_trade
                        else "provider_empty_no_trade_observation"
                    ),
                }
            )
            continue
        if any(not value for value in prices) or any(not value for value in volume_amount):
            raise ValueError("partially empty numeric provider row")
        values = {
            field: float(str(native[field]))
            for field in ("open", "high", "low", "close", "volume", "amount")
        }
        if min(values.values()) < 0:
            raise ValueError("prices, volume and amount must be non-negative")
        if values["high"] < max(values["open"], values["low"], values["close"]):
            raise ValueError("invalid high relationship")
        if values["low"] > min(values["open"], values["high"], values["close"]):
            raise ValueError("invalid low relationship")
        rows.append(
            {
                "instrument_id": instrument.instrument_id,
                "symbol": instrument.symbol,
                "venue": instrument.venue,
                "trading_date": trading_date,
                "ts_event": datetime.fromisoformat(f"{trading_date}T15:00:00+08:00").astimezone(
                    UTC
                ),
                **values,
                "currency": "CNY",
                "provider": "baostock",
                "provider_symbol": provider_symbol,
                "adjustment": "none",
                "volume_unit": "share",
                "amount_unit": "CNY",
                "calendar_basis": "provider_observed_coverage_only",
                "retrieval_timestamp": datetime.fromisoformat(
                    str(native["_retrieval_timestamp"])
                ).astimezone(UTC),
                "raw_uri": raw_uri,
                "raw_sha256": raw_sha256,
            }
        )
    return (
        sorted(rows, key=lambda row: (str(row["instrument_id"]), row["trading_date"])),
        empty_observations,
    )


def _quality(
    rows: Sequence[Mapping[str, object]],
    shard: BackfillShard,
    empty_observations: Sequence[Mapping[str, str]],
    universe_instruments: Mapping[str, BackfillUniverseInstrument],
) -> dict[str, object]:
    seen: set[tuple[str, object]] = set()
    duplicates = 0
    anomalies: list[str] = []
    prior: dict[str, date] = {}
    counts = Counter[str]()
    first: dict[str, date] = {}
    last: dict[str, date] = {}
    for row in rows:
        instrument_id = str(row["instrument_id"])
        observed = row["trading_date"]
        assert isinstance(observed, date)
        key = (instrument_id, observed)
        if key in seen:
            duplicates += 1
        seen.add(key)
        if prior.get(instrument_id, observed) > observed:
            anomalies.append(f"{instrument_id}:out_of_order")
        prior[instrument_id] = observed
        counts[instrument_id] += 1
        first[instrument_id] = min(first.get(instrument_id, observed), observed)
        last[instrument_id] = max(last.get(instrument_id, observed), observed)
    missing_instruments = sorted(set(shard.instrument_ids) - set(counts))
    empty_instrument_ids = {item["instrument_id"] for item in empty_observations}
    missing_classifications: dict[str, dict[str, object]] = {}
    for instrument_id in missing_instruments:
        instrument = universe_instruments[instrument_id]
        if (
            instrument.boundary_evidence == "verified"
            and instrument.listing_date is not None
            and shard.end_date < instrument.listing_date
        ):
            classification = "verified_pre_listing"
            rationale = f"shard ends before verified listing date {instrument.listing_date}"
        elif (
            instrument.boundary_evidence == "verified"
            and instrument.delisting_date is not None
            and shard.start_date > instrument.delisting_date
        ):
            classification = "verified_post_delisting"
            rationale = f"shard starts after verified delisting date {instrument.delisting_date}"
        elif instrument_id in empty_instrument_ids:
            classification = "provider_empty_no_trade"
            rationale = "provider returned only explicit empty or suspended observations"
        else:
            classification = "unknown_missing"
            rationale = "no verified listing boundary or explicit provider no-trade observation"
        missing_classifications[instrument_id] = {
            "classification": classification,
            "rationale": rationale,
            "listing_or_delisting_inferred": False,
        }
    return {
        "schema_version": "1.0.0",
        "shard_id": shard.shard_id,
        "checks": [
            "provider_native_schema",
            "unadjusted",
            "ohlc",
            "volume_non_negative",
            "amount_non_negative_cny",
            "instrument_date_unique",
            "sorted_per_instrument",
            "first_last_date",
            "annual_row_count",
            "empty_instrument_visibility",
        ],
        "status": (
            "failed"
            if duplicates or anomalies
            else ("empty" if not rows else ("warning" if empty_observations else "passed"))
        ),
        "row_count": len(rows),
        "duplicate_count": duplicates,
        "anomalies": anomalies,
        "missing_instruments": missing_instruments,
        "missing_classifications": missing_classifications,
        "provider_empty_observation_count": len(empty_observations),
        "provider_empty_observations": list(empty_observations),
        "coverage": {
            key: {
                "rows": counts[key],
                "first_date": first[key].isoformat(),
                "last_date": last[key].isoformat(),
            }
            for key in sorted(counts)
        },
        "calendar_claim": "provider-observed dates only; gap analysis is not an official calendar",
    }


def _artifact_evidence(
    storage: StagingFileSystemStorage,
    *,
    kind: Literal["raw", "parquet", "quality", "task_state", "universe"],
    uri: str,
    row_count: int | None = None,
) -> BackfillArtifactEvidence:
    path = storage.resolve_uri(uri)
    if not path.is_file():
        raise FileNotFoundError(path)
    return BackfillArtifactEvidence(
        kind=kind,
        uri=uri,
        sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        bytes=path.stat().st_size,
        row_count=row_count,
    )


def _verify_artifact(storage: StagingFileSystemStorage, artifact: BackfillArtifactEvidence) -> Path:
    path = storage.resolve_uri(artifact.uri)
    if not path.is_file():
        raise FileNotFoundError(path)
    if path.stat().st_size != artifact.bytes:
        raise ValueError(f"artifact size mismatch: {artifact.uri}")
    if hashlib.sha256(path.read_bytes()).hexdigest() != artifact.sha256:
        raise ValueError(f"artifact hash mismatch: {artifact.uri}")
    return path


def _file_contains(path: Path, needle: bytes) -> bool:
    overlap = max(0, len(needle) - 1)
    previous = b""
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            block = previous + chunk
            if needle in block:
                return True
            previous = block[-overlap:] if overlap else b""
    return False


def rebuild_query_index(data_root: Path, inventory_uri: str) -> tuple[str, dict[str, object]]:
    """Rebuild a versioned, path-independent DuckDB index from an immutable inventory."""

    data_root = data_root.expanduser().resolve()
    storage = StagingFileSystemStorage(data_root)
    inventory_path = storage.resolve_uri(inventory_uri)
    inventory_payload = inventory_path.read_bytes()
    inventory = BackfillRunInventory.model_validate_json(inventory_payload)
    _verify_artifact(storage, inventory.universe_artifact)
    parquet_artifacts = [
        item.parquet_artifact for item in inventory.shards if item.parquet_artifact is not None
    ]
    paths = [str(_verify_artifact(storage, item)) for item in parquet_artifacts]
    for shard in inventory.shards:
        for artifact in (
            *shard.raw_artifacts,
            shard.quality_artifact,
            shard.task_state_artifact,
        ):
            if artifact is not None:
                _verify_artifact(storage, artifact)

    relative = (
        Path("pilot/query") / f"version={inventory.dataset_version}" / "backfill-research.duckdb"
    )
    index_path = data_root / relative
    inventory_file_sha256 = hashlib.sha256(inventory_payload).hexdigest()

    def build(temporary: Path) -> None:
        connection = duckdb.connect(str(temporary))
        try:
            if paths:
                quoted = ",".join("'" + item.replace("'", "''") + "'" for item in paths)
                connection.execute(
                    "CREATE TABLE pilot_bars AS SELECT * "
                    f"FROM read_parquet([{quoted}], union_by_name=true)"
                )
            else:
                connection.execute(
                    "CREATE TABLE pilot_bars AS SELECT "
                    "NULL::VARCHAR instrument_id, NULL::VARCHAR symbol, NULL::VARCHAR venue, "
                    'NULL::DATE trading_date, NULL::DOUBLE "close" WHERE false'
                )
            connection.execute(
                "CREATE INDEX pilot_bars_instrument_date ON pilot_bars(instrument_id, trading_date)"
            )
            connection.execute(
                "CREATE TABLE backfill_index_metadata(key VARCHAR PRIMARY KEY, value VARCHAR)"
            )
            connection.executemany(
                "INSERT INTO backfill_index_metadata VALUES (?, ?)",
                [
                    ("dataset_id", inventory.dataset_id),
                    ("dataset_version", inventory.dataset_version),
                    ("inventory_uri", inventory_uri),
                    ("inventory_file_sha256", inventory_file_sha256),
                    ("inventory_content_sha256", inventory.content_sha256),
                    ("table_materialization", "physical_from_authoritative_parquet"),
                ],
            )
            connection.execute("CHECKPOINT")
        finally:
            connection.close()

    def validate(temporary: Path) -> None:
        connection = duckdb.connect(str(temporary), read_only=True)
        try:
            bound = dict(
                connection.execute("SELECT key, value FROM backfill_index_metadata").fetchall()
            )
            if bound["dataset_version"] != inventory.dataset_version:
                raise ValueError("query index dataset version mismatch")
            if bound["inventory_file_sha256"] != inventory_file_sha256:
                raise ValueError("query index inventory hash mismatch")
            connection.execute("SELECT count(*) FROM pilot_bars").fetchone()
        finally:
            connection.close()

    atomic_replace_file(index_path, write=build, validate=validate)
    connection = duckdb.connect(str(index_path), read_only=True)
    try:
        query_started = time.perf_counter()
        total, instruments, first_date, last_date = connection.execute(
            "SELECT count(*), count(DISTINCT instrument_id), min(trading_date), "
            "max(trading_date) FROM pilot_bars"
        ).fetchone()
        aggregate_ms = (time.perf_counter() - query_started) * 1000
        annual = connection.execute(
            "SELECT year(trading_date) AS year, count(*) AS rows FROM pilot_bars "
            "GROUP BY 1 ORDER BY 1"
        ).fetchall()
        venue = connection.execute(
            "SELECT venue, count(*) AS rows, count(DISTINCT instrument_id) AS instruments "
            "FROM pilot_bars GROUP BY 1 ORDER BY 1"
        ).fetchall()
        duplicate_keys = connection.execute(
            "SELECT count(*) FROM (SELECT instrument_id, trading_date FROM pilot_bars "
            "GROUP BY 1, 2 HAVING count(*) > 1)"
        ).fetchone()[0]
        sample = connection.execute(
            "SELECT instrument_id, min(trading_date), max(trading_date), count(*) "
            "FROM pilot_bars GROUP BY 1 ORDER BY md5(instrument_id) LIMIT 12"
        ).fetchall()
        repeat_sql = (
            "SELECT instrument_id, trading_date, close FROM pilot_bars "
            "ORDER BY instrument_id, trading_date LIMIT 25"
        )
        repeat_a = connection.execute(repeat_sql).fetchall()
        repeat_b = connection.execute(repeat_sql).fetchall()
        range_started = time.perf_counter()
        range_rows = connection.execute(
            "SELECT count(*) FROM pilot_bars WHERE trading_date BETWEEN ? AND ?",
            [first_date, last_date],
        ).fetchone()[0]
        range_ms = (time.perf_counter() - range_started) * 1000
    finally:
        connection.close()
    source_paths_embedded = _file_contains(index_path, data_root.as_posix().encode())
    proof = {
        "cross_shard_table": "pilot_bars",
        "table_materialization": "physical_from_authoritative_parquet",
        "rows": total,
        "instruments": instruments,
        "first_date": first_date.isoformat() if first_date else None,
        "last_date": last_date.isoformat() if last_date else None,
        "annual_rows": [{"year": year, "rows": rows} for year, rows in annual],
        "venue_rows": [
            {"venue": item, "rows": rows, "instruments": count} for item, rows, count in venue
        ],
        "duplicate_instrument_date_keys": duplicate_keys,
        "deterministic_sample": [
            {
                "instrument_id": item,
                "first_date": first.isoformat(),
                "last_date": last.isoformat(),
                "rows": rows,
            }
            for item, first, last, rows in sample
        ],
        "repeat_query_equal": repeat_a == repeat_b,
        "range_query_rows": range_rows,
        "aggregate_query_ms": round(aggregate_ms, 3),
        "range_query_ms": round(range_ms, 3),
        "inventory_uri": inventory_uri,
        "inventory_file_sha256": inventory_file_sha256,
        "source_paths_embedded": source_paths_embedded,
        "ss_database_accessed": False,
        "per_symbol_http_queries": 0,
    }
    return f"quant-data://{relative.as_posix()}", proof


class BackfillPilotRunner:
    def __init__(
        self,
        data_root: Path,
        universe: BackfillUniverse,
        fetch_bars: FetchBars,
        *,
        dataset_version: str,
        max_attempts: int = 3,
        backoff_seconds: float = 0,
    ) -> None:
        self.data_root = data_root.expanduser().resolve()
        self.storage = StagingFileSystemStorage(self.data_root)
        self.store = FileTaskStore(
            self.data_root / "pilot/state" / f"version={dataset_version}" / "tasks"
        )
        self.universe = universe
        self.fetch_bars = fetch_bars
        self.dataset_version = dataset_version
        self.max_attempts = max_attempts
        self.backoff_seconds = backoff_seconds
        self.instruments = _instruments(universe)

    def _task(self, shard: BackfillShard, now: datetime) -> Task:
        contract = BAOSTOCK_LIVE_RUNTIME_CONTRACT
        return Task(
            dataset_id=BACKFILL_DATASET_ID,
            dataset_version=self.dataset_version,
            provider=contract.provider,
            provider_version=contract.provider_version,
            endpoint=contract.endpoint,
            adapter_version=contract.adapter_version,
            native_schema_version=contract.native_schema_version,
            market="CN",
            instrument_scope=shard.instrument_ids,
            start_date=shard.start_date,
            end_date=shard.end_date,
            max_attempts=self.max_attempts,
            retry_backoff_seconds=self.backoff_seconds,
            created_at=now,
        )

    def task_for_shard(self, shard: BackfillShard) -> Task:
        """Return the stable task identity used by campaign progress inspection."""

        return self._task(shard, datetime.now(UTC))

    def run_shard(self, shard: BackfillShard) -> BackfillShardResult:
        """Execute or resume one shard without materializing a partial final inventory."""

        return self._run_shard(shard)

    def _run_shard(self, shard: BackfillShard) -> BackfillShardResult:
        now = datetime.now(UTC)
        task = self._task(shard, now)
        existing = self.store.load(task.task_id)
        attempts = list(existing.attempts) if existing else []
        if existing and existing.task.state is TaskState.SUCCEEDED:
            checkpoint = existing.attempts[-1].checkpoint
            return BackfillShardResult(
                shard_id=shard.shard_id,
                task_id=task.task_id,
                status="replayed",
                attempts=len(existing.attempts),
                instrument_ids=shard.instrument_ids,
                start_date=shard.start_date,
                end_date=shard.end_date,
                row_count=int(checkpoint["row_count"]),
                first_date=checkpoint.get("first_date"),
                last_date=checkpoint.get("last_date"),
                raw_uri=existing.attempts[-1].raw_uri,
                raw_sha256=str(checkpoint["raw_sha256"]),
                parquet_uri=checkpoint.get("parquet_uri"),
                parquet_sha256=checkpoint.get("parquet_sha256"),
                quality_uri=checkpoint.get("quality_uri"),
                duplicate_count=int(checkpoint.get("duplicate_count", 0)),
                anomaly_count=int(checkpoint.get("anomaly_count", 0)),
                missing_classifications=dict(checkpoint.get("missing_classifications", {})),
            )
        if existing and existing.task.state is TaskState.RUNNING:
            if not attempts or attempts[-1].state is not TaskState.RUNNING:
                raise ValueError("persisted running backfill task has no active attempt")
            active = attempts[-1]
            retryable = active.attempt_number < existing.task.max_attempts
            recovered_state = (
                TaskState.RETRYABLE_FAILED if retryable else TaskState.PERMANENTLY_FAILED
            )
            attempts[-1] = active.model_copy(
                update={
                    "state": recovered_state,
                    "completed_at": max(now, active.started_at),
                    "retry": RetryMetadata(
                        max_attempts=existing.task.max_attempts,
                        retryable=retryable,
                        backoff_seconds=existing.task.retry_backoff_seconds,
                        next_retry_at=(
                            max(now, active.started_at)
                            + timedelta(seconds=existing.task.retry_backoff_seconds)
                            if retryable
                            else None
                        ),
                        error_type="InterruptedExecution",
                        error_message="worker process ended before the attempt completed",
                    ),
                }
            )
            recovered = existing.task.model_copy(update={"state": recovered_state})
            self.store.save(recovered, tuple(attempts))
            existing = self.store.load(task.task_id)
        if existing and existing.task.state in {
            TaskState.PERMANENTLY_FAILED,
            TaskState.QUARANTINED,
            TaskState.CANCELLED,
        }:
            last = attempts[-1]
            return BackfillShardResult(
                shard_id=shard.shard_id,
                task_id=task.task_id,
                status="failed",
                attempts=len(attempts),
                instrument_ids=shard.instrument_ids,
                start_date=shard.start_date,
                end_date=shard.end_date,
                row_count=0,
                raw_uri=last.raw_uri,
                error_class=last.retry.error_type,
                error_stage=(last.failure_context.failure_stage if last.failure_context else None),
            )
        task = (existing.task if existing else task).model_copy(update={"state": TaskState.RUNNING})
        last_error: Exception | None = None
        last_stage = "provider_fetch"
        with self.store.execution_lock(task.task_id):
            # A daily quota pause is not a provider retry.  Keep the immutable
            # pause observations, but allow the same task to resume on the
            # next local day instead of turning a safe pause into a permanent
            # failure after max_attempts.
            last_was_budget_pause = bool(
                attempts
                and attempts[-1].retry.error_type
                in {
                    BaoStockDailyQuotaExceeded.__name__,
                    BaoStockDailyBatchLimitExceeded.__name__,
                }
            )
            attempt_end = (
                len(attempts) + 2 if last_was_budget_pause else task.max_attempts + 1
            )
            for attempt_number in range(len(attempts) + 1, attempt_end):
                task = task.model_copy(update={"state": TaskState.RUNNING})
                started = datetime.now(UTC)
                raw_uri = None
                completed_uris: list[str] = []
                attempts.append(
                    TaskAttempt(
                        attempt_id=(f"attempt-{task.idempotency_key[:24]}-{attempt_number}"),
                        task_id=task.task_id,
                        attempt_number=attempt_number,
                        state=TaskState.RUNNING,
                        checkpoint={"shard_id": shard.shard_id},
                        retry=RetryMetadata(max_attempts=task.max_attempts),
                        started_at=started,
                    )
                )
                self.store.save(task, tuple(attempts))
                try:
                    native = list(
                        self.fetch_bars(shard.instrument_ids, shard.start_date, shard.end_date)
                    )
                    last_stage = "raw_persist"
                    raw = self.storage.write_raw_object(
                        Path("pilot/raw/baostock")
                        / f"version={self.dataset_version}"
                        / f"shard={shard.shard_id}"
                        / f"attempt={attempt_number}.json",
                        _json_bytes(
                            {
                                "dataset_id": BACKFILL_DATASET_ID,
                                "dataset_version": self.dataset_version,
                                "universe_version": self.universe.version,
                                "shard": shard.model_dump(mode="json"),
                                "provider_contract": BAOSTOCK_LIVE_RUNTIME_CONTRACT.model_dump(
                                    mode="json"
                                ),
                                "records": native,
                            }
                        ),
                    )
                    raw_uri = raw.uri
                    completed_uris.append(raw.uri)
                    last_stage = "provider_schema_validation"
                    ProviderSchemaValidator().validate(
                        provider="baostock", records=native, schema=BAOSTOCK_LIVE_DAILY_SCHEMA
                    )
                    last_stage = "normalization"
                    selected = {key: self.instruments[key] for key in shard.instrument_ids}
                    rows, empty_observations = _normalize(native, selected, raw.uri, raw.sha256)
                    selected_universe = {
                        item.instrument_id: item
                        for item in self.universe.instruments
                        if item.instrument_id in shard.instrument_ids
                    }
                    quality = _quality(rows, shard, empty_observations, selected_universe)
                    last_stage = "quality_gate"
                    quality_artifact = self.storage.write_quality_report(
                        Path("pilot/quality")
                        / f"version={self.dataset_version}"
                        / f"shard={shard.shard_id}.json",
                        _json_bytes(quality),
                    )
                    completed_uris.append(quality_artifact.uri)
                    if quality["status"] == "failed":
                        raise ValueError("pilot shard quality gate failed")
                    parquet = None
                    if rows:
                        last_stage = "parquet_write"
                        parquet = self.storage.write_parquet_dataset(
                            Path("pilot/normalized")
                            / f"version={self.dataset_version}"
                            / f"year={shard.start_date.year}"
                            / f"{shard.shard_id}.parquet",
                            rows,
                        )
                        completed_uris.append(parquet.uri)
                    dates = [row["trading_date"] for row in rows]
                    checkpoint = {
                        "shard_id": shard.shard_id,
                        "row_count": len(rows),
                        "first_date": min(dates).isoformat() if dates else None,
                        "last_date": max(dates).isoformat() if dates else None,
                        "raw_sha256": raw.sha256,
                        "parquet_uri": parquet.uri if parquet else None,
                        "parquet_sha256": parquet.sha256 if parquet else None,
                        "quality_uri": quality_artifact.uri,
                        "duplicate_count": quality["duplicate_count"],
                        "anomaly_count": (
                            len(quality["anomalies"])
                            + int(quality["provider_empty_observation_count"])
                        ),
                        "missing_classifications": {
                            key: value["classification"]
                            for key, value in quality["missing_classifications"].items()
                        },
                    }
                    finished = datetime.now(UTC)
                    attempts[-1] = TaskAttempt(
                        attempt_id=f"attempt-{task.idempotency_key[:24]}-{attempt_number}",
                        task_id=task.task_id,
                        attempt_number=attempt_number,
                        state=TaskState.SUCCEEDED,
                        checkpoint=checkpoint,
                        retry=RetryMetadata(max_attempts=task.max_attempts),
                        started_at=started,
                        completed_at=finished,
                        records_received=len(rows),
                        raw_uri=raw.uri,
                    )
                    task = task.model_copy(update={"state": TaskState.SUCCEEDED})
                    self.store.save(task, tuple(attempts))
                    return BackfillShardResult(
                        shard_id=shard.shard_id,
                        task_id=task.task_id,
                        status="succeeded" if rows else "empty",
                        attempts=len(attempts),
                        instrument_ids=shard.instrument_ids,
                        start_date=shard.start_date,
                        end_date=shard.end_date,
                        row_count=len(rows),
                        first_date=min(dates) if dates else None,
                        last_date=max(dates) if dates else None,
                        raw_uri=raw.uri,
                        raw_sha256=raw.sha256,
                        parquet_uri=parquet.uri if parquet else None,
                        parquet_sha256=parquet.sha256 if parquet else None,
                        quality_uri=quality_artifact.uri,
                        duplicate_count=int(quality["duplicate_count"]),
                        anomaly_count=(
                            len(quality["anomalies"])
                            + int(quality["provider_empty_observation_count"])
                        ),
                        missing_classifications=checkpoint["missing_classifications"],
                    )
                except Exception as exc:  # noqa: BLE001 - isolate a failed shard and continue
                    last_error = exc
                    budget_pause = _is_baostock_budget_pause(exc)
                    retryable = budget_pause or (
                        _is_retryable_error(exc) and attempt_number < task.max_attempts
                    )
                    finished = datetime.now(UTC)
                    attempts[-1] = TaskAttempt(
                        attempt_id=f"attempt-{task.idempotency_key[:24]}-{attempt_number}",
                        task_id=task.task_id,
                        attempt_number=attempt_number,
                        state=(
                            TaskState.RETRYABLE_FAILED
                            if retryable
                            else TaskState.PERMANENTLY_FAILED
                        ),
                        checkpoint={"shard_id": shard.shard_id},
                        retry=RetryMetadata(
                            max_attempts=task.max_attempts,
                            retryable=retryable,
                            backoff_seconds=task.retry_backoff_seconds,
                            next_retry_at=(
                                finished + timedelta(seconds=task.retry_backoff_seconds)
                                if retryable
                                else None
                            ),
                            error_type=type(exc).__name__,
                            error_message=_sanitized_error_message(exc),
                        ),
                        started_at=started,
                        completed_at=finished,
                        records_received=0,
                        raw_uri=raw_uri,
                        failure_context=TaskFailureContext(
                            failure_stage=last_stage,
                            raw_uri=raw_uri,
                            completed_artifact_uris=tuple(completed_uris),
                            provider="baostock",
                            endpoint=BAOSTOCK_LIVE_RUNTIME_CONTRACT.endpoint,
                            job_id=shard.shard_id,
                        ),
                    )
                    task = task.model_copy(
                        update={
                            "state": (
                                TaskState.RETRYABLE_FAILED
                                if retryable
                                else TaskState.PERMANENTLY_FAILED
                            )
                        }
                    )
                    self.store.save(task, tuple(attempts))
                    if retryable and task.retry_backoff_seconds and not budget_pause:
                        time.sleep(task.retry_backoff_seconds)
                    if not retryable or budget_pause:
                        break
        assert last_error is not None
        return BackfillShardResult(
            shard_id=shard.shard_id,
            task_id=task.task_id,
            status="failed",
            attempts=len(attempts),
            instrument_ids=shard.instrument_ids,
            start_date=shard.start_date,
            end_date=shard.end_date,
            row_count=0,
            raw_uri=attempts[-1].raw_uri,
            error_class=type(last_error).__name__,
            error_stage=last_stage,
        )

    def run(self, shards: Sequence[BackfillShard]) -> BackfillPilotReport:
        if not shards:
            raise ValueError("backfill run requires at least one shard")
        invocation_started = datetime.now(UTC)
        results = tuple(self._run_shard(shard) for shard in shards)
        universe_artifact = self.storage.write_raw_object(
            Path("pilot/universe") / f"version={self.universe.version}" / "universe.json",
            _json_bytes(self.universe.model_dump(mode="json", exclude_computed_fields=True)),
        )
        inventory_entries: list[BackfillShardInventoryEntry] = []
        task_created_at: list[datetime] = []
        for result in results:
            state = self.store.load(result.task_id)
            if state is None:
                raise ValueError(f"missing task state for inventory: {result.task_id}")
            task_created_at.append(state.task.created_at)
            state_path = self.store.path_for(result.task_id)
            state_uri = f"quant-data://{state_path.relative_to(self.data_root).as_posix()}"
            raw_artifacts = tuple(
                _artifact_evidence(self.storage, kind="raw", uri=attempt.raw_uri)
                for attempt in state.attempts
                if attempt.raw_uri is not None
            )
            checkpoint = state.attempts[-1].checkpoint
            row_count = int(checkpoint.get("row_count", 0))
            if state.task.state is TaskState.SUCCEEDED:
                inventory_status = "succeeded" if row_count else "empty"
            else:
                inventory_status = "failed"
            parquet_uri = checkpoint.get("parquet_uri")
            quality_uri = checkpoint.get("quality_uri")
            if quality_uri is None and state.attempts[-1].failure_context is not None:
                quality_uri = next(
                    (
                        uri
                        for uri in state.attempts[-1].failure_context.completed_artifact_uris
                        if "/quality/" in uri
                    ),
                    None,
                )
            inventory_entries.append(
                BackfillShardInventoryEntry(
                    shard_id=result.shard_id,
                    task_id=result.task_id,
                    dataset_version=self.dataset_version,
                    instrument_ids=result.instrument_ids,
                    start_date=result.start_date,
                    end_date=result.end_date,
                    status=inventory_status,
                    attempts=len(state.attempts),
                    row_count=row_count,
                    first_date=checkpoint.get("first_date"),
                    last_date=checkpoint.get("last_date"),
                    raw_artifacts=raw_artifacts,
                    parquet_artifact=(
                        _artifact_evidence(
                            self.storage,
                            kind="parquet",
                            uri=str(parquet_uri),
                            row_count=row_count,
                        )
                        if parquet_uri
                        else None
                    ),
                    quality_artifact=(
                        _artifact_evidence(
                            self.storage,
                            kind="quality",
                            uri=str(quality_uri),
                            row_count=row_count,
                        )
                        if quality_uri
                        else None
                    ),
                    task_state_artifact=_artifact_evidence(
                        self.storage, kind="task_state", uri=state_uri
                    ),
                    missing_classifications=dict(checkpoint.get("missing_classifications", {})),
                    error_class=(
                        state.attempts[-1].retry.error_type
                        if state.task.state is not TaskState.SUCCEEDED
                        else None
                    ),
                    error_stage=(
                        state.attempts[-1].failure_context.failure_stage
                        if state.attempts[-1].failure_context is not None
                        else None
                    ),
                )
            )
        inventory = BackfillRunInventory(
            dataset_version=self.dataset_version,
            universe_version=self.universe.version,
            universe_content_sha256=self.universe.content_sha256,
            universe_artifact=BackfillArtifactEvidence(
                kind="universe",
                uri=universe_artifact.uri,
                sha256=universe_artifact.sha256,
                bytes=universe_artifact.path.stat().st_size,
                row_count=len(self.universe.instruments),
            ),
            created_at=min(task_created_at),
            shards=tuple(inventory_entries),
        )
        inventory_relative = (
            Path("pilot/inventory") / f"version={self.dataset_version}" / "run-inventory.json"
        )
        inventory_uri = f"quant-data://{inventory_relative.as_posix()}"
        inventory_path = self.data_root / inventory_relative
        if inventory_path.is_file():
            persisted_inventory = BackfillRunInventory.model_validate_json(
                inventory_path.read_bytes()
            )
            requested_ids = {item.shard_id for item in inventory_entries}
            persisted_ids = {item.shard_id for item in persisted_inventory.shards}
            if not requested_ids <= persisted_ids:
                raise ValueError("immutable run inventory does not cover requested shards")
            inventory_sha256 = hashlib.sha256(inventory_path.read_bytes()).hexdigest()
        else:
            inventory_artifact = self.storage.write_quality_report(
                inventory_relative,
                _json_bytes(inventory.model_dump(mode="json", exclude_computed_fields=True)),
            )
            inventory_sha256 = inventory_artifact.sha256
        query_uri, query_proof = rebuild_query_index(self.data_root, inventory_uri)
        total_rows = sum(item.row_count for item in results)
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        peak_rss_mb = rss / (1024 * 1024) if rss > 10_000_000 else rss / 1024
        completed = datetime.now(UTC)
        logical_started = min(task_created_at)
        wall = max(0.0, (completed - logical_started).total_seconds())
        report = BackfillPilotReport(
            dataset_version=self.dataset_version,
            universe_version=self.universe.version,
            universe_sha256=self.universe.content_sha256,
            started_at=logical_started,
            completed_at=completed,
            requested_start=min(item.start_date for item in shards),
            requested_end=max(item.end_date for item in shards),
            shard_results=results,
            total_rows=total_rows,
            succeeded_shards=sum(item.status == "succeeded" for item in results),
            empty_shards=sum(item.status == "empty" for item in results),
            failed_shards=sum(item.status == "failed" for item in results),
            replayed_shards=sum(item.status == "replayed" for item in results),
            retries=sum(max(0, item.attempts - 1) for item in results),
            duplicate_count=sum(item.duplicate_count for item in results),
            anomaly_count=sum(item.anomaly_count for item in results),
            wall_seconds=wall,
            rows_per_second=total_rows / wall if wall else 0,
            peak_rss_mb=peak_rss_mb,
            inventory_uri=inventory_uri,
            inventory_sha256=inventory_sha256,
            query_index_uri=query_uri,
            query_index_rebuildable=True,
            clean_single_version=True,
            query_proof=query_proof,
        )
        self.storage.write_quality_report(
            Path("pilot/reports")
            / f"version={self.dataset_version}"
            / f"run={invocation_started.strftime('%Y%m%dT%H%M%S%fZ')}.json",
            _json_bytes(report.model_dump(mode="json")),
        )
        return report
