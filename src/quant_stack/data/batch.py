from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime
from datetime import time as datetime_time
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import Field, model_validator

from .catalog.duckdb_catalog import manifest_sha256
from .cn_qfq_dataset import read_materialized_cn_qfq_rows
from .market_bars_dataset import market_bars_relation_sql
from .models import (
    CalendarEvidence,
    DataModel,
    DatasetKind,
    DatasetLayer,
    DatasetManifest,
    Market,
    MarketStateSample,
)
from .publication import (
    ConsumerEligibilityPolicy,
    PublicationBundle,
    PublicationReceipt,
    PublicationState,
    canonical_consumer_eligibility,
    find_publication_receipt_for_manifest,
)
from .semantics import (
    DEFAULT_DATASET_SEMANTICS,
    DatasetSemanticsRegistry,
    ResearchQueryCapability,
)
from .us_total_return_dataset import read_materialized_us_total_return_rows

logger = logging.getLogger(__name__)

BATCH_QUERY_CONTRACT_VERSION = "1.1.0"
InstrumentIdentifier = Annotated[str, Field(min_length=3, max_length=64)]
SymbolIdentifier = Annotated[str, Field(min_length=1, max_length=32)]


class BatchError(RuntimeError):
    """Base class for errors with a fixed, client-safe detail."""

    detail = "canonical batch request failed"

    def __init__(self, detail: str | None = None) -> None:
        self.detail = detail or type(self).detail
        super().__init__(self.detail)


class BatchRequestError(BatchError):
    pass


class BatchDatasetNotFoundError(BatchError):
    detail = "eligible canonical dataset version was not found"


class BatchSnapshotError(BatchError):
    detail = "canonical dataset snapshot is unavailable"


class BatchPayloadError(BatchError):
    detail = "canonical dataset payload query failed"


class BatchInstrumentQuery(DataModel):
    market: Market | None = None
    instrument_ids: tuple[InstrumentIdentifier, ...] = Field(default=(), max_length=10_000)
    symbols: tuple[SymbolIdentifier, ...] = Field(default=(), max_length=10_000)
    venues: tuple[Annotated[str, Field(min_length=2, max_length=16)], ...] = Field(
        default=(), max_length=32
    )
    active: bool | None = None
    dataset_id: str | None = None
    dataset_version: str = "latest"
    page_size: int = Field(default=1_000, ge=1, le=10_000)
    cursor: str | None = None

    @model_validator(mode="after")
    def require_dataset_for_explicit_version(self) -> BatchInstrumentQuery:
        if self.dataset_version != "latest" and self.dataset_id is None:
            raise ValueError("explicit dataset_version requires dataset_id")
        return self


class BatchBarsQuery(DataModel):
    instrument_ids: tuple[InstrumentIdentifier, ...] = Field(min_length=1, max_length=10_000)
    start: date | None = None
    end: date | None = None
    frequency: str = "1d"
    market: Market | None = None
    dataset_id: str | None = None
    dataset_version: str = "latest"
    page_size: int = Field(default=10_000, ge=1, le=100_000)
    cursor: str | None = None

    @model_validator(mode="after")
    def validate_query(self) -> BatchBarsQuery:
        if self.dataset_version != "latest" and self.dataset_id is None:
            raise ValueError("explicit dataset_version requires dataset_id")
        if (self.start is None) != (self.end is None):
            raise ValueError("start and end must be provided together")
        if self.start is None or self.end is None:
            raise ValueError("bounded start and end are required")
        if self.end < self.start:
            raise ValueError("end cannot be earlier than start")
        return self


class BatchInstrumentItem(DataModel):
    instrument_id: str
    symbol: str
    venue: str
    currency: str
    timezone: str
    active: bool
    dataset_id: str
    dataset_version: str
    manifest_fingerprint: str


class BatchBarItem(DataModel):
    instrument_id: str
    ts_event: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    volume_unit: Literal["shares"] = "shares"
    amount: float | None = None
    currency: str
    adjustment_semantics: str
    dataset_id: str
    dataset_version: str
    quality_status: Literal["passed", "warning"]
    manifest_fingerprint: str


class BatchInstrumentResponse(DataModel):
    items: tuple[BatchInstrumentItem, ...]
    dataset_id: str
    dataset_version: str
    quality_status: Literal["passed", "warning"]
    manifest_fingerprint: str
    snapshot_token: str
    next_cursor: str | None = None
    etag: str


class BatchBarsResponse(DataModel):
    items: tuple[BatchBarItem, ...]
    dataset_id: str
    dataset_version: str
    quality_status: Literal["passed", "warning"]
    manifest_fingerprint: str
    snapshot_token: str
    next_cursor: str | None = None
    etag: str


class BatchDatasetMetadata(DataModel):
    dataset_id: str
    dataset_version: str
    kind: DatasetKind
    market: Market
    frequency: str
    row_count: int
    quality_status: Literal["passed", "warning"]
    manifest_fingerprint: str
    snapshot_token: str
    query_contract_version: Literal["1.1.0"] = BATCH_QUERY_CONTRACT_VERSION
    semantics_schema_version: str | None = None
    semantics_fingerprint: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    calendar_evidence: CalendarEvidence | None = None
    research_capability: ResearchQueryCapability | None = None
    calendar_capability: dict[str, object] | None = None
    market_state_capability: dict[str, object] | None = None
    market_state_sample: MarketStateSample | None = None
    routing_evidence: dict[str, object] | None = None
    publication_gate: Literal["eligible", "incomplete", "blocked", "quarantined"]
    publication_state: PublicationState
    consumer_eligibility: ConsumerEligibilityPolicy
    comparison_status: Literal[
        "not_required",
        "passed",
        "unavailable",
        "serious_discrepancy",
        "source_identity_drift",
    ]
    comparison_required: bool
    comparison_complete: bool
    canonical_source_provider: str
    canonical_source_endpoint: str
    selection_rule_id: str
    publication_evidence_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    shadow_eligible: bool
    candidate_eligible: bool
    cutover_eligible: bool
    etag: str


@dataclass(frozen=True)
class CanonicalDatasetSnapshot:
    manifest: DatasetManifest
    fingerprint: str
    manifest_relative: str | None = None


@dataclass
class BatchQueryMetrics:
    catalog_queries: int = 0
    payload_queries: int = 0
    duckdb_connections: int = 0
    parquet_scans: int = 0
    prepare_latency_ms: float = 0.0
    payload_latency_ms: float = 0.0

    @property
    def total_repository_latency_ms(self) -> float:
        return self.prepare_latency_ms + self.payload_latency_ms

    def response_headers(self) -> dict[str, str]:
        return {
            "X-Quant-Catalog-Queries": str(self.catalog_queries),
            "X-Quant-Payload-Queries": str(self.payload_queries),
            "X-Quant-DuckDB-Connections": str(self.duckdb_connections),
            "X-Quant-Parquet-Scans": str(self.parquet_scans),
            "Server-Timing": (
                f"prepare;dur={self.prepare_latency_ms:.3f}, "
                f"payload;dur={self.payload_latency_ms:.3f}, "
                f"total;dur={self.total_repository_latency_ms:.3f}"
            ),
        }


@dataclass
class PreparedBatchQuery:
    operation: str
    snapshot: CanonicalDatasetSnapshot
    snapshot_token: str
    etag: str
    query_hash: str
    after: tuple[str, ...] | None
    payload_path: Path
    connection: Any
    metrics: BatchQueryMetrics
    publication_receipt: PublicationReceipt
    semantics_fingerprint: str | None = None

    def close(self) -> None:
        if self.connection is not None:
            self.connection.close()
            self.connection = None


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _sha256_payload(value: object) -> str:
    return hashlib.sha256(_canonical_json(value).encode()).hexdigest()


def _snapshot_token(snapshot: CanonicalDatasetSnapshot) -> str:
    return _sha256_payload(
        {
            "dataset_id": snapshot.manifest.dataset_id,
            "dataset_version": snapshot.manifest.version,
            "manifest_fingerprint": snapshot.fingerprint,
            "quality_eligibility": snapshot.manifest.quality_status,
            "query_contract_version": BATCH_QUERY_CONTRACT_VERSION,
        }
    )


def _representation_etag(operation: str, snapshot_token: str, page_request: object) -> str:
    payload = {
        "operation": operation,
        "snapshot_token": snapshot_token,
        "page_request": page_request,
    }
    return f'"{_sha256_payload(payload)}"'


def _encode_cursor(payload: dict[str, object]) -> str:
    value = base64.urlsafe_b64encode(_canonical_json(payload).encode()).decode()
    return value.rstrip("=")


def _decode_cursor(value: str) -> dict[str, object]:
    try:
        padding = "=" * (-len(value) % 4)
        payload = json.loads(base64.urlsafe_b64decode(value + padding))
    except (ValueError, json.JSONDecodeError) as exc:
        raise BatchRequestError("batch cursor is malformed") from exc
    if not isinstance(payload, dict) or payload.get("v") != 1:
        raise BatchRequestError("batch cursor version is unsupported")
    return payload


class CanonicalBatchRepository:
    """Fail-closed canonical reader with measured storage work per page."""

    def __init__(
        self,
        data_root: str | Path,
        *,
        semantics_registry: DatasetSemanticsRegistry = DEFAULT_DATASET_SEMANTICS,
        consumer_access: Literal["candidate", "shadow"] = "candidate",
    ) -> None:
        if consumer_access not in {"candidate", "shadow"}:
            raise ValueError("unsupported canonical Batch consumer access")
        self.data_root = Path(data_root).expanduser().resolve()
        self.semantics_registry = semantics_registry
        self.consumer_access = consumer_access

    @property
    def max_instruments(self) -> int:
        return int(os.environ.get("QUANT_BATCH_MAX_INSTRUMENTS", "6000"))

    @property
    def max_page_size(self) -> int:
        return int(os.environ.get("QUANT_BATCH_MAX_PAGE_SIZE", "100000"))

    @property
    def max_date_range_days(self) -> int:
        return int(os.environ.get("QUANT_BATCH_MAX_DATE_RANGE_DAYS", "3660"))

    def _resolve_relative(self, relative: str) -> Path:
        value = Path(relative)
        if value.is_absolute():
            raise BatchSnapshotError
        resolved = (self.data_root / value).resolve()
        if self.data_root not in resolved.parents:
            raise BatchSnapshotError
        return resolved

    def _resolve_storage_uri(self, uri: str) -> Path:
        if not uri.startswith("quant-data://normalized/") or "/legacy/" in uri:
            raise BatchSnapshotError
        return self._resolve_relative(uri.removeprefix("quant-data://"))

    def _connect_catalog(self, metrics: BatchQueryMetrics):
        import duckdb

        catalog_path = self.data_root / "catalog/quant.duckdb"
        if not catalog_path.is_file():
            raise BatchSnapshotError
        try:
            connection = duckdb.connect(str(catalog_path), read_only=True)
        except Exception as exc:
            raise BatchSnapshotError from exc
        metrics.duckdb_connections += 1
        return connection

    def _authoritative_manifests(
        self,
        connection,
        metrics: BatchQueryMetrics,
        *,
        dataset_id: str | None = None,
        dataset_version: str = "latest",
    ) -> list[tuple[DatasetManifest, str]]:
        pointer_path = self.data_root / "catalog/current-manifest-set.json"
        if not pointer_path.is_file():
            raise BatchSnapshotError
        try:
            pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise BatchSnapshotError from exc
        if pointer.get("schema_version") != "1.0.0":
            raise BatchSnapshotError
        relatives = pointer.get("manifests")
        if not isinstance(relatives, list) or len(relatives) != len(set(relatives)):
            raise BatchSnapshotError
        exact_selection = dataset_id is not None and dataset_version != "latest"
        selected_relatives = relatives
        if exact_selection:
            expected_name = f"{dataset_id}@{dataset_version}.json"
            selected_relatives = [
                relative
                for relative in relatives
                if isinstance(relative, str) and Path(relative).name == expected_name
            ]
            if len(selected_relatives) != 1:
                raise BatchDatasetNotFoundError

        manifests: list[tuple[DatasetManifest, str]] = []
        pointer_rows: dict[tuple[str, str], str] = {}
        try:
            for relative in selected_relatives:
                if not isinstance(relative, str):
                    raise BatchSnapshotError
                path = self._resolve_relative(relative)
                if not path.is_file():
                    raise BatchSnapshotError
                manifest = DatasetManifest.model_validate_json(path.read_text(encoding="utf-8"))
                key = (manifest.dataset_id, manifest.version)
                if key in pointer_rows:
                    raise BatchSnapshotError
                pointer_rows[key] = manifest_sha256(manifest)
                manifests.append((manifest, relative))
            if connection is None:
                if not exact_selection:
                    raise BatchSnapshotError
                return manifests
            metrics.catalog_queries += 1
            if exact_selection:
                catalog_rows = {
                    (row[0], row[1]): row[2]
                    for row in connection.execute(
                        "SELECT dataset_id, version, manifest_sha256 FROM datasets "
                        "WHERE dataset_id = ? AND version = ?",
                        [dataset_id, dataset_version],
                    ).fetchall()
                }
            else:
                catalog_rows = {
                    (row[0], row[1]): row[2]
                    for row in connection.execute(
                        "SELECT dataset_id, version, manifest_sha256 FROM datasets"
                    ).fetchall()
                }
        except BatchError:
            raise
        except Exception as exc:
            raise BatchSnapshotError from exc
        if catalog_rows != pointer_rows:
            raise BatchSnapshotError
        return manifests

    def _verify_exact_publication_bundle(
        self,
        snapshot: CanonicalDatasetSnapshot,
        receipt: PublicationReceipt,
    ) -> None:
        """Bind an exact immutable read to its published Bundle without opening Catalog."""
        if (
            receipt.evidence_status != "verified"
            or receipt.bundle_uri is None
            or receipt.bundle_sha256 is None
            or receipt.dataset_id != snapshot.manifest.dataset_id
            or receipt.dataset_version != snapshot.manifest.version
        ):
            raise BatchSnapshotError
        manifest_uri = f"quant-data://{snapshot.manifest_relative}"
        if snapshot.manifest_relative is None or manifest_uri not in receipt.manifest_uris:
            raise BatchSnapshotError
        if not receipt.bundle_uri.startswith("quant-data://"):
            raise BatchSnapshotError
        bundle_path = self._resolve_relative(receipt.bundle_uri.removeprefix("quant-data://"))
        try:
            payload = bundle_path.read_bytes()
            if hashlib.sha256(payload).hexdigest() != receipt.bundle_sha256:
                raise BatchSnapshotError
            bundle = PublicationBundle.model_validate_json(payload)
        except BatchError:
            raise
        except Exception as exc:
            raise BatchSnapshotError from exc
        matching = [
            item
            for item in bundle.proposed_manifests
            if (item.dataset_id, item.version)
            == (snapshot.manifest.dataset_id, snapshot.manifest.version)
        ]
        if (
            bundle.bundle_id != receipt.bundle_id
            or (bundle.dataset_id, bundle.dataset_version)
            != (receipt.dataset_id, receipt.dataset_version)
            or len(matching) != 1
            or manifest_sha256(matching[0]) != snapshot.fingerprint
            or bundle.publication_state != receipt.publication_state
            or bundle.consumer_eligibility != receipt.consumer_eligibility
        ):
            raise BatchSnapshotError

    def _select_snapshot(
        self,
        manifests: list[DatasetManifest | tuple[DatasetManifest, str]],
        *,
        kind: DatasetKind | None,
        dataset_id: str | None,
        dataset_version: str,
        market: Market | None,
        frequency: str | None,
    ) -> CanonicalDatasetSnapshot:
        manifest_entries = [
            (item, None) if isinstance(item, DatasetManifest) else item
            for item in manifests
        ]
        candidates = [
            (item, relative)
            for item, relative in manifest_entries
            if (kind is None or item.kind == kind)
            and item.layer == DatasetLayer.NORMALIZED
            and item.quality_status in {"passed", "warning"}
            and item.storage_uri.startswith("quant-data://normalized/")
            and "/legacy/" not in item.storage_uri
            and (dataset_id is None or item.dataset_id == dataset_id)
            and (dataset_version == "latest" or item.version == dataset_version)
            and (market is None or item.market == market)
            and (frequency is None or item.frequency == frequency)
            and self._semantics_consumer_eligible(item)
        ]
        if not candidates:
            raise BatchDatasetNotFoundError
        manifest, manifest_relative = max(
            candidates,
            key=lambda pair: (
                pair[0].created_at,
                pair[0].version,
                pair[0].dataset_id,
            ),
        )
        payload_path = self._resolve_storage_uri(manifest.storage_uri)
        if not payload_path.is_file():
            raise BatchSnapshotError
        return CanonicalDatasetSnapshot(
            manifest,
            manifest_sha256(manifest),
            manifest_relative,
        )

    def _semantics_consumer_eligible(self, manifest: DatasetManifest) -> bool:
        if not self.semantics_registry.consumer_eligible(manifest.dataset_id, "batch_api"):
            return False
        semantics = self.semantics_registry.find(manifest.dataset_id)
        if semantics is None:
            return True
        try:
            semantics.validate_manifest(manifest)
        except ValueError:
            return False
        return True

    def _validate_limits(self, *, instruments: int, page_size: int) -> None:
        if instruments > self.max_instruments:
            raise BatchRequestError("batch instrument limit exceeded")
        if page_size > self.max_page_size:
            raise BatchRequestError("batch page size limit exceeded")

    def _cursor_after(
        self,
        cursor: str | None,
        *,
        snapshot_token: str,
        query_hash: str,
        fields: int,
    ) -> tuple[str, ...] | None:
        if cursor is None:
            return None
        payload = _decode_cursor(cursor)
        after = payload.get("after")
        if (
            payload.get("snapshot_token") != snapshot_token
            or payload.get("query_sha256") != query_hash
            or not isinstance(after, list)
            or len(after) != fields
        ):
            raise BatchRequestError("batch cursor does not match this request")
        return tuple(str(value) for value in after)

    @staticmethod
    def _placeholders(values: tuple[str, ...]) -> str:
        return ",".join("?" for _ in values)

    def _prepare(
        self,
        *,
        operation: str,
        kind: DatasetKind | None,
        dataset_id: str | None,
        dataset_version: str,
        market: Market | None,
        frequency: str | None,
        query_payload: dict[str, object],
        cursor_fields: int,
    ) -> PreparedBatchQuery:
        started = time.perf_counter()
        metrics = BatchQueryMetrics()
        exact_bundle_read = (
            operation == "batch_bars"
            and dataset_id is not None
            and dataset_version != "latest"
        )
        connection = None if exact_bundle_read else self._connect_catalog(metrics)
        try:
            manifests = self._authoritative_manifests(
                connection,
                metrics,
                dataset_id=dataset_id,
                dataset_version=dataset_version,
            )
            snapshot = self._select_snapshot(
                manifests,
                kind=kind,
                dataset_id=dataset_id,
                dataset_version=dataset_version,
                market=market,
                frequency=frequency,
            )
            receipt = find_publication_receipt_for_manifest(
                self.data_root,
                snapshot.manifest,
                manifest_relative=snapshot.manifest_relative,
            )
            if receipt is None or receipt.publication_state is None:
                raise BatchDatasetNotFoundError
            if exact_bundle_read:
                self._verify_exact_publication_bundle(snapshot, receipt)
            state = receipt.publication_state
            consumer_eligible = (
                state.shadow_eligible
                if self.consumer_access == "shadow"
                else state.candidate_eligible
            )
            if not consumer_eligible:
                raise BatchDatasetNotFoundError
            token = _snapshot_token(snapshot)
            filter_payload = {
                key: value
                for key, value in query_payload.items()
                if key not in {"cursor", "page_size"}
            }
            query_hash = _sha256_payload(filter_payload)
            after = self._cursor_after(
                query_payload.get("cursor"),
                snapshot_token=token,
                query_hash=query_hash,
                fields=cursor_fields,
            )
            semantics_fingerprint = (
                self.semantics_registry.fingerprint(snapshot.manifest.dataset_id)
                if operation == "dataset_metadata"
                else None
            )
            etag_payload = query_payload
            if operation == "dataset_metadata":
                etag_payload = {
                    **query_payload,
                    "semantics_fingerprint": semantics_fingerprint,
                    "publication_evidence_fingerprint": (receipt.publication_evidence_fingerprint),
                }
            etag = _representation_etag(operation, token, etag_payload)
            metrics.prepare_latency_ms = (time.perf_counter() - started) * 1000
            return PreparedBatchQuery(
                operation=operation,
                snapshot=snapshot,
                snapshot_token=token,
                etag=etag,
                query_hash=query_hash,
                after=after,
                payload_path=self._resolve_storage_uri(snapshot.manifest.storage_uri),
                connection=connection,
                metrics=metrics,
                publication_receipt=receipt,
                semantics_fingerprint=semantics_fingerprint,
            )
        except Exception:
            if connection is not None:
                connection.close()
            raise

    def prepare_instruments(self, query: BatchInstrumentQuery) -> PreparedBatchQuery:
        self._validate_limits(
            instruments=len(query.instrument_ids) + len(query.symbols),
            page_size=query.page_size,
        )
        return self._prepare(
            operation="batch_instruments",
            kind=DatasetKind.INSTRUMENTS,
            dataset_id=query.dataset_id,
            dataset_version=query.dataset_version,
            market=query.market,
            frequency=None,
            query_payload=query.model_dump(mode="json"),
            cursor_fields=1,
        )

    def prepare_bars(self, query: BatchBarsQuery) -> PreparedBatchQuery:
        self._validate_limits(instruments=len(query.instrument_ids), page_size=query.page_size)
        assert query.start is not None and query.end is not None
        if (query.end - query.start).days > self.max_date_range_days:
            raise BatchRequestError("batch date range limit exceeded")
        return self._prepare(
            operation="batch_bars",
            kind=DatasetKind.MARKET_BARS,
            dataset_id=query.dataset_id,
            dataset_version=query.dataset_version,
            market=query.market,
            frequency=query.frequency,
            query_payload=query.model_dump(mode="json"),
            cursor_fields=2,
        )

    def prepare_metadata(self, dataset_id: str, version: str = "latest") -> PreparedBatchQuery:
        return self._prepare(
            operation="dataset_metadata",
            kind=DatasetKind.MARKET_BARS,
            dataset_id=dataset_id,
            dataset_version=version,
            market=None,
            frequency=None,
            query_payload={"dataset_id": dataset_id, "version": version},
            cursor_fields=0,
        )

    def execute_instruments(
        self, plan: PreparedBatchQuery, query: BatchInstrumentQuery
    ) -> BatchInstrumentResponse:
        conditions: list[str] = []
        parameters: list[object] = []
        if query.instrument_ids:
            conditions.append(f"instrument_id IN ({self._placeholders(query.instrument_ids)})")
            parameters.extend(query.instrument_ids)
        if query.symbols:
            conditions.append(f"upper(symbol) IN ({self._placeholders(query.symbols)})")
            parameters.extend(value.upper() for value in query.symbols)
        if query.venues:
            conditions.append(f"venue IN ({self._placeholders(query.venues)})")
            parameters.extend(query.venues)
        if query.active is not None:
            conditions.append("active = ?")
            parameters.append(query.active)
        if plan.after is not None:
            conditions.append("instrument_id > ?")
            parameters.append(plan.after[0])
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        sql = f"""
            SELECT instrument_id, symbol, venue, currency, timezone, active
            FROM read_parquet(?)
            {where}
            ORDER BY instrument_id
            LIMIT ?
        """
        rows = self._payload_rows(
            plan, sql, [str(plan.payload_path), *parameters, query.page_size + 1]
        )
        has_more = len(rows) > query.page_size
        rows = rows[: query.page_size]
        manifest = plan.snapshot.manifest
        items = tuple(
            BatchInstrumentItem(
                instrument_id=row[0],
                symbol=row[1],
                venue=row[2],
                currency=row[3],
                timezone=row[4],
                active=row[5],
                dataset_id=manifest.dataset_id,
                dataset_version=manifest.version,
                manifest_fingerprint=plan.snapshot.fingerprint,
            )
            for row in rows
        )
        next_cursor = None
        if has_more and items:
            next_cursor = _encode_cursor(
                {
                    "v": 1,
                    "snapshot_token": plan.snapshot_token,
                    "query_sha256": plan.query_hash,
                    "after": [items[-1].instrument_id],
                }
            )
        response = BatchInstrumentResponse(
            items=items,
            dataset_id=manifest.dataset_id,
            dataset_version=manifest.version,
            quality_status=manifest.quality_status,
            manifest_fingerprint=plan.snapshot.fingerprint,
            snapshot_token=plan.snapshot_token,
            next_cursor=next_cursor,
            etag=plan.etag,
        )
        self._log_query(plan, len(items), len(_canonical_json(response.model_dump())))
        return response

    def execute_bars(self, plan: PreparedBatchQuery, query: BatchBarsQuery) -> BatchBarsResponse:
        assert query.start is not None and query.end is not None
        conditions = [
            f"instrument_id IN ({self._placeholders(query.instrument_ids)})",
            "ts_event >= ?",
            "ts_event <= ?",
        ]
        parameters: list[object] = [
            *query.instrument_ids,
            datetime.combine(query.start, datetime_time.min, tzinfo=UTC),
            datetime.combine(query.end, datetime_time.max, tzinfo=UTC),
        ]
        if plan.after is not None:
            conditions.append("(instrument_id > ? OR (instrument_id = ? AND ts_event > ?))")
            parameters.extend((plan.after[0], plan.after[0], datetime.fromisoformat(plan.after[1])))
        rows = self._materialized_payload_rows(
            plan,
            query,
            start_ts=parameters[len(query.instrument_ids)],
            end_ts=parameters[len(query.instrument_ids) + 1],
        )
        if rows is None:
            relation = market_bars_relation_sql(
                plan.payload_path, instrument_ids=query.instrument_ids
            )
            sql = f"""
                SELECT instrument_id, ts_event, open, high, low, close, volume, amount,
                       currency, coalesce(lineage.adjustment, 'unknown')
                FROM {relation}
                WHERE {" AND ".join(conditions)}
                ORDER BY instrument_id, ts_event
                LIMIT ?
            """
            rows = self._payload_rows(plan, sql, [*parameters, query.page_size + 1])
        has_more = len(rows) > query.page_size
        rows = rows[: query.page_size]
        manifest = plan.snapshot.manifest
        items = tuple(
            BatchBarItem(
                instrument_id=row[0],
                ts_event=row[1],
                open=row[2],
                high=row[3],
                low=row[4],
                close=row[5],
                volume=row[6],
                amount=row[7],
                currency=row[8],
                adjustment_semantics=row[9],
                dataset_id=manifest.dataset_id,
                dataset_version=manifest.version,
                quality_status=manifest.quality_status,
                manifest_fingerprint=plan.snapshot.fingerprint,
            )
            for row in rows
        )
        next_cursor = None
        if has_more and items:
            next_cursor = _encode_cursor(
                {
                    "v": 1,
                    "snapshot_token": plan.snapshot_token,
                    "query_sha256": plan.query_hash,
                    "after": [items[-1].instrument_id, items[-1].ts_event.isoformat()],
                }
            )
        response = BatchBarsResponse(
            items=items,
            dataset_id=manifest.dataset_id,
            dataset_version=manifest.version,
            quality_status=manifest.quality_status,
            manifest_fingerprint=plan.snapshot.fingerprint,
            snapshot_token=plan.snapshot_token,
            next_cursor=next_cursor,
            etag=plan.etag,
        )
        self._log_query(plan, len(items), len(_canonical_json(response.model_dump())))
        return response

    def metadata_response(self, plan: PreparedBatchQuery) -> BatchDatasetMetadata:
        manifest = plan.snapshot.manifest
        receipt = plan.publication_receipt
        state = receipt.publication_state
        assert state is not None
        assert receipt.publication_gate is not None
        assert receipt.comparison_status is not None
        assert receipt.comparison_required is not None
        assert receipt.canonical_source_provider is not None
        assert receipt.canonical_source_endpoint is not None
        assert receipt.selection_rule_id is not None
        assert receipt.publication_evidence_fingerprint is not None
        semantics = self.semantics_registry.find(manifest.dataset_id)
        if semantics is not None:
            semantics.validate_manifest(manifest)
        routing_evidence = None
        calendar_capability = None
        market_state_capability = None
        if semantics is not None:
            routing_evidence = semantics.routing.model_dump(mode="json")
            for route in routing_evidence["routes"]:
                route.pop("evidence_refs", None)
            for route in routing_evidence["cross_validation"]:
                route.pop("evidence_refs", None)
            if semantics.calendar is not None:
                calendar_capability = semantics.calendar.model_dump(mode="json")
                calendar_capability.pop("evidence_refs", None)
            if semantics.market_states is not None:
                market_state_capability = semantics.market_states.model_dump(mode="json")
                market_state_capability.pop("evidence_refs", None)
        return BatchDatasetMetadata(
            dataset_id=manifest.dataset_id,
            dataset_version=manifest.version,
            kind=manifest.kind,
            market=manifest.market,
            frequency=manifest.frequency,
            row_count=manifest.row_count,
            quality_status=manifest.quality_status,
            manifest_fingerprint=plan.snapshot.fingerprint,
            snapshot_token=plan.snapshot_token,
            semantics_schema_version=(semantics.schema_version if semantics is not None else None),
            semantics_fingerprint=getattr(plan, "semantics_fingerprint", None),
            calendar_evidence=manifest.calendar_evidence,
            research_capability=semantics.research if semantics is not None else None,
            calendar_capability=calendar_capability,
            market_state_capability=market_state_capability,
            market_state_sample=manifest.market_state_sample,
            routing_evidence=routing_evidence,
            publication_gate=receipt.publication_gate,
            publication_state=state,
            consumer_eligibility=getattr(
                receipt, "consumer_eligibility", canonical_consumer_eligibility()
            ),
            comparison_status=receipt.comparison_status,
            comparison_required=receipt.comparison_required,
            comparison_complete=state.comparison_complete,
            canonical_source_provider=receipt.canonical_source_provider,
            canonical_source_endpoint=receipt.canonical_source_endpoint,
            selection_rule_id=receipt.selection_rule_id,
            publication_evidence_fingerprint=(receipt.publication_evidence_fingerprint),
            shadow_eligible=state.shadow_eligible,
            candidate_eligible=state.candidate_eligible,
            cutover_eligible=state.cutover_eligible,
            etag=plan.etag,
        )

    @staticmethod
    def _materialized_payload_rows(
        plan: PreparedBatchQuery,
        query: BatchBarsQuery,
        *,
        start_ts: datetime,
        end_ts: datetime,
    ):
        started = time.perf_counter()
        market = query.market or plan.snapshot.manifest.market
        reader = {
            "CN": read_materialized_cn_qfq_rows,
            "US": read_materialized_us_total_return_rows,
        }.get(market)
        if reader is None:
            return None
        try:
            rows = reader(
                plan.payload_path,
                instrument_ids=query.instrument_ids,
                start_ts=start_ts,
                end_ts=end_ts,
                after=plan.after,
                limit=query.page_size + 1,
            )
        except Exception as exc:
            raise BatchPayloadError from exc
        if rows is None:
            return None
        plan.metrics.payload_queries += 1
        plan.metrics.parquet_scans += 1
        plan.metrics.payload_latency_ms = (time.perf_counter() - started) * 1000
        return rows

    @staticmethod
    def _payload_rows(plan: PreparedBatchQuery, sql: str, parameters: list[object]):
        started = time.perf_counter()
        plan.metrics.payload_queries += 1
        plan.metrics.parquet_scans += 1
        try:
            if plan.connection is None:
                import duckdb

                plan.connection = duckdb.connect()
                plan.metrics.duckdb_connections += 1
            return plan.connection.execute(sql, parameters).fetchall()
        except Exception as exc:
            raise BatchPayloadError from exc
        finally:
            plan.metrics.payload_latency_ms = (time.perf_counter() - started) * 1000

    @staticmethod
    def _log_query(plan: PreparedBatchQuery, rows: int, response_bytes: int) -> None:
        metrics = plan.metrics
        logger.info(
            "canonical batch %s rows=%d bytes=%d prepare_ms=%.3f payload_ms=%.3f "
            "total_ms=%.3f catalog_queries=%d payload_queries=%d connections=%d "
            "parquet_scans=%d dataset=%s version=%s snapshot_prefix=%s etag_prefix=%s",
            plan.operation,
            rows,
            response_bytes,
            metrics.prepare_latency_ms,
            metrics.payload_latency_ms,
            metrics.total_repository_latency_ms,
            metrics.catalog_queries,
            metrics.payload_queries,
            metrics.duckdb_connections,
            metrics.parquet_scans,
            plan.snapshot.manifest.dataset_id,
            plan.snapshot.manifest.version,
            plan.snapshot_token[:12],
            plan.etag[1:13],
        )
