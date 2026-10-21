"""Durable, versioned Daily Snapshot publications.

The browser read path must not rebuild a full-market aggregate when Redis or
process memory is empty.  A scan analytics transaction therefore stores the
validated JSON response as a first-class database publication before exposing
the scan's analytics readiness marker.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any

SNAPSHOT_PUBLICATION_VERSION = "quantstack.daily-snapshot-publication.v1"
SNAPSHOT_PUBLICATION_INPUT_VERSION = "quantstack.daily-snapshot-publication-input.v1"


class SnapshotPublicationUnavailable(RuntimeError):
    """Raised when no valid publication matches the selected scan exactly."""


class _BorrowedSqlUnitOfWork:
    """Expose upstream repositories on an owner-managed SQLAlchemy session."""

    def __init__(self, session: Any) -> None:
        from app.infra.db.repositories.feature_run_repo import SqlFeatureRunRepository
        from app.infra.db.repositories.feature_store_repo import SqlFeatureStoreRepository
        from app.infra.db.repositories.scan_repo import SqlScanRepository
        from app.infra.db.repositories.scan_result_repo import SqlScanResultRepository
        from app.infra.db.repositories.universe_repo import SqlUniverseRepository

        self.session = session
        self.scans = SqlScanRepository(session)
        self.scan_results = SqlScanResultRepository(session)
        self.universe = SqlUniverseRepository(session)
        self.feature_runs = SqlFeatureRunRepository(session)
        self.feature_store = SqlFeatureStoreRepository(session)

    def __enter__(self) -> _BorrowedSqlUnitOfWork:
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        # Transaction and session ownership remain with sync_scan_analytics.
        return None

    def commit(self) -> None:
        raise RuntimeError("borrowed snapshot publication UoW cannot commit")

    def rollback(self) -> None:
        raise RuntimeError("borrowed snapshot publication UoW cannot roll back")


def _payload_digest(payload_json: str) -> str:
    return hashlib.sha256(payload_json.encode("utf-8")).hexdigest()


def _serialize_key_markets(entries: Any) -> str:
    if not isinstance(entries, list) or any(not isinstance(item, dict) for item in entries):
        raise SnapshotPublicationUnavailable("snapshot key-markets input is invalid")
    return json.dumps(entries, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _publication_input(
    db: Any,
    *,
    scan: Any,
    market: str,
    analytics_version: str,
    snapshot_schema_version: int,
    capture: bool,
) -> list[dict[str, Any]]:
    """Capture once or load the committed key-markets leaf for publication."""

    from app.models.daily_snapshot_publication import DailySnapshotPublicationInput
    from app.services import daily_snapshot_service

    normalized = market.upper()
    row = (
        db.query(DailySnapshotPublicationInput)
        .filter_by(
            market=normalized,
            scan_id=scan.scan_id,
            analytics_version=analytics_version,
            snapshot_schema_version=snapshot_schema_version,
        )
        .one_or_none()
    )
    if capture:
        entries = daily_snapshot_service.build_key_market_entries(
            db,
            normalized,
            as_of_date=daily_snapshot_service._snapshot_anchor_date(scan),
        )
        serialized = _serialize_key_markets(entries)
        if row is None:
            row = DailySnapshotPublicationInput(
                market=normalized,
                scan_id=scan.scan_id,
                analytics_version=analytics_version,
                snapshot_schema_version=snapshot_schema_version,
            )
            db.add(row)
        row.input_version = SNAPSHOT_PUBLICATION_INPUT_VERSION
        row.key_markets_json = serialized
        row.key_markets_sha256 = _payload_digest(serialized)
        row.captured_at = datetime.now(UTC)
        db.flush()
    if row is None or row.input_version != SNAPSHOT_PUBLICATION_INPUT_VERSION:
        raise SnapshotPublicationUnavailable("snapshot publication input is unavailable")
    serialized = row.key_markets_json
    if _payload_digest(serialized) != row.key_markets_sha256:
        raise SnapshotPublicationUnavailable("snapshot publication input digest does not match")
    try:
        entries = json.loads(serialized)
    except (TypeError, ValueError) as exc:
        raise SnapshotPublicationUnavailable(
            "snapshot publication input is not valid JSON"
        ) from exc
    _serialize_key_markets(entries)
    return entries


def _validate_identity(
    payload_json: str,
    *,
    market: str,
    scan_id: str,
    analytics_version: str,
    snapshot_schema_version: int,
) -> None:
    try:
        payload = json.loads(payload_json)
    except (TypeError, ValueError) as exc:
        raise SnapshotPublicationUnavailable("snapshot publication is not valid JSON") from exc
    freshness = payload.get("freshness")
    if not isinstance(freshness, dict):
        raise SnapshotPublicationUnavailable("snapshot publication freshness is missing")
    expected = {
        "market": market.upper(),
        "scan_id": scan_id,
        "schema_version": snapshot_schema_version,
    }
    observed = {key: payload.get(key) for key in expected}
    if observed != expected:
        raise SnapshotPublicationUnavailable("snapshot publication identity does not match")
    if freshness.get("scan_id") != scan_id:
        raise SnapshotPublicationUnavailable("snapshot publication freshness scan does not match")
    if freshness.get("analytics_version") != analytics_version:
        raise SnapshotPublicationUnavailable(
            "snapshot publication analytics version does not match"
        )


def build_and_store_snapshot_publication(
    db: Any,
    *,
    scan: Any,
    market: str,
    analytics_version: str,
    capture_inputs: bool,
) -> Any:
    """Build and upsert a validated publication inside the caller transaction."""

    from app.domain.markets.catalog import get_market_catalog
    from app.models.daily_snapshot_publication import DailySnapshotPublication
    from app.schemas.market_scan import DailySnapshotResponse
    from app.services import daily_snapshot_service
    from app.use_cases.scanning.get_scan_results import GetScanResultsUseCase

    normalized = market.upper()
    db.flush()
    schema_version = int(daily_snapshot_service.DAILY_SNAPSHOT_SCHEMA_VERSION)
    key_markets = _publication_input(
        db,
        scan=scan,
        market=normalized,
        analytics_version=analytics_version,
        snapshot_schema_version=schema_version,
        capture=capture_inputs,
    )
    override = getattr(daily_snapshot_service, "_quantstack_key_markets_override", None)
    if override is None:
        raise SnapshotPublicationUnavailable("snapshot key-markets override is unavailable")
    token = override.set(tuple(dict(item) for item in key_markets))
    try:
        payload = daily_snapshot_service.build_daily_snapshot_payload(
            db,
            market=normalized,
            market_display_name=get_market_catalog().get(normalized).label,
            scan=scan,
            uow=_BorrowedSqlUnitOfWork(db),
            scan_results_use_case=GetScanResultsUseCase(),
        )
    finally:
        override.reset(token)
    DailySnapshotResponse.model_validate(payload)
    payload_json = daily_snapshot_service.serialize_daily_snapshot_payload(payload)
    if int(payload["schema_version"]) != schema_version:
        raise SnapshotPublicationUnavailable("snapshot schema changed during publication")
    _validate_identity(
        payload_json,
        market=normalized,
        scan_id=scan.scan_id,
        analytics_version=analytics_version,
        snapshot_schema_version=schema_version,
    )
    publication = (
        db.query(DailySnapshotPublication)
        .filter_by(
            market=normalized,
            scan_id=scan.scan_id,
            analytics_version=analytics_version,
            snapshot_schema_version=schema_version,
        )
        .one_or_none()
    )
    if publication is None:
        publication = DailySnapshotPublication(
            market=normalized,
            scan_id=scan.scan_id,
            analytics_version=analytics_version,
            snapshot_schema_version=schema_version,
        )
        db.add(publication)
    publication.publication_version = SNAPSHOT_PUBLICATION_VERSION
    publication.payload_json = payload_json
    publication.payload_sha256 = _payload_digest(payload_json)
    publication.generated_at = datetime.fromisoformat(payload["generated_at"])
    publication.published_at = datetime.now(UTC)
    db.flush()
    return publication


def load_snapshot_publication_json(
    db: Any,
    *,
    scan: Any,
    market: str,
    analytics_version: str,
) -> str:
    """Load the exact durable publication; never fall back to live aggregation."""

    from app.models.daily_snapshot_publication import DailySnapshotPublication
    from app.services import daily_snapshot_service

    if scan is None or scan.analytics_version != analytics_version:
        raise SnapshotPublicationUnavailable("no analytics-ready scan publication")
    schema_version = int(daily_snapshot_service.DAILY_SNAPSHOT_SCHEMA_VERSION)
    publication = (
        db.query(DailySnapshotPublication)
        .filter_by(
            market=market.upper(),
            scan_id=scan.scan_id,
            analytics_version=analytics_version,
            snapshot_schema_version=schema_version,
            publication_version=SNAPSHOT_PUBLICATION_VERSION,
        )
        .one_or_none()
    )
    if publication is None:
        raise SnapshotPublicationUnavailable("snapshot publication is unavailable")
    return _validated_publication_payload(
        publication.payload_json,
        payload_sha256=publication.payload_sha256,
        market=market,
        scan_id=scan.scan_id,
        analytics_version=analytics_version,
        snapshot_schema_version=schema_version,
    )


def _validated_publication_payload(
    payload_json: str,
    *,
    payload_sha256: str,
    market: str,
    scan_id: str,
    analytics_version: str,
    snapshot_schema_version: int,
) -> str:
    if _payload_digest(payload_json) != payload_sha256:
        raise SnapshotPublicationUnavailable("snapshot publication digest does not match")
    _validate_identity(
        payload_json,
        market=market,
        scan_id=scan_id,
        analytics_version=analytics_version,
        snapshot_schema_version=snapshot_schema_version,
    )
    return payload_json


def _latest_snapshot_publication_statement(
    *,
    market: str,
    analytics_version: str,
) -> Any:
    """Build the exact latest-scan publication statement used by the route."""

    from app.models.daily_snapshot_publication import DailySnapshotPublication
    from app.models.scan_result import Scan
    from app.services import daily_snapshot_service
    from sqlalchemy import select

    normalized = market.upper()
    schema_version = int(daily_snapshot_service.DAILY_SNAPSHOT_SCHEMA_VERSION)
    scans = Scan.__table__
    publications = DailySnapshotPublication.__table__
    latest_scan_id = (
        select(scans.c.scan_id)
        .where(
            scans.c.status == "completed",
            scans.c.universe_market == normalized,
            scans.c.analytics_version == analytics_version,
        )
        .order_by(scans.c.completed_at.desc().nullslast(), scans.c.id.desc())
        .limit(1)
        .scalar_subquery()
    )
    statement = select(
        publications.c.payload_json,
        publications.c.payload_sha256,
        publications.c.scan_id,
    ).where(
        publications.c.market == normalized,
        publications.c.scan_id == latest_scan_id,
        publications.c.analytics_version == analytics_version,
        publications.c.snapshot_schema_version == schema_version,
        publications.c.publication_version == SNAPSHOT_PUBLICATION_VERSION,
    )
    return statement


def warm_latest_snapshot_publication_query_plan(
    db: Any,
    *,
    analytics_version: str,
) -> None:
    """Warm only PostgreSQL's plan/catalog path without reading business rows.

    The startup query uses ``LIMIT 0`` for both market branches. PostgreSQL
    still resolves the exact publication statement and its scalar latest-scan
    subquery, but it returns no publication payload. This keeps the first
    user request from paying the catalog-planning cost after a cold restart.
    """

    for market in ("CN", "US"):
        db.execute(
            _latest_snapshot_publication_statement(
                market=market,
                analytics_version=analytics_version,
            ).limit(0)
        )


def load_latest_snapshot_publication_json(
    db: Any,
    *,
    market: str,
    analytics_version: str,
) -> str:
    """Load the latest exact publication with one column-only SQL statement.

    The scalar subquery selects the latest analytics-ready scan before looking
    for its publication. If that scan has no exact publication, the read fails
    closed instead of falling back to an older published scan.
    """

    from app.services import daily_snapshot_service

    normalized = market.upper()
    schema_version = int(daily_snapshot_service.DAILY_SNAPSHOT_SCHEMA_VERSION)
    row = db.execute(
        _latest_snapshot_publication_statement(
            market=market,
            analytics_version=analytics_version,
        )
    ).one_or_none()
    if row is None:
        raise SnapshotPublicationUnavailable("snapshot publication is unavailable")
    payload_json, payload_sha256, scan_id = row
    return _validated_publication_payload(
        payload_json,
        payload_sha256=payload_sha256,
        market=normalized,
        scan_id=scan_id,
        analytics_version=analytics_version,
        snapshot_schema_version=schema_version,
    )


def snapshot_publication_is_current(
    db: Any,
    *,
    scan: Any,
    market: str,
    analytics_version: str,
) -> bool:
    try:
        load_snapshot_publication_json(
            db,
            scan=scan,
            market=market,
            analytics_version=analytics_version,
        )
    except SnapshotPublicationUnavailable:
        return False
    return True
