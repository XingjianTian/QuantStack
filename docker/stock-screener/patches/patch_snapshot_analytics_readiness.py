"""Patch the locked upstream backend with scan analytics publication state."""

from __future__ import annotations

import sys
from pathlib import Path


def replace_once(path: Path, old: str, new: str) -> None:
    source = path.read_text(encoding="utf-8")
    if source.count(old) != 1:
        raise SystemExit(f"snapshot analytics readiness anchor drifted in {path}")
    path.write_text(source.replace(old, new), encoding="utf-8")


def main(root: Path) -> None:
    model = root / "app/models/scan_result.py"
    replace_once(
        model,
        '''    warnings = Column(JSON, nullable=False, default=list, server_default=text("'[]'"))

    # Results summary
''',
        '''    warnings = Column(JSON, nullable=False, default=list, server_default=text("'[]'"))

    # Local analytics publication state. A completed base scan is not eligible
    # for Daily Snapshot until the required analytics transaction commits.
    analytics_version = Column(String(64), nullable=True)
    analytics_completed_at = Column(DateTime(timezone=True), nullable=True)

    # Results summary
''',
    )
    replace_once(
        model,
        '''        Index(
            "idx_scans_market_status_trigger",
            "universe_market",
            "status",
            "trigger_source",
        ),
''',
        '''        Index(
            "idx_scans_market_status_trigger",
            "universe_market",
            "status",
            "trigger_source",
        ),
        Index(
            "idx_scans_market_analytics_ready",
            "universe_market",
            "status",
            "analytics_version",
            "completed_at",
        ),
''',
    )

    service = root / "app/services/daily_snapshot_service.py"
    replace_once(
        service,
        '''import logging
from dataclasses import dataclass
''',
        '''import logging
import os
from dataclasses import dataclass
''',
    )
    replace_once(
        service,
        '''def latest_completed_scan(db: Session, market: str) -> Scan | None:
    return (
        db.query(Scan)
        .filter(
            Scan.status == "completed",
            Scan.universe_market == market.upper(),
        )
        .order_by(Scan.completed_at.desc().nullslast(), Scan.id.desc())
        .first()
    )
''',
        '''def latest_completed_scan(db: Session, market: str) -> Scan | None:
    query = db.query(Scan).filter(
        Scan.status == "completed",
        Scan.universe_market == market.upper(),
    )
    required_version = os.environ.get("SS_REQUIRED_SCAN_ANALYTICS_VERSION", "").strip()
    if required_version:
        query = query.filter(Scan.analytics_version == required_version)
    return query.order_by(Scan.completed_at.desc().nullslast(), Scan.id.desc()).first()
''',
    )
    replace_once(
        service,
        '''    return {
        "scan_id": scan.scan_id,
        "scan_as_of_date": as_of,
        "scan_published_at": published_at,
    }
''',
        '''    return {
        "scan_id": scan.scan_id,
        "scan_as_of_date": as_of,
        "scan_published_at": published_at,
        "analytics_version": scan.analytics_version,
        "analytics_completed_at": (
            scan.analytics_completed_at.isoformat()
            if scan.analytics_completed_at is not None
            else None
        ),
    }
''',
    )

    schema = root / "app/schemas/market_scan.py"
    replace_once(
        schema,
        '''    scan_as_of_date: Optional[str] = None
    scan_published_at: Optional[str] = None
    snapshot_as_of_date: Optional[str] = None
''',
        '''    scan_as_of_date: Optional[str] = None
    scan_published_at: Optional[str] = None
    analytics_version: Optional[str] = None
    analytics_completed_at: Optional[str] = None
    snapshot_as_of_date: Optional[str] = None
''',
    )

    route = root / "app/api/v1/market_scan.py"
    replace_once(
        route,
        '''from typing import Any, List
''',
        '''import os
from typing import Any, List

from daily_snapshot_publication import (
    SnapshotPublicationUnavailable,
    load_latest_snapshot_publication_json,
)
''',
    )
    replace_once(
        route,
        '''# The handler returns a raw Response so the cached JSON string is served
# byte-for-byte (the ETag hashes those exact bytes); response_model documents
# the contract in OpenAPI, and the payload is validated against it on the
# cache-miss path below.
''',
        '''# The handler returns a raw Response so the published JSON string is
# served byte-for-byte (the ETag hashes those exact bytes). The durable path is
# schema-validated during publication and identity-validated during each read.
''',
    )
    replace_once(
        route,
        '''    Replaces the per-section fan-out (watchlist + per-symbol history + scan
    bootstrap + scan results + group rankings) with one cached payload.
    Served with an ETag; clients sending ``If-None-Match`` get a 304.
''',
        '''    Replaces the per-section fan-out (watchlist + per-symbol history + scan
    bootstrap + scan results + group rankings) with one published payload.
    Served with an ETag; clients sending ``If-None-Match`` get a 304.
''',
    )
    replace_once(
        route,
        '''    # Keyed on the latest scan run: publishing a new run switches the key,
    # so cached snapshots invalidate immediately (TTL is just a backstop).
    scan = latest_completed_scan(db, code)
    cache_key = daily_snapshot_cache_key(code, scan.scan_id if scan else None)

    def build_payload_json() -> str:
        payload = build_daily_snapshot_payload(
            db,
            market=code,
            market_display_name=_market_catalog.get(code).label,
            scan=scan,
            uow=uow,
            scan_results_use_case=use_case,
        )
        DailySnapshotResponse.model_validate(payload)
        return serialize_daily_snapshot_payload(payload)

    payload_json = get_or_build_daily_snapshot_payload(cache_key, build_payload_json)
''',
        '''    required_version = os.environ.get("SS_REQUIRED_SCAN_ANALYTICS_VERSION", "").strip()
    if required_version:
        # The publication is an authoritative data product committed with the
        # scan readiness marker. Never rebuild it in a cold browser request.
        try:
            payload_json = load_latest_snapshot_publication_json(
                db,
                market=code,
                analytics_version=required_version,
            )
        except SnapshotPublicationUnavailable as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
    else:
        # Preserve locked-upstream behavior when the QuantStack publication
        # contract is not enabled (including the documented rollback path).
        scan = latest_completed_scan(db, code)
        cache_key = daily_snapshot_cache_key(code, scan.scan_id if scan else None)

        def build_payload_json() -> str:
            payload = build_daily_snapshot_payload(
                db,
                market=code,
                market_display_name=_market_catalog.get(code).label,
                scan=scan,
                uow=uow,
                scan_results_use_case=use_case,
            )
            DailySnapshotResponse.model_validate(payload)
            return serialize_daily_snapshot_payload(payload)

        payload_json = get_or_build_daily_snapshot_payload(cache_key, build_payload_json)
''',
    )

    tasks = root / "app/tasks/scan_tasks.py"
    replace_once(
        tasks,
        "@celery_app.task(name='app.tasks.scan_tasks.finalize_scan_artifacts', queue='celery')\n",
        "@celery_app.task(\n"
        "    name='app.tasks.scan_tasks.finalize_scan_artifacts',\n"
        "    autoretry_for=(Exception,),\n"
        "    retry_backoff=True,\n"
        "    max_retries=3,\n"
        "    queue='celery',\n"
        ")\n",
    )


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: patch_snapshot_analytics_readiness.py APP_ROOT")
    main(Path(sys.argv[1]))
