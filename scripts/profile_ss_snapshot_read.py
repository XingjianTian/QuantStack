#!/usr/bin/env python3
"""Profile the durable Stock Screener Snapshot read without issuing HTTP.

Run this inside a patched downstream backend with ``PYTHONPATH=/app``. SQLite
is accepted only through the upstream test switch. PostgreSQL is accepted only
for an explicitly marked isolated database whose name contains ``t207``,
``profile``, or ``isolated``. The script performs SELECTs only and never runs
migrations, cache warmups, publication, or reconciliation.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any


def _milliseconds(started_ns: int) -> float:
    return round((time.perf_counter_ns() - started_ns) / 1_000_000, 3)


@contextmanager
def _timed(stages: dict[str, float], name: str) -> Iterator[None]:
    started_ns = time.perf_counter_ns()
    try:
        yield
    finally:
        stages[name] = _milliseconds(started_ns)


def _database_identity() -> tuple[str, str]:
    from sqlalchemy.engine import make_url

    raw_url = os.environ.get("DATABASE_URL", "")
    if not raw_url:
        raise SystemExit("DATABASE_URL is required")
    url = make_url(raw_url)
    backend = url.get_backend_name()
    database = str(url.database or "")
    if backend == "sqlite":
        if os.environ.get("STOCKSCANNER_TEST_ALLOW_SQLITE") != "1":
            raise SystemExit("SQLite profiling requires STOCKSCANNER_TEST_ALLOW_SQLITE=1")
    elif backend == "postgresql":
        allowed_name = any(token in database.lower() for token in ("t207", "profile", "isolated"))
        if (
            os.environ.get("SS_SNAPSHOT_PROFILE_ALLOW_ISOLATED_POSTGRES") != "1"
            or not allowed_name
        ):
            raise SystemExit(
                "PostgreSQL profiling requires an explicitly allowed isolated database"
            )
    else:
        raise SystemExit(f"unsupported profiling database backend: {backend}")
    return backend, database


def _profile(market: str, *, prepare_runtime: bool) -> dict[str, Any]:
    backend, database = _database_identity()
    stages: dict[str, float] = {}
    import_started_ns = time.perf_counter_ns()
    import quant_workspace_compat  # noqa: F401

    stages["overlay_and_app_import"] = _milliseconds(import_started_ns)

    import daily_snapshot_publication as publication_module
    from app.database import engine, get_db
    from app.models.scan_result import Scan
    from app.services.daily_snapshot_service import daily_snapshot_etag
    from app.wiring.bootstrap import get_get_scan_results_use_case, get_uow
    from sqlalchemy import event
    from sqlalchemy.orm import Mapper, configure_mappers
    from starlette.responses import Response

    mapper_started_ns: int | None = None
    mapper_configure_ms: list[float] = []
    dbapi_connect_ms: list[float] = []
    sql_events: list[dict[str, Any]] = []
    cursor_started_ns: dict[int, int] = {}

    def before_configured() -> None:
        nonlocal mapper_started_ns
        mapper_started_ns = time.perf_counter_ns()

    def after_configured() -> None:
        if mapper_started_ns is not None:
            mapper_configure_ms.append(_milliseconds(mapper_started_ns))

    def do_connect(
        dialect: Any,
        connection_record: Any,
        connection_args: list[Any],
        connection_params: dict[str, Any],
    ) -> Any:
        del connection_record
        started_ns = time.perf_counter_ns()
        connection = dialect.connect(*connection_args, **connection_params)
        dbapi_connect_ms.append(_milliseconds(started_ns))
        return connection

    def before_cursor_execute(
        connection: Any,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool,
    ) -> None:
        del connection, cursor, parameters, context, executemany
        cursor_started_ns[id(statement)] = time.perf_counter_ns()

    def after_cursor_execute(
        connection: Any,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool,
    ) -> None:
        del connection, cursor, parameters, context, executemany
        normalized = " ".join(statement.lower().split())
        if "daily_snapshot_publications" in normalized and " from scans " in (
            f" {normalized} "
        ):
            kind = "snapshot_publication"
        elif "daily_snapshot_publications" in normalized:
            kind = "publication"
        elif " from scans " in f" {normalized} ":
            kind = "scan"
        else:
            kind = "other"
        started_ns = cursor_started_ns.pop(id(statement), time.perf_counter_ns())
        sql_events.append(
            {
                "kind": kind,
                "duration_ms": _milliseconds(started_ns),
                "statement_bytes": len(statement.encode("utf-8")),
            }
        )

    event.listen(Mapper, "before_configured", before_configured)
    event.listen(Mapper, "after_configured", after_configured)
    event.listen(engine, "do_connect", do_connect, retval=True)
    event.listen(engine, "before_cursor_execute", before_cursor_execute)
    event.listen(engine, "after_cursor_execute", after_cursor_execute)

    mapper_configured_before_prepare = bool(Scan.__mapper__.configured)
    startup_preparation: dict[str, float] = {}
    if prepare_runtime:
        # This mirrors a pure process-runtime preparation candidate. It may
        # configure Python/SQLAlchemy state and establish a pooled connection,
        # but deliberately does not query or preload Snapshot business data.
        with _timed(startup_preparation, "configure_mappers_total"):
            configure_mappers()
        with _timed(startup_preparation, "connection_checkout_total"):
            with engine.connect():
                pass
        startup_preparation["orm_mapper_configure"] = round(
            sum(mapper_configure_ms), 3
        )
        startup_preparation["dbapi_connect"] = round(sum(dbapi_connect_ms), 3)
        mapper_configure_ms.clear()
        dbapi_connect_ms.clear()
        sql_events.clear()

    mapper_configured_before_dependencies = bool(Scan.__mapper__.configured)
    db_generator = get_db()
    uow_generator = get_uow()
    db: Any | None = None
    try:
        with _timed(stages, "dependency_get_db"):
            db = next(db_generator)
        with _timed(stages, "dependency_get_uow"):
            next(uow_generator)
        with _timed(stages, "dependency_get_scan_results_use_case"):
            get_get_scan_results_use_case()

        with _timed(stages, "market_catalog_check"):
            from app.domain.markets.catalog import get_market_catalog

            normalized = market.upper()
            if normalized not in get_market_catalog().supported_market_codes():
                raise RuntimeError(f"unsupported market: {normalized}")

        analytics_version = os.environ.get(
            "SS_REQUIRED_SCAN_ANALYTICS_VERSION", ""
        ).strip()
        if not analytics_version:
            raise RuntimeError("SS_REQUIRED_SCAN_ANALYTICS_VERSION is required")

        digest_ms: list[float] = []
        identity_ms: list[float] = []
        json_loads_ms: list[float] = []
        original_digest: Callable[..., Any] = publication_module._payload_digest
        original_validate: Callable[..., Any] = publication_module._validate_identity
        original_json_loads: Callable[..., Any] = publication_module.json.loads

        def timed_digest(*args: Any, **kwargs: Any) -> Any:
            started_ns = time.perf_counter_ns()
            try:
                return original_digest(*args, **kwargs)
            finally:
                digest_ms.append(_milliseconds(started_ns))

        def timed_json_loads(*args: Any, **kwargs: Any) -> Any:
            started_ns = time.perf_counter_ns()
            try:
                return original_json_loads(*args, **kwargs)
            finally:
                json_loads_ms.append(_milliseconds(started_ns))

        def timed_validate(*args: Any, **kwargs: Any) -> Any:
            started_ns = time.perf_counter_ns()
            try:
                return original_validate(*args, **kwargs)
            finally:
                identity_ms.append(_milliseconds(started_ns))

        publication_module._payload_digest = timed_digest
        publication_module._validate_identity = timed_validate
        publication_module.json.loads = timed_json_loads
        try:
            with _timed(stages, "durable_publication_lookup_total"):
                payload_json = publication_module.load_latest_snapshot_publication_json(
                    db,
                    market=normalized,
                    analytics_version=analytics_version,
                )
        finally:
            publication_module._payload_digest = original_digest
            publication_module._validate_identity = original_validate
            publication_module.json.loads = original_json_loads

        stages["publication_sha256"] = round(sum(digest_ms), 3)
        stages["publication_identity_total"] = round(sum(identity_ms), 3)
        stages["publication_json_loads"] = round(sum(json_loads_ms), 3)
        stages["publication_identity_checks_residual"] = round(
            max(0.0, sum(identity_ms) - sum(json_loads_ms)), 3
        )
        stages["durable_publication_lookup_residual"] = round(
            max(
                0.0,
                stages["durable_publication_lookup_total"]
                - sum(digest_ms)
                - sum(identity_ms),
            ),
            3,
        )

        with _timed(stages, "etag_sha1"):
            etag = daily_snapshot_etag(payload_json)
        with _timed(stages, "response_construct"):
            response = Response(
                content=payload_json,
                media_type="application/json",
                headers={"ETag": etag, "Cache-Control": "private, max-age=60"},
            )
        payload = original_json_loads(payload_json)
        if payload.get("market") != normalized or len(response.body) != len(
            payload_json.encode("utf-8")
        ):
            raise RuntimeError("profiled Snapshot response identity changed")
    finally:
        uow_generator.close()
        db_generator.close()
        event.remove(Mapper, "before_configured", before_configured)
        event.remove(Mapper, "after_configured", after_configured)
        event.remove(engine, "do_connect", do_connect)
        event.remove(engine, "before_cursor_execute", before_cursor_execute)
        event.remove(engine, "after_cursor_execute", after_cursor_execute)

    stages["orm_mapper_configure"] = round(sum(mapper_configure_ms), 3)
    stages["dbapi_connect"] = round(sum(dbapi_connect_ms), 3)
    stages["sql_cursor_execute_total"] = round(
        sum(float(item["duration_ms"]) for item in sql_events), 3
    )
    stages["dependencies_total"] = round(
        stages["dependency_get_db"]
        + stages["dependency_get_uow"]
        + stages["dependency_get_scan_results_use_case"],
        3,
    )
    stages["read_path_total"] = round(
        stages["dependencies_total"]
        + stages["market_catalog_check"]
        + stages["durable_publication_lookup_total"]
        + stages["etag_sha1"]
        + stages["response_construct"],
        3,
    )
    return {
        "schema_version": "quantstack.ss-snapshot-read-profile.v2",
        "market": normalized,
        "database": {"backend": backend, "name": database},
        "prepare_runtime": prepare_runtime,
        "startup_preparation_ms": startup_preparation,
        "mapper_configured_before_prepare": mapper_configured_before_prepare,
        "mapper_configured_before_dependencies": mapper_configured_before_dependencies,
        "payload_bytes": len(payload_json.encode("utf-8")),
        "candidate_count": len(payload.get("top_candidates", {}).get("rows", [])),
        "scan_id": payload["scan_id"],
        "analytics_version": payload["freshness"]["analytics_version"],
        "stages_ms": stages,
        "sql": {
            "statement_count": len(sql_events),
            "statements": sql_events,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--market", choices=("CN", "US"), required=True)
    parser.add_argument(
        "--prepare-runtime",
        action="store_true",
        help=(
            "separately time mapper configuration and a connection checkout "
            "before the measured read; no Snapshot rows are queried"
        ),
    )
    args = parser.parse_args()
    print(
        json.dumps(
            _profile(args.market, prepare_runtime=args.prepare_runtime),
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
