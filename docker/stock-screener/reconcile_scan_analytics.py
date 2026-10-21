#!/usr/bin/env python3
"""Plan or apply versioned analytics publication for one completed SS scan."""

from __future__ import annotations

import argparse
import json
from typing import Any


def initialize_runtime_services() -> Any:
    """Bind the locked-upstream process runtime for CLI and direct callers."""

    # The standalone CLI must install the same QuantStack service adaptations
    # as the web and worker processes before the upstream container is built.
    import quant_workspace_compat as _quant_workspace_compat  # noqa: F401
    from app.database import SessionLocal
    from app.wiring.bootstrap import initialize_process_runtime_services

    return initialize_process_runtime_services(session_factory=SessionLocal)


def resolve_scan_id(*, market: str | None, scan_id: str | None) -> str:
    if scan_id:
        return scan_id
    if not market:
        raise ValueError("either market or scan_id is required")

    from app.database import SessionLocal
    from app.models.scan_result import Scan

    db = SessionLocal()
    try:
        scan = (
            db.query(Scan)
            .filter(
                Scan.status == "completed",
                Scan.universe_market == market.upper(),
            )
            .order_by(Scan.completed_at.desc().nullslast(), Scan.id.desc())
            .first()
        )
        if scan is None:
            raise ValueError(f"no completed {market.upper()} scan exists")
        return str(scan.scan_id)
    finally:
        db.close()


def reconcile(
    *,
    market: str | None,
    scan_id: str | None,
    apply: bool,
    force: bool = False,
) -> dict[str, Any]:
    initialize_runtime_services()
    from quantstack_scan_analytics import sync_scan_analytics

    selected = resolve_scan_id(market=market, scan_id=scan_id)
    return sync_scan_analytics(selected, apply=apply, force=force)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--market", choices=("CN", "US"))
    target.add_argument("--scan-id")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="commit analytics and readiness; default is rollback-only dry run",
    )
    parser.add_argument("--force", action="store_true", help="recompute the current version")
    args = parser.parse_args()
    report = reconcile(
        market=args.market,
        scan_id=args.scan_id,
        apply=args.apply,
        force=args.force,
    )
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
