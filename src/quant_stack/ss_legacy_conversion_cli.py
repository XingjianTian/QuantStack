from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .data.continuity.ss_archive_migration import (
    DESIGNATED_SS_ARCHIVE_ID,
    SSArchiveMigrationPolicy,
    accept_ss_archive_for_phase_23,
)
from .data.continuity.ss_legacy_conversion import (
    CONVERTER_VERSION,
    SSLegacyOfflineConverter,
    verify_ss_legacy_conversion,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="quant-stack-ss-legacy-convert")
    parser.add_argument("--archive-root", type=Path, required=True)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--archive-id", default=DESIGNATED_SS_ARCHIVE_ID)
    parser.add_argument("--pg-restore", default="pg_restore")
    parser.add_argument("--accept-backup-blocker", action="store_true")
    parser.add_argument("--accept-restore-blocker", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        migration_input = accept_ss_archive_for_phase_23(
            args.archive_root,
            archive_id=args.archive_id,
            policy=SSArchiveMigrationPolicy(
                accept_backup_blocker=args.accept_backup_blocker,
                accept_restore_blocker=args.accept_restore_blocker,
            ),
            pg_restore=args.pg_restore,
        )
        result = SSLegacyOfflineConverter(args.archive_root, args.data_root).convert(
            migration_input
        )
        verification = verify_ss_legacy_conversion(
            args.data_root, result, archive_root=args.archive_root
        )
    except (OSError, RuntimeError, ValueError) as exc:
        print(
            json.dumps(
                {"status": "failed", "error_class": type(exc).__name__},
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1
    print(
        json.dumps(
            {
                "status": "verified",
                "archive_id": result.migration_input.archive_id,
                "converter_version": CONVERTER_VERSION,
                "conversion_id": result.conversion_id,
                "conversion_identity_sha256": result.conversion_identity_sha256,
                "output_uri": (
                    "quant-data://legacy/stock_screener/"
                    f"{result.migration_input.archive_id}/{result.conversion_id}/"
                ),
                "mapped_symbols": result.mapped_symbols,
                "unresolved_symbols": result.unresolved_symbols,
                "orphan_price_symbols": result.orphan_price_symbols,
                "price_rows": result.price_rows,
                "volume_anomaly_rows": result.volume_anomaly_rows,
                "source_price_file_count": verification.source_price_file_count,
                "catalog_exclusion": verification.catalog_status,
                "canonical_eligible": result.canonical_eligible,
                "quarantined": result.quarantined,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
