#!/usr/bin/env python3
"""Build the small authenticated factor input transferred to the NAS qfq materializer."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from quant_stack.data.qfq_input_bundle import build_qfq_factor_input_archive


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--factor", type=Path, required=True)
    parser.add_argument("--daily-import-report", type=Path, required=True)
    parser.add_argument("--session", type=date.fromisoformat, required=True)
    parser.add_argument("--dataset-version", required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--hmac-key-file", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    envelope = build_qfq_factor_input_archive(
        factor_path=args.factor,
        daily_import_report_path=args.daily_import_report,
        session=args.session,
        dataset_version=args.dataset_version,
        source_commit=args.source_commit,
        hmac_key=args.hmac_key_file.read_bytes(),
        output_path=args.output,
    )
    print(
        json.dumps(
            {
                "status": "ready",
                "session_date": envelope.session_date.isoformat(),
                "dataset_version": envelope.dataset_version,
                "archive_size_bytes": args.output.stat().st_size,
                "contains_raw_parquet": False,
                "contains_qfq_parquet": False,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
