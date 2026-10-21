#!/usr/bin/env python3
"""Build one lightweight CN qfq session release from local session evidence."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from quant_stack.data.cn_qfq_session import build_cn_qfq_session_release


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session", type=date.fromisoformat, required=True)
    parser.add_argument("--raw-session", type=Path, required=True)
    parser.add_argument("--current-dataset", type=Path, required=True)
    parser.add_argument("--current-ledger", type=Path, required=True)
    parser.add_argument("--next-ledger", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args()
    report = build_cn_qfq_session_release(
        session=args.session,
        raw_session_path=args.raw_session.resolve(),
        current_dataset_path=args.current_dataset.resolve(),
        current_ledger_path=args.current_ledger.resolve(),
        next_ledger_path=args.next_ledger.resolve(),
        output_root=args.output_root.resolve(),
    )
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
