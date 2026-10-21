#!/usr/bin/env python3
"""Run one zero-Provider semantic readback for the latest installed session."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from quant_stack.data.session_parquet import inspect_session_package, replay_session_package


def run(root: Path, market: str) -> dict[str, object]:
    packages = sorted((root / f"market={market}").glob("session=*"))
    if not packages:
        return {"status": "noop", "market": market, "provider_requested": False}
    package = packages[-1]
    inspected = inspect_session_package(package)
    equivalent = replay_session_package(package, package / "bars.parquet")
    if not equivalent:
        raise ValueError("session semantic replay diverged")
    return {
        "status": "completed",
        "market": market,
        "session": inspected.session.isoformat(),
        "rows": inspected.row_count,
        "equivalent": True,
        "provider_requested": False,
        "business_files_written": 0,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--market", choices=("CN", "US"), required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.root.resolve(), args.market), sort_keys=True))


if __name__ == "__main__":
    main()
