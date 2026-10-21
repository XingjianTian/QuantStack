from __future__ import annotations

import argparse
from pathlib import Path

DIRECTORIES = (
    "raw/us/equity",
    "raw/us/etf",
    "raw/cn/equity",
    "raw/cn/etf",
    "raw/global/fundamentals",
    "raw/global/news",
    "normalized/bars_1d",
    "normalized/instruments",
    "normalized/corporate_actions",
    "normalized/fundamentals_pit",
    "features/factors",
    "features/universes",
    "experiments",
    "backtests",
    "reports",
    "logs",
    "catalog",
    "backups",
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    for relative in DIRECTORIES:
        path = args.root / relative
        print(path)
        if args.apply:
            path.mkdir(parents=True, exist_ok=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
