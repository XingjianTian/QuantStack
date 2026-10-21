from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timedelta
from importlib.metadata import version
from pathlib import Path

from quant_stack.data.storage.atomic import atomic_replace_bytes


def _dates(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def build(*, source_path: Path, raw_path: Path, normalized_path: Path, observed_at: datetime):
    source = json.loads(source_path.read_text(encoding="utf-8"))
    library = source["calendar_library"]
    if version(library["distribution"]) != library["version"]:
        raise RuntimeError("daily calendar library version does not match its source contract")
    if source["exchanges"] != ["XSHG", "XSHE"]:
        raise ValueError("daily calendar must cover exactly XSHG and XSHE")
    year = int(source["version"].split("-")[2])
    start = date(year, 1, 1)
    end = date(year, 12, 31)
    holiday_dates = {
        day
        for first, last in source["holiday_ranges"]
        for day in _dates(date.fromisoformat(first), date.fromisoformat(last))
    }

    import exchange_calendars as xc

    calendar = xc.get_calendar(library["calendar_name"])
    sessions = {
        item.date() for item in calendar.sessions_in_range(start.isoformat(), end.isoformat())
    }
    for holiday in holiday_dates:
        if holiday in sessions:
            raise ValueError(f"official holiday was classified as a session: {holiday}")
    unexplained_weekday_closures = [
        day
        for day in _dates(start, end)
        if day.weekday() < 5 and day not in sessions and day not in holiday_dates
    ]
    if unexplained_weekday_closures:
        raise ValueError("calendar contains a weekday closure absent from official notices")

    days = []
    for day in _dates(start, end):
        if day in sessions:
            state = "session"
        elif day.weekday() >= 5:
            state = "weekend"
        elif day in holiday_dates:
            state = "holiday"
        else:
            raise ValueError(f"daily calendar could not classify {day}")
        days.append({"trading_date": day.isoformat(), "state": state})
    raw = {
        "schema_version": "1.0.0",
        "fetched_at": observed_at.isoformat(),
        "version": source["version"],
        "sources": source["sources"],
        "holiday_ranges": source["holiday_ranges"],
        "calendar_library": library,
    }
    normalized = {
        "schema_version": "1.0.0",
        "version": source["version"],
        "market": source["market"],
        "timezone": source["timezone"],
        "open_time": source["open_time"],
        "close_time": source["close_time"],
        "exchanges": source["exchanges"],
        "supported_start": start.isoformat(),
        "supported_end": end.isoformat(),
        "days": days,
    }
    for path, payload in ((raw_path, raw), (normalized_path, normalized)):
        atomic_replace_bytes(
            path,
            (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(),
        )
    return {
        "version": source["version"],
        "days": len(days),
        "sessions": len(sessions),
        "unknown": 0,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build official-notice-bound 2026 CN calendar")
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--raw", required=True, type=Path)
    parser.add_argument("--normalized", required=True, type=Path)
    parser.add_argument("--observed-at", required=True, type=datetime.fromisoformat)
    args = parser.parse_args()
    print(
        json.dumps(
            build(
                source_path=args.source,
                raw_path=args.raw,
                normalized_path=args.normalized,
                observed_at=args.observed_at,
            ),
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
