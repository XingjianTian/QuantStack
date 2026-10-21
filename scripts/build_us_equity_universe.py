from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal

from quant_stack.data.providers.yfinance.instruments import (
    USInstrumentRecord,
    USInstrumentSnapshot,
    USInstrumentStatus,
)
from quant_stack.data.storage.atomic import atomic_replace_bytes

NASDAQ_LISTED_SOURCE = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
OTHER_LISTED_SOURCE = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"

_INCLUDE_PATTERNS = (
    re.compile(r"\bcommon (stock|shares?)\b", re.IGNORECASE),
    re.compile(r"\b(?:ordinary|ord) shares?\b", re.IGNORECASE),
    re.compile(r"\bamerican depos(?:itary|itory) (shares?|receipts?)\b", re.IGNORECASE),
    re.compile(r"\bADSs?\b", re.IGNORECASE),
    re.compile(r"\bnew york registry shares?\b", re.IGNORECASE),
    re.compile(r"\bclass [A-Z] shares?\b", re.IGNORECASE),
    re.compile(r"\bclass [A-Z] common\b", re.IGNORECASE),
    re.compile(r"\bclass [A-Z]\b$", re.IGNORECASE),
    re.compile(r"\b(?:subordinate voting|registered) shares?\b", re.IGNORECASE),
    re.compile(r"\bcapital stock\b", re.IGNORECASE),
    re.compile(r"\bshares? of beneficial interest\b", re.IGNORECASE),
    re.compile(r"\bbusiness development company\b", re.IGNORECASE),
    re.compile(r"\breal estate investment trust\b", re.IGNORECASE),
    re.compile(r"\bADR\b|\bADS\b", re.IGNORECASE),
)
_EXCLUDE_PATTERNS = (
    ("preferred", re.compile(r"\bpreferred\b|\bpreference shares?\b", re.IGNORECASE)),
    ("warrant", re.compile(r"\bwarrants?\b", re.IGNORECASE)),
    ("right", re.compile(r"\brights?\b", re.IGNORECASE)),
    ("unit", re.compile(r"\bunits?\b", re.IGNORECASE)),
    ("fund", re.compile(r"\bfund\b|\bportfolio\b", re.IGNORECASE)),
    (
        "debt",
        re.compile(r"\bnotes?\b.*\bdue\b|\bdebentures?\b|\bbonds?\b", re.IGNORECASE),
    ),
    (
        "structured_product",
        re.compile(
            r"\bETN\b|\bexchange[- ]traded notes?\b|\bindex[- ]linked\b",
            re.IGNORECASE,
        ),
    ),
)


@dataclass(frozen=True)
class Classification:
    disposition: Literal["included", "quarantined", "excluded"]
    reason: str


def _records(text: str, *, required: set[str]) -> list[dict[str, str]]:
    reader = csv.DictReader(text.splitlines(), delimiter="|")
    if reader.fieldnames is None or not required.issubset(reader.fieldnames):
        raise ValueError(f"Nasdaq Trader schema changed; required columns: {sorted(required)}")
    rows: list[dict[str, str]] = []
    for row in reader:
        first = next(iter(row.values()), "")
        if not row or first.startswith("File Creation"):
            continue
        rows.append({key: (value or "").strip() for key, value in row.items()})
    return rows


def classify_security(*, name: str, etf: str, venue: str) -> Classification:
    if etf != "N":
        return Classification("excluded", "etf_flag")
    for reason, pattern in _EXCLUDE_PATTERNS:
        if pattern.search(name):
            return Classification("excluded", reason)
    if any(pattern.search(name) for pattern in _INCLUDE_PATTERNS):
        return Classification("included", "common_or_depositary_equity")
    if venue == "XNYS":
        return Classification("included", "plain_xnys_equity_name")
    return Classification("quarantined", "ambiguous_security_name")


def _candidate_rows(nasdaq_listed: str, other_listed: str) -> list[dict[str, str]]:
    candidates: list[dict[str, str]] = []
    for row in _records(
        nasdaq_listed,
        required={"Symbol", "Security Name", "Test Issue", "Financial Status", "ETF"},
    ):
        candidates.append(
            {
                "source_symbol": row["Symbol"].upper(),
                "name": row["Security Name"],
                "venue": "XNAS",
                "test_issue": row["Test Issue"],
                "financial_status": row["Financial Status"],
                "etf": row["ETF"],
            }
        )
    for row in _records(
        other_listed,
        required={"ACT Symbol", "Security Name", "Exchange", "ETF", "Test Issue"},
    ):
        candidates.append(
            {
                "source_symbol": row["ACT Symbol"].upper(),
                "name": row["Security Name"],
                "venue": "XNYS" if row["Exchange"] == "N" else f"UNSUPPORTED:{row['Exchange']}",
                "test_issue": row["Test Issue"],
                "financial_status": "N",
                "etf": row["ETF"],
            }
        )
    symbols = [item["source_symbol"] for item in candidates]
    duplicates = sorted(symbol for symbol, count in Counter(symbols).items() if count > 1)
    if duplicates:
        raise ValueError(f"duplicate Nasdaq Trader symbols: {', '.join(duplicates)}")
    return candidates


def build_snapshot(
    *,
    nasdaq_listed: str,
    other_listed: str,
    snapshot_id: str,
    observed_at: datetime,
) -> tuple[USInstrumentSnapshot, dict[str, object]]:
    if observed_at.tzinfo is None:
        raise ValueError("observed_at must be timezone-aware")
    candidates = _candidate_rows(nasdaq_listed, other_listed)
    instruments: list[USInstrumentRecord] = []
    excluded: list[dict[str, str]] = []
    quarantined: list[dict[str, str]] = []
    reason_counts: Counter[str] = Counter()
    for row in candidates:
        if row["test_issue"] != "N":
            result = Classification("excluded", "test_issue")
        elif row["venue"] not in {"XNAS", "XNYS"}:
            result = Classification("excluded", "unsupported_exchange")
        elif row["venue"] == "XNAS" and row["financial_status"] != "N":
            result = Classification("excluded", "non_normal_financial_status")
        else:
            result = classify_security(
                name=row["name"], etf=row["etf"], venue=row["venue"]
            )
        reason_counts[result.reason] += 1
        if result.disposition != "included":
            item = {
                "source_symbol": row["source_symbol"],
                "name": row["name"],
                "venue": row["venue"],
                "reason": result.reason,
            }
            (excluded if result.disposition == "excluded" else quarantined).append(item)
            continue
        provider_symbol = row["source_symbol"].replace(".", "-")
        instruments.append(
            USInstrumentRecord(
                instrument_id=f"{row['venue']}.{provider_symbol}",
                symbol=provider_symbol,
                source_symbol=row["source_symbol"],
                canonical_venue=row["venue"],
                provider_symbol=provider_symbol,
                name=row["name"],
                status=USInstrumentStatus.LISTED,
            )
        )
    instruments.sort(key=lambda item: (item.canonical_venue, item.provider_symbol))
    excluded.sort(key=lambda item: item["source_symbol"])
    quarantined.sort(key=lambda item: item["source_symbol"])
    snapshot = USInstrumentSnapshot(
        snapshot_id=snapshot_id,
        as_of=observed_at,
        membership_basis="current_xnas_xnys_listings_not_point_in_time",
        source_refs=(NASDAQ_LISTED_SOURCE, OTHER_LISTED_SOURCE),
        excluded_source_symbols=tuple(
            item["source_symbol"] for item in excluded + quarantined
        ),
        instruments=tuple(instruments),
    )
    report: dict[str, object] = {
        "schema_version": "1.0.0",
        "snapshot_id": snapshot.snapshot_id,
        "observed_at": observed_at.isoformat(),
        "membership_basis": snapshot.membership_basis,
        "source_refs": list(snapshot.source_refs),
        "input_rows": len(candidates),
        "included_count": len(instruments),
        "quarantined_count": len(quarantined),
        "excluded_count": len(excluded),
        "counts_closed": len(candidates) == len(instruments) + len(quarantined) + len(excluded),
        "reason_counts": dict(sorted(reason_counts.items())),
        "quarantined": quarantined,
        "excluded": excluded,
        "survivorship_bias": "current-listed snapshot; not a point-in-time historical universe",
    }
    if not instruments or not report["counts_closed"]:
        raise ValueError("full-market universe classification is incomplete")
    return snapshot, report


def _write_json(path: Path, payload: object) -> None:
    atomic_replace_bytes(
        path,
        (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(),
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build a versioned current XNAS/XNYS equity research snapshot"
    )
    parser.add_argument("--nasdaq-listed", type=Path, required=True)
    parser.add_argument("--other-listed", type=Path, required=True)
    parser.add_argument("--snapshot-id", required=True)
    parser.add_argument("--observed-at", type=datetime.fromisoformat, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    snapshot, report = build_snapshot(
        nasdaq_listed=args.nasdaq_listed.read_text(encoding="utf-8"),
        other_listed=args.other_listed.read_text(encoding="utf-8"),
        snapshot_id=args.snapshot_id,
        observed_at=args.observed_at,
    )
    _write_json(args.output, snapshot.model_dump(mode="json"))
    _write_json(args.report, report)
    summary_keys = (
        "snapshot_id",
        "input_rows",
        "included_count",
        "quarantined_count",
        "excluded_count",
    )
    print(json.dumps({key: report[key] for key in summary_keys}, sort_keys=True))


if __name__ == "__main__":
    main()
