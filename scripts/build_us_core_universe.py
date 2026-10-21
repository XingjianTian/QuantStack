from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path

from quant_stack.data.providers.yfinance.instruments import (
    USInstrumentRecord,
    USInstrumentSnapshot,
    USInstrumentStatus,
)
from quant_stack.data.storage.atomic import atomic_replace_bytes

SP500_SOURCE = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
NASDAQ_LISTED_SOURCE = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
OTHER_LISTED_SOURCE = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"


class _ConstituentTableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.in_table = False
        self.in_row = False
        self.in_cell = False
        self.current_cell: list[str] = []
        self.current_row: list[str] = []
        self.rows: list[list[str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = dict(attrs)
        if tag == "table" and attributes.get("id") == "constituents":
            self.in_table = True
        elif self.in_table and tag == "tr":
            self.in_row = True
            self.current_row = []
        elif self.in_row and tag in {"th", "td"}:
            self.in_cell = True
            self.current_cell = []
        elif self.in_cell and tag == "br":
            self.current_cell.append(" ")

    def handle_endtag(self, tag: str) -> None:
        if self.in_cell and tag in {"th", "td"}:
            self.current_row.append(" ".join("".join(self.current_cell).split()))
            self.current_cell = []
            self.in_cell = False
        elif self.in_row and tag == "tr":
            if self.current_row:
                self.rows.append(self.current_row)
            self.in_row = False
        elif self.in_table and tag == "table":
            self.in_table = False

    def handle_data(self, data: str) -> None:
        if self.in_cell:
            self.current_cell.append(data)


def parse_sp500_constituents(html: str) -> list[dict[str, str]]:
    parser = _ConstituentTableParser()
    parser.feed(html)
    if len(parser.rows) < 2:
        raise ValueError("S&P 500 constituents table is missing or empty")
    headers = parser.rows[0]
    required = {"Symbol", "Security"}
    if not required.issubset(headers):
        raise ValueError("S&P 500 constituents table schema changed")
    records: list[dict[str, str]] = []
    for row in parser.rows[1:]:
        if len(row) != len(headers):
            raise ValueError("S&P 500 constituent row width changed")
        records.append(dict(zip(headers, row, strict=True)))
    return records


def _pipe_records(text: str) -> list[dict[str, str]]:
    rows = list(csv.DictReader(text.splitlines(), delimiter="|"))
    return [row for row in rows if row and not next(iter(row.values())).startswith("File Creation")]


def build_exchange_index(nasdaq_listed: str, other_listed: str) -> dict[str, str]:
    venues: dict[str, str] = {}
    for row in _pipe_records(nasdaq_listed):
        symbol = row.get("Symbol", "").strip().upper()
        if symbol and row.get("Test Issue") == "N":
            venues[symbol] = "XNAS"
    for row in _pipe_records(other_listed):
        symbol = row.get("ACT Symbol", "").strip().upper()
        if symbol and row.get("Test Issue") == "N":
            exchange = row.get("Exchange", "")
            venues[symbol] = "XNYS" if exchange == "N" else f"UNSUPPORTED:{exchange}"
    return venues


def build_snapshot(
    *,
    constituents_html: str,
    nasdaq_listed: str,
    other_listed: str,
    snapshot_id: str,
    observed_at: datetime,
) -> USInstrumentSnapshot:
    if observed_at.tzinfo is None:
        raise ValueError("observed_at must be timezone-aware")
    constituents = parse_sp500_constituents(constituents_html)
    venues = build_exchange_index(nasdaq_listed, other_listed)
    instruments: list[USInstrumentRecord] = []
    missing: list[str] = []
    excluded: list[str] = []
    for item in constituents:
        source_symbol = item["Symbol"].strip().upper()
        venue = venues.get(source_symbol)
        if venue is None:
            missing.append(source_symbol)
            continue
        if venue not in {"XNAS", "XNYS"}:
            excluded.append(source_symbol)
            continue
        provider_symbol = source_symbol.replace(".", "-")
        instruments.append(
            USInstrumentRecord(
                instrument_id=f"{venue}.{provider_symbol}",
                symbol=provider_symbol,
                source_symbol=source_symbol,
                canonical_venue=venue,
                provider_symbol=provider_symbol,
                name=item["Security"].strip(),
                status=USInstrumentStatus.LISTED,
            )
        )
    if missing:
        raise ValueError(f"S&P 500 symbols missing XNAS/XNYS mapping: {', '.join(missing)}")
    if not 500 <= len(instruments) <= 505:
        raise ValueError(f"unexpected S&P 500 security count: {len(instruments)}")
    return USInstrumentSnapshot(
        snapshot_id=snapshot_id,
        as_of=observed_at,
        membership_basis="current_sp500_constituents_not_point_in_time",
        source_refs=(SP500_SOURCE, NASDAQ_LISTED_SOURCE, OTHER_LISTED_SOURCE),
        excluded_source_symbols=tuple(excluded),
        instruments=tuple(instruments),
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build a versioned current S&P 500 research snapshot"
    )
    parser.add_argument("--constituents-html", type=Path, required=True)
    parser.add_argument("--nasdaq-listed", type=Path, required=True)
    parser.add_argument("--other-listed", type=Path, required=True)
    parser.add_argument("--snapshot-id", required=True)
    parser.add_argument("--observed-at", type=datetime.fromisoformat, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    snapshot = build_snapshot(
        constituents_html=args.constituents_html.read_text(encoding="utf-8"),
        nasdaq_listed=args.nasdaq_listed.read_text(encoding="utf-8"),
        other_listed=args.other_listed.read_text(encoding="utf-8"),
        snapshot_id=args.snapshot_id,
        observed_at=args.observed_at,
    )
    atomic_replace_bytes(
        args.output,
        (json.dumps(snapshot.model_dump(mode="json"), indent=2, sort_keys=True) + "\n").encode(),
    )
    print(
        json.dumps(
            {"snapshot_id": snapshot.snapshot_id, "instruments": len(snapshot.instruments)}
        )
    )


if __name__ == "__main__":
    main()
