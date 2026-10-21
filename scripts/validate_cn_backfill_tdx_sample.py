#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import duckdb

from quant_stack.data.models import Instrument
from quant_stack.data.providers.cn.live import TDXReferenceLiveProvider
from quant_stack.data.quality import ProviderSchemaValidator
from quant_stack.data.storage.atomic import atomic_replace_bytes

RULE_VERSION = "cn-backfill-tdx-sample-v2"
SEED = 2303
INSTRUMENTS_PER_VENUE_WINDOW = 2
PRICE_ABS_TOLERANCE = Decimal("0.01")
VOLUME_RELATIVE_TOLERANCE = Decimal("0.005")
AMOUNT_RELATIVE_TOLERANCE = Decimal("0.005")
WINDOWS = (
    ("early", date(2016, 6, 1), date(2016, 6, 30)),
    ("middle", date(2020, 6, 1), date(2020, 6, 30)),
    ("recent", date(2026, 7, 1), date(2026, 7, 31)),
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run fixed-seed TDX comparison for the pilot")
    parser.add_argument("--data-root", required=True, type=Path)
    parser.add_argument("--dataset-version", required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--sessions-per-window", type=int, default=5)
    return parser


def _relative(left: Decimal, right: Decimal) -> Decimal:
    denominator = max(abs(left), abs(right), Decimal("1"))
    return abs(left - right) / denominator


def _selection_score(window: str, venue: str, instrument_id: str) -> str:
    return hashlib.sha256(
        f"{RULE_VERSION}|{SEED}|{window}|{venue}|{instrument_id}".encode()
    ).hexdigest()


def main() -> None:
    args = _parser().parse_args()
    index = (
        args.data_root
        / "pilot/query"
        / f"version={args.dataset_version}"
        / "backfill-research.duckdb"
    )
    connection = duckdb.connect(str(index), read_only=True)
    try:
        results: list[dict[str, object]] = []
        used: set[str] = set()
        for label, start, end in WINDOWS:
            for venue in ("XSHG", "XSHE"):
                available = [
                    row[0]
                    for row in connection.execute(
                        "SELECT instrument_id FROM pilot_bars WHERE venue = ? GROUP BY 1 "
                        "HAVING min(trading_date) <= DATE '2016-06-01' "
                        "AND max(trading_date) >= DATE '2026-07-31' "
                        "AND count(*) FILTER (WHERE trading_date BETWEEN ? AND ?) >= ?",
                        [venue, start, end, args.sessions_per_window],
                    ).fetchall()
                    if row[0] not in used
                ]
                selected = sorted(
                    available,
                    key=lambda item: _selection_score(label, venue, item),
                )[:INSTRUMENTS_PER_VENUE_WINDOW]
                if len(selected) != INSTRUMENTS_PER_VENUE_WINDOW:
                    raise RuntimeError(f"not enough {venue} instruments in {label} window")
                used.update(selected)
                for instrument_id in selected:
                    symbol = instrument_id.split(".", 1)[1]
                    sessions = [
                        row[0]
                        for row in connection.execute(
                            "SELECT trading_date FROM pilot_bars WHERE instrument_id = ? "
                            "AND trading_date BETWEEN ? AND ? ORDER BY trading_date LIMIT ?",
                            [instrument_id, start, end, args.sessions_per_window],
                        ).fetchall()
                    ]
                    primary = {
                        row[0]: row[1:]
                        for row in connection.execute(
                            "SELECT trading_date, open, high, low, close, volume, amount "
                            "FROM pilot_bars WHERE instrument_id = ? "
                            "AND trading_date IN (SELECT unnest(?::DATE[])) "
                            "ORDER BY trading_date",
                            [instrument_id, sessions],
                        ).fetchall()
                    }
                    instrument = Instrument(
                        instrument_id=instrument_id,
                        symbol=symbol,
                        market="CN",
                        venue=venue,
                        provider_symbol=symbol,
                        provider_exchange_alias="SH" if venue == "XSHG" else "SZ",
                        currency="CNY",
                        timezone="Asia/Shanghai",
                    )
                    provider = TDXReferenceLiveProvider(
                        (instrument,), verified_sessions=sessions
                    )
                    native = list(
                        provider.fetch_bars(
                            "CN", [instrument_id], sessions[0], sessions[-1]
                        )
                    )
                    ProviderSchemaValidator().validate(
                        provider="tdx_reference", records=native, schema=provider.bar_schema
                    )
                    differences: list[dict[str, object]] = []
                    for row in native:
                        observed = date.fromisoformat(str(row["datetime"])[:10])
                        reference = primary[observed]
                        fields = ("open", "high", "low", "close")
                        failed_fields: list[str] = []
                        for offset, field in enumerate(fields):
                            if abs(
                                Decimal(str(reference[offset])) - Decimal(str(row[field]))
                            ) > PRICE_ABS_TOLERANCE:
                                failed_fields.append(field)
                        reference_volume = Decimal(str(reference[4]))
                        tdx_volume = Decimal(str(row["vol"])) * 100
                        if _relative(reference_volume, tdx_volume) > VOLUME_RELATIVE_TOLERANCE:
                            failed_fields.append("volume")
                        reference_amount = Decimal(str(reference[5]))
                        tdx_amount = Decimal(str(row["amount"]))
                        if _relative(reference_amount, tdx_amount) > AMOUNT_RELATIVE_TOLERANCE:
                            failed_fields.append("amount")
                        if failed_fields:
                            differences.append(
                                {"trading_date": observed.isoformat(), "fields": failed_fields}
                            )
                    results.append(
                        {
                            "window": label,
                            "venue": venue,
                            "instrument_id": instrument_id,
                            "start": sessions[0].isoformat(),
                            "end": sessions[-1].isoformat(),
                            "sessions": len(sessions),
                            "status": (
                                "passed" if not differences else "local_discrepancy"
                            ),
                            "differences": differences,
                            "transport_audit": provider.transport_audit(),
                        }
                    )
    finally:
        connection.close()
    failed_samples = sum(item["status"] != "passed" for item in results)
    failed_windows = len(
        {item["window"] for item in results if item["status"] != "passed"}
    )
    failed_instruments = len(
        {item["instrument_id"] for item in results if item["status"] != "passed"}
    )
    systemic = failed_windows >= 2 or failed_instruments >= 2
    report = {
        "schema_version": "1.0.0",
        "rule_version": RULE_VERSION,
        "dataset_version": args.dataset_version,
        "selection_seed": SEED,
        "selection_basis": (
            "fixed hash by window and venue; two unique instruments per venue/window"
        ),
        "unique_instruments": len({item["instrument_id"] for item in results}),
        "xshg_instruments": sum(item["venue"] == "XSHG" for item in results),
        "xshe_instruments": sum(item["venue"] == "XSHE" for item in results),
        "windows": [
            {"label": label, "requested_start": start.isoformat(), "requested_end": end.isoformat()}
            for label, start, end in WINDOWS
        ],
        "tolerances": {
            "ohlc_absolute": str(PRICE_ABS_TOLERANCE),
            "volume_relative": str(VOLUME_RELATIVE_TOLERANCE),
            "amount_relative": str(AMOUNT_RELATIVE_TOLERANCE),
            "tdx_volume_conversion": "Decimal(str(vol))*100 exact shares",
        },
        "results": results,
        "failed_samples": failed_samples,
        "failed_windows": failed_windows,
        "failed_instruments": failed_instruments,
        "status": "systemic_block"
        if systemic
        else ("local_isolation" if failed_windows else "passed"),
        "whole_pilot_blocked": systemic,
        "canonical_selection_changed": False,
        "akshare_required": False,
    }
    payload = (json.dumps(report, ensure_ascii=True, indent=2, sort_keys=True) + "\n").encode()
    if args.output is not None:
        atomic_replace_bytes(
            args.output,
            payload,
            validate=lambda path: json.loads(path.read_text()),
        )
    print(json.dumps(report, ensure_ascii=True, sort_keys=True))


if __name__ == "__main__":
    main()
