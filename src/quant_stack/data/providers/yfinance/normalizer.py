from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime

from ...interfaces import MarketCalendarProvider
from ...models import Instrument


def normalize_yfinance_bars(
    records: Iterable[Mapping[str, object]],
    instruments: Mapping[str, Instrument],
    calendar: MarketCalendarProvider,
) -> list[Mapping[str, object]]:
    bars: list[Mapping[str, object]] = []
    for record in records:
        symbol = str(record.get("ticker", "")).upper()
        instrument = instruments.get(symbol)
        try:
            trading_date = datetime.fromisoformat(
                str(record.get("Date", "")).replace("Z", "+00:00")
            ).date()
        except ValueError:
            trading_date = None
        timestamp = (
            calendar.session_close(instrument.market, trading_date)
            if instrument is not None and trading_date is not None
            else None
        )
        bars.append(
            {
                "symbol": symbol,
                "instrument_id": instrument.instrument_id if instrument else None,
                "ts_event": timestamp,
                "open": record.get("Open"),
                "high": record.get("High"),
                "low": record.get("Low"),
                "close": record.get("Close"),
                "volume": record.get("Volume"),
                "vwap": None,
                "currency": instrument.currency if instrument else None,
                "lineage": {
                    "provider_symbol": symbol,
                    "provider_exchange_alias": (
                        instrument.provider_exchange_alias if instrument else None
                    ),
                    "canonical_mic": instrument.venue if instrument else None,
                    "instrument_snapshot_id": (
                        instrument.instrument_snapshot_id if instrument else None
                    ),
                    "adjustment": "unadjusted_ohlcv; adjusted_close_raw_only",
                },
            }
        )
    return bars
