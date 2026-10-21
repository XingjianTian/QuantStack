from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import UTC, datetime

from ..interfaces import MarketCalendarProvider
from ..models import Instrument


def _timestamp(
    record: Mapping[str, object],
    instrument: Instrument | None,
    calendar: MarketCalendarProvider,
) -> datetime | None:
    value = record.get("timestamp", record.get("date"))
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if "timestamp" in record:
        return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)
    if instrument is None:
        return None
    return calendar.session_close(instrument.market, parsed.date())


def normalize_bars(
    records: Iterable[Mapping[str, object]],
    instruments: Mapping[str, Instrument],
    calendar: MarketCalendarProvider,
) -> list[Mapping[str, object]]:
    result: list[Mapping[str, object]] = []
    for record in records:
        symbol = str(record.get("instrument_id") or record.get("symbol") or "")
        instrument = instruments.get(symbol)
        if instrument is None:
            instrument = next(
                (item for item in instruments.values() if item.symbol == symbol), None
            )
        result.append(
            {
                "symbol": instrument.symbol if instrument else symbol,
                "instrument_id": instrument.instrument_id if instrument else None,
                "ts_event": _timestamp(record, instrument, calendar),
                "open": record.get("open"),
                "high": record.get("high"),
                "low": record.get("low"),
                "close": record.get("close"),
                "volume": record.get("volume"),
                "vwap": record.get("vwap"),
                "currency": instrument.currency if instrument else record.get("currency"),
            }
        )
    return result
