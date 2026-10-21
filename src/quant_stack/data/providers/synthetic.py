from __future__ import annotations

import hashlib
import math
from collections.abc import Iterable, Mapping, Sequence
from datetime import date, timedelta

from ..models import Instrument, Market

_MARKET_CONFIG = {
    "CN": ("SSE", "CNY", "Asia/Shanghai"),
    "HK": ("XHKG", "HKD", "Asia/Hong_Kong"),
    "US": ("XNAS", "USD", "America/New_York"),
}


class SyntheticProvider:
    name = "synthetic"
    source_url = "synthetic://quant-stack"
    license_scope = "synthetic"
    available_at_policy = "session_close_plus_provider_delay"

    def list_instruments(
        self, market: Market, symbols: Sequence[str] | None = None
    ) -> Iterable[Instrument]:
        venue, currency, timezone = _MARKET_CONFIG[market]
        symbols = list(symbols or {"CN": ["600000"], "HK": ["0700"], "US": ["AAPL"]}[market])
        for symbol in symbols:
            yield Instrument(
                instrument_id=f"{venue}.{symbol}",
                symbol=symbol,
                market=market,
                venue=venue,
                currency=currency,
                timezone=timezone,
                name=f"Synthetic {symbol}",
            )

    def fetch_bars(
        self,
        market: Market,
        symbols: Sequence[str],
        start: date,
        end: date,
        frequency: str = "1d",
    ) -> Iterable[Mapping[str, object]]:
        if frequency != "1d":
            raise ValueError("SyntheticProvider currently supports only 1d")
        venue, currency, _ = _MARKET_CONFIG[market]
        instruments = {item.symbol: item for item in self.list_instruments(market, symbols)}
        current = start
        while current <= end:
            if current.weekday() < 5:
                for symbol, instrument in instruments.items():
                    seed = int(hashlib.sha256(f"{market}:{symbol}".encode()).hexdigest()[:8], 16)
                    index = (current - start).days
                    close = 100 + seed % 50 + index * 0.25 + math.sin(index / 3) * 0.5
                    yield {
                        "symbol": symbol,
                        "instrument_id": instrument.instrument_id,
                        "date": current.isoformat(),
                        "open": close - 0.2,
                        "high": close + 0.6,
                        "low": close - 0.7,
                        "close": close,
                        "volume": 1_000_000 + (seed % 100_000) + index * 1_000,
                        "currency": currency,
                        "venue": venue,
                    }
            current += timedelta(days=1)
