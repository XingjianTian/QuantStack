from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from datetime import date

from ...ingestion import ProviderRuntimeContract
from ...models import Instrument, Market
from ...quality import ProviderNativeSchema
from .instruments import USInstrumentSnapshot, load_us_instrument_snapshot
from .transport import LiveYFinanceTransport, YFinanceTransport

YFINANCE_DAILY_BAR_SCHEMA = ProviderNativeSchema(
    required_fields={
        "Date": str,
        "Open": (int, float),
        "High": (int, float),
        "Low": (int, float),
        "Close": (int, float),
        "Adj Close": (int, float),
        "Volume": (int, float),
    }
)

YFINANCE_DAILY_BAR_ACTION_SCHEMA = ProviderNativeSchema(
    required_fields={
        **YFINANCE_DAILY_BAR_SCHEMA.required_fields,
        "Dividends": (int, float),
        "Stock Splits": (int, float),
    }
)


class YFinanceProvider:
    name = "yfinance"
    source_url = "https://finance.yahoo.com"
    license_scope = "research_only_review_yahoo_terms"
    available_at_policy = "session_close_plus_vendor_delay"
    bar_schema = YFINANCE_DAILY_BAR_SCHEMA
    runtime_contract = ProviderRuntimeContract(
        provider="yfinance",
        endpoint="download",
        provider_version="1.5.2",
        adapter_version="phase-2.9a",
        native_schema_version="daily-ohlcv-v1",
        supported_markets=("US",),
        supported_frequencies=("1d",),
    )
    action_runtime_contract = ProviderRuntimeContract(
        provider="yfinance",
        endpoint="download",
        provider_version="1.5.2",
        adapter_version="phase-2.16",
        native_schema_version="daily-ohlcv-actions-v1",
        supported_markets=("US",),
        supported_frequencies=("1d",),
    )

    def __init__(
        self,
        transport: YFinanceTransport | None = None,
        instrument_snapshot: USInstrumentSnapshot | None = None,
        *,
        include_actions: bool = False,
    ) -> None:
        self.transport = transport or LiveYFinanceTransport()
        self.instrument_snapshot = instrument_snapshot or load_us_instrument_snapshot()
        self.include_actions = include_actions
        if include_actions:
            self.runtime_contract = self.action_runtime_contract
            self.bar_schema = YFINANCE_DAILY_BAR_ACTION_SCHEMA

    def list_instruments(
        self, market: Market, symbols: Sequence[str] | None = None
    ) -> Iterable[Instrument]:
        if market != "US":
            raise ValueError("YFinanceProvider supports only the US market")
        return iter(self.instrument_snapshot.select(list(symbols) if symbols is not None else None))

    def fetch_bars(
        self,
        market: Market,
        symbols: Sequence[str],
        start: date,
        end: date,
        frequency: str = "1d",
    ) -> Iterable[Mapping[str, object]]:
        list(self.list_instruments(market, symbols))
        if frequency != "1d":
            raise ValueError("YFinanceProvider supports only daily bars")
        if self.include_actions:
            return self.transport.fetch_daily_bars_with_actions(symbols, start, end)
        return self.transport.fetch_daily_bars(symbols, start, end)

    def transport_audit(self) -> Mapping[str, object]:
        audit_snapshot = getattr(self.transport, "audit_snapshot", None)
        return {
            "instrument_snapshot_id": self.instrument_snapshot.snapshot_id,
            "instrument_snapshot_as_of": self.instrument_snapshot.as_of.isoformat(),
            "frequency": "1d",
            "parallel_requests": False,
            "auto_adjust": False,
            "normalized_price_semantics": "unadjusted_ohlcv",
            "adjusted_close_semantics": "provider_native_raw_only",
            "corporate_actions_in_raw": self.include_actions,
            **(audit_snapshot() if callable(audit_snapshot) else {}),
        }
