from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from datetime import date, datetime
from pathlib import Path

from ....ingestion.contracts import ProviderRuntimeContract
from ....interfaces import MarketCalendarProvider
from ....models import Instrument, Market
from ....quality import ProviderNativeSchema
from ..capabilities import (
    AKSHARE_CANDIDATE_CAPABILITIES,
    AKSHARE_STOCK_ZH_A_HIST_CANDIDATE,
    BAOSTOCK_CANDIDATE_CAPABILITIES,
    BAOSTOCK_DAILY_CANDIDATE,
)

_FIXTURE_ROOT = Path(__file__).parent / "fixtures"

BAOSTOCK_REPLAY_DAILY_BAR_SCHEMA = ProviderNativeSchema(
    required_fields={
        "date": str,
        "code": str,
        "open": str,
        "high": str,
        "low": str,
        "close": str,
        "volume": str,
        "amount": str,
        "adjustflag": str,
    }
)

AKSHARE_REPLAY_DAILY_BAR_SCHEMA = ProviderNativeSchema(
    required_fields={
        "日期": str,
        "股票代码": str,
        "开盘": (int, float),
        "最高": (int, float),
        "最低": (int, float),
        "收盘": (int, float),
        "成交量": (int, float),
        "成交额": (int, float),
        "复权": str,
    }
)

BAOSTOCK_REPLAY_RUNTIME_CONTRACT = ProviderRuntimeContract(
    provider="baostock_replay",
    endpoint=BAOSTOCK_DAILY_CANDIDATE.metadata.endpoint,
    provider_version=BAOSTOCK_DAILY_CANDIDATE.metadata.provider_version,
    adapter_version=BAOSTOCK_DAILY_CANDIDATE.metadata.adapter_version,
    native_schema_version=BAOSTOCK_DAILY_CANDIDATE.metadata.native_schema_version,
    supported_markets=("CN",),
    supported_frequencies=("1d",),
)

AKSHARE_REPLAY_RUNTIME_CONTRACT = ProviderRuntimeContract(
    provider="akshare_replay",
    endpoint=AKSHARE_STOCK_ZH_A_HIST_CANDIDATE.metadata.endpoint,
    provider_version=AKSHARE_STOCK_ZH_A_HIST_CANDIDATE.metadata.provider_version,
    adapter_version=AKSHARE_STOCK_ZH_A_HIST_CANDIDATE.metadata.adapter_version,
    native_schema_version=AKSHARE_STOCK_ZH_A_HIST_CANDIDATE.metadata.native_schema_version,
    supported_markets=("CN",),
    supported_frequencies=("1d",),
)


def _load_fixture(name: str) -> dict[str, object]:
    return json.loads((_FIXTURE_ROOT / name).read_text(encoding="utf-8"))


class _CNReplayProvider:
    market: Market = "CN"
    instrument_fixture: str
    daily_bar_fixture: str
    symbol_field: str
    date_field: str

    def _instrument_payloads(self) -> list[dict[str, object]]:
        payload = _load_fixture(self.instrument_fixture)
        records = payload.get("records")
        if not isinstance(records, list):
            raise ValueError(f"Invalid instrument fixture: {self.instrument_fixture}")
        return [dict(item) for item in records]

    def _bar_payloads(self) -> list[dict[str, object]]:
        payload = _load_fixture(self.daily_bar_fixture)
        records = payload.get("records")
        if not isinstance(records, list):
            raise ValueError(f"Invalid daily-bar fixture: {self.daily_bar_fixture}")
        return [dict(item) for item in records]

    def _record_symbol(self, record: Mapping[str, object]) -> str:
        return str(record[self.symbol_field])

    def list_instruments(
        self, market: Market, symbols: Sequence[str] | None = None
    ) -> Iterable[Instrument]:
        if market != self.market:
            raise ValueError(f"{self.name} supports only CN replay data")
        records = self._instrument_payloads()
        by_alias: dict[str, dict[str, object]] = {}
        for record in records:
            for alias in (
                str(record["instrument_id"]),
                str(record["symbol"]),
                str(record["provider_symbol"]),
            ):
                by_alias[alias] = record
        requested = (
            list(symbols)
            if symbols is not None
            else [str(item["symbol"]) for item in records]
        )
        missing = [symbol for symbol in requested if symbol not in by_alias]
        if missing:
            raise ValueError(f"Unsupported {self.name} replay symbols: {', '.join(missing)}")
        emitted: set[str] = set()
        for requested_symbol in requested:
            record = by_alias[requested_symbol]
            instrument_id = str(record["instrument_id"])
            if instrument_id in emitted:
                continue
            emitted.add(instrument_id)
            yield Instrument(
                instrument_id=instrument_id,
                symbol=str(record["symbol"]),
                market="CN",
                venue=str(record["venue"]),
                provider_symbol=str(record["provider_symbol"]),
                provider_exchange_alias=str(record["provider_exchange_alias"]),
                currency="CNY",
                timezone="Asia/Shanghai",
                name=str(record["name"]),
                active=bool(record.get("active", True)),
            )

    def fetch_bars(
        self,
        market: Market,
        symbols: Sequence[str],
        start: date,
        end: date,
        frequency: str = "1d",
    ) -> Iterable[Mapping[str, object]]:
        instruments = list(self.list_instruments(market, symbols))
        if frequency != "1d":
            raise ValueError(f"{self.name} replay supports only daily bars")
        selected_symbols = {item.symbol for item in instruments}
        for record in self._bar_payloads():
            trading_date = date.fromisoformat(str(record[self.date_field]))
            if start <= trading_date <= end and self._record_symbol(record) in selected_symbols:
                yield record


class BaoStockReplayProvider(_CNReplayProvider):
    name = "baostock_replay"
    source_url = "fixture://cn/baostock/query_history_k_data_plus"
    license_scope = "offline_fixture_only"
    available_at_policy = "fixture_session_close"
    endpoint = "query_history_k_data_plus"
    cn_capabilities = BAOSTOCK_CANDIDATE_CAPABILITIES
    instrument_fixture = "baostock_instruments.json"
    daily_bar_fixture = "baostock_daily_bars.json"
    symbol_field = "code"
    date_field = "date"
    bar_schema = BAOSTOCK_REPLAY_DAILY_BAR_SCHEMA
    runtime_contract = BAOSTOCK_REPLAY_RUNTIME_CONTRACT

    def _record_symbol(self, record: Mapping[str, object]) -> str:
        return str(record["code"]).split(".", maxsplit=1)[-1]


class AKShareReplayProvider(_CNReplayProvider):
    name = "akshare_replay"
    source_url = "fixture://cn/akshare/stock_zh_a_hist"
    license_scope = "offline_fixture_only"
    available_at_policy = "fixture_session_close"
    endpoint = "stock_zh_a_hist"
    cn_capabilities = AKSHARE_CANDIDATE_CAPABILITIES
    instrument_fixture = "akshare_instruments.json"
    daily_bar_fixture = "akshare_daily_bars.json"
    symbol_field = "股票代码"
    date_field = "日期"
    bar_schema = AKSHARE_REPLAY_DAILY_BAR_SCHEMA
    runtime_contract = AKSHARE_REPLAY_RUNTIME_CONTRACT


def _instrument_for_symbol(
    instruments: Mapping[str, Instrument], symbol: str
) -> Instrument | None:
    instrument = instruments.get(symbol)
    if instrument is not None:
        return instrument
    return next((item for item in instruments.values() if item.symbol == symbol), None)


def _lineage(
    *,
    instrument: Instrument,
    provider_symbol: str,
    runtime_contract: ProviderRuntimeContract,
    metadata,
    source_timestamp: datetime,
) -> dict[str, object]:
    alias = instrument.provider_exchange_alias
    alias_to_mic = {
        "sh": "XSHG",
        "sz": "XSHE",
        "SH": "XSHG",
        "SZ": "XSHE",
        "BJ": "BJSE",
    }
    if alias is None or alias not in alias_to_mic:
        raise ValueError(f"Unknown provider exchange alias: {alias!r}")
    if alias_to_mic[alias] != instrument.venue:
        raise ValueError(
            f"Provider exchange alias {alias!r} conflicts with canonical MIC {instrument.venue}"
        )
    return {
        "provider": runtime_contract.provider,
        "endpoint": runtime_contract.endpoint,
        "provider_version": runtime_contract.provider_version,
        "adapter_version": runtime_contract.adapter_version,
        "native_schema_version": runtime_contract.native_schema_version,
        "provider_symbol": provider_symbol,
        "provider_exchange_alias": alias,
        "canonical_mic": instrument.venue,
        "source_timestamp": source_timestamp,
        "retrieval_timestamp": datetime.fromisoformat("2026-07-29T00:00:00+00:00"),
        "original_volume_unit": metadata.volume_unit.value,
        "applied_volume_multiplier": metadata.volume_multiplier_to_shares,
        "adjustment": "none",
    }


def normalize_baostock_replay_bars(
    records: Iterable[Mapping[str, object]],
    instruments: Mapping[str, Instrument],
    calendar: MarketCalendarProvider,
) -> list[Mapping[str, object]]:
    bars: list[Mapping[str, object]] = []
    for record in records:
        symbol = str(record.get("code", "")).split(".", maxsplit=1)[-1]
        instrument = _instrument_for_symbol(instruments, symbol)
        trading_date = datetime.fromisoformat(str(record["date"])).date()
        bars.append(
            {
                "symbol": symbol,
                "instrument_id": instrument.instrument_id if instrument else None,
                "ts_event": calendar.session_close("CN", trading_date) if instrument else None,
                "open": float(str(record["open"])),
                "high": float(str(record["high"])),
                "low": float(str(record["low"])),
                "close": float(str(record["close"])),
                "volume": float(str(record["volume"])),
                "amount": float(str(record["amount"])),
                "vwap": None,
                "currency": "CNY" if instrument else None,
                "lineage": _lineage(
                    instrument=instrument,
                    provider_symbol=str(record["code"]),
                    runtime_contract=BAOSTOCK_REPLAY_RUNTIME_CONTRACT,
                    metadata=BAOSTOCK_DAILY_CANDIDATE.metadata,
                    source_timestamp=datetime.fromisoformat(
                        f"{record['date']}T15:00:00+08:00"
                    ),
                ) if instrument else {},
            }
        )
    return bars


def normalize_akshare_replay_bars(
    records: Iterable[Mapping[str, object]],
    instruments: Mapping[str, Instrument],
    calendar: MarketCalendarProvider,
) -> list[Mapping[str, object]]:
    bars: list[Mapping[str, object]] = []
    for record in records:
        symbol = str(record.get("股票代码", ""))
        instrument = _instrument_for_symbol(instruments, symbol)
        trading_date = datetime.fromisoformat(str(record["日期"])).date()
        bars.append(
            {
                "symbol": symbol,
                "instrument_id": instrument.instrument_id if instrument else None,
                "ts_event": calendar.session_close("CN", trading_date) if instrument else None,
                "open": record["开盘"],
                "high": record["最高"],
                "low": record["最低"],
                "close": record["收盘"],
                "volume": float(record["成交量"]) * 100,
                "amount": float(record["成交额"]),
                "vwap": None,
                "currency": "CNY" if instrument else None,
                "lineage": _lineage(
                    instrument=instrument,
                    provider_symbol=symbol,
                    runtime_contract=AKSHARE_REPLAY_RUNTIME_CONTRACT,
                    metadata=AKSHARE_STOCK_ZH_A_HIST_CANDIDATE.metadata,
                    source_timestamp=datetime.fromisoformat(
                        f"{record['日期']}T15:00:00+08:00"
                    ),
                ) if instrument else {},
            }
        )
    return bars
