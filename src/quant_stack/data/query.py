from __future__ import annotations

from datetime import UTC, date, datetime, time

from .interfaces import DataStorage
from .models import Bar, DatasetKind, DatasetLayer, DatasetManifest, Instrument, Market


class MarketDataRepository:
    """Read model over DataStorage; callers never access Parquet or DuckDB directly."""

    def __init__(self, storage: DataStorage) -> None:
        self.storage = storage

    def datasets(self) -> list[DatasetManifest]:
        return self.storage.list_manifests()

    def instruments(self, market: Market | None = None) -> list[Instrument]:
        manifests = self.storage.list_manifests(
            kind=DatasetKind.INSTRUMENTS,
            market=market,
            layer=DatasetLayer.NORMALIZED,
        )
        latest: dict[str, Instrument] = {}
        for manifest in reversed(manifests):
            for record in self.storage.read_dataset(manifest.storage_uri):
                normalized_record = dict(record)
                normalized_record.setdefault("market", manifest.market)
                instrument = Instrument.model_validate(normalized_record)
                latest[instrument.instrument_id] = instrument
        return sorted(latest.values(), key=lambda item: (item.market, item.symbol))

    def bars(
        self,
        symbol: str,
        *,
        market: Market,
        frequency: str,
        start: date | None = None,
        end: date | None = None,
        limit: int = 10_000,
    ) -> list[Bar]:
        instruments = self.instruments(market)
        instrument = next(
            (
                item
                for item in instruments
                if item.symbol.casefold() == symbol.casefold()
                or item.instrument_id.casefold() == symbol.casefold()
            ),
            None,
        )
        if instrument is None:
            raise LookupError(f"Unknown instrument in {market}: {symbol}")
        manifests = self.storage.list_manifests(
            kind=DatasetKind.MARKET_BARS,
            market=market,
            layer=DatasetLayer.NORMALIZED,
            frequency=frequency,
        )
        if not manifests:
            return []
        start_at = datetime.combine(start, time.min, tzinfo=UTC) if start else None
        end_at = datetime.combine(end, time.max, tzinfo=UTC) if end else None
        records: dict[datetime, Bar] = {}
        for manifest in reversed(manifests):
            for record in self.storage.read_dataset(manifest.storage_uri):
                if record.get("instrument_id") != instrument.instrument_id:
                    continue
                bar = Bar.model_validate(record).normalized()
                if start_at and bar.ts_event < start_at:
                    continue
                if end_at and bar.ts_event > end_at:
                    continue
                records[bar.ts_event] = bar
        return sorted(records.values(), key=lambda item: item.ts_event)[-limit:]
