from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import UTC, datetime

from ..models import Bar, Instrument
from .models import DatasetQualityReport

BAR_QUALITY_CHECKS = (
    "dataset_non_empty",
    "required_fields",
    "instrument_identity",
    "ohlc_valid",
    "volume_valid",
    "timestamp_ordered",
    "duplicate_check",
)

_REQUIRED_FIELDS = (
    "instrument_id",
    "ts_event",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "currency",
)


class QualityChecker:
    """Collects bar-quality defects without raising on individual bad rows."""

    def check_bars(
        self,
        *,
        dataset_id: str,
        provider: str,
        records: Iterable[Bar | Mapping[str, object]],
        instruments: Mapping[str, Instrument],
    ) -> DatasetQualityReport:
        rows = [self._mapping(record) for record in records]
        failed: set[str] = set()
        findings: list[str] = []
        if not rows:
            failed.add("dataset_non_empty")

        instrument_by_id = {
            instrument.instrument_id: instrument for instrument in instruments.values()
        }
        instrument_by_symbol = {
            instrument.symbol: instrument for instrument in instrument_by_id.values()
        }
        previous_timestamp: dict[str, datetime] = {}
        seen: set[tuple[str, datetime]] = set()

        for index, row in enumerate(rows):
            missing = [field for field in _REQUIRED_FIELDS if row.get(field) is None]
            if missing:
                failed.add("required_fields")
                findings.append(f"row {index}: missing required fields {', '.join(missing)}")

            instrument_id = str(row.get("instrument_id") or "")
            symbol = str(row.get("symbol") or "")
            instrument = instrument_by_id.get(instrument_id)
            if (
                instrument is None
                or (symbol and instrument_by_symbol.get(symbol) != instrument)
                or (instrument is not None and row.get("currency") != instrument.currency)
            ):
                failed.add("instrument_identity")
                findings.append(
                    f"row {index}: unresolved instrument identity "
                    f"instrument_id={instrument_id!r} symbol={symbol!r}"
                )

            prices = self._prices(row, index, failed, findings)
            if prices is not None:
                open_price, high, low, close = prices
                if (
                    min(open_price, high, low, close) < 0
                    or high < open_price
                    or high < close
                    or low > open_price
                    or low > close
                ):
                    failed.add("ohlc_valid")
                    findings.append(f"row {index}: invalid OHLC relationship")

            try:
                volume = float(row["volume"])
                if volume < 0:
                    raise ValueError
            except (KeyError, TypeError, ValueError):
                failed.add("volume_valid")
                findings.append(f"row {index}: volume must be numeric and non-negative")

            timestamp = self._timestamp(row.get("ts_event"))
            if timestamp is None:
                failed.add("timestamp_ordered")
                findings.append(f"row {index}: invalid timestamp")
                continue
            symbol_instrument = instrument_by_symbol.get(symbol)
            identity_key = (
                instrument_id
                or (symbol_instrument.instrument_id if symbol_instrument else symbol)
                or f"row-{index}"
            )
            prior = previous_timestamp.get(identity_key)
            if prior is not None and timestamp < prior:
                failed.add("timestamp_ordered")
                findings.append(f"row {index}: timestamp is earlier than the previous bar")
            previous_timestamp[identity_key] = timestamp
            duplicate_key = (identity_key, timestamp)
            if duplicate_key in seen:
                failed.add("duplicate_check")
                findings.append(f"row {index}: duplicate instrument/timestamp")
            seen.add(duplicate_key)

        passed = [check for check in BAR_QUALITY_CHECKS if check not in failed]
        failed_checks = [check for check in BAR_QUALITY_CHECKS if check in failed]
        status = "failed" if failed_checks else ("warning" if findings else "passed")
        return DatasetQualityReport(
            dataset_id=dataset_id,
            provider=provider,
            checked_rows=len(rows),
            passed_checks=passed,
            failed_checks=failed_checks,
            warnings=findings,
            quality_status=status,
        )

    @staticmethod
    def materialize_bars(records: Iterable[Bar | Mapping[str, object]]) -> list[Bar]:
        bars: list[Bar] = []
        for record in records:
            if isinstance(record, Bar):
                bars.append(record.normalized())
                continue
            payload = {key: value for key, value in record.items() if key != "symbol"}
            payload["ts_event"] = QualityChecker._timestamp(payload.get("ts_event"))
            bars.append(Bar.model_validate(payload).normalized())
        return bars

    @staticmethod
    def _mapping(record: Bar | Mapping[str, object]) -> Mapping[str, object]:
        if isinstance(record, Bar):
            return record.model_dump(mode="python")
        return record

    @staticmethod
    def _timestamp(value: object) -> datetime | None:
        try:
            parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
        except (TypeError, ValueError):
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

    @staticmethod
    def _prices(
        row: Mapping[str, object],
        index: int,
        failed: set[str],
        findings: list[str],
    ) -> tuple[float, float, float, float] | None:
        try:
            return tuple(float(row[field]) for field in ("open", "high", "low", "close"))
        except (KeyError, TypeError, ValueError):
            failed.add("ohlc_valid")
            findings.append(f"row {index}: OHLC values must be numeric")
            return None
