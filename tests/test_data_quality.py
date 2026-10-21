import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from quant_stack.data.ingestion import DataJob, IngestionService, RetryPolicy
from quant_stack.data.models import Instrument
from quant_stack.data.providers import SyntheticProvider
from quant_stack.data.quality import DataQualityError, QualityChecker
from quant_stack.data.storage import LocalFileSystemStorage

ROOT = Path(__file__).parents[1]
FIXTURE_ROOT = ROOT / "tests/fixtures/quality"


def _instrument() -> Instrument:
    return Instrument(
        instrument_id="XNAS.AAPL",
        symbol="AAPL",
        market="US",
        venue="XNAS",
        currency="USD",
        timezone="America/New_York",
    )


def _records(name: str) -> list[dict[str, object]]:
    return json.loads((FIXTURE_ROOT / name).read_text(encoding="utf-8"))


def _check(name: str):
    instrument = _instrument()
    return QualityChecker().check_bars(
        dataset_id="quality_fixture",
        provider="fixture",
        records=_records(name),
        instruments={instrument.instrument_id: instrument, instrument.symbol: instrument},
    )


def test_valid_bar_fixture_passes_all_quality_checks() -> None:
    report = _check("valid_bars.json")
    assert report.quality_status == "passed"
    assert report.checked_rows == 2
    assert report.failed_checks == []
    assert report.warnings == []


@pytest.mark.parametrize(
    ("fixture", "failed_check"),
    [
        ("corrupted_ohlc_bars.json", "ohlc_valid"),
        ("negative_volume_bars.json", "volume_valid"),
        ("duplicate_timestamp_bars.json", "duplicate_check"),
    ],
)
def test_corrupted_bar_fixtures_return_structured_failure(fixture: str, failed_check: str) -> None:
    report = _check(fixture)
    assert report.quality_status == "failed"
    assert failed_check in report.failed_checks
    assert report.warnings


def test_empty_missing_identity_and_reversed_time_are_reported_without_raising() -> None:
    instrument = _instrument()
    checker = QualityChecker()
    empty = checker.check_bars(
        dataset_id="empty",
        provider="fixture",
        records=[],
        instruments={instrument.instrument_id: instrument},
    )
    assert empty.failed_checks == ["dataset_non_empty"]

    records = _records("valid_bars.json")
    records[0].pop("close")
    records[0]["instrument_id"] = ""
    records.reverse()
    report = checker.check_bars(
        dataset_id="damaged",
        provider="fixture",
        records=records,
        instruments={instrument.instrument_id: instrument, instrument.symbol: instrument},
    )
    assert {"required_fields", "instrument_identity", "timestamp_ordered"} <= set(
        report.failed_checks
    )


def test_quality_failure_keeps_raw_and_report_but_blocks_normalized_dataset(
    tmp_path: Path,
) -> None:
    class CorruptedProvider(SyntheticProvider):
        def fetch_bars(self, *args, **kwargs):
            records = list(super().fetch_bars(*args, **kwargs))
            records[0]["high"] = records[0]["open"] - 1
            return records

    provider = CorruptedProvider()
    storage = LocalFileSystemStorage(tmp_path)
    service = IngestionService(providers={provider.name: provider}, storage=storage)
    job = DataJob(
        dataset="quality_gate_test",
        provider="synthetic",
        market="US",
        frequency="1d",
        execution_time=datetime(2026, 7, 29, tzinfo=UTC),
        retry_policy=RetryPolicy(max_attempts=1, backoff_seconds=0),
        symbols=["AAPL"],
        start=date(2026, 7, 20),
        end=date(2026, 7, 21),
    )
    with pytest.raises(DataQualityError) as exc_info:
        service.run(job)

    assert "ohlc_valid" in exc_info.value.report.failed_checks
    assert exc_info.value.report_uri is not None
    assert (tmp_path / exc_info.value.report_uri.removeprefix("quant-data://")).is_file()
    assert [manifest.layer.value for manifest in storage.list_manifests()] == ["raw"]
    assert not (tmp_path / "normalized").exists()
