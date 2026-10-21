from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path

import pytest
from pydantic import ValidationError

from quant_stack.data.calendars import SyntheticMarketCalendar
from quant_stack.data.models import BarLineage
from quant_stack.data.providers.cn.replay import (
    AKShareReplayProvider,
    BaoStockReplayProvider,
    normalize_akshare_replay_bars,
    normalize_baostock_replay_bars,
)
from quant_stack.data.storage import StagingFileSystemStorage


def _lookup(provider) -> dict[str, object]:
    instruments = list(provider.list_instruments("CN"))
    lookup = {item.instrument_id: item for item in instruments}
    lookup.update({item.symbol: item for item in instruments})
    return lookup


def test_akshare_provider_alias_is_not_canonical_mic() -> None:
    calendar = SyntheticMarketCalendar()
    akshare = AKShareReplayProvider()
    ak_records = list(
        akshare.fetch_bars(
            "CN",
            ["XSHG.600000", "XSHE.000001", "BJSE.430047"],
            date(2026, 7, 27),
            date(2026, 7, 27),
        )
    )
    normalized = normalize_akshare_replay_bars(
        ak_records, _lookup(akshare), calendar
    )
    aliases = {
        item["instrument_id"]: (
            item["lineage"]["provider_exchange_alias"],
            item["lineage"]["canonical_mic"],
        )
        for item in normalized
    }
    assert aliases == {
        "XSHG.600000": ("SH", "XSHG"),
        "XSHE.000001": ("SZ", "XSHE"),
        "BJSE.430047": ("BJ", "BJSE"),
    }
    assert all(alias != mic for alias, mic in aliases.values())

    baostock = BaoStockReplayProvider()
    bao_records = list(
        baostock.fetch_bars(
            "CN",
            ["XSHG.600000", "XSHE.000001"],
            date(2026, 7, 27),
            date(2026, 7, 27),
        )
    )
    bao_normalized = normalize_baostock_replay_bars(
        bao_records, _lookup(baostock), calendar
    )
    assert {
        item["lineage"]["provider_exchange_alias"] for item in bao_normalized
    } == {"sh", "sz"}

    bad_lookup = _lookup(akshare)
    bad_lookup["600000"] = bad_lookup["600000"].model_copy(
        update={"provider_exchange_alias": "UNKNOWN"}
    )
    bad_lookup["XSHG.600000"] = bad_lookup["600000"]
    with pytest.raises(ValueError, match="Unknown provider exchange alias"):
        normalize_akshare_replay_bars([ak_records[0]], bad_lookup, calendar)


def test_lineage_timestamps_are_timezone_aware(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="timezone-aware"):
        BarLineage(source_timestamp=datetime(2026, 7, 27, 15))
    with pytest.raises(ValidationError):
        BarLineage(source_timestamp="not-a-timestamp")

    source = datetime(2026, 7, 27, 15, tzinfo=timezone(timedelta(hours=8)))
    retrieval = datetime(2026, 7, 27, 8, tzinfo=UTC)
    lineage = BarLineage(
        source_timestamp=source,
        retrieval_timestamp=retrieval,
    )
    assert lineage.source_timestamp == source
    assert lineage.retrieval_timestamp == retrieval
    with pytest.raises(ValidationError, match="earlier"):
        BarLineage(
            source_timestamp=source,
            retrieval_timestamp=datetime(2026, 7, 27, 6, tzinfo=UTC),
        )

    storage = StagingFileSystemStorage(tmp_path)
    artifact = storage.write_parquet_dataset(
        "normalized/lineage.parquet",
        [{"lineage": lineage.model_dump(mode="python")}],
    )
    round_trip = storage.read_dataset(artifact.uri)[0]["lineage"]
    assert isinstance(round_trip["source_timestamp"], datetime)
    assert isinstance(round_trip["retrieval_timestamp"], datetime)
    assert round_trip["source_timestamp"].tzinfo is not None
    assert round_trip["retrieval_timestamp"].tzinfo is not None
