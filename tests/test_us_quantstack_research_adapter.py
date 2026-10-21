import importlib.util
import sys
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

MODULE_PATH = (
    Path(__file__).parents[1]
    / "docker/stock-screener/us_quantstack_research_adapter.py"
)
SPEC = importlib.util.spec_from_file_location("us_quantstack_research_adapter", MODULE_PATH)
assert SPEC and SPEC.loader
module = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = module
SPEC.loader.exec_module(module)


class Client:
    state = {
        "quality_passed": True,
        "catalog_published": True,
        "comparison_complete": False,
        "shadow_eligible": True,
        "candidate_eligible": False,
        "cutover_eligible": False,
    }

    def get_dataset_metadata(self, dataset_id, version="latest"):
        return {
            "dataset_id": dataset_id,
            "dataset_version": version,
            "quality_status": "passed",
            "snapshot_token": "snapshot-us-v1",
            "publication_state": self.state,
        }

    def fetch_instruments(self, payload):
        assert payload["market"] == "US"
        return {
            "items": [
                {
                    "symbol": payload["symbols"][0],
                    "instrument_id": f"XNAS.{payload['symbols'][0]}",
                }
            ]
        }

    def fetch_bars(self, payload):
        return {
            "dataset_id": payload["dataset_id"],
            "dataset_version": payload["dataset_version"],
            "snapshot_token": "snapshot-us-v1",
            "next_cursor": None,
            "items": [
                {
                    "instrument_id": payload["instrument_ids"][0],
                    "ts_event": f"2026-07-{day:02d}T20:00:00Z",
                    "open": 100 + day,
                    "high": 101 + day,
                    "low": 99 + day,
                    "close": 100 + day,
                    "volume": 1000 + day,
                    "adjustment_semantics": "total_return",
                }
                for day in range(1, 26)
            ],
        }


def test_us_adapter_resolves_plain_symbol_only_through_batch_api() -> None:
    adapter = module.USQuantStackResearchAdapter(
        Client(),
        bars_dataset_id="us-equity-total-return-shadow",
        dataset_version="2026-08-25.v1",
    )

    result = adapter.get_price_history(
        "AAPL", start=date(2026, 7, 1), end=date(2026, 7, 25)
    )

    assert result.instrument_id == "XNAS.AAPL"
    assert len(result.frame) == 25
    assert result.frame.attrs["timezone"] == "America/New_York"
    assert result.frame.attrs["research_only"] is True
    assert result.adjustment_semantics == "total_return"


def test_us_adapter_rejects_candidate_or_cutover_eligible_dataset() -> None:
    client = Client()
    client.state = {**client.state, "candidate_eligible": True}
    adapter = module.USQuantStackResearchAdapter(
        client,
        bars_dataset_id="us-equity-total-return-shadow",
        dataset_version="2026-08-25.v1",
    )

    with pytest.raises(module.USResearchAdapterError, match="research shadow"):
        adapter.get_price_history("AAPL", start=date(2026, 7, 1), end=date(2026, 7, 25))


def test_us_research_comparison_reports_kline_indicator_scan_and_watermark() -> None:
    adapter = module.USQuantStackResearchAdapter(
        Client(),
        bars_dataset_id="us-equity-total-return-shadow",
        dataset_version="2026-08-25.v1",
    )
    candidate = adapter.get_price_history(
        "AAPL", start=date(2026, 7, 1), end=date(2026, 7, 25)
    )
    legacy = candidate.frame.copy()
    legacy.index = pd.DatetimeIndex(candidate.frame.index)

    report = module.compare_us_research_view(legacy, candidate)

    assert report["shared_rows"] == 25
    assert report["kline_close_max_absolute_difference"] == 0
    assert report["kline_close_max_difference_bps"] == 0
    assert report["sma20_max_absolute_difference"] == 0
    assert report["sma20_max_difference_bps"] == 0
    assert report["scan_predicate_equal"] is True
    assert report["legacy_latest_date"] == report["candidate_latest_date"]
    assert report["production_path_changed"] is False
