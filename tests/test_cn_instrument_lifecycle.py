from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from scripts.build_cn_instrument_lifecycle import build_lifecycle

ROOT = Path(__file__).parents[1]


def test_lifecycle_replay_keeps_missing_facts_explicit(tmp_path: Path) -> None:
    universe = json.loads(
        (ROOT / "config/cn_backfill_medium_universe_v1.json").read_text()
    )
    selected = universe["instruments"][:2]
    raw = tmp_path / "source.json"
    raw.write_text(
        json.dumps(
            {
                "schema_version": "1.0.0",
                "provider": "baostock",
                "provider_version": "0.9.3",
                "endpoint": "query_stock_basic",
                "native_fields": ["code", "code_name", "ipoDate", "outDate", "type", "status"],
                "retrieved_at": "2026-08-04T00:00:00+00:00",
                "rows": [
                    {
                        "code": selected[0]["provider_symbol"],
                        "code_name": "Sample",
                        "ipoDate": "1999-11-10",
                        "outDate": "",
                        "type": "1",
                        "status": "1",
                    }
                ],
            }
        )
    )
    manifest = build_lifecycle(
        universe_path=ROOT / "config/cn_backfill_medium_universe_v1.json",
        output_root=tmp_path / "release",
        raw_path=raw,
        observed_at=datetime(2026, 8, 4, tzinfo=UTC),
    )
    normalized = json.loads(
        (tmp_path / "release/normalized/instrument-lifecycle.json").read_text()
    )["rows"]
    assert len(normalized) == 500
    assert normalized[0]["evidence_status"] == "partial"
    assert normalized[0]["listing_date"] == "1999-11-10"
    assert normalized[1]["evidence_status"] == "unavailable"
    assert normalized[1]["listing_date"] is None
    assert manifest["pit_safe"] is False
    assert (tmp_path / "release/normalized/instrument-lifecycle.parquet").is_file()
