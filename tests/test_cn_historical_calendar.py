from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

from scripts.build_cn_historical_calendar import build_calendar

ROOT = Path(__file__).parents[1]


def test_historical_calendar_rebuild_is_complete_and_offline(tmp_path: Path) -> None:
    manifest = build_calendar(
        start=date(2015, 1, 1),
        end=date(2015, 1, 10),
        source_config_path=ROOT / "config/cn_historical_calendar_sources_v1.json",
        output_root=tmp_path,
        observed_at=datetime(2026, 8, 4, tzinfo=UTC),
    )
    assert manifest["unknown_rows"] == 0
    assert manifest["sessions_per_venue"] == 5
    normalized = json.loads((tmp_path / "normalized/calendar-days.json").read_text())
    assert len(normalized["rows"]) == 20
    assert {row["state"] for row in normalized["rows"]} == {"session", "weekend", "holiday"}
    assert all(row["timezone"] == "Asia/Shanghai" for row in normalized["rows"])
    assert (tmp_path / "manifest.json").is_file()
