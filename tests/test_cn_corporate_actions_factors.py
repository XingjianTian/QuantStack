from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from scripts.build_cn_corporate_actions_factors import build_actions_factors

ROOT = Path(__file__).parents[1]


def test_corporate_action_replay_blocks_unvalidated_views(tmp_path: Path) -> None:
    universe = json.loads(
        (ROOT / "config/cn_backfill_medium_universe_v1.json").read_text()
    )
    selected = universe["instruments"][0]
    factor_raw = tmp_path / "factor.json"
    action_raw = tmp_path / "action.json"
    factor_raw.write_text(
        json.dumps(
            {
                "schema_version": "1.0.0",
                "provider": "baostock",
                "provider_version": "0.9.3",
                "factor_endpoint": "query_adjust_factor",
                "requests": [
                    {
                        "instrument_id": selected["instrument_id"],
                        "provider_symbol": selected["provider_symbol"],
                        "status": "ok",
                        "fields": ["code", "dividOperateDate", "adjustFactor"],
                        "rows": [
                            {
                                "code": selected["provider_symbol"],
                                "dividOperateDate": "2020-01-02",
                                "adjustFactor": "1.0",
                            }
                        ],
                    }
                ],
            }
        )
    )
    action_raw.write_text(
        json.dumps(
            {
                "schema_version": "1.0.0",
                "provider": "baostock",
                "provider_version": "0.9.3",
                "action_endpoint": "query_dividend_data",
                "requests": [
                    {
                        "instrument_id": selected["instrument_id"],
                        "provider_symbol": selected["provider_symbol"],
                        "year": 2020,
                        "status": "ok",
                        "fields": ["code", "dividOperateDate"],
                        "rows": [
                            {
                                "code": selected["provider_symbol"],
                                "dividOperateDate": "2020-01-02",
                            }
                        ],
                    }
                ],
            }
        )
    )
    manifest = build_actions_factors(
        universe_path=ROOT / "config/cn_backfill_medium_universe_v1.json",
        output_root=tmp_path / "release",
        factor_raw_path=factor_raw,
        action_raw_path=action_raw,
        start_date="2015-01-01",
        end_date="2026-08-04",
        action_years=[2020],
        max_action_requests=1,
        observed_at=datetime(2026, 8, 4, tzinfo=UTC),
    )
    assert manifest["pit_safe"] is False
    assert manifest["qfq_view"] == "blocked"
    factors = json.loads(
        (tmp_path / "release/normalized/adjustment-factors.json").read_text()
    )["rows"]
    assert factors[0]["factor_direction"] == "provider_native_direction_unverified"
    assert (tmp_path / "release/normalized/corporate-actions.parquet").is_file()
