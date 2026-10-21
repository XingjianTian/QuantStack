import json
import sys
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parents[1] / "scripts"))

from build_cn_research_release import _build_database


def _write_parquet(path: Path, data: dict[str, list[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table(data), path)


def test_build_database_includes_metadata_and_all_quarantine_scopes(tmp_path: Path) -> None:
    bars_index = tmp_path / "bars.duckdb"
    bars = duckdb.connect(str(bars_index))
    bars.execute(
        "CREATE TABLE pilot_bars AS SELECT * FROM (VALUES "
        "('XSHG.600000', DATE '2020-01-02', 10.0), "
        "('XSHE.000001', DATE '2020-01-02', 20.0)) "
        "AS t(instrument_id,trading_date,close)"
    )
    bars.close()

    files = {
        "calendar": tmp_path / "calendar.parquet",
        "lifecycle": tmp_path / "lifecycle.parquet",
        "actions": tmp_path / "actions.parquet",
        "factors": tmp_path / "factors.parquet",
        "audit": tmp_path / "audit.parquet",
        "audit_quarantine": tmp_path / "audit-quarantine.json",
        "lifecycle_quarantine": tmp_path / "lifecycle-quarantine.json",
        "factors_quarantine": tmp_path / "factors-quarantine.json",
    }
    _write_parquet(
        files["calendar"],
        {
            "date": ["2020-01-02"],
            "venue": ["XSHG"],
            "state": ["session"],
            "is_session": [True],
            "timezone": ["Asia/Shanghai"],
            "open_at": ["2020-01-02T09:30:00+08:00"],
            "close_at": ["2020-01-02T15:00:00+08:00"],
            "calendar_version": ["test"],
        },
    )
    _write_parquet(
        files["lifecycle"],
        {
            "instrument_id": ["XSHG.600000"],
            "listing_date": ["1999-11-10"],
            "delisting_date": [None],
        },
    )
    for key in ("actions", "factors"):
        _write_parquet(
            files[key],
            {
                "instrument_id": ["XSHG.600000"],
                "effective_date": ["2020-01-02"],
            },
        )
    _write_parquet(
        files["audit"],
        {
            "instrument_id": ["XSHG.600000"],
            "date": ["2020-01-02"],
            "coverage_status": ["bar_present"],
        },
    )
    files["audit_quarantine"].write_text(
        json.dumps(
            {
                "rows": [
                    {
                        "instrument_id": "XSHG.600000",
                        "trading_date": "2020-01-03",
                        "status": "unexpected",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    files["lifecycle_quarantine"].write_text(
        json.dumps(
            {
                "rows": [
                    {
                        "instrument_id": "XSHG.600000",
                        "trading_date": "2020-01-04",
                        "reason": "conflict",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    files["factors_quarantine"].write_text(
        json.dumps(
            {
                "rows": [
                    {
                        "instrument_id": "XSHG.600000",
                        "effective_date": "2020-01-05",
                        "reason": "factor_conflict",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    temporary = tmp_path / "release.duckdb"
    metrics = _build_database(temporary=temporary, bars_index=bars_index, files=files)
    assert metrics["rows"] == 2
    connection = duckdb.connect(str(temporary), read_only=True)
    assert connection.execute("SELECT count(*) FROM research_release_metadata").fetchone()[0] == 20
    assert connection.execute("SELECT count(*) FROM quarantine_scopes").fetchone()[0] == 3
    assert set(
        connection.execute("SELECT scope FROM quarantine_scopes ORDER BY scope").fetchnumpy()[
            "scope"
        ]
    ) == {"coverage_audit", "factor_conflicts", "lifecycle_conflicts"}
    connection.close()
