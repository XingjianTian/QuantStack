from __future__ import annotations

import importlib.util
import io
import json
import sys
import tarfile
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from quant_stack.data.session_parquet import inspect_session_package

ROOT = Path(__file__).parents[1]
SCRIPT = ROOT / "scripts/recover_cn_session_from_legacy_archives.py"
SPEC = importlib.util.spec_from_file_location("recover_cn_session_test", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def _archive(path: Path, rows: list[dict[str, object]]) -> None:
    payload = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(rows), payload)
    parquet = payload.getvalue()
    member = tarfile.TarInfo(
        "artifacts/normalized/bars_1d/market=CN/version=test/bars.parquet"
    )
    member.size = len(parquet)
    with tarfile.open(path, "w") as bundle:
        bundle.addfile(member, io.BytesIO(parquet))


def test_recovers_complete_legacy_batches_without_provider(tmp_path: Path) -> None:
    session = date(2026, 8, 28)
    run_root = tmp_path / "daily-20260828"
    archives = run_root / "archives"
    archives.mkdir(parents=True)
    rows = [
        {
            "instrument_id": instrument,
            "trading_date": session,
            "market": "CN",
            "open": 10.0,
            "high": 11.0,
            "low": 9.0,
            "close": 10.5,
            "volume": 100.0,
        }
        for instrument in ("XSHG.600000", "XSHE.000001")
    ]
    entries = []
    for index, row in enumerate(rows, start=1):
        relative = f"archives/task-{index}.tar"
        _archive(run_root / relative, [row])
        entries.append({"archive_relative_path": relative, "records_received": 1})
    (run_root / "archive-manifest.json").write_text(
        json.dumps(
            {
                "schema_version": "1.0.0",
                "session_date": session.isoformat(),
                "active_universe_instruments": 2,
                "entries": entries,
            }
        ),
        encoding="utf-8",
    )

    output = tmp_path / "sessions"
    first = MODULE.recover(
        legacy_run_root=run_root,
        output_root=output,
        session=session,
    )
    second = MODULE.recover(
        legacy_run_root=run_root,
        output_root=output,
        session=session,
    )

    assert first["provider_requested"] is False
    assert first["legacy_batches_read"] == 2
    assert first["rows"] == 2
    assert first["installed"] is True
    assert second["installed"] is False
    package = inspect_session_package(output / "market=CN/session=2026-08-28")
    assert package.row_count == package.instrument_count == 2
