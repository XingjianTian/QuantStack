from __future__ import annotations

import importlib.util
import json
import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace

_ROOT = Path(__file__).resolve().parents[1]
_SCRIPT = _ROOT / "scripts/run_daily_archive_replay_legacy.py"
_SPEC = importlib.util.spec_from_file_location("periodic_daily_replay_test", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = _MODULE
_SPEC.loader.exec_module(_MODULE)


def test_periodic_replay_reuses_completed_import_without_provider_or_qfq(
    monkeypatch, tmp_path: Path
) -> None:
    session = date(2026, 8, 27)
    run_root = tmp_path / "work/daily-20260827"
    run_root.mkdir(parents=True)
    (run_root / "runner-summary.json").write_text(
        json.dumps({"status": "completed", "source_commit": "a" * 40}),
        encoding="utf-8",
    )
    (run_root / "remote-archive-manifest.json").write_text("{}", encoding="utf-8")
    config = SimpleNamespace(
        work_root=tmp_path / "work",
        remote_staging_root="/isolated/daily",
    )
    monkeypatch.setattr(_MODULE, "_load_config", lambda _path: config)
    monkeypatch.setattr(
        _MODULE.FullMarketArchiveManifest,
        "model_validate_json",
        lambda _text: SimpleNamespace(
            session_date=session,
            entries=[object(), object()],
            records_received=100,
        ),
    )
    monkeypatch.setattr(
        _MODULE,
        "_remote_import",
        lambda **_kwargs: {"imported_archives": 2, "idempotent_replays": 2},
    )
    monkeypatch.setattr(
        _MODULE,
        "_remote_verify",
        lambda **_kwargs: {
            "verified_receipts": 2,
            "persisted_bar_rows": 100,
            "duplicate_keys": 0,
            "source_commit": "a" * 40,
        },
    )

    report = _MODULE.run(tmp_path / "config.json", session=session)

    assert report["status"] == "completed"
    assert report["provider_requested"] is False
    assert report["qfq_rebuilt"] is False
    assert (run_root / "periodic-replay-report.json").is_file()
