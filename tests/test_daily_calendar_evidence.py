import importlib.util
import sys
from datetime import UTC, date, datetime
from pathlib import Path

from quant_stack.data.evidence import load_official_calendar_evidence

_ROOT = Path(__file__).resolve().parents[1]
_SCRIPT = _ROOT / "scripts/build_daily_calendar_evidence.py"
_SPEC = importlib.util.spec_from_file_location("daily_calendar_builder_test", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = _MODULE
_SPEC.loader.exec_module(_MODULE)


def test_daily_calendar_covers_current_2026_sessions_and_official_holidays(tmp_path: Path) -> None:
    raw = tmp_path / "raw.json"
    normalized = tmp_path / "normalized.json"
    result = _MODULE.build(
        source_path=_ROOT / "config/cn_daily_calendar_sources_2026.json",
        raw_path=raw,
        normalized_path=normalized,
        observed_at=datetime(2026, 8, 9, tzinfo=UTC),
    )
    assert result == {
        "version": "cn-official-2026-v1",
        "days": 365,
        "sessions": 242,
        "unknown": 0,
    }
    evidence = load_official_calendar_evidence(
        raw_path=raw,
        normalized_path=normalized,
        observed_at=datetime(2026, 8, 9, tzinfo=UTC),
        raw_uri="quant-data://evidence/calendar/cn-official-2026-v1-raw.json",
        normalized_uri="quant-data://evidence/calendar/cn-official-2026-v1-normalized.json",
    )
    assert evidence.supported_start == date(2026, 1, 1)
    assert evidence.supported_end == date(2026, 12, 31)
    assert date(2026, 8, 10) in evidence.confirmed_sessions
    assert date(2026, 8, 9) in evidence.confirmed_non_sessions
    assert date(2026, 10, 1) in evidence.confirmed_non_sessions
