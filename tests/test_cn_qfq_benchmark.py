from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[1]
FIXTURE = ROOT / "tests/fixtures/stock_screener/qfq_benchmark_t133.json"
SCRIPT = ROOT / "scripts/qualify_cn_qfq_benchmark.py"


def _qualify(payload: dict[str, object]) -> dict[str, object]:
    spec = importlib.util.spec_from_file_location("t133_qfq_benchmark", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module.qualify_benchmark(payload)


def test_independent_events_select_forward_adjustment_direction() -> None:
    result = _qualify(json.loads(FIXTURE.read_text(encoding="utf-8")))

    assert result["status"] == "passed"
    assert result["selected_direction"] == "A"
    assert result["selected_formula"] == "raw_price * factor_t / factor_latest"
    assert result["latest_valid_date_anchor"] == "qfq_equals_raw"
    assert result["raw_bars_unchanged"] is True
    action_samples = [row for row in result["samples"] if "a_continuity_pass" in row]
    assert all(row["a_continuity_pass"] for row in action_samples)
    assert all(not row["b_continuity_pass"] for row in action_samples)


def test_missing_required_event_control_fails_closed() -> None:
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    payload["samples"] = [
        sample for sample in payload["samples"] if sample["event_type"] != "cash_dividend"
    ]

    with pytest.raises(ValueError, match="missing a required event control"):
        _qualify(payload)
