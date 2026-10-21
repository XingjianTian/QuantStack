import importlib.util
import sys
from datetime import date
from pathlib import Path

import pytest

MODULE_PATH = Path(__file__).parents[1] / "scripts/validate_us_ss_research_shadow.py"
SPEC = importlib.util.spec_from_file_location("validate_us_ss_research_shadow", MODULE_PATH)
assert SPEC and SPEC.loader
module = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = module
SPEC.loader.exec_module(module)


@pytest.fixture
def valid_report() -> dict[str, object]:
    return {
        "shared_rows": 2512,
        "legacy_latest_date": "2026-08-24",
        "candidate_latest_date": "2026-08-24",
        "kline_close_max_difference_bps": 0.02,
        "sma20_max_difference_bps": 0.01,
        "scan_predicate_equal": True,
        "research_only": True,
        "production_path_changed": False,
    }


def passes(report: dict[str, object]) -> bool:
    return module._comparison_passes(
        report,
        expected_date=date(2026, 8, 24),
        max_difference_bps=1.0,
    )


def test_acceptance_requires_all_parity_checks(valid_report) -> None:
    assert passes(valid_report)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("legacy_latest_date", "2026-08-21"),
        ("candidate_latest_date", "2026-08-21"),
        ("kline_close_max_difference_bps", 1.01),
        ("sma20_max_difference_bps", 1.01),
        ("scan_predicate_equal", False),
    ],
)
def test_each_parity_regression_blocks_acceptance(valid_report, field, value) -> None:
    valid_report[field] = value

    assert not passes(valid_report)
