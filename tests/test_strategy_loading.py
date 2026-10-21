from pathlib import Path

import pytest
from pydantic import ValidationError

from quant_stack.models import StrategyStatus, load_strategy_spec

ROOT = Path(__file__).parents[1]


def test_example_strategy_loads() -> None:
    spec = load_strategy_spec(ROOT / "strategies/examples/sma_cross/strategy_spec.yaml")
    assert spec.strategy_id == "sma_cross_demo"
    assert spec.status is StrategyStatus.RESEARCH
    assert spec.entry_rules[0].execution_timing == "next_bar_open"


def test_unknown_status_is_rejected(tmp_path: Path) -> None:
    source = (ROOT / "strategy_spec.yaml").read_text(encoding="utf-8")
    invalid = tmp_path / "strategy.yaml"
    invalid.write_text(source.replace("status: research", "status: invented"), encoding="utf-8")
    with pytest.raises(ValidationError):
        load_strategy_spec(invalid)
