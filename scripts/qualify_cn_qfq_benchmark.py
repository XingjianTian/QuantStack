#!/usr/bin/env python3
"""Qualify the qfq factor direction against independent corporate-action events."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def _candidate_return(sample: dict[str, object], direction: str) -> float:
    before = float(sample["raw_close_before"])
    after = float(sample["raw_close_after"])
    factor_before = float(sample["factor_before"])
    factor_after = float(sample["factor_after"])
    latest = float(sample["factor_latest"])
    if direction == "A":
        adjusted_before = before * factor_before / latest
        adjusted_after = after * factor_after / latest
    elif direction == "B":
        adjusted_before = before * latest / factor_before
        adjusted_after = after * latest / factor_after
    else:
        raise ValueError("unknown qfq candidate direction")
    return adjusted_after / adjusted_before - 1.0


def qualify_benchmark(payload: dict[str, object]) -> dict[str, object]:
    samples = payload.get("samples")
    if not isinstance(samples, list) or not samples:
        raise ValueError("qfq benchmark requires event samples")
    event_types = {str(sample.get("event_type")) for sample in samples}
    required = {"split_bonus_transfer", "cash_dividend", "no_corporate_action"}
    if not required.issubset(event_types):
        raise ValueError("qfq benchmark is missing a required event control")

    results = []
    for sample in samples:
        result = {
            "sample_id": sample["sample_id"],
            "event_type": sample["event_type"],
            "return_a": _candidate_return(sample, "A"),
            "return_b": _candidate_return(sample, "B"),
        }
        if sample["event_type"] == "no_corporate_action":
            raw_return = (
                float(sample["raw_close_after"]) / float(sample["raw_close_before"]) - 1.0
            )
            result["raw_return"] = raw_return
            result["control_pass"] = (
                abs(result["return_a"] - raw_return) <= 1e-12
                and abs(result["return_b"] - raw_return) <= 1e-12
            )
        else:
            result["a_continuity_pass"] = abs(result["return_a"]) <= 0.05
            result["b_continuity_pass"] = abs(result["return_b"]) <= 0.05
        results.append(result)

    action_results = [row for row in results if "a_continuity_pass" in row]
    controls_pass = all(row.get("control_pass", True) for row in results)
    direction_a_pass = controls_pass and all(row["a_continuity_pass"] for row in action_results)
    direction_b_pass = controls_pass and all(row["b_continuity_pass"] for row in action_results)
    selected = "A" if direction_a_pass and not direction_b_pass else None
    return {
        "status": "passed" if selected else "blocked",
        "selected_formula": (
            "raw_price * factor_t / factor_latest" if selected == "A" else None
        ),
        "selected_direction": selected,
        "latest_valid_date_anchor": "qfq_equals_raw",
        "raw_bars_unchanged": True,
        "samples": results,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = qualify_benchmark(json.loads(args.input.read_text(encoding="utf-8")))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
