from __future__ import annotations

import json
from pathlib import Path


def compare_metrics(
    baseline_path: str | Path,
    candidate_path: str | Path,
    tolerances: dict[str, float],
) -> list[str]:
    baseline = json.loads(Path(baseline_path).read_text(encoding="utf-8"))["metrics"]
    candidate = json.loads(Path(candidate_path).read_text(encoding="utf-8"))["metrics"]
    failures: list[str] = []
    for metric, tolerance in tolerances.items():
        if metric not in baseline or metric not in candidate:
            failures.append(f"{metric}: missing")
            continue
        delta = abs(float(candidate[metric]) - float(baseline[metric]))
        if delta > tolerance:
            failures.append(f"{metric}: delta {delta:g} exceeds {tolerance:g}")
    return failures
