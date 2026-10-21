"""Compare SS technical and price-driven screener results on legacy versus qfq bars."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime, timedelta
from typing import Any

import pandas as pd
from app.scanners.criteria.moving_averages import MovingAverageAnalyzer
from app.scanners.criteria.stage_analysis import WeinsteinstageAnalyzer
from app.scanners.criteria.vcp_detection import VCPDetector
from app.services.technical_calculator_service import TechnicalCalculatorService
from quant_workspace_compat import PriceCacheService
from quantstack_consumer_adapter import SSQuantStackConsumerAdapter


def _scanner_signature(frame: pd.DataFrame) -> dict[str, object]:
    close = frame["Close"].reset_index(drop=True)
    volume = frame["Volume"].reset_index(drop=True)
    current = float(close.iloc[-1])
    ma_50 = float(close.rolling(50, min_periods=50).mean().iloc[-1])
    ma_150 = float(close.rolling(150, min_periods=150).mean().iloc[-1])
    ma_200_series = close.rolling(200, min_periods=200).mean()
    ma_200 = float(ma_200_series.iloc[-1])
    ma_200_month_ago = float(ma_200_series.iloc[-21])
    ma = MovingAverageAnalyzer().comprehensive_ma_analysis(
        current, ma_50, ma_150, ma_200, ma_200_month_ago
    )
    stage = WeinsteinstageAnalyzer().determine_stage(
        current,
        ma_200,
        ma_200_series[::-1].reset_index(drop=True),
        close[::-1].reset_index(drop=True),
        volume[::-1].reset_index(drop=True),
    )
    vcp = VCPDetector().detect_vcp(
        close[::-1].reset_index(drop=True), volume[::-1].reset_index(drop=True)
    )
    year = frame.tail(252)
    high_52w = float(year["High"].max())
    low_52w = float(year["Low"].min())
    above_low = ((current - low_52w) / low_52w) * 100
    from_high = ((high_52w - current) / high_52w) * 100
    return {
        "ma_alignment": bool(ma["meets_all_criteria"]),
        "ma_score": float(ma["minervini_ma_score"]),
        "stage": int(stage["stage"]),
        "vcp_detected": bool(vcp["vcp_detected"]),
        "vcp_score": float(vcp["vcp_score"]),
        "meets_52w_low": above_low >= 30,
        "meets_52w_high": from_high <= 25,
    }


def _metric_deltas(left: dict[str, Any], right: dict[str, Any]) -> dict[str, float]:
    deltas = {}
    for key in sorted(left.keys() & right.keys()):
        if isinstance(left[key], (int, float)) and isinstance(right[key], (int, float)):
            deltas[key] = abs(float(left[key]) - float(right[key]))
    return deltas


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols", required=True)
    args = parser.parse_args()
    symbols = [value.strip().upper() for value in args.symbols.split(",") if value.strip()]
    adapter = SSQuantStackConsumerAdapter.from_environment()
    cache = PriceCacheService()
    legacy_read = PriceCacheService._quantstack_legacy_get_cached_only
    end = datetime.now(UTC).date()
    start = end - timedelta(days=730)
    calculator = TechnicalCalculatorService()
    rows = []
    for symbol in symbols:
        legacy = legacy_read(cache, symbol, period="2y")
        candidate = adapter.get_price_history(symbol, start=start, end=end).frame
        if legacy is None or legacy.empty or candidate.empty:
            raise RuntimeError(f"missing comparison frame for {symbol}")
        legacy_technical = calculator.calculate_all(legacy)
        candidate_technical = calculator.calculate_all(candidate)
        legacy_scanner = _scanner_signature(legacy)
        candidate_scanner = _scanner_signature(candidate)
        technical_deltas = _metric_deltas(legacy_technical, candidate_technical)
        categorical_keys = (
            "ma_alignment",
            "stage",
            "vcp_detected",
            "meets_52w_low",
            "meets_52w_high",
        )
        rows.append(
            {
                "symbol": symbol,
                "legacy_rows": len(legacy),
                "candidate_rows": len(candidate),
                "technical_metric_count": len(technical_deltas),
                "technical_metrics_changed": sum(value != 0 for value in technical_deltas.values()),
                "technical_max_absolute_delta": max(technical_deltas.values(), default=0.0),
                "scanner_categorical_changed": [
                    key
                    for key in categorical_keys
                    if legacy_scanner[key] != candidate_scanner[key]
                ],
                "ma_score_delta": abs(
                    float(legacy_scanner["ma_score"])
                    - float(candidate_scanner["ma_score"])
                ),
                "vcp_score_delta": abs(
                    float(legacy_scanner["vcp_score"])
                    - float(candidate_scanner["vcp_score"])
                ),
            }
        )
    summary = {
        "symbols": len(rows),
        "candidate_adjustment": "qfq",
        "scanner_categorical_change_symbols": sum(
            bool(row["scanner_categorical_changed"]) for row in rows
        ),
        "ma_score_change_symbols": sum(row["ma_score_delta"] != 0 for row in rows),
        "vcp_score_change_symbols": sum(row["vcp_score_delta"] != 0 for row in rows),
        "technical_metric_changes": sum(row["technical_metrics_changed"] for row in rows),
        "rows": rows,
    }
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
