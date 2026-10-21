from __future__ import annotations

import argparse
import importlib.util
import json
import os
import stat
import sys
import tempfile
import threading
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from app.services.price_cache_service import PriceCacheService
from pandas.testing import assert_frame_equal


def _load_adapter(path: Path):
    spec = importlib.util.spec_from_file_location("quantstack_live_ss_probe", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("SS adapter module could not be loaded")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--adapter", required=True, type=Path)
    parser.add_argument("--symbol", default="600000.SS")
    args = parser.parse_args()
    module = _load_adapter(args.adapter)

    legacy_before = PriceCacheService().get_many_cached_only([args.symbol], period="2y")
    before_frame = legacy_before[args.symbol]
    if before_frame is None or before_frame.empty:
        raise RuntimeError("controlled SS shadow symbol has no legacy cache rows")

    adapter = module.SSQuantStackConsumerAdapter.from_environment()

    class GatedAdapter:
        def __init__(self, delegate):
            self.delegate = delegate
            self.entered = [threading.Event(), threading.Event()]
            self.release = [threading.Event(), threading.Event()]
            self.calls = 0

        def get_batch_price_history(self, *args, **kwargs):
            index = self.calls
            self.calls += 1
            self.entered[index].set()
            if not self.release[index].wait(timeout=30):
                raise TimeoutError("controlled shadow release gate timed out")
            return self.delegate.get_batch_price_history(*args, **kwargs)

    gated_adapter = GatedAdapter(adapter)
    with tempfile.TemporaryDirectory(prefix="phase23a-live-ss-") as temporary:
        report_root = Path(temporary) / "shadow-reports"
        config = module.SSReadModeConfig(
            mode="shadow",
            shadow_enabled=True,
            sample_rate=1.0,
            allowlist=frozenset({args.symbol}),
            max_window_days=30,
            runtime_environment="production",
            report_root=str(report_root),
        )
        module.install_price_cache_read_modes(
            PriceCacheService,
            adapter_factory=lambda: gated_adapter,
            config=config,
        )
        worker = PriceCacheService._quantstack_shadow_worker
        legacy_returned_timestamps: list[str] = []
        candidate_completed_timestamps: list[str] = []
        response_frame = None
        worker_alive_after_first = False
        seen_report_paths: set[Path] = set()
        for index in range(2):
            response = PriceCacheService().get_many_cached_only([args.symbol], period="2y")
            legacy_returned = datetime.now(UTC)
            legacy_returned_timestamps.append(legacy_returned.isoformat())
            response_frame = response[args.symbol]
            assert response_frame is not None
            assert_frame_equal(before_frame, response_frame, check_exact=True)
            if not gated_adapter.entered[index].wait(timeout=5):
                raise RuntimeError("shadow candidate did not enter the controlled gate")
            gated_adapter.release[index].set()
            if not worker.wait_for_idle(timeout_seconds=30):
                raise RuntimeError("shadow worker did not complete within the validation bound")
            current_report_paths = set(report_root.rglob("*.json"))
            new_report_paths = current_report_paths - seen_report_paths
            if len(new_report_paths) != 1:
                raise RuntimeError(
                    "controlled shadow task did not generate exactly one new report"
                )
            new_report_path = new_report_paths.pop()
            new_report = json.loads(new_report_path.read_text(encoding="utf-8"))
            candidate_completed_timestamps.append(new_report["completed_at"])
            seen_report_paths = current_report_paths
            if index == 0:
                worker_alive_after_first = worker.health_snapshot().thread_alive

        report_paths = list(report_root.rglob("*.json"))
        reports = [json.loads(path.read_text(encoding="utf-8")) for path in report_paths]
        if len(reports) != 2:
            raise RuntimeError("controlled shadow probe did not generate exactly two reports")
        report_mode = stat.S_IMODE(report_paths[0].stat().st_mode)
        report_directory_mode = stat.S_IMODE(report_paths[0].parent.stat().st_mode)
        health_before_shutdown = asdict(worker.health_snapshot())
        if not worker.shutdown():
            raise RuntimeError("shadow worker did not stop within the shutdown bound")

    assert response_frame is not None
    legacy_returned_before_candidate_completed = all(
        datetime.fromisoformat(legacy) < datetime.fromisoformat(candidate.replace("Z", "+00:00"))
        for legacy, candidate in zip(
            legacy_returned_timestamps,
            candidate_completed_timestamps,
            strict=True,
        )
    )

    result = {
        "sample_symbol": args.symbol,
        "legacy_rows_before": len(before_frame),
        "legacy_rows_response": len(response_frame),
        "legacy_response_equal": True,
        "shadow_report_count": len(reports),
        "shadow_statuses": sorted({item["status"] for item in reports}),
        "shadow_comparison_metrics": [
            {
                "legacy_rows": item["legacy_rows"],
                "candidate_rows": item["candidate_rows"],
                "missing_legacy_dates": len(item["missing_legacy_dates"]),
                "missing_candidate_dates": len(item["missing_candidate_dates"]),
                "ohlc_mismatches": item["ohlc_mismatches"],
                "volume_compatibility": item["volume_compatibility"],
                "amount_coverage": item["amount_coverage"],
                "amount_mismatches": item["amount_mismatches"],
                "adjustment_semantics": item["adjustment_semantics"],
                "latency_ms": item["latency_ms"],
                "ohlc_shape_compatible": item["ohlc_shape_compatible"],
                "consumer_contract_compatible": item[
                    "consumer_contract_compatible"
                ],
                "cutover_compatible": item["cutover_compatible"],
            }
            for item in reports
        ],
        "candidate_error_count": sum(
            item["status"] == "candidate_error" for item in reports
        ),
        "candidate_rows": sum(item["candidate_rows"] for item in reports),
        "legacy_returned_timestamps": legacy_returned_timestamps,
        "candidate_completed_timestamps": candidate_completed_timestamps,
        "legacy_returned_before_candidate_completed": (
            legacy_returned_before_candidate_completed
        ),
        "worker_alive_after_first": worker_alive_after_first,
        "second_report_generated": True,
        "worker_health_before_shutdown": health_before_shutdown,
        "report_mode": oct(report_mode),
        "report_directory_mode": oct(report_directory_mode),
        "process_only_overlay": True,
        "production_default_unchanged": os.environ.get(
            "SS_QUANTSTACK_READ_MODE", "legacy"
        )
        == "legacy",
    }
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
