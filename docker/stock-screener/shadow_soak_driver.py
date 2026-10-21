"""Serial, fail-closed HTTP controller for the isolated T-078 SS raw shadow."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

STAGE_LIMITS = {
    "S1": {
        "symbols": 8,
        "default_calls": 80,
        "maximum_calls": 80,
        "duration_seconds": 15 * 60,
    },
    "S2": {
        "symbols": 50,
        "default_calls": 500,
        "maximum_calls": 500,
        "duration_seconds": 30 * 60,
    },
    "S3": {
        "symbols": 50,
        "default_calls": 244,
        "maximum_calls": 800,
        "duration_seconds": 60 * 60,
    },
}
MIN_CALL_INTERVAL_SECONDS = 0.050
BATCH_SIZE = 500
BATCH_PAUSE_SECONDS = 5.0
GLOBAL_CALL_LIMIT = sum(int(value["maximum_calls"]) for value in STAGE_LIMITS.values())
FAMILY_LIMITS = {
    "single_history": 200,
    "batch_history": 24,
    "technical_indicators": 250,
    "scanner": 50,
    "price_cache_methods": 300,
}
FUTURE_PRICE_SEED_RESERVE = {
    "S1": 100,  # S2/S3 each verify 8 fixtures and preserve 42 missing-legacy symbols.
    "S2": 50,
    "S3": 0,
}
FAILURE_FIELDS = (
    "dropped",
    "candidate_failures",
    "comparison_failures",
    "report_failures",
    "worker_failures",
)


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _append_jsonl(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(value, sort_keys=True) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    path.chmod(0o600)


def _load_global_state(path: Path) -> tuple[int, dict[str, int], float]:
    """Rebuild the T-078-wide call and family budgets from durable evidence."""

    count = 0
    family_counts = {family: 0 for family in FAMILY_LIMITS}
    last_started_epoch = 0.0
    if not path.exists():
        return count, family_counts, last_started_epoch
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            if not line.strip():
                continue
            record = json.loads(line)
            count += 1
            if int(record.get("global_call", -1)) != count:
                raise RuntimeError("persistent global ledger is not contiguous")
            family = str(record.get("family", ""))
            if family not in family_counts:
                raise RuntimeError("persistent global ledger has an unknown family")
            family_counts[family] += 1
            if family_counts[family] > FAMILY_LIMITS[family]:
                raise RuntimeError("persistent traffic-family budget is invalid")
            last_started_epoch = float(record.get("started_epoch", 0.0))
    return count, family_counts, last_started_epoch


def _stage_family_ceilings(stage: str) -> dict[str, int]:
    ceilings = dict(FAMILY_LIMITS)
    ceilings["price_cache_methods"] -= FUTURE_PRICE_SEED_RESERVE[stage]
    return ceilings


def _load_corpus(path: Path, maximum: int) -> list[str]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    symbols = payload.get("symbols")
    if not isinstance(symbols, list):
        raise ValueError("corpus must contain a symbols list")
    normalized = [str(symbol).strip().upper() for symbol in symbols]
    if len(normalized) < maximum or len(set(normalized)) != len(normalized):
        raise ValueError("corpus is too small or contains duplicate symbols")
    if any(not symbol.endswith((".SS", ".SZ")) for symbol in normalized):
        raise ValueError("T-078 corpus only supports explicit SSE/SZSE symbols")
    return normalized[:maximum]


def _assert_environment(stage: str, symbols: list[str]) -> None:
    required = {
        "SS_QUANTSTACK_READ_MODE": "shadow",
        "SS_QUANTSTACK_SHADOW_ENABLED": "true",
        "SS_QUANTSTACK_SHADOW_SAMPLE_RATE": "1",
        "SS_QUANTSTACK_ALLOW_CANDIDATE": "false",
        "SS_RUNTIME_ENV": "isolated-shadow",
    }
    mismatches = {
        key: os.environ.get(key)
        for key, expected in required.items()
        if os.environ.get(key, "").lower() != expected
    }
    if mismatches:
        raise RuntimeError(f"unsafe T-078 environment for {stage}: {sorted(mismatches)}")
    allowlist = {
        value.strip().upper()
        for value in os.environ.get("SS_QUANTSTACK_SHADOW_ALLOWLIST", "").split(",")
        if value.strip()
    }
    if not set(symbols).issubset(allowlist):
        raise RuntimeError("stage corpus is not covered by the frozen shadow allowlist")


class T078HTTPClient:
    def __init__(self) -> None:
        self.base_url = os.environ.get("T078_BACKEND_BASE_URL", "http://127.0.0.1:8000")
        self.control_token = os.environ.get("T078_CONTROL_TOKEN", "")
        if not self.control_token:
            raise RuntimeError("T078_CONTROL_TOKEN is required")
        self._opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def request(
        self,
        method: str,
        path: str,
        payload: dict[str, object] | None = None,
        *,
        control: bool = False,
    ) -> dict[str, Any]:
        body = None if payload is None else json.dumps(payload).encode()
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        if control:
            headers["X-T078-Control-Token"] = self.control_token
        request = urllib.request.Request(
            f"{self.base_url}{path}", data=body, method=method, headers=headers
        )
        try:
            with self._opener.open(request, timeout=45) as response:
                raw = response.read()
        except urllib.error.HTTPError as exc:
            exc.read()
            raise RuntimeError(f"isolated backend returned HTTP {exc.code}") from exc
        return json.loads(raw)

    def health(self, *, wait_for_idle: bool = False) -> dict[str, Any]:
        suffix = "?wait_for_idle=true" if wait_for_idle else ""
        return self.request("GET", f"/__t078/control/health{suffix}", control=True)


class StageGuard:
    def __init__(
        self,
        *,
        client: T078HTTPClient,
        stage_root: Path,
        evidence_root: Path,
        maximum_calls: int,
        deadline: float,
    ) -> None:
        self.client = client
        self.stage_root = stage_root
        self.stage_ledger = stage_root / "request-ledger.jsonl"
        self.global_ledger = evidence_root / "logical-calls.jsonl"
        self.health_ledger = stage_root / "worker-health-samples.jsonl"
        self.maximum_calls = maximum_calls
        self.deadline = deadline
        self.count = 0
        (
            self.global_count,
            self.global_family_counts,
            self._last_started_epoch,
        ) = _load_global_state(self.global_ledger)
        if self.global_count > GLOBAL_CALL_LIMIT:
            raise RuntimeError("persistent global logical-call budget is invalid")
        self.family_counts = {family: 0 for family in FAMILY_LIMITS}
        self._queue_high_since: float | None = None
        baseline = self.client.health()
        self.baseline_worker = baseline["worker"]
        self.baseline_reports = set(baseline["report_files"])
        self.accepted_tasks = 0
        self.expected_reports = 0

    def remaining_capacity(self) -> int:
        return sum(
            FAMILY_LIMITS[family] - self.global_family_counts[family] for family in FAMILY_LIMITS
        )

    def _assert_deadline(self, point: str) -> None:
        if time.monotonic() >= self.deadline:
            raise RuntimeError(f"stage duration bound exceeded {point}")

    def _paced_sleep(self, seconds: float, reason: str) -> None:
        if seconds <= 0:
            return
        remaining = self.deadline - time.monotonic()
        if remaining <= seconds:
            raise RuntimeError(f"stage duration bound would expire during {reason}")
        time.sleep(seconds)
        self._assert_deadline(f"after {reason}")

    def _sample(self, point: str, snapshot: dict[str, Any]) -> None:
        worker = snapshot["worker"]
        record = {"at": _now(), "point": point, "worker": worker}
        _append_jsonl(self.health_ledger, record)
        if not worker["thread_alive"]:
            raise RuntimeError("shadow worker thread is not alive")
        changed_failures = [
            (field, int(self.baseline_worker[field]), int(worker[field]))
            for field in FAILURE_FIELDS
            if worker[field] != self.baseline_worker[field]
        ]
        if changed_failures:
            details = ", ".join(
                f"{field}={baseline}->{current}"
                for field, baseline, current in changed_failures
            )
            if any(field == "candidate_failures" for field, _, _ in changed_failures):
                details += (
                    f", candidate_stage={worker.get('last_candidate_failure_stage')}"
                    f", candidate_class={worker.get('last_candidate_failure_class')}"
                )
            raise RuntimeError(f"shadow worker hard SLO failed: {details}")
        now = time.monotonic()
        if worker["queue_size"] > 16:
            self._queue_high_since = self._queue_high_since or now
            if now - self._queue_high_since >= 10:
                raise RuntimeError("shadow queue exceeded 16 for 10 seconds")
        else:
            self._queue_high_since = None

    def _before_call(self) -> dict[str, Any]:
        if self.count >= self.maximum_calls:
            raise RuntimeError("stage logical-call budget exhausted")
        if self.global_count >= GLOBAL_CALL_LIMIT:
            raise RuntimeError("global logical-call budget exhausted")
        self._assert_deadline("before logical call")
        if self.global_count and self.global_count % BATCH_SIZE == 0:
            self._paced_sleep(BATCH_PAUSE_SECONDS, "batch pause")
        wait_seconds = MIN_CALL_INTERVAL_SECONDS - (time.time() - self._last_started_epoch)
        self._paced_sleep(wait_seconds, "logical-call spacing")
        self._assert_deadline("after pacing")
        snapshot = self.client.health()
        self._sample("before", snapshot)
        if snapshot["worker"]["queue_size"]:
            raise RuntimeError("shadow queue was not empty before logical call")
        return snapshot

    def _drain_one(self, completed_before: int) -> dict[str, Any]:
        drain_deadline = min(self.deadline, time.monotonic() + 30)
        while True:
            snapshot = self.client.health()
            self._sample("drain-poll", snapshot)
            worker = snapshot["worker"]
            if worker["completed"] - completed_before == 1 and worker["queue_size"] == 0:
                return snapshot
            if worker["completed"] - completed_before > 1:
                raise RuntimeError("more than one shadow task completed for a logical call")
            if time.monotonic() >= drain_deadline:
                raise RuntimeError("shadow worker did not drain within 30 seconds")
            time.sleep(0.1)

    def call(
        self,
        *,
        family: str,
        read_path: str,
        symbols: list[str],
        period: str,
        operation: Callable[[], dict[str, Any]],
    ) -> dict[str, Any]:
        if family not in FAMILY_LIMITS:
            raise RuntimeError("unknown T-078 traffic family")
        if self.global_family_counts[family] >= FAMILY_LIMITS[family]:
            raise RuntimeError(f"{family} T-078-wide call budget exhausted")
        before = self._before_call()
        self._assert_deadline("immediately before operation")
        started = time.monotonic()
        started_epoch = time.time()
        self._last_started_epoch = started_epoch
        status = "error"
        response_payload: dict[str, Any] = {}
        error_class = None
        after: dict[str, Any] | None = None
        drained: dict[str, Any] | None = None
        try:
            response_payload = operation()
            after = self.client.health()
            self._sample("after-response", after)
            submitted_delta = after["worker"]["submitted"] - before["worker"]["submitted"]
            if submitted_delta != 1:
                raise RuntimeError("logical call was not accepted exactly once")
            self.accepted_tasks += 1
            expected = len(symbols)
            self.expected_reports += expected
            drained = self._drain_one(before["worker"]["completed"])
            if drained["worker"]["completed"] - before["worker"]["completed"] != 1:
                raise RuntimeError("accepted/completed task accounting mismatch")
            report_delta = len(set(drained["report_files"]) - set(before["report_files"]))
            if report_delta != expected:
                raise RuntimeError("shadow report accounting mismatch")
            if time.monotonic() >= self.deadline:
                raise RuntimeError("stage duration bound exceeded after logical call")
            status = "ok"
            return response_payload
        except Exception as exc:
            error_class = type(exc).__name__
            raise
        finally:
            self.count += 1
            self.global_count += 1
            self.family_counts[family] += 1
            self.global_family_counts[family] += 1
            record = {
                "stage_call": self.count,
                "global_call": self.global_count,
                "family": family,
                "read_path": read_path,
                "symbols": symbols,
                "symbol_count": len(symbols),
                "period": period,
                "status": status,
                "error_class": error_class,
                "accepted": bool(
                    after and after["worker"]["submitted"] - before["worker"]["submitted"] == 1
                ),
                "expected_report_count": len(symbols),
                "started_at": _now(),
                "started_epoch": started_epoch,
                "latency_ms": round((time.monotonic() - started) * 1000, 3),
            }
            _append_jsonl(self.stage_ledger, record)
            _append_jsonl(self.global_ledger, record)


def _control_operation(
    client: T078HTTPClient, method: str, symbols: list[str], period: str
) -> Callable[[], dict[str, Any]]:
    return lambda: client.request(
        "POST",
        "/__t078/control/read",
        {"method": method, "symbols": symbols, "period": period},
        control=True,
    )


def _scanner_operation(
    client: T078HTTPClient, symbol: str
) -> Callable[[], dict[str, Any]]:
    return lambda: client.request(
        "POST",
        "/__t078/control/scan",
        {
            "universe": "custom",
            "symbols": [symbol],
            "screeners": ["minervini"],
            "composite_method": "weighted_average",
        },
        control=True,
    )


def _seed_or_verify(
    *,
    stage: str,
    symbols: list[str],
    predecessor_symbols: set[str],
    guard: StageGuard,
) -> list[str]:
    seeded: list[str] = []
    preserve_missing_legacy = (
        os.environ.get("T111_LEGACY_FIXTURE_MISSING_AS_EMPTY", "").lower() == "true"
    )
    fixture_symbol_count = int(STAGE_LIMITS["S1"]["symbols"])
    for index, symbol in enumerate(symbols):
        missing_legacy_symbol = (
            preserve_missing_legacy and stage != "S1" and index >= fixture_symbol_count
        )
        if missing_legacy_symbol:
            missing = guard.call(
                family="price_cache_methods",
                read_path="get_cached_only_missing_legacy",
                symbols=[symbol],
                period="2y",
                operation=_control_operation(guard.client, "get_cached_only", [symbol], "2y"),
            )
            if int(missing.get("row_count", 0)) != 0:
                raise RuntimeError("missing-legacy symbol unexpectedly had cached rows")
        elif stage == "S1" or symbol not in predecessor_symbols:
            missing = guard.call(
                family="price_cache_methods",
                read_path="get_cached_only_controlled_miss",
                symbols=[symbol],
                period="2y",
                operation=_control_operation(guard.client, "get_cached_only", [symbol], "2y"),
            )
            if int(missing.get("row_count", 0)) != 0:
                raise RuntimeError("controlled-miss symbol was already seeded")
            guard.call(
                family="price_cache_methods",
                read_path="seed",
                symbols=[symbol],
                period="2y",
                operation=_control_operation(guard.client, "seed", [symbol], "2y"),
            )
        else:
            result = guard.call(
                family="price_cache_methods",
                read_path="get_cached_only",
                symbols=[symbol],
                period="2y",
                operation=_control_operation(guard.client, "get_cached_only", [symbol], "2y"),
            )
            if int(result.get("row_count", 0)) == 0:
                raise RuntimeError("predecessor seed is missing")
        seeded.append(symbol)
    return seeded


def _traffic_operations(
    client: T078HTTPClient, symbols: list[str]
) -> list[tuple[str, str, list[str], str, Callable[[], dict[str, Any]]]]:
    operations = []
    batch_operations = []
    periods = ("6mo", "1y", "2y", "5y")
    cached_analyses = ("stage", "ma-analysis", "52-week-position")
    methods = (
        "get_historical_data",
        "get_cached_only",
        "get_cached_only_fresh",
        "get_many",
        "get_many_cached_only",
        "get_many_cached_only_fresh",
    )
    legacy_symbols = symbols[: int(STAGE_LIMITS["S1"]["symbols"])]
    for index, symbol in enumerate(legacy_symbols):
        period = periods[index % len(periods)]
        single_path = f"/api/v1/stocks/{urllib.parse.quote(symbol)}/history?period={period}"
        operations.append(
            (
                "single_history",
                "get_historical_data",
                [symbol],
                period,
                lambda path=single_path: client.request("GET", path),
            )
        )
        # VCP is the one approved 6mo direct-replay probe. Keep it to one per
        # operation cycle so the independent 100-connection proxy budget
        # remains well below its ceiling after the 50 controlled seeds.
        analysis = "vcp" if index == 0 else cached_analyses[(index - 1) % 3]
        technical_path = f"/api/v1/technical/{urllib.parse.quote(symbol)}/{analysis}"
        operations.append(
            (
                "technical_indicators",
                "yfinance_direct",
                [symbol],
                "6mo" if analysis == "vcp" else "2y",
                lambda path=technical_path: client.request("GET", path),
            )
        )
        operations.append(
            (
                "scanner",
                "get_historical_data",
                [symbol],
                "2y",
                _scanner_operation(client, symbol),
            )
        )
    for index, symbol in enumerate(symbols):
        period = periods[index % len(periods)]
        for method in methods:
            selected = symbols[index : index + min(10, len(symbols) - index)]
            if not method.startswith("get_many"):
                selected = [symbol]
            batch_operations.append(
                (
                    "price_cache_methods",
                    method,
                    selected,
                    period,
                    _control_operation(client, method, selected, period),
                )
            )
    for size in (1, 10, 50):
        selected = symbols[: min(size, len(symbols))]
        for period in periods:
            operations.append(
                (
                    "batch_history",
                    "get_many_cached_only",
                    selected,
                    period,
                    lambda selected=selected, period=period: client.request(
                        "POST",
                        "/api/v1/stocks/history/batch",
                        {"symbols": selected, "period": period},
                    ),
                )
            )
    grouped = {family: [] for family in FAMILY_LIMITS}
    for operation in operations + batch_operations:
        grouped[operation[0]].append(operation)
    ordered = []
    while any(grouped.values()):
        for family in FAMILY_LIMITS:
            if grouped[family]:
                ordered.append(grouped[family].pop(0))
    return ordered


def _predecessor(stage: str, evidence_root: Path, symbols: list[str]) -> set[str]:
    predecessor = {"S1": None, "S2": "S1", "S3": "S2"}[stage]
    if predecessor is None:
        return set()
    path = evidence_root / predecessor.lower() / "stage-summary.json"
    if not path.is_file():
        raise RuntimeError(f"{stage} requires a passed {predecessor} summary")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("status") != "passed":
        raise RuntimeError("predecessor stage did not pass")
    seeded_list = payload.get("seeded_symbols", [])
    if not isinstance(seeded_list, list) or len(set(seeded_list)) != len(seeded_list):
        raise RuntimeError("predecessor seed manifest is invalid")
    expected = symbols[: int(STAGE_LIMITS[predecessor]["symbols"])]
    if seeded_list != expected:
        raise RuntimeError(
            f"{stage} requires the complete ordered {len(expected)}-symbol "
            f"{predecessor} seed manifest"
        )
    return set(seeded_list)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", choices=sorted(STAGE_LIMITS), required=True)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--evidence-root", type=Path, default=Path("/evidence/driver"))
    parser.add_argument("--calls", type=int)
    args = parser.parse_args()

    limits = STAGE_LIMITS[args.stage]
    requested_calls = int(limits["default_calls"]) if args.calls is None else args.calls
    if requested_calls < 1 or requested_calls > int(limits["maximum_calls"]):
        raise ValueError("requested calls exceed the stage contract")
    symbols = _load_corpus(args.corpus, int(limits["symbols"]))
    _assert_environment(args.stage, symbols)
    evidence_root = args.evidence_root.resolve()
    evidence_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    predecessor_symbols = _predecessor(args.stage, evidence_root, symbols)
    stage_root = evidence_root / args.stage.lower()
    if stage_root.exists():
        raise RuntimeError("stage evidence root already exists")
    stage_root.mkdir(mode=0o700)
    lock_path = evidence_root / "run.lock"
    lock_descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        try:
            fcntl.flock(lock_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("another T-078 driver is active") from exc
        started = time.monotonic()
        client = T078HTTPClient()
        guard = StageGuard(
            client=client,
            stage_root=stage_root,
            evidence_root=evidence_root,
            maximum_calls=requested_calls,
            deadline=started + int(limits["duration_seconds"]),
        )
        if requested_calls > guard.remaining_capacity():
            raise RuntimeError("requested stage calls exceed remaining T-078-wide family budgets")
        if (
            os.environ.get("T111_LEGACY_FIXTURE_MISSING_AS_EMPTY", "").lower() == "true"
            and args.stage != "S1"
        ):
            seed_call_count = len(symbols)
        else:
            seed_call_count = sum(1 if symbol in predecessor_symbols else 2 for symbol in symbols)
        remaining_price_capacity = (
            FAMILY_LIMITS["price_cache_methods"] - guard.global_family_counts["price_cache_methods"]
        )
        if seed_call_count > remaining_price_capacity or seed_call_count > requested_calls:
            raise RuntimeError("stage seed plan exceeds remaining bounded capacity")
        stage_family_ceilings = _stage_family_ceilings(args.stage)
        available_this_stage = sum(
            max(
                0,
                stage_family_ceilings[family] - guard.global_family_counts[family],
            )
            for family in FAMILY_LIMITS
        )
        if requested_calls > available_this_stage:
            raise RuntimeError("requested stage calls would consume a future seed reserve")
        seeded = _seed_or_verify(
            stage=args.stage,
            symbols=symbols,
            predecessor_symbols=predecessor_symbols,
            guard=guard,
        )
        operations = _traffic_operations(client, symbols)
        index = 0
        while guard.count < requested_calls:
            family, read_path, selected, period, operation = operations[index % len(operations)]
            if guard.global_family_counts[family] >= stage_family_ceilings[family]:
                index += 1
                if all(
                    guard.global_family_counts[name] >= maximum
                    for name, maximum in stage_family_ceilings.items()
                ):
                    raise RuntimeError("traffic-family budgets cannot satisfy requested calls")
                continue
            guard.call(
                family=family,
                read_path=read_path,
                symbols=selected,
                period=period,
                operation=operation,
            )
            index += 1
        final = client.health()
        guard._sample("stage-final", final)
        if args.stage in {"S1", "S2"} and any(count == 0 for count in guard.family_counts.values()):
            raise RuntimeError("stage did not cover all five traffic families")
        worker_delta = {
            key: final["worker"][key] - guard.baseline_worker[key]
            for key in final["worker"]
            if isinstance(final["worker"][key], int) and not isinstance(final["worker"][key], bool)
        }
        report_files = sorted(set(final["report_files"]) - guard.baseline_reports)
        if not (worker_delta["submitted"] == worker_delta["completed"] == guard.accepted_tasks):
            raise RuntimeError("final task accounting mismatch")
        if len(report_files) != guard.expected_reports:
            raise RuntimeError("final report accounting mismatch")
        elapsed = time.monotonic() - started
        summary = {
            "stage": args.stage,
            "status": "passed",
            "symbols": symbols,
            "seeded_symbols": seeded,
            "logical_calls": guard.count,
            "family_counts": guard.family_counts,
            "global_family_counts": guard.global_family_counts,
            "accepted_tasks": guard.accepted_tasks,
            "expected_report_count": guard.expected_reports,
            "report_count": len(report_files),
            "report_files": report_files,
            "minimum_call_interval_ms": int(MIN_CALL_INTERVAL_SECONDS * 1000),
            "batch_size": BATCH_SIZE,
            "batch_pause_seconds": BATCH_PAUSE_SECONDS,
            "elapsed_seconds": round(elapsed, 3),
            "worker_deltas": worker_delta,
            "completed_at": _now(),
        }
        summary_path = stage_root / "stage-summary.json"
        summary_path.write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        summary_path.chmod(0o600)
        print(json.dumps(summary, sort_keys=True))
        return 0
    finally:
        os.close(lock_descriptor)


if __name__ == "__main__":
    raise SystemExit(main())
