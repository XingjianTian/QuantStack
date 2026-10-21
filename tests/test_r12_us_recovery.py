from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from quant_stack.data.r12_us_recovery import (
    build_frozen_queue,
    load_frozen_queue,
    manifest_paths_from_raw_analysis,
    reserve_budget,
    run_targeted_recovery,
    write_frozen_queue,
)

ROOT = Path(__file__).parents[1]
SNAPSHOT = (
    ROOT
    / "src/quant_stack/data/providers/yfinance/"
    "us_equity_xnas_xnys_2026_08_27_v1.json"
)
SESSION_COUNTS = {
    "2026-09-09": 122,
    "2026-09-10": 70,
    "2026-09-11": 36,
    "2026-09-14": 41,
    "2026-09-15": 52,
    "2026-09-16": 69,
    "2026-09-17": 45,
    "2026-09-18": 43,
    "2026-09-21": 37,
}


def _snapshot_ids() -> list[str]:
    return [
        str(row["instrument_id"])
        for row in json.loads(SNAPSHOT.read_text(encoding="utf-8"))["instruments"]
    ]


def _manifest(path: Path, session: str, omissions: list[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    returned = 5_079 - len(omissions)
    payload = {
        "schema_version": "quantstack.daily-session-parquet.v1",
        "dataset_id": "us_equity_daily",
        "version": f"{session}.session-v1",
        "market": "US",
        "session": session,
        "adjustment": "raw",
        "row_count": returned,
        "instrument_count": returned,
        "requested_instruments": 5_079,
        "omissions": omissions,
        "files": ["bars.parquet"],
        "validation": [
            "single_session",
            "unique_instrument_date",
            "ohlcv",
            "requested_equals_rows_plus_omissions",
        ],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _full_inputs(tmp_path: Path) -> tuple[Path, Path, list[Path]]:
    identities = _snapshot_ids()
    manifests: list[Path] = []
    analysis_sessions: list[dict[str, object]] = []
    historical: list[dict[str, str]] = []
    offset = 0
    for session, count in SESSION_COUNTS.items():
        # Pair identity is session/object, so an object may legitimately recur by session.
        omissions = identities[offset : offset + count]
        offset = (offset + 53) % 1_000
        manifest = _manifest(
            tmp_path / "raw" / f"market=US/session={session}/manifest.json",
            session,
            omissions,
        )
        manifests.append(manifest)
        digest = hashlib.sha256(manifest.read_bytes()).hexdigest()
        analysis_sessions.append(
            {
                "market": "US",
                "session": session,
                "package_path": str(manifest.parent),
                "input_integrity": {"manifest": {"observed_sha256": digest}},
            }
        )
        if session in {"2026-09-09", "2026-09-21"}:
            historical.extend(
                {
                    "market": "US",
                    "session": session,
                    "object_id": object_id,
                    "original_status": "reported_omission",
                }
                for object_id in omissions
            )
    raw_analysis = tmp_path / "raw-analysis.json"
    raw_analysis.write_text(json.dumps({"sessions": analysis_sessions}), encoding="utf-8")
    historical_path = tmp_path / "historical.csv"
    with historical_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=("market", "session", "object_id", "original_status"),
        )
        writer.writeheader()
        writer.writerows(historical)
    return raw_analysis, historical_path, manifests


def _small_queue(tmp_path: Path, count: int = 3) -> tuple[Path, Path, dict[str, object]]:
    raw_analysis, _, manifests = _full_inputs(tmp_path)
    queue = build_frozen_queue(
        snapshot_path=SNAPSHOT,
        manifest_paths=manifest_paths_from_raw_analysis(raw_analysis),
        now=lambda: datetime(2026, 9, 29, tzinfo=UTC),
    )
    queue_path = tmp_path / "queue.json"
    write_frozen_queue(queue_path, queue)
    selected = [row for row in queue["entries"] if row["session"] == "2026-09-21"][:count]
    queue["test_selected_object_ids"] = [row["object_id"] for row in selected]
    return manifests[-1], queue_path, queue


def _shared_runtime(tmp_path: Path) -> Path:
    runtime = tmp_path / "shared-runtime"
    route = runtime / "routes/explicit-proxy-v1"
    route.mkdir(parents=True)
    (route / "request.lock").write_text("", encoding="utf-8")
    (route / "last_request_at").write_text("0\n", encoding="utf-8")
    return runtime


def test_builds_frozen_515_queue_from_r11_analysis_and_maps_historical_159(
    tmp_path: Path,
) -> None:
    raw_analysis, historical, manifests = _full_inputs(tmp_path)

    resolved = manifest_paths_from_raw_analysis(raw_analysis)
    queue = build_frozen_queue(
        snapshot_path=SNAPSHOT,
        manifest_paths=resolved,
        historical_ledger=historical,
        now=lambda: datetime(2026, 9, 29, tzinfo=UTC),
    )

    assert resolved == manifests
    assert queue["status"] == "frozen"
    assert queue["pinned_snapshot"]["universe_count"] == 5_079
    assert queue["counts"]["entries"] == 515
    assert queue["counts"]["by_session"] == SESSION_COUNTS
    assert queue["counts"]["historical_159_subset"] == 159
    assert all(row["original_status"] == "reported_omission" for row in queue["entries"])
    assert not any(row["semantic_no_bar_verified"] for row in queue["entries"])


def test_explicit_ledger_rejects_duplicate_session_object_pairs(tmp_path: Path) -> None:
    object_id = _snapshot_ids()[0]
    ledger = tmp_path / "duplicate.json"
    ledger.write_text(
        json.dumps(
            [
                {"session": "2026-09-21", "object_id": object_id},
                {"session": "2026-09-21", "object_id": object_id},
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="duplicate session/object"):
        build_frozen_queue(
            snapshot_path=SNAPSHOT,
            omission_ledger=ledger,
        )


def test_dry_run_is_default_and_creates_no_candidate_or_budget(tmp_path: Path) -> None:
    _, queue_path, queue = _small_queue(tmp_path, count=2)
    candidate = tmp_path / "candidate"

    def forbidden_transport(**kwargs):  # pragma: no cover - failure is the assertion
        raise AssertionError("dry-run must not construct a provider transport")

    result = run_targeted_recovery(
        queue_path=queue_path,
        session=date(2026, 9, 21),
        candidate_root=candidate,
        budget_ledger=candidate / "control/budget.json",
        runtime_root=tmp_path / "missing-runtime",
        maximum_objects=2,
        run_id="dry-run",
        object_ids=queue["test_selected_object_ids"],
        proxy_url="http://127.0.0.1:7890",
        transport_factory=forbidden_transport,
    )

    assert result["mode"] == "dry_run"
    assert result["provider_calls"] == 0
    assert not candidate.exists()


def test_targeted_candidate_distinguishes_outcomes_and_never_touches_source(
    tmp_path: Path,
) -> None:
    manifest, queue_path, queue = _small_queue(tmp_path)
    before_hash = hashlib.sha256(manifest.read_bytes()).hexdigest()
    before_names = sorted(path.name for path in manifest.parent.iterdir())
    selected_ids = set(queue["test_selected_object_ids"])
    symbols = [
        str(row["provider_symbol"])
        for row in queue["entries"]
        if row["object_id"] in selected_ids and row["session"] == "2026-09-21"
    ]

    class MixedTransport:
        def __init__(self, **kwargs) -> None:
            self.requested: list[str] = []

        def fetch_daily_bars_batch(
            self, requested, start, end, *, include_actions, threads
        ):
            self.requested = list(requested)
            assert start == end == date(2026, 9, 21)
            assert include_actions is False
            assert threads is False
            return [
                {
                    "ticker": requested[0],
                    "Date": "2026-09-21T00:00:00",
                    "Open": 10.0,
                    "High": 11.0,
                    "Low": 9.0,
                    "Close": 10.5,
                    "Volume": 100.0,
                }
            ]

        def audit_snapshot(self):
            assert self.requested == symbols
            return {
                "request_outcomes": ["returned", "provider_error", "no_bar"],
                "completed_requests": 3,
            }

    runtime = _shared_runtime(tmp_path)
    candidate = tmp_path / "candidate"
    budget = candidate / "control/budget.json"
    result = run_targeted_recovery(
        queue_path=queue_path,
        session=date(2026, 9, 21),
        candidate_root=candidate,
        budget_ledger=budget,
        runtime_root=runtime,
        maximum_objects=3,
        run_id="run-1",
        execute=True,
        caller_holds_operation_lock=True,
        object_ids=queue["test_selected_object_ids"],
        threads=1,
        timeout_seconds=45,
        proxy_url="http://127.0.0.1:7890",
        transport_factory=MixedTransport,
        now=lambda: datetime(2026, 9, 29, tzinfo=UTC),
    )

    assert result["counts"] == {
        "selected": 3,
        "provider_error": 1,
        "provider_reported_no_bar": 1,
        "returned": 1,
    }
    assert [row["recovery_outcome"] for row in result["outcomes"]] == [
        "returned",
        "provider_error",
        "provider_reported_no_bar",
    ]
    assert not any(row["semantic_no_bar_accepted"] for row in result["outcomes"])
    assert (candidate / "run-1/candidate.json").is_file()
    assert hashlib.sha256(manifest.read_bytes()).hexdigest() == before_hash
    assert sorted(path.name for path in manifest.parent.iterdir()) == before_names

    persisted = json.loads(budget.read_text(encoding="utf-8"))
    assert persisted["used_objects"] == 3
    assert persisted["remaining_objects"] == 0
    assert {row["status"] for row in persisted["attempts"].values()} == {
        "returned",
        "provider_error",
        "provider_reported_no_bar",
    }

    with pytest.raises(ValueError, match="zero-retry policy"):
        run_targeted_recovery(
            queue_path=queue_path,
            session=date(2026, 9, 21),
            candidate_root=candidate,
            budget_ledger=budget,
            runtime_root=runtime,
            maximum_objects=3,
            run_id="run-after-restart",
            execute=True,
            caller_holds_operation_lock=True,
            object_ids=queue["test_selected_object_ids"],
            proxy_url="http://127.0.0.1:7890",
            transport_factory=MixedTransport,
        )


def test_reserved_budget_survives_reload_and_cannot_be_retried(tmp_path: Path) -> None:
    _, _, queue = _small_queue(tmp_path, count=2)
    ledger = tmp_path / "candidate/control/budget.json"
    first = [queue["entries"][0]]

    reserve_budget(
        ledger_path=ledger,
        queue_id=str(queue["queue_id"]),
        entries=first,
        maximum_objects=2,
        now=lambda: datetime(2026, 9, 29, tzinfo=UTC),
    )
    reloaded = json.loads(ledger.read_text(encoding="utf-8"))
    assert reloaded["used_objects"] == 1
    assert next(iter(reloaded["attempts"].values()))["status"] == "reserved"

    with pytest.raises(ValueError, match="zero-retry policy"):
        reserve_budget(
            ledger_path=ledger,
            queue_id=str(queue["queue_id"]),
            entries=first,
            maximum_objects=2,
        )


def test_execute_requires_existing_shared_runtime_and_caller_lock_assertion(
    tmp_path: Path,
) -> None:
    _, queue_path, queue = _small_queue(tmp_path, count=1)
    candidate = tmp_path / "candidate"

    with pytest.raises(ValueError, match="caller_holds_operation_lock"):
        run_targeted_recovery(
            queue_path=queue_path,
            session=date(2026, 9, 21),
            candidate_root=candidate,
            budget_ledger=candidate / "control/budget.json",
            runtime_root=tmp_path / "runtime",
            maximum_objects=1,
            run_id="run-1",
            execute=True,
            object_ids=queue["test_selected_object_ids"],
            proxy_url="http://127.0.0.1:7890",
        )


def test_load_rejects_changed_515_session_distribution(tmp_path: Path) -> None:
    _, queue_path, _ = _small_queue(tmp_path)
    payload = json.loads(queue_path.read_text(encoding="utf-8"))
    payload["entries"][0]["session"] = "2026-09-21"
    payload["counts"]["by_session"]["2026-09-09"] -= 1
    payload["counts"]["by_session"]["2026-09-21"] += 1
    queue_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="fixed 515-object session matrix"):
        load_frozen_queue(queue_path)


@pytest.mark.parametrize("run_id", ["../escape", "/tmp/escape", "nested/run", ".."])
def test_run_id_must_be_one_safe_component(tmp_path: Path, run_id: str) -> None:
    _, queue_path, queue = _small_queue(tmp_path, count=1)
    with pytest.raises(ValueError, match="one safe path component"):
        run_targeted_recovery(
            queue_path=queue_path,
            session=date(2026, 9, 21),
            candidate_root=tmp_path / "candidate",
            budget_ledger=tmp_path / "candidate/control/budget.json",
            runtime_root=tmp_path / "runtime",
            maximum_objects=1,
            run_id=run_id,
            object_ids=queue["test_selected_object_ids"],
            proxy_url="http://127.0.0.1:7890",
        )


def test_candidate_cannot_contain_an_authority_source_package(tmp_path: Path) -> None:
    manifest, queue_path, queue = _small_queue(tmp_path, count=1)
    with pytest.raises(ValueError, match="outside source package"):
        run_targeted_recovery(
            queue_path=queue_path,
            session=date(2026, 9, 21),
            candidate_root=manifest.parents[2],
            budget_ledger=manifest.parents[2] / "control/budget.json",
            runtime_root=tmp_path / "runtime",
            maximum_objects=1,
            run_id="run-1",
            object_ids=queue["test_selected_object_ids"],
            proxy_url="http://127.0.0.1:7890",
        )


def test_targeted_recovery_requires_proxy_and_one_thread(tmp_path: Path) -> None:
    _, queue_path, queue = _small_queue(tmp_path, count=1)
    common = {
        "queue_path": queue_path,
        "session": date(2026, 9, 21),
        "candidate_root": tmp_path / "candidate",
        "budget_ledger": tmp_path / "candidate/control/budget.json",
        "runtime_root": tmp_path / "runtime",
        "maximum_objects": 1,
        "run_id": "run-1",
        "object_ids": queue["test_selected_object_ids"],
    }
    with pytest.raises(ValueError, match="threads=1"):
        run_targeted_recovery(**common, threads=8, proxy_url="http://127.0.0.1:7890")
    with pytest.raises(ValueError, match="explicit proxy route"):
        run_targeted_recovery(**common)
