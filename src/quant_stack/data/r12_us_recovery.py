from __future__ import annotations

import csv
import fcntl
import hashlib
import json
import math
import os
import re
import signal
import tempfile
from collections.abc import Callable, Iterable, Mapping, Sequence
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from quant_stack.data.providers.yfinance import (
    LiveYFinanceTransport,
    load_us_instrument_snapshot,
)
from quant_stack.data.providers.yfinance.pacing import yfinance_runtime_root

QUEUE_SCHEMA = "quantstack.r12-us-recovery-queue.v1"
CANDIDATE_SCHEMA = "quantstack.r12-us-targeted-recovery-candidate.v1"
BUDGET_SCHEMA = "quantstack.r12-us-recovery-budget.v1"
PINNED_SNAPSHOT_ID = "us-equity-xnas-xnys-2026-08-27.v1"
PINNED_UNIVERSE_COUNT = 5_079
MAX_BATCH_SIZE = 128
MAX_THREADS = 1
MAX_TIMEOUT_SECONDS = 45
RECOVERY_SESSION_COUNTS = {
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
RECOVERY_OBJECT_COUNT = sum(RECOVERY_SESSION_COUNTS.values())
_SAFE_RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _timestamp(now: Callable[[], datetime]) -> str:
    value = now()
    if value.tzinfo is None:
        raise ValueError("clock must return a timezone-aware datetime")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_json(payload: object) -> bytes:
    return json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _atomic_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, prefix=f".{path.name}.", delete=False
    ) as handle:
        handle.write(body)
        handle.flush()
        os.fsync(handle.fileno())
        temporary = Path(handle.name)
    os.replace(temporary, path)


def _write_new_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    with path.open("x", encoding="utf-8") as handle:
        handle.write(body)


def _load_rows(path: Path) -> list[dict[str, Any]]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        with path.open(encoding="utf-8", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle)]
    if suffix in {".jsonl", ".ndjson"}:
        return [
            dict(json.loads(line))
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return [dict(row) for row in payload]
    if isinstance(payload, dict):
        for key in ("entries", "objects", "rows"):
            if isinstance(payload.get(key), list):
                return [dict(row) for row in payload[key]]
    raise ValueError(f"unsupported recovery ledger structure: {path}")


def _ledger_pairs(path: Path) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for row in _load_rows(path):
        if str(row.get("market", "US")) != "US":
            continue
        session = str(row.get("session", ""))
        object_id = str(row.get("object_id", row.get("instrument_id", "")))
        if not session or not object_id:
            raise ValueError("ledger rows require session and object_id")
        date.fromisoformat(session)
        status = str(
            row.get(
                "current_file_status",
                row.get("original_status", row.get("status", "reported_omission")),
            )
        )
        if status not in {"reported_omission", "omission"}:
            raise ValueError(
                f"ledger row {session}/{object_id} is not a reported omission: {status}"
            )
        pairs.append((session, object_id))
    return pairs


def _manifest_pairs(path: Path) -> list[tuple[str, str]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != "quantstack.daily-session-parquet.v1":
        raise ValueError(f"unsupported raw session manifest: {path}")
    if payload.get("market") != "US":
        return []
    session = str(payload["session"])
    date.fromisoformat(session)
    omissions = payload.get("omissions")
    if not isinstance(omissions, list):
        raise ValueError(f"raw session omissions must be a list: {path}")
    if len(omissions) != len(set(omissions)):
        raise ValueError(f"raw session omissions contain duplicates: {path}")
    requested = payload.get("requested_instruments")
    instruments = payload.get("instrument_count")
    if not isinstance(requested, int) or not isinstance(instruments, int):
        raise ValueError(f"raw session manifest counts are invalid: {path}")
    if requested != PINNED_UNIVERSE_COUNT:
        raise ValueError(
            f"raw session manifest denominator is not pinned U={PINNED_UNIVERSE_COUNT}: {path}"
        )
    if requested != instruments + len(omissions):
        raise ValueError(f"raw session manifest does not close internally: {path}")
    return [(session, str(object_id)) for object_id in omissions]


def manifest_paths_from_raw_analysis(path: Path) -> list[Path]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    sessions = payload.get("sessions")
    if not isinstance(sessions, list):
        raise ValueError("raw analysis must contain a sessions list")
    paths: list[Path] = []
    for row in sessions:
        if not isinstance(row, dict) or row.get("market") != "US":
            continue
        if row.get("package_validation_pass") is False:
            raise ValueError("raw analysis contains a failed US session package")
        denominator = row.get("authoritative_denominator")
        if isinstance(denominator, dict) and denominator.get("identity_partition_closed") is False:
            raise ValueError("raw analysis contains a non-closed US identity partition")
        package = Path(str(row.get("package_path", "")))
        manifest = package / "manifest.json"
        if not manifest.is_file():
            raise ValueError(f"raw analysis manifest is unavailable: {manifest}")
        expected = (
            row.get("input_integrity", {}).get("manifest", {}).get("observed_sha256")
        )
        if expected and _sha256_file(manifest) != expected:
            raise ValueError(f"raw analysis manifest digest changed: {manifest}")
        paths.append(manifest)
    if not paths:
        raise ValueError("raw analysis contains no US session manifests")
    return paths


def build_frozen_queue(
    *,
    snapshot_path: Path,
    manifest_paths: Sequence[Path] = (),
    omission_ledger: Path | None = None,
    historical_ledger: Path | None = None,
    now: Callable[[], datetime] = _utc_now,
) -> dict[str, Any]:
    snapshot = load_us_instrument_snapshot(snapshot_path)
    if snapshot.snapshot_id != PINNED_SNAPSHOT_ID:
        raise ValueError(f"recovery requires pinned snapshot {PINNED_SNAPSHOT_ID}")
    if len(snapshot.instruments) != PINNED_UNIVERSE_COUNT:
        raise ValueError(f"pinned snapshot must contain {PINNED_UNIVERSE_COUNT} instruments")
    by_id = {item.instrument_id: item for item in snapshot.instruments}

    if bool(manifest_paths) == bool(omission_ledger):
        raise ValueError("provide exactly one of manifest_paths or omission_ledger")
    pairs: list[tuple[str, str]] = []
    sources: list[dict[str, Any]] = []
    if manifest_paths:
        for path in sorted(Path(item).resolve() for item in manifest_paths):
            pairs.extend(_manifest_pairs(path))
            sources.append(
                {"kind": "raw_session_manifest", "path": str(path), "sha256": _sha256_file(path)}
            )
    else:
        assert omission_ledger is not None
        ledger_path = omission_ledger.resolve()
        pairs = _ledger_pairs(ledger_path)
        sources.append(
            {
                "kind": "explicit_omission_ledger",
                "path": str(ledger_path),
                "sha256": _sha256_file(ledger_path),
            }
        )

    if len(pairs) != len(set(pairs)):
        duplicates = sorted(pair for pair, count in _counts(pairs).items() if count > 1)
        raise ValueError(f"duplicate session/object recovery entries: {duplicates[:5]}")
    observed_session_counts = dict(sorted(_counts(session for session, _ in pairs).items()))
    if observed_session_counts != RECOVERY_SESSION_COUNTS:
        raise ValueError(
            "recovery queue session matrix mismatch: "
            f"expected {RECOVERY_SESSION_COUNTS}, got {observed_session_counts}"
        )

    historical_pairs: set[tuple[str, str]] = set()
    if historical_ledger is not None:
        historical_pairs = set(_ledger_pairs(historical_ledger.resolve()))
        missing = historical_pairs.difference(pairs)
        if missing:
            raise ValueError(
                "historical subset is absent from current omissions: "
                f"{sorted(missing)[:5]}"
            )

    entries: list[dict[str, Any]] = []
    for session, object_id in sorted(pairs):
        instrument = by_id.get(object_id)
        if instrument is None:
            raise ValueError(f"recovery object is absent from pinned snapshot: {object_id}")
        entry_id = hashlib.sha256(f"{session}\n{object_id}\n".encode()).hexdigest()
        entries.append(
            {
                "entry_id": entry_id,
                "session": session,
                "object_id": object_id,
                "provider_symbol": instrument.provider_symbol,
                "canonical_venue": instrument.canonical_venue,
                "original_status": "reported_omission",
                "semantic_no_bar_verified": False,
                "historical_159_subset": (session, object_id) in historical_pairs,
            }
        )

    identity = {
        "snapshot_id": snapshot.snapshot_id,
        "snapshot_sha256": _sha256_file(snapshot_path),
        "entries": [
            {key: entry[key] for key in ("session", "object_id", "provider_symbol")}
            for entry in entries
        ],
    }
    queue_id = "r12-us-omission-" + hashlib.sha256(_canonical_json(identity)).hexdigest()[:20]
    session_counts = _counts(entry["session"] for entry in entries)
    return {
        "schema_version": QUEUE_SCHEMA,
        "queue_id": queue_id,
        "frozen_at_utc": _timestamp(now),
        "status": "frozen",
        "pinned_snapshot": {
            "path": str(snapshot_path.resolve()),
            "snapshot_id": snapshot.snapshot_id,
            "sha256": _sha256_file(snapshot_path),
            "universe_count": len(snapshot.instruments),
            "membership_basis": snapshot.membership_basis,
        },
        "sources": sources,
        "policy": {
            "original_status": "reported_omission",
            "omission_is_not_no_bar": True,
            "provider_retry_count": 0,
            "maximum_batch_size": MAX_BATCH_SIZE,
            "maximum_threads": MAX_THREADS,
            "maximum_timeout_seconds": MAX_TIMEOUT_SECONDS,
            "output_scope": "isolated_candidate_only",
        },
        "counts": {
            "entries": len(entries),
            "sessions": len(session_counts),
            "by_session": dict(sorted(session_counts.items())),
            "historical_159_subset": sum(entry["historical_159_subset"] for entry in entries),
        },
        "entries": entries,
    }


def _counts(values: Iterable[Any]) -> dict[Any, int]:
    result: dict[Any, int] = {}
    for value in values:
        result[value] = result.get(value, 0) + 1
    return result


def write_frozen_queue(path: Path, payload: Mapping[str, Any]) -> None:
    if payload.get("schema_version") != QUEUE_SCHEMA or payload.get("status") != "frozen":
        raise ValueError("invalid frozen recovery queue")
    _write_new_json(path, payload)


def load_frozen_queue(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != QUEUE_SCHEMA or payload.get("status") != "frozen":
        raise ValueError("unsupported or mutable recovery queue")
    entries = payload.get("entries")
    if not isinstance(entries, list):
        raise ValueError("recovery queue entries must be a list")
    pinned = payload.get("pinned_snapshot", {})
    if (
        pinned.get("snapshot_id") != PINNED_SNAPSHOT_ID
        or pinned.get("universe_count") != PINNED_UNIVERSE_COUNT
    ):
        raise ValueError("recovery queue does not use the required pinned US snapshot")
    keys = [(str(row.get("session")), str(row.get("object_id"))) for row in entries]
    if len(keys) != len(set(keys)):
        raise ValueError("recovery queue contains duplicate session/object entries")
    if payload.get("counts", {}).get("entries") != len(entries):
        raise ValueError("recovery queue count does not match entries")
    observed_session_counts = dict(sorted(_counts(session for session, _ in keys).items()))
    if len(entries) != RECOVERY_OBJECT_COUNT or observed_session_counts != RECOVERY_SESSION_COUNTS:
        raise ValueError("recovery queue does not match the fixed 515-object session matrix")
    if payload.get("counts", {}).get("by_session") != RECOVERY_SESSION_COUNTS:
        raise ValueError("recovery queue declared session matrix does not match the fixed scope")
    if any(
        row.get("original_status") != "reported_omission"
        or row.get("semantic_no_bar_verified") is not False
        for row in entries
    ):
        raise ValueError("recovery queue changed omission semantics")
    identity = {
        "snapshot_id": pinned["snapshot_id"],
        "snapshot_sha256": pinned.get("sha256"),
        "entries": [
            {key: row[key] for key in ("session", "object_id", "provider_symbol")}
            for row in entries
        ],
    }
    expected_id = "r12-us-omission-" + hashlib.sha256(_canonical_json(identity)).hexdigest()[:20]
    if payload.get("queue_id") != expected_id:
        raise ValueError("recovery queue identity digest does not match its entries")
    return payload


@contextmanager
def _budget_lock(path: Path):
    lock_path = path.with_suffix(path.suffix + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def reserve_budget(
    *,
    ledger_path: Path,
    queue_id: str,
    entries: Sequence[Mapping[str, Any]],
    maximum_objects: int,
    now: Callable[[], datetime] = _utc_now,
) -> dict[str, Any]:
    if maximum_objects < 1:
        raise ValueError("maximum_objects must be positive")
    with _budget_lock(ledger_path):
        if ledger_path.exists():
            ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
            if ledger.get("schema_version") != BUDGET_SCHEMA:
                raise ValueError("unsupported recovery budget ledger")
            if ledger.get("queue_id") != queue_id:
                raise ValueError("recovery budget ledger belongs to a different queue")
            if ledger.get("maximum_objects") != maximum_objects:
                raise ValueError("recovery budget maximum cannot change after creation")
        else:
            ledger = {
                "schema_version": BUDGET_SCHEMA,
                "queue_id": queue_id,
                "maximum_objects": maximum_objects,
                "attempts": {},
            }
        attempts = ledger["attempts"]
        duplicates = [str(row["entry_id"]) for row in entries if str(row["entry_id"]) in attempts]
        if duplicates:
            raise ValueError(
                f"zero-retry policy blocks already attempted entries: {duplicates[:5]}"
            )
        if len(attempts) + len(entries) > maximum_objects:
            raise ValueError(
                "persistent object budget exceeded: "
                f"{len(attempts)}+{len(entries)}>{maximum_objects}"
            )
        reserved_at = _timestamp(now)
        for row in entries:
            attempts[str(row["entry_id"])] = {
                "session": str(row["session"]),
                "object_id": str(row["object_id"]),
                "status": "reserved",
                "reserved_at_utc": reserved_at,
            }
        ledger["used_objects"] = len(attempts)
        ledger["remaining_objects"] = maximum_objects - len(attempts)
        ledger["updated_at_utc"] = reserved_at
        _atomic_json(ledger_path, ledger)
        return ledger


def finalize_budget(
    *,
    ledger_path: Path,
    outcomes: Mapping[str, str],
    now: Callable[[], datetime] = _utc_now,
) -> dict[str, Any]:
    with _budget_lock(ledger_path):
        ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
        attempts = ledger["attempts"]
        completed_at = _timestamp(now)
        for entry_id, outcome in outcomes.items():
            attempt = attempts.get(entry_id)
            if attempt is None or attempt.get("status") != "reserved":
                raise ValueError(f"cannot finalize unreserved recovery entry: {entry_id}")
            attempt["status"] = outcome
            attempt["completed_at_utc"] = completed_at
        ledger["updated_at_utc"] = completed_at
        _atomic_json(ledger_path, ledger)
        return ledger


@contextmanager
def _deadline(seconds: int):
    if seconds < 1 or seconds > MAX_TIMEOUT_SECONDS:
        raise ValueError(f"timeout_seconds must be between 1 and {MAX_TIMEOUT_SECONDS}")
    if not hasattr(signal, "setitimer"):
        yield
        return

    def timeout_handler(signum, frame):  # noqa: ARG001
        raise TimeoutError(f"targeted provider batch exceeded {seconds} seconds")

    previous = signal.signal(signal.SIGALRM, timeout_handler)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def _valid_record(record: Mapping[str, Any], *, symbol: str, session: str) -> bool:
    if str(record.get("ticker", "")).upper() != symbol.upper():
        return False
    if str(record.get("Date", ""))[:10] != session:
        return False
    try:
        open_, high, low, close, volume = (
            float(record[field]) for field in ("Open", "High", "Low", "Close", "Volume")
        )
    except (KeyError, TypeError, ValueError):
        return False
    return bool(
        all(math.isfinite(value) for value in (open_, high, low, close, volume))
        and volume >= 0
        and high >= max(open_, close)
        and low <= min(open_, close)
        and low <= high
    )


def _source_packages(queue: Mapping[str, Any]) -> list[Path]:
    return [
        Path(str(source["path"])).resolve().parent
        for source in queue.get("sources", [])
        if source.get("kind") == "raw_session_manifest"
    ]


def _require_isolated(candidate_root: Path, queue: Mapping[str, Any]) -> Path:
    candidate = candidate_root.resolve()
    for source in _source_packages(queue):
        if (
            candidate == source
            or candidate.is_relative_to(source)
            or source.is_relative_to(candidate)
        ):
            raise ValueError(f"candidate root must be outside source package: {source}")
    return candidate


def _require_safe_run_root(candidate: Path, run_id: str) -> Path:
    if run_id in {".", ".."} or _SAFE_RUN_ID.fullmatch(run_id) is None:
        raise ValueError("run_id must be one safe path component")
    run_root = candidate / run_id
    if run_root.is_symlink() or run_root.exists():
        raise ValueError(f"candidate run already exists: {run_root}")
    resolved = run_root.resolve(strict=False)
    if resolved.parent != candidate:
        raise ValueError("candidate run path escapes the isolated candidate root")
    return resolved


def _shared_proxy_runtime_receipt(runtime_root: Path, proxy_url: str) -> dict[str, Any]:
    if not proxy_url.strip():
        raise ValueError("the established explicit proxy route is required")
    route = yfinance_runtime_root(runtime_root, proxy_url=proxy_url).resolve()
    if not route.is_dir():
        raise ValueError("shared explicit-proxy yfinance runtime route must already exist")
    request_lock = route / "request.lock"
    last_request = route / "last_request_at"
    pacing_policy = route / "pacing-policy.json"
    if not request_lock.is_file():
        raise ValueError("shared explicit-proxy request lock is unavailable")
    if not last_request.is_file() and not pacing_policy.is_file():
        raise ValueError("shared explicit-proxy pacing state is unavailable")
    circuit = route / "rate_limit_blocked_until"
    circuit_state = "closed"
    blocked_until: float | None = None
    if circuit.exists():
        blocked_until = float(circuit.read_text(encoding="utf-8").strip())
        circuit_state = "persisted"
    return {
        "resolved_route": str(route),
        "request_lock": str(request_lock),
        "pacing_state": str(last_request if last_request.is_file() else pacing_policy),
        "circuit_path": str(circuit),
        "circuit_state": circuit_state,
        "blocked_until_epoch": blocked_until,
    }


def run_targeted_recovery(
    *,
    queue_path: Path,
    session: date,
    candidate_root: Path,
    budget_ledger: Path,
    runtime_root: Path,
    maximum_objects: int,
    run_id: str,
    execute: bool = False,
    caller_holds_operation_lock: bool = False,
    object_ids: Sequence[str] = (),
    batch_size: int = MAX_BATCH_SIZE,
    threads: int = MAX_THREADS,
    timeout_seconds: int = MAX_TIMEOUT_SECONDS,
    proxy_url: str | None = None,
    transport_factory: Callable[..., Any] = LiveYFinanceTransport,
    now: Callable[[], datetime] = _utc_now,
) -> dict[str, Any]:
    if not 1 <= batch_size <= MAX_BATCH_SIZE:
        raise ValueError(f"batch_size must be between 1 and {MAX_BATCH_SIZE}")
    if threads != 1:
        raise ValueError("R12 targeted recovery requires threads=1")
    if not 1 <= timeout_seconds <= MAX_TIMEOUT_SECONDS:
        raise ValueError(f"timeout_seconds must be between 1 and {MAX_TIMEOUT_SECONDS}")
    queue = load_frozen_queue(queue_path)
    candidate = _require_isolated(candidate_root, queue)
    run_root = _require_safe_run_root(candidate, run_id)
    if proxy_url is None:
        raise ValueError("the established explicit proxy route is required")
    selected = [row for row in queue["entries"] if row["session"] == session.isoformat()]
    if object_ids:
        wanted = set(object_ids)
        selected = [row for row in selected if row["object_id"] in wanted]
        missing = wanted.difference(row["object_id"] for row in selected)
        if missing:
            raise ValueError(f"requested objects are absent from queue/session: {sorted(missing)}")
    if not selected:
        raise ValueError(f"queue has no selected entries for session {session.isoformat()}")
    if len(selected) > batch_size:
        raise ValueError(
            f"selected recovery set {len(selected)} exceeds one-batch limit {batch_size}"
        )

    dry_run = {
        "schema_version": CANDIDATE_SCHEMA,
        "mode": "dry_run",
        "queue_id": queue["queue_id"],
        "session": session.isoformat(),
        "selected_objects": len(selected),
        "provider_calls": 0,
        "would_write": str(candidate / run_id / "candidate.json"),
        "policy": {
            "batch_size": batch_size,
            "threads": 1,
            "transport_threads": False,
            "timeout_seconds": timeout_seconds,
            "retries": 0,
            "shared_runtime_root": str(runtime_root.resolve()),
            "caller_operation_lock_required": True,
        },
    }
    if not execute:
        return dry_run
    if not caller_holds_operation_lock:
        raise ValueError("--execute requires caller_holds_operation_lock")
    runtime_receipt = _shared_proxy_runtime_receipt(runtime_root, proxy_url)
    if not budget_ledger.resolve().is_relative_to(candidate):
        raise ValueError("budget ledger must be inside the isolated candidate root")
    reserve_budget(
        ledger_path=budget_ledger,
        queue_id=str(queue["queue_id"]),
        entries=selected,
        maximum_objects=maximum_objects,
        now=now,
    )
    requested_symbols = [str(row["provider_symbol"]).upper() for row in selected]
    records: list[Mapping[str, Any]] = []
    error: dict[str, str] | None = None
    transport = transport_factory(
        runtime_root=runtime_root,
        proxy_url=proxy_url,
        include_actions=False,
    )
    try:
        with _deadline(timeout_seconds):
            records = list(
                transport.fetch_daily_bars_batch(
                    requested_symbols,
                    session,
                    session,
                    include_actions=False,
                    threads=False,
                )
            )
    except Exception as exc:  # provider/circuit/deadline all fail closed without retry
        error = {"type": type(exc).__name__, "message": str(exc)}

    audit = dict(transport.audit_snapshot())
    reported = list(audit.get("request_outcomes", []))
    records_by_symbol: dict[str, list[dict[str, Any]]] = {}
    for raw in records:
        symbol = str(raw.get("ticker", "")).upper()
        records_by_symbol.setdefault(symbol, []).append(dict(raw))

    outcomes: list[dict[str, Any]] = []
    budget_outcomes: dict[str, str] = {}
    for index, entry in enumerate(selected):
        symbol = str(entry["provider_symbol"]).upper()
        symbol_records = records_by_symbol.get(symbol, [])
        provider_status = reported[index] if index < len(reported) else None
        valid_records = [
            row
            for row in symbol_records
            if _valid_record(row, symbol=symbol, session=session.isoformat())
        ]
        if error is not None or provider_status == "provider_error":
            outcome = "provider_error"
            accepted_records: list[dict[str, Any]] = []
        elif (
            provider_status == "returned"
            and len(valid_records) == 1
            and len(symbol_records) == 1
        ):
            outcome = "returned"
            accepted_records = valid_records
        elif provider_status == "no_bar" and not symbol_records:
            outcome = "provider_reported_no_bar"
            accepted_records = []
        else:
            outcome = "provider_error"
            accepted_records = []
        budget_outcomes[str(entry["entry_id"])] = outcome
        outcomes.append(
            {
                "entry_id": entry["entry_id"],
                "session": entry["session"],
                "object_id": entry["object_id"],
                "provider_symbol": symbol,
                "original_status": "reported_omission",
                "provider_audit_status": provider_status,
                "recovery_outcome": outcome,
                "semantic_no_bar_accepted": False,
                "records": accepted_records,
            }
        )

    finalize_budget(ledger_path=budget_ledger, outcomes=budget_outcomes, now=now)
    outcome_counts = _counts(row["recovery_outcome"] for row in outcomes)
    candidate_payload = {
        "schema_version": CANDIDATE_SCHEMA,
        "mode": "executed",
        "run_id": run_id,
        "created_at_utc": _timestamp(now),
        "queue": {
            "path": str(queue_path.resolve()),
            "queue_id": queue["queue_id"],
            "sha256": _sha256_file(queue_path),
        },
        "session": session.isoformat(),
        "status": "requires_review",
        "policy": {
            "batch_size": batch_size,
            "threads": 1,
            "transport_threads": False,
            "timeout_seconds": timeout_seconds,
            "retries": 0,
            "shared_runtime_root": str(runtime_root.resolve()),
            "shared_runtime_route": runtime_receipt,
            "caller_operation_lock_asserted": True,
            "original_packages_mutated": False,
            "provider_reported_no_bar_is_not_accepted_semantic_no_bar": True,
        },
        "provider_error": error,
        "transport_audit": audit,
        "counts": {"selected": len(selected), **dict(sorted(outcome_counts.items()))},
        "outcomes": outcomes,
    }
    run_root.mkdir(parents=True, exist_ok=False)
    _write_new_json(run_root / "candidate.json", candidate_payload)
    return candidate_payload
