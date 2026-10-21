"""Process-safe operational controls for the BaoStock free API.

BaoStock is a shared free service rather than an unlimited batch endpoint.  The
controls in this module are deliberately conservative: every live caller uses
one process-wide (and, through file locks, cross-process) flight, persists a
daily request ledger, and stops locally before the upstream 50,000-request
ceiling.  The ledger is runtime state and must live beside the isolated data
root, never in Git.
"""

from __future__ import annotations

import fcntl
import json
import os
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

BAOSTOCK_TIMEZONE = "Asia/Shanghai"
BAOSTOCK_HARD_DAILY_REQUEST_LIMIT = 50_000
BAOSTOCK_DEFAULT_DAILY_REQUEST_LIMIT = 49_000
BAOSTOCK_DEFAULT_BATCH_SIZE = 50
BAOSTOCK_MAX_BATCH_SIZE = 500
BAOSTOCK_HISTORY_REQUEST_INTERVAL_SECONDS = 0.05
BAOSTOCK_DEFAULT_BATCH_INTERVAL_SECONDS = 600.0
# BaoStock's official limits are the daily request ceiling and the ban on
# concurrent connections; there is no official minimum batch interval.  The
# default ten-minute gap is a self-imposed pacing choice, and with it a
# 24-hour UTC+8 window can start at most 144 batches.  Operators may lower the
# interval via QUANTSTACK_BAOSTOCK_MIN_BATCH_INTERVAL_SECONDS; the daily
# serial-batch ceiling then scales with the configured interval while the
# request ledger stays the binding limit.
BAOSTOCK_DEFAULT_MAX_BATCHES_PER_DAY = 144
BAOSTOCK_BATCH_INTERVAL_ENV = "QUANTSTACK_BAOSTOCK_MIN_BATCH_INTERVAL_SECONDS"
BAOSTOCK_BLACKLIST_ERROR_CODE = "10001011"
BAOSTOCK_BLOCK_COOLDOWN_SECONDS = 24 * 60 * 60
_LEDGER_SCHEMA_VERSION = "1.0.0"


def configured_batch_interval_seconds() -> float:
    """Effective minimum batch interval: operator env override or the 600s default."""

    raw = os.environ.get(BAOSTOCK_BATCH_INTERVAL_ENV)
    if raw is None or not raw.strip():
        return BAOSTOCK_DEFAULT_BATCH_INTERVAL_SECONDS
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{BAOSTOCK_BATCH_INTERVAL_ENV} must be a number") from exc
    if not 0 <= value <= 24 * 60 * 60:
        raise ValueError(f"{BAOSTOCK_BATCH_INTERVAL_ENV} must be between 0 and 86400 seconds")
    return value


def max_batches_per_day_for_interval(min_batch_interval_seconds: float) -> int:
    """Daily serial-batch ceiling derived from the effective pacing interval."""

    if min_batch_interval_seconds > 0:
        return max(
            BAOSTOCK_DEFAULT_MAX_BATCHES_PER_DAY,
            int(24 * 60 * 60 // min_batch_interval_seconds),
        )
    return BAOSTOCK_DEFAULT_MAX_BATCHES_PER_DAY


class BaoStockAccessRejected(RuntimeError):
    """BaoStock rejected the current egress, normally with blacklist code 10001011."""

    def __init__(self, error_code: str, error_message: str) -> None:
        self.error_code = str(error_code)
        self.error_message = str(error_message)
        super().__init__(f"BaoStock access rejected: {self.error_code} {self.error_message}")


class BaoStockDailyQuotaExceeded(RuntimeError):
    """The local persisted ledger has no safe capacity for another API call."""

    def __init__(self, *, day: str, used: int, requested: int, limit: int) -> None:
        self.day = day
        self.used = used
        self.requested = requested
        self.limit = limit
        super().__init__(
            "BaoStock daily request budget exhausted: "
            f"day={day} used={used} requested={requested} limit={limit}"
        )


class BaoStockDailyBatchLimitExceeded(RuntimeError):
    """The pacing ceiling has been reached for the current local day."""

    def __init__(self, *, day: str, used: int, limit: int) -> None:
        self.day = day
        self.used = used
        self.limit = limit
        super().__init__(
            "BaoStock daily serial batch ceiling exhausted: "
            f"day={day} used={used} limit={limit}"
        )


class BaoStockRequestBudget:
    """Persistent quota, pacing, and single-flight control for BaoStock.

    ``daily_limit`` is intentionally below the provider's stated hard ceiling
    so a login and recovery probe never push a day over 50,000 operations.  A
    request is counted for each login and each history/basic query call; row
    pagination is part of that call and is not used to create additional
    credits.
    """

    def __init__(
        self,
        path: Path,
        *,
        daily_limit: int = BAOSTOCK_DEFAULT_DAILY_REQUEST_LIMIT,
        min_batch_interval_seconds: float = BAOSTOCK_DEFAULT_BATCH_INTERVAL_SECONDS,
        timezone: str = BAOSTOCK_TIMEZONE,
    ) -> None:
        if not 1 <= daily_limit <= BAOSTOCK_DEFAULT_DAILY_REQUEST_LIMIT:
            raise ValueError("BaoStock runtime daily limit must be between 1 and 49000")
        if not 0 <= min_batch_interval_seconds <= 24 * 60 * 60:
            raise ValueError("BaoStock batch interval must be between 0 and 86400 seconds")
        try:
            ZoneInfo(timezone)
        except Exception as exc:  # pragma: no cover - only invalid local configuration
            raise ValueError(f"unknown BaoStock budget timezone: {timezone}") from exc
        self.path = Path(path).expanduser()
        self.lock_path = self.path.with_name(f"{self.path.name}.lock")
        self.session_lock_path = self.path.with_name(f"{self.path.name}.session.lock")
        self.daily_limit = daily_limit
        self.min_batch_interval_seconds = min_batch_interval_seconds
        self.timezone = timezone
        # The daily serial-batch ceiling is a pacing derivative: it never goes
        # below the historical 144, and scales up when an operator configures a
        # shorter interval.  The request ledger remains the binding limit.
        self.max_batches_per_day = max_batches_per_day_for_interval(min_batch_interval_seconds)

    @classmethod
    def from_default(cls) -> BaoStockRequestBudget:
        configured = os.environ.get("QUANTSTACK_BAOSTOCK_BUDGET_PATH")
        path = (
            Path(configured).expanduser()
            if configured
            else Path(".local-data/baostock/baostock-request-budget.json")
        )
        return cls(path, min_batch_interval_seconds=configured_batch_interval_seconds())

    @staticmethod
    def for_data_root(data_root: Path) -> BaoStockRequestBudget:
        """Return the campaign-local ledger beside a ``staging`` data root."""

        root = Path(data_root).expanduser()
        return BaoStockRequestBudget.for_control_root(root.parent / "control")

    @staticmethod
    def for_control_root(control_root: Path) -> BaoStockRequestBudget:
        """Return the ledger and session lock for one shared control domain."""

        return BaoStockRequestBudget(
            Path(control_root).expanduser() / "baostock-request-budget.json",
            min_batch_interval_seconds=configured_batch_interval_seconds(),
        )

    def _now(self) -> datetime:
        return datetime.now(ZoneInfo(self.timezone))

    def _empty_state(self) -> dict[str, object]:
        return {
            "schema_version": _LEDGER_SCHEMA_VERSION,
            "timezone": self.timezone,
            "daily_limit": self.daily_limit,
            "hard_daily_limit": BAOSTOCK_HARD_DAILY_REQUEST_LIMIT,
            "min_batch_interval_seconds": self.min_batch_interval_seconds,
            "blocked": None,
            "last_batch_completed_at": None,
            "days": {},
        }

    def _load_unlocked(self) -> dict[str, object]:
        if not self.path.exists():
            return self._empty_state()
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"BaoStock request ledger is unreadable: {self.path}") from exc
        if not isinstance(payload, dict) or payload.get("schema_version") != _LEDGER_SCHEMA_VERSION:
            raise RuntimeError(
                "BaoStock request ledger schema is unsupported; stop before requests"
            )
        return payload

    def _write_unlocked(self, payload: Mapping[str, object]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(f".{self.path.name}.tmp-{os.getpid()}")
        temporary.write_text(
            json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.chmod(temporary, 0o600)
        os.replace(temporary, self.path)

    @contextmanager
    def _ledger_lock(self) -> Iterator[None]:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        with self.lock_path.open("a+", encoding="utf-8") as stream:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)

    @contextmanager
    def session_lock(self) -> Iterator[None]:
        """Serialize all BaoStock sessions across processes and workers."""

        self.session_lock_path.parent.mkdir(parents=True, exist_ok=True)
        with self.session_lock_path.open("a+", encoding="utf-8") as stream:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)

    def _day_record(self, payload: dict[str, object], day: str) -> dict[str, object]:
        days = payload.setdefault("days", {})
        if not isinstance(days, dict):
            raise RuntimeError("BaoStock request ledger days field is malformed")
        record = days.setdefault(day, {"used": 0, "batches": 0})
        if not isinstance(record, dict):
            raise RuntimeError("BaoStock request ledger day record is malformed")
        return record

    @staticmethod
    def _blocked_until(payload: Mapping[str, object]) -> datetime | None:
        blocked = payload.get("blocked")
        if not isinstance(blocked, Mapping):
            return None
        value = blocked.get("until")
        if not value:
            return None
        try:
            return datetime.fromisoformat(str(value))
        except ValueError as exc:
            raise RuntimeError("BaoStock request ledger blocked-until value is malformed") from exc

    def _ensure_not_blocked(self, payload: Mapping[str, object], now: datetime) -> None:
        until = self._blocked_until(payload)
        if until is not None and now < until:
            blocked = payload.get("blocked")
            code = blocked.get("error_code") if isinstance(blocked, Mapping) else "unknown"
            message = (
                blocked.get("message")
                if isinstance(blocked, Mapping)
                else "provider rejected access"
            )
            raise BaoStockAccessRejected(
                str(code), f"local cooldown until {until.isoformat()}: {message}"
            )

    def ensure_capacity(self, requests: int = 1) -> None:
        if not isinstance(requests, int) or isinstance(requests, bool) or requests < 1:
            raise ValueError("BaoStock request reservation must be a positive integer")
        now = self._now()
        day = now.date().isoformat()
        with self._ledger_lock():
            payload = self._load_unlocked()
            self._ensure_not_blocked(payload, now)
            record = self._day_record(payload, day)
            used = int(record.get("used", 0))
            if used + requests > self.daily_limit:
                raise BaoStockDailyQuotaExceeded(
                    day=day, used=used, requested=requests, limit=self.daily_limit
                )

    def reserve(self, requests: int = 1) -> None:
        """Atomically consume request credits after a preflight capacity check."""

        if not isinstance(requests, int) or isinstance(requests, bool) or requests < 1:
            raise ValueError("BaoStock request reservation must be a positive integer")
        now = self._now()
        day = now.date().isoformat()
        with self._ledger_lock():
            payload = self._load_unlocked()
            self._ensure_not_blocked(payload, now)
            record = self._day_record(payload, day)
            used = int(record.get("used", 0))
            if used + requests > self.daily_limit:
                raise BaoStockDailyQuotaExceeded(
                    day=day, used=used, requested=requests, limit=self.daily_limit
                )
            record["used"] = used + requests
            record["last_reserved_at"] = now.isoformat()
            self._write_unlocked(payload)

    def wait_for_batch_slot(self) -> None:
        """Wait until the previous batch has completed its mandatory cool-down."""

        while True:
            now = self._now()
            with self._ledger_lock():
                payload = self._load_unlocked()
                self._ensure_not_blocked(payload, now)
                previous = payload.get("last_batch_completed_at")
                if not previous:
                    return
                try:
                    completed = datetime.fromisoformat(str(previous))
                except ValueError as exc:
                    raise RuntimeError(
                        "BaoStock request ledger batch timestamp is malformed"
                    ) from exc
                remaining = self.min_batch_interval_seconds - (now - completed).total_seconds()
            if remaining <= 0:
                return
            time.sleep(min(remaining, 60.0))

    def begin_batch(self, instrument_count: int) -> None:
        if not 1 <= instrument_count <= BAOSTOCK_MAX_BATCH_SIZE:
            raise ValueError("BaoStock request batch must contain 1 to 500 instruments")
        now = self._now()
        day = now.date().isoformat()
        with self._ledger_lock():
            payload = self._load_unlocked()
            self._ensure_not_blocked(payload, now)
            record = self._day_record(payload, day)
            batches = int(record.get("batches", 0))
            if batches >= self.max_batches_per_day:
                raise BaoStockDailyBatchLimitExceeded(
                    day=day,
                    used=batches,
                    limit=self.max_batches_per_day,
                )
            record["batches"] = batches + 1
            record["last_batch_started_at"] = now.isoformat()
            self._write_unlocked(payload)

    def complete_batch(self) -> None:
        now = self._now()
        with self._ledger_lock():
            payload = self._load_unlocked()
            payload["last_batch_completed_at"] = now.isoformat()
            self._write_unlocked(payload)

    def mark_blocked(self, *, error_code: str, message: str) -> None:
        now = self._now()
        until = now + timedelta(seconds=BAOSTOCK_BLOCK_COOLDOWN_SECONDS)
        with self._ledger_lock():
            payload = self._load_unlocked()
            payload["blocked"] = {
                "error_code": str(error_code),
                "message": str(message).replace("\n", " ")[:500],
                "observed_at": now.isoformat(),
                "until": until.isoformat(),
                "reason": "provider blacklist or access rejection; no blind retry",
            }
            self._write_unlocked(payload)

    def clear_block(self, *, reason: str) -> None:
        """Clear a local cooldown only after an operator verified a safe new egress."""

        if not reason.strip():
            raise ValueError("clearing BaoStock block requires an operator reason")
        with self._ledger_lock():
            payload = self._load_unlocked()
            payload["blocked"] = {
                "cleared_at": self._now().isoformat(),
                "reason": reason.strip()[:500],
            }
            self._write_unlocked(payload)

    def snapshot(self) -> dict[str, object]:
        with self._ledger_lock():
            payload = self._load_unlocked()
            return json.loads(json.dumps(payload))
