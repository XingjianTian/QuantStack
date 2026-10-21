from __future__ import annotations

import json
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path

DEFAULT_YFINANCE_INTERVAL_SECONDS = 1.0
MINIMUM_LIVE_YFINANCE_INTERVAL_SECONDS = 0.05
CALIBRATION_INTERVAL_SECONDS = (1.0, 0.5, 0.25)
LEGACY_SAFE_FALLBACK_INTERVAL_SECONDS = 10.0
PACING_POLICY_SCHEMA = "quantstack.yfinance-pacing-policy.v1"


def yfinance_runtime_root(
    runtime_root: str | Path | None,
    *,
    proxy_url: str | None,
) -> Path:
    configured_root = runtime_root or os.environ.get("QUANTSTACK_YFINANCE_RUNTIME_ROOT")
    base = Path(configured_root or Path.home() / ".cache/quantstack/yfinance")
    return base / "routes/explicit-proxy-v1" if proxy_url else base


def pacing_policy_path(runtime_root: Path) -> Path:
    return runtime_root / "pacing-policy.json"


def load_calibrated_interval(runtime_root: Path) -> float | None:
    path = pacing_policy_path(runtime_root)
    if not path.is_file():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != PACING_POLICY_SCHEMA:
        raise ValueError("unsupported yfinance pacing policy schema")
    interval = float(payload["selected_interval_seconds"])
    require_live_yfinance_interval(interval)
    return interval


def resolve_yfinance_interval(
    runtime_root: Path,
    explicit_interval_seconds: float | None,
) -> float:
    if explicit_interval_seconds is not None:
        if explicit_interval_seconds < 0:
            raise ValueError("yfinance request interval must be non-negative")
        return float(explicit_interval_seconds)
    calibrated = load_calibrated_interval(runtime_root)
    return calibrated if calibrated is not None else DEFAULT_YFINANCE_INTERVAL_SECONDS


def require_live_yfinance_interval(interval_seconds: float) -> None:
    if interval_seconds < MINIMUM_LIVE_YFINANCE_INTERVAL_SECONDS:
        raise ValueError("live yfinance requests require at least a 0.05 second interval")


def write_pacing_policy(
    runtime_root: Path,
    *,
    selected_interval_seconds: float,
    status: str,
    stages: list[dict[str, object]],
) -> Path:
    require_live_yfinance_interval(selected_interval_seconds)
    payload = {
        "schema_version": PACING_POLICY_SCHEMA,
        "updated_at": datetime.now(UTC).isoformat(),
        "status": status,
        "selected_interval_seconds": selected_interval_seconds,
        "strictly_serial": True,
        "provider_retries": 0,
        "stages": stages,
    }
    path = pacing_policy_path(runtime_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return path
