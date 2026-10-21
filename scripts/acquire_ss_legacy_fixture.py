#!/usr/bin/env python3
"""Acquire one bounded, immutable Stock Screener legacy YFinance fixture."""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import os
import shutil
import sys
import time
from datetime import UTC, datetime
from importlib import metadata
from pathlib import Path
from typing import Any

import pandas as pd

EXPECTED_YFINANCE_VERSION = "0.2.66"
EXPECTED_FROZENDICT_VERSION = "2.4.7"
PACKAGE_ARTIFACTS = (
    {
        "filename": "yfinance-0.2.66-py2.py3-none-any.whl",
        "sha256": "511a1a40a687f277aae3a02543009a8aeaa292fce5509671f58915078aebb5c7",
    },
    {
        "filename": "frozendict-2.4.7-py3-none-any.whl",
        "sha256": "972af65924ea25cf5b4d9326d549e69a9a4918d8a76a9d3a7cd174d98b237550",
    },
)
FROZEN_CORPUS = (
    "600000.SS",
    "600519.SS",
    "601318.SS",
    "601398.SS",
    "000001.SZ",
    "000333.SZ",
    "000858.SZ",
    "002594.SZ",
)
REQUIRED_COLUMNS = ("Open", "High", "Low", "Close", "Volume")
PERIOD = "5y"
INTERVAL = "1d"
MINIMUM_INTERVAL_SECONDS = 2.0
SOURCE_PROVENANCE = {
    "stock_screener_repository": "xang1234/stock-screener",
    "stock_screener_head": "da4d01a83e420ac9c148ff4fe0260ee1e4228885",
    "service_path": "backend/app/services/yfinance_service.py",
    "service_git_blob": "b1bd286515d2c5460e9f0c036ddc8050e383979b",
    "call_shape": 'yf.Ticker(symbol).history(period="5y", interval="1d")',
}


def _canonical_json(value: object) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _iso(value: Any) -> str:
    if hasattr(value, "isoformat"):
        return str(value.isoformat())
    return str(value)


def _index_timezone(frame: pd.DataFrame) -> str | None:
    timezone = getattr(frame.index, "tz", None)
    return None if timezone is None else str(timezone)


def _verify_runtime_dependencies() -> None:
    observed = metadata.version("frozendict")
    if observed != EXPECTED_FROZENDICT_VERSION:
        raise RuntimeError(
            f"expected frozendict {EXPECTED_FROZENDICT_VERSION}, observed {observed}"
        )


def _validate_frame(symbol: str, frame: pd.DataFrame) -> None:
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        raise RuntimeError(f"{symbol}: legacy service returned an empty frame")
    missing = [column for column in REQUIRED_COLUMNS if column not in frame.columns]
    if missing:
        raise RuntimeError(f"{symbol}: legacy frame missing required columns {missing}")
    if not isinstance(frame.index, pd.DatetimeIndex):
        raise RuntimeError(f"{symbol}: legacy frame index is not DatetimeIndex")
    if frame.index.has_duplicates:
        raise RuntimeError(f"{symbol}: legacy frame contains duplicate index values")
    if not frame.index.is_monotonic_increasing:
        raise RuntimeError(f"{symbol}: legacy frame index is not monotonic increasing")
    if frame[list(REQUIRED_COLUMNS)].isna().all(axis=None):
        raise RuntimeError(f"{symbol}: legacy OHLCV columns contain no values")
    span_days = (frame.index.max() - frame.index.min()).total_seconds() / 86400
    if span_days < 365 * 4.7:
        raise RuntimeError(f"{symbol}: legacy frame does not cover approximately five years")


def _frame_metadata(
    symbol: str,
    frame: pd.DataFrame,
    artifact: str,
    digest: str,
) -> dict[str, object]:
    return {
        "symbol": symbol,
        "artifact": artifact,
        "sha256": digest,
        "rows": int(len(frame)),
        "first_index": _iso(frame.index.min()),
        "last_index": _iso(frame.index.max()),
        "columns": [str(column) for column in frame.columns],
        "dtypes": {str(column): str(dtype) for column, dtype in frame.dtypes.items()},
        "index_name": frame.index.name,
        "index_dtype": str(frame.index.dtype),
        "index_timezone": _index_timezone(frame),
    }


def _window_verification(symbol: str, frame: pd.DataFrame) -> dict[str, object]:
    anchor = frame.index.max()
    windows = {
        "6mo": anchor - pd.DateOffset(months=6),
        "1y": anchor - pd.DateOffset(years=1),
        "2y": anchor - pd.DateOffset(years=2),
        "5y": frame.index.min(),
    }
    verified: dict[str, object] = {}
    for period, cutoff in windows.items():
        sliced = frame.loc[frame.index >= cutoff]
        if sliced.empty:
            raise RuntimeError(f"{symbol}: {period} replay window is empty")
        verified[period] = {
            "cutoff": _iso(cutoff),
            "rows": int(len(sliced)),
            "first_index": _iso(sliced.index.min()),
            "last_index": _iso(sliced.index.max()),
        }
    return {"symbol": symbol, "anchor": _iso(anchor), "windows": verified}


def acquire(
    output_root: Path,
    *,
    yfinance_module: Any | None = None,
    sleep=time.sleep,
    monotonic=time.monotonic,
) -> dict[str, object]:
    """Acquire exactly one serial logical call for every frozen symbol."""

    if output_root.exists() or output_root.is_symlink():
        raise RuntimeError("output root must not already exist")
    output_root.parent.mkdir(parents=True, exist_ok=True)
    scratch = output_root.parent / f".{output_root.name}.acquiring"
    if scratch.exists() or scratch.is_symlink():
        raise RuntimeError("acquisition scratch root already exists")

    yf = yfinance_module or importlib.import_module("yfinance")
    version = str(getattr(yf, "__version__", ""))
    if version != EXPECTED_YFINANCE_VERSION:
        raise RuntimeError(
            f"expected yfinance {EXPECTED_YFINANCE_VERSION}, observed {version or 'unknown'}"
        )

    raw_root = scratch / "raw"
    raw_root.mkdir(parents=True, mode=0o700)
    ledger: list[dict[str, object]] = []
    artifacts: list[dict[str, object]] = []
    windows: list[dict[str, object]] = []
    previous_started: float | None = None

    try:
        for logical_call, symbol in enumerate(FROZEN_CORPUS, start=1):
            if previous_started is not None:
                remaining = MINIMUM_INTERVAL_SECONDS - (monotonic() - previous_started)
                if remaining > 0:
                    sleep(remaining)
            started_epoch = monotonic()
            previous_started = started_epoch
            started_at = datetime.now(UTC).isoformat()
            ticker = yf.Ticker(symbol)
            frame = ticker.history(period=PERIOD, interval=INTERVAL)
            finished_at = datetime.now(UTC).isoformat()
            _validate_frame(symbol, frame)

            artifact_name = f"{symbol.replace('.', '_')}.parquet"
            artifact_path = raw_root / artifact_name
            frame.to_parquet(artifact_path, engine="pyarrow", compression="zstd", index=True)
            digest = _sha256(artifact_path)
            metadata = _frame_metadata(symbol, frame, f"raw/{artifact_name}", digest)
            artifacts.append(metadata)
            windows.append(_window_verification(symbol, frame))
            ledger.append(
                {
                    "logical_call": logical_call,
                    "symbol": symbol,
                    "period": PERIOD,
                    "interval": INTERVAL,
                    "started_at": started_at,
                    "finished_at": finished_at,
                    "status": "success",
                    **metadata,
                }
            )

        if len(ledger) != len(FROZEN_CORPUS):
            raise RuntimeError("logical-call ledger is incomplete")

        provenance = {
            "schema_version": "ss.legacy-yfinance-source-provenance.v1",
            "created_at": datetime.now(UTC).isoformat(),
            **SOURCE_PROVENANCE,
            "yfinance_version": version,
            "frozendict_version": EXPECTED_FROZENDICT_VERSION,
            "package_artifacts": list(PACKAGE_ARTIFACTS),
            "pandas_version": pd.__version__,
            "python_version": sys.version.split()[0],
            "period": PERIOD,
            "interval": INTERVAL,
            "frozen_corpus": list(FROZEN_CORPUS),
            "logical_calls": len(ledger),
            "concurrency": 1,
            "script_level_retries": 0,
            "normalization_applied": False,
            "research_only": True,
        }
        manifest = {
            "schema_version": "ss.legacy-yfinance-fixture.v1",
            "fixture_id": "legacy_yfinance_cn_t110",
            "source_provenance": "source-provenance.json",
            "ledger": "acquisition-ledger.jsonl",
            "window_verification": "window-verification.json",
            "artifact_count": len(artifacts),
            "symbols": list(FROZEN_CORPUS),
            "artifacts": artifacts,
            "adjustment_semantics": (
                "Exact yfinance 0.2.66 Ticker.history default output from the pinned "
                "Stock Screener call shape; no post-fetch normalization or adjustment."
            ),
        }
        (scratch / "source-provenance.json").write_bytes(_canonical_json(provenance))
        (scratch / "manifest.json").write_bytes(_canonical_json(manifest))
        (scratch / "window-verification.json").write_bytes(
            _canonical_json(
                {
                    "schema_version": "ss.legacy-window-verification.v1",
                    "symbols": windows,
                }
            )
        )
        ledger_bytes = b"".join(
            (json.dumps(row, sort_keys=True) + "\n").encode() for row in ledger
        )
        (scratch / "acquisition-ledger.jsonl").write_bytes(ledger_bytes)

        os.replace(scratch, output_root)
        return manifest
    except BaseException:
        shutil.rmtree(scratch, ignore_errors=True)
        raise


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    _verify_runtime_dependencies()
    manifest = acquire(args.output_root)
    print(json.dumps({"status": "PASS", "fixture_id": manifest["fixture_id"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
