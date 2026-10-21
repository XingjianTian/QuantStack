"""Fail-closed replay of the authentic T110 Stock Screener legacy fixture."""

from __future__ import annotations

import hashlib
import json
import os
from functools import wraps
from pathlib import Path
from typing import Any

import pandas as pd

ENABLE_ENV = "T111_LEGACY_FIXTURE_REPLAY_ENABLED"
MISSING_AS_EMPTY_ENV = "T111_LEGACY_FIXTURE_MISSING_AS_EMPTY"
ROOT_ENV = "T111_LEGACY_FIXTURE_ROOT"
RUNTIME_ENV = "SS_RUNTIME_ENV"
CONTAINER_FIXTURE_ROOT = Path("/fixtures/legacy_yfinance_cn_t110")
EXPECTED_COLUMNS = [
    "Open",
    "High",
    "Low",
    "Close",
    "Volume",
    "Dividends",
    "Stock Splits",
]
EXPECTED_DTYPES = {
    "Open": "float64",
    "High": "float64",
    "Low": "float64",
    "Close": "float64",
    "Volume": "int64",
    "Dividends": "float64",
    "Stock Splits": "float64",
}
EXPECTED_PERIODS = {"6mo", "1y", "2y", "5y"}


def _empty_legacy_frame() -> pd.DataFrame:
    frame = pd.DataFrame(
        {column: pd.Series(dtype=dtype) for column, dtype in EXPECTED_DTYPES.items()}
    )
    frame.index = pd.DatetimeIndex([], name="Date", tz="Asia/Shanghai")
    return frame


class LegacyFixtureError(RuntimeError):
    """The isolated replay fixture is absent, unexpected, or has drifted."""


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LegacyFixtureError(f"cannot read fixture metadata: {path.name}") from exc
    if not isinstance(value, dict):
        raise LegacyFixtureError(f"fixture metadata is not an object: {path.name}")
    return value


def _artifact_digest(path: Path) -> str:
    """Verify only the returned Parquet artifact, as required by the fixture contract."""

    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise LegacyFixtureError(f"cannot read fixture artifact: {path.name}") from exc
    return digest.hexdigest()


class LegacyFixtureReplay:
    def __init__(self, root: Path) -> None:
        self.root = root.resolve()
        if not self.root.is_dir():
            raise LegacyFixtureError("fixture root is missing")

        self.manifest = _read_json(self.root / "manifest.json")
        self.provenance = _read_json(self.root / "source-provenance.json")
        self.windows_document = _read_json(self.root / "window-verification.json")
        self.ledger = self._read_ledger()
        self.artifacts = self._validate_metadata()
        self.windows = self._validate_windows()
        self._verified_frames: dict[str, pd.DataFrame] = {}

    def _read_ledger(self) -> dict[str, dict[str, Any]]:
        path = self.root / "acquisition-ledger.jsonl"
        rows: list[dict[str, Any]] = []
        try:
            for line in path.read_text(encoding="utf-8").splitlines():
                value = json.loads(line)
                if not isinstance(value, dict):
                    raise ValueError("ledger row is not an object")
                rows.append(value)
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            raise LegacyFixtureError("cannot read acquisition ledger") from exc
        if len(rows) != 8 or len({row.get("symbol") for row in rows}) != 8:
            raise LegacyFixtureError("acquisition ledger identity drift")
        return {str(row["symbol"]): row for row in rows}

    def _validate_metadata(self) -> dict[str, dict[str, Any]]:
        manifest = self.manifest
        provenance = self.provenance
        if (
            manifest.get("schema_version") != "ss.legacy-yfinance-fixture.v1"
            or manifest.get("fixture_id") != "legacy_yfinance_cn_t110"
            or manifest.get("artifact_count") != 8
            or manifest.get("ledger") != "acquisition-ledger.jsonl"
            or manifest.get("source_provenance") != "source-provenance.json"
            or manifest.get("window_verification") != "window-verification.json"
        ):
            raise LegacyFixtureError("fixture manifest identity drift")
        if (
            provenance.get("schema_version")
            != "ss.legacy-yfinance-source-provenance.v1"
            or provenance.get("yfinance_version") != "0.2.66"
            or provenance.get("call_shape")
            != 'yf.Ticker(symbol).history(period="5y", interval="1d")'
            or provenance.get("period") != "5y"
            or provenance.get("interval") != "1d"
            or provenance.get("logical_calls") != 8
            or provenance.get("concurrency") != 1
            or provenance.get("script_level_retries") != 0
            or provenance.get("normalization_applied") is not False
        ):
            raise LegacyFixtureError("fixture provenance drift")

        symbols = manifest.get("symbols")
        if symbols != provenance.get("frozen_corpus") or not isinstance(symbols, list):
            raise LegacyFixtureError("fixture symbol corpus drift")
        artifacts = manifest.get("artifacts")
        if not isinstance(artifacts, list) or len(artifacts) != 8:
            raise LegacyFixtureError("fixture artifact inventory drift")
        by_symbol = {str(item.get("symbol")): item for item in artifacts}
        if set(by_symbol) != set(symbols):
            raise LegacyFixtureError("fixture artifact symbols drift")
        for symbol in symbols:
            item = by_symbol[symbol]
            ledger = self.ledger.get(symbol)
            comparable = (
                "artifact",
                "columns",
                "dtypes",
                "first_index",
                "index_dtype",
                "index_name",
                "index_timezone",
                "last_index",
                "rows",
                "sha256",
            )
            if ledger is None or any(item.get(key) != ledger.get(key) for key in comparable):
                raise LegacyFixtureError(f"manifest/ledger drift for {symbol}")
            if (
                ledger.get("status") != "success"
                or ledger.get("period") != "5y"
                or ledger.get("interval") != "1d"
                or item.get("columns") != EXPECTED_COLUMNS
                or item.get("dtypes") != EXPECTED_DTYPES
            ):
                raise LegacyFixtureError(f"artifact metadata drift for {symbol}")
        return by_symbol

    def _validate_windows(self) -> dict[str, dict[str, Any]]:
        document = self.windows_document
        rows = document.get("symbols")
        if document.get("schema_version") != "ss.legacy-window-verification.v1":
            raise LegacyFixtureError("window metadata schema drift")
        if not isinstance(rows, list) or len(rows) != 8:
            raise LegacyFixtureError("window metadata identity drift")
        by_symbol = {str(item.get("symbol")): item for item in rows}
        if set(by_symbol) != set(self.artifacts):
            raise LegacyFixtureError("window metadata symbols drift")
        for symbol, item in by_symbol.items():
            windows = item.get("windows")
            if not isinstance(windows, dict) or set(windows) != EXPECTED_PERIODS:
                raise LegacyFixtureError(f"window periods drift for {symbol}")
            if item.get("anchor") != self.artifacts[symbol].get("last_index"):
                raise LegacyFixtureError(f"window anchor drift for {symbol}")
        return by_symbol

    def _artifact_path(self, item: dict[str, Any]) -> Path:
        relative = Path(str(item.get("artifact", "")))
        path = (self.root / relative).resolve()
        if not relative.parts or relative.is_absolute() or not path.is_relative_to(self.root):
            raise LegacyFixtureError("fixture artifact path escapes root")
        return path

    def _portable_frame(self, item: dict[str, Any], symbol: str) -> pd.DataFrame:
        portable = self._artifact_path({"artifact": item.get("portable_artifact")})
        expected_digest = str(item.get("portable_sha256", ""))
        if not expected_digest or _artifact_digest(portable) != expected_digest:
            raise LegacyFixtureError(f"portable fixture integrity drift for {symbol}")
        try:
            frame = pd.read_csv(portable, index_col="Date")
            frame.index = pd.DatetimeIndex(
                pd.to_datetime(frame.index, utc=True).tz_convert(str(item["index_timezone"])),
                name="Date",
            )
            for column, dtype in EXPECTED_DTYPES.items():
                frame[column] = frame[column].astype(dtype)
            return frame
        except Exception as exc:
            raise LegacyFixtureError(f"cannot decode portable fixture for {symbol}") from exc

    def load(self, symbol: str, period: str, interval: str) -> pd.DataFrame:
        normalized_symbol = str(symbol).strip().upper()
        if normalized_symbol not in self.artifacts:
            raise LegacyFixtureError(f"unexpected replay symbol: {normalized_symbol}")
        if period not in EXPECTED_PERIODS:
            raise LegacyFixtureError(f"unexpected replay period: {period}")
        if interval != "1d":
            raise LegacyFixtureError(f"unexpected replay interval: {interval}")

        item = self.artifacts[normalized_symbol]
        frame = self._verified_frames.get(normalized_symbol)
        if frame is None:
            path = self._artifact_path(item)
            expected_digest = str(item["sha256"])
            if self.ledger[normalized_symbol].get("sha256") != expected_digest:
                raise LegacyFixtureError("fixture integrity records disagree")
            if _artifact_digest(path) != expected_digest:
                raise LegacyFixtureError(
                    f"fixture artifact integrity drift for {normalized_symbol}"
                )
            try:
                frame = pd.read_parquet(path)
            except ImportError:
                frame = self._portable_frame(item, normalized_symbol)
            except Exception as exc:
                raise LegacyFixtureError(
                    f"cannot decode fixture artifact for {normalized_symbol}"
                ) from exc
            self._validate_frame(frame, item, normalized_symbol)
            self._verified_frames[normalized_symbol] = frame

        window = self.windows[normalized_symbol]["windows"][period]
        cutoff = pd.Timestamp(str(window["cutoff"]))
        result = frame.loc[frame.index >= cutoff]
        if (
            len(result) != window.get("rows")
            or result.empty
            or result.index[0].isoformat() != window.get("first_index")
            or result.index[-1].isoformat() != window.get("last_index")
        ):
            raise LegacyFixtureError(f"fixture window drift for {normalized_symbol} {period}")
        result.attrs.update({"volume_unit": "shares", "adjustment": "qfq"})
        return result

    @staticmethod
    def _validate_frame(frame: pd.DataFrame, item: dict[str, Any], symbol: str) -> None:
        if (
            list(frame.columns) != EXPECTED_COLUMNS
            or {column: str(dtype) for column, dtype in frame.dtypes.items()} != EXPECTED_DTYPES
            or frame.index.name != "Date"
            or str(frame.index.dtype) != item.get("index_dtype")
            or str(frame.index.tz) != item.get("index_timezone")
            or len(frame) != item.get("rows")
            or frame.empty
            or frame.index[0].isoformat() != item.get("first_index")
            or frame.index[-1].isoformat() != item.get("last_index")
            or not frame.index.is_monotonic_increasing
            or frame.index.has_duplicates
        ):
            raise LegacyFixtureError(f"fixture frame drift for {symbol}")


def install_legacy_fixture_replay(
    yfinance_class,
    *,
    expected_root: Path = CONTAINER_FIXTURE_ROOT,
) -> None:
    """Install one replay wrapper while preserving the disabled path exactly."""

    if getattr(yfinance_class, "_t111_fixture_replay_installed", False):
        return
    original_method = yfinance_class.get_historical_data
    replay: LegacyFixtureReplay | None = None

    @wraps(original_method)
    def wrapped_historical_data(self, symbol, period="1y", *args, **kwargs):
        nonlocal replay
        if os.environ.get(ENABLE_ENV, "").lower() != "true":
            return original_method(self, symbol, period, *args, **kwargs)
        if os.environ.get(RUNTIME_ENV) != "isolated-shadow":
            raise LegacyFixtureError("fixture replay requires isolated-shadow runtime")
        configured = os.environ.get(ROOT_ENV, "")
        if not configured or Path(configured).resolve() != expected_root.resolve():
            raise LegacyFixtureError("fixture replay root is not the fixed read-only root")
        interval = kwargs.get("interval", args[0] if args else "1d")
        if len(args) > 1:
            raise LegacyFixtureError("unexpected positional replay arguments")
        unexpected = set(kwargs) - {"interval", "use_cache"}
        if unexpected:
            raise LegacyFixtureError("unexpected keyword replay arguments")
        replay = replay or LegacyFixtureReplay(expected_root)
        normalized_symbol = str(symbol).strip().upper()
        if (
            normalized_symbol not in replay.artifacts
            and os.environ.get(MISSING_AS_EMPTY_ENV, "").lower() == "true"
        ):
            allowlist = {
                item.strip().upper()
                for item in os.environ.get("SS_QUANTSTACK_SHADOW_ALLOWLIST", "").split(",")
                if item.strip()
            }
            if normalized_symbol in allowlist:
                return _empty_legacy_frame()
        return replay.load(symbol, period, interval)

    yfinance_class._t111_fixture_replay_original = original_method
    yfinance_class.get_historical_data = wrapped_historical_data
    yfinance_class._t111_fixture_replay_installed = True
