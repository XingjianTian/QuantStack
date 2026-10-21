"""Generate deterministic synthetic OHLCV; never fetch or copy market prices."""

from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests/fixtures/stock_screener/legacy_yfinance_cn_t110"


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n")


def generate(root: Path = FIXTURE) -> None:
    dates = json.loads((root / "calendar.json").read_text())
    symbols = json.loads((root / "symbols.json").read_text())
    index = pd.DatetimeIndex(pd.to_datetime(dates), name="Date").tz_localize("Asia/Shanghai")
    (root / "raw").mkdir(exist_ok=True)
    (root / "portable").mkdir(exist_ok=True)
    artifacts, ledger, windows = [], [], []
    for number, symbol in enumerate(symbols):
        baseline = [20.0 + number * 10 + (i % 100) / 10 for i in range(len(index))]
        frame = pd.DataFrame({
            "Open": baseline,
            "High": [v + 1.0 for v in baseline],
            "Low": [v - 1.0 for v in baseline],
            "Close": [v + 0.25 for v in baseline],
            "Volume": [1000 + i * 10 for i in range(len(index))],
            "Dividends": [0.0] * len(index),
            "Stock Splits": [0.0] * len(index),
        }, index=index)
        name = symbol.replace(".", "_")
        raw = root / "raw" / (name + ".parquet")
        portable = root / "portable" / (name + ".csv.gz")
        frame.to_parquet(raw)
        portable.write_bytes(gzip.compress(frame.to_csv().encode(), mtime=0))
        item = {
            "symbol": symbol, "artifact": str(raw.relative_to(root)),
            "portable_artifact": str(portable.relative_to(root)),
            "sha256": hashlib.sha256(raw.read_bytes()).hexdigest(),
            "portable_sha256": hashlib.sha256(portable.read_bytes()).hexdigest(),
            "columns": list(frame.columns),
            "dtypes": {column: str(dtype) for column, dtype in frame.dtypes.items()},
            "first_index": index[0].isoformat(), "last_index": index[-1].isoformat(),
            "index_dtype": str(index.dtype), "index_name": "Date",
            "index_timezone": "Asia/Shanghai", "rows": len(frame),
        }
        artifacts.append(item)
        ledger.append({**item, "status": "success", "period": "5y", "interval": "1d",
                       "logical_call": number + 1, "synthetic": True, "provider_calls": 0})
        periods = {}
        for period, months in {"6mo": 6, "1y": 12, "2y": 24, "5y": 60}.items():
            cutoff = index[-1] - pd.DateOffset(months=months)
            selected = index[index >= cutoff]
            periods[period] = {"cutoff": cutoff.isoformat(), "rows": len(selected),
                               "first_index": selected[0].isoformat(),
                               "last_index": selected[-1].isoformat()}
        windows.append({"symbol": symbol, "anchor": index[-1].isoformat(), "windows": periods})
    write_json(root / "manifest.json", {
        "schema_version": "ss.legacy-yfinance-fixture.v1",
        "fixture_id": "legacy_yfinance_cn_t110", "synthetic": True, "provider_calls": 0,
        "adjustment_semantics": "Synthetic replay data; qfq is a compatibility label only.",
        "artifact_count": len(artifacts), "artifacts": artifacts, "symbols": symbols,
        "ledger": "acquisition-ledger.jsonl", "source_provenance": "source-provenance.json",
        "window_verification": "window-verification.json",
    })
    write_json(root / "source-provenance.json", {
        "schema_version": "ss.legacy-yfinance-source-provenance.v1",
        "synthetic": True, "provider_calls": 0,
        "generator": "scripts/generate_public_replay_fixture.py",
        "notice": (
            "Version and call-shape fields are replay compatibility labels, "
            "not evidence of a real yfinance acquisition. All OHLCV are generated."
        ),
        "yfinance_version": "0.2.66",
        "call_shape": 'yf.Ticker(symbol).history(period="5y", interval="1d")',
        "period": "5y", "interval": "1d", "logical_calls": 8, "concurrency": 1,
        "script_level_retries": 0, "normalization_applied": False, "frozen_corpus": symbols,
    })
    write_json(root / "window-verification.json", {
        "schema_version": "ss.legacy-window-verification.v1", "synthetic": True,
        "symbols": windows,
    })
    (root / "acquisition-ledger.jsonl").write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in ledger),
    )


if __name__ == "__main__":
    generate()
