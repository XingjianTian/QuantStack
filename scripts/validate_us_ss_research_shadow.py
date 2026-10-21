from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import os
import sys
from datetime import date
from pathlib import Path
from urllib.parse import urlencode

import pandas as pd

from quant_stack.api import create_app
from quant_stack.data.storage import StagingFileSystemStorage
from quant_stack.platform import PlatformRepository


class ASGIResponse:
    def __init__(self, status_code: int, body: bytes) -> None:
        self.status_code = status_code
        self.body = body

    def json(self):
        return json.loads(self.body)


class InProcessASGIClient:
    def __init__(self, app, *, api_key: str) -> None:
        self.app = app
        self.api_key = api_key

    async def _request(self, method: str, path: str, *, params=None, payload=None):
        body = b"" if payload is None else json.dumps(payload).encode()
        sent_request = False
        response_status = 500
        response_body = bytearray()

        async def receive():
            nonlocal sent_request
            if sent_request:
                return {"type": "http.disconnect"}
            sent_request = True
            return {"type": "http.request", "body": body, "more_body": False}

        async def send(message):
            nonlocal response_status
            if message["type"] == "http.response.start":
                response_status = int(message["status"])
            elif message["type"] == "http.response.body":
                response_body.extend(message.get("body", b""))

        headers = [(b"x-quant-api-key", self.api_key.encode())]
        if payload is not None:
            headers.append((b"content-type", b"application/json"))
        query_string = urlencode(params or {}).encode()
        await self.app(
            {
                "type": "http",
                "asgi": {"version": "3.0", "spec_version": "2.3"},
                "http_version": "1.1",
                "method": method,
                "scheme": "http",
                "path": path,
                "raw_path": path.encode(),
                "query_string": query_string,
                "headers": headers,
                "client": ("127.0.0.1", 1),
                "server": ("isolated", 80),
            },
            receive,
            send,
        )
        return ASGIResponse(response_status, bytes(response_body))

    def post(self, path: str, *, json=None):
        return asyncio.run(self._request("POST", path, payload=json))

    def get(self, path: str, *, params=None):
        return asyncio.run(self._request("GET", path, params=params))


class InProcessBatchClient:
    def __init__(self, client: InProcessASGIClient) -> None:
        self.client = client

    def _response(self, response):
        if response.status_code != 200:
            detail = response.body.decode(errors="replace")[:500]
            raise RuntimeError(
                f"Batch API returned HTTP {response.status_code}: {detail}"
            )
        return response.json()

    def fetch_instruments(self, payload):
        return self._response(self.client.post("/market/batch/instruments", json=payload))

    def fetch_bars(self, payload):
        return self._response(self.client.post("/market/batch/bars", json=payload))

    def get_dataset_metadata(self, dataset_id, version="latest"):
        return self._response(
            self.client.get(
                f"/market/datasets/{dataset_id}/metadata", params={"version": version}
            )
        )


def _adapter_module(path: Path):
    spec = importlib.util.spec_from_file_location("us_quantstack_research_adapter", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("US research adapter cannot be loaded")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _comparison_passes(
    report: dict[str, object],
    *,
    expected_date: date,
    max_difference_bps: float,
) -> bool:
    kline_bps = report.get("kline_close_max_difference_bps")
    sma20_bps = report.get("sma20_max_difference_bps")
    expected_watermark = expected_date.isoformat()
    return (
        int(report.get("shared_rows", 0)) > 20
        and report.get("legacy_latest_date") == expected_watermark
        and report.get("candidate_latest_date") == expected_watermark
        and isinstance(kline_bps, (int, float))
        and float(kline_bps) <= max_difference_bps
        and isinstance(sma20_bps, (int, float))
        and float(sma20_bps) <= max_difference_bps
        and report.get("scan_predicate_equal") is True
        and report.get("research_only") is True
        and report.get("production_path_changed") is False
    )


def run(args: argparse.Namespace) -> dict[str, object]:
    os.environ["QUANT_API_KEY"] = args.api_key
    os.environ["QUANT_BATCH_CONSUMER_ACCESS"] = "shadow"
    repository = PlatformRepository(
        args.repository_root, args.data_root / "experiments", args.data_root
    )
    api = InProcessASGIClient(
        create_app(repository, data_storage=StagingFileSystemStorage(args.data_root)),
        api_key=args.api_key,
    )
    module = _adapter_module(args.adapter)
    adapter = module.USQuantStackResearchAdapter(
        InProcessBatchClient(api),
        bars_dataset_id=args.dataset_id,
        dataset_version=args.dataset_version,
    )
    provider = pd.read_parquet(args.provider_shadow)
    reports: dict[str, dict[str, object]] = {}
    for symbol in args.symbols:
        rows = provider.loc[provider["symbol"] == symbol].copy()
        if rows.empty:
            raise ValueError(f"provider shadow is missing {symbol}")
        index = pd.to_datetime(rows.pop("trading_date")).dt.tz_localize(
            "America/New_York"
        )
        rows.index = index
        legacy = rows.rename(
            columns={
                "open": "Open",
                "high": "High",
                "low": "Low",
                "close": "Close",
                "volume": "Volume",
            }
        )[["Open", "High", "Low", "Close", "Volume"]]
        candidate = adapter.get_price_history(symbol, start=args.start, end=args.end)
        reports[symbol] = module.compare_us_research_view(legacy, candidate)

    passed = all(
        _comparison_passes(
            report,
            expected_date=args.end,
            max_difference_bps=args.max_difference_bps,
        )
        for report in reports.values()
    )
    result = {
        "schema_version": "quantstack.us-ss-research-shadow-acceptance.v1",
        "status": "passed" if passed else "blocked",
        "dataset_id": args.dataset_id,
        "dataset_version": args.dataset_version,
        "symbols": list(args.symbols),
        "batch_api": "in_process_real_routes",
        "stock_screener_adapter": "us_quantstack_research_adapter",
        "research_only": True,
        "production_path_changed": False,
        "max_difference_bps": args.max_difference_bps,
        "comparisons": reports,
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if not passed:
        raise RuntimeError("US SS research shadow acceptance did not pass")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate the US SS research shadow")
    parser.add_argument("--repository-root", type=Path, required=True)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--adapter", type=Path, required=True)
    parser.add_argument("--provider-shadow", type=Path, required=True)
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--dataset-version", required=True)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--symbols", nargs="+", required=True)
    parser.add_argument("--max-difference-bps", type=float, default=1.0)
    parser.add_argument("--api-key", default="isolated-us-shadow")
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(run(args), sort_keys=True))


if __name__ == "__main__":
    main()
