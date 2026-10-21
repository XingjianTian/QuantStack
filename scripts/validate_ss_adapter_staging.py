from __future__ import annotations

import argparse
import importlib.util
import json
import os
import stat
import sys
import tempfile
import urllib.error
import urllib.request
from datetime import date
from pathlib import Path


def _load_adapter(path: Path):
    spec = importlib.util.spec_from_file_location("quantstack_consumer_adapter_probe", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("SS adapter module could not be loaded")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _request(
    base_url: str,
    api_key: str,
    method: str,
    path: str,
    payload: dict | None = None,
    etag: str | None = None,
) -> tuple[int, dict | None, dict[str, str]]:
    headers = {"Accept": "application/json", "X-Quant-API-Key": api_key}
    body = None
    if payload is not None:
        body = json.dumps(payload).encode()
        headers["Content-Type"] = "application/json"
    if etag is not None:
        headers["If-None-Match"] = etag
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}{path}", data=body, method=method, headers=headers
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            raw = response.read()
            return (
                response.status,
                json.loads(raw) if raw else None,
                dict(response.headers.items()),
            )
    except urllib.error.HTTPError as exc:
        if exc.code == 304:
            return 304, None, dict(exc.headers.items())
        raise


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--adapter", required=True, type=Path)
    parser.add_argument("--dataset-id", default="synthetic_cn_batch_benchmark")
    parser.add_argument("--dataset-version", default="5500x20-v1")
    parser.add_argument("--start", type=date.fromisoformat, default=date(2026, 7, 6))
    parser.add_argument("--end", type=date.fromisoformat, default=date(2026, 7, 31))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    api_key = os.environ.get("SS_QUANTSTACK_API_KEY", "")
    if not api_key:
        raise RuntimeError("SS_QUANTSTACK_API_KEY is required")

    metadata_status, metadata, metadata_headers = _request(
        args.base_url,
        api_key,
        "GET",
        f"/market/datasets/{args.dataset_id}/metadata?version={args.dataset_version}",
    )
    if metadata is None:
        raise RuntimeError("metadata response was empty")
    metadata_etag = next(
        value for key, value in metadata_headers.items() if key.lower() == "etag"
    )
    cached_status, _, _ = _request(
        args.base_url,
        api_key,
        "GET",
        f"/market/datasets/{args.dataset_id}/metadata?version={args.dataset_version}",
        etag=metadata_etag,
    )
    module = _load_adapter(args.adapter)
    client = module.HTTPQuantStackMarketDataClient(args.base_url, api_key)
    adapter = module.SSQuantStackConsumerAdapter(
        client,
        bars_dataset_id=args.dataset_id,
        dataset_version=args.dataset_version,
    )
    candidate = adapter.get_price_history(
        "600000.SS", start=args.start, end=args.end
    )
    legacy = candidate.frame.copy(deep=True)

    class PriceCache:
        def get_historical_data(
            self, symbol, period="2y", force_refresh=False, market=None
        ):
            return legacy

        def get_many_cached_only(self, symbols, period="2y"):
            return {symbol: legacy for symbol in symbols}

    with tempfile.TemporaryDirectory(prefix="phase23a-shadow-") as temporary:
        report_root = Path(temporary) / "shadow-reports"
        config = module.SSReadModeConfig(
            mode="shadow",
            shadow_enabled=True,
            sample_rate=1.0,
            allowlist=frozenset({"600000.SS"}),
            report_root=str(report_root),
        )
        module.install_price_cache_read_modes(
            PriceCache, adapter_factory=lambda: adapter, config=config
        )
        shadow_result = PriceCache().get_many_cached_only(["600000.SS"])
        worker = PriceCache._quantstack_shadow_worker
        if not worker.wait_for_idle(timeout_seconds=30):
            raise RuntimeError("shadow worker did not complete within the validation bound")
        report_paths = list(report_root.rglob("*.json"))
        reports = [json.loads(path.read_text(encoding="utf-8")) for path in report_paths]
        report_mode = stat.S_IMODE(report_paths[0].stat().st_mode)
        report_directory_mode = stat.S_IMODE(report_paths[0].parent.stat().st_mode)
        if not worker.shutdown():
            raise RuntimeError("shadow worker did not stop within the shutdown bound")

    class LegacyOnlyPriceCache:
        def get_historical_data(
            self, symbol, period="2y", force_refresh=False, market=None
        ):
            return legacy

    module.install_price_cache_read_modes(
        LegacyOnlyPriceCache,
        adapter_factory=lambda: (_ for _ in ()).throw(AssertionError()),
        config=module.SSReadModeConfig(),
    )
    default_legacy_identity = (
        LegacyOnlyPriceCache().get_historical_data("600000.SS") is legacy
    )
    physical_path_exposed = any(
        marker in json.dumps(metadata)
        for marker in ("/volume", "/home/", "/quant-stage", "parquet")
    )
    result = {
        "api_metadata_status": metadata_status,
        "etag_304_status": cached_status,
        "dataset_id": metadata["dataset_id"],
        "dataset_version": metadata["dataset_version"],
        "quality_status": metadata["quality_status"],
        "candidate_rows": len(candidate.frame),
        "candidate_instrument_id": candidate.metadata.instrument_id,
        "candidate_volume_unit": candidate.frame.attrs["volume_unit"],
        "default_legacy_identity": default_legacy_identity,
        "shadow_legacy_identity": shadow_result["600000.SS"] is legacy,
        "shadow_report_count": len(reports),
        "shadow_statuses": sorted({item["status"] for item in reports}),
        "shadow_candidate_errors": sum(
            item["status"] == "candidate_error" for item in reports
        ),
        "shadow_report_mode": oct(report_mode),
        "shadow_report_directory_mode": oct(report_directory_mode),
        "physical_path_exposed": physical_path_exposed,
    }
    rendered = json.dumps(result, sort_keys=True)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
