#!/usr/bin/env python3
"""Run an idempotent, one-symbol CN acceptance scan and verify its result."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from http.cookiejar import CookieJar
from urllib.error import HTTPError, URLError
from urllib.request import HTTPCookieProcessor, Request, build_opener


def request_json(opener, url: str, *, payload: dict | None = None):
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = Request(url, data=data, headers=headers)
    with opener.open(request, timeout=30) as response:
        return response.status, json.load(response)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:18910")
    parser.add_argument("--symbol", default="600519.SS")
    parser.add_argument("--timeout", type=int, default=300)
    args = parser.parse_args()

    password = os.environ.get("QUANT_WORKSPACE_PASSWORD", "")
    if not password:
        print("QUANT_WORKSPACE_PASSWORD is required", file=sys.stderr)
        return 2

    base_url = args.base_url.rstrip("/")
    opener = build_opener(HTTPCookieProcessor(CookieJar()))
    try:
        _, auth = request_json(
            opener,
            f"{base_url}/api/v1/auth/login",
            payload={"password": password},
        )
        if auth.get("authenticated") is not True:
            print("Authentication was not accepted", file=sys.stderr)
            return 1

        _, created = request_json(
            opener,
            f"{base_url}/api/v1/scans",
            payload={
                "universe": "custom",
                "symbols": [args.symbol],
                "screeners": ["minervini"],
                "composite_method": "weighted_average",
                "idempotency_key": "quant-workspace-cn-smoke-v1",
            },
        )
        scan_id = created["scan_id"]
        deadline = time.monotonic() + args.timeout
        scan_status = created.get("status")
        status_payload = created
        while scan_status not in {"completed", "failed", "cancelled"}:
            if time.monotonic() >= deadline:
                print(f"Acceptance scan timed out: {scan_id}", file=sys.stderr)
                return 1
            time.sleep(2)
            _, status_payload = request_json(
                opener,
                f"{base_url}/api/v1/scans/{scan_id}/status",
            )
            scan_status = status_payload.get("status")

        if scan_status != "completed":
            print(f"Acceptance scan ended with status {scan_status}", file=sys.stderr)
            return 1

        _, results = request_json(
            opener,
            f"{base_url}/api/v1/scans/{scan_id}/results"
            "?include_sparklines=false&per_page=10",
        )
        rows = results.get("results", [])
        row = next((item for item in rows if item.get("symbol") == args.symbol), None)
        if row is None:
            print("Acceptance symbol is missing from scan results", file=sys.stderr)
            return 1
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        print(f"Acceptance scan failed: HTTP {exc.code}: {body[:500]}", file=sys.stderr)
        return 1
    except (URLError, TimeoutError, json.JSONDecodeError) as exc:
        print(f"Acceptance scan failed: {exc}", file=sys.stderr)
        return 1

    print(
        json.dumps(
            {
                "scan_id": scan_id,
                "status": scan_status,
                "total_stocks": status_payload.get("total_stocks"),
                "completed_stocks": status_payload.get("completed_stocks"),
                "result_symbol": row.get("symbol"),
                "rating": row.get("rating"),
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
