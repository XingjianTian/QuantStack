#!/usr/bin/env python3
"""Read-only health and CN-symbol verification for the Quant Workspace."""

from __future__ import annotations

import argparse
import json
import os
import sys
from http.cookiejar import CookieJar
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import HTTPCookieProcessor, Request, build_opener


def request_json(opener, base_url: str, path: str, *, payload: dict | None = None):
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = Request(f"{base_url.rstrip('/')}{path}", data=data, headers=headers)
    with opener.open(request, timeout=20) as response:
        return response.status, json.load(response)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:18910")
    parser.add_argument("--symbol", default="600519.SS")
    parser.add_argument(
        "--require-ready",
        action="store_true",
        help="Fail unless bootstrap is ready and cached price history is available.",
    )
    args = parser.parse_args()

    password = os.environ.get("QUANT_WORKSPACE_PASSWORD", "")
    if not password:
        print("QUANT_WORKSPACE_PASSWORD is required", file=sys.stderr)
        return 2

    opener = build_opener(HTTPCookieProcessor(CookieJar()))
    checks: dict[str, object] = {}
    try:
        status, auth = request_json(
            opener,
            args.base_url,
            "/api/v1/auth/login",
            payload={"password": password},
        )
        checks["authentication"] = status == 200 and auth.get("authenticated") is True

        status, capabilities = request_json(opener, args.base_url, "/api/v1/app-capabilities")
        checks["api"] = status == 200
        checks["primary_market"] = capabilities.get("primary_market")
        checks["enabled_markets"] = capabilities.get("enabled_markets")
        checks["bootstrap_state"] = capabilities.get("bootstrap_state")
        checks["features"] = capabilities.get("features")

        status, activity = request_json(opener, args.base_url, "/api/v1/runtime/activity")
        bootstrap = activity.get("bootstrap", {}) if isinstance(activity, dict) else {}
        checks["runtime_activity"] = {
            "http_status": status,
            "state": bootstrap.get("state"),
            "app_ready": bootstrap.get("app_ready"),
            "current_stage": bootstrap.get("current_stage"),
            "percent": bootstrap.get("percent"),
            "queue_state": bootstrap.get("queue_state"),
        }

        query = urlencode({"q": args.symbol.split(".", 1)[0], "limit": 20})
        status, results = request_json(opener, args.base_url, f"/api/v1/stocks/search?{query}")
        symbols = [row.get("symbol") for row in results if isinstance(row, dict)]
        checks["symbol_search"] = args.symbol in symbols
        checks["matched_symbols"] = symbols

        history_status = None
        history_points = 0
        try:
            history_status, history = request_json(
                opener,
                args.base_url,
                f"/api/v1/stocks/{args.symbol}/history?period=1mo",
            )
            history_points = len(history) if isinstance(history, list) else 0
        except HTTPError as exc:
            history_status = exc.code
        checks["price_history"] = {
            "http_status": history_status,
            "points": history_points,
            "available": history_status == 200 and history_points > 0,
        }
    except HTTPError as exc:
        print(f"HTTP verification failed: {exc.code} {exc.reason}", file=sys.stderr)
        return 1
    except (URLError, TimeoutError, json.JSONDecodeError) as exc:
        print(f"Workspace verification failed: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(checks, ensure_ascii=False, indent=2, sort_keys=True))
    required = [checks["authentication"], checks["api"], checks["symbol_search"]]
    if args.require_ready:
        features = checks.get("features")
        if not isinstance(features, dict):
            features = {}
        required.extend(
            (
                checks["bootstrap_state"] == "ready",
                checks["runtime_activity"]["app_ready"] is True,
                checks["price_history"]["available"] is True,
                checks["primary_market"] == "CN",
                checks["enabled_markets"] == ["CN"],
                features.get("chatbot") is False,
                features.get("themes") is False,
            )
        )
    return 0 if all(required) else 1


if __name__ == "__main__":
    raise SystemExit(main())
