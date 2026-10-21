#!/usr/bin/env python3
"""Create the idempotent starter watchlist used for workspace acceptance."""

from __future__ import annotations

import argparse
import json
import os
import sys
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
    parser.add_argument("--name", default="研究候选")
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

        _, listing = request_json(opener, f"{base_url}/api/v1/user-watchlists/")
        watchlists = listing.get("watchlists", [])
        watchlist = next(
            (item for item in watchlists if item.get("name") == args.name),
            None,
        )
        created_watchlist = watchlist is None
        if watchlist is None:
            _, watchlist = request_json(
                opener,
                f"{base_url}/api/v1/user-watchlists/",
                payload={
                    "name": args.name,
                    "description": "量化工作台验收与人工研究候选",
                    "color": "#F97316",
                },
            )

        watchlist_id = int(watchlist["id"])
        _, data = request_json(
            opener,
            f"{base_url}/api/v1/user-watchlists/{watchlist_id}/data",
        )
        symbols = {item.get("symbol") for item in data.get("items", [])}
        added_symbol = args.symbol not in symbols
        if added_symbol:
            request_json(
                opener,
                f"{base_url}/api/v1/user-watchlists/{watchlist_id}/items",
                payload={
                    "symbol": args.symbol,
                    "display_name": "贵州茅台",
                    "notes": "平台验收样本；不构成投资建议",
                },
            )
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        print(f"Seeding failed: HTTP {exc.code}: {body[:300]}", file=sys.stderr)
        return 1
    except (URLError, TimeoutError, json.JSONDecodeError) as exc:
        print(f"Seeding failed: {exc}", file=sys.stderr)
        return 1

    print(
        json.dumps(
            {
                "watchlist_id": watchlist_id,
                "watchlist_name": args.name,
                "created_watchlist": created_watchlist,
                "symbol": args.symbol,
                "added_symbol": added_symbol,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
