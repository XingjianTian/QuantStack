#!/usr/bin/env python3
"""Start the explicit CN-only first-run bootstrap for Quant Workspace."""

from __future__ import annotations

import argparse
import json
import os
import sys
from http.cookiejar import CookieJar
from urllib.error import HTTPError, URLError
from urllib.request import HTTPCookieProcessor, Request, build_opener


def post_json(opener, url: str, payload: dict) -> tuple[int, dict]:
    request = Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Accept": "application/json", "Content-Type": "application/json"},
    )
    with opener.open(request, timeout=30) as response:
        return response.status, json.load(response)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:18910")
    args = parser.parse_args()

    password = os.environ.get("QUANT_WORKSPACE_PASSWORD", "")
    if not password:
        print("QUANT_WORKSPACE_PASSWORD is required", file=sys.stderr)
        return 2

    base_url = args.base_url.rstrip("/")
    opener = build_opener(HTTPCookieProcessor(CookieJar()))
    try:
        _, auth = post_json(
            opener,
            f"{base_url}/api/v1/auth/login",
            {"password": password},
        )
        if auth.get("authenticated") is not True:
            print("Authentication was not accepted", file=sys.stderr)
            return 1

        status, payload = post_json(
            opener,
            f"{base_url}/api/v1/runtime/bootstrap",
            {"primary_market": "CN", "enabled_markets": ["CN"]},
        )
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        if exc.code == 409 and "bootstrap_already_running" in body:
            print(json.dumps({"status": "already_running"}, indent=2))
            return 0
        print(f"Bootstrap request failed: HTTP {exc.code}", file=sys.stderr)
        return 1
    except (URLError, TimeoutError, json.JSONDecodeError) as exc:
        print(f"Bootstrap request failed: {exc}", file=sys.stderr)
        return 1

    safe = {
        "http_status": status,
        "bootstrap_state": payload.get("bootstrap_state"),
        "primary_market": payload.get("primary_market"),
        "enabled_markets": payload.get("enabled_markets"),
        "task_queued": bool(payload.get("task_id")),
    }
    print(json.dumps(safe, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if status == 200 and safe["task_queued"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
