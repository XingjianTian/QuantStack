from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).parents[1]
OPENRESTY = ROOT / "docker" / "openresty"
SERVICES = {
    "quant": "12.conf",
    "vibe": "13.conf",
    "stock": "14.conf",
    "nautilus": "15.conf",
    "learn": "17.conf",
    "ragflow": "18.conf",
    "raggraph": "19.conf",
}


def read_openresty(name: str) -> str:
    return (OPENRESTY / name).read_text(encoding="utf-8")


def test_all_browser_uis_use_the_unified_session_gateway() -> None:
    for service, filename in SERVICES.items():
        config = read_openresty(filename)
        assert 'require("sudox-platform-auth")' in config
        assert f'require_session("{service}")' in config
        assert f"set $sudox_platform_service {service};" in config
        assert "include sudox-platform-browser-session.inc" in config

    gateway = read_openresty("16.conf")
    assert "server_name auth.example.com" in gateway
    assert "sudox-platform.htpasswd" in gateway
    assert "limit_req zone=sudox_platform_login" in gateway
    assert "limit_req_status 429" in gateway
    assert "WWW-Authenticate" in gateway
    assert "Content-Security-Policy" in gateway
    assert "return 302 /login/quant" in gateway


def test_api_authentication_contracts_remain_separate() -> None:
    quant = read_openresty("12.conf")
    ragflow = read_openresty("18.conf")
    raggraph = read_openresty("19.conf")

    quant_api = quant.split("location /api/", 1)[1].split("location /", 1)[0]
    assert 'auth_basic "Quant Center"' in quant_api
    assert "quant-platform.htpasswd" in quant_api

    ragflow_api = ragflow.split("location /api/", 1)[1].split("location /", 1)[0]
    assert "auth_basic off" in ragflow_api
    assert "sudox-platform-auth.lua" not in ragflow_api

    raggraph_api = raggraph.split("location /api/", 1)[1].split("location /", 1)[0]
    assert "include raggraph-api-compat.conf" in raggraph_api
    assert "$http_cookie" not in raggraph_api
    assert "$http_authorization" not in raggraph_api


def test_session_cookie_is_host_only_service_bound_and_rotatable() -> None:
    script = read_openresty("sudox-platform-auth.lua")

    assert 'TOKEN_VERSION = "v3"' in script
    assert "ngx.hmac_sha1" in script
    assert "secure_equal" in script
    assert "Domain=" not in script
    assert "Secure; HttpOnly; SameSite=Lax" in script
    assert "SESSION_TTL = 8 * 60 * 60" in script
    assert "HANDOFF_TTL = 30" in script
    assert 'issue_cookie(COOKIE_NAME, "browser", service)' in script
    assert 'issue_cookie(GATEWAY_COOKIE_NAME, "gateway", "auth")' in script
    assert 'validate_token(args.ticket, "handoff", service' in script
    assert "sudox_platform_handoffs" in script
    assert "sudox-platform-session-secret.previous" in script
    assert "SERVICE_TARGETS" in script
    assert "arg_redirect" not in script
    assert "request_uri" not in script


def test_login_page_uses_external_assets_and_fixed_destinations() -> None:
    html = read_openresty("platform-login/index.html")
    javascript = read_openresty("platform-login/login.js")
    stylesheet = read_openresty("platform-login/login.css")

    assert '<script src="/login.js" defer></script>' in html
    assert "<script>" not in html
    assert 'autocomplete="username"' in html
    assert 'autocomplete="current-password"' in html
    assert 'aria-live="polite"' in html
    assert "Authorization: basicValue" in javascript
    assert "window.fetch(`/handoff/${service}`" in javascript
    assert "payload.consume !== expectedConsume" in javascript
    assert "handoff.method = 'POST'" in javascript
    assert "ticket.value = payload.ticket" in javascript
    assert "handoff.submit()" in javascript
    assert "parentDomain" in javascript
    assert "https://stock-screener.example.com" not in javascript
    assert "safeErrors.has(error.message)" in javascript
    assert "location.search" not in javascript
    assert "location.hash" not in javascript
    assert "prefers-reduced-motion" in stylesheet


def test_lua_session_lifecycle_and_service_allowlist() -> None:
    luajit = shutil.which("luajit") or shutil.which("lua")
    if not luajit:
        pytest.skip("Lua runtime is unavailable")
    result = subprocess.run(
        [
            luajit,
            str(ROOT / "tests" / "fixtures" / "openresty_auth_harness.lua"),
            str(OPENRESTY / "sudox-platform-auth.lua"),
            str(OPENRESTY / "quant-platform-auth.lua"),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    assert result.stdout.strip() == "openresty auth harness: PASS"


def test_legacy_auth_module_remains_available_during_staged_migration() -> None:
    legacy = read_openresty("quant-platform-auth.lua")
    assert "function M.set_session_cookie()" in legacy
    assert "function M.require_session(bridge_path)" in legacy
    assert "function M.issue_and_redirect(target)" in legacy
    assert 'return require("sudox-platform-auth")' not in legacy


def test_nautilus_lab_is_an_independent_compose_service() -> None:
    compose = yaml.safe_load((ROOT / "docker" / "compose.nas.yaml").read_text(encoding="utf-8"))
    service = compose["services"]["nautilus-lab-web"]

    assert service["image"] == "personal/nautilus-lab-web:0.4.0"
    assert service["ports"] == [
        "${QUANT_BIND_ADDRESS:-127.0.0.1}:${QUANT_NAUTILUS_PUBLISHED_PORT:-18930}:8080",
    ]
    assert service["read_only"] is True
    assert "quant_api_key" in service["secrets"]
    assert "nautilus_lab_12" in read_openresty("12-upstream.conf")
