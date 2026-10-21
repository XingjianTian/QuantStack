from __future__ import annotations

import json
import stat
from pathlib import Path

import pytest

from quant_stack.configuration import render_gateway, render_json_config, render_secret

ROOT = Path(__file__).parents[1]


def test_json_templates_preserve_types_and_do_not_execute_values(tmp_path: Path) -> None:
    source = tmp_path / "template.json"
    source.write_text(json.dumps({"root": "${QUANT_DATA_ROOT}", "nested": ["${KEY}"], "n": 3}))
    target = tmp_path / "runtime.local.json"
    value = 'a "quoted" path/$(touch SHOULD_NOT_EXIST)'
    render_json_config(source, target, {"QUANT_DATA_ROOT": value, "KEY": "fixture-secret"})
    assert json.loads(target.read_text()) == {"root": value, "nested": ["fixture-secret"], "n": 3}
    assert stat.S_IMODE(target.stat().st_mode) == 0o600
    assert not (tmp_path / "SHOULD_NOT_EXIST").exists()


def test_missing_configuration_does_not_write_or_disclose_values(tmp_path: Path) -> None:
    source = tmp_path / "template.json"
    source.write_text('{"secret":"${KEY}","root":"${MISSING}"}')
    target = tmp_path / "runtime.local.json"
    with pytest.raises(ValueError, match="MISSING") as error:
        render_json_config(source, target, {"KEY": "fixture-secret"})
    assert "fixture-secret" not in str(error.value)
    assert not target.exists()


def test_gateway_uses_environment_and_keeps_secrets_out_of_browser_files(tmp_path: Path) -> None:
    output = tmp_path / "gateway"
    render_gateway(ROOT / "docker/openresty", output, {
        "QUANT_PUBLIC_DOMAIN": "fixture.invalid",
        "QUANT_PUBLIC_PORT": "14443",
        "QUANT_GATEWAY_UPSTREAM_HOST": "192.0.2.123",
        "QUANT_API_PUBLISHED_PORT": "19900",
        "QUANT_API_KEY": "fixture-api-secret",
    })
    auth = (output / "sudox-platform-auth.lua").read_text()
    assert 'https://auth.fixture.invalid:14443' in auth
    assert 'https://stock-screener.fixture.invalid:14443/' in auth
    assert "server 192.0.2.123:19900;" in (output / "12-upstream.conf").read_text()
    assert "server_name auth.fixture.invalid;" in (output / "16.conf").read_text()
    assert "form-action https://quant.fixture.invalid:14443" in (output / "16.conf").read_text()
    assert "Secure; HttpOnly; SameSite=Lax" in auth
    for path in output.rglob("*"):
        if path.is_file():
            assert "fixture-api-secret" not in path.read_text()
    assert stat.S_IMODE(output.stat().st_mode) == 0o700


@pytest.mark.parametrize("variable,value", [
    ("QUANT_PUBLIC_DOMAIN", 'bad.invalid"; return 200; #'),
    ("QUANT_GATEWAY_UPSTREAM_HOST", "localhost;"),
    ("QUANT_PUBLIC_PORT", "70000"),
])
def test_gateway_rejects_config_injection_before_writing(
    tmp_path: Path, variable: str, value: str,
) -> None:
    target = tmp_path / "gateway"
    environment = {
        "QUANT_PUBLIC_DOMAIN": "fixture.invalid",
        "QUANT_GATEWAY_UPSTREAM_HOST": "127.0.0.1", variable: value,
    }
    with pytest.raises(ValueError, match=variable):
        render_gateway(ROOT / "docker/openresty", target, environment)
    assert not target.exists()


def test_api_secret_is_written_only_to_private_runtime_file(tmp_path: Path) -> None:
    target = tmp_path / "secrets/quant-api-key"
    render_secret("QUANT_API_KEY", target, {"QUANT_API_KEY": "synthetic-token-" * 3})
    assert target.read_text() == "synthetic-token-" * 3 + "\n"
    assert stat.S_IMODE(target.stat().st_mode) == 0o600
    with pytest.raises(ValueError, match="QUANT_API_KEY"):
        render_secret("QUANT_API_KEY", target, {"QUANT_API_KEY": ""})
