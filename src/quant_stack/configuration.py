"""Render private deployment files from an explicitly loaded environment."""

from __future__ import annotations

import argparse
import json
import os
import re
import tempfile
from collections.abc import Mapping
from pathlib import Path
from string import Template


def _write_private(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor, name = tempfile.mkstemp(dir=path.parent, prefix=".quant-config-")
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(text)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def render_json_config(source: Path, target: Path, environment: Mapping[str, str]) -> None:
    if source.resolve() == target.resolve():
        raise ValueError("runtime output must differ from the checked-in template")

    def expand(value: object) -> object:
        if isinstance(value, str):
            template = Template(value)
            for name in template.get_identifiers():
                if not environment.get(name):
                    raise ValueError(f"missing environment variable: {name}")
            return template.substitute(environment)
        if isinstance(value, list):
            return [expand(item) for item in value]
        if isinstance(value, dict):
            return {key: expand(item) for key, item in value.items()}
        return value

    payload = expand(json.loads(source.read_text(encoding="utf-8")))
    _write_private(target, json.dumps(payload, ensure_ascii=False, indent=2) + "\n")


def render_gateway(source: Path, target: Path, environment: Mapping[str, str]) -> None:
    if target.resolve().is_relative_to(source.resolve()):
        raise ValueError("gateway output must be outside the checked-in examples")

    def host(name: str) -> str:
        value = environment.get(name, "")
        if not re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?", value):
            raise ValueError(f"invalid hostname in {name}")
        return value

    def port(name: str, default: str) -> str:
        value = environment.get(name, default)
        if not value.isascii() or not value.isdigit() or not 1 <= int(value) <= 65535:
            raise ValueError(f"invalid port in {name}")
        return str(int(value))

    domain = host("QUANT_PUBLIC_DOMAIN")
    upstream = host("QUANT_GATEWAY_UPSTREAM_HOST")
    public_port = port("QUANT_PUBLIC_PORT", "11443")
    ports = {
        old: port(name, old)
        for old, name in {
            "18900": "QUANT_API_PUBLISHED_PORT", "18920": "QUANT_CENTER_PUBLISHED_PORT",
            "18899": "QUANT_VIBE_PUBLISHED_PORT", "18910": "QUANT_SCREENER_PUBLISHED_PORT",
            "18930": "QUANT_NAUTILUS_PUBLISHED_PORT", "18081": "QUANT_LEARN_PUBLISHED_PORT",
            "18082": "QUANT_RAGFLOW_PUBLISHED_PORT", "18083": "QUANT_RAGGRAPH_PUBLISHED_PORT",
        }.items()
    }
    rendered = {}
    for path in source.rglob("*"):
        if not path.is_file() or path.suffix not in {
            ".conf", ".lua", ".inc", ".html", ".css", ".js",
        }:
            continue
        text = path.read_text(encoding="utf-8")
        text = text.replace(".example.com", "." + domain).replace(":11443", ":" + public_port)
        text = re.sub(r"10\.0\.0\.10:(\d+)", lambda match: upstream + ":" + ports[match[1]], text)
        rendered[path.relative_to(source)] = text
    target.mkdir(parents=True, exist_ok=True, mode=0o700)
    target.chmod(0o700)
    for relative, text in rendered.items():
        _write_private(target / relative, text)


def render_secret(name: str, target: Path, environment: Mapping[str, str]) -> None:
    value = environment.get(name, "")
    if not re.fullmatch(r"[A-Za-z0-9._~-]{32,}", value):
        raise ValueError(f"{name} requires a token of at least 32 supported characters")
    _write_private(target, value + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Load .env with uv run --env-file before rendering",
    )
    parser.add_argument("kind", choices=["json", "gateway", "secret"])
    parser.add_argument("source", type=Path)
    parser.add_argument("target", type=Path)
    args = parser.parse_args()
    try:
        if args.kind == "json":
            render_json_config(args.source, args.target, os.environ)
        elif args.kind == "gateway":
            render_gateway(args.source, args.target, os.environ)
        else:
            render_secret(str(args.source), args.target, os.environ)
    except ValueError as error:
        parser.exit(2, f"configuration error: {error}\n")
    print("Private configuration rendered; values are not printed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
