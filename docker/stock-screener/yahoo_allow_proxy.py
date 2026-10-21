"""Tiny T-078-only CONNECT/HTTP proxy with an exact Yahoo hostname allowlist."""

from __future__ import annotations

import json
import os
import select
import socket
import socketserver
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

MAX_HEADER_BYTES = 64 * 1024


def _allowed_hosts() -> frozenset[str]:
    return frozenset(
        item.strip().lower()
        for item in os.environ.get("T078_PROXY_ALLOWED_HOSTS", "").split(",")
        if item.strip()
    )


class ConnectionBudget:
    def __init__(self, *, maximum: int, minimum_interval_seconds: float, ledger: Path) -> None:
        if maximum < 1 or minimum_interval_seconds < 0:
            raise ValueError("invalid proxy connection budget")
        self.maximum = maximum
        self.minimum_interval_seconds = minimum_interval_seconds
        self.ledger = ledger
        self._lock = threading.Lock()
        self._count = 0
        if ledger.is_file():
            self._count = sum(1 for line in ledger.read_text(encoding="utf-8").splitlines() if line)
        if self._count > self.maximum:
            raise ValueError("existing proxy ledger exceeds the configured budget")
        self._last_started = 0.0

    def acquire(self, *, host: str, port: int) -> int | None:
        with self._lock:
            if self._count >= self.maximum:
                return None
            wait_seconds = self.minimum_interval_seconds - (time.monotonic() - self._last_started)
            if wait_seconds > 0:
                time.sleep(wait_seconds)
            self._count += 1
            self._last_started = time.monotonic()
            self.ledger.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            with self.ledger.open("a", encoding="utf-8") as stream:
                stream.write(
                    json.dumps(
                        {
                            "connection": self._count,
                            "host": host,
                            "port": port,
                            "started_at": datetime.now(UTC).isoformat(),
                        },
                        sort_keys=True,
                    )
                    + "\n"
                )
            self.ledger.chmod(0o600)
            return self._count


CONNECTION_BUDGET: ConnectionBudget | None = None


def _target(request_line: str, headers: bytes) -> tuple[str, int, bytes | None]:
    method, raw_target, _ = request_line.split(" ", maxsplit=2)
    if method.upper() == "CONNECT":
        host, separator, raw_port = raw_target.rpartition(":")
        if not separator or not raw_port.isdigit():
            raise ValueError("invalid CONNECT target")
        return host.lower(), int(raw_port), None
    parsed = urlsplit(raw_target)
    host = (parsed.hostname or "").lower()
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    if not host:
        raise ValueError("absolute proxy URL required")
    origin_target = parsed.path or "/"
    if parsed.query:
        origin_target += f"?{parsed.query}"
    first_line = f"{method} {origin_target} HTTP/1.1\r\n".encode()
    return host, port, first_line + headers


class ProxyHandler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        self.request.settimeout(10)
        received = bytearray()
        while b"\r\n\r\n" not in received and len(received) <= MAX_HEADER_BYTES:
            chunk = self.request.recv(4096)
            if not chunk:
                return
            received.extend(chunk)
        if len(received) > MAX_HEADER_BYTES:
            self.request.sendall(b"HTTP/1.1 431 Request Header Fields Too Large\r\n\r\n")
            return
        head, _, remainder = bytes(received).partition(b"\r\n")
        try:
            request_line = head.decode("ascii")
            host, port, forwarded_head = _target(request_line, remainder)
        except (UnicodeDecodeError, ValueError):
            self.request.sendall(b"HTTP/1.1 400 Bad Request\r\n\r\n")
            return
        if host not in _allowed_hosts() or port not in {80, 443}:
            self.request.sendall(b"HTTP/1.1 403 Forbidden\r\n\r\n")
            return
        if CONNECTION_BUDGET is None or CONNECTION_BUDGET.acquire(host=host, port=port) is None:
            self.request.sendall(b"HTTP/1.1 429 Too Many Requests\r\n\r\n")
            return
        try:
            upstream = socket.create_connection((host, port), timeout=10)
        except OSError:
            self.request.sendall(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
            return
        with upstream:
            if forwarded_head is None:
                self.request.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            else:
                upstream.sendall(forwarded_head)
            sockets = [self.request, upstream]
            while True:
                readable, _, exceptional = select.select(sockets, [], sockets, 30)
                if exceptional or not readable:
                    return
                for current in readable:
                    data = current.recv(64 * 1024)
                    if not data:
                        return
                    (upstream if current is self.request else self.request).sendall(data)


class SerialProxy(socketserver.TCPServer):
    allow_reuse_address = True


def main() -> None:
    global CONNECTION_BUDGET
    if not _allowed_hosts():
        raise RuntimeError("T078 proxy allowlist is empty")
    bind = os.environ.get("T078_PROXY_BIND", "0.0.0.0")
    port = int(os.environ.get("T078_PROXY_PORT", "3128"))
    CONNECTION_BUDGET = ConnectionBudget(
        maximum=int(os.environ.get("T078_PROXY_MAX_CONNECTIONS", "100")),
        minimum_interval_seconds=float(
            os.environ.get("T078_PROXY_MIN_INTERVAL_SECONDS", "1")
        ),
        ledger=Path(os.environ.get("T078_PROXY_LEDGER", "/evidence/external-fetch-ledger.jsonl")),
    )
    with SerialProxy((bind, port), ProxyHandler) as server:
        server.serve_forever()


if __name__ == "__main__":
    main()
