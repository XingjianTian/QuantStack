#!/usr/bin/env python3
"""Expose a loopback HTTP proxy only on a selected Docker bridge address."""

from __future__ import annotations

import os
import socket
import socketserver
import threading

BUFFER_SIZE = 64 * 1024


class RelayHandler(socketserver.BaseRequestHandler):
    @staticmethod
    def _pump(source: socket.socket, destination: socket.socket) -> None:
        try:
            while chunk := source.recv(BUFFER_SIZE):
                destination.sendall(chunk)
        finally:
            try:
                destination.shutdown(socket.SHUT_WR)
            except OSError:
                pass

    def handle(self) -> None:
        upstream_host = os.environ.get("PROXY_RELAY_UPSTREAM_HOST", "127.0.0.1")
        upstream_port = int(os.environ.get("PROXY_RELAY_UPSTREAM_PORT", "7890"))
        with socket.create_connection((upstream_host, upstream_port), timeout=15) as upstream:
            upstream.settimeout(None)
            uplink = threading.Thread(
                target=self._pump,
                args=(self.request, upstream),
                daemon=True,
            )
            uplink.start()
            self._pump(upstream, self.request)
            uplink.join(timeout=1)


class RelayServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


def main() -> None:
    bind_host = os.environ.get("PROXY_RELAY_BIND_ADDRESS", "127.0.0.1")
    bind_port = int(os.environ.get("PROXY_RELAY_BIND_PORT", "7891"))
    with RelayServer((bind_host, bind_port), RelayHandler) as server:
        server.serve_forever()


if __name__ == "__main__":
    main()
