#!/usr/bin/env python3
"""Test-only JSON-RPC proxy in front of rbitcoin-node.

Not the operator product. Core functional tests speak to this process.
Node methods are forwarded unchanged. Wallet/utility methods are handled
locally in later steps.
"""

from __future__ import annotations

import base64
import json
import threading
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable


# GBT longpoll can sit ~80s; stay under Core's client-side patience.
FORWARD_TIMEOUT_S = 180.0


class RpcError(Exception):
    def __init__(self, code: int, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class RpcProxy:
    """HTTP JSON-RPC server that forwards to an internal rbitcoin-node."""

    def __init__(
        self,
        listen: tuple[str, int],
        node_url: str,
        cookie_line: Callable[[], str | None],
    ) -> None:
        self.node_url = node_url.rstrip("/") + "/"
        self.cookie_line = cookie_line
        self._handlers: dict[str, Callable[[Any], dict[str, Any]]] = {}
        proxy = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, _fmt: str, *_args: object) -> None:
                return

            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(length) if length else b""
                auth = self.headers.get("Authorization", "")
                status, body = proxy.handle_http(raw, auth)
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self._httpd = ThreadingHTTPServer(listen, Handler)
        self._thread: threading.Thread | None = None

    def register(self, method: str, fn: Callable[[Any], dict[str, Any]]) -> None:
        self._handlers[method] = fn

    def start(self) -> None:
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)
        self._thread.start()

    def shutdown(self) -> None:
        self._httpd.shutdown()
        if self._thread is not None:
            self._thread.join(timeout=2)

    def handle_http(self, raw: bytes, authorization: str) -> tuple[int, bytes]:
        cookie = self.cookie_line()
        if cookie:
            want = "Basic " + base64.b64encode(cookie.encode()).decode()
            if authorization != want:
                return 401, b'{"error":"unauthorized"}\n'
        try:
            payload = json.loads(raw.decode() or "null")
        except (UnicodeDecodeError, json.JSONDecodeError):
            return self.forward_raw(raw)
        if isinstance(payload, list):
            return self.forward_raw(raw)
        if isinstance(payload, dict):
            method = payload.get("method")
            if isinstance(method, str) and method in self._handlers:
                return 200, json.dumps(self._one(payload)).encode()
        return self.forward_raw(raw)

    def forward_raw(self, raw: bytes) -> tuple[int, bytes]:
        cookie = self.cookie_line() or ""
        tok = base64.b64encode(cookie.encode()).decode()
        req = urllib.request.Request(
            self.node_url,
            data=raw,
            headers={
                "Authorization": f"Basic {tok}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=FORWARD_TIMEOUT_S) as resp:
                return resp.status, resp.read()
        except urllib.error.HTTPError as e:
            return e.code, e.read() if e.fp else b""
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            body = json.dumps(
                {
                    "result": None,
                    "error": {"code": -28, "message": f"Loading... ({e})"},
                    "id": None,
                }
            ).encode()
            return 200, body

    def _one(self, item: Any) -> dict[str, Any]:
        if not isinstance(item, dict):
            return {
                "result": None,
                "error": {"code": -32600, "message": "Invalid request"},
                "id": None,
            }
        req_id = item.get("id")
        method = item.get("method")
        params = item.get("params", [])
        if not isinstance(method, str):
            return {
                "result": None,
                "error": {"code": -32600, "message": "Invalid request"},
                "id": req_id,
            }
        local = self._handlers.get(method)
        if local is not None:
            try:
                result = local(params)
            except RpcError as e:
                return {
                    "result": None,
                    "error": {"code": e.code, "message": e.message},
                    "id": req_id,
                }
            except Exception as e:  # noqa: BLE001 — surface as RPC error
                return {
                    "result": None,
                    "error": {"code": -1, "message": str(e)},
                    "id": req_id,
                }
            if isinstance(result, dict) and "error" in result and "result" in result:
                result.setdefault("id", req_id)
                return result
            return {"result": result, "error": None, "id": req_id}
        return self.forward(item)

    def forward(self, item: dict[str, Any]) -> dict[str, Any]:
        cookie = self.cookie_line() or ""
        tok = base64.b64encode(cookie.encode()).decode()
        body = json.dumps(item).encode()
        req = urllib.request.Request(
            self.node_url,
            data=body,
            headers={
                "Authorization": f"Basic {tok}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=FORWARD_TIMEOUT_S) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as e:
            raw = e.read()
            try:
                return json.loads(raw.decode())
            except (UnicodeDecodeError, json.JSONDecodeError):
                return {
                    "result": None,
                    "error": {"code": -1, "message": f"HTTP {e.code}"},
                    "id": item.get("id"),
                }
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            # Core wait_for_rpc_connection retries -28 / -342 only. A
            # forwarded "node not listening yet" must look like warmup, not
            # a fatal -1 (restart_node races the proxy vs rbitcoin-node).
            return {
                "result": None,
                "error": {
                    "code": -28,
                    "message": f"Loading... ({e})",
                },
                "id": item.get("id"),
            }
        try:
            parsed = json.loads(raw.decode())
        except (UnicodeDecodeError, json.JSONDecodeError):
            return {
                "result": None,
                "error": {"code": -1, "message": "node returned non-JSON"},
                "id": item.get("id"),
            }
        if isinstance(parsed, dict):
            return parsed
        return {
            "result": None,
            "error": {"code": -1, "message": "node returned non-object"},
            "id": item.get("id"),
        }


def _offset_port(public_rpc: int, offset: int) -> int:
    """Shift a Core-assigned RPC port; wrap instead of overflowing 65535."""
    p = public_rpc + offset
    if p <= 65535:
        return p
    p = public_rpc - offset
    if p >= 1:
        return p
    return max(1, public_rpc - 1)


def node_rpc_port(public_rpc: int) -> int:
    """Internal node RPC. Public port stays on the proxy."""
    return _offset_port(public_rpc, 10_000)


def esplora_port(public_rpc: int) -> int:
    """Esplora listen for the test wallet shim (Step 18).

    Must not sit next to ``node_rpc_port``: Core assigns consecutive
    ``-rpcport`` values, so ``node_rpc(n) + 1 == node_rpc(n + 1)``. That
    collision made the next node's proxy POST ``getblockcount`` at the
    previous node's Esplora (HTTP 404).
    """
    return _offset_port(public_rpc, 20_000)
