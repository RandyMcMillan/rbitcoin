#!/usr/bin/env python3
"""Hit every rbitcoin query surface on localhost with realistic calls.

One-off timing harness for `--api-log`. Talks to:

  Electrum TCP   127.0.0.1:50001
  Esplora HTTP   127.0.0.1:8080
  Core RPC HTTP  127.0.0.1:8332   (cookie {datadir}/.cookie)

Discovers tip / a real txid / a scripthash from the node, then walks every
supported method. Does **not** call RPC ``stop``. Broadcast / sendraw use
garbage hex so we time the reject path without polluting the mempool.

  python3 scripts/api-bench.py
  python3 scripts/api-bench.py --datadir ./datadir-mainnet --tweaks-count 8
  ELECTRUM=127.0.0.1:50001 ESPLORA=http://127.0.0.1:8080 python3 scripts/api-bench.py

Prints a table of surface, method, HTTP/RPC status, client wall_ms.
Compare with the node's api.jsonl ``wall_ms`` (server-side).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import socket
import sys
import time
import urllib.error
import urllib.request
from typing import Any
from urllib.parse import quote

# Mainnet genesis (display hex). Used when discovery fails.
GENESIS_HASH = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
GENESIS_TXID = "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b"
# Famous genesis P2PK wrapped as a P2PKH-looking address (Esplora will parse or 404).
GENESIS_ADDR = "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"
# Minimal consensus-invalid raw tx (deserialize may fail; still times the path).
GARBAGE_TX = "01000000000000000000"


def electrum_scripthash(script: bytes) -> str:
    """Electrum / Esplora display-order scripthash: reverse(sha256(script))."""
    return hashlib.sha256(script).digest()[::-1].hex()


def ms_since(t0: float) -> int:
    return int((time.perf_counter() - t0) * 1000)


class Results:
    def __init__(self) -> None:
        self.rows: list[tuple[str, str, str, int, str]] = []

    def add(self, surface: str, method: str, status: str, wall_ms: int, note: str = "") -> None:
        self.rows.append((surface, method, status, wall_ms, note))
        flag = "ok" if status.startswith("ok") or status == "200" else status
        extra = f"  {note}" if note else ""
        print(f"{wall_ms:6d}ms  {surface:<8} {flag:<16} {method}{extra}", flush=True)

    def summary(self) -> None:
        print()
        print(f"{'ms':>6}  {'surface':<8} {'status':<16} method")
        print("-" * 78)
        for surface, method, status, wall_ms, note in self.rows:
            extra = f"  {note}" if note else ""
            print(f"{wall_ms:6d}  {surface:<8} {status:<16} {method}{extra}")
        print(f"\n{len(self.rows)} calls")


# ── Electrum ──────────────────────────────────────────────────────────────


class Electrum:
    def __init__(self, host: str, port: int, timeout: float) -> None:
        self.host = host
        self.port = port
        self.timeout = timeout
        self.sock: socket.socket | None = None
        self._id = 0

    def connect(self) -> None:
        s = socket.create_connection((self.host, self.port), timeout=self.timeout)
        s.settimeout(self.timeout)
        self.sock = s

    def close(self) -> None:
        if self.sock is not None:
            try:
                self.sock.close()
            except OSError:
                pass
            self.sock = None

    def call(self, method: str, params: list[Any]) -> tuple[Any, int, str]:
        if self.sock is None:
            raise RuntimeError("not connected")
        self._id += 1
        req = json.dumps({"jsonrpc": "2.0", "id": self._id, "method": method, "params": params})
        t0 = time.perf_counter()
        self.sock.sendall((req + "\n").encode())
        buf = b""
        while b"\n" not in buf:
            chunk = self.sock.recv(1 << 20)
            if not chunk:
                raise ConnectionError("electrum EOF")
            buf += chunk
        wall = ms_since(t0)
        msg = json.loads(buf.split(b"\n", 1)[0])
        if "error" in msg and msg["error"] is not None:
            err = msg["error"]
            if isinstance(err, dict):
                return None, wall, f"err:{err.get('message', err)}"
            return None, wall, f"err:{err}"
        return msg.get("result"), wall, "ok"


def run_electrum(r: Results, host: str, port: int, timeout: float, tweaks_count: int) -> None:
    el = Electrum(host, port, timeout)
    try:
        t0 = time.perf_counter()
        el.connect()
        r.add("electrum", "(connect)", "ok", ms_since(t0))
    except OSError as e:
        r.add("electrum", "(connect)", "down", 0, str(e))
        return

    def go(method: str, params: list[Any], note: str = "") -> Any:
        try:
            result, wall, status = el.call(method, params)
        except Exception as e:
            r.add("electrum", method, "exc", 0, str(e)[:80])
            return None
        extra = note
        if status == "ok" and isinstance(result, dict) and method.endswith("tweaks.subscribe"):
            extra = extra or f"heights={len(result)}"
        r.add("electrum", method, status, wall, extra)
        return result

    try:
        go("server.version", ["rbitcoin-api-bench", "1.4"])
        go("server.ping", [])
        go("server.banner", [])
        go("server.donation_address", [])
        feat = go("server.features", [])
        go("server.peers.subscribe", [])

        tip = go("blockchain.headers.subscribe", [])
        tip_h = 0
        if isinstance(tip, dict):
            tip_h = int(tip.get("height") or 0)
        go("blockchain.block.header", [tip_h])
        go("blockchain.block.headers", [max(0, tip_h - 9), 10])

        # Cake probe + a live window + a pre-Taproot empty height.
        go("blockchain.tweaks.subscribe", [0, 1, False], "probe")
        go("blockchain.tweaks.subscribe", [709632, 1, False], "taproot act")
        start = max(0, tip_h - max(0, tweaks_count - 1))
        go("blockchain.tweaks.subscribe", [start, tweaks_count, False], f"tip window n={tweaks_count}")
        go("blockchain.tweaks.subscribe", [tip_h, 1, True], "historicalMode")

        txid = go("blockchain.transaction.id_from_pos", [tip_h, 0])
        if not isinstance(txid, str) or len(txid) != 64:
            txid = GENESIS_TXID
            tip_for_merkle = 0
        else:
            tip_for_merkle = tip_h
        go("blockchain.transaction.get", [txid])
        go("blockchain.transaction.get", [txid, True], "verbose")
        go("blockchain.transaction.get_merkle", [txid, tip_for_merkle])
        go("blockchain.transaction.id_from_pos", [0, 0], "genesis")

        empty_sh = electrum_scripthash(b"\x00")
        op_true_sh = electrum_scripthash(bytes([0x51]))
        for sh, label in ((empty_sh, "empty-script"), (op_true_sh, "OP_TRUE")):
            go("blockchain.scripthash.get_balance", [sh], label)
            go("blockchain.scripthash.listunspent", [sh], label)
            go("blockchain.scripthash.get_mempool", [sh], label)
            go("blockchain.scripthash.subscribe", [sh], label)
            go("blockchain.scripthash.get_history", [sh], label)
            go("blockchain.scripthash.get_history", [sh, 0, -1], f"{label} window")

        go("blockchain.estimatefee", [2])
        go("blockchain.estimatefee", [6])
        go("blockchain.relayfee", [])
        go("mempool.get_fee_histogram", [])
        go("blockchain.transaction.broadcast", [GARBAGE_TX], "garbage (expect err)")
        go("no.such.method", [], "expect unknown")
        _ = feat
    finally:
        el.close()


# ── Esplora ───────────────────────────────────────────────────────────────


def http_get(url: str, timeout: float) -> tuple[int, bytes, int]:
    t0 = time.perf_counter()
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            return resp.status, body, ms_since(t0)
    except urllib.error.HTTPError as e:
        _ = e.read()
        return e.code, b"", ms_since(t0)
    except Exception:
        return 0, b"", ms_since(t0)


def http_post(url: str, data: bytes, timeout: float, content_type: str) -> tuple[int, bytes, int]:
    t0 = time.perf_counter()
    req = urllib.request.Request(
        url, data=data, method="POST", headers={"Content-Type": content_type}
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            return resp.status, body, ms_since(t0)
    except urllib.error.HTTPError as e:
        _ = e.read()
        return e.code, b"", ms_since(t0)
    except Exception:
        return 0, b"", ms_since(t0)


def run_esplora(r: Results, base: str, timeout: float) -> None:
    base = base.rstrip("/")

    def get(path: str, note: str = "") -> bytes:
        code, body, wall = http_get(base + path, timeout)
        status = str(code) if code else "down"
        r.add("esplora", f"GET {path}", status, wall, note)
        return body

    def post(path: str, data: bytes, ctype: str, note: str = "") -> None:
        code, _body, wall = http_post(base + path, data, timeout, ctype)
        status = str(code) if code else "down"
        r.add("esplora", f"POST {path}", status, wall, note)

    tip_h_raw = get("/blocks/tip/height")
    tip_hash_raw = get("/blocks/tip/hash")
    try:
        tip_h = int(tip_h_raw.decode().strip())
    except ValueError:
        tip_h = 0
    tip_hash = tip_hash_raw.decode().strip() or GENESIS_HASH
    if len(tip_hash) != 64:
        tip_hash = GENESIS_HASH

    get("/blocks")
    get(f"/blocks/{max(0, tip_h - 5)}")
    get(f"/block-height/{tip_h}")
    get(f"/block/{tip_hash}")
    get(f"/block/{tip_hash}/header")
    get(f"/block/{tip_hash}/status")
    get(f"/block/{tip_hash}/raw")
    txids_raw = get(f"/block/{tip_hash}/txids")
    get(f"/block/{tip_hash}/txid/0")
    get(f"/block/{tip_hash}/txs")
    get(f"/block/{tip_hash}/txs/0")

    txid = GENESIS_TXID
    try:
        txids = json.loads(txids_raw.decode() or "[]")
        if isinstance(txids, list) and txids:
            txid = str(txids[0])
    except json.JSONDecodeError:
        pass

    tx_json = get(f"/tx/{txid}")
    get(f"/tx/{txid}/hex")
    get(f"/tx/{txid}/raw")
    get(f"/tx/{txid}/status")
    get(f"/tx/{txid}/merkle-proof")
    get(f"/tx/{txid}/merkleblock-proof")
    get(f"/tx/{txid}/outspend/0")
    get(f"/tx/{txid}/outspends")

    addr = GENESIS_ADDR
    try:
        obj = json.loads(tx_json.decode() or "{}")
        for vout in obj.get("vout") or []:
            a = vout.get("scriptpubkey_address")
            if isinstance(a, str) and a:
                addr = a
                break
    except json.JSONDecodeError:
        pass

    aq = quote(addr, safe="")
    get(f"/address/{aq}")
    get(f"/address/{aq}/utxo")
    get(f"/address/{aq}/txs")
    get(f"/address/{aq}/txs/mempool")
    chain = get(f"/address/{aq}/txs/chain")
    last = None
    try:
        page = json.loads(chain.decode() or "[]")
        if isinstance(page, list) and page:
            last = page[-1].get("txid")
    except json.JSONDecodeError:
        pass
    if last:
        get(f"/address/{aq}/txs/chain/{last}")

    sh = electrum_scripthash(bytes([0x51]))
    get(f"/scripthash/{sh}")
    get(f"/scripthash/{sh}/utxo")
    get(f"/scripthash/{sh}/txs")
    get(f"/scripthash/{sh}/txs/mempool")
    sh_chain = get(f"/scripthash/{sh}/txs/chain")
    sh_last = None
    try:
        page = json.loads(sh_chain.decode() or "[]")
        if isinstance(page, list) and page:
            sh_last = page[-1].get("txid")
    except json.JSONDecodeError:
        pass
    if sh_last:
        get(f"/scripthash/{sh}/txs/chain/{sh_last}")

    get("/mempool")
    get("/mempool/txids")
    get("/mempool/recent")
    get("/fee-estimates")
    post("/tx", GARBAGE_TX.encode(), "text/plain", "garbage hex")
    post("/txs/package", b"[]", "application/json", "empty package")


# ── RPC ───────────────────────────────────────────────────────────────────


def load_cookie(datadir: str, user: str | None, password: str | None) -> tuple[str, str] | None:
    if user is not None:
        return user, password or ""
    for name in (".cookie",):
        p = os.path.join(datadir, name)
        if os.path.isfile(p):
            raw = open(p, encoding="utf-8").read().strip()
            if ":" in raw:
                u, pw = raw.split(":", 1)
                return u, pw
    return None


def run_rpc(
    r: Results,
    url: str,
    datadir: str,
    timeout: float,
    user: str | None,
    password: str | None,
) -> None:
    auth = load_cookie(datadir, user, password)
    if auth is None:
        r.add("rpc", "(auth)", "skip", 0, f"no cookie in {datadir} and no --rpcuser")
        # Still try unauthenticated so a down/up bind is visible.
        code, _, wall = http_post(
            url,
            b'{"jsonrpc":"1.0","id":"1","method":"getblockcount","params":[]}',
            timeout,
            "application/json",
        )
        r.add("rpc", "getblockcount", str(code) if code else "down", wall, "no auth")
        return

    import base64

    token = base64.b64encode(f"{auth[0]}:{auth[1]}".encode()).decode()

    def call(method: str, params: list[Any], note: str = "") -> Any:
        body = json.dumps({"jsonrpc": "1.0", "id": "bench", "method": method, "params": params})
        t0 = time.perf_counter()
        req = urllib.request.Request(
            url,
            data=body.encode(),
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Basic {token}",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                wall = ms_since(t0)
                msg = json.loads(raw.decode())
        except urllib.error.HTTPError as e:
            _ = e.read()
            r.add("rpc", method, str(e.code), ms_since(t0), note)
            return None
        except Exception as e:
            r.add("rpc", method, "down", ms_since(t0), str(e)[:80])
            return None
        if msg.get("error"):
            err = msg["error"]
            if isinstance(err, dict):
                r.add("rpc", method, f"err:{err.get('code')}", wall, note or str(err.get("message", ""))[:60])
            else:
                r.add("rpc", method, "err", wall, note)
            return None
        r.add("rpc", method, "ok", wall, note)
        return msg.get("result")

    call("help", [])
    call("help", ["getblockchaininfo"])
    call("getrpcinfo", [])
    call("uptime", [])
    info = call("getblockchaininfo", [])
    count = call("getblockcount", [])
    best = call("getbestblockhash", [])
    tip_h = 0
    if isinstance(count, int):
        tip_h = count
    elif isinstance(info, dict):
        tip_h = int(info.get("blocks") or 0)
    best_s = best if isinstance(best, str) else GENESIS_HASH
    call("getblockhash", [tip_h])
    call("getblockhash", [0], "genesis")
    call("getblockheader", [best_s])
    call("getblockheader", [best_s, False], "hex")
    call("getblock", [best_s, 1], "verbosity=1")
    call("getblock", [GENESIS_HASH, 0], "genesis raw")
    call("getdifficulty", [])
    call("getnetworkinfo", [])
    call("getconnectioncount", [])
    call("getpeerinfo", [])
    call("getmempoolinfo", [])
    mem = call("getrawmempool", [])
    call("getrawmempool", [True], "verbose")
    sample = None
    if isinstance(mem, list) and mem:
        sample = str(mem[0])
        call("getmempoolentry", [sample])
    else:
        call("getmempoolentry", [GENESIS_TXID], "expect miss")
    call("getrawtransaction", [GENESIS_TXID])
    call("getrawtransaction", [GENESIS_TXID, True], "verbose")
    call("decoderawtransaction", [GARBAGE_TX], "garbage")
    call("decodescript", ["51"])
    call("validateaddress", [GENESIS_ADDR])
    call("estimatesmartfee", [2])
    call("estimatesmartfee", [6])
    call("testmempoolaccept", [[GARBAGE_TX]])
    call("sendrawtransaction", [GARBAGE_TX], "garbage (expect err)")
    # Intentionally omit `stop`.


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--electrum", default=os.environ.get("ELECTRUM", "127.0.0.1:50001"))
    p.add_argument("--esplora", default=os.environ.get("ESPLORA", "http://127.0.0.1:8080"))
    p.add_argument("--rpc", default=os.environ.get("RPC", "http://127.0.0.1:8332"))
    p.add_argument("--datadir", default=os.environ.get("DATADIR", "./datadir-mainnet"))
    p.add_argument("--rpcuser", default=os.environ.get("RPCUSER"))
    p.add_argument("--rpcpassword", default=os.environ.get("RPCPASSWORD"))
    p.add_argument("--timeout", type=float, default=120.0, help="per-call socket timeout (s)")
    p.add_argument("--tweaks-count", type=int, default=1, help="Electrum tweaks count (server caps at 8)")
    p.add_argument("--skip-electrum", action="store_true")
    p.add_argument("--skip-esplora", action="store_true")
    p.add_argument("--skip-rpc", action="store_true")
    args = p.parse_args()

    r = Results()
    if not args.skip_electrum:
        host, _, port_s = args.electrum.rpartition(":")
        host = host or "127.0.0.1"
        port = int(port_s or "50001")
        print(f"# electrum {host}:{port}", flush=True)
        run_electrum(r, host, port, args.timeout, max(1, args.tweaks_count))
    if not args.skip_esplora:
        print(f"# esplora {args.esplora}", flush=True)
        run_esplora(r, args.esplora, args.timeout)
    if not args.skip_rpc:
        print(f"# rpc {args.rpc}", flush=True)
        run_rpc(r, args.rpc, args.datadir, args.timeout, args.rpcuser, args.rpcpassword)
    r.summary()
    return 0


if __name__ == "__main__":
    sys.exit(main())
