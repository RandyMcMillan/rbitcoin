#!/usr/bin/env bash
# Pin Core validateaddress error / error_locations dialect (rpc_invalid_address_message).
set -euo pipefail
cd "$(dirname "$0")"
ROOT="$(cd ../.. && pwd)"
export PYTHONPATH="${ROOT}/third_party/bitcoin/test/functional:${PWD}${PYTHONPATH:+:${PYTHONPATH}}"
python3 - <<'PY'
from rpc_proxy import RpcError
from rpc_util import validateaddress

ERR_MISC = -1
ERR_TYPE = -3

INVALID = [
    (
        "bcrt1s0xlxvlhemja6c4dqv22uapctqupfhlxm9h8z3k2e72q4k9hcz7v8n0nx0muaewav25430mtr",
        "Invalid Bech32 address program size (41 bytes)",
        [],
    ),
    (
        "bc1pw508d6qejxtdg4y5r3zarvary0c5xw7kw508d6qejxtdg4y5r3zarvary0c5xw7k7grplx",
        "Invalid or unsupported Segwit (Bech32) or Base58 encoding.",
        [],
    ),
    (
        "bcrt1p0xlxvlhemja6c4dqv22uapctqupfhlxm9h8z3k2e72q4k9hcz7vqdmchcc",
        "Version 1+ witness address must use Bech32m checksum",
        [],
    ),
    (
        "bcrt1qw508d6qejxtdg4y5r3zarvary0c5xw7k35mrzd",
        "Version 0 witness address must use Bech32 checksum",
        [],
    ),
    (
        "bcrt130xlxvlhemja6c4dqv22uapctqupfhlxm9h8z3k2e72q4k9hcz7vqynjegk",
        "Invalid Bech32 address witness version",
        [],
    ),
    (
        "bcrt1qw508d6qejxtdg4y5r3zarvary0c5xw7kqqq5k3my",
        "Invalid Bech32 v0 address program size (21 bytes), per BIP141",
        [],
    ),
    (
        "bcrt1q049edschfnwystcqnsvyfpj23mpsg3jcedq9xv049edschfnwystcqnsvyfpj23mpsg3jcedq9xv049edschfnwystcqnsvyfpj23m",
        "Bech32 string too long",
        list(range(90, 108)),
    ),
    (
        "bcrt1q049edschfnwystcqnsvyfpj23mpsg3jcedq9xv",
        "Invalid Bech32 checksum",
        [9],
    ),
    (
        "bcrt1qax9suht3qv95sw33xavx8crpxduefdrsvgsklu",
        "Invalid Bech32 checksum",
        [22, 43],
    ),
    (
        "BCRT1QPLMTZKC2XHARPPZDLNPAQL78RSHJ68U32RAH7R",
        "Invalid Bech32 checksum",
        [38],
    ),
    ("bcrtq049ldschfnwystcqnsvyfpj23mpsg3jcedq9xv", "Missing separator", []),
    (
        "bcrt1q04oldschfnwystcqnsvyfpj23mpsg3jcedq9xv",
        "Invalid Base 32 character",
        [8],
    ),
    (
        "bcrt1qdg3myrgvzw7ml8q0ejxhlkyxn7vl9r56yzkfgvzclrf4hkpx9yfqhpsuks",
        "Invalid Bech32 checksum",
        [19, 30],
    ),
    (
        "bcrt1ptmp74ayg7p24uslctssvjm06q5phz4yrxucgnv",
        "Invalid Bech32 checksum",
        [5],
    ),
    (
        "17VZNX1SN5NtKa8UQFxwQbFeFc3iqRYhem",
        "Invalid or unsupported Base58-encoded address.",
        [],
    ),
    (
        "mipcBbFg9gMiCh81Kj8tqqdgoZub1ZJJfn",
        "Invalid checksum or length of Base58 address (P2PKH or P2SH)",
        [],
    ),
    (
        "2VKf7XKMrp4bVNVmuRbyCewkP8FhGLP2E54LHDPakr9Sq5mtU2",
        "Invalid checksum or length of Base58 address (P2PKH or P2SH)",
        [],
    ),
    (
        "asfah14i8fajz0123f",
        "Invalid or unsupported Segwit (Bech32) or Base58 encoding.",
        [],
    ),
    (
        "1q049ldschfnwystcqnsvyfpj23mpsg3jcedq9xv",
        "Invalid or unsupported Segwit (Bech32) or Base58 encoding.",
        [],
    ),
]

VALID = [
    "bcrt1qtmp74ayg7p24uslctssvjm06q5phz4yrxucgnv",
    "bcrt1p424qxxyd0r",
    "BCRT1QPLMTZKC2XHARPPZDLNPAQL78RSHJ68U33RAH7R",
    "bcrt1qdg3myrgvzw7ml9q0ejxhlkyxm7vl9r56yzkfgvzclrf4hkpx9yfqhpsuks",
    "mipcBbFg9gMiCh81Kj8tqqdgoZub1ZJRfn",
]

for addr, err, locs in INVALID:
    res = validateaddress([addr])
    assert res["isvalid"] is False, (addr, res)
    assert res["error"] == err, (addr, res.get("error"), err)
    assert res["error_locations"] == locs, (addr, res.get("error_locations"), locs)

for addr in VALID:
    res = validateaddress([addr])
    assert res["isvalid"] is True, (addr, res)
    assert "error" not in res, res
    assert "error_locations" not in res, res

try:
    validateaddress([])
    raise SystemExit("expected missing-arg RpcError")
except RpcError as e:
    assert e.code == ERR_MISC, e.code
    assert "Return information about the given bitcoin address." in e.message, e.message

try:
    validateaddress([None])
    raise SystemExit("expected null-type RpcError")
except RpcError as e:
    assert e.code == ERR_TYPE, e.code
    assert e.message == "JSON value of type null is not of expected type string", e.message

# getaddressinfo raises the same DecodeDestination error string (wallet section).
from core_dest import decode_destination
from rpc_wallet import WalletHub

ERR_INVALID_ADDRESS = -5
detail, _err, _locs = decode_destination("bcrt1p424qxxyd0r")
assert detail is not None and "isscript" not in detail and detail["iswitness"] is True

class _FakeProxy:
    def register(self, *a, **k):
        return None

hub = WalletHub(_FakeProxy(), "http://127.0.0.1:9")
try:
    hub.getaddressinfo(
        [
            "bcrt1s0xlxvlhemja6c4dqv22uapctqupfhlxm9h8z3k2e72q4k9hcz7v8n0nx0muaewav25430mtr"
        ]
    )
    raise SystemExit("expected getaddressinfo RpcError")
except RpcError as e:
    assert e.code == ERR_INVALID_ADDRESS, e.code
    assert e.message == "Invalid Bech32 address program size (41 bytes)", e.message

print("rpc_util_validateaddress: ok")
PY
