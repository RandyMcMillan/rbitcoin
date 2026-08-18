#!/usr/bin/env bash
# Pin Core ScriptToAsmStr for OP_0 / empty push (rpc_decodescript.py multisig scriptSig).
set -euo pipefail
cd "$(dirname "$0")"
ROOT="$(cd ../.. && pwd)"
export PYTHONPATH="${ROOT}/third_party/bitcoin/test/functional:${PWD}${PYTHONPATH:+:${PYTHONPATH}}"
python3 - <<'PY'
from test_framework.script import CScript, OP_0, OP_1
from rpc_util import _script_asm

sig = bytes.fromhex(
    "304502207fa7a6d1e0ee81132a269ad84e68d695483745cde8b541e3bf630749894e342a"
    "022100c1f7ab20e13e22fb95281a870f3dcf38d782e53023ee313d741ad0cfbc0c509001"
)
# OP_0 <sig> <sig> — Core asm is "0 <sig> <sig>"
asm = _script_asm(CScript([OP_0, sig, sig]), attempt_sighash=False)
assert asm == f"0 {sig.hex()} {sig.hex()}", asm
# OP_1 OP_0 — Core "1 0"
assert _script_asm(CScript([OP_1, OP_0])) == "1 0"
# Non-sig push starting with 0x30 must not get [ALL] (OP_RETURN-like).
from rpc_util import _push_asm
odd = bytes.fromhex("3011020701010101010101020601010101010101")
assert _push_asm(odd, attempt_sighash=True) == odd.hex(), _push_asm(odd, True)
# Valid DER+sighash does get [ALL].
good = bytes.fromhex(
    "304402207174775824bec6c2700023309a168231ec80b82c6069282f5133e6f11cbb0446"
    "0220570edc55c7c5da2ca687ebd0372d3546ebc3f810516a002350cac72dfe192dfb01"
)
assert _push_asm(good, True).endswith("[ALL]"), _push_asm(good, True)
# Core decodescript can_wrap: OP_RETURN unspendable / OP_RESERVED / OP_CHECKSIGADD skip wrap.
from rpc_util import _decode_script
for hx in ("6aee", "6a02ee", "ba", "50"):
    d = _decode_script(bytes.fromhex(hx))
    assert "p2sh" not in d and "segwit" not in d, (hx, d)
# Wrappable nonstandard still gets p2sh+segwit.
d = _decode_script(bytes.fromhex("02eeee"))
assert "p2sh" in d and "segwit" in d, d
print("rpc_util_asm: ok")
PY
