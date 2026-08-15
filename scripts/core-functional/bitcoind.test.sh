#!/usr/bin/env bash
# Contract pin for the test-only bitcoind shim (argv / conf; no cargo).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SHIM="$ROOT/scripts/core-functional/bitcoind"
PASS=0
FAIL=0

assert_ok() {
  local name="$1"
  shift
  if "$@" >/dev/null 2>&1; then
    echo "ok - $name"
    PASS=$((PASS + 1))
  else
    echo "not ok - $name"
    FAIL=$((FAIL + 1))
  fi
}

assert_fail_msg() {
  local name="$1"
  local needle="$2"
  shift 2
  local out
  if out="$("$@" 2>&1)"; then
    echo "not ok - $name (expected failure)"
    FAIL=$((FAIL + 1))
    return
  fi
  if printf '%s' "$out" | grep -q -- "$needle"; then
    echo "ok - $name"
    PASS=$((PASS + 1))
  else
    echo "not ok - $name (missing '$needle' in: $out)"
    FAIL=$((FAIL + 1))
  fi
}

WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/rbitcoin-cf-shim.XXXXXX")"
cleanup() { rm -rf "$WORKDIR"; }
trap cleanup EXIT

# Fake node binary so --print-cmd does not require a cargo build.
FAKE="$WORKDIR/rbitcoin-node"
printf '#!/bin/sh\nexit 0\n' >"$FAKE"
chmod +x "$FAKE"
export RBITCOIN_NODE="$FAKE"

DATADIR="$WORKDIR/node0"
mkdir -p "$DATADIR"
cat >"$DATADIR/bitcoin.conf" <<'EOF'
regtest=1
[regtest]
port=18444
rpcport=18443
server=1
EOF

OUT="$("$SHIM" --print-cmd -datadir="$DATADIR" -regtest -debug 2>/dev/null)"
if printf '%s' "$OUT" | grep -q -- "--network regtest" \
  && printf '%s' "$OUT" | grep -q -- "--datadir ${DATADIR}/regtest" \
  && printf '%s' "$OUT" | grep -q -- "--rpc-listen 127.0.0.1:18443" \
  && printf '%s' "$OUT" | grep -q -- "--listen 127.0.0.1:18444" \
  && printf '%s' "$OUT" | grep -q -- "--no-seeds" \
  && printf '%s' "$OUT" | grep -q -- "--log-level debug"; then
  echo "ok - print-cmd maps conf + -debug"
  PASS=$((PASS + 1))
else
  echo "not ok - print-cmd maps conf + -debug (got: $OUT)"
  FAIL=$((FAIL + 1))
fi

# CLI -rpcport / -port override conf; -v2transport=0 ignored (we force v2).
OUT2="$("$SHIM" --print-cmd -datadir="$DATADIR" -regtest \
  -rpcport=19111 -port=19222 -v2transport=0 -disablewallet -server \
  -uacomment=testnode0 2>/dev/null)"
if printf '%s' "$OUT2" | grep -q -- "--rpc-listen 127.0.0.1:19111" \
  && printf '%s' "$OUT2" | grep -q -- "--listen 127.0.0.1:19222"; then
  echo "ok - CLI ports override conf"
  PASS=$((PASS + 1))
else
  echo "not ok - CLI ports override conf (got: $OUT2)"
  FAIL=$((FAIL + 1))
fi

# -bind=0.0.0.0:P supplies the P2P port; we still listen on 127.0.0.1.
OUT3="$("$SHIM" --print-cmd -datadir="$DATADIR" -regtest \
  -bind=0.0.0.0:19333 -bind=127.0.0.1:19444=onion 2>/dev/null)"
if printf '%s' "$OUT3" | grep -q -- "--listen 127.0.0.1:19333" \
  && ! printf '%s' "$OUT3" | grep -q -- "--listen 127.0.0.1:19444"; then
  echo "ok - bind port becomes listen (onion ignored)"
  PASS=$((PASS + 1))
else
  echo "not ok - bind port becomes listen (got: $OUT3)"
  FAIL=$((FAIL + 1))
fi

# Unknown flags abort like Core (feature_help.py -fakearg).
assert_fail_msg "unknown flag parse error" "Error parsing command line arguments" \
  env RBITCOIN_NODE="$FAKE" "$SHIM" --print-cmd -datadir="$DATADIR" -regtest -notarealflag

# -h / -version exit 0 on stdout without starting the node.
if OUTH="$("$SHIM" -datadir="$DATADIR" -h 2>/dev/null)" && printf '%s' "$OUTH" | grep -q Options; then
  echo "ok - -h prints Options"
  PASS=$((PASS + 1))
else
  echo "not ok - -h prints Options"
  FAIL=$((FAIL + 1))
fi
if OUTV="$("$SHIM" -datadir="$DATADIR" -version 2>/dev/null)" && printf '%s' "$OUTV" | grep -qi version; then
  echo "ok - -version prints version"
  PASS=$((PASS + 1))
else
  echo "not ok - -version prints version"
  FAIL=$((FAIL + 1))
fi

OUT4="$("$SHIM" --print-cmd -datadir="$DATADIR" -regtest -uacomment=testnode0 2>/dev/null)"
if printf '%s' "$OUT4" | grep -q -- '--uacomment=testnode0'; then
  echo "ok - uacomment forwarded"
  PASS=$((PASS + 1))
else
  echo "not ok - uacomment forwarded (got: $OUT4)"
  FAIL=$((FAIL + 1))
fi

assert_fail_msg "datadir required" "datadir is required" \
  env RBITCOIN_NODE="$FAKE" "$SHIM" --print-cmd -regtest

# Live smoke when a real node binary is on disk (optional in this script).
REAL=""
if [[ -n "${RBITCOIN_NODE_REAL:-}" && -x "${RBITCOIN_NODE_REAL}" ]]; then
  REAL="$RBITCOIN_NODE_REAL"
elif [[ -x "${CARGO_TARGET_DIR:-$ROOT/target/dev}/debug/rbitcoin-node" ]]; then
  REAL="${CARGO_TARGET_DIR:-$ROOT/target/dev}/debug/rbitcoin-node"
fi
if [[ -n "$REAL" ]]; then
  if RBITCOIN_NODE="$REAL" python3 "$ROOT/scripts/core-functional/smoke_rpc_up.py"; then
    echo "ok - smoke_rpc_up"
    PASS=$((PASS + 1))
  else
    echo "not ok - smoke_rpc_up"
    FAIL=$((FAIL + 1))
  fi
else
  echo "ok - smoke_rpc_up skipped (no rbitcoin-node binary)"
  PASS=$((PASS + 1))
fi

echo
echo "$PASS passed, $FAIL failed"
if [ "$FAIL" -ne 0 ]; then
  exit 1
fi
