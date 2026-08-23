#!/usr/bin/env bash
# Contract pin for create_cache.py (no cargo, no 199-block mine).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PY="$ROOT/scripts/core-functional/create_cache.py"
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

WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/rbitcoin-cf-cache.XXXXXX")"
cleanup() { rm -rf "$WORKDIR"; }
trap cleanup EXIT

# --preseed-core writes empty blocks/chainstate (Core skip-remine marker).
CORE="$WORKDIR/bitcoin"
assert_ok "preseed-core" python3 "$PY" --preseed-core "$CORE" --cache "$WORKDIR/unused"
if [[ -d "$CORE/test/cache/node0/regtest/blocks" ]] \
  && [[ -d "$CORE/test/cache/node0/regtest/chainstate" ]]; then
  echo "ok - dummy blocks/chainstate exist"
  PASS=$((PASS + 1))
else
  echo "not ok - dummy blocks/chainstate exist"
  FAIL=$((FAIL + 1))
fi

# --ensure is a no-op when HEIGHT+store already look ready.
READY="$WORKDIR/ready"
mkdir -p "$READY/store"
echo x >"$READY/store/keep"
echo 199-mw1-genesis-mock >"$READY/HEIGHT"
if OUT="$(python3 "$PY" --ensure --cache "$READY" 2>&1)" \
  && printf '%s' "$OUT" | grep -q 'already ready' \
  && [[ -f "$READY/store/keep" ]]; then
  echo "ok - --ensure skips a ready cache"
  PASS=$((PASS + 1))
else
  echo "not ok - --ensure skips a ready cache (got: $OUT)"
  FAIL=$((FAIL + 1))
fi

echo
echo "$PASS passed, $FAIL failed"
if [ "$FAIL" -ne 0 ]; then
  exit 1
fi
