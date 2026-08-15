#!/usr/bin/env bash
# Contract pin for sync-core-fixtures.sh (no git submodule, no cargo).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SYNC="$ROOT/scripts/core-functional/sync-core-fixtures.sh"
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

WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/rbitcoin-sync-fix.XXXXXX")"
cleanup() { rm -rf "$WORKDIR"; }
trap cleanup EXIT

CORE="$WORKDIR/core-data"
OURS="$WORKDIR/fixtures"
mkdir -p "$CORE" "$OURS"
printf '{}\n' >"$CORE/script_tests.json"
printf '{}\n' >"$CORE/tx_valid.json"
printf '{}\n' >"$CORE/tx_invalid.json"
cp "$CORE/script_tests.json" "$OURS/script_tests.json"
cp "$CORE/tx_valid.json" "$OURS/tx_valid.json"
cp "$CORE/tx_invalid.json" "$OURS/tx_invalid.json"

assert_ok "check matches" \
  "$SYNC" --check --core-data "$CORE" --fixtures "$OURS"

printf 'stale\n' >"$OURS/script_tests.json"
assert_fail_msg "check reports mismatch" "fixture mismatch: script_tests.json" \
  "$SYNC" --check --core-data "$CORE" --fixtures "$OURS"

assert_ok "write restores" \
  "$SYNC" --write --core-data "$CORE" --fixtures "$OURS"
assert_ok "check after write" \
  "$SYNC" --check --core-data "$CORE" --fixtures "$OURS"

rm -f "$CORE/tx_valid.json"
assert_fail_msg "missing core file" "missing core fixture: tx_valid.json" \
  "$SYNC" --check --core-data "$CORE" --fixtures "$OURS"

echo
echo "$PASS passed, $FAIL failed"
if [ "$FAIL" -ne 0 ]; then
  exit 1
fi
