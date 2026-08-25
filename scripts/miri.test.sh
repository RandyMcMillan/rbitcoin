#!/usr/bin/env bash
# Contract: Miri runner forces nightly, tests only rbitcoin-primitives,
# and never --workspace. Does not invoke cargo miri.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RUN="$ROOT/scripts/miri.sh"
PASS=0
FAIL=0

assert_ok() {
  local name="$1"
  shift
  if "$@"; then
    echo "ok - $name"
    PASS=$((PASS + 1))
  else
    echo "not ok - $name"
    FAIL=$((FAIL + 1))
  fi
}

out="$(MIRI_DRY_RUN=1 "$RUN")"
assert_ok "dry-run default toolchain is nightly" \
  grep -qx "RUSTUP_TOOLCHAIN=nightly" <<<"$out"
assert_ok "dry-run tests rbitcoin-primitives" \
  grep -q -- "-p rbitcoin-primitives" <<<"$out"
assert_ok "dry-run does not use --workspace" \
  bash -c '! grep -q -- "--workspace" <<<"$1"' _ "$out"

out="$(MIRI_DRY_RUN=1 RUSTUP_TOOLCHAIN=nightly-2026-01-01 "$RUN")"
assert_ok "dry-run honors an explicit RUSTUP_TOOLCHAIN" \
  grep -qx "RUSTUP_TOOLCHAIN=nightly-2026-01-01" <<<"$out"
assert_ok "dry-run cargo line uses the same toolchain" \
  grep -q "cargo +nightly-2026-01-01 miri test -p rbitcoin-primitives" <<<"$out"

if [[ "$FAIL" -ne 0 ]]; then
  echo "miri.test.sh: $PASS passed, $FAIL failed"
  exit 1
fi
echo "miri.test.sh: $PASS passed"
