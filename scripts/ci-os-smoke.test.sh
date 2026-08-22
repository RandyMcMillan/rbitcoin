#!/usr/bin/env bash
# Contract: PR OS smoke is store create/open + node --smoke, not a
# packaged musl/Windows/Darwin snapshot. Does not invoke cargo.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RUN="$ROOT/scripts/ci-os-smoke.sh"
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

out="$(CI_OS_SMOKE_DRY_RUN=1 "$RUN")"
assert_ok "dry-run names store roundtrip + node --smoke" \
  grep -qx "smoke=store-roundtrip+node-smoke" <<<"$out"
assert_ok "dry-run default HEAD_SCALE is tiny" \
  grep -qx "RBITCOIN_HEAD_SCALE=tiny" <<<"$out"

out="$(CI_OS_SMOKE_DRY_RUN=1 RBITCOIN_HEAD_SCALE=tiny "$RUN")"
assert_ok "dry-run honors RBITCOIN_HEAD_SCALE" \
  grep -qx "RBITCOIN_HEAD_SCALE=tiny" <<<"$out"

if [[ "$FAIL" -ne 0 ]]; then
  echo "ci-os-smoke.test.sh: $PASS passed, $FAIL failed"
  exit 1
fi
echo "ci-os-smoke.test.sh: $PASS passed"
