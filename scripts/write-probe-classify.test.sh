#!/usr/bin/env bash
# Contract: the write-probe classifier fails on any unreviewed entry and on
# an empty log. Fixtures under lint/write-probe.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PY="$ROOT/scripts/write-probe-classify.py"
FIX="$ROOT/lint/write-probe"
PASS=0
FAIL=0
source "$ROOT/scripts/contract-assert.sh"

assert_ok "safe classes and reviewed families pass" \
  python3 "$PY" "$FIX/classified.log"
assert_fail "a multi-sector write with no reviewed reason fails" \
  python3 "$PY" "$FIX/unclassified.log"
assert_fail "an empty log fails: the probe did not run" \
  python3 "$PY" "$FIX/empty.log"
assert_fail "a missing log fails" \
  python3 "$PY" "$FIX/missing.log"

if [[ "$FAIL" -ne 0 ]]; then
  echo "write-probe-classify.test.sh: $PASS passed, $FAIL failed"
  exit 1
fi
echo "write-probe-classify.test.sh: $PASS passed"
