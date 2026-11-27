#!/usr/bin/env bash
# Contract: bare production unwraps are frozen per file.
# Fixtures under lint/unwrap-ratchet/fixtures. Does not scan the repo.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PY="$ROOT/scripts/unwrap-ratchet.py"
FIX="$ROOT/lint/unwrap-ratchet/fixtures"
PASS=0
FAIL=0
source "$ROOT/scripts/contract-assert.sh"

assert_fail "bare unwrap in src fails an empty baseline" \
  python3 "$PY" --root "$FIX/bare"
assert_ok "cfg(test) module unwrap is not counted" \
  python3 "$PY" --root "$FIX/cfg-test"
assert_ok "lock and try_into unwraps are not counted" \
  python3 "$PY" --root "$FIX/excluded"
assert_fail "count above the baseline fails" \
  python3 "$PY" --root "$FIX/over"
assert_fail "count below the baseline fails" \
  python3 "$PY" --root "$FIX/under"
assert_ok "a matching count passes" \
  python3 "$PY" --root "$FIX/match"
assert_fail "unwrap on the line after lock is counted" \
  python3 "$PY" --root "$FIX/after-lock"
assert_fail "second unwrap on a lock line is counted" \
  python3 "$PY" --root "$FIX/same-line"
assert_fail "unwrap on the line after try_into is counted" \
  python3 "$PY" --root "$FIX/after-try"
assert_fail "two unwraps on one line count as two" \
  python3 "$PY" --root "$FIX/two-calls"
assert_fail "cfg(test) inside a string does not open a skip" \
  python3 "$PY" --root "$FIX/cfg-string"
assert_fail "cfg(test) inside a block comment does not open a skip" \
  python3 "$PY" --root "$FIX/cfg-block"
assert_fail "braceless cfg(test) item does not skip the next function" \
  python3 "$PY" --root "$FIX/cfg-braceless"
assert_ok "cfg(test) function whose signature holds a semicolon is skipped" \
  python3 "$PY" --root "$FIX/cfg-header"
assert_fail "try_into is excluded only when it is the unwrap receiver" \
  python3 "$PY" --root "$FIX/try-chain"
assert_ok "try_into turbofish is still not counted" \
  python3 "$PY" --root "$FIX/turbofish"
assert_fail "a nested block comment does not hide the next unwrap" \
  python3 "$PY" --root "$FIX/cfg-nest"
assert_fail "cfg(test) inside a nested comment does not skip later code" \
  python3 "$PY" --root "$FIX/cfg-hidden"
assert_fail "a name that only ends in try_into is counted" \
  python3 "$PY" --root "$FIX/try-name"
assert_ok "cfg(test) where-clause body is not counted" \
  python3 "$PY" --root "$FIX/cfg-where"
assert_ok "a where-clause test item does not hide the next function" \
  python3 "$PY" --root "$FIX/cfg-where-next"
assert_ok "cfg(test) generic impl where-clause is not counted" \
  python3 "$PY" --root "$FIX/cfg-impl"
assert_fail "rb raw string does not hide a later unwrap" \
  python3 "$PY" --root "$FIX/raw-rb"
assert_fail "rc and rb# raw strings do not hide a later unwrap" \
  python3 "$PY" --root "$FIX/raw-rc"
assert_fail "a cooked string across lines does not hide a later unwrap" \
  python3 "$PY" --root "$FIX/multiline-string"

if [[ "$FAIL" -ne 0 ]]; then
  echo "unwrap-ratchet.test.sh: $PASS passed, $FAIL failed"
  exit 1
fi
echo "unwrap-ratchet.test.sh: $PASS passed"
