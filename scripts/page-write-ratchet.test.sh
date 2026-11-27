#!/usr/bin/env bash
# Contract: production raw page writes are frozen per file and token.
# Fixtures under lint/page-write-ratchet/fixtures. Does not scan the repo.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PY="$ROOT/scripts/page-write-ratchet.py"
FIX="$ROOT/lint/page-write-ratchet/fixtures"
PASS=0
FAIL=0
source "$ROOT/scripts/contract-assert.sh"

assert_fail "a raw write in src fails an empty baseline" \
  python3 "$PY" --root "$FIX/bare"
assert_ok "comments and strings are not counted" \
  python3 "$PY" --root "$FIX/comment"
assert_ok "cfg(test) writes are not counted" \
  python3 "$PY" --root "$FIX/cfg-test"
assert_ok "writes under tests/ are not counted" \
  python3 "$PY" --root "$FIX/in-tests"
assert_ok "a crate outside the store and mempool is not counted" \
  python3 "$PY" --root "$FIX/other-crate"
assert_fail "count above the baseline fails" \
  python3 "$PY" --root "$FIX/over"
assert_fail "count below the baseline fails" \
  python3 "$PY" --root "$FIX/under"
assert_ok "a matching count passes" \
  python3 "$PY" --root "$FIX/match"
assert_fail "a new token in a known file fails" \
  python3 "$PY" --root "$FIX/new-token"
assert_ok "a door call is not a raw page write" \
  python3 "$PY" --root "$FIX/alias"
assert_ok "store write_all is not a raw page write" \
  python3 "$PY" --root "$FIX/store-write-all"
assert_fail "mempool write_all fails an empty baseline" \
  python3 "$PY" --root "$FIX/mempool-write-all"
assert_ok "pwrite_all is the checked door" \
  python3 "$PY" --root "$FIX/pwrite-all"
assert_fail "a call split before its parenthesis fails an empty baseline" \
  python3 "$PY" --root "$FIX/split-call"
assert_fail "a call split by comment-only and blank lines fails an empty baseline" \
  python3 "$PY" --root "$FIX/split-comment"
assert_fail "a path-qualified FileExt::write_at fails an empty baseline" \
  python3 "$PY" --root "$FIX/fileext-write-at"
assert_fail "a FileExt import with a method-form write_at fails an empty baseline" \
  python3 "$PY" --root "$FIX/fileext-method"
assert_fail "a second FileExt write in a file that imports it fails its baseline" \
  python3 "$PY" --root "$FIX/fileext-second-write"
assert_ok "FileExt writes that match their baseline pass" \
  python3 "$PY" --root "$FIX/fileext-writes-match"
assert_fail "FileExt::write_at split after the path fails an empty baseline" \
  python3 "$PY" --root "$FIX/fileext-split-path"
assert_fail "a cooked string across lines does not hide a later write" \
  python3 "$PY" --root "$FIX/multiline-string"

if [[ "$FAIL" -ne 0 ]]; then
  echo "page-write-ratchet.test.sh: $PASS passed, $FAIL failed"
  exit 1
fi
echo "page-write-ratchet.test.sh: $PASS passed"
