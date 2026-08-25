#!/usr/bin/env bash
# Contract: ast-grep scan is clean on lint/ast-grep/fixtures/good and
# reports error-severity hits on fixtures/bad (detached tokio::spawn).
# Does not scan crates/. Missing binary: fail in CI, skip locally.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
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

ast_grep_bin() {
  if command -v ast-grep >/dev/null 2>&1; then
    command -v ast-grep
    return 0
  fi
  return 1
}

BIN="$(ast_grep_bin || true)"
if [[ -z "$BIN" ]]; then
  if [[ "${CI:-}" == "true" ]]; then
    echo "ast-grep.test.sh: ast-grep missing in CI" >&2
    exit 1
  fi
  echo "ast-grep.test.sh: skip (ast-grep not installed)"
  exit 0
fi

cd "$ROOT"

good_json="$("$BIN" scan --json=compact lint/ast-grep/fixtures/good)"
assert_ok "good fixtures produce no findings" \
  bash -c '[[ "$1" == "[]" ]]' _ "$good_json"

bad_status=0
bad_json="$("$BIN" scan --json=compact lint/ast-grep/fixtures/bad 2>/dev/null)" || bad_status=$?
assert_ok "bad fixtures scan exits non-zero" \
  bash -c '[[ "$1" -ne 0 ]]' _ "$bad_status"
assert_ok "bad fixtures report detached-tokio-spawn" \
  grep -q 'detached-tokio-spawn' <<<"$bad_json"
assert_ok "bad fixtures include statement-level tokio::spawn" \
  python3 -c 'import json,sys; d=json.load(sys.stdin); assert any(e["text"]=="tokio::spawn(async {});" for e in d)' <<<"$bad_json"
assert_ok "bad fixtures include discarded JoinHandle" \
  python3 -c 'import json,sys; d=json.load(sys.stdin); assert any(e["text"].startswith("let _ = tokio::spawn") for e in d)' <<<"$bad_json"

if [[ "$FAIL" -ne 0 ]]; then
  echo "ast-grep.test.sh: $PASS passed, $FAIL failed"
  exit 1
fi
echo "ast-grep.test.sh: $PASS passed"
