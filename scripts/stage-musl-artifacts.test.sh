#!/usr/bin/env bash
# Contract pin for scripts/stage-musl-artifacts.sh (no Nix / no musl compile).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
STAGE="$ROOT/scripts/stage-musl-artifacts.sh"
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

assert_fail() {
  local name="$1"
  shift
  if "$@" >/dev/null 2>&1; then
    echo "not ok - $name (expected failure)"
    FAIL=$((FAIL + 1))
  else
    echo "ok - $name"
    PASS=$((PASS + 1))
  fi
}

WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/rbitcoin-stage-musl.XXXXXX")"
cleanup() { rm -rf "$WORKDIR"; }
trap cleanup EXIT

MOCK="$WORKDIR/mockbin"
mkdir -p "$MOCK"

# file(1) stub: MODE=static|dynamic via env (default static).
cat >"$MOCK/file" <<'EOF'
#!/usr/bin/env bash
shift || true
if [[ "${MOCK_FILE_MODE:-static}" == "dynamic" ]]; then
  echo "ELF 64-bit LSB pie executable, x86-64, dynamically linked, interpreter /lib64/ld-linux-x86-64.so.2"
else
  echo "ELF 64-bit LSB executable, x86-64, statically linked, stripped"
fi
EOF
chmod +x "$MOCK/file"

RESULT="$WORKDIR/result"
DEST="$WORKDIR/dist"
mkdir -p "$RESULT/bin"
printf 'node-bytes\n' >"$RESULT/bin/rbitcoin-node"
printf 'cli-bytes\n' >"$RESULT/bin/rbitcoin-cli"
chmod +x "$RESULT/bin/rbitcoin-node" "$RESULT/bin/rbitcoin-cli"

export PATH="$MOCK:$PATH"

assert_ok "script exists" test -x "$STAGE"

assert_ok "static pair stages" env MOCK_FILE_MODE=static "$STAGE" "$RESULT" "$DEST"
assert_ok "copied node" test -f "$DEST/rbitcoin-node"
assert_ok "copied cli" test -f "$DEST/rbitcoin-cli"
assert_ok "sha256sums present" test -s "$DEST/SHA256SUMS"
assert_ok "sha256sums names node" grep -q 'rbitcoin-node' "$DEST/SHA256SUMS"
assert_ok "sha256sums names cli" grep -q 'rbitcoin-cli' "$DEST/SHA256SUMS"

rm -rf "$DEST"
assert_fail "dynamic link is refused" env MOCK_FILE_MODE=dynamic "$STAGE" "$RESULT" "$DEST"

rm -rf "$DEST"
rm -f "$RESULT/bin/rbitcoin-cli"
assert_fail "missing binary is refused" env MOCK_FILE_MODE=static "$STAGE" "$RESULT" "$DEST"

echo
echo "passed=$PASS failed=$FAIL"
test "$FAIL" -eq 0
