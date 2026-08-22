#!/usr/bin/env bash
# Contract pin for scripts/release.sh (no network, no origin push).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
REL="$ROOT/scripts/release.sh"
PASS=0
FAIL=0

ok() { echo "ok - $1"; PASS=$((PASS + 1)); }
bad() { echo "not ok - $1"; FAIL=$((FAIL + 1)); }

assert_ok() {
  local name="$1"
  shift
  if "$@" >/dev/null 2>&1; then
    ok "$name"
  else
    bad "$name"
  fi
}

assert_fail() {
  local name="$1"
  shift
  if "$@" >/dev/null 2>&1; then
    bad "$name (expected failure)"
  else
    ok "$name"
  fi
}

assert_ok "script exists" test -x "$REL"
assert_fail "unknown flag" "$REL" --nope

WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/rbitcoin-release.XXXXXX")"
cleanup() { rm -rf "$WORKDIR"; }
trap cleanup EXIT

mini="$WORKDIR/repo"
mkdir -p "$mini/nix" "$mini/scripts"
cp "$REL" "$mini/scripts/release.sh"
chmod +x "$mini/scripts/release.sh"

write_tree() {
  local ver="${1:-0.5.1}"
  cat >"$mini/Cargo.toml" <<EOF
[workspace]
members = []
[workspace.package]
version = "$ver"
EOF
  cat >"$mini/nix/rbitcoin.nix" <<EOF
  commonArgs = {
    version = "$ver";
  };
EOF
  cat >"$mini/CHANGELOG.md" <<EOF
# Changelog

## [Unreleased]

## [$ver] — 2026-08-22

Workspace version **$ver**.

### Fixed

- Example note.

## [0.5.0] — 2026-08-22

Prior.
EOF
}

git -C "$mini" init -q
git -C "$mini" config user.name "release-test"
git -C "$mini" config user.email "release-test@example.test"
write_tree 0.5.1
git -C "$mini" add Cargo.toml nix/rbitcoin.nix CHANGELOG.md scripts
git -C "$mini" commit -q -m "v0.5.1"
git -C "$mini" branch -M master

run() {
  "$mini/scripts/release.sh" --root "$mini" "$@"
}

assert_ok "dry-run on matching master" run --dry-run
assert_fail "wrong --allow-branch" run --dry-run --allow-branch other

write_tree 0.5.9
assert_fail "dirty tree refused" run --dry-run
git -C "$mini" checkout -q -- .

# Nix mismatch
sed -i 's/version = "0.5.1"/version = "9.9.9"/' "$mini/nix/rbitcoin.nix"
git -C "$mini" add nix/rbitcoin.nix
git -C "$mini" commit -q -m "nix mismatch"
assert_fail "nix version mismatch" run --dry-run
git -C "$mini" reset -q --hard HEAD~1

# Missing changelog heading
printf '[workspace.package]\nversion = "9.9.9"\n' >"$mini/Cargo.toml"
printf '  version = "9.9.9";\n' >"$mini/nix/rbitcoin.nix"
git -C "$mini" add Cargo.toml nix/rbitcoin.nix
git -C "$mini" commit -q -m "no changelog 9.9.9"
assert_fail "missing changelog heading" run --dry-run
git -C "$mini" reset -q --hard HEAD~1

assert_ok "no-push tags locally" run --no-push
assert_ok "tag v0.5.1 exists" git -C "$mini" rev-parse -q --verify refs/tags/v0.5.1
assert_fail "second tag refused" run --no-push
msg="$(git -C "$mini" tag -l --format='%(contents)' v0.5.1)"
if echo "$msg" | grep -q 'Example note'; then
  ok "annotated tag includes changelog"
else
  bad "annotated tag missing changelog body"
fi
if echo "$msg" | grep -q 'rbitcoin v0.5.1'; then
  ok "annotated tag subject"
else
  bad "annotated tag subject"
fi

echo
echo "passed=$PASS failed=$FAIL"
test "$FAIL" -eq 0
