#!/usr/bin/env bash
# Hermetic pin for scripts/release.sh: version files, --no-push line,
# ls-remote failure, origin/master sync gate. Does not push anywhere.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
REL="$ROOT/scripts/release.sh"
PASS=0
FAIL=0
export GIT_AUTHOR_NAME=rbitcoin-release-test
export GIT_AUTHOR_EMAIL=test@example.invalid
export GIT_COMMITTER_NAME=rbitcoin-release-test
export GIT_COMMITTER_EMAIL=test@example.invalid
WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/rbitcoin-release-test.XXXXXX")"
cleanup() { rm -rf "$WORKDIR"; }
trap cleanup EXIT

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

git_c() {
  git -c user.name=rbitcoin-release-test -c user.email=test@example.invalid "$@"
}

seed_tree() {
  local dest="$1"
  mkdir -p "$dest/nix"
  cat >"$dest/Cargo.toml" <<'EOF'
[workspace.package]
version = "9.9.9"
EOF
  cat >"$dest/nix/rbitcoin.nix" <<'EOF'
{
  version = "9.9.9";
}
EOF
  cat >"$dest/CHANGELOG.md" <<'EOF'
## [9.9.9]

Release notes for the hermetic pin.
EOF
  git_c -C "$dest" init -q -b master
  git_c -C "$dest" add Cargo.toml nix/rbitcoin.nix CHANGELOG.md
  git_c -C "$dest" commit -q -m init
}

FAKE="$WORKDIR/tree"
mkdir -p "$FAKE"
seed_tree "$FAKE"

out="$(bash "$REL" --root "$FAKE" --no-push --allow-branch master)"
assert_ok "no-push prints master+tag push line" \
  grep -qx "  git push origin refs/heads/master refs/tags/v9.9.9" <<<"$out"

LSFAIL="$WORKDIR/lsfail"
mkdir -p "$LSFAIL"
seed_tree "$LSFAIL"
git_c -C "$LSFAIL" remote add origin /no/such/rbitcoin-release-origin.git
assert_fail_msg "ls-remote network failure is loud" \
  "git ls-remote origin refs/tags/v9.9.9 failed" \
  bash "$REL" --root "$LSFAIL" --no-push --allow-branch master

SYNC="$WORKDIR/sync"
mkdir -p "$SYNC"
seed_tree "$SYNC"
git_c clone -q --bare "$SYNC" "$WORKDIR/origin.git"
git_c -C "$SYNC" remote add origin "$WORKDIR/origin.git"
out="$(bash "$REL" --root "$SYNC" --dry-run --allow-branch master)"
assert_ok "dry-run ok when HEAD matches origin/master" \
  grep -q "dry-run ok" <<<"$out"

echo extra >>"$SYNC/CHANGELOG.md"
git_c -C "$SYNC" add CHANGELOG.md
git_c -C "$SYNC" commit -q -m diverge
assert_fail_msg "push requires HEAD == origin/master" \
  "HEAD is not origin/master" \
  bash "$REL" --root "$SYNC" --dry-run --allow-branch master

out="$(bash "$REL" --root "$SYNC" --dry-run --allow-branch master --allow-diverged)"
assert_ok "allow-diverged skips origin/master gate" \
  grep -q "dry-run ok" <<<"$out"

MAIN="$WORKDIR/main"
mkdir -p "$MAIN"
seed_tree "$MAIN"
git_c -C "$MAIN" branch -m main
git_c clone -q --bare "$MAIN" "$WORKDIR/origin-main.git"
git_c -C "$MAIN" remote add origin "$WORKDIR/origin-main.git"
out="$(bash "$REL" --root "$MAIN" --dry-run --allow-branch main)"
assert_ok "dry-run ok when HEAD matches origin/main" \
  grep -q "dry-run ok" <<<"$out"

TOPIC="$WORKDIR/topic"
mkdir -p "$TOPIC"
seed_tree "$TOPIC"
git_c -C "$TOPIC" checkout -q -b release-topic
git_c clone -q --bare "$TOPIC" "$WORKDIR/origin-topic.git"
git_c -C "$TOPIC" remote add origin "$WORKDIR/origin-topic.git"
out="$(bash "$REL" --root "$TOPIC" --dry-run --allow-branch release-topic)"
assert_ok "dry-run ok when HEAD matches origin/release-topic" \
  grep -q "dry-run ok" <<<"$out"

if [[ "$FAIL" -ne 0 ]]; then
  echo "release.test.sh: $PASS passed, $FAIL failed"
  exit 1
fi
echo "release.test.sh: $PASS passed"
