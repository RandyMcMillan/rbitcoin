#!/usr/bin/env bash
# Contract pin for check_core_release.py (no network, no cargo).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
CHK="$ROOT/scripts/core-functional/check_core_release.py"
PASS=0
FAIL=0

assert_ok_msg() {
  local name="$1"
  local needle="$2"
  shift 2
  local out
  if ! out="$("$@" 2>&1)"; then
    echo "not ok - $name (unexpected failure: $out)"
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

WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/rbitcoin-cf-rel.XXXXXX")"
cleanup() { rm -rf "$WORKDIR"; }
trap cleanup EXIT

INV="$WORKDIR/inv.toml"
cat >"$INV" <<'EOF'
pin = "v31.1"
core_commit = "9be056a8a72b624dae9623b2f7bded92c2a21c91"
EOF

# Same pin as latest final → no warning.
assert_ok_msg "pin matches latest" "ok pin=v31.1 latest=v31.1" \
  python3 "$CHK" --inventory "$INV" --latest v31.1

# Newer major / minor / patch → warn, still exit 0.
assert_ok_msg "warn newer major" "WARNING pin=v31.1 latest=v32.0" \
  python3 "$CHK" --inventory "$INV" --latest v32.0
assert_ok_msg "warn newer patch" "WARNING pin=v31.1 latest=v31.2" \
  python3 "$CHK" --inventory "$INV" --latest v31.2

# Older maintenance line published later must not warn (semver, not GitHub latest).
cat >"$WORKDIR/releases.json" <<'EOF'
[
  {"tag_name": "v29.4", "prerelease": false, "draft": false},
  {"tag_name": "v30.3", "prerelease": false, "draft": false},
  {"tag_name": "v31.1", "prerelease": false, "draft": false},
  {"tag_name": "v32.0rc1", "prerelease": true, "draft": false}
]
EOF
assert_ok_msg "older tags + rc do not warn" "ok pin=v31.1 latest=v31.1" \
  python3 "$CHK" --inventory "$INV" --releases-json "$WORKDIR/releases.json"

# Final newer in the list → warn; mention fixtures + functional tests.
cat >"$WORKDIR/releases-new.json" <<'EOF'
[
  {"tag_name": "v31.1", "prerelease": false, "draft": false},
  {"tag_name": "v32.0", "prerelease": false, "draft": false}
]
EOF
assert_ok_msg "json newer warns fixtures" "fixtures" \
  python3 "$CHK" --inventory "$INV" --releases-json "$WORKDIR/releases-new.json"
assert_ok_msg "json newer warns functional" "functional" \
  python3 "$CHK" --inventory "$INV" --releases-json "$WORKDIR/releases-new.json"

# --fail-on-stale is opt-in (nightly stays warn-only).
assert_fail_msg "fail-on-stale exits 1" "WARNING pin=v31.1 latest=v32.0" \
  python3 "$CHK" --inventory "$INV" --latest v32.0 --fail-on-stale

# Actions annotation when GITHUB_ACTIONS=true.
GA_OUT="$(GITHUB_ACTIONS=true python3 "$CHK" --inventory "$INV" --latest v32.0 2>&1 || true)"
if printf '%s' "$GA_OUT" | grep -q '::warning'; then
  echo "ok - GitHub Actions warning annotation"
  PASS=$((PASS + 1))
else
  echo "not ok - GitHub Actions warning annotation (got: $GA_OUT)"
  FAIL=$((FAIL + 1))
fi

echo
echo "$PASS passed, $FAIL failed"
if [ "$FAIL" -ne 0 ]; then
  exit 1
fi
