#!/usr/bin/env bash
# Contract: LCOV gate ignores test files (not substring "test"), runs default
# workspace tests (Tier A IBD included), 92% floor (no never-falls ratchet),
# writes Shields JSON. Does not run llvm-cov.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COV="$ROOT/scripts/coverage.sh"
PASS=0
FAIL=0
source "$ROOT/scripts/contract-assert.sh"


IGNORE="$(sed -n "s/^IGNORE='\\(.*\\)'/\\1/p" "$COV")"
if [ "${#IGNORE}" -eq 0 ]; then
  echo "not ok - coverage.sh defines IGNORE"
  exit 1
fi
echo "ok - coverage.sh defines IGNORE"
PASS=$((PASS + 1))

python3 - "$IGNORE" "$ROOT" <<'PY' || exit 1
import os, re, sys
rx = re.compile(sys.argv[1])
root = sys.argv[2]
excluded = [
    "/repo/crates/rbitcoin-store/tests/scripthash.rs",
    "/repo/crates/rbitcoin-ibd/tests/ibd/confirm.rs",
    "/repo/crates/rbitcoin-net/tests/peer/mod.rs",
    "/repo/crates/rbitcoin-ibd/tests/ibd_smoke.rs",
    "/repo/crates/rbitcoin-test/src/lib.rs",
    "/repo/crates/rbitcoin-test/tests/integration_multinode.rs",
    "/repo/crates/rbitcoin-bench/src/suite.rs",
    "/repo/crates/rbitcoin-store/src/testutil.rs",
    "/repo/crates/rbitcoin-query/src/testutil.rs",
    "/repo/crates/rbitcoin-consensus/tests/script/tests_verify.rs",
    "/repo/crates/rbitcoin-node/src/main.rs",
]
kept = [
    "/repo/crates/rbitcoin-store/src/scripthash.rs",
    "/repo/crates/rbitcoin-net/src/peer.rs",
    "/repo/crates/rbitcoin-ibd/src/ibd/mod.rs",
    "/repo/crates/rbitcoin-query/src/regtest_pad.rs",
    "/repo/crates/rbitcoin-consensus/src/regtest_pad.rs",
    "/repo/crates/rbitcoin-node/src/run.rs",
    "/repo/crates/rbitcoin-rpc/src/methods/chain.rs",
]
fail = 0
# Fixtures are real paths, so a crate move fails here instead of rotting.
for p in excluded + kept:
    if not os.path.exists(os.path.join(root, p.removeprefix("/repo/"))):
        print(f"not ok - fixture path is gone {p}", file=sys.stderr)
        fail += 1
for p in excluded:
    if rx.search(p) is None:
        print(f"not ok - should ignore {p}", file=sys.stderr)
        fail += 1
for p in kept:
    if rx.search(p) is not None:
        print(f"not ok - should count {p}", file=sys.stderr)
        fail += 1
sys.exit(fail)
PY
assert_ok "IGNORE drops test files and keeps production (incl. regtest_*)" true

assert_ok "two_node is not skipped" \
  bash -c '! grep -q "skip two_node_header_and_block_sync" "$1"' _ "$COV"
assert_ok "reconstruct is not skipped" \
  bash -c '! grep -q "skip serve_after_restart_via_reconstruct" "$1"' _ "$COV"
assert_ok "dead-peer is not skipped" \
  bash -c '! grep -q "skip ibd_skips_dead_peer" "$1"' _ "$COV"
assert_ok "llvm-cov test has no --skip" \
  bash -c '! grep -E "llvm-cov test" -A12 "$1" | grep -q -- "--skip"' _ "$COV"
assert_ok "llvm-cov excludes rbitcoin-bench" \
  bash -c 'grep -E "llvm-cov test" -A12 "$1" | grep -q -- "--exclude rbitcoin-bench"' _ "$COV"
assert_ok "llvm-cov test is GNU-timeout wrapped" \
  grep -q 'timeout --kill-after=30s' "$COV"
assert_ok "llvm-cov test does not pass --report-time (stable rustc)" \
  bash -c '! grep -E "report-time" "$1"' _ "$COV"
# CI uploads badge.json and crap.json, never the HTML tree.
assert_ok "COVERAGE_HTML=0 runs llvm-cov test with --no-report" \
  bash -c 'grep -q "COVERAGE_HTML:-1" "$1" && grep -q -- "--no-report" "$1"' _ "$COV"
assert_ok "ci coverage step skips the HTML report" \
  bash -c 'awk "/^  coverage:/{p=1;next} p&&/^  [a-z]/{exit} p" "$1" | grep -Eq "COVERAGE_HTML: \"0\""' _ "$ROOT/.github/workflows/ci.yml"
assert_ok "ci coverage job has a wall timeout" \
  bash -c 'awk "/^  coverage:/{p=1} p&&/timeout-minutes:/{print; exit}" "$1" | grep -q timeout-minutes' _ "$ROOT/.github/workflows/ci.yml"
assert_ok "required test job excludes rbitcoin-bench" \
  bash -c 'awk "/^  test:/{p=1;next} p&&/^  [a-z]/{exit} p" "$1" | grep -Eq -- "^ +run: cargo test --workspace --exclude rbitcoin-bench( --features rbitcoin-store/write-probe)?$"' _ "$ROOT/.github/workflows/ci.yml"
assert_ok "required test job classifies the write probe" \
  bash -c 'awk "/^  test:/{p=1;next} p&&/^  [a-z]/{exit} p" "$1" | grep -q -- "scripts/write-probe-classify.py"' _ "$ROOT/.github/workflows/ci.yml"
assert_ok "required test job still pins the bench scripthash" \
  grep -q 'cargo test -p rbitcoin-bench --lib hot_corpus_includes_p2a' "$ROOT/.github/workflows/ci.yml"
assert_ok "pread-release job runs the skip-flushes switch test" \
  bash -c 'awk "/^  pread-release:/{p=1;next} p&&/^  [a-z]/{exit} p" "$1" | grep -q -- "--features skip-flushes --test suite"' _ "$ROOT/.github/workflows/ci.yml"
assert_ok "macos-test job runs the workspace suite with skip-flushes" \
  bash -c 'awk "/^  macos-test:/{p=1;next} p&&/^  [a-z]/{exit} p" "$1" | grep -Eq -- "^ +run: cargo test --workspace --exclude rbitcoin-bench --features skip-flushes$"' _ "$ROOT/.github/workflows/ci.yml"
assert_ok "macos-test job starts with the other jobs (no needs)" \
  bash -c '! awk "/^  macos-test:/{p=1;next} p&&/^  [a-z]/{exit} p" "$1" | grep -Eq -- "^    needs:"' _ "$ROOT/.github/workflows/ci.yml"
# coverage-gate.py owns the floor (FLOOR_PCT, written to gate.json). The badge
# takes it from there; neither script carries a second copy of the number.
assert_ok "coverage.sh reads the badge gate from gate.json, not a literal" \
  bash -c 'grep -q -- "--gate \"\$GATE_FLOOR\"" "$1" && grep -q "GATE_FLOOR=.*gate.json\|GATE_FLOOR=.*GATE_JSON" "$1" && ! grep -qE -- "--gate [0-9]+" "$1"' _ "$COV"
assert_ok "coverage-badge.py has no default gate" \
  bash -c 'grep -q "\"--gate\", type=int, required=True" "$1" && ! grep -qE "default=[0-9]{2}" "$1"' _ "$ROOT/scripts/coverage-badge.py"
# TESTING.md (Coverage): release is check-only, in both shapes CI names.
assert_ok "pread-release checks the release profile compiles (workspace shape)" \
  grep -q 'cargo check --release --workspace$' "$ROOT/.github/workflows/ci.yml"
assert_ok "pread-release checks the release profile compiles (product shape)" \
  grep -q 'cargo check --release -p rbitcoin-node -p rbitcoin-cli -p rbitcoin-store$' "$ROOT/.github/workflows/ci.yml"
assert_ok "pread-release builds rustdoc for the workspace" \
  grep -q 'cargo doc --workspace --no-deps$' "$ROOT/.github/workflows/ci.yml"
assert_ok "gitignore ignores apilog jsonl" \
  bash -c 'grep -q "apilog\\*\\.jsonl" "$1/.gitignore" && git -C "$1" check-ignore -q -- apilog.jsonl && git -C "$1" check-ignore -q -- apilog-old.jsonl' _ "$ROOT"

tmp="$(mktemp)"
python3 "$ROOT/scripts/coverage-badge.py" \
  --lh 906 --lf 1000 --gate 90 --sha abcdef1234567890 --date 2026-09-13 --out "$tmp"
python3 - "$tmp" <<'PY'
import json, sys
from pathlib import Path
d = json.loads(Path(sys.argv[1]).read_text())
need = {
    "schemaVersion": 1,
    "label": "coverage",
    "message": "90.60%",
    "color": "brightgreen",
    "pct": 90.6,
    "lh": 906,
    "lf": 1000,
    "gate": 90,
    "scope": "production",
    "sha": "abcdef123456",
    "date": "2026-09-13",
}
for k, v in need.items():
    if d.get(k) != v:
        print(f"not ok - badge {k}: {d.get(k)!r} != {v!r}", file=sys.stderr)
        sys.exit(1)
PY
assert_ok "badge JSON is Shields endpoint + LH/LF extras" true
rm -f "$tmp"

tmp="$(mktemp)"
python3 "$ROOT/scripts/coverage-badge.py" \
  --lh 89 --lf 100 --gate 90 --sha deadbeef --date 2026-09-13 --out "$tmp"
python3 - "$tmp" <<'PY'
import json, sys
from pathlib import Path
d = json.loads(Path(sys.argv[1]).read_text())
assert d["color"] == "red", d["color"]
assert d["message"] == "89.00%", d["message"]
PY
assert_ok "badge is red below the gate" true
rm -f "$tmp"

lcov="$(mktemp)"
tmp="$(mktemp)"
cat >"$lcov" <<'EOF'
SF:/build/crates/rbitcoin-store/src/a.rs
LH:40
LF:50
end_of_record
SF:crates/rbitcoin-store/src/b.rs
LH:10
LF:10
end_of_record
SF:crates/rbitcoin-net/src/peer.rs
LH:1
LF:4
end_of_record
EOF
python3 "$ROOT/scripts/coverage-badge.py" \
  --lh 51 --lf 64 --gate 93 --sha abcdef1234567890 --date 2026-10-05 \
  --lcov "$lcov" --out "$tmp"
python3 - "$tmp" <<'PY'
import json, sys
from pathlib import Path
d = json.loads(Path(sys.argv[1]).read_text())
assert d["lh"] == 51 and d["lf"] == 64, d
crates = d["crates"]
assert [c["name"] for c in crates] == ["rbitcoin-net", "rbitcoin-store"], crates
net, store = crates
assert net == {"name": "rbitcoin-net", "lh": 1, "lf": 4, "pct": 25.0}, net
assert store == {"name": "rbitcoin-store", "lh": 50, "lf": 60, "pct": 83.33}, store
assert sum(c["lh"] for c in crates) == d["lh"]
assert sum(c["lf"] for c in crates) == d["lf"]
PY
assert_ok "badge crates sum to the workspace lines" true
rm -f "$tmp" "$lcov"

lcov="$(mktemp)"
tmp="$(mktemp)"
cat >"$lcov" <<'EOF'
SF:crates/rbitcoin-log/src/lib.rs
LH:1
LF:2
end_of_record
SF:fuzz/src/target.rs
LH:1
LF:1
end_of_record
EOF
if python3 "$ROOT/scripts/coverage-badge.py" \
  --lh 2 --lf 3 --gate 93 --lcov "$lcov" --out "$tmp" >/dev/null 2>"$tmp.err"; then
  echo "not ok - badge accepts lines outside crates/"
  FAIL=$((FAIL + 1))
else
  echo "ok - badge rejects lines outside crates/"
  PASS=$((PASS + 1))
fi
rm -f "$tmp" "$tmp.err" "$lcov"

assert_ok "coverage.sh passes the filtered LCOV into the badge" \
  grep -q -- '--lcov' "$COV"

out="$(BADGE_DRY_RUN=1 "$ROOT/scripts/publish-coverage-badge.sh")"
assert_ok "publish dry-run names badges/coverage.json" \
  grep -q "badges/coverage.json" <<<"$out"

GATE="$ROOT/scripts/coverage-gate.py"
assert_ok "coverage-gate.py exists" test -f "$GATE"

assert_gate_pass() {
  local name="$1"
  shift
  if python3 "$GATE" "$@" >/dev/null; then
    echo "ok - $name"
    PASS=$((PASS + 1))
  else
    echo "not ok - $name"
    FAIL=$((FAIL + 1))
  fi
}

assert_gate_fail() {
  local name="$1"
  shift
  if python3 "$GATE" "$@" >/dev/null 2>&1; then
    echo "not ok - $name (wanted fail)"
    FAIL=$((FAIL + 1))
  else
    echo "ok - $name"
    PASS=$((PASS + 1))
  fi
}

assert_gate_pass "92.00% floor passes" --lh 92 --lf 100
assert_gate_fail "91.99% floor fails" --lh 9199 --lf 10000
assert_gate_fail "suite-rlib 91.26% fails the 92% floor" \
  --lh 93410 --lf 102361
assert_gate_pass "92.40% passes the 92% floor" \
  --lh 94582 --lf 102361

st="$(mktemp)"
python3 "$GATE" --lh 94582 --lf 102361 --status-out "$st" >/dev/null
python3 - "$st" <<'PY'
import json, sys
from pathlib import Path
d = json.loads(Path(sys.argv[1]).read_text())
assert d["ok"] is True, d
assert d["mode"] == "floor", d
assert d["floor"] == 92, d
PY
assert_ok "status-out JSON names 92% floor" true
rm -f "$st"

assert_ok "coverage.sh calls coverage-gate.py" \
  grep -q "coverage-gate.py" "$COV"
assert_ok "coverage.sh does not fetch merge-base for the gate" \
  bash -c '! grep -q "Coverage merge-base" "$1"' _ "$COV"

if [[ "$FAIL" -ne 0 ]]; then
  echo "coverage.test.sh: $PASS passed, $FAIL failed"
  exit 1
fi
echo "coverage.test.sh: $PASS passed"
