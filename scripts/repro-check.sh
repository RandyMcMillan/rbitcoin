#!/usr/bin/env bash
# Two independent clean *rebuilds* of the same revision; fail if digests diverge
# or if Nix does not re-execute the builder (cache-hit theater).
#
# **Release / digest gate only** — not the day-to-day install path. After ordinary
# code edits use `./scripts/repro-build.sh` or `nix build .#rbitcoin-musl` once.
#
# Usage:
#   ./scripts/repro-check.sh              # musl static only (primary / default)
#   ./scripts/repro-check.sh both         # musl + optional glibc
#   ./scripts/repro-check.sh glibc        # glibc only
#   ./scripts/repro-check.sh musl         # same as default
#   REPRO_OUT=/path ./scripts/repro-check.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
MODE="${1:-musl}"
OUT="${REPRO_OUT:-${ROOT}/.repro-out}"
rm -rf "$OUT"
mkdir -p "$OUT"

# Record git rev under test (must be clean for release claims).
{
  echo "git_head=$(git rev-parse HEAD)"
  echo "git_describe=$(git describe --always --dirty 2>/dev/null || true)"
  git status --porcelain | head -50 || true
} | tee "$OUT/tree-state.txt"

require_checking_outputs() {
  local log="$1"
  local label="$2"
  # Nix 2.x prints this when --rebuild re-executes and compares store outputs.
  if ! grep -qE 'checking outputs of' "$log"; then
    echo "FAIL: $label rebuild log lacks 'checking outputs of' (not a real --rebuild)" >&2
    echo "---- log tail ----" >&2
    tail -40 "$log" >&2 || true
    exit 1
  fi
}

# Realize the derivation once so --rebuild has a prior path to check against.
# This may be a cache hit; that is OK. Honesty lives in rebuild_once.
realize() {
  local attr="$1"
  local link="$2"
  local log="$3"
  rm -f "$link"
  echo "realize: nix build $attr"
  nix build "$attr" --out-link "$link" --print-build-logs 2>&1 | tee "$log"
}

# Force re-execution. Never fall back to plain `nix build` (that is a false-pass).
rebuild_once() {
  local attr="$1"
  local link="$2"
  local log="$3"
  local label="$4"
  rm -f "$link"
  echo "rebuild: nix build $attr --rebuild ($label)"
  set +e
  nix build "$attr" --out-link "$link" --rebuild --print-build-logs >"$log" 2>&1
  local status=$?
  set -e
  cat "$log"
  if [[ "$status" -ne 0 ]]; then
    echo "FAIL: nix build --rebuild failed for $label (exit $status); no plain-build fallback" >&2
    exit 1
  fi
  require_checking_outputs "$log" "$label"
}

hash_bins() {
  local prefix="$1"
  local link="$2"
  local report="$3"
  : >"$report"
  # Record the store path we hashed (for skeptic correlation with outPath).
  readlink -f "$link" | tee "$OUT/${prefix}.outPath"
  for b in rbitcoin-node rbitcoin-cli; do
    local f="$link/bin/$b"
    if [[ ! -e "$f" ]]; then
      echo "missing $f" >&2
      exit 1
    fi
    local sum
    sum="$(sha256sum "$f" | awk '{print $1}')"
    printf '%s  %s\n' "$sum" "$b" | tee -a "$report"
    cp -a --remove-destination "$f" "$OUT/${prefix}-$b" 2>/dev/null || cp -a "$f" "$OUT/${prefix}-$b"
    printf '%s\n' "$sum" >"$OUT/${prefix}-$b.sha256"
  done
}

compare_pair() {
  local a="$1"
  local b="$2"
  local label="$3"
  if ! diff -u "$a" "$b"; then
    echo "FAIL: $label digests differ" >&2
    exit 1
  fi
  echo "OK: $label digests match"
}

check_target() {
  local attr="$1"
  local tag="$2" # primary | secondary
  echo "=== $tag ($attr) realize + two --rebuilds ==="
  realize "$attr" "$OUT/result-${tag}-seed" "$OUT/${tag}-realize.log"
  # First independent rebuild
  rebuild_once "$attr" "$OUT/result-${tag}-a" "$OUT/${tag}-rebuild-a.log" "${tag}-a"
  hash_bins "${tag}-a" "$OUT/result-${tag}-a" "$OUT/repro-${tag}-a.sha256"
  # Second independent rebuild
  rebuild_once "$attr" "$OUT/result-${tag}-b" "$OUT/${tag}-rebuild-b.log" "${tag}-b"
  hash_bins "${tag}-b" "$OUT/result-${tag}-b" "$OUT/repro-${tag}-b.sha256"
  compare_pair "$OUT/repro-${tag}-a.sha256" "$OUT/repro-${tag}-b.sha256" "$tag"
  # outPaths must be the same store realisation for both rebuilds
  if ! diff -u "$OUT/${tag}-a.outPath" "$OUT/${tag}-b.outPath"; then
    echo "FAIL: $tag outPath changed between rebuilds" >&2
    exit 1
  fi
  echo "OK: $tag outPath=$(cat "$OUT/${tag}-a.outPath")"
}

run_musl=0
run_glibc=0
case "$MODE" in
  musl|static|default|native|primary|"")
    run_musl=1
    ;;
  glibc|gnu|dynamic)
    run_glibc=1
    ;;
  both|all)
    run_musl=1
    run_glibc=1
    ;;
  *)
    echo "usage: $0 [musl|glibc|both]" >&2
    exit 2
    ;;
esac

if [[ "$run_musl" -eq 1 ]]; then
  if ! nix eval --raw ".#rbitcoin-musl.name" >/dev/null 2>&1; then
    echo "rbitcoin-musl attr unavailable on this flake/system" | tee "$OUT/repro-primary-env-limit.txt"
    exit 1
  fi
  check_target ".#rbitcoin-musl" "primary"
fi

if [[ "$run_glibc" -eq 1 ]]; then
  check_target ".#rbitcoin-glibc" "secondary"
fi

echo "=== all checks passed ==="
ls -la "$OUT"/*.sha256 "$OUT"/*-rebuild-*.log "$OUT"/*.outPath
echo "SCRATCH_HINT: copy $OUT to verification scratch if REPRO_OUT was set"
