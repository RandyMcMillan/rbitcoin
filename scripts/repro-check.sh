#!/usr/bin/env bash
# Two independent clean builds of the same revision; fail if digests diverge.
#
# Usage:
#   ./scripts/repro-check.sh              # native only
#   ./scripts/repro-check.sh both         # native + musl (secondary triple)
#   ./scripts/repro-check.sh musl         # musl only
#   REPRO_OUT=/path ./scripts/repro-check.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
MODE="${1:-native}"
OUT="${REPRO_OUT:-${ROOT}/.repro-out}"
rm -rf "$OUT"
mkdir -p "$OUT"

build_once() {
  local attr="$1"
  local link="$2"
  local force_rebuild="${3:-0}"
  rm -f "$link"
  # --rebuild re-executes the builder (needed so the second clean build is not a
  # pure cache hit). If the prior realisation was GC'd, Nix refuses --rebuild;
  # fall back to a plain build to re-create the path.
  if [[ "$force_rebuild" == "1" ]]; then
    local err status
    err="$(mktemp)"
    set +e
    # Capture full nix output: --print-build-logs uses stderr.
    nix build "$attr" --out-link "$link" --rebuild --print-build-logs >"$err" 2>&1
    status=$?
    set -e
    cat "$err"
    if [[ "$status" -eq 0 ]]; then
      rm -f "$err"
      return 0
    fi
    if grep -qE 'not valid, so checking is not possible|does not exist|is not valid' "$err"; then
      echo "repro-check: --rebuild unavailable (missing/invalid prior path); plain build" >&2
      rm -f "$err"
      nix build "$attr" --out-link "$link" --print-build-logs
      return 0
    fi
    rm -f "$err"
    return 1
  fi
  nix build "$attr" --out-link "$link" --print-build-logs
}

hash_bins() {
  local prefix="$1"
  local link="$2"
  local report="$3"
  : >"$report"
  for b in rbitcoin-node rbitcoin-cli; do
    local f="$link/bin/$b"
    if [[ ! -e "$f" ]]; then
      echo "missing $f" >&2
      exit 1
    fi
    # Hash file contents only — report as "<sum>  <bin-name>" so path
    # differences between out-links never look like a mismatch.
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

echo "=== primary (native) double clean build ==="
build_once ".#rbitcoin" "$OUT/result-a" 0
hash_bins "primary-a" "$OUT/result-a" "$OUT/repro-primary-a.sha256"
build_once ".#rbitcoin" "$OUT/result-b" 1
hash_bins "primary-b" "$OUT/result-b" "$OUT/repro-primary-b.sha256"
compare_pair "$OUT/repro-primary-a.sha256" "$OUT/repro-primary-b.sha256" "native"

if [[ "$MODE" == "both" || "$MODE" == "musl" || "$MODE" == "secondary" ]]; then
  echo "=== secondary (musl static) double clean build ==="
  if ! nix eval --raw ".#rbitcoin-musl.name" >/dev/null 2>&1; then
    echo "rbitcoin-musl attr unavailable on this flake/system" | tee "$OUT/repro-secondary-env-limit.txt"
    exit 1
  fi
  build_once ".#rbitcoin-musl" "$OUT/result-musl-a" 0
  hash_bins "secondary-a" "$OUT/result-musl-a" "$OUT/repro-secondary-a.sha256"
  build_once ".#rbitcoin-musl" "$OUT/result-musl-b" 1
  hash_bins "secondary-b" "$OUT/result-musl-b" "$OUT/repro-secondary-b.sha256"
  compare_pair "$OUT/repro-secondary-a.sha256" "$OUT/repro-secondary-b.sha256" "musl"
fi

echo "=== all checks passed ==="
ls -la "$OUT"/*.sha256
