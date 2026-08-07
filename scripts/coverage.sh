#!/usr/bin/env bash
# Enforce ≥90% line coverage on first-party crates (LCOV LH/LF).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Line coverage gate (percent). New and existing first-party code share this bar.
LINE_MIN_PCT=90

export CARGO_TERM_COLOR="${CARGO_TERM_COLOR:-always}"

if ! command -v cargo >/dev/null 2>&1; then
  echo "cargo not found; enter nix-shell first" >&2
  exit 1
fi

# Ensure bins exist for binary smoke scenarios.
cargo build -p rbitcoin-node -p rbitcoin-cli

# Prefer system llvm-tools when rustup component is unavailable (Nix).
if [[ -z "${LLVM_COV:-}" ]] && command -v llvm-cov >/dev/null 2>&1; then
  export LLVM_COV="$(command -v llvm-cov)"
fi
if [[ -z "${LLVM_PROFDATA:-}" ]] && command -v llvm-profdata >/dev/null 2>&1; then
  export LLVM_PROFDATA="$(command -v llvm-profdata)"
fi

# main.rs trampolines are one-liners; logic is covered via cli_main in libs.
# store_bench is a host-only microbench binary (not production path).
IGNORE='(/\.cargo/|/rustc-|/nix/store/|library/std/|/src/main\.rs$|/bin/store_bench\.rs$)'

if command -v cargo-llvm-cov >/dev/null 2>&1 || cargo llvm-cov --version >/dev/null 2>&1; then
  # Default: do NOT clean instrumented artifacts. Incremental llvm-cov rebuilds
  # are much faster for iterative work and still re-run all tests with coverage.
  # Force a full clean when debugging stale counters: COVERAGE_CLEAN=1 ./scripts/coverage.sh
  if [[ "${COVERAGE_CLEAN:-0}" == "1" ]]; then
    echo "COVERAGE_CLEAN=1: wiping llvm-cov workspace artifacts"
    cargo llvm-cov clean --workspace
  fi

  # Branch coverage requires nightly on many toolchains; line gate is always on.
  EXTRA=()
  if cargo llvm-cov test --help 2>&1 | grep -q -- '--fail-under-branches'; then
    if rustc -vV 2>/dev/null | grep -q nightly; then
      EXTRA+=(--branch --fail-under-branches 90)
    fi
  fi
  mkdir -p "$ROOT/coverage"
  cargo llvm-cov test --workspace \
    --ignore-filename-regex "$IGNORE" \
    "${EXTRA[@]}" \
    --html --output-dir "$ROOT/coverage"

  REPORT="$(cargo llvm-cov report --ignore-filename-regex "$IGNORE" 2>/dev/null || true)"
  echo "$REPORT"

  cargo llvm-cov report \
    --ignore-filename-regex "$IGNORE" \
    --lcov --output-path "$ROOT/coverage/lcov.info" || true

  # Line gate: LCOV LH/LF (authoritative for the 90% bar).
  LCOV_STATS="$(python3 - <<'PY'
from pathlib import Path
p = Path("coverage/lcov.info")
lf = lh = 0
if p.exists():
    for line in p.read_text(errors="replace").splitlines():
        if line.startswith("LF:"):
            lf += int(line[3:])
        elif line.startswith("LH:"):
            lh += int(line[3:])
print(f"{lh} {lf}")
PY
)"
  read -r LCOV_HIT LCOV_TOT <<<"$LCOV_STATS"
  LCOV_HIT="${LCOV_HIT:-0}"
  LCOV_TOT="${LCOV_TOT:-0}"
  if [[ "$LCOV_TOT" -le 0 ]]; then
    echo "FAIL: no LCOV totals (missing coverage/lcov.info or empty LF)" >&2
    exit 1
  fi
  LCOV_PCT="$(python3 -c "print(f'{100.0*$LCOV_HIT/$LCOV_TOT:.2f}')")"
  MISS=$((LCOV_TOT > LCOV_HIT ? LCOV_TOT - LCOV_HIT : 0))
  echo "LCOV lines: ${LCOV_HIT}/${LCOV_TOT} (${LCOV_PCT}%) miss=${MISS}"
  echo "Line coverage gate: ≥${LINE_MIN_PCT}% (constant LINE_MIN_PCT=${LINE_MIN_PCT})"

  # Optional HTML diagnostic (not the pass condition).
  HTML_PRESENT=0
  if find "$ROOT/coverage" -name '*.html' -print -quit 2>/dev/null | grep -q .; then
    HTML_PRESENT=1
  fi
  if [[ "$HTML_PRESENT" -eq 1 ]]; then
    UNCOV_TOTAL="$(
      python3 - <<'PY'
from pathlib import Path
import re
n = 0
for p in Path("coverage").rglob("*.html"):
    t = p.read_text(errors="replace")
    n += len(re.findall(r"class=['\"]uncovered-line['\"]", t))
print(n)
PY
    )"
    echo "HTML uncovered-line markers (diagnostic): ${UNCOV_TOTAL}"
  fi

  PASS="$(python3 -c "print(1 if float('$LCOV_PCT') + 1e-9 >= float('$LINE_MIN_PCT') else 0)")"
  if [[ "$PASS" -ne 1 ]]; then
    echo "FAIL: line coverage ${LCOV_PCT}% < ${LINE_MIN_PCT}% (${LCOV_HIT}/${LCOV_TOT})" >&2
    cargo llvm-cov report --ignore-filename-regex "$IGNORE" --show-missing-lines || true
    exit 1
  fi
  echo "Coverage OK: ${LCOV_PCT}% ≥ ${LINE_MIN_PCT}% (${LCOV_HIT}/${LCOV_TOT})"
  echo "Note: full branch coverage requires nightly --branch; region-partial lines may still appear in text report."
  echo "Tip: set COVERAGE_CLEAN=1 only when you need a cold instrumented rebuild."
  echo "Tooling: use llvmPackages matching rustc (rustc 1.82 → LLVM 19; see shell.nix)."
  exit 0
fi

echo "cargo-llvm-cov not installed; running tests without coverage gate." >&2
echo "Install: cargo install cargo-llvm-cov --locked" >&2
echo "Or: nix-shell -p cargo-llvm-cov" >&2
cargo test --workspace
echo "WARNING: coverage gate skipped (tooling missing)" >&2
exit 0
