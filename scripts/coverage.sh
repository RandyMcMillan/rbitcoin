#!/usr/bin/env bash
# Enforce 100% line coverage on first-party crates (HTML uncovered-line = 0).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

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
IGNORE='(/\.cargo/|/rustc-|/nix/store/|library/std/|/src/main\.rs$)'

if command -v cargo-llvm-cov >/dev/null 2>&1 || cargo llvm-cov --version >/dev/null 2>&1; then
  # Default: do NOT clean instrumented artifacts. Incremental llvm-cov rebuilds
  # are much faster for iterative work and still re-run all tests with coverage.
  # Force a full clean when debugging stale counters: COVERAGE_CLEAN=1 ./scripts/coverage.sh
  if [[ "${COVERAGE_CLEAN:-0}" == "1" ]]; then
    echo "COVERAGE_CLEAN=1: wiping llvm-cov workspace artifacts"
    cargo llvm-cov clean --workspace
  fi

  # Branch coverage requires nightly on many toolchains; always enforce 100% lines.
  EXTRA=()
  if cargo llvm-cov test --help 2>&1 | grep -q -- '--fail-under-branches'; then
    if rustc -vV 2>/dev/null | grep -q nightly; then
      EXTRA+=(--branch --fail-under-branches 100)
    fi
  fi
  mkdir -p "$ROOT/coverage"
  cargo llvm-cov test --workspace \
    --ignore-filename-regex "$IGNORE" \
    "${EXTRA[@]}" \
    --html --output-dir "$ROOT/coverage"

  REPORT="$(cargo llvm-cov report --ignore-filename-regex "$IGNORE" 2>/dev/null || true)"
  echo "$REPORT"

  # cargo-llvm-cov's text "Missed Lines" counts partially-covered *regions* inside a line
  # (e.g. match or-patterns). The HTML report's uncovered-line markers are the source of
  # truth for "every executable line executed at least once".
  cargo llvm-cov report \
    --ignore-filename-regex "$IGNORE" \
    --lcov --output-path "$ROOT/coverage/lcov.info" || true

  # Line gate: LCOV LH/LF (HTML class names vary by llvm-cov version).
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
  if [[ "$LCOV_TOT" -gt 0 ]]; then
    LCOV_PCT="$(python3 -c "print(f'{100.0*$LCOV_HIT/$LCOV_TOT:.2f}')")"
    echo "LCOV lines: ${LCOV_HIT}/${LCOV_TOT} (${LCOV_PCT}%)"
  fi
  # `coverage/` is gitignored — need --no-ignore or rg finds nothing (false 0).
  UNCOV="$(rg --no-ignore -c 'uncovered-line' "$ROOT/coverage" --glob '*.html' 2>/dev/null || true)"
  UNCOV_TOTAL=0
  if [[ -n "$UNCOV" ]]; then
    UNCOV_TOTAL="$(echo "$UNCOV" | awk -F: '{s+=$2} END {print s+0}')"
  fi
  HTML_PRESENT=0
  if find "$ROOT/coverage" -name '*.html' 2>/dev/null | head -1 | grep -q .; then
    HTML_PRESENT=1
  fi
  # Prefer HTML uncovered-line (COVERAGE.md) when report exists; LCOV is backup
  # when HTML is missing (some llvm-cov versions). LCOV also counts region-
  # partial / non-executable DA:0 rows — do not hard-fail LCOV when HTML is 0.
  MISS=$((LCOV_TOT > LCOV_HIT ? LCOV_TOT - LCOV_HIT : 0))
  if [[ "$HTML_PRESENT" -eq 1 ]]; then
    echo "HTML uncovered-line markers: ${UNCOV_TOTAL}"
    if [[ "$UNCOV_TOTAL" -ne 0 ]]; then
      echo "FAIL: $UNCOV_TOTAL uncovered executable line(s) in HTML report (need 0)" >&2
      echo "$UNCOV" | head -40 >&2
      cargo llvm-cov report --ignore-filename-regex "$IGNORE" --show-missing-lines || true
      exit 1
    fi
    echo "Coverage OK: 0 uncovered executable lines (HTML; see coverage/html/)"
    if [[ "$MISS" -ne 0 ]]; then
      echo "Note: LCOV DA rows still show ${MISS} zero-hit entries (${LCOV_HIT}/${LCOV_TOT}); region-partial / non-exec may remain — HTML line gate is authoritative."
    fi
  else
    if [[ "$MISS" -ne 0 ]]; then
      echo "FAIL: no HTML report and LCOV reports $MISS missed line(s) (${LCOV_HIT}/${LCOV_TOT}; need 100%)" >&2
      cargo llvm-cov report --ignore-filename-regex "$IGNORE" --show-missing-lines || true
      exit 1
    fi
    echo "Coverage OK: LCOV full hit (no HTML report)"
  fi
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
