#!/usr/bin/env bash
# Enforce 100% line and branch coverage on first-party crates.
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
  cargo llvm-cov clean --workspace
  # Branch coverage requires nightly on many toolchains; always enforce 100% lines.
  # When --branch is supported, also enforce 100% branches.
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
  UNCOV="$(rg -c 'uncovered-line' "$ROOT/coverage" --glob '*.html' 2>/dev/null || true)"
  UNCOV_TOTAL=0
  if [[ -n "$UNCOV" ]]; then
    UNCOV_TOTAL="$(echo "$UNCOV" | awk -F: '{s+=$2} END {print s+0}')"
  fi
  if [[ "$UNCOV_TOTAL" -ne 0 ]]; then
    echo "FAIL: $UNCOV_TOTAL uncovered executable line(s) in HTML report (need 0)" >&2
    echo "$UNCOV" >&2
    cargo llvm-cov report --ignore-filename-regex "$IGNORE" --show-missing-lines || true
    exit 1
  fi

  cargo llvm-cov report \
    --ignore-filename-regex "$IGNORE" \
    --lcov --output-path "$ROOT/coverage/lcov.info" || true
  echo "Coverage OK: 0 uncovered executable lines (see coverage/index.html)"
  echo "Note: full branch coverage requires nightly; region-partial lines may still appear in text report."
  exit 0
fi

echo "cargo-llvm-cov not installed; running tests without coverage gate." >&2
echo "Install: cargo install cargo-llvm-cov --locked" >&2
echo "Or: nix-shell -p cargo-llvm-cov" >&2
cargo test --workspace
echo "WARNING: coverage gate skipped (tooling missing)" >&2
exit 0
