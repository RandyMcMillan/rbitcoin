#!/usr/bin/env bash
# CRAP report after a successful LCOV gate (Q-52). Report-only: missing
# cargo-crap or a tool error does not fail coverage. No --fail-above.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LCOV="${1:-$ROOT/coverage/lcov.info}"
OUT="${2:-$ROOT/coverage/crap.json}"

SUMMARY=(cargo crap --workspace --lcov "$LCOV" --summary)
JSON=(cargo crap --workspace --lcov "$LCOV" --format json --sort file --output "$OUT")

if [[ "${CRAP_DRY_RUN:-}" == "1" ]]; then
  echo "${SUMMARY[*]}"
  echo "${JSON[*]}"
  exit 0
fi

if ! command -v cargo >/dev/null 2>&1; then
  echo "coverage-crap: skip (cargo not on PATH)"
  exit 0
fi
if ! command -v cargo-crap >/dev/null 2>&1 && ! cargo crap --help >/dev/null 2>&1; then
  echo "coverage-crap: skip (cargo-crap not installed)"
  exit 0
fi

if [[ ! -f "$LCOV" ]]; then
  echo "coverage-crap: skip (missing $LCOV)"
  exit 0
fi

mkdir -p "$(dirname "$OUT")"
if ! "${SUMMARY[@]}"; then
  echo "coverage-crap: summary failed (report-only)" >&2
fi
if ! "${JSON[@]}"; then
  echo "coverage-crap: json failed (report-only)" >&2
fi
exit 0
