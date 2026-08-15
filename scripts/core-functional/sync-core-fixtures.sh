#!/usr/bin/env bash
# Compare or copy Bitcoin Core src/test/data JSON corpora into our fixtures/.
#
# Default Core tree: third_party/bitcoin (submodule, pin v31.1).
# cargo test uses the in-tree copies and does not need the submodule.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
MODE=""
CORE_DATA="${ROOT}/third_party/bitcoin/src/test/data"
FIXTURES="${ROOT}/crates/rbitcoin-consensus/tests/fixtures"
FILES=(script_tests.json tx_valid.json tx_invalid.json)

usage() {
  echo "usage: $0 --check|--write [--core-data DIR] [--fixtures DIR]" >&2
  exit 2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --check) MODE=check; shift ;;
    --write) MODE=write; shift ;;
    --core-data)
      [[ $# -ge 2 ]] || usage
      CORE_DATA="$2"
      shift 2
      ;;
    --fixtures)
      [[ $# -ge 2 ]] || usage
      FIXTURES="$2"
      shift 2
      ;;
    -h | --help) usage ;;
    *) usage ;;
  esac
done

[[ -n "$MODE" ]] || usage

if [[ ! -d "$CORE_DATA" ]]; then
  echo "missing core data dir: $CORE_DATA" >&2
  echo "hint: git submodule update --init --depth 1 third_party/bitcoin" >&2
  exit 1
fi

if [[ ! -d "$FIXTURES" ]]; then
  echo "missing fixtures dir: $FIXTURES" >&2
  exit 1
fi

err=0
for f in "${FILES[@]}"; do
  src="${CORE_DATA}/${f}"
  dst="${FIXTURES}/${f}"
  if [[ ! -f "$src" ]]; then
    echo "missing core fixture: ${f}" >&2
    err=1
    continue
  fi
  if [[ "$MODE" == write ]]; then
    cp -f "$src" "$dst"
    echo "wrote ${dst}"
    continue
  fi
  if [[ ! -f "$dst" ]]; then
    echo "missing in-tree fixture: ${f}" >&2
    err=1
    continue
  fi
  if ! cmp -s "$src" "$dst"; then
    echo "fixture mismatch: ${f}" >&2
    err=1
  fi
done

if [[ "$MODE" == check ]]; then
  if [[ "$err" -ne 0 ]]; then
    echo "sync-core-fixtures: check failed" >&2
    exit 1
  fi
  echo "sync-core-fixtures: ok (${#FILES[@]} files match ${CORE_DATA})"
fi
exit "$err"
