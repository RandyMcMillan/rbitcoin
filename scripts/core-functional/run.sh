#!/usr/bin/env bash
# Run inventory `run` tests via Core test_runner.py. Skip names cannot be invoked.
#
# Default cargo test never calls this. No node for --list / --dry-run.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
INVENTORY="$HERE/inventory.toml"
TESTS_DIR="${ROOT}/third_party/bitcoin/test/functional"
CONFIG_OUT=""
LIST=0
DRY=0
NAMES=()

usage() {
  echo "usage: $0 [--list] [--dry-run] [--inventory FILE] [--tests-dir DIR] [--config-out FILE] [test.py…]" >&2
  exit 2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --list) LIST=1; shift ;;
    --dry-run) DRY=1; shift ;;
    --inventory)
      [[ $# -ge 2 ]] || usage
      INVENTORY="$2"
      shift 2
      ;;
    --tests-dir)
      [[ $# -ge 2 ]] || usage
      TESTS_DIR="$2"
      shift 2
      ;;
    --config-out)
      [[ $# -ge 2 ]] || usage
      CONFIG_OUT="$2"
      shift 2
      ;;
    -h | --help) usage ;;
    --) shift; NAMES+=("$@"); break ;;
    -*) usage ;;
    *) NAMES+=("$1"); shift ;;
  esac
done

CHECK=(python3 "$HERE/check_inventory.py" --inventory "$INVENTORY")
if [[ -d "$TESTS_DIR" ]]; then
  CHECK+=(--tests-dir "$TESTS_DIR")
fi
# Diagnostics on stderr so --list stdout is names only.
"${CHECK[@]}" >&2

mapfile -t RUN_NAMES < <(python3 "$HERE/select_tests.py" --inventory "$INVENTORY" --print-run)
mapfile -t SKIP_NAMES < <(python3 "$HERE/select_tests.py" --inventory "$INVENTORY" --print-skip)

if [[ "$LIST" -eq 1 ]]; then
  if [[ ${#RUN_NAMES[@]} -gt 0 ]]; then
    printf '%s\n' "${RUN_NAMES[@]}"
  fi
  exit 0
fi

if [[ ${#NAMES[@]} -gt 0 ]]; then
  python3 "$HERE/select_tests.py" --inventory "$INVENTORY" --require-run "${NAMES[@]}"
  SELECTED=("${NAMES[@]}")
else
  SELECTED=("${RUN_NAMES[@]+"${RUN_NAMES[@]}"}")
fi

# Normalize to basenames with .py for the runner.
NORM=()
for n in "${SELECTED[@]+"${SELECTED[@]}"}"; do
  base="$(basename "$n")"
  if [[ "$base" != *.py ]]; then
    base="${base}.py"
  fi
  NORM+=("$base")
done

if [[ ${#NORM[@]} -eq 0 ]]; then
  echo "0 run tests"
  exit 0
fi

CORE_ROOT="$(cd "$TESTS_DIR/.." && pwd)"
SRC_DIR="$(cd "$CORE_ROOT/.." && pwd)"
if [[ -z "$CONFIG_OUT" ]]; then
  CONFIG_OUT="${CORE_ROOT}/config.ini"
fi

sed \
  -e "s|@SRCDIR@|${SRC_DIR}|g" \
  -e "s|@BUILDDIR@|${CORE_ROOT}|g" \
  "$HERE/config.ini.template" >"$CONFIG_OUT"

SHIM="${HERE}/bitcoind"
CMD=(
  python3 "${TESTS_DIR}/test_runner.py"
  --v2transport
)
for s in "${SKIP_NAMES[@]+"${SKIP_NAMES[@]}"}"; do
  CMD+=(--exclude "$s")
done
CMD+=("${NORM[@]}")

if [[ "$DRY" -eq 1 ]]; then
  printf '%q ' "${CMD[@]}"
  printf '\n'
  exit 0
fi

if [[ ! -f "${TESTS_DIR}/test_runner.py" ]]; then
  echo "missing ${TESTS_DIR}/test_runner.py (run ./scripts/core-functional/init-submodule.sh)" >&2
  exit 1
fi

export BITCOIND="$SHIM"
exec "${CMD[@]}"
