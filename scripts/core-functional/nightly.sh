#!/usr/bin/env bash
# Nightly / labeled-PR Core functional gate.
#
# Warns (does not fail) when a newer Bitcoin Core *release* exists than the
# inventory pin — bump the submodule, fixtures, and inventory when that fires.
# Default cargo test never calls this.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
cd "$ROOT"

./scripts/core-functional/init-submodule.sh
python3 "$HERE/check_inventory.py" \
  --tests-dir "$ROOT/third_party/bitcoin/test/functional"
# Warn-only: a newer Core release must not red the job.
python3 "$HERE/check_core_release.py"
# Inventory `run` set (feature_help + feature_uacomment today). Needs a node.
if [[ -n "${RBITCOIN_NODE:-}" && -x "${RBITCOIN_NODE}" ]]; then
  :
elif command -v cargo >/dev/null 2>&1; then
  cargo build -p rbitcoin-node
  if [[ -x "${CARGO_TARGET_DIR:-target}/debug/rbitcoin-node" ]]; then
    export RBITCOIN_NODE="${CARGO_TARGET_DIR:-target}/debug/rbitcoin-node"
  elif [[ -x target/dev/debug/rbitcoin-node ]]; then
    export RBITCOIN_NODE="target/dev/debug/rbitcoin-node"
  fi
fi
if [[ -n "${RBITCOIN_NODE:-}" && -x "${RBITCOIN_NODE}" ]]; then
  "$HERE/run.sh"
else
  echo "core-functional nightly: no rbitcoin-node; --list only" >&2
  "$HERE/run.sh" --list
fi
echo "core-functional nightly: inventory + release-pin check done"
