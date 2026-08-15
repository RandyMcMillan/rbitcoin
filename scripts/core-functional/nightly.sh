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
# Today the run set is empty; later this becomes the full `run` suite.
"$HERE/run.sh" --list
echo "core-functional nightly: inventory + release-pin check done"
