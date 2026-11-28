#!/usr/bin/env bash
# Production raw-write ceiling for the store and the mempool.
# See lint/page-write-baseline.txt.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
exec python3 "$ROOT/scripts/page-write-ratchet.py" "$@"
