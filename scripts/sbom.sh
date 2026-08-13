#!/usr/bin/env bash
# CycloneDX 1.5 from Cargo.lock (Q-21). After nix build .#rbitcoin-musl, keep
# the JSON next to release bits.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
OUT="${1:-$ROOT/rbitcoin.cdx.json}"
python3 "$ROOT/scripts/sbom.py" --lock "$ROOT/Cargo.lock" --out "$OUT"
echo "sbom: wrote $OUT"
