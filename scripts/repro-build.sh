#!/usr/bin/env bash
# Build release rbitcoin-node + rbitcoin-cli via the pinned Nix path.
# Usage:
#   ./scripts/repro-build.sh                 # native package → ./result
#   ./scripts/repro-build.sh aarch64         # cross aarch64 (x86_64 host)
#   ./scripts/repro-build.sh native ./out    # custom out-link path
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

TARGET="${1:-native}"
OUT_LINK="${2:-$ROOT/result}"

if ! command -v nix >/dev/null 2>&1; then
  echo "error: nix is required for reproducible builds" >&2
  exit 1
fi

case "$TARGET" in
  native|x86_64|default|gnu)
    ATTR=".#rbitcoin"
    ;;
  musl|static|secondary)
    ATTR=".#rbitcoin-musl"
    ;;
  aarch64|aarch64-linux)
    ATTR=".#rbitcoin-aarch64"
    ;;
  *)
    echo "usage: $0 [native|musl|aarch64] [out-link]" >&2
    exit 2
    ;;
esac

echo "repro-build: nix build $ATTR --out-link $OUT_LINK"
# Pure evaluation; use flake lock pins only.
nix build "$ATTR" --out-link "$OUT_LINK" --print-build-logs

echo "repro-build: artifacts"
ls -la "$OUT_LINK/bin/"
for b in rbitcoin-node rbitcoin-cli; do
  f="$OUT_LINK/bin/$b"
  if [[ -e "$f" ]]; then
    sha256sum "$f"
    file "$f" || true
  fi
done
