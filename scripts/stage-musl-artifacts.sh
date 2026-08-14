#!/usr/bin/env bash
# Copy musl node/cli from a nix result dir and refuse anything not static.
#
# Usage:
#   ./scripts/stage-musl-artifacts.sh RESULT_DIR DEST_DIR
#
# RESULT_DIR is the `nix build .#rbitcoin-musl --out-link` path (binaries in
# bin/). DEST_DIR receives rbitcoin-node, rbitcoin-cli, and SHA256SUMS.
# `file(1)` must report statically linked (or static-pie).
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 RESULT_DIR DEST_DIR" >&2
  exit 2
fi

RESULT="$1"
DEST="$2"

if ! command -v file >/dev/null 2>&1; then
  echo "error: file(1) is required to verify a static musl link" >&2
  exit 1
fi

mkdir -p "$DEST"

for b in rbitcoin-node rbitcoin-cli; do
  src="$RESULT/bin/$b"
  if [[ ! -e "$src" ]]; then
    echo "error: missing $src" >&2
    exit 1
  fi
  desc="$(file -b "$src" || true)"
  if ! grep -Eiq 'statically linked|static-pie' <<<"$desc"; then
    echo "error: $b is not statically linked: $desc" >&2
    exit 1
  fi
  install -m 755 "$src" "$DEST/$b"
done

(
  cd "$DEST"
  sha256sum rbitcoin-node rbitcoin-cli >SHA256SUMS
)

echo "stage-musl-artifacts: $DEST"
cat "$DEST/SHA256SUMS"
