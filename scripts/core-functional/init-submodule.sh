#!/usr/bin/env bash
# Shallow, sparse checkout of bitcoin/bitcoin @ the inventory pin (v31.1).
# cargo test does not need this — only fixture --check and later the Python runner.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
DEST="${ROOT}/third_party/bitcoin"
PIN_SHA="9be056a8a72b624dae9623b2f7bded92c2a21c91"
URL="https://github.com/bitcoin/bitcoin.git"

if [[ ! -e "${DEST}/.git" && ! -f "${DEST}/.git" ]]; then
  mkdir -p "$(dirname "$DEST")"
  git clone --filter=blob:none --sparse --depth 1 --no-checkout "$URL" "$DEST"
  git -C "$DEST" fetch --depth 1 origin tag v31.1
  git -C "$DEST" sparse-checkout set src/test/data test
  git -C "$DEST" checkout "$PIN_SHA"
else
  git -C "$DEST" fetch --depth 1 origin tag v31.1
  git -C "$DEST" sparse-checkout set src/test/data test 2>/dev/null || true
  git -C "$DEST" checkout "$PIN_SHA"
fi

got="$(git -C "$DEST" rev-parse HEAD)"
if [[ "$got" != "$PIN_SHA" ]]; then
  echo "init-submodule: expected $PIN_SHA got $got" >&2
  exit 1
fi
echo "init-submodule: ok $got (v31.1) at $DEST"
