#!/usr/bin/env bash
# Periodic multi-node P2P integration suite (heavier than default unit scenarios).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! command -v cargo >/dev/null; then
  echo "enter nix-shell first" >&2
  exit 1
fi

echo "== default multi-node tests =="
cargo test -p rbitcoin-test --test integration_multinode -- --nocapture

echo "== ignored / periodic mesh =="
cargo test -p rbitcoin-test --test integration_multinode -- --ignored --nocapture

echo "integration suite OK"
