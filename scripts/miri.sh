#!/usr/bin/env bash
# Nightly Miri for rbitcoin-primitives only (Q-53). rust-toolchain.toml
# pins 1.95; Miri needs nightly. Never --workspace (store io_uring, net,
# secp FFI). Callers inherit RUSTUP_TOOLCHAIN if already set.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

export RUSTUP_TOOLCHAIN="${RUSTUP_TOOLCHAIN:-nightly}"
cmd=(cargo "+${RUSTUP_TOOLCHAIN}" miri test -p rbitcoin-primitives)

if [[ "${MIRI_DRY_RUN:-}" == "1" ]]; then
  echo "RUSTUP_TOOLCHAIN=$RUSTUP_TOOLCHAIN"
  echo "${cmd[*]}"
  exit 0
fi

if ! command -v cargo >/dev/null 2>&1; then
  echo "miri.sh: cargo not found" >&2
  exit 2
fi

if ! cargo "+${RUSTUP_TOOLCHAIN}" miri --version >/dev/null 2>&1; then
  echo "miri.sh: nightly miri missing. rustup toolchain install nightly && rustup component add miri --toolchain nightly" >&2
  exit 2
fi

exec "${cmd[@]}"
