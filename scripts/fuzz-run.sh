#!/usr/bin/env bash
# Nightly libFuzzer for fuzz/block_wire. rust-toolchain.toml pins 1.95;
# cargo-fuzz needs nightly -Zsanitizer. Callers inherit RUSTUP_TOOLCHAIN
# if already set; default is nightly.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

export RUSTUP_TOOLCHAIN="${RUSTUP_TOOLCHAIN:-nightly}"

# Prebuilt cargo-fuzz (taiki-e/install-action musl binary) uses
# CURRENT_PLATFORM as cargo --target, so it builds
# x86_64-unknown-linux-musl. ASan cannot use statically linked musl
# (`sanitizer is incompatible with statically linked libc`). Pin the
# rustc host triple (gnu on GHA ubuntu-latest).
target="$(rustc "+${RUSTUP_TOOLCHAIN}" -vV 2>/dev/null | sed -n 's/^host: //p' || true)"
if [[ -z "$target" ]]; then
  target="$(rustc -vV | sed -n 's/^host: //p')"
fi
if [[ -z "$target" ]]; then
  echo "fuzz-run: could not read rustc host triple" >&2
  exit 1
fi

if [[ "${FUZZ_DRY_RUN:-}" == "1" ]]; then
  echo "RUSTUP_TOOLCHAIN=$RUSTUP_TOOLCHAIN"
  echo "CARGO_FUZZ_TARGET=$target"
  exit 0
fi

mkdir -p fuzz/corpus/block_wire
cp crates/rbitcoin-consensus/tests/fixtures/signet_block_1.bin \
  fuzz/corpus/block_wire/signet_block_1.bin

exec cargo fuzz run --target "$target" block_wire -- \
  -max_total_time="${FUZZ_MAX_TOTAL_TIME:-120}" \
  -timeout=10 \
  -max_len=1048576
