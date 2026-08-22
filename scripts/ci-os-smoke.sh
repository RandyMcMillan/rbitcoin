#!/usr/bin/env bash
# Native OS smoke for ci.yml windows / macos (and local). Store TableFile
# create/open plus rbitcoin-node --smoke. Not a packaged snapshot.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

export RBITCOIN_HEAD_SCALE="${RBITCOIN_HEAD_SCALE:-tiny}"

if [[ "${CI_OS_SMOKE_DRY_RUN:-}" == "1" ]]; then
  echo "smoke=store-roundtrip+node-smoke"
  echo "RBITCOIN_HEAD_SCALE=$RBITCOIN_HEAD_SCALE"
  exit 0
fi

cargo test -p rbitcoin-store --lib -- \
  file::advise_tests::scripthash_body_create_open_roundtrip

cargo build -p rbitcoin-node -p rbitcoin-cli

bin_root="${CARGO_TARGET_DIR:-target}/debug"
node="${bin_root}/rbitcoin-node"
if [[ -f "${node}.exe" ]]; then
  node="${node}.exe"
fi
if [[ ! -f "$node" ]]; then
  echo "ci-os-smoke: missing $node" >&2
  exit 1
fi

datadir="$(mktemp -d "${TMPDIR:-/tmp}/rbitcoin-os-smoke.XXXXXX")"
cleanup() { rm -rf "$datadir"; }
trap cleanup EXIT

"$node" --smoke --network regtest --datadir "$datadir" \
  --no-seeds --log-level error

body="$datadir/store/scripthash.body"
if [[ -d "$body" ]]; then
  test -f "$body/00"
  test -f "$datadir/store/scripthash.ovf/body"
else
  test -f "$body"
fi
