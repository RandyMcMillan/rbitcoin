#!/usr/bin/env bash
# Copy Windows / Darwin node+cli from a cargo release dir and refuse extra
# runtime libraries (VC++/MinGW CRT on Windows; Homebrew/Nix dylibs on Darwin).
#
# These are *not* Linux-musl fully static binaries. Apple does not allow a
# fully static libSystem link; Windows always loads KERNEL32. The contract is
# "no extra non-OS runtime" so the artifact runs without a Nix store, VS
# redistributable, or Homebrew.
#
# Usage:
#   ./scripts/stage-native-artifacts.sh windows SRC_DIR DEST_DIR
#   ./scripts/stage-native-artifacts.sh darwin  SRC_DIR DEST_DIR
#
# SRC_DIR holds cargo --release outputs (rbitcoin-node[.exe], rbitcoin-cli[.exe]).
set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "usage: $0 windows|darwin SRC_DIR DEST_DIR" >&2
  exit 2
fi

KIND="$1"
SRC="$2"
DEST="$3"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

sha256_dir() {
  local dir="$1"
  shift
  (
    cd "$dir"
    if command -v sha256sum >/dev/null 2>&1; then
      sha256sum "$@"
    else
      shasum -a 256 "$@"
    fi
  ) >"$dir/SHA256SUMS"
}

stage_pair() {
  local -a names=("$@")
  mkdir -p "$DEST"
  for b in "${names[@]}"; do
    local src="$SRC/$b"
    if [[ ! -e "$src" ]]; then
      echo "error: missing $src" >&2
      exit 1
    fi
    cp "$src" "$DEST/$b"
    chmod 755 "$DEST/$b"
  done
}

sign_darwin_adhoc() {
  if ! command -v codesign >/dev/null 2>&1; then
    if [[ "$(uname -s)" == Darwin ]]; then
      echo "error: codesign is required to ad-hoc sign Darwin binaries" >&2
      exit 1
    fi
    return 0
  fi
  local b
  for b in rbitcoin-node rbitcoin-cli; do
    # Ad-hoc identity (`-`). No Apple timestamp (no Developer ID on CI).
    codesign --sign - --force --timestamp=none "$DEST/$b"
    if ! codesign --verify "$DEST/$b"; then
      echo "error: codesign verify failed for $DEST/$b" >&2
      exit 1
    fi
  done
}

check_darwin_otool() {
  local bin="$1"
  if ! command -v otool >/dev/null 2>&1; then
    echo "error: otool is required to verify Darwin dylibs" >&2
    exit 1
  fi
  local lines
  lines="$(otool -L "$bin")"
  # First line is `path:`; remaining are tab + install-name + version.
  while IFS= read -r line; do
    [[ "$line" == *: ]] && continue
    local lib
    lib="$(sed -E 's/^[[:space:]]+//; s/ \(compatibility.*//' <<<"$line")"
    [[ -z "$lib" ]] && continue
    case "$lib" in
    /usr/lib/* | /System/Library/*) ;;
    *)
      echo "error: $bin links non-system dylib: $lib" >&2
      echo "$lines" >&2
      exit 1
      ;;
    esac
  done <<<"$lines"
}

case "$KIND" in
windows)
  stage_pair rbitcoin-node.exe rbitcoin-cli.exe
  py=""
  if command -v python3 >/dev/null 2>&1; then
    py=python3
  elif command -v python >/dev/null 2>&1; then
    py=python
  else
    echo "error: python3 is required to check Windows PE imports" >&2
    exit 1
  fi
  "$py" "$ROOT/scripts/check_pe_imports.py" \
    "$DEST/rbitcoin-node.exe" "$DEST/rbitcoin-cli.exe"
  sha256_dir "$DEST" rbitcoin-node.exe rbitcoin-cli.exe
  ;;
darwin)
  stage_pair rbitcoin-node rbitcoin-cli
  check_darwin_otool "$DEST/rbitcoin-node"
  check_darwin_otool "$DEST/rbitcoin-cli"
  sign_darwin_adhoc
  sha256_dir "$DEST" rbitcoin-node rbitcoin-cli
  ;;
*)
  echo "error: kind must be windows or darwin (got $KIND)" >&2
  exit 2
  ;;
esac

echo "stage-native-artifacts: $KIND $DEST"
cat "$DEST/SHA256SUMS"
