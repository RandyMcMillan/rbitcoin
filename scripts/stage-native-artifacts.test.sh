#!/usr/bin/env bash
# Contract pin for scripts/stage-native-artifacts.sh (no cargo / no cross compile).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
STAGE="$ROOT/scripts/stage-native-artifacts.sh"
PASS=0
FAIL=0

assert_ok() {
  local name="$1"
  shift
  if "$@"; then
    echo "ok - $name"
    PASS=$((PASS + 1))
  else
    echo "not ok - $name"
    FAIL=$((FAIL + 1))
  fi
}

assert_fail() {
  local name="$1"
  shift
  if "$@" >/dev/null 2>&1; then
    echo "not ok - $name (expected failure)"
    FAIL=$((FAIL + 1))
  else
    echo "ok - $name"
    PASS=$((PASS + 1))
  fi
}

WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/rbitcoin-stage-native.XXXXXX")"
cleanup() { rm -rf "$WORKDIR"; }
trap cleanup EXIT

assert_ok "script exists" test -x "$STAGE"

# --- Darwin: otool -L must be system dylibs only ---
MOCK="$WORKDIR/mockbin"
mkdir -p "$MOCK"
cat >"$MOCK/otool" <<'EOF'
#!/usr/bin/env bash
# last arg is the binary path
bin="${@: -1}"
echo "$bin:"
if [[ "${MOCK_OTOOL_MODE:-ok}" == "homebrew" ]]; then
  echo $'\t/opt/homebrew/opt/openssl/lib/libssl.3.dylib (compatibility version 3.0.0, current version 3.0.0)'
else
  echo $'\t/usr/lib/libSystem.B.dylib (compatibility version 1.0.0, current version 1351.0.0)'
fi
EOF
chmod +x "$MOCK/otool"

cat >"$MOCK/codesign" <<'EOF'
#!/usr/bin/env bash
if [[ "${MOCK_CODESIGN_FAIL:-}" == "1" && ( "$1" == "--verify" || "$1" == "-v" ) ]]; then
  echo "code object is not signed" >&2
  exit 1
fi
echo "$*" >> "${CODESIGN_LOG:-/dev/null}"
exit 0
EOF
chmod +x "$MOCK/codesign"

DARWIN_SRC="$WORKDIR/darwin-src"
DARWIN_DEST="$WORKDIR/darwin-dest"
mkdir -p "$DARWIN_SRC"
printf 'node\n' >"$DARWIN_SRC/rbitcoin-node"
printf 'cli\n' >"$DARWIN_SRC/rbitcoin-cli"
chmod +x "$DARWIN_SRC/rbitcoin-node" "$DARWIN_SRC/rbitcoin-cli"

CODESIGN_LOG="$WORKDIR/codesign.log"
assert_ok "darwin system-only dylibs stage" \
  env PATH="$MOCK:$PATH" MOCK_OTOOL_MODE=ok CODESIGN_LOG="$CODESIGN_LOG" \
  "$STAGE" darwin "$DARWIN_SRC" "$DARWIN_DEST"
assert_ok "darwin copied node" test -f "$DARWIN_DEST/rbitcoin-node"
assert_ok "darwin copied cli" test -f "$DARWIN_DEST/rbitcoin-cli"
assert_ok "darwin sha256sums present" test -s "$DARWIN_DEST/SHA256SUMS"
assert_ok "darwin ad-hoc codesign node" grep -q 'rbitcoin-node' "$CODESIGN_LOG"
assert_ok "darwin ad-hoc codesign cli" grep -q 'rbitcoin-cli' "$CODESIGN_LOG"

rm -rf "$DARWIN_DEST"
: >"$CODESIGN_LOG"
assert_fail "darwin codesign verify failure is refused" \
  env PATH="$MOCK:$PATH" MOCK_OTOOL_MODE=ok MOCK_CODESIGN_FAIL=1 \
  "$STAGE" darwin "$DARWIN_SRC" "$DARWIN_DEST"

rm -rf "$DARWIN_DEST"
assert_fail "darwin homebrew dylib is refused" \
  env PATH="$MOCK:$PATH" MOCK_OTOOL_MODE=homebrew \
  "$STAGE" darwin "$DARWIN_SRC" "$DARWIN_DEST"

# --- Windows: PE import denylist (CRT / mingw) ---
WIN_SRC="$WORKDIR/win-src"
WIN_DEST="$WORKDIR/win-dest"
mkdir -p "$WIN_SRC"
# Tiny PE fixtures: kernel32-only vs vcruntime.
python3 - "$WIN_SRC" <<'PY'
import struct, sys
from pathlib import Path

out = Path(sys.argv[1])

def pe_with_imports(dlls: list[str]) -> bytes:
    # Minimal PE32+ with an import directory listing `dlls`.
    dos = bytearray(64)
    dos[0:2] = b"MZ"
    struct.pack_into("<I", dos, 0x3C, 64)
    # PE header + coff + optional (PE32+) + 1 section
    pe = bytearray()
    pe += b"PE\0\0"
    pe += struct.pack("<HHIIIHH", 0x8664, 1, 0, 0, 0, 0xF0, 0x0002)  # coff
    # Optional header PE32+
    opt = bytearray(0xF0)
    struct.pack_into("<H", opt, 0, 0x20B)
    struct.pack_into("<I", opt, 16, 0x1000)  # entry
    struct.pack_into("<Q", opt, 24, 0x140000000)  # image base
    struct.pack_into("<I", opt, 32, 0x1000)  # section align
    struct.pack_into("<I", opt, 36, 0x200)  # file align
    struct.pack_into("<H", opt, 64, 6)  # major subsystem
    struct.pack_into("<H", opt, 68, 3)  # subsystem = console
    struct.pack_into("<I", opt, 56, 0x2000)  # size of image
    struct.pack_into("<I", opt, 60, 0x200)  # size of headers
    struct.pack_into("<I", opt, 108, 16)  # number of RVA/sizes
    # Import directory at RVA 0x1200, file offset 0x400
    struct.pack_into("<II", opt, 120, 0x1200, 0x80)
    pe += opt
    # Section .rdata
    sec = bytearray(40)
    sec[0:6] = b".rdata"
    struct.pack_into("<I", sec, 8, 0x400)  # virtual size
    struct.pack_into("<I", sec, 12, 0x1000)  # virtual address
    struct.pack_into("<I", sec, 16, 0x400)  # raw size
    struct.pack_into("<I", sec, 20, 0x200)  # raw ptr
    struct.pack_into("<I", sec, 36, 0x40000040)
    pe += sec
    header = bytes(dos) + bytes(pe)
    header = header.ljust(0x200, b"\0")
    # Import descriptors + names at file 0x200 (RVA 0x1000) — keep simple:
    # Place names at 0x200 and descriptors at 0x200; checker only needs DLL names
    # as ASCII in the file for our production parser's fallback scan.
    blob = header + b"\0" * 0x200
    extra = b"".join(d.encode("ascii") + b"\0" for d in dlls)
    return blob + extra

(out / "rbitcoin-node.exe").write_bytes(pe_with_imports(["KERNEL32.dll"]))
(out / "rbitcoin-cli.exe").write_bytes(pe_with_imports(["KERNEL32.dll"]))
(out / "bad-node.exe").write_bytes(pe_with_imports(["KERNEL32.dll", "VCRUNTIME140.dll"]))
(out / "bad-cli.exe").write_bytes(pe_with_imports(["KERNEL32.dll"]))
PY

assert_ok "windows crt-static pair stages" \
  "$STAGE" windows "$WIN_SRC" "$WIN_DEST"
assert_ok "windows copied node.exe" test -f "$WIN_DEST/rbitcoin-node.exe"
assert_ok "windows copied cli.exe" test -f "$WIN_DEST/rbitcoin-cli.exe"
assert_ok "windows sha256sums present" test -s "$WIN_DEST/SHA256SUMS"
assert_ok "windows sha256sums names node" grep -q 'rbitcoin-node.exe' "$WIN_DEST/SHA256SUMS"

BAD_SRC="$WORKDIR/win-bad"
mkdir -p "$BAD_SRC"
cp "$WIN_SRC/bad-node.exe" "$BAD_SRC/rbitcoin-node.exe"
cp "$WIN_SRC/bad-cli.exe" "$BAD_SRC/rbitcoin-cli.exe"
rm -rf "$WIN_DEST"
assert_fail "windows vcruntime import is refused" \
  "$STAGE" windows "$BAD_SRC" "$WIN_DEST"

rm -rf "$WIN_DEST"
rm -f "$WIN_SRC/rbitcoin-cli.exe"
assert_fail "windows missing binary is refused" \
  "$STAGE" windows "$WIN_SRC" "$WIN_DEST"

echo
echo "passed=$PASS failed=$FAIL"
test "$FAIL" -eq 0
