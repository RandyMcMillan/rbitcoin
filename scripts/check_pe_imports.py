#!/usr/bin/env python3
"""Refuse Windows PE files that import a non-static CRT or MinGW runtime.

Used by scripts/stage-native-artifacts.sh. Exit 0 if every listed import is
a system DLL; exit 1 and print the forbidden names otherwise.
"""

from __future__ import annotations

import struct
import sys
from pathlib import Path

# Dynamic CRT / MinGW runtimes — these mean the .exe is not CRT-static.
FORBIDDEN = (
    "vcruntime",
    "msvcp",
    "msvcirt",
    "ucrtbase",
    "api-ms-win-crt",
    "libgcc",
    "libstdc++",
    "libwinpthread",
    "libgcc_s",
)


def _u16(b: bytes, off: int) -> int:
    return struct.unpack_from("<H", b, off)[0]


def _u32(b: bytes, off: int) -> int:
    return struct.unpack_from("<I", b, off)[0]


def rva_to_off(data: bytes, rva: int, sections: list[tuple[int, int, int]]) -> int | None:
    for va, vsz, raw in sections:
        if va <= rva < va + max(vsz, 1):
            return raw + (rva - va)
    return None


def read_cstr(data: bytes, off: int) -> str:
    end = data.find(b"\0", off)
    if end < 0:
        end = len(data)
    return data[off:end].decode("ascii", errors="replace")


def pe_import_dlls(data: bytes) -> list[str]:
    if len(data) < 64 or data[0:2] != b"MZ":
        raise ValueError("not a PE (missing MZ)")
    e_lfanew = _u32(data, 0x3C)
    if e_lfanew + 24 > len(data) or data[e_lfanew : e_lfanew + 4] != b"PE\0\0":
        raise ValueError("not a PE (missing PE signature)")
    coff = e_lfanew + 4
    nsections = _u16(data, coff + 2)
    opt_size = _u16(data, coff + 16)
    opt = coff + 20
    magic = _u16(data, opt)
    if magic == 0x10B:
        dd = opt + 96
    elif magic == 0x20B:
        dd = opt + 112
    else:
        raise ValueError(f"unknown optional magic {magic:#x}")
    if dd + 8 > opt + opt_size:
        # Fixture / tiny PE: fall back to scanning ASCII DLL names.
        return scan_ascii_dlls(data)
    import_rva = _u32(data, dd + 8)
    if import_rva == 0:
        return scan_ascii_dlls(data)
    sec_off = opt + opt_size
    sections: list[tuple[int, int, int]] = []
    for i in range(nsections):
        s = sec_off + i * 40
        if s + 40 > len(data):
            break
        va = _u32(data, s + 12)
        vsz = _u32(data, s + 8)
        raw = _u32(data, s + 20)
        sections.append((va, vsz, raw))
    imp_off = rva_to_off(data, import_rva, sections)
    if imp_off is None:
        return scan_ascii_dlls(data)
    dlls: list[str] = []
    pos = imp_off
    while pos + 20 <= len(data):
        fields = struct.unpack_from("<IIIII", data, pos)
        if all(x == 0 for x in fields):
            break
        name_rva = fields[3]
        name_off = rva_to_off(data, name_rva, sections)
        if name_off is not None:
            dlls.append(read_cstr(data, name_off))
        pos += 20
    return dlls or scan_ascii_dlls(data)


def scan_ascii_dlls(data: bytes) -> list[str]:
    """Last-resort: collect ASCII `*.dll` tokens (test fixtures + odd linkers)."""
    out: list[str] = []
    i = 0
    n = len(data)
    while i < n:
        if 65 <= data[i] <= 90 or 97 <= data[i] <= 122:
            j = i
            while j < n and (
                48 <= data[j] <= 57
                or 65 <= data[j] <= 90
                or 97 <= data[j] <= 122
                or data[j] in (ord("."), ord("-"), ord("_"))
            ):
                j += 1
            tok = data[i:j].decode("ascii")
            if tok.lower().endswith(".dll"):
                out.append(tok)
            i = j
        else:
            i += 1
    return out


def forbidden_imports(dlls: list[str]) -> list[str]:
    bad: list[str] = []
    for d in dlls:
        low = d.lower()
        if any(f in low for f in FORBIDDEN):
            bad.append(d)
    return bad


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print("usage: check_pe_imports.py FILE [FILE...]", file=sys.stderr)
        return 2
    status = 0
    for path in argv[1:]:
        data = Path(path).read_bytes()
        try:
            dlls = pe_import_dlls(data)
        except ValueError as e:
            print(f"error: {path}: {e}", file=sys.stderr)
            return 1
        bad = forbidden_imports(dlls)
        if bad:
            print(f"error: {path}: non-static imports: {', '.join(bad)}", file=sys.stderr)
            status = 1
        else:
            print(f"ok: {path}: {', '.join(dlls) or '(no imports)'}")
    return status


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
