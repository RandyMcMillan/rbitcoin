#!/usr/bin/env python3
"""Per-file ceiling for raw store and mempool page writes.

Counts call sites under crates/rbitcoin-store/src and
crates/rbitcoin-mempool/src. Skips tests/, benches/, *tests.rs, and
items marked #[cfg(test)]. Comments and strings do not count.

lint/page-write-baseline.txt is sorted `path<TAB>token<TAB>count` lines
for counts above zero. A token above its line, a positive token with no
line, or a line that is now lower (or whose file is gone) fails.
--write rewrites the file from the tree.

`write_all(` is counted only in rbitcoin-mempool. The store uses it to
fill a temp file inside the rename helper, which is not a live page
write. `handle_pwrite_all(` does not match inside `pwrite_all`.

`write_at`, `write_at_pwrite`, `write_uncommitted`, `pwrite_all`,
`persist_hwm`, `persist_logical_len`, `write_trailer`, and `zero_range`
go through `TableFile::pwrite_all` or the same armed-page check. They
are the door, not a raw site. The raw positional write is
`std::os::unix::fs::FileExt`, which shares the door's `write_at` name, so
each mention of `FileExt` (its import or a path-qualified call) is counted.
In a file that mentions `FileExt`, each `write_at(` and `write_all_at(`
call is also counted as `FileExt_write`, door calls included, so a new
method-form raw write moves the baseline even after the import is there.
"""

import argparse
import importlib.util
import os
import re
import sys

CRATES = ("rbitcoin-mempool", "rbitcoin-store")

# (token, pattern, crates that count it). None counts both crates.
TOKENS = (
    ("FileExt", r"(?<![A-Za-z0-9_])FileExt(?![A-Za-z0-9_])", None),
    ("WriteOp", r"(?<![A-Za-z0-9_])WriteOp\s*\{", None),
    ("opcode_Write", r"opcode::Write::new\s*\(", None),
    ("fs_write", r"(?<![A-Za-z0-9_])fs::write\s*\(", None),
    ("handle_pwrite_all", r"(?<![A-Za-z0-9_])handle_pwrite_all\s*\(", None),
    ("pwrite", r"(?<![A-Za-z0-9_])pwrite\s*\(", None),
    ("pwrite_batch", r"(?<![A-Za-z0-9_])pwrite_batch\s*\(", None),
    ("pwrite_exact", r"(?<![A-Za-z0-9_])pwrite_exact\s*\(", None),
    ("set_file_len", r"(?<![A-Za-z0-9_])set_file_len\s*\(", None),
    ("set_len", r"(?<![A-Za-z0-9_])set_len\s*\(", None),
    ("truncate", r"(?<![A-Za-z0-9_])truncate\s*\(", None),
    ("write_all", r"(?<![A-Za-z0-9_])write_all\s*\(", ("rbitcoin-mempool",)),
)

# Counted only in a file whose `FileExt` count is above zero. The method
# form `file.write_at(..)` names no trait, so the import alone would hide
# every later call. Door `write_at` calls in such a file count too.
FILEEXT_WRITE = ("FileExt_write", r"(?<![A-Za-z0-9_])write(?:_all)?_at\s*\(")


def load_unwrap():
    path = os.path.join(os.path.dirname(__file__), "unwrap-ratchet.py")
    spec = importlib.util.spec_from_file_location("unwrap_ratchet", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def scan(root: str) -> dict[str, int]:
    ratchet = load_unwrap()
    compiled = []
    for token, pattern, only in TOKENS + ((*FILEEXT_WRITE, None),):
        # The name may end one line and its `(` or `{` open the next code
        # line. Blank and comment-only lines between them do not count.
        # A bare name (`FileExt`) has no opener to split from.
        call = re.match(r"(.*)\\s\*(\\\(|\\\{)$", pattern)
        split = None
        if call:
            name, opener = call.groups()
            split = (re.compile(name + r"\s*$"), re.compile(r"^\s*" + opener))
        compiled.append((token, re.compile(pattern), only, split))
    counts: dict[str, int] = {}
    for crate in CRATES:
        src = os.path.join(root, "crates", crate, "src")
        if not os.path.isdir(src):
            continue
        for dirpath, dirnames, filenames in os.walk(src):
            dirnames[:] = [d for d in dirnames if d != "target"]
            rel_dir = os.path.relpath(dirpath, root).replace(os.sep, "/")
            parts = rel_dir.split("/")
            if "tests" in parts or "benches" in parts:
                continue
            for name in filenames:
                if not name.endswith(".rs") or name.endswith("tests.rs"):
                    continue
                rel = f"{rel_dir}/{name}"
                full = os.path.join(dirpath, name)
                for token, rx, only, split in compiled:
                    if only is not None and crate not in only:
                        continue
                    if token == FILEEXT_WRITE[0] and f"{rel}\tFileExt" not in counts:
                        continue

                    def line_fn(code, prev, rx=rx, split=split):
                        n = len(rx.findall(code))
                        if split and split[0].search(prev) and split[1].search(code):
                            n += 1
                        return n

                    got = ratchet.file_count(full, line_fn)
                    if got:
                        counts[f"{rel}\t{token}"] = got
    return counts


def read_baseline(path: str) -> dict[str, int]:
    if not os.path.exists(path):
        sys.exit(f"missing baseline {path}")
    out: dict[str, int] = {}
    for raw in open(path, encoding="utf-8"):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        path_s, token, count_s = line.split("\t")
        out[f"{path_s}\t{token}"] = int(count_s)
    return out


def write_baseline(path: str, counts: dict[str, int]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    lines = [f"{key}\t{counts[key]}\n" for key in sorted(counts)]
    with open(path, "w", encoding="utf-8") as handle:
        handle.writelines(lines)


def check(counts: dict[str, int], base: dict[str, int]) -> list[str]:
    errors = []
    for key in sorted(set(counts) | set(base)):
        got = counts.get(key, 0)
        if key not in base:
            errors.append(f"{key}: {got} raw writes, baseline has no line")
            continue
        want = base[key]
        if got > want:
            errors.append(f"{key}: {got} raw writes, baseline {want}")
        elif got < want:
            errors.append(f"{key}: {got} raw writes, baseline {want} (lower the line)")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=None)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    root = os.path.abspath(args.root or os.path.join(os.path.dirname(__file__), ".."))
    baseline = os.path.join(root, "lint", "page-write-baseline.txt")
    counts = scan(root)
    if args.write:
        write_baseline(baseline, counts)
        print(f"wrote {len(counts)} lines to {baseline}")
        return 0
    errors = check(counts, read_baseline(baseline))
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
