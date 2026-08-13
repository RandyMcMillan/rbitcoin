#!/usr/bin/env python3
"""Offline repair: remove a double-appended idx window (clone of prior starts).

Schema 15 stems are txout.idx / inwit.idx / spent.idx (coupled). Packed tx.idx
is historic (schema ≤14). The node refuses a non-monotone tail on TxIdx::open;
this script is the optional compact, not an in-process heal.

Mainnet 2026-08-07 pattern (verified offline):
  source fks [L-W+1, L] identical u32 starts as ghost fks [L+1, L+W]
  with L=1412912843, W=3330.

Body was NOT re-written for the ghost window (next real body starts at start(L+1+W)).
This tool:
  1) Verifies the clone
  2) Compacts Class A: drop ghost W slots; shift later idx starts and txid.body down by W
  3) Adjusts header_txs_first for first_fk > L+W
  4) Zeros header_txs for headers whose body first_fk falls inside the ghost window
  5) Leaves tip Class C alone — operator must disconnect tip past any create_fk > new_count
     OR re-run with --disconnect-tip (uses confirmed.body length only as a coarse cut)

Default is --dry-run. Pass --apply to write.
"""
from __future__ import annotations

import argparse
import os
import struct
import sys
from pathlib import Path

FILE_HEADER_LEN = 16
IDX_STRIDE = 8
TXID_ENTRY = 32
# Must match crates/rbitcoin-store/src/txid_body.rs TXID_BODY_HEADER (TableFile 16 + pad).
TXID_HEADER = 32
ELEM = 8


def read_idx_meta(idx_dir: Path):
    meta = (idx_dir / "meta").read_bytes()
    magic = meta[0:4]
    if magic != b"RBT1":
        raise SystemExit(f"bad idx meta magic {magic!r}")
    schema, kind = struct.unpack_from("<HH", meta, 4)
    meta_ver, seg_count, _ = struct.unpack_from("<III", meta, 8)
    segs = []
    off = 20
    for _ in range(seg_count):
        first_fk, count, body_base = struct.unpack_from("<QQQ", meta, off)
        file_id, _res = struct.unpack_from("<II", meta, off + 24)
        segs.append(
            {
                "first_fk": first_fk,
                "count": count,
                "body_base": body_base,
                "file_id": file_id,
            }
        )
        off += 32
    return schema, segs


def load_seg_u32s(idx_dir: Path, seg) -> memoryview:
    path = idx_dir / f"{seg['file_id']:06d}"
    data = bytearray(path.read_bytes())
    return memoryview(data)


def u32_at(mv: memoryview, slot: int) -> int:
    off = FILE_HEADER_LEN + slot * 4
    return struct.unpack_from("<I", mv, off)[0]


def set_u32(mv: memoryview, slot: int, val: int) -> None:
    off = FILE_HEADER_LEN + slot * 4
    struct.pack_into("<I", mv, off, val)


def abs_start(seg, u: int) -> int:
    return seg["body_base"] + u * IDX_STRIDE


def detect_clone(segs, idx_dir: Path):
    """Return (last_good, ghost_n, seg_index) or raise."""
    # Scan last segment for first inversion, then measure clone length.
    seg = segs[-1]
    mv = load_seg_u32s(idx_dir, seg)
    n = int(seg["count"])
    inv = None
    prev = abs_start(seg, u32_at(mv, 0))
    for i in range(1, n):
        s = abs_start(seg, u32_at(mv, i))
        if s < prev:
            inv = i  # slot i has start < slot i-1; bad range is for fk = first+i-1
            break
        prev = s
    if inv is None:
        raise SystemExit("no idx inversion found in last segment — nothing to repair")
    # last_good fk = first_fk + inv - 1  (slot inv-1)
    last_good = seg["first_fk"] + inv - 1
    # ghost starts at slot inv; measure how long u32s match prior window of same length
    # Pattern: dst[i] == src[i] for i in 0..W-1 where dst_start = inv, src_start = inv - W
    # W is unknown: distance from first of prior equal run.
    # From forensics W = inv - (src_start relative). Try W such that
    # slot(inv + k) == slot(inv - W + k) for k in 0..W-1 and inv-W >= 0.
    # The source window ends at inv-1 (last_good slot). Source length W means
    # source slots [inv-W, inv-1] match dest [inv, inv+W-1].
    best_w = None
    for w in range(1, inv + 1):
        ok = True
        if inv + w > n:
            break
        for k in range(w):
            if u32_at(mv, inv - w + k) != u32_at(mv, inv + k):
                ok = False
                break
        if ok:
            best_w = w
    if not best_w:
        raise SystemExit(f"inversion at slot {inv} but no exact clone window found")
    # Prefer maximal w
    w = best_w
    # extend maximal
    while inv + w < n and inv - w >= 0:
        if u32_at(mv, inv - w - 1 + w) == u32_at(mv, inv + w) if False else True:
            # try w+1
            nw = w + 1
            if inv - nw < 0 or inv + nw > n:
                break
            if all(u32_at(mv, inv - nw + k) == u32_at(mv, inv + k) for k in range(nw)):
                w = nw
            else:
                break
        else:
            break
    # recompute maximal properly
    w = best_w
    while True:
        nw = w + 1
        if inv - nw < 0 or inv + nw > n:
            break
        if all(u32_at(mv, inv - nw + k) == u32_at(mv, inv + k) for k in range(nw)):
            w = nw
        else:
            break
    return last_good, w, len(segs) - 1, inv


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("store_dir", type=Path, help="path to store/ directory")
    ap.add_argument("--apply", action="store_true", help="write changes (default dry-run)")
    ap.add_argument(
        "--last-good",
        type=int,
        default=None,
        help="override last good create_fk (default: auto-detect)",
    )
    ap.add_argument(
        "--ghost-n",
        type=int,
        default=None,
        help="override ghost window length (default: auto-detect)",
    )
    args = ap.parse_args()
    store = args.store_dir
    idx_dir = store / "tx.idx"
    if not idx_dir.is_dir():
        raise SystemExit(f"missing {idx_dir}")

    schema, segs = read_idx_meta(idx_dir)
    total = sum(s["count"] for s in segs)
    print(f"schema={schema} segments={len(segs)} class_a_count={total}")

    if args.last_good is not None and args.ghost_n is not None:
        last_good, ghost_n = args.last_good, args.ghost_n
        si = len(segs) - 1
        inv = last_good - segs[si]["first_fk"] + 1
    else:
        last_good, ghost_n, si, inv = detect_clone(segs, idx_dir)

    seg = segs[si]
    print(
        f"repair plan: last_good={last_good} ghost_n={ghost_n} "
        f"ghost=[{last_good+1}..{last_good+ghost_n}] "
        f"new_count={total - ghost_n}"
    )

    mv = bytearray(load_seg_u32s(idx_dir, seg))
    # verify clone
    for k in range(ghost_n):
        a = u32_at(mv, inv - ghost_n + k)
        b = u32_at(mv, inv + k)
        if a != b:
            raise SystemExit(f"clone verify failed at k={k}: {a} != {b}")
    print("clone verify: OK")

    new_count = total - ghost_n
    # Compact idx in last segment: slots from inv .. end-ghost_n get values from inv+ghost_n ..
    # slot for fk F is F - first_fk
    first = seg["first_fk"]
    old_seg_count = int(seg["count"])
    # number of fks after ghost in this segment
    # last fk in segment = first + old_seg_count - 1
    last_fk_seg = first + old_seg_count - 1
    # slots to keep after last_good: from last_good+1+ghost_n .. last_fk_seg
    # written starting at slot for last_good+1
    src0 = (last_good + 1 + ghost_n) - first  # first source slot to keep
    dst0 = (last_good + 1) - first
    n_keep = last_fk_seg - (last_good + ghost_n)
    if n_keep < 0:
        raise SystemExit("n_keep negative")
    print(f"shift {n_keep} idx slots: slot[{src0}..] -> slot[{dst0}..]")

    if args.apply:
        for i in range(n_keep):
            set_u32(mv, dst0 + i, u32_at(mv, src0 + i))
        new_seg_count = (last_good - first + 1) + n_keep
        # Published HWM in TableFile header (u64 @ offset 8) must equal
        # FILE_HEADER + count*4 — open rejects segment count mismatch otherwise.
        need = FILE_HEADER_LEN + new_seg_count * 4
        path = idx_dir / f"{seg['file_id']:06d}"
        # keep only published slots in the bytearray
        out = bytearray(mv[:need])
        struct.pack_into("<Q", out, 8, need)
        path.write_bytes(out)
        # update meta count for last segment
        segs[si]["count"] = new_seg_count
        write_idx_meta(idx_dir, schema, segs)
        print(
            f"wrote {path} size={need} HWM={need} meta last count={new_seg_count} "
            f"total_class_a={new_count}"
        )

        # txid.body compact
        txid_path = store / "txid.body"
        compact_txid(txid_path, last_good, ghost_n, total, apply=True)

        # header_txs_first
        adjust_header_txs_first(
            store / "header_txs_first.body",
            store / "header_txs_count.body",
            last_good,
            ghost_n,
            apply=True,
        )

        # wipe segmented head so node rebuilds from Class A
        head = store / "tx.head"
        if head.is_dir():
            import shutil

            shutil.rmtree(head)
            print(f"removed {head} (rebuild on open)")
        print(
            "APPLY done. Restart node: head rebuilds; if tip create_fks still "
            f"> {new_count}, disconnect tip until tip is behind repaired Class A."
        )
    else:
        print("dry-run only (pass --apply to write)")
        compact_txid(store / "txid.body", last_good, ghost_n, total, apply=False)
        adjust_header_txs_first(
            store / "header_txs_first.body",
            store / "header_txs_count.body",
            last_good,
            ghost_n,
            apply=False,
        )


def write_idx_meta(idx_dir: Path, schema: int, segs: list) -> None:
    # Rebuild meta matching write_meta_from_segs format
    buf = bytearray()
    buf += b"RBT1"
    buf += struct.pack("<HH", schema, 9)  # kind ArrayLink-ish 9 from earlier read
    buf += struct.pack("<III", 1, len(segs), 0)
    for s in segs:
        buf += struct.pack(
            "<QQQII",
            s["first_fk"],
            s["count"],
            s["body_base"],
            s["file_id"],
            0,
        )
    (idx_dir / "meta").write_bytes(buf)


def compact_txid(path: Path, last_good: int, ghost_n: int, old_count: int, apply: bool) -> None:
    size = path.stat().st_size
    if size < TXID_HEADER:
        raise SystemExit(f"txid.body too short ({size} < header {TXID_HEADER})")
    # Prefer TableFile HWM (offset 8) when present; clamp to file size (open does the same).
    with open(path, "rb") as f:
        hdr = f.read(FILE_HEADER_LEN)
    hwm = struct.unpack_from("<Q", hdr, 8)[0] if len(hdr) == FILE_HEADER_LEN else size
    logical = min(hwm, size) if hwm >= TXID_HEADER else size
    n = (logical - TXID_HEADER) // TXID_ENTRY
    print(f"txid.body entries={n} size={size} hwm={hwm} logical={logical}")
    if n != old_count:
        print(f"WARN txid count {n} != idx count {old_count} (using idx count as authority)")
    # Authority is Class A idx count (old_count arg), not inflated txid file.
    new_count = old_count - ghost_n
    new_len = TXID_HEADER + new_count * TXID_ENTRY
    print(f"txid.body compact -> {new_count} entries (new_len={new_len})")
    if not apply:
        return
    # In-place move of tail [last_good+1+ghost_n .. old_count] → [last_good+1 ..]
    # Offsets use TXID_HEADER=32 (schema-13 sidefile), not FILE_HEADER_LEN alone.
    buf_fks = 4096
    with open(path, "r+b") as f:
        dst_fk = last_good + 1
        src_fk = last_good + 1 + ghost_n
        while src_fk <= old_count:
            batch = min(buf_fks, old_count - src_fk + 1)
            src = TXID_HEADER + (src_fk - 1) * TXID_ENTRY
            dst = TXID_HEADER + (dst_fk - 1) * TXID_ENTRY
            f.seek(src)
            chunk = f.read(batch * TXID_ENTRY)
            if len(chunk) != batch * TXID_ENTRY:
                raise SystemExit(
                    f"txid.body short read at fk={src_fk}: got {len(chunk)} want {batch * TXID_ENTRY}"
                )
            f.seek(dst)
            f.write(chunk)
            src_fk += batch
            dst_fk += batch
        f.truncate(new_len)
        # Publish HWM in the 16-byte TableFile header (node open clamps HWM to size).
        f.seek(8)
        f.write(struct.pack("<Q", new_len))
        f.flush()
        os.fsync(f.fileno())
    print(f"txid.body truncated to {new_len} bytes (HWM updated)")


def adjust_header_txs_first(
    first_path: Path,
    count_path: Path,
    last_good: int,
    ghost_n: int,
    apply: bool,
) -> None:
    first = bytearray(first_path.read_bytes())
    count = bytearray(count_path.read_bytes())
    n_first = (len(first) - FILE_HEADER_LEN) // ELEM
    n_count = (len(count) - FILE_HEADER_LEN) // ELEM
    n = min(n_first, n_count)
    adjusted = 0
    cleared = 0
    ghost_lo = last_good + 1
    ghost_hi = last_good + ghost_n
    for i in range(n):
        off = FILE_HEADER_LEN + i * ELEM
        f = struct.unpack_from("<Q", first, off)[0]
        c = struct.unpack_from("<Q", count, off)[0]
        if f == 0 or c == 0:
            continue
        last = f + c - 1
        if f >= ghost_lo and last <= ghost_hi:
            # body entirely in ghost window — clear
            struct.pack_into("<Q", first, off, 0)
            struct.pack_into("<Q", count, off, 0)
            cleared += 1
        elif f > ghost_hi:
            struct.pack_into("<Q", first, off, f - ghost_n)
            adjusted += 1
        elif f <= last_good and last > last_good:
            # spans into ghost / beyond — clear for re-archive
            struct.pack_into("<Q", first, off, 0)
            struct.pack_into("<Q", count, off, 0)
            cleared += 1
    print(f"header_txs: adjusted_first={adjusted} cleared={cleared}")
    if apply:
        first_path.write_bytes(first)
        count_path.write_bytes(count)


if __name__ == "__main__":
    main()
