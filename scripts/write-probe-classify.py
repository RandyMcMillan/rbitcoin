#!/usr/bin/env python3
"""Fail unless every write-probe entry has a reviewed reason.

`cargo test --features rbitcoin-store/write-probe` with `WRITE_PROBE_OUT=f`
logs each distinct (class, file, caller) that writes below a file's durable
end or publishes a length. Classes in SAFE are safe by structure. Any other
entry needs a RULES match, or this exits 1. An empty or missing log exits 1:
it means the probe did not run. See docs/crash-recovery.md (write probe).
"""
import re
import sys

# Safe by structure: a fresh file, pages a mark images, one sector (a
# length and the bytes it names inside sector 0), or a length published
# after the bytes it names were synced. FIXTURE is a file outside any store
# (a table built bare in a test). LEN_ON_MARK is never safe by structure: a
# length that names bytes on pages a live mark images, with the length
# itself outside that mark, survives a restore that rewrites those bytes.
# LEN_MULTI_SECTOR is a length write (a trailing footer) that spans two
# sectors with no mark over it: it can land torn even after its bytes sync.
SAFE = {"CREATE", "ARMED", "ONE_SECTOR", "LEN_ONE_SECTOR", "LEN_ARMED",
        "LEN_AFTER_SYNC", "FIXTURE"}

CLASS_A = (r"(txout|seqsigwit|spent|txid|txstat|input)\.body|"
           r"(create|seqsigwit)\.(loc|loc\.ovf|off)|txstat\.(ovf|blk)|input\.(loc|off)")

# (class regex, file regex, frames regex, reason). First match wins.
RULES = [
    (r"LEN_BEFORE_SYNC", rf"/({CLASS_A})$", r".*",
     "Class A length: open keeps only the creates class_a.synced or a header_txs range names"),
    (r"MULTI_SECTOR", r"/spent\.body$", r"SpentFold|spend_delta::",
     "spent.body fold: open replays it from spent.delta"),
    (r"MULTI_SECTOR|LEN_BEFORE_SYNC", r"tx\.head/", r"AddressHead::write_snapshot",
     "fresh shutdown snapshot: footer generation stays 0, which open does not trust, until the image is synced"),
    # A shard pack soft-clears the ingest keys its main now owns
    # (publish_packed_shard -> drop_ingest_covered_by_packed_main ->
    # stage_rewrites). Those frames are inlined, so the stack shows seal_shard.
    (r"MULTI_SECTOR", r"scripthash\.ovf/ingest|shhead",
     r"ScriptHashHead|SlotPageCache|unlink_write_home|scripthash_head::|scripthash_materialize::seal_shard",
     "scripthash ingest head: synced ingest.ahead image"),
    # The cold pack publishes a shard body's length before its sync. Open
    # trusts a packed shard only through its NN.packed mark, and
    # publish_packed_shard writes that mark only after it flushes the body
    # (data, length, alloc header). Only that class, on that stack.
    (r"LEN_BEFORE_SYNC", r"scripthash\.body/",
     r"scripthash_materialize::pack_and_seal_unsorted_shard|ScriptHashBulkSession::finish_pack",
     "cold pack body: open trusts a shard only by its NN.packed mark, written after the body sync"),
    # A new head generation sets its length, zeroes its slot table (already
    # zero in a new sparse file), then syncs the file and its directory
    # before anything can name it. No byte below that length was durable.
    (r"MULTI_SECTOR", r"header\.head", r"HashHead::create_with_slots",
     "new head generation: slot table zeroed before its first sync"),
    (r"MULTI_SECTOR", r"scripthash", r"unlink::restore_page|unlink::arm_undo|recover_unlink_undos",
     "legacy unlink undo replay at open, or its test hook"),
    (r"MULTI_SECTOR", r"blockfilter\.body", r"repair_body|repaired_filter_body",
     "filter repair: every read checks the filter hash, and a torn body is rebuilt"),
    (r".*", r".*", r"testutil::tear_|flush_observing|test_tear_",
     "test hook"),
    (r".*", r".*", r"^PROBE \S+ \S+ test-body:",
     "a test body writes the file directly"),
]


def entries(path):
    try:
        text = open(path, errors="replace").read()
    except OSError:
        return []
    out, cur = [], None
    for line in text.split("\n"):
        if line.startswith("PROBE "):
            cur = [line]
            out.append(cur)
        elif cur is not None and line.startswith("    "):
            cur.append(line.strip())
        else:
            cur = None
    return out


def main():
    if len(sys.argv) != 2:
        sys.exit("usage: write-probe-classify.py <log>")
    ents = entries(sys.argv[1])
    if not ents:
        print("no write-probe entries: the probe did not run", file=sys.stderr)
        return 1
    seen, counts, reasons, bad = set(), {}, {}, []
    for e in ents:
        head = e[0].split()
        # The whole header: a demangled caller can hold a space.
        key = e[0].rsplit(" end=", 1)[0]
        if key in seen:
            continue
        seen.add(key)
        cls, path = head[1], head[2]
        counts[cls] = counts.get(cls, 0) + 1
        if cls in SAFE:
            continue
        frames = " ".join(e)
        for rc, rf, rr, why in RULES:
            if re.fullmatch(rc, cls) and re.search(rf, path) and re.search(rr, frames):
                reasons[why] = reasons.get(why, 0) + 1
                break
        else:
            bad.append(e)
    print(f"{len(seen)} distinct entries")
    for c in sorted(counts):
        print(f"class {c}: {counts[c]}")
    for why, n in sorted(reasons.items(), key=lambda x: -x[1]):
        print(f"reviewed [{n}] {why}")
    print(f"UNCLASSIFIED: {len(bad)}")
    for e in bad:
        print("\n".join(e[:6]))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
