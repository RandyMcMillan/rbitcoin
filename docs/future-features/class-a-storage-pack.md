# Class A storage pack (future)

Parked from a 2026-08 storage-cost session. **Not** the `tx_height` drop
(that shipped separately: resident height fence, no `tx_height.body`).

**Goal:** Cut schema-15 Class A disk without maps, without variable spent
slots, and without collapsing the annotate io_uring machine. One later
`SCHEMA_VERSION` bump (next unused after the height-fence schema) lands:
8-byte spent slots, thin `txout` meta, script templates (including P2A),
and inwit prevout Δfk.

**Census (SCHEMA.md, tip 962,298):** 1.417e9 creates, 2.46 in / 2.70 out.
Hot set today ≈ 227 GiB (`txout` 129 + `spent` 32 + 3×idx 16 + `txid` 42 +
`tx.head` 8). `inwit` 486 GiB is cold.

---

## Constraints

- Lock-free store: one Class A appender, one spend annotator, N readers;
  publish body → idx → HWM.
- Annotate stays the existing page-coalesce RMW machine. Change slot width /
  parse only. Do **not** flatten to batched `pread`/`pwrite` helpers.
- Fixed spent slots (`abs = off + SLOT×vout`). No variable-length spenders.
- No maps, no mmap, no bigger head cache.
- Durable Class A / spent / inwit / txout layout change ⇒ **`SCHEMA_VERSION`
  bump** + `SCHEMA.md` / `SCHEMA_HISTORY.md` in the same commit as the format
  code. Refuse prior Class A bodies with creates (wipe + IBD). Empty prior
  may silent-rewrite `meta`.
- TDD: Red → Green → Refactor per step. Synthetic `/tmp` only.
- Do **not** push intermediate format commits to origin. Unreleased schema is
  mutable until the cutover commit is complete.

## Out of scope

- Host parent-age / script-type histogram (nice, not required to land codecs).
- Compress `txid.body` / `tx.head` / witness.
- Variable spent slots, stride-1 or u24 `spent.idx`.
- `tx_height.body` (already replaced by the RAM fence).

## Pack (one reindex)

| Slice | ~GiB | Hot? |
|-------|-----:|:----:|
| Spent 9→8 B (pad dies) | **11** | yes |
| Thin `txout` meta | **12–16** | yes |
| Script templates (P2PKH/P2SH/P2WPKH/P2WSH/**P2TR**/OP_RETURN/**P2A**) | **6–10** | yes |
| Prevout Δfk uleb | **11–15** | no (`inwit`) |
| **Hot working set** | **~30–35** of 227 | |
| **All of the pack** | **~40–50** of ~710 | |

---

## Background

### Current prevout is not 12-byte fixed

Non-coinbase `inwit` prev is already **`create_fk: u64` LE + CompactSize
vout**. Typical edge **9 B**. Coinbase is flags-only (`NULL_PREV`). Savings
below are vs **8 B fk**, not vs a 12-byte mental model.

`uleb128(this_fk − parent_fk)` is valid: parent fk is strictly less than the
spender (topo assign). Same-block / short chains → 1 B; recent → 2–3 B;
ancient (≥268e6) → 5 B. Honest average **~3.5–4.5 B** → **~11–15 GiB** of
`inwit` (~2–3%). Vout is already CompactSize; varint vout saves nothing.
`parent_fk ≥ this_fk` → `Corrupt` (no wrap). Decode needs the record’s own
fk (idx already has it).

### Spent 9 → 8

```text
[0]     flags (MULTI_SPENDER, …)
[1..8)  spender_field as u56 LE (0 = unspent / sole fk / multi-list head)
```

`2^56` fks is beyond any chain we will run. `8 × n_out` is always 8-aligned,
so **spent records need no pad**. Today’s ~32 GiB includes `9×n_out` plus
pad-to-8. After: ~21 GiB. Annotate: `abs = off + 8×vout`. Slots per 4 KiB
page: 455 → **512**.

`spent.idx` stays u32 stride-8 (**5.28 GiB**). Alignment does **not** buy a
useful idx-width cut. Soft 16 GiB segments hold ~0.67e9 → **~1.0e9** creates
(fewer rolls, same 4 B/tx).

### Thin `txout` meta

Every `txout.body` record starts with a **fixed 16-byte header**, then the
output run:

```text
0..4   version     i32 LE
4..8   locktime    u32 LE
8..12  input_count u32 LE     (n_in; inputs themselves live in inwit)
12..16 output_count u32 LE
```

Pin **must** parse this header on every archived parent. Version and
locktime ride along because reconstruct/RPC need them on the same file.

Proposed:

```text
flags:u8
  bit 0  VER_1          version == 1 (i32 1), omit version bytes
  bit 1  VER_2          version == 2
  bit 2  VER_3          version == 3 (TRUC / ephemeral-anchor era)
  bit 3  LOCKTIME_ZERO  locktime == 0, omit locktime bytes
  bits 4–7 reserved; must be 0 or Corrupt

if no VER_1/2/3 bit:  i32 LE version
if !LOCKTIME_ZERO:    uleb128(locktime as u64)
uleb128 input_count
uleb128 output_count
```

Typical v2 + locktime 0 + small counts: **3 B** (save 13). Anti-fee-snip
locktime ≈ tip: **6 B** (save 10). High-bit nVersion (finding 003) uses
explicit i32 — flags fire only on exact `1`/`2`/`3`.

**`n_in` stays on txout only.** `inwit` has no count (unframed
self-delimiting run). `decode_inwit_secret(raw, in_count)` takes `in_count`
from txout meta. Do **not** duplicate `n_in` into inwit.

`BODY_META_LEN = 16` becomes `decode_body_meta(buf) -> (TxRecord, usize)`.
Idx record starts stay 8-aligned.

### Script templates

Today each non-empty / non-`OP_TRUE` script is `CompactSize(len) + raw
scriptPubKey`.

| Template | Script today | Store | Save |
|----------|-------------:|------:|-----:|
| P2PKH | 25 B | 20 B hash | ~5 B |
| P2SH | 23 B | 20 B | ~3 B |
| P2WPKH | 22 B | 20 B | ~3 B |
| P2WSH | 34 B | 32 B | ~3 B |
| **P2TR** | 34 B | 32 B | ~3 B (growth win) |
| OP_RETURN single-push | `6a` + push + data | data only | ~2 B |
| **P2A** | 4 B (`51 02 4e 73`) | 0 B payload | ~5 B |

```text
// txout flags byte
bits 0–3  SCRIPT_KIND
bits 4–7  reserved, must be 0

KIND:
  0 RAW              CompactSize + bytes
  1 EMPTY            no payload
  2 OP_TRUE          no payload
  3 P2PKH            20 B
  4 P2SH             20 B
  5 P2WPKH           20 B
  6 P2WSH            32 B
  7 P2TR             32 B
  8 OP_RETURN_PUSH   CompactSize + data   // single push only
  9 P2A              0 B  (script `51 02 4e 73`)
 10–15 reserved      decode → Corrupt
```

`MULTI_SPENDER` moves to `spent_flags` (illegal on txout). Canonical encode
when the script exactly matches a kind. RAW that looks like P2PKH still
decodes. XOR at rest: hash/data payload only.

P2TR is the *new-block* win (same 32 B payload as P2WSH, witness v1). P2A
is new but will gain share with TRUC / ephemeral anchors — implement it,
do not leave kind 9 reserved.

### Not worth it / heroic

| Idea | Why skip |
|------|----------|
| Variable-length spent slots | Breaks `8×vout` / page RMW |
| Stride-1 / u24 spent idx | Pad is gone; u24 ≈ 1 GiB vs more files |
| Compress `txid.body` / `tx.head` | Random-access identity |
| Compress witness | ~450 GiB; pin does not read it |
| Maps / mmap / bigger caches | Not a disk win |

---

## Implementation slices (when picked up)

Follow [`docs/how-we-plan.md`](../how-we-plan.md): one Red→Green→Refactor
per step. Codecs + tests first (production still writes the current Class A
layout). **One format commit** swaps production encode/decode, bumps
`SCHEMA_VERSION`, refuses prior Class A with creates, updates SCHEMA.md /
HISTORY / CHANGELOG.

1. Thin meta codec (flags + ulebs; high-bit version explicit i32).
2. Script template codec (kinds 0–9 including P2A).
3. Spent 8 B slot codec (flags + u56); do not change `SPENT_SLOT_LEN` until cutover.
4. Prevout Δfk codec (`uleb(this_fk − parent_fk)` + CompactSize vout).
5. Tests stop hardcoding 16-byte meta / 9-byte slots (use constants).
6. Schema cutover + refuse-old + docs (the format commit).
7. `/tmp` Class A roundtrip (v1 P2PKH, v2 P2TR+P2WSH, v3 OP_RETURN+P2A, high-bit version).
8. Annotate 8 B neighbor-preserve + page groups (keep the uring machine).
9. Pin / load variable meta (`need_vouts` + decode-fail trash header).
10. Reconstruct / RPC script identity (P2TR is `5120\|\|32`, P2A is `51024e73`).
11. Plan-end gates (fmt / clippy / workspace / coverage). Musl after merge to master.

No default test that opens `datadir-mainnet`. No production-scale `n_out`
(use 4 and 500 as today for page-span).
