# On-disk schema (current)

**Version:** `SCHEMA_VERSION = 14` (`rbitcoin_primitives`).  
**Status:** unstable until 1.0 — incompatible layout changes are reindex-only (wipe store / redo IBD).  
**Endianness:** little-endian for all multi-byte integers.

Older versions and migration notes live in [`SCHEMA_HISTORY.md`](./SCHEMA_HISTORY.md).

---

## Design at a glance

| Concern | Choice | Why |
|---------|--------|-----|
| Class A body | **Packed** full tx in one `tx.body` record (**no** leading txid) | One IO for outs/payload; identity is not body-bound |
| Class A identity | Dense **`txid.body`** sidefile (32 B header + 32 B/txid by create_fk) | Fixed `fk → offset`; head-resolve multi-cand without Prefix33 body peeks |
| Non-coinbase prevout | On-disk **`create_fk:u64` + CompactSize vout** | Smaller than `prev_txid[32]`; archive stamps fk once; wire fills soft `prev_txid` from sidefile/create |
| Txid → create | Segmented keyless **`tx.head.*`** (25-bit + fuse8) | Fixed-bits per segment; seal-time binary fuse8; **txid.body** verifies identity |
| Spentness | Annotation on **create output** (+ rare multi-list) | No multi-GiB `point.head` open-hash |
| Electrum index | Thin **create_tx_fk only** (inline ≤2 or **4 KiB page chains**) | Small index; expand vouts/value/height at query via Class A + Class C |
| Best-chain commit | Advance **`confirmed[]` last** | Tip is the commit point; strong/height may lead tip after kill |

---

## Datadir layout

```text
<datadir>/
  store/
    meta                         # store magic + schema version
    header.body / header.head    # Class A headers + hash index
    tx.body / tx.idx.meta / tx.idx.NNNNNN           # Class A + segmented idx
    txid.body                                       # dense create_fk-ordered txids (schema 13+)
    tx.head.meta / tx.head.NNNNNN [/ .fuse8]        # segmented 25-bit heads + sealed fuse8
    spenders.body                # multi-spender list nodes only
    confirmed.body               # Class C: height → header_fk
    strong_tx.body               # Class C: bitset, bit (tx_fk-1) = strong
    tx_height.body               # Class C: tx_fk → create height (u32 slots)
    header_txs_first.body        # header_fk-1 → first_tx_fk
    header_txs_count.body        # header_fk-1 → tx count
    scripthash.body / *.head     # Class B Electrum SH (thin creates)
    archive_epoch
    scripthash.runs              # SH sorted runs (Direct IBD; bulk-load at tip)
  wire/                          # tip wire ring (soft zone)
```

**Height → txs:** `confirmed[h]` → `header_fk` → contiguous Class A range  
`[header_txs_first[h−1], header_txs_first[h−1] + header_txs_count[h−1])`.

**Who writes what:** see [`docs/concurrency.md`](./docs/concurrency.md). IBD unified pipeline: confirm **commit** stage is the sole Class A appender (+ Class C / spends / tip); prep only plans Class A; peer IO does not write the store.

---

## Common file header (16 bytes)

| Offset | Size | Field |
|--------|------|-------|
| 0 | 4 | Magic `RBT1` |
| 4 | 2 | Schema version (u16) — **14** |
| 6 | 2 | Table kind (u16) |
| 8 | 8 | Logical length (bytes), including this header |

### Table kinds

| Kind | Name |
|------|------|
| 1 | meta |
| 2 | header |
| 3 | tx |
| 4 | input *(legacy kind id; no standalone tables)* |
| 5 | output *(legacy kind id; no standalone tables)* |
| 6 | point *(legacy kind id; no point.head)* |
| 7 | strong_tx |
| 8 | confirmed |
| 9 | array_link (idx files, dense arrays) |
| 10 | hash_head |
| 11 | scripthash |
| 14 | txid_body (`txid.body`) |

---

## Identity

- **FK 0** = null / absent; otherwise **1-based** dense id into the table’s idx or bit/slot space.
- Lookups that use a **16 B key prefix** must **verify** full identity against Class A body when required.

---

## Growable var records (`*.body` + `*.idx`)

Used for headers and packed txs.

- **body:** append-oriented **unframed** payloads (no per-record length prefix).
- **idx:** dense `u64` absolute offsets into body; count = `(logical_len − 16) / 8`.
- Record length = `idx[i+1] − idx[i]` (last: body logical end − start).
- FK = 1-based index into idx.

---

## Class A — headers

### `header.body` record (fixed 88 bytes)

| Field | Type |
|-------|------|
| prev_fk | u64 |
| version | i32 |
| timestamp | u32 |
| bits | u32 |
| nonce | u32 |
| merkle_root | [u8; 32] |
| hash | [u8; 32] |

### `header.head`

Open-address hash head (see [Hash heads](#hash-heads-headerhead--generic)): key = 16 B prefix of block hash → header fk. Multi-list for prefix collisions. Rehash at load **7/8**.

**Decision:** single file (not 256-way shards) with modest pre-size — header count is small vs txs.

---

## Class A — transactions

### Dense identity sidefile (`txid.body`, schema 13+)

```text
offset 0..32    — 32-byte file header (standard 16-byte TableFile header + 16 pad)
offset 32+(fk-1)*32 — txid for create_fk = fk (1-based)
```

Append-published with Class A body/idx on the sole Class A write path. Count must match `tx.body` entry count. Head-resolve multi-cand identity peeks this file (fixed offset), **not** a body prefix.

**IO policy (RWF_DONTCACHE):** sidefile peeks do **not** set `RWF_DONTCACHE` (permanent spend-annotate body pwrite only).

### Packed body (schema 13+)

Each `tx.body` record starts at an absolute offset `S` with:

```text
S (8-byte aligned only):
  body_meta (32 B)             # version, locktime, null I/O fks, counts — NO txid
  inputs…
  outputs…
  [optional 0x00 …]            # pad to next record start (included in idx length)
```

There is **no** leading magic byte and **no** leading txid (schema 11–12 stored txid at `[S, S+32)`). There are **no** standalone `input.body` / `output.body` tables.

**Alignment** (schema 13+): `S % 8 == 0` only. The schema-11/12 page non-straddle rule for a leading 32-byte txid is retired.

Decode walks meta + runs to a logical end; any remaining bytes in the idx span must be **all zeros**. Non-zero trailing garbage is corrupt. Identity for a known `create_fk` is **`txid.body`**, not body bytes.

**Body meta (32 B):** version, locktime, `input_start_fk`, `input_count`, `output_start_fk`, `output_count`.  
On packed rows, `input_start_fk` / `output_start_fk` are always null (layout reserved; I/O lives in the same payload). Soft `TxRecord.txid` is filled from the sidefile on get paths.

**IO policy (RWF_DONTCACHE):** permanent **spend-annotate `tx.body` pwrites only** (fd-only drop after spender meta write — not `madvise`). Class A append, confirm/load body reads, generic body reads, and head/idx/sidefile peeks do **not** set the flag. Kernel ENOTSUP demotes capability for the process.

### Segmented body index (`tx.idx.*`)

```text
tx.idx.meta                 # segment map
tx.idx.000000               # dense u32 LE stride units
tx.idx.000001
…
```

Each segment covers a contiguous create_fk range with a fixed **8-aligned** `body_base`:

```text
abs_start = body_base + (u32_le[i] as u64) * 8
i = fk - first_fk
```

| Segment field | Meaning |
|---------------|---------|
| `first_fk` | 1-based inclusive start of the range |
| `count` | number of u32 slots in the segment file |
| `body_base` | absolute body base (8-aligned) for relatives |
| `file_id` | maps to `tx.idx.{file_id:06}` |

Hard span per segment: `2^32 × 8` ≈ 32 GiB. Soft rollover earlier (default 16 GiB; `RBITCOIN_TX_IDX_SOFT_SPAN`). Length: `start(fk+1) − start(fk)` (may cross segments); last record uses published body end. ~**4 B/tx** vs prior 8 B absolute u64 index (~50% smaller).

### Input encoding (embedded)

| Field | Encoding |
|-------|----------|
| flags | u8 — `SEQ_FINAL`, `EMPTY_SCRIPT`, `EMPTY_WITNESS`, `NULL_PREV` |
| prev | coinbase (`NULL_PREV`): no payload; else **`create_fk:u64` LE** + CompactSize vout |
| sequence | omitted if `SEQ_FINAL`; else u32 LE |
| script_sig | omitted if empty; else CompactSize len + bytes |
| witness | omitted if empty; else CompactSize n + (len + bytes)×n |

Legacy `LOCAL_PREV` is **rejected** on decode.

**Soft `prev_txid`:** RAM-only for wire rebuild; filled from the create body’s packed txid when needed. Not stored in the input stream.

**Decision:** stamp `create_fk` at archive (batch map → sticky → `tx.head`) so confirm/cache can skip head probes on already-resolved edges.

### Output encoding (embedded)

```text
spender_field:u64 LE | flags:u8 | uleb128 value | [script…]
```

| `MULTI_SPENDER` (flags bit 2) | `spender_field` |
|-------------------------------|-----------------|
| 0 | 0 = unspent; else sole **spending_tx_fk** |
| 1 | head fk into `spenders.body` |

Best-chain spentness also requires `is_confirmed_strong(spender)` (annotations may outlive reorgs).

### Multi-spender list (`spenders.body`)

Fixed 16 B records, append-only: `spending_tx_fk:u64 | next:u64`.  
Only when an outpoint has **≥2** annotated spenders.

**Decision:** sole spends stay on the create output (no giant spend multimap head).

### Header ↔ tx range

- `header_txs_first[header_fk − 1]` = first_tx_fk (0 = no body)
- `header_txs_count[header_fk − 1]` = n  
Contiguous assignment required: block membership is an arithmetic range.

---

## Tx address head (segmented `tx.head.*`)

Keyless open-address tables: **txid → dense create_fk**, one **fixed-bits** head
per segment. There is **no** monolithic growing single `tx.head` file and **no**
bits-widen / shadow-resize path.

| Property | Current |
|----------|---------|
| Files | `tx.head.meta` + `tx.head.NNNNNN` (+ `tx.head.NNNNNN.fuse8` when sealed) |
| Default | **BITS=25**, **4 B relative** entries → **128 MiB** per segment (`2^25` slots) |
| Env | `RBITCOIN_TX_HEAD_BITS` in **8..=34** (tests/tiny only); product default **25** |
| Entry | LE **relative** create id; **0 = empty**; `fk = first_fk + rel − 1` |
| Capacity | Segment ends at **`MIN(body soft span ~16 GiB, 80% of head slots)`** → seal + new open |
| Seal filter | **Binary fuse8** (~9 bits/key, no false negatives, FP ≈ 0.39%) built **once on seal**; open segment has **no** filter |
| Probe | Page-local double-hash (1024 slots/page); one page load (4 KiB @ 4 B); max depth 1024 |
| Insert | First empty in-page (or same relative id idempotent); second same-txid goes **deeper** |
| Lookup | **Open always** → sealed **newest→oldest** (fuse gate) → body-verify candidates (deepest match wins, BIP30-shaped) |
| Legacy | Monolithic `tx.head` / `tx.head.new` / `tx.head.resize` / `tx.head.overflow` **refused on open** — reindex |

**Publish order on seal:** write fuse8 file durable → mark segment sealed in
`tx.head.meta` → open next head for subsequent creates.

**Probe note:** all candidates for a key share one page (single IO). Keyless
slots cannot Robin-Hood. Incomplete seal after kill: delete incomplete
segment/meta and rebuild from Class A, or reindex.

**Capacity @ 0.80 load (25-bit):** ≈ **26.8 M creates/segment**, ~29 MiB fuse8 when sealed (~6.1 B total sealed storage per create including head slots).

---

## Hash heads (`header.head`, generic)

Used where the key is a 32 B hash and the value is a single fk (or multi-list).

- Slot = **16 B key prefix** + **8 B packed value** (24 B); power-of-two slots; linear probe.
- Packed value: sole fk (high bit clear), or `MULTI_BIT | list_fk` → sibling `.mlt` (`create_fk:u64 | next:u64`, newest first).
- Multi-list: 16 B prefix collisions and BIP30-style multiples.
- Identity: `get_all` candidates + **body verify**.
- Rehash when load would exceed **7/8**.

**Not** used for `tx.head` (keyless address) or for scripthash **create lists** (those use page chains).

---

## Class B — scripthash (Electrum)

Thin create index: **create_tx_fk only** (no vout in the index).

### Head

- Key = first **16 B** of `SHA256(scriptPubKey)` (Electrum hash; wire APIs still use 32 B).
- Slot = **32 B**: key[16] + value[16] (two u64s). **Bit 63** of each value word is a flag; payload in low 63 bits.
- Sharded **64-way** on mainnet (prefix of `scripthash[0]`; sorted runs stream
  one shard band at a time). Cold load builds one **final-sized** OA image in RAM
  then sequential-writes the shard file (~0.5–1 GiB peak).
- **No rehash** after create/materialize size: new keys after ~**0.80** load seal main
  and go to **`scripthash.ovf.head`** (overflow OA). Existing main keys still append
  on main pages. Optional fuse8 product on seal (`scripthash.head.fuse8` / ovf).

| Mode | When | Value (`w0`, `w1`) |
|------|------|---------------------|
| Empty | no creates | `0`, `0` |
| Inline | ≤2 create_tx_fks | bit63=0, fk0; bit63=0, fk1 or `0` |
| **Paged** | ≥3 | bit63=1, **first** page off; bit63=0, **last** page off |

Schema-13 **slab** packing (flag + class/used + slab off) is **rejected** on decode —
rebuild / rematerialize SH (no dual-read).

### Body pages (schema 14)

- After RBT1 header: 4 KiB alloc page (`SHAL`) + **fixed 4096 B pages**.
- Page layout: `next_page_off:u64` | `n_fks:u16` | reserved 6 B | up to **510** create_tx_fks.
- Chain is singly linked first→last; head stores first+last for O(1) walk start and append target.
- Append-only growth RMW last page / link a new page (page freelist reuses class-7 4 KiB slots).

### Query join

Heights, value, spentness, vouts: expand from Class A outputs (match full scripthash) + spend annotations + Class C.  
IBD may stage creates in **sorted runs** and bulk-materialize durable SH at tip entry (paged layout).

**Decision:** inline for rare scripts; **page chains** for multi-use scripts so history is
contiguous page walks, not relocating geometric slabs. Query cost for busy wallets is
dominated by Class A + spend joins, not SH pointer chasing.

---

## Class C — chain tip

### `confirmed.body`

Dense u64 array: index = height → header_fk. Length = tip_height + 1 when non-empty.

### `strong_tx.body`

Bitset: bit `(tx_fk − 1)` set ⇒ tx is strong on the best chain.

### `tx_height.body`

Index = `tx_fk − 1`; value = **height + 1** as **u32** (0 = unset).  
Used for maturity and `is_confirmed_strong` (height ≤ tip).

### Commit order (confirm)

1. `strong_tx` + `tx_height` (may lead tip after kill)
2. Thin scripthash creates (may lead tip)
3. **`confirmed[]` tip advance** ← **commit**

`is_confirmed_strong(tx)` ⇔ strong ∧ `tx_height ≤ tip`.  
On open: `repair_class_c_above_tip` clears strong/height above tip.

---

## Archive epoch (`archive_epoch`)

Small control file (~32 B): magic, schema version, archive_mode flag, optional finalized_height, wire_depth. Coordinates durable-archive soft/hard zones with the tip wire ring.

---

## Query-layer notes

- `spenders(outpoint)`: confirmed-strong only; `spenders_raw` for full annotation multimap.
- Electrum history / balance / listunspent: join thin SH rows → Class A → spends → Class C.
- Optional manual `backfill_tx_index` rebuilds segmented `tx.head.*` mappings from Class A (not part of tip entry).

---

## Related docs

| Doc | Topic |
|-----|--------|
| [`SCHEMA_HISTORY.md`](./SCHEMA_HISTORY.md) | Prior schema versions |
| [`docs/concurrency.md`](./docs/concurrency.md) | Writer ownership, IBD vs tip |
| [`docs/crash-recovery.md`](./docs/crash-recovery.md) | Kill safety, reorg, segmented head seal |
| [`OPERATOR.md`](./OPERATOR.md) | Datadir ops, env knobs |
