# On-disk schema (current)

**Version:** `SCHEMA_VERSION = 16` (`rbitcoin_primitives`).  
**Status:** unstable until 1.0 — most layout changes are reindex-only.  
**13/14→16 open:** Empty Class A (no creates) + empty/missing SH may silently
rewrite `meta` to 16. A packed `tx.body` **with creates**, or a durable page-era
(or schema-13 slab) SH index, is refused (wipe + IBD). Schema 15 Class A is
`txout` + `inwit` + `spent` (not a single packed `tx.body`).  
**15→16 open:** Soft migrate — Class A unchanged; leftover `tx_height.body` is
unlinked; create height is the RAM fence.  
**Endianness:** little-endian for all multi-byte integers.

Older versions and migration notes live in [`SCHEMA_HISTORY.md`](./SCHEMA_HISTORY.md).

---

## Design at a glance

| Concern | Choice | Why |
|---------|--------|-----|
| Class A body | **Split** `txout` (outs) + `inwit` (ins+wit) + `spent` (9 B×n_out) | Pin/SH read outs only; annotate isolates scripts |
| Class A identity | Dense **`txid.body`** sidefile (32 B header + 32 B/txid by create_fk) | Fixed `fk → offset`; head-resolve multi-cand without Prefix33 body peeks |
| Non-coinbase prevout | On-disk **`create_fk:u64` + CompactSize vout** | Smaller than `prev_txid[32]`; archive stamps fk once; wire fills soft `prev_txid` from sidefile/create |
| Txid → create | Segmented keyless **`tx.head.*`** (25-bit + fuse8) | Fixed-bits per segment; seal-time binary fuse8; **txid.body** verifies identity |
| Spentness | Annotation on **create output** (+ rare multi-list) | No multi-GiB `point.head` open-hash |
| Electrum index | Thin **create_tx_fk only** (inline ≤2 / geometric slabs / megakey pages) | Packed to ~run size; expand vouts/value/height at query via Class A + Class C |
| Best-chain commit | Advance **`confirmed[]` last** | Tip is the commit point; strong/height may lead tip after kill |

---

## Datadir layout

```text
<datadir>/
  store/
    meta                         # store magic + schema version
    header.body / header.head    # Class A headers + hash index
    txout.body / txout.idx/                         # Class A outs (hot)
    inwit.body / inwit.idx/                         # Class A inputs+witness (cold)
    spent.body / spent.idx/                         # sole-spender 9 B × n_out
    tx.body / tx.idx.*                              # schema ≤14 packed (refused if non-empty)
    txid.body                                       # dense create_fk-ordered txids (schema 13+)
    tx.head/                     # meta + NNNNNN + .fuse8 (segmented 25-bit)
    spenders.body                # multi-spender list nodes only
    confirmed.body               # Class C: height → header_fk
    strong_tx.body               # Class C: bitset, bit (tx_fk-1) = strong
    # tx_height.body retired in 16 (RAM fence from confirmed + header_txs)
    header_txs_first.body        # header_fk-1 → first_tx_fk
    header_txs_count.body        # header_fk-1 → tx count
    scripthash.body / scripthash.head/NN[.idx]        # Class B (slabs + sorted heads; no fuse)
    scripthash.ovf/ingest                                # global OA ingest
    scripthash.ovf/NNNNNN[.fuse8][.idx]                  # sealed global ovf (sorted)
    archive_epoch
    scripthash.runs              # SH sorted runs (Direct IBD; bulk-load at tip)
    sp_tweaks.idx / sp_tweaks.body  # optional BIP-352 thin tweaks (schema 14 side)
  wire/                          # unused (opened, never filled)
```

**Height → txs:** `confirmed[h]` → `header_fk` → contiguous Class A range  
`[header_txs_first[h−1], header_txs_first[h−1] + header_txs_count[h−1])`.

**Who writes what:** see [`docs/concurrency.md`](./docs/concurrency.md). IBD unified pipeline: confirm **commit** stage is the sole Class A appender (+ Class C / spends / tip); prep only plans Class A; peer IO does not write the store.

---

## Common file header (16 bytes)

| Offset | Size | Field |
|--------|------|-------|
| 0 | 4 | Magic `RBT1` |
| 4 | 2 | Schema version (u16) — **16** |
| 6 | 2 | Table kind (u16) |
| 8 | 8 | Logical length (bytes), including this header |

### Table kinds

| Kind | Name |
|------|------|
| 1 | meta |
| 2 | header |
| 3 | txout (`txout.body`; was `tx` through schema 14) |
| 4 | input *(legacy kind id; no standalone tables)* |
| 5 | output *(legacy kind id; no standalone tables)* |
| 6 | point *(legacy kind id; no point.head)* |
| 7 | strong_tx |
| 8 | confirmed |
| 9 | array_link (idx files, dense arrays) |
| 10 | hash_head |
| 11 | scripthash |
| 14 | txid_body (`txid.body`) |
| 15 | sp_tweaks (`sp_tweaks.body`; idx uses array_link) |
| 16 | inwit (`inwit.body`) |
| 17 | spent (`spent.body`) |

---

## Identity

- **FK 0** = null / absent; otherwise **1-based** dense id into the table’s idx or bit/slot space.
- Lookups that use a **16 B key prefix** must **verify** full identity against Class A body when required.

---

## Growable var records (`*.body` + `*.idx`)

Used for Class A `txout` / `inwit` / `spent` (and historically packed `tx.body`).

- **body:** append-oriented **unframed** payloads (no per-record length prefix).
- **idx:** segmented **u32 stride-8** relatives (`{stem}.idx/`); see Class A index below.
  Header hash lookup is a separate `HashHead`, not this idx.
- Record length = `start(fk+1) − start(fk)` (last: published body end − start).
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

Append-published with Class A body/idx on the sole Class A write path. Count must match `txout` / `inwit` / `spent` / `txid.body`. Head-resolve multi-cand identity peeks this file (fixed offset), **not** a body prefix.

**IO policy (RWF_DONTCACHE):** sidefile peeks do **not** set `RWF_DONTCACHE` (permanent spend-annotate body pwrite only).

### Split bodies (schema 15)

Each create_fk has three 8-aligned var records (coupled idx `first_fk` / `file_id`):

```text
txout.body  S:  meta 16B (version, locktime, in_count, out_count) | outputs (no spender)
inwit.body Sw:  per-input flags|create_fk+vout|seq?|script_sig?|witness?
spent.body Ss:  9 B × out_count  (spender u64 + flags). Multi overflow → spenders.body
```

Empty inwit / zero-out spent: **8-byte zero pad** so idx starts stay strictly monotone.
Pin / SH / Cake read **`txout` only**. Annotate RMW is on **`spent`** (`abs = Ss + 9×vout`).
Reconstruct zips `txout` + `inwit`. First-page Outs reads are 4 KiB; truncated outs extend
to the full idx span.

Packed `tx.body` (schema 13–14: 32 B meta | inputs+witness | outputs) is **refused** if it contains creates.

### Packed body (schema 13–14, historic)

There is **no** leading magic byte and **no** leading txid (schema 11–12 stored txid at `[S, S+32)`). There are **no** standalone `input.body` / `output.body` tables.

**Alignment** (schema 13+): `S % 8 == 0` only. The pad exists so record starts match **`tx.idx` u32 stride-8** (`IDX_STRIDE = 8`): idx stores body offsets as stride units from `body_base`. The schema-11/12 page non-straddle rule for a leading 32-byte txid is **retired** — identity is **`txid.body`**, not body bytes.

Decode walks meta + runs to a logical end; any remaining bytes in the idx span must be **all zeros**. Non-zero trailing garbage is corrupt.

**Body meta (schema 15, 16 B):** version, locktime, `input_count`, `output_count`.
`input_start_fk` / `output_start_fk` stay null in RAM. Soft `TxRecord.txid` is filled from the sidefile on get paths.

**IO policy (RWF_DONTCACHE):** permanent **spend-annotate `spent.body` pwrites only** (fd-only drop after spender meta write — not `madvise`). Class A append, confirm/load body reads, generic body reads, and head/idx/sidefile peeks do **not** set the flag. Kernel ENOTSUP demotes capability for the process.

### Segmented body index (`txout.idx.*` / `inwit.idx.*` / `spent.idx.*`)

```text
{txout,inwit,spent}.idx/meta     # segment map (coupled first_fk / file_id)
{txout,inwit,spent}.idx/000000   # dense u32 LE stride units
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
| `file_id` | maps to `{stem}.idx/{file_id:06}` |

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

**Soft `prev_txid`:** RAM-only for wire rebuild; filled from **`txid.body`**
(or the create’s known identity) when needed. Not stored in the input stream.

**Decision:** stamp `create_fk` at archive (batch map → sticky → `tx.head`) so confirm/cache can skip head probes on already-resolved edges.

### Output encoding (`txout.body`)

```text
flags:u8 | uleb128 value | [CompactSize script…]
```

No spender bytes. `MULTI_SPENDER` on a `txout` output is corrupt.

### Sole-spender slot (`spent.body`)

9 B per vout at `Ss + 9×vout`:

| Offset | Field |
|--------|-------|
| 0–7 | `spender_field` u64 LE (0 = unspent; else sole `spending_tx_fk` or multi-list head) |
| 8 | flags (`MULTI_SPENDER` bit 2) |

| `MULTI_SPENDER` | `spender_field` |
|-----------------|-----------------|
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

### Optional BIP-352 thin tweaks (`sp_tweaks.*`)

Schema **14** side product — **no** `SCHEMA_VERSION` bump. Soft-open: missing
files are empty (not `Corrupt`, not a head recreate). Created when `--sptweaks`
is on.

`sp_tweaks.idx` (kind array_link): after the 16-byte header, `origin_height:u32`
+ 4-byte pad, then dense slots from `origin` (`ChainParams::taproot_height`):

```text
slot[i] = block_fk:u64  ‖  off:u32
          header_fk        absolute start in sp_tweaks.body
```

`n_tx` is **not** stored (`header_txs_count[block_fk]`). `block_fk` must match
`confirmed[h]` or the slot is a hole (naive fallback).

`sp_tweaks.body` (kind 15): per tx in `header_txs` order, `u8 len` then `len`
bytes. `len=0` = no tweak; `len=33` = compressed `A_tweak`. No txids, outs, or
parent scripts.

Reorg: truncate slots above the new tip (same era as SH HWM).

---

## Tx address head (segmented `tx.head/`)

Keyless open-address tables: **txid → dense create_fk**, one **fixed-bits** head
per segment. There is **no** monolithic growing single `tx.head` file and **no**
bits-widen / shadow-resize path. Module map: [`docs/heads.md`](./docs/heads.md).

| Property | Current |
|----------|---------|
| Files | `tx.head/meta` + `tx.head/NNNNNN` (+ `tx.head/NNNNNN.fuse8` when sealed) |
| Default | **BITS=25**, **4 B relative** entries → **128 MiB** per segment (`2^25` slots) |
| Env | `RBITCOIN_TX_HEAD_BITS` in **8..=34** (tests/tiny only); product default **25** |
| Entry | LE **relative** create id; **0 = empty**; `fk = first_fk + rel − 1` |
| Capacity | Segment ends at **`MIN(body soft span ~16 GiB, 80% of head slots)`** → seal + new open |
| Seal filter | **Binary fuse8** (~9 bits/key, no false negatives, FP ≈ 0.39%) built **once on seal**; open segment has **no** filter |
| Fuse file | `BF8R` + **version** + body. **v2** = in-tree LE layout (current). **v1** = historical xorf+bincode (open migrates to v2 from Class A; does **not** wipe head) |
| Probe | Page-local double-hash (1024 slots/page); one page load (4 KiB @ 4 B); max depth 1024 |
| Insert | First empty in-page (or same relative id idempotent); second same-txid goes **deeper** |
| Lookup | Pin by txid → **hot** (open + ages ≤3) → ID/idx → **cold** (ages ≥4) if needed; fuse-gate sealed; body-verify ([`docs/heads.md`](./docs/heads.md)) |
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
Not `tx.head` — see [`docs/heads.md`](./docs/heads.md).

- Slot = **16 B key prefix** + **8 B packed value** (24 B); power-of-two slots; linear probe.
- Packed value: sole fk (high bit clear), or `MULTI_BIT | list_fk` → sibling `.mlt` (`create_fk:u64 | next:u64`, newest first).
- Multi-list: 16 B prefix collisions and BIP30-style multiples.
- Identity: `get_all` candidates + **body verify**.
- Rehash when load would exceed **7/8**.

**Not** used for `tx.head` (keyless address) or for scripthash **create lists** (slabs; megakey page chains).

---

## Class B — scripthash (Electrum)

Thin create index: **create_tx_fk only** (no vout in the index). Creates only
(outputs); spends join via Class A + spend annotations.

### Sorted create_fk invariant

For each key, durable create_tx_fks are **strictly increasing** by `create_tx_fk.0`
(within a slab, within each megakey page, and across pages).

**Insert / batch model (tip + warm residual):**

1. Read **max existing** FK (slab decode or **last page only** when paged; inline from head).
2. From the batch (sort+dedup by fk), **skip every `fk ≤ max`** (re-queue / HWM
   replay is safe — not a hard error).
3. Append remaining higher FKs: grow the slab class if needed, or fill last
   megakey page + new pages. **No full chain walk** on insert.

**Caller contract:** apply SH create batches for a key in **non-decreasing
block/batch time order**. Skipping lower fks assumes an earlier batch already
wrote them; inserting a later block before an earlier one can leave permanent holes.

Cold bulk: pick the **exact** geometric class from the run-group length (or emit
pages if `n ≥ 257`). One write per key. No half-empty 4 KiB.

### Head (schema 15)

- Key = first **16 B** of `SHA256(scriptPubKey)` (Electrum hash; wire APIs still use 32 B).
- Record = **32 B**: key[16] + value[16] (two u64s). **Bit 63** of each value word is a flag; payload in low 63 bits.
- Main is a **sealed sorted** file per shard (`scripthash.head/NN`) plus `.idx`
  (16 B key + 8 B off per 128 records / 4 KiB page). **No** main `.fuse8` —
  misses pay one 4 KiB data pread. Record count is immutable after seal.
  Existing keys update `value16` in place. New keys are **not** punched into main.
- Sharded **64-way** on mainnet (prefix of `scripthash[0]`; sorted runs stream
  one shard band at a time). Cold load writes packed records (no 2 GiB OA image).
- **Overflow:** one **global** ingest OA (`scripthash.ovf/ingest`, 256 slots tiny /
  2²² slots mainnet). Load ≥ ~0.80 **seals** ingest to sorted+fuse+idx
  (`scripthash.ovf/NNNNNN`). ≥8 sealed files **compact** (k-way merge of
  disjoint records). **Do not fold ovf into main.** Body offs are not copied.
- Lookup (tip / sorted main): **ingest OA → sealed ovf newest→oldest (fuse
  skip) → main idx → data**. Post-seal new keys live only on overflow, so they
  skip the main page. A key has **exactly one** home.

| Mode | When | Value (`w0`, `w1`) |
|------|------|---------------------|
| Empty | no creates | `0`, `0` |
| Inline | ≤2 create_tx_fks | bit63=0, fk0; bit63=0, fk1 or `0` (fk0 < fk1) |
| **Slab** | 3–256 fks | both bit63=1; `w0` = body off; `w1` = used:u16 \| class<<16 |
| **Paged** | ≥257 fks (megakey) | bit63=1, **first** page off; bit63=0, **last** page off |

Schema-13 slab packing (`w0` flagged, `w1` clear) still decodes as paged;
store open refuses a durable pre-15 SH index (no dual-read of 4 KiB pages as slabs).

### Body (schema 15)

- Combined prefix: RBT1 at 0–15, SHAL v3 fields at 16–4095, **payload at 4096**.
  Small slabs pack from bump with **no** 4 KiB align. Megakey pages 4 KiB-align
  that alloc only.
- Geometric slabs class 0–6 (`32 B`–`2 KiB`; cap `4 << class`). Payload:
  `used:u16` + ULEB128 `fk0` + ULEB128 deltas.
- Megakey **pages**: `next_page_off:u64` | `n_fks:u16` | reserved 6 B | up to
  **510** raw `u64` fks. Chain first→last; last-page append only.
- Size-class freelist on SHAL. Grow relocates O(log n) times; megakeys never relocate.

### Query join

Heights, value, spentness, vouts: expand from Class A outputs (match full scripthash) + spend annotations + Class C.  
IBD may stage creates in **sorted runs** and bulk-materialize durable SH at tip entry (slab/page packer).

**Decision:** inline for 1–2-use scripts (~95 % of keys); geometric slabs for
typical multi-use; page chains only for megakeys. Query cost for busy wallets is
dominated by Class A + spend joins, not SH pointer chasing.

---

## Class C — chain tip

### `confirmed.body`

Dense u64 array: index = height → header_fk. Length = tip_height + 1 when non-empty.

### `strong_tx.body`

Bitset: bit `(tx_fk − 1)` set ⇒ tx is strong on the best chain.

### Create height (schema 16: RAM fence, no `tx_height.body`)

`tx_height.body` (4 B/tx) is **gone**. Create height is O(blocks): a resident
fence of `confirmed[h]` → `header_txs` `(first_fk, count)`. Point query is a
binary search over confirmed runs. Reorg holes (orphaned Class A fks between
two confirmed runs) return unconnected (`None`), not the neighbor height.

Schema 15 leftover `tx_height.body` is unlinked on open (logged).

### Commit order (confirm)

1. `strong_tx` (may lead tip after kill)
2. Thin scripthash creates (may lead tip)
3. **`confirmed[]` tip advance** ← **commit**, then fence extend

`is_confirmed_strong(tx)` ⇔ strong ∧ fence contains the fk (implies height ≤ tip
and membership in `confirmed[h]` header_txs).  
On open: `repair_class_c_above_tip` unstrongs bits not on the fence.

---

## Archive epoch (`archive_epoch`)

Small control file (~32 B): magic, schema version, archive_mode flag, optional finalized_height, `wire_depth`. Bytes stay on disk; **`wire_depth` is unread** (leftover from a removed tip wire ring). Epoch finalize still fsyncs buried archive prefixes — it does not coordinate a live ring.

---

## Mainnet census (this tree’s reference datadir, 2026-08-13)

Tip **962,298**, **1,416,970,187** creates, mean packed **502.2 B/tx**,
~2.46 in / **2.70 out**. Exact HWM; outs ±2%; witness/in_base split ±10%.

| File | Packed 13/14 | Schema 15 |
|------|--------------|-----------|
| `tx.body` / `txout.body` | **662.73 GiB** | **~129 GiB** (16 B meta + outs, no spender) |
| `inwit.body` | — | **~486 GiB** (ins + witness; cold) |
| `spent.body` | (9 B inside packed outs, ~32 GiB) | **~32 GiB** |
| `{stem}.idx` | 5.28 GiB (`tx.idx`) | 5.28 GiB × **3** (grow-tight; do not 256 MiB-slab each) |
| `txid.body` / `tx.head` | 42.23 / 8.23 GiB | unchanged |

Hot pin+annotate working set: **txout + spent + three idx + txid + tx.head**
(~129+32+16+42+8 ≈ **227 GiB**) vs packed **tx.body + idx + txid + head**
(~663+5+42+8 ≈ **718 GiB**). Reconstruct / `getrawtransaction` also needs
`inwit` (~486 GiB), which pin/SH/Cake do **not** open.

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
