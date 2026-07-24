# On-disk schema (current)

**Version:** `SCHEMA_VERSION = 10` (`rbitcoin_primitives`).  
**Status:** unstable until 1.0 — incompatible layout changes are reindex-only (wipe store / redo IBD).  
**Endianness:** little-endian for all multi-byte integers.

Older versions and migration notes live in [`SCHEMA_HISTORY.md`](./SCHEMA_HISTORY.md).

---

## Design at a glance

| Concern | Choice | Why |
|---------|--------|-----|
| Class A body | **Packed** full tx in one `tx.body` record | One IO to reconstruct; no separate input/output tables |
| Non-coinbase prevout | On-disk **`create_fk:u64` + CompactSize vout** | Smaller than `prev_txid[32]`; archive stamps fk once; wire fills soft `prev_txid` from create body |
| Txid → create | Keyless **`tx.head`** (address table of dense fks) | No 16 B key material; online sequential resize; body verifies identity |
| Spentness | Annotation on **create output** (+ rare multi-list) | No multi-GiB `point.head` open-hash |
| Electrum index | Thin **create_tx_fk only** (inline ≤2 or geometric slab) | Small index; expand vouts/value/height at query via Class A + Class C |
| Best-chain commit | Advance **`confirmed[]` last** | Tip is the commit point; strong/height may lead tip after kill |

---

## Datadir layout

```text
<datadir>/
  store/
    meta                         # store magic + schema version
    header.body / header.head    # Class A headers + hash index
    tx.body / tx.idx / tx.head   # Class A txs + address head (layout in footer)
    tx.head.resize / tx.head.new # online head rebuild (transient)
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

**Who writes what:** see [`docs/concurrency.md`](./docs/concurrency.md). IBD: dedicated archive thread owns Class A; confirm owns Class C; peer IO does not write the store.

---

## Common file header (16 bytes)

| Offset | Size | Field |
|--------|------|-------|
| 0 | 4 | Magic `RBT1` |
| 4 | 2 | Schema version (u16) — **10** |
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

### Packed body (`PACKED_TX_V1`)

Each `tx.body` payload:

```text
0x01 | TxRecord (64 B fixed meta) | inputs… | outputs…
```

There are **no** standalone `input.body` / `output.body` tables.

**TxRecord (64 B):** txid, version, locktime, `input_start_fk`, `input_count`, `output_start_fk`, `output_count`.  
On packed rows, `input_start_fk` / `output_start_fk` are always null (layout reserved; I/O lives in the same payload).

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

## Tx address head (`tx.head`)

Keyless open-address table: **txid → dense create_fk**.

| Property | Current |
|----------|---------|
| File | Single `tx.head` (not sharded) |
| Default | **BITS=26**, **4 B** entries → **256 MiB** sparse (`2^26` slots = 2¹⁶ pages × 1024) |
| Env | `RBITCOIN_TX_HEAD_BITS` in **8..=34**; tiny scale uses BITS=16 |
| Entry | LE create_fk; **0 = empty**; **no HAS_NEXT** |
| Entry width | **4 B** for BITS ≤ 32; **8 B** for BITS ≥ 33 (page then 8 KiB) |
| Meta | Embedded in **trailing footer** (no sidecar): bits, entry_bytes, generation; **version 5** |
| File layout | Slots at **offset 0** (page-aligned); **32-byte** footer at end (16-byte store magic/HWM + 16-byte layout) |
| Probe | **Page** from high txid bits; **10-bit** in-page double-hash; one page load (4 KiB @ 4 B); max depth **1024**; first insert depth **>128** starts online resize if not already running |
| Insert | First empty in-page (or same fk idempotent); second same-txid goes **deeper** in-page |
| Lookup | Body-verify from **last occupied → first** (newest BIP30-shaped create wins) |

**Probe note:** all candidates for a key share one page (single IO). Keyless slots cannot Robin-Hood. Footer layout **≠ v5** (or missing magic) refused on open → recreate + rebuild from Class A.

### Online sequential resize

Trigger: `txs.count() / slots ≥ 0.75` (or probe exhaust → sleep-retry while resize runs).

1. Create `tx.head.new` at `bits+1` (entry width from policy).
2. Fill shadow **only** from dense Class A `fk = 1..=count` via `tx.idx` (deterministic order).
3. Live archive inserts continue on **primary only** (no dual-write).
4. Catch-up + brief exclusive insert lock → rename swap; control `tx.head.resize` for crash resume.

**Capacity @ 0.75 load (approx):**  
26→50 M · 27→100 M · 28→215 M · 29→429 M · 30→859 M · 31→1.72 B · 32→3.44 B · 33→6.87 B (8 B) · 34→13.7 B.

**Decision:** start small (28) and resize during IBD rather than one fixed 8 GiB 31-bit cliff; sequential rebuild keeps the archiver hot path free of dual-write.

---

## Hash heads (`header.head`, generic)

Used where the key is a 32 B hash and the value is a single fk (or multi-list).

- Slot = **16 B key prefix** + **8 B packed value** (24 B); power-of-two slots; linear probe.
- Packed value: sole fk (high bit clear), or `MULTI_BIT | list_fk` → sibling `.mlt` (`create_fk:u64 | next:u64`, newest first).
- Multi-list: 16 B prefix collisions and BIP30-style multiples.
- Identity: `get_all` candidates + **body verify**.
- Rehash when load would exceed **7/8**.

**Not** used for `tx.head` (keyless address) or for scripthash **create lists** (those use hybrid slabs).

---

## Class B — scripthash (Electrum)

Thin create index: **create_tx_fk only** (no vout in the index).

### Head

- Key = first **16 B** of `SHA256(scriptPubKey)` (Electrum hash; wire APIs still use 32 B).
- Slot = **32 B**: key[16] + value[16] (two u64s).
- May be sharded (16 shards) for open-address locality.

| Mode | When | Value |
|------|------|--------|
| Empty | no creates | 0, 0 |
| Inline | ≤2 create_tx_fks | fk0, fk1 (0 = unused second) |
| Slab | ≥3 | high bit + class/used; `w1` = absolute slab offset |

### Body slabs

- After RBT1 header: 4 KiB alloc page (`SHAL`) + geometric slabs (cap 4, 8, 16, …; 8 B/entry).
- Size-class freelist reuses free slabs.
- Same-class growth is append-only when capacity allows; class bump copies into a larger slab.

### Query join

Heights, value, spentness, vouts: expand from Class A outputs (match full scripthash) + spend annotations + Class C.  
IBD may stage creates in **sorted runs** and bulk-materialize durable SH at tip entry.

**Decision:** hybrid inline/slab so multi-use scripts are **one contiguous slab read**, not a multi-linked-list of creates. Query cost for busy wallets is dominated by Class A + spend joins, not SH pointer chasing.

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
- Optional manual `backfill_tx_index` rebuilds primary `tx.head` mappings (not part of tip entry).

---

## Related docs

| Doc | Topic |
|-----|--------|
| [`SCHEMA_HISTORY.md`](./SCHEMA_HISTORY.md) | Prior schema versions |
| [`docs/concurrency.md`](./docs/concurrency.md) | Writer ownership, IBD vs tip |
| [`docs/crash-recovery.md`](./docs/crash-recovery.md) | Kill safety, reorg, resize control |
| [`OPERATOR.md`](./OPERATOR.md) | Datadir ops, env knobs |
