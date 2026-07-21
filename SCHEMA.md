# On-disk schema (v9)

Versioned layouts for the chain store. **Format is unstable until 1.0.** Magic bytes and a schema version live in each file header.

**Schema v9** (current): **`tx.head`** is a fixed keyless address table — single file, `2^BITS` × **4 B** entries (mainnet `BITS=31` → **8 GiB** sparse), double-hash probe, **`u32` create_fk** (0 = empty), **no HAS_NEXT** (continue until empty). Dense Class A fks + **`tx.idx`** retained. **`tx_height`** uses **4 B** slots (`height+1`, 0 = unset). Lookups verify Class A body txid. Header / scripthash heads remain open-address with 16 B key prefixes. Fresh datadir from v8.

**Future head pain** (mainnet `BITS=31`, ~2.1 B slots; ~75% load ≈ **1.6 B** entries): first widen address **BITS** (e.g. 32-bit rebuild); ~**3.2 B** txs → second widen (e.g. 33-bit); only then ~**4 B** txs → **`u32` create_fk exhausted**, head entries must become **8 B**.

**Schema v8**: `tx.head` 2^31 × 8 B (fk + HAS_NEXT); `tx_height` u64 slots.

**Schema v7**: hash heads use **16 B key prefixes** (24 B slots) plus optional multi-fk lists (`*.head.mlt` / shard `NN.mlt`).

**Class A bodies are packed-only**: each `tx.body` record is `PACKED_TX_V1 || TxRecord || inputs || outputs` (one body IO for full reconstruct). There are **no** standalone `input.body` / `output.body` tables.

**Schema v6**: scripthash head **16 B key prefix** + **16 B value** (32 B slots); body entries are **create_tx_fk only** (8 B). Plus v5 spends.

**Schema v5**: spend annotations on each create **output** (`spender_field:u64` + `MULTI_SPENDER` flag); rare multi-spend lists in `spenders.body` (16 B: spending_tx_fk | next). **No `point.head` open-hash multimap.**

**Schema v4**: hybrid scripthash (2-inline head or geometric body slab + size-class freelist). Builds on v3:

- **Class A inputs always external** `prev_txid[32]` + vout (no on-disk `prev_tx_fk` / local-prev mix)
- **Thin point** body (spend edge only; outpoint is head key via SHA256) — **removed in v5**
- **strong_tx bitset** (1 bit per tx_fk vs u64)
- **Hash heads** rehash at ~7/8 load (was 1/2)

Legacy input flag `LOCAL_PREV` (old local `prev_tx_fk`) is **rejected** on decode.

Endianness: **little-endian** for all multi-byte integers.

## Datadir layout

```text
<datadir>/
  store/
    meta                         # store magic + schema version
    header.body / header.head    # Class A headers + hash index
    tx.body / tx.idx / tx.head   # Class A txs (growable idx + v8 keyless address head)
    spenders.body                # multi-spender list nodes only (v5; sole spends on outputs)
    confirmed.body               # Class C: height → header fk
    strong_tx.body               # Class C: bitset, bit (tx_fk-1) = strong
    header_txs_first.body        # header_fk-1 → first_tx_fk (0 = no body)
    header_txs_count.body        # header_fk-1 → tx count
    scripthash.body / scripthash.head  # Class B Electrum scripthash (thin)
    archive_epoch                # finalize + archive_mode
    scripthash.runs              # SH sorted runs during Direct IBD (bulk-load at tip)
  wire/                          # tip wire ring (soft zone only)
```

Confirmed height → txs: `confirmed[h]` → header_fk → `(first_tx_fk, count)` arithmetic range.

## Who writes what

See [`docs/concurrency.md`](./docs/concurrency.md): during IBD, one dedicated OS thread owns Class A writes; confirm owns Class C on another OS thread; peer IO does not write the store.

## Common file header (16 bytes)

| Offset | Size | Field |
|--------|------|-------|
| 0 | 4 | Magic `RBT1` |
| 4 | 2 | Schema version (u16) — **3** |
| 6 | 2 | Table kind (u16) |
| 8 | 8 | Logical length of file (bytes), including this header |

## Table kinds

| Kind | Name |
|------|------|
| 1 | meta |
| 2 | header |
| 3 | tx |
| 4 | input |
| 5 | output |
| 6 | point |
| 7 | strong_tx |
| 8 | confirmed |
| 9 | array_link (idx files, dense u64 arrays) |
| 10 | hash_head |
| 11 | scripthash (Electrum script hash multimap) |

## Growable var records (`*.body` + `*.idx`)

- **body**: append-only **unframed** payloads (no per-record length prefix).
- **idx**: dense `u64` absolute offsets into body; count = `(logical_len - 16) / 8`.
- Record length = `idx[i+1] - idx[i]` (last record: body logical end − start).
- FK is **1-based** index into idx.

## Hash head (`header.head`, `scripthash.head`, …)

- Slot = **16-byte key prefix** + 8-byte packed value (24 B); power-of-two slot count; linear probe.
- Packed value: sole `create_fk` (high bit clear), or multi-list head (`MULTI_BIT | list_fk`) pointing at a sibling multi-list file (`path.mlt` or sharded `NN.mlt`).
- Multi-list record (16 B): `create_fk:u64 | next:u64` (prepended on insert; newest first). Used for 16-byte prefix collisions and BIP30 (identical full txid → multiple Class A rows).
- Lookups that need exact identity load candidate fks via `get_all` and **verify** the body hash/txid.
- Rehashes when load would exceed **7/8**.

## Tx address head (`tx.head`, schema v9)

- **Single file** (not a shard directory). Fixed `2^BITS` slots × **4 B** (mainnet `BITS=31` → **8 GiB** sparse; tests: `RBITCOIN_HEAD_SCALE=tiny` / `RBITCOIN_TX_HEAD_BITS`).
- Entry = LE **`u32`**: dense `create_fk` (1-based; **0 = empty**). **No HAS_NEXT** — all 32 bits available for fk (cap ~4 B txs).
- **No key bytes**. Double-hash probe from txid:
  - `h1` = leading `BITS` of `txid[0..4]` (BE)
  - `h2` = odd step from `txid[4..8]`
  - `idx(d) = (h1 + d·h2) mod 2^BITS`, `d < MAX_PROBE` (128)
- Body mismatch ⇒ continue until **empty** slot (no deletes on Class A).
- BIP30: newest `create_fk` at earliest matching probe slot; older pushed deeper.
- Lookups verify packed Class A body txid. Dense fk still resolves body via **`tx.idx`**.
- **No growth rehash**. Future: ~**1.6 B** txs → first **BITS** widen; ~**3.2 B** → second **BITS** widen; ~**4 B** → **8 B** entries (fk width).

## Dense u64 arrays (`confirmed`, `header_txs_first`, `header_txs_count`)

- After file header: packed `u64` values.
- Confirmed: index = height; length = tip_height+1 when chain non-empty.
- header_txs_first / header_txs_count: index = header_fk - 1; block membership is `[first, first+count)`.

## strong_tx bitset

- After file header: packed bits; bit `(tx_fk - 1)` set means the tx is strong on the best chain.
- `set_strong_range` sets a contiguous bit range (confirm path).
- **Commit point:** Class C writes `strong_tx` / `tx_height` first, then advances `confirmed[]`. Point edges are written at **archive** when spend_index is on (confirm does not re-probe). A kill mid-batch can leave strong bits above tip; `spenders` / `is_confirmed_strong` require `tx_height ≤ tip`, and open runs `repair_class_c_above_tip` to clear those bits.

## Record layouts

### Header (fixed 88 bytes)

| Field | Type |
|-------|------|
| prev_fk | u64 |
| version | i32 |
| timestamp | u32 |
| bits | u32 |
| nonce | u32 |
| merkle_root | [u8; 32] |
| hash | [u8; 32] |

### Tx (variable; fixed payload 64 B)

txid, version, locktime, input_start_fk, input_count, output_start_fk, output_count.  
`input_start_fk` / `output_start_fk` are always null on packed rows (layout reserved; I/O live in the same body payload).

### Input run (one var record per tx with inputs)

Concatenation of compact inputs. Count is on the parent `TxRecord`.

Each input:

| Field | Encoding |
|-------|----------|
| flags | u8 — `SEQ_FINAL`, `EMPTY_SCRIPT`, `EMPTY_WITNESS`, `NULL_PREV` (`LOCAL_PREV` bit reserved; **decode reject**) |
| prev | `NULL_PREV`: coinbase (no payload); else `prev_txid[32]` + CompactSize vout |
| sequence | omitted if `SEQ_FINAL`; else u32 LE |
| script_sig | omitted if empty; else CompactSize len + bytes |
| witness | omitted if empty; else CompactSize n + (CompactSize len + bytes)×n |

Catch-up parent resolve uses light UTXO (`outpoint → create Class A fk`) or tip/wave caches — not a Class A `prev_tx_fk` field. Tip mode uses points / `tx.head`.

### Output run (one var record per tx with outputs) — also embedded in packed Class A

```text
spender_field:u64 LE | flags:u8 | uleb128 value | [script…]
```

| `MULTI_SPENDER` (flags bit 2) | `spender_field` |
|-------------------------------|-----------------|
| 0 | 0 = unspent; else sole **spending_tx_fk** |
| 1 | head fk into `spenders.body` list |

Best-chain spentness still requires `is_confirmed_strong(spender)` (leave annotations on reorg).

### Spenders body (multi only, fixed 16)

`spending_tx_fk:u64 | next:u64`. Append-only; used only when an outpoint has ≥2 annotated spenders.

### Header tx range

`header_txs_first[header_fk-1]` + `header_txs_count[header_fk-1]`. Contiguous assignment required.

### Scripthash (hybrid thin creates — schema v6)

Head key = first **16 B** of `SHA256(scriptPubKey)` (Electrum hash; wire still 32 B).

**Create entry** = 8 B: `create_tx_fk:u64` only. Vouts expanded at query by loading Class A outputs and matching full SHA256(spk).

**Head slot** = **32 B**: `key[16] + value[16]` (two u64s).

| Head mode | When | Value (`w0`, `w1`) |
|-----------|------|---------------------|
| Empty | no creates | 0, 0 |
| Inline 1–2 | ≤2 create_tx_fks | fk0, fk1 (0 = unused second) |
| Slab | ≥3 | `w0` high bit set + class/used; `w1` = file-absolute slab_off |

**Body** = RBT1 header + 4 KiB alloc page (`SHAL`) + geometric slabs (cap 4, 8, 16, …; 8 B/entry). Freelist reuses free slabs.

Heights, value, spentness joined at query from Class A / spend annotations / Class C.

### Archive epoch (`archive_epoch`, 32 bytes)

magic, schema version, archive_mode flag, optional finalized_height, wire_depth.

## Chain ops (query layer)

- `connect_block` / archive + confirm as before.
- `spenders(outpoint)` — confirmed-strong only (`is_strong` and `tx_height ≤ tip`); `spenders_raw` for full multimap.
- Electrum helpers join thin scripthash rows to Class A outputs.

## Identity

- FK 0 = null; otherwise 1-based.
- Until 1.0, incompatible layout changes are reindex-only.
