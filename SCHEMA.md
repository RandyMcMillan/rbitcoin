# On-disk schema (v8)

Versioned layouts for the chain store. **Format is unstable until 1.0.** Magic bytes and a schema version live in each file header.

**Schema v8** (current): **`tx.head` is a fixed keyless address table** — single file, `2^BITS` × **8 B** entries (mainnet `BITS=31` → **16 GiB** sparse), double-hash open-address probe, high bit `HAS_NEXT`, full `create_fk` in low 63 bits (0 = empty). No key material in the head; lookups verify Class A body txid. No shard directory / no `.mlt` for tx. **Header** and **scripthash** heads remain v7-style (16 B key prefixes + optional multi-lists). Fresh datadir / reindex required from v7.

**Schema v7**: hash heads (`tx.head`, `header.head`, …) use **16 B key prefixes** (24 B slots) plus optional multi-fk lists (`*.head.mlt` / shard `NN.mlt`) for prefix collisions and BIP30 duplicate full txids. Lookups verify the Class A body. Keeps v6 scripthash compression and v5 spends.

**Class A bodies are packed-only**: each `tx.body` record is `PACKED_TX_V1 || TxRecord || inputs || outputs` (one body IO for full reconstruct). Legacy 3-table rows (bare `TxRecord` + separate `input.body` / `output.body` runs addressed by `input_start_fk` / `output_start_fk`) are **rejected** on read. Standalone `input.body` / `output.body` files may still exist empty on create for layout stability; they are not the Class A read path.

**Schema v6**: scripthash head **16 B key prefix** + **16 B value** (32 B slots); body entries are **create_tx_fk only** (8 B). Plus v5 spends.

**Schema v5**: spend annotations on each create **output** (`spender_field:u64` + `MULTI_SPENDER` flag); rare multi-spend lists in `spenders.body` (16 B: spending_tx_fk | next). **No `point.head` open-hash multimap.**

**Schema v4**: hybrid scripthash (2-inline head or geometric body slab + size-class freelist). Builds on v3:

- **Class A inputs always external** `prev_txid[32]` + vout (no on-disk `prev_tx_fk` / local-prev mix)
- **Thin point** body (spend edge only; outpoint is head key via SHA256) — **removed in v5**
- **strong_tx bitset** (1 bit per tx_fk vs u64)
- **Hash heads** rehash at ~7/8 load (was 1/2)

Catch-up also uses process-local **light UTXO** (`ibd_utxo.map`, magic `RBUXTO03`) — not part of the `RBT1` table set; rebuilt from confirmed chain if missing/corrupt.

Legacy input flag `LOCAL_PREV` (old local `prev_tx_fk`) is **rejected** on decode.

Endianness: **little-endian** for all multi-byte integers.

## Datadir layout

```text
<datadir>/
  store/
    meta                         # store magic + schema version
    header.body / header.head    # Class A headers + hash index
    tx.body / tx.idx / tx.head   # Class A txs (growable idx + v8 keyless address head)
    input.body / input.idx       # per-tx input runs (+ BIP141 witness)
    output.body / output.idx     # per-tx output runs
    spenders.body                # multi-spender list nodes only (v5; sole spends on outputs)
    confirmed.body               # Class C: height → header fk
    strong_tx.body               # Class C: bitset, bit (tx_fk-1) = strong
    header_txs_first.body        # header_fk-1 → first_tx_fk (0 = no body)
    header_txs_count.body        # header_fk-1 → tx count
    scripthash.body / scripthash.head  # Class B Electrum scripthash (thin)
    archive_epoch                # finalize + archive_mode
    ibd_utxo.map                 # catch-up light UTXO (RBUXTO03; optional rebuild)
    tx.runs / point.runs / scripthash.runs  # catch-up sorted runs (when index_run mode)
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

## Tx address head (`tx.head`, schema v8)

- **Single file** (not a shard directory). Fixed `2^BITS` slots × **8 B** (mainnet `BITS=31` → 16 GiB sparse; tests use tiny bits via `RBITCOIN_HEAD_SCALE=tiny` / `RBITCOIN_TX_HEAD_BITS`).
- Entry = LE `u64`: low 63 bits = `create_fk` (1-based; **0 = empty**), bit 63 = **`HAS_NEXT`** (further probes may exist on this key’s sequence).
- **No key bytes** in the head. Primary probe uses double hashing from the txid:
  - `h1` = leading `BITS` of `txid[0..4]` (BE)
  - `h2` = odd step from `txid[4..8]`
  - `idx(d) = (h1 + d·h2) mod 2^BITS`, `d < MAX_PROBE` (128)
- Foreign occupants (different body txid at a probe slot) are normal: body mismatch ⇒ continue until empty or `HAS_NEXT` clear.
- BIP30 (duplicate full txid): newest `create_fk` at earliest probe slot; older fks pushed deeper on the same sequence.
- Lookups (`get_by_txid`) probe candidates and **verify** packed Class A body txid.
- **No growth rehash** (fixed table). Full-table rebuild only for a future width bump.

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
`input_start_fk` / `output_start_fk` address a **run** (one var record for all I/O of that tx).

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

### Light UTXO (`ibd_utxo.map`, catch-up only)

Separate file (not `RBT1`). Magic **`RBUXTO03`**, 4 KiB header + open-addressed slots.

| Slot field (24 B) | Layout |
|-------------------|--------|
| prefix | first 12 bytes of txid |
| pack | `state:u8` (empty/live/tomb) + `vout:u24` |
| create_fk | u64 LE Class A fk of the creating tx |

- Membership ≈ unspent; miss ⇒ spent or never created (when spend_index off).
- Full-txid collisions (same prefix+vout) use a rare process-local overflow map.
- Tip height in header must stay aligned with confirmed tip; corrupt/unsupported version → delete and rebuild from chain.
- Slot count: power of two; default `1<<22`; override `RBITCOIN_IBD_UTXO_SLOTS`.

## Chain ops (query layer)

- `connect_block` / archive + confirm as before.
- `spenders(outpoint)` — confirmed-strong only (`is_strong` and `tx_height ≤ tip`); `spenders_raw` for full multimap.
- Electrum helpers join thin scripthash rows to Class A outputs.

## Identity

- FK 0 = null; otherwise 1-based.
- Until 1.0, incompatible layout changes are reindex-only.
