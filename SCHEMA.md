# On-disk schema (v3)

Versioned layouts for the chain store. **Format is unstable until 1.0.** Magic bytes and a schema version live in each file header.

**Schema v3** (current): builds on v2 density (length-from-idx, per-tx I/O runs, header ranges, compact flags) and adds:

- **Local prevouts** as `prev_tx_fk` + vout (not 32-byte txid) when the prev tx is in this store
- **Thin point** body (spend edge only; outpoint is head key via SHA256)
- **Thin scripthash** body (create_tx_fk + vout + next only; spentness via points + Class C)
- **strong_tx bitset** (1 bit per tx_fk vs u64)
- **Hash heads** rehash at ~7/8 load (was 1/2)

Reindex-only from earlier versions.

Endianness: **little-endian** for all multi-byte integers.

## Datadir layout

```text
<datadir>/
  store/
    meta                         # store magic + schema version
    header.body / header.head    # Class A headers + hash index
    tx.body / tx.idx / tx.head   # Class A txs (growable idx + hash head)
    input.body / input.idx       # per-tx input runs (+ BIP141 witness)
    output.body / output.idx     # per-tx output runs
    point.body / point.head      # Class B spend multimap (thin edges)
    confirmed.body               # Class C: height → header fk
    strong_tx.body               # Class C: bitset, bit (tx_fk-1) = strong
    header_txs_first.body        # header_fk-1 → first_tx_fk (0 = no body)
    header_txs_count.body        # header_fk-1 → tx count
    scripthash.body / scripthash.head  # Class B Electrum scripthash (thin)
    archive_epoch                # finalize + archive_mode
  wire/                          # tip wire ring (soft zone only)
```

Confirmed height → txs: `confirmed[h]` → header_fk → `(first_tx_fk, count)` arithmetic range.

## Who writes what

See [`docs/concurrency.md`](./docs/concurrency.md): during parallel IBD, one dedicated OS thread owns Class A writes; confirm owns Class C on another OS thread; peer IO does not write the store.

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

## Hash head (`*.head`)

- Slot = 32-byte key + 8-byte fk; power-of-two slot count; linear probe.
- Rehashes when load would exceed **7/8**.

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
| flags | u8 — `SEQ_FINAL`, `EMPTY_SCRIPT`, `EMPTY_WITNESS`, `NULL_PREV`, `LOCAL_PREV` |
| prev | `NULL_PREV`: none; `LOCAL_PREV`: CompactSize `prev_tx_fk` + CompactSize vout; else `prev_txid[32]` + CompactSize vout |
| sequence | omitted if `SEQ_FINAL`; else u32 LE |
| script_sig | omitted if empty; else CompactSize len + bytes |
| witness | omitted if empty; else CompactSize n + (CompactSize len + bytes)×n |

Archive writer resolves local prev when prev tx is in the same mega-batch or (if `tx.head` on) already indexed.

### Output run (one var record per tx with outputs)

flags (empty / OP_TRUE) + uleb128 value + optional CompactSize script.

### Point multimap entry (fixed 20)

spending_tx_fk u64, spending_input_index u32, next u64.  
Head key = SHA256(out_txid \|\| vout_le). Outpoint filled from query args when walking spenders.

### Header tx range

`header_txs_first[header_fk-1]` + `header_txs_count[header_fk-1]`. Contiguous assignment required.

### Scripthash entry (fixed 20 bytes — thin outpoint pointer)

Head key = `SHA256(scriptPubKey)`. Body does **not** store the scripthash.

| Field | Type |
|-------|------|
| create_tx_fk | u64 (`0` = tombstone / unlinked) |
| vout | u32 |
| next | u64 |

Heights, spend state, txid, and value are **joined** at query time from Class A / points / Class C (`tx_height`, `is_confirmed_strong`, `has_confirmed_strong_spender`). Older 68-byte layouts require reindex.

### Archive epoch (`archive_epoch`, 32 bytes)

magic, schema version, archive_mode flag, optional finalized_height, wire_depth.

## Chain ops (query layer)

- `connect_block` / archive + confirm as before.
- `spenders(outpoint)` — confirmed-strong only (`is_strong` and `tx_height ≤ tip`); `spenders_raw` for full multimap.
- Electrum helpers join thin scripthash rows to Class A outputs.

## Identity

- FK 0 = null; otherwise 1-based.
- Until 1.0, incompatible layout changes are reindex-only.
