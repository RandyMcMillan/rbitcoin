# On-disk schema (v0)

Versioned layouts for the chain store. **Format is unstable until 1.0.** Magic bytes and a schema version live in each file header.

Endianness: **little-endian** for all multi-byte integers.

## Datadir layout

```text
<datadir>/
  store/
    meta                         # store magic + schema version
    header.body / header.head    # Class A headers + hash index
    tx.body / tx.idx / tx.head   # Class A txs (growable idx + hash head)
    input.body / input.idx
    output.body / output.idx
    point.body / point.head      # Class B spend multimap
    confirmed.body               # Class C: height → header fk
    strong_tx.body               # Class C: (tx_fk-1) → header fk (0 = unstrong)
    block_txs.body / block_txs.idx
    block_txs_height.body        # height → block_txs list fk
    scripthash.body / scripthash.head  # Class B Electrum scripthash multimap
    archive_epoch                # finalize + archive_mode
  wire/                          # tip wire ring (soft zone only)
```

## Common file header (16 bytes)

| Offset | Size | Field |
|--------|------|-------|
| 0 | 4 | Magic `RBT1` |
| 4 | 2 | Schema version (u16) — **0** |
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
| 9 | array_link (idx files, block lists, dense u64 arrays) |
| 10 | hash_head |
| 11 | scripthash (Electrum script hash multimap) |

## Growable var records (`*.body` + `*.idx`)

- **body**: append-only framed payloads (`u32 total_len` including the 4-byte prefix + bytes).
- **idx**: dense `u64` absolute offsets into body; count = `(logical_len - 16) / 8`.
- FK is **1-based** index into idx.

## Hash head (`*.head`)

- Slot = 32-byte key + 8-byte fk; power-of-two slot count; linear probe.
- **Rehashes (doubles slots)** when insert finds no free/equal slot — no silent fixed capacity.

## Dense u64 arrays (`confirmed`, `strong_tx`, `block_txs_height`)

- After file header: packed `u64` values.
- Confirmed: index = height; length = tip_height+1 when chain non-empty.
- Strong_tx: index = tx_fk - 1; value = header_fk or 0.

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

### Tx (variable, framed)

txid, version, locktime, input_start_fk, input_count, output_start_fk, output_count, raw_len, raw.

### Input (variable, framed)

parent_tx_fk, index, prev_txid, prev_index, sequence, script_sig_len, script_sig.

### Output (variable, framed)

parent_tx_fk, index, value, script_len, script. **No spender field.**

### Point multimap entry (fixed 56)

out_txid, out_index, spending_tx_fk, spending_input_index, next.

### Block tx list (variable, framed)

u32 count, then count × u64 tx_fk.

### Scripthash entry (fixed 108 bytes)

| Field | Type |
|-------|------|
| scripthash | [u8; 32] SHA256(scriptPubKey) |
| txid | [u8; 32] |
| vout | u32 |
| value | i64 |
| create_height | u32 |
| create_tx_fk | u64 |
| spend_height | u32 (`u32::MAX` = unspent) |
| spend_tx_fk | u64 |
| next | u64 (multimap chain) |

### Archive epoch (`archive_epoch`, 32 bytes)

magic, schema version, archive_mode flag, optional finalized_height, wire_depth.

## Chain ops (query layer)

- `connect_block(height, header, txs)` — archive write + point spends + **scripthash create/spend** + set strong + confirmed + block_txs.
- `disconnect_tip()` — clear strong for tip txs, clear scripthash spends at tip, clear confirmed tip (Class A rows remain).
- `spenders(outpoint)` — **only strong** spending txs; `spenders_raw` for full multimap history.
- `scripthash_history` / `scripthash_balance` / `scripthash_listunspent` — strong-filtered Electrum helpers.

## Identity

- FK 0 = null; otherwise 1-based.
- Until 1.0, incompatible layout changes are reindex-only.
