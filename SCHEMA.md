# On-disk schema (v0 draft)

Versioned layouts for the chain store. **Format is unstable until 1.0.** Magic bytes and a schema version live in each file header.

Endianness: **little-endian** for all multi-byte integers.

## Datadir layout

```text
<datadir>/
  node.conf                 # optional generated snapshot of effective config
  store/
    meta                    # store magic, schema version, flags
    header.body             # Class A: block headers
    header.head             # hash head for header-by-hash
    tx.body
    tx.head
    input.body
    output.body
    point.body              # Class B: spend multimap body
    point.head
    strong_tx.body          # Class C
    confirmed.body          # height -> header fk (best chain)
    archive_epoch           # durable finalize record (Phase 5+)
  wire/                     # tip wire ring (post-IBD); absent during IBD
```

## Common file header (16 bytes)

| Offset | Size | Field |
|--------|------|-------|
| 0 | 4 | Magic `RBT1` (0x52 0x42 0x54 0x31) |
| 4 | 2 | Schema version (u16) — current **0** |
| 6 | 2 | Table kind (u16), see below |
| 8 | 8 | Reserved / flags |

## Table kinds

| Kind | Name | Class |
|------|------|-------|
| 1 | `meta` | — |
| 2 | `header` | A |
| 3 | `tx` | A |
| 4 | `input` | A |
| 5 | `output` | A |
| 6 | `point` | B |
| 7 | `strong_tx` | C |
| 8 | `confirmed` | C |
| 9 | `array_link` | A (generic dense array) |
| 10 | `hash_head` | head file |

## Record layouts (v0)

### Header body record

Fixed 88 bytes after file header region:

| Field | Type | Notes |
|-------|------|-------|
| prev_fk | u64 | FK to previous header (0 = genesis sentinel) |
| version | i32 | Block version |
| timestamp | u32 | |
| bits | u32 | |
| nonce | u32 | |
| merkle_root | [u8; 32] | |
| hash | [u8; 32] | Block hash (double-SHA256 of header) |

### Tx body record (variable)

| Field | Type |
|-------|------|
| txid | [u8; 32] |
| version | i32 |
| locktime | u32 |
| input_start_fk | u64 |
| input_count | u32 |
| output_start_fk | u64 |
| output_count | u32 |
| raw_len | u32 |
| raw | [u8; raw_len] | Full consensus serialization (witness included when present) |

### Output body record

| Field | Type |
|-------|------|
| parent_tx_fk | u64 |
| index | u32 |
| value | i64 | sats |
| script_len | u32 |
| script | [u8; script_len] |

**No spender field** (Class A write-once).

### Point multimap entry (Class B)

| Field | Type |
|-------|------|
| out_txid | [u8; 32] |
| out_index | u32 |
| spending_tx_fk | u64 |
| spending_input_index | u32 |
| next | u64 | Next entry fk in bucket chain (0 = end) |

Heads file: open-addressed or chained hash slots → first entry fk. Publish via allocate-then-CAS.

### Confirmed (height index)

Dense array: height `u32` → header `fk u64`.

## Identity

- **FK**: 1-based index into a table body (0 = null).
- Hash keys: full 32-byte txid/block hash in heads (v0; may truncate later with care).

## Epoch file (reserved)

See durable-archive variant. Not written until `archive_mode` path lands.

## Evolution

- Bump schema version on incompatible changes.
- Until 1.0, reindex-only migrations are acceptable.
