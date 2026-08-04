# Schema history

Historic on-disk layouts for the rbitcoin chain store.  
**Current layout:** [`SCHEMA.md`](./SCHEMA.md) (`SCHEMA_VERSION = 13`).

Until 1.0 there is **no in-place migration**: a new major layout generally means wipe the store and redo IBD. This file is for archaeology, code archaeology, and understanding why the current design looks the way it does.

Versions below are listed **newest → oldest** after the summary table.

---

## Summary

| Version | Headline change | Still in current tree as… |
|--------:|-----------------|---------------------------|
| **13** | Dense `txid.body` sidefile; packed body **without** leading txid; RWF_DONTCACHE policy | **Current** |
| **12** | Datadir `store.secret`; script/witness XOR at rest; keyed `tx.head` mix; head overflow; durable `block_queue/` | Prior |
| **11** | Txid-first packed body; 8-byte align + page rule; segmented u32 stride `tx.idx.*` | Prior |
| **10** | Packed inputs: `create_fk:u64` + vout (not `prev_txid[32]`); online `tx.head` resize; default BITS=28 | Prior packed layout |
| **9** | Keyless `tx.head` 4 B entries (no HAS_NEXT); `tx_height` u32 slots | `tx.head` layout family (evolved) |
| **8** | Keyless `tx.head` 8 B (fk + HAS_NEXT); `tx_height` u64 | Superseded by v9 packing |
| **7** | Hash heads: 16 B key prefix + multi-fk `.mlt` lists | `header.head` / generic `HashHead` |
| **6** | SH head 32 B slots; body entry = create_tx_fk only (no vout) | Current SH value encoding |
| **5** | Spend annotations on create outputs; remove `point.head` | Current spend model |
| **4** | Hybrid SH (inline / geometric slab); external prev_txid inputs; strong_tx bitset | SH slab idea; inputs later packed |
| **≤3** | Early mmap store; fat heads; mixed prev encoding | Mostly gone |

---

## v13 (current)

See [`SCHEMA.md`](./SCHEMA.md).

**Relative to v12:**

- Dense **`txid.body`**: 32-byte header + 32-byte txid per create_fk (append with Class A).
- Packed **`tx.body` meta without leading txid** (32 B meta only); 8-byte align only (no page non-straddle for body txid).
- Head-resolve identity via **sidefile**, not Prefix33 body peeks.
- **RWF_DONTCACHE** on uring SQEs: all `tx.body` r/w; `tx.idx`/`tx.head` older than open+past 3 sealed; `txid.body` reads more than 100M entries from tail.
- IBD **body queue is process-local RAM** (not durable `store/block_queue/`): avoids double disk write of every block (queue + Class A); restart redownloads; soft densify ~1.5 min / resume ~1 min of tip-rate depth. Legacy on-disk queue dirs are ignored/removed on open.
- **Wipe / reindex required** from schema 12 (body layout + new sidefile incompatible).

## v12

**Relative to v11:**

- **`store/store.secret`** (32 B CSPRNG) on datadir create; required on open.
- Class A **scriptSig / witness / scriptPubKey** XOR-obfuscated on disk (amounts/txids clear).
- **`tx.head` probe keys** = `SHA256(secret || txid)` (not raw prefixes).
- **`tx.head.overflow`** sidecar for depth-exhausted inserts (overflow-first lookup).
- Durable **`block_queue/`** for IBD payloads (dequeue after confirm-write).
- **Wipe datadir from v11** (secret + mixed head keys + XOR incompatible).

## v11

**Relative to v10:**

- Drop leading **`PACKED_TX_V1` (0x01)** magic; record starts with **TxRecord** so **txid is at absolute offset `S`** (bytes `[S, S+32)`).
- Record starts are **8-byte aligned**; a 32-byte txid **must not cross a 4 KiB page** (`S % 4096 ≤ 4064`).
- Writer **zero-pads** between records so the next start meets alignment; pad is included in the previous record’s idx span; decode accepts **trailing zeros** only.
- Thin `body_txid` / head-resolve prefixes are **32 B at `S`** (was 33 B magic+txid).
- Replace single-file **u64 absolute** `tx.idx` with **segmented u32 stride-8** files (`tx.idx.meta` + `tx.idx.NNNNNN`) — ~50% smaller idx (~4 B/tx).
- **Wipe datadir from v10** (packed payload + idx layout incompatible).
- `tx.head` algorithm unchanged (still page-then-open keyless). Keyless lookups always walk unsuccessful probe depth then reverse body-check candidates; page-then-open costs **1+C** cold pages vs **U+C** for plain open (~1.7–1.9× worse for open at α=0.70–0.90).

---

## v10

**Relative to v9:**

- Class A **inputs** store **`create_fk:u64` + CompactSize vout** instead of `prev_txid[32]` + vout (~−24 B per non-coinbase input).
- Soft `prev_txid` is **RAM-only** (wire rebuild from create body).
- Archive resolves parent fks once (batch map → sticky → durable `tx.head`).
- `tx.head` gains **online sequential resize**, **meta** (`bits` / `entry_bytes` / `generation`), default **BITS=28**, **8 B entries from BITS ≥ 33**.
- **Wipe datadir from v9** (input stream incompatible).
- Packed layout was `0x01 | TxRecord | inputs | outputs` (strict length, no trailing pad).

---

## v9 — keyless 4 B `tx.head`

**Theme:** Shrink the address head and height table; drop HAS_NEXT so all 32 bits of a 4 B slot are create_fk.

### `tx.head`

- Single file (not shards): fixed `2^BITS` × **4 B** entries.
- Typical mainnet create: **BITS=31** → **8 GiB** sparse.
- Entry = LE **u32** create_fk (`0` = empty). **No HAS_NEXT** — probe until empty.
- Double-hash probe from txid; max probe 128.
- Insert: first empty or same-fk idempotent; second same-txid create appends deeper (no write-time BIP30 displace).
- Lookup: body-verify last occupied → first.
- **No growth rehash** in v9 (capacity planning: ~75% of 2^31 ≈ 1.6 B entries; further growth was “future pain”).

### `tx_height`

- **4 B** slots: stored value = height + 1 (0 = unset). Index = `tx_fk − 1`.

### Unchanged vs later packing

- Inputs still carried **`prev_txid[32]`** + vout on disk.
- Dense Class A fk + `tx.idx` retained.
- Header / SH heads still open-address with 16 B prefixes.

### Why

- 4 B head entries cut address-head RAM/disk vs v8’s 8 B.
- Dropping HAS_NEXT doubles usable fk range in 32 bits (cap ~4 B txs before entry widen).

---

## v8 — keyless 8 B `tx.head` + u64 heights

**Theme:** Move `tx.head` from sharded/open-hash-with-keys toward a **fixed address** table, still 8 B wide.

### `tx.head`

- `2^31` × **8 B** entries (fk + **HAS_NEXT** bit / packing).
- Still keyless probing from txid in spirit; capacity and packing differ from v9.

### `tx_height`

- **u64** slots (later compressed to u32 in v9).

### Why → v9

- HAS_NEXT burned a bit (or complicated packing); 8 B × 2^31 is 16 GiB-class sparse.
- v9 reclaimed bits and halved entry width for the common case (`fk ≤ u32::MAX`).

---

## v7 — 16 B key prefixes + multi-lists

**Theme:** Shrink open-address heads that store full 32 B keys.

### Hash heads (`header.head`, early tx heads, etc.)

- Slot = **16 B key prefix** + **8 B packed value** (24 B) instead of 32+8.
- When multiple fks share a prefix (or BIP30 same full key): packed value high bit → **multi-list** in sibling `*.mlt` / shard `NN.mlt`.
- Multi-list record: `create_fk:u64 | next:u64` (prepended; newest first).
- Lookups that need exact identity: `get_all` + **body verify**.
- Rehash at load **7/8** (earlier eras rehashed more aggressively, e.g. ~1/2).

### Why

- ~40% smaller head slots vs full-key heads.
- Multi-list handles collisions without encoding full keys in every slot.

### Still current

- This model remains for **`header.head`** and generic `HashHead`.
- **Not** how scripthash create multisets are stored (those use hybrid slabs).
- **Not** how v9+ `tx.head` works (keyless u32/u64 address table).

---

## v6 — scripthash value layout (create_fk only)

**Theme:** Make the Electrum index thinner.

### Scripthash

- Head key = first **16 B** of `SHA256(spk)`.
- Head slot = **32 B**: key[16] + value[16] (two u64s) — same hybrid modes as v4/v5:
  - inline ≤2 create_tx_fks
  - slab meta for ≥3
- **Body entry = 8 B create_tx_fk only** (no vout in the index).
- Vouts expanded at query by loading Class A outputs and matching full scripthash.

### Why

- Dropping vout from every SH entry cuts index size and simplifies slab packing.
- Tradeoff: query must open Class A (or meta+outputs) to expand outpoints — acceptable for Electrum join cost already dominated by Class A/spend.

### Still current

- This is the live SH entry encoding (with v5 spends and later archive packing).

---

## v5 — spend-on-output (kill `point.head`)

**Theme:** Delete the multi-GiB spend open-hash multimap.

### Spends

- Each create **output** carries:
  - `spender_field:u64`
  - `MULTI_SPENDER` flag when needed
- Sole spender: field = spending_tx_fk.
- Multi: field = head into **`spenders.body`** list (`spending_tx_fk | next`, 16 B).
- **No `point.head`**, no point multimap body as primary spend index.

### Why

- `point.head` was on the order of **~11 GiB** open-hash with rehash storms (see older IBD IO notes).
- Annotations ride create outputs that confirm already touches; rare multi-lists stay small.

### Semantics retained today

- Best-chain spentness filters with `is_confirmed_strong(spender)`.
- Annotations may remain after disconnect / for non-strong spenders.

---

## v4 — hybrid scripthash + structural cleanup

**Theme:** Electrum-friendly create lists without a full linked list per busy script; tighten Class A input model and Class C.

### Scripthash (hybrid)

- Head holds **≤2 inline** creates **or** one **geometric body slab**.
- Size-class freelist reuses free slabs.
- Early hybrid entries sometimes carried **create_tx_fk | vout** (vout later dropped in v6).
- **No spend columns** on SH — spentness from points (then v5 annotations) + Class C.

### Class A inputs

- Always **external** `prev_txid[32]` + vout on disk (no mixed on-disk `prev_tx_fk` / local-prev).
- Legacy `LOCAL_PREV` later rejected on decode.

### Class C

- **`strong_tx` bitset** (1 bit per tx_fk) instead of fat per-tx strong records.

### Hash heads

- Rehash threshold moved toward **~7/8** load (from more aggressive growth).

### Points (pre-v5)

- Thin point body: spend edge only; outpoint as head key via SHA256 — **removed in v5**.

### Why

- Busy scripthashes: **one slab read** instead of walking a multi-list of creates.
- Strong bitset: ~64× smaller than u64-per-tx strong storage.

### Still current

- Hybrid inline/slab **idea** and freelist; entry payload simplified in v6; spends moved in v5.

---

## v3 and earlier (sketch)

Early mmap relational store (libbitcoin-class tables):

- Class A / B / C split already present in spirit.
- Fatter heads (full keys, lower load thresholds, more rehash churn).
- Mixed or local prev encodings on inputs (later standardized then packed).
- Point multimap as primary spend index (removed v5).
- File magic `RBT1` and 16 B headers established early; **schema version field** advanced as layouts broke.

Exact byte layouts for ≤v3 are not required for operating a v10 node; treat as “wipe and IBD” territory.

---

## Cross-cutting removals (by theme)

| Removed feature | Gone as of | Replacement |
|-----------------|-----------|-------------|
| Standalone `input.body` / `output.body` | packed Class A era | `PACKED_TX_V1` single body |
| `point.head` + point multimap primary | **v5** | output `spender_field` + rare `spenders.body` |
| SH body vout columns | **v6** | expand from Class A |
| `tx.head` HAS_NEXT / 8 B fixed mainnet | **v9** | 4 B entries, probe until empty |
| On-disk `prev_txid[32]` in inputs | **v10** | `create_fk` + vout; soft txid in RAM |
| Fixed non-resizing 31-bit-only head | **v10** (ops) | default 28 + online sequential resize |

---

## Related code comments

Workspace crates still mention version numbers in module docs (e.g. “schema v5 spends”, “schema v6 SH”). Those tags mean **“behavior introduced in that version and still current,”** not “this file is only valid at that version.” For the live byte layout, trust [`SCHEMA.md`](./SCHEMA.md) and `SCHEMA_VERSION` in `rbitcoin-primitives`.
