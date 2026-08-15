# Schema history

Historic on-disk layouts for the rbitcoin chain store.  
**Current layout:** [`SCHEMA.md`](./SCHEMA.md) (`SCHEMA_VERSION = 17`, durable).

Until 1.0 there is **no in-place migration**: a new major layout generally means wipe the store and redo IBD. This file is for archaeology, code archaeology, and understanding why the current design looks the way it does.

Versions below are listed **newest → oldest** after the summary table.

---

## Summary

| Version | Headline change | Still in current tree as… |
|--------:|-----------------|---------------------------|
| **17** | **Durable.** SH runs `key_len=40`; Class A thin meta + kinds 0–9 + 8 B spent; megakey pages delta-stream; `spent.ovf`; no `archive_epoch`. Refuse leftover `key_len=32` runs, raw-u64 SH pages, and 16-layout Class A. | **Current** |
| **16** | Drop `tx_height.body`; RAM fence from `confirmed[]` + `header_txs_*`. Soft-open 15 | Prior |
| **15** | Class A `txout`/`inwit`/`spent` split; SH slabs + sorted heads; refuse packed Class A with txs and page-era SH | Prior |
| **14** | SH head Empty/Inline/**Paged** (4 KiB page chains); seal @0.8 + overflow OA; refuse slab values | Prior |
| **13** | Dense `txid.body` sidefile; packed body **without** leading txid; RWF_DONTCACHE policy | Prior |
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

## v17 (durable)

Closed layout. SH run catalogs are unique `(scripthash, create_fk)` at
`key_len=40`. Schema-16 `key_len=32` catalogs are refused.

Hot Class A: thin LAYOUT17 `txout` meta, script kinds **0–9** (unknown
kind is Corrupt — a new implicit-width template is 18), 8 B spent slots,
`spent.ovf` overflow. Inwit flags bits 4–7 and spent flags other than
`MULTI_SPENDER` are Corrupt. Inwit prevout is still `create_fk:u64` +
CompactSize vout (Δfk is an **18 / inwit-only** follow-up).

Megakey SH pages store ULEB128 fk0+deltas (`ver=1`). Leftover raw-u64
pages (`ver=0`, `n>0`) rematerialize.

`archive_epoch` is gone. Create does not plant `wire/` or packed
`tx.body`. Open unlinks leftover `archive_epoch` and `store/wire`.

## v16

Create height is no longer a 4 B/tx L0 file. A resident fence is built from
`confirmed[]` + `header_txs_first/count` (O(blocks), ~16 B/height). Point
query is an in-RAM binary search; reorg holes return unconnected. Schema 15
stores soft-open: leftover `tx_height.body` is unlinked and `meta` rewritten
to 16. Class A / Class B layout unchanged.

## v15

See [`SCHEMA.md`](./SCHEMA.md). Class B: geometric slabs + delta fks,
sealed sorted heads with idx (main has no fuse8; sealed ovf keeps fuse8),
one global ingest OA. Empty 13/14 SH
upgrades `meta` silently; a materialized page-era index is refused
(wipe `store/scripthash*` and rematerialize). Class A is split:
`txout` + `inwit` + `spent` (refuse packed `tx.body` with creates).

## v14

SH head Empty/Inline/Paged (4 KiB page chains). Sealed overflow OA segments
sized to one main shard. Combined prefix hole at 4112–8191. Replaced by v15.

### Side product: `sp_tweaks.*` thin BIP-352 index (still schema 14)

Optional `sp_tweaks.idx` + `sp_tweaks.body`. Soft-open; missing files are empty.
Not a `SCHEMA_VERSION` bump. Persist is `len:tweak` only (`0` or `33` +
compressed `A_tweak`); Cake outs join Class A packed body.

### Side format: sealed fuse8 payload v1 → v2 (still schema 14)

Not a full `SCHEMA_VERSION` bump — Class A / `tx.head` OA layout unchanged.

| | |
|--|--|
| **v1** | `BF8R` envelope + bincode of **xorf** `BinaryFuse8` (pre in-tree port) |
| **v2** | `BF8R` + explicit LE body of in-tree `BinaryFuse8` |

**Open behavior:** a sealed `tx.head.NNNNNN.fuse8` at **v1** (or unreadable v2 body) logs
`store: tx.head fuse migrate …` and uses an always-probe gate, then **rewrites** the
fuse from Class A txids as v2. The head tables are **not** wiped. (A binary that
only hard-failed on decode would recreate+rebuild the entire segmented head — avoid.)

**Process for next format change:** bump the fuse **version** field, soft-open legacy,
log a one-line warn, migrate or refuse with a clear message — do not silently
`Corrupt` → full head wipe unless the OA layout itself changed.

**Relative to v13:**

- Class B SH head value: **Empty / Inline (≤2) / Paged** (first+last **4 KiB** page offs; bit-63 flags).
- Body uses fixed **4096 B page chains** (≤510 FKs/page); geometric **slab** packing is refused on decode.
- Main OA **does not rehash** after create size; load ≥ **~0.80** seals main (`scripthash.main_sealed` + optional fuse product); **new keys** go to **`scripthash.ovf/`** mono segment stack (slots = one main shard; seal+roll at ~0.8 with real BF8R).
- Cold bulk materialize writes paged layout only (same put routing spirit as tip).
- **Open path:** schema **13** stores with **no materialized scripthash head** open
  on this binary and **silently rewrite** store `meta` to 14 (Class A layout matches).
  Empty SH body with **SHAL alloc v1** is rewritten to **alloc v2** (page-chain era).
  A schema-13 store that already has a durable SH index (or non-empty alloc v1) is
  **refused** — wipe `store/scripthash*` (or full datadir) and rematerialize; no dual-read of slab values.
- Interim full-size single-file `scripthash.ovf.head` (if any) is **wiped on open**;
  segmented `scripthash.ovf/` is the only live overflow layout (schema still **14**).

## v13

**Relative to v12:**

- Dense **`txid.body`**: 32-byte header + 32-byte txid per create_fk (append with Class A).
- Packed **`tx.body` meta without leading txid** (32 B meta only); 8-byte align only (no page non-straddle for body txid).
- Head-resolve identity via **sidefile**, not Prefix33 body peeks.
- **RWF_DONTCACHE** on uring SQEs (historical multi-target policy; current code is spend-annotate body pwrites only — see SCHEMA.md).
- IBD **body queue is process-local RAM** (not durable `store/block_queue/`): avoids double disk write of every block (queue + Class A); restart redownloads; soft densify assign: under ~100 MiB free ahead; over that only ~1 min confirm window at tip rate. Legacy on-disk queue dirs are ignored/removed on open.
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
