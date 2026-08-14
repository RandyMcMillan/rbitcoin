# Architecture: how rbitcoin differs

This page is the **newcomer map** for design uniqueness. Normative layouts and
role tables live in the linked deep docs; this document explains *why* the node
is built this way and how it compares to Bitcoin Core and to external Electrum
indexers.

**Status:** experimental 0.x. On-disk format and APIs are **unstable until 1.0**.

---

## One-screen picture

```text
  Peers (BIP324 v2)
        │
        ▼
  IBD densify getdata ──► in-RAM body queue (process-local FIFO)
        │                              │
        │                              ▼
        │                    Confirm lookup → load → scripts → write
        │                    (sole Class A appender + Class C tip)
        │                              │
        └──── Mempool / tip follow ────┘
                                       │
                 Reconstruct wire ◄────┤
                 Electrum joins   ◄────┘  Class A + SH + mempool
```

**IBD height-ordered path (current):** peer **offers raw framed wire into the
body queue** and notes readiness on the confirm feed; confirm **lookup** claims and **load** reloads
wire by height, **scripts** are pure CPU, **write** is the only Class A
appender and dequeues the body-queue entry after tip advance.

**Invariant — Class A never leads tip:** there is no dual-track “archive Class A
far ahead of confirmed tip.” Wire plan + Class A append + Class C tip advance
are one confirm-write era. Do not reintroduce plan-time “archive lead” heuristics
(e.g. body `posix_fadvise(DONTNEED)` when body count ≫ tip) — under this path
just-written body pages stay tip-hot. **No** ContigPark / archive-job fallback
for unknown-height bodies (mark missing → re-getdata).

- **Storage center** is a **transaction-relational archive** on **map-free**
  tables (pread/pwrite + fallocate; no process `mmap` of Class A/B/C), not a
  UTXO set + LevelDB chainstate. IO modality: [`io-modality.md`](./io-modality.md).
- **Consensus scripts** are verified in **pure Rust** (secp256k1 only as the
  crypto primitive via the rust-bitcoin stack — **no** `libbitcoinconsensus`
  dual-eval).
- **Electrum** is **native** to the store (optional **scripthash** tables via
  `--shindex`, default off), not a second process re-indexing blk files.
  Tip-follow does **not** wait on SH materialize; Electrum/Esplora do.
- **JSON-RPC** (optional) is a Core-class **subset** over archive + mempool —
  see [`rpc.md`](./rpc.md).

---

## How we differ

### vs Bitcoin Core

| Concern | rbitcoin | Bitcoin Core (typical) |
|---------|----------|------------------------|
| Primary store | **Map-free** Class A/B/C tables (fd pread/pwrite + heads; page cache L0) | `blocks/blk*.dat` + `undo` + LevelDB `chainstate` (UTXO) |
| Historical block serve | **Reconstruct** from `txout`+`inwit`; tip soft zone keeps a **wire ring** | Serve raw blk files / undo |
| Spentness | Annotations on create outputs (+ rare multi-list); no mutable UTXO set as truth | Coins view / UTXO mutations |
| Concurrency during IBD | Fixed **roles** (one Class A appender, separate confirm pipeline); HWM publish order — **no map epochs** | More global chainstate coupling |
| Transport | **BIP324 v2 only** | v1 + v2 |
| Script verification | Pure Rust in-tree (`rbitcoin-consensus::script`) | libbitcoinconsensus / script interpreter in C++ |
| Electrum | In-process index on confirm | External (Fulcrum, ElectrumX, …) |
| Product scope | Full node + Electrum backend; **no** wallet/mining/GUI/prune | Full Core product surface |

Product / wire intentional differences: [`COMPAT.md`](../COMPAT.md).

### vs Fulcrum / ElectrumX-style indexers

| Concern | rbitcoin | Typical external indexer |
|---------|----------|--------------------------|
| Data source | Same process as the validating node; Class A is authoritative | Reads Core RPC or blk files after the fact |
| SH index | Written on confirm (runs in Direct IBD → bulk at tip) | Separate DB built by scanning history |
| Unconfirmed | Mempool attached in-process | Depends on Core mempool RPC |
| Consensus | This binary validates blocks/scripts | Trusts the node it indexes |

---

## Novel on-disk model

Deep layout: [`SCHEMA.md`](../SCHEMA.md). Crash / tip commit:
[`docs/crash-recovery.md`](./crash-recovery.md).

### Class A / B / C (intuition)

| Class | Role | Mutation style |
|-------|------|----------------|
| **A** | Canonical archive: headers, split txs (`txout` / `inwit` / `spent` + `txid.body` / `tx.head/`) | Append bodies; publish via HWM / heads (**allocate-then-publish**) |
| **B** | Forever-open indexes (e.g. Electrum scripthash) | Append + head updates; may grow forever per key |
| **C** | Tip / confirmation: `confirmed[]`, `strong_tx`, `tx_height` | Tip advance is the **commit**; may lead/lag slightly across crash |

Spend model: **do not rewrite old output rows** as a UTXO set. Spends are
recorded as annotations (and rare multi-spender lists), with best-chain
visibility defined by confirmation / strong flags — not by deleting coins from
a LevelDB bag.

### Reconstruct + tip wire ring

- **Historical blocks** are rebuilt from Class A (zip `txout` + `inwit`) rather than
  kept forever as raw wire `blk` files.
- After IBD, a **wire-format ring** covers the soft tip window for serve,
  reorg, and recovery ([crash recovery](./crash-recovery.md)).
- **Epoch finalize** fsyncs buried archive prefixes in steady state; IBD itself
  does not promise Core-class durability mid-catch-up.

### Most-work chain selection (IBD + tip-follow)

The node follows the **fully valid** chain with **strictly most cumulative
work** (Bitcoin rule). Header work only **ranks candidates**; full block
connect decides the tip.

```text
headers / BQ / pending
    → MostWorkSelector (skip invalid-marked)
    → gather bodies (BQ · held-by-hash side bodies · Class A)
    → ChainHub::accept_branch (snapshot → disconnect → connect;
         mid-branch fail restores prior tip; mark invalid; re-rank)
```

| Path | Behavior |
|------|----------|
| **IBD** | Any depth; BadPrev at tip+1 is **corrupt wire** (soft re-get) or **competing path** (reorg). Side-branch bodies are held **by hash** (BQ is height first-wins). |
| **Tip-follow** | Pending cap ≥128; assemble max-work fork into `accept_branch`. |
| **Resume** | Prefer deeper/more-work header children; Class A body only tie-breaks. |
| **Invalid heavy** | Heavier header path that fails connect does not win; re-rank remaining candidates (may adopt a third valid chain). |

Normative detail: [`design-ibd-most-work-reorg.md`](./design-ibd-most-work-reorg.md).
(Brief history: an earlier soft-only BadPrev path could livelock on a losing
sibling tip; that is replaced by the design above.)

### Identity without fat keys

`tx.head/` is a **segmented keyless address table** of dense create foreign
keys (txid identity verified against **`txid.body`**): fixed **25-bit** open-address heads
with **4 B relative** ids, roll at 80% load / body soft span, and **binary fuse8**
built only on seal. Open segments always probe; sealed segments are fuse-gated.
See SCHEMA.

---

## Concurrent IBD / IO model

Roles and locks: [`docs/concurrency.md`](./concurrency.md). IO modality:
[`docs/io-modality.md`](./io-modality.md). Process RAM budgets:
[`docs/ibd-memory.md`](./ibd-memory.md).

### Design principles

1. **Roles, not a global store mutex.** At most one Class A appender and one
   spend annotator per process; **N readers** of published ranges are free.
2. **Allocate-then-publish.** Write body → idx → count/HWM (Release); readers
   use Acquire. Incomplete records are invisible.
3. **Confirm pipeline** splits **lookup (stamp) → load (body-queue wire + pin) → scripts
   (CPU only) → write** so disk work, script verify, and Class A/C publish
   overlap without pausing queries under a map lock. Confirm write is the
   **sole Class A appender** on the unified IBD path.
4. **Request-bounded wire memory.** Durable **body-queue soft time-depth**
   (and optional absolute byte ceiling) limit new densify `getdata` — **not**
   peer TCP accept of already-requested blocks (see ibd-memory).
5. **Bulk IO vs table transport.** `RBITCOIN_IO=uring|pread` selects **bulk
   batch** backends for `txout` pin, `txid.body` identity, `spent` annotate, spend paths
   (thread-local ring depth 128). **Table files** are always **fd** (page-/chunk-
   coalesced pread/pwrite); compact Class C is **L2 write-behind**; mempool is
   private InRam+sidecar. Head resolve **page-batches multi-key probes**.
   Historical host A/B: naive uring head insert ~5× slower than page RMW —
   production uses coalesced pages, not per-slot uring. Fuse8 builds in process
   RAM on seal. See [`docs/io-modality.md`](./io-modality.md).

### Capacity growth / durability

Store tables: fallocate only (no maps). Class C tip flush
(`flush_class_c_tip`) completes before body-queue dequeue. Mempool durability is
**InRam + private sidecars** under `{datadir}/mempool/` (not Class A).

---

## Pure-Rust consensus (secp exception)

| Piece | Implementation |
|-------|----------------|
| Headers, structure, connect, BIP68, sigops, … | `rbitcoin-consensus` |
| Script interpreter + typed paths (P2PKH/WPKH/WSH/TR/…) | `rbitcoin-consensus::script` (**no** `bitcoinconsensus` / libbitcoinconsensus) |
| ECDSA / Schnorr primitives | **secp256k1** via the **rust-bitcoin** dependency stack only |
| Types / wire at edges | rust-bitcoin |

Consensus workarounds where rust-bitcoin is not Core-faithful: living list
[`rust-bitcoin-limitations.md`](./rust-bitcoin-limitations.md).

Workspace Cargo.toml explicitly avoids enabling bitcoin’s `bitcoinconsensus`
feature. Script verification is a pure function of `(tx, input_index, prevout)`
after connect resolves prevouts.

**Milestone (assumevalid-style):** by default mainnet skips **script/sig**
checks at/below `--milestone` (840000). Prevouts, double-spend, maturity, and
fees still run. Use `--milestone 0` for full historical scripts. This is an
honest speed tradeoff, not a claim that all historical scripts were checked
under the default flag.

Test matrix for rules we own: [`docs/consensus-tests.md`](./consensus-tests.md).

---

## Pipeline summary (IBD)

1. **Peer IO** downloads headers/blocks (v2 transport).
2. **Archive prep/write** encodes and appends Class A (single writer thread).
3. **Confirm load** pins parent bodies / builds work for a height batch.
4. **Confirm scripts** verifies scripts in parallel (`rbtc-scripts` steal) with **no store I/O**.
5. **Confirm write** structural checks + Class C + spend annotations + SH runs.
6. At tip: bulk materialize durable scripthash; enable tip-follow, mempool
   relay, Electrum.

Tip follow adds compact blocks (BIP152 v2), wtxid relay (BIP339), and
libre-class mempool policy — see COMPAT and the experimental mainnet runbook.

---

## Further reading

| Doc | Contents |
|-----|----------|
| [`SCHEMA.md`](../SCHEMA.md) | Current on-disk tables and versions |
| [`docs/crash-recovery.md`](./crash-recovery.md) | Tip commit, SEAL/HWM, crash resume |
| [`docs/concurrency.md`](./concurrency.md) | Who may write which table |
| [`docs/design-ibd-most-work-reorg.md`](./design-ibd-most-work-reorg.md) | Most-work reorg design (selector, apply, invalid-heavy, resume) |
| [`docs/experimental-mainnet.md`](./experimental-mainnet.md) | Lab mainnet ops |
| [`OPERATOR.md`](../OPERATOR.md) | Knobs, logging, memory budgets |
| [`COMPAT.md`](../COMPAT.md) | Product surface vs Core / Electrum methods |
