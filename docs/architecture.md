# Architecture: how rbitcoin differs

This page is the **newcomer map** for design uniqueness. Normative layouts and
role tables live in the linked deep docs; this document explains *why* the node
is built this way and how it compares to Bitcoin Core and to external Electrum
indexers.

**Status:** experimental 0.x. On-disk format and APIs are **unstable until 1.0**.

Recent tidy pass (what was deleted vs intentional dual paths):
[`cleanup-2026-07-27.md`](./cleanup-2026-07-27.md).

---

## One-screen picture

```text
  Peers (BIP324 v2) ──► IBD / tip P2P ──► Archive Class A (bodies, heads)
                              │                    │
                              │                    ▼
                              │           Confirm (scripts CPU) ──► Class C tip
                              │                    │
                              └──── Mempool / relay ─┘
                                                   │
                              Reconstruct wire ◄───┤
                              Electrum joins  ◄────┘ Class A + SH + mempool
```

- **Storage center** is a **transaction-relational mmap archive**, not a UTXO
  set + LevelDB chainstate.
- **Consensus scripts** are verified in **pure Rust** (secp256k1 only as the
  crypto primitive via the rust-bitcoin stack — **no** `libbitcoinconsensus`
  dual-eval).
- **Electrum** is **native** to the store (scripthash tables), not a second
  process re-indexing blk files.

---

## How we differ

### vs Bitcoin Core

| Concern | rbitcoin | Bitcoin Core (typical) |
|---------|----------|------------------------|
| Primary store | Memory-mapped Class A/B/C tables (append + heads) | `blocks/blk*.dat` + `undo` + LevelDB `chainstate` (UTXO) |
| Historical block serve | **Reconstruct** from packed tx archive; tip soft zone keeps a **wire ring** | Serve raw blk files / undo |
| Spentness | Annotations on create outputs (+ rare multi-list); no mutable UTXO set as truth | Coins view / UTXO mutations |
| Concurrency during IBD | Fixed **roles** (one Class A appender, separate confirm pipeline); lock-free publish on hot path | More global chainstate coupling |
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

Deep layout: [`SCHEMA.md`](../SCHEMA.md). Durability zones:
[`libbitcoin-durable-archive-variant.md`](../libbitcoin-durable-archive-variant.md).
Crash / tip commit: [`docs/crash-recovery.md`](./crash-recovery.md).

### Class A / B / C (intuition)

| Class | Role | Mutation style |
|-------|------|----------------|
| **A** | Canonical archive: headers, packed txs (`tx.body` / `tx.idx` / `tx.head`) | Append bodies; publish via HWM / heads (**allocate-then-publish**) |
| **B** | Forever-open indexes (e.g. Electrum scripthash) | Append + head updates; may grow forever per key |
| **C** | Tip / confirmation: `confirmed[]`, `strong_tx`, `tx_height` | Tip advance is the **commit**; may lead/lag slightly across crash |

Spend model: **do not rewrite old output rows** as a UTXO set. Spends are
recorded as annotations (and rare multi-spender lists), with best-chain
visibility defined by confirmation / strong flags — not by deleting coins from
a LevelDB bag.

### Reconstruct + tip wire ring

- **Historical blocks** are rebuilt from Class A (packed full txs) rather than
  kept forever as raw wire `blk` files.
- After IBD, a **wire-format ring** covers the soft tip window for serve,
  reorg, and recovery ([durable archive variant](../libbitcoin-durable-archive-variant.md)).
- **Epoch finalize** fsyncs buried archive prefixes in steady state; IBD itself
  does not promise Core-class durability mid-catch-up.

### Identity without fat keys

`tx.head` is a **keyless address table** of dense create foreign keys (txid
identity verified against body). Online **sequential resize** (shadow file +
background fill) grows capacity without multi-writer open-hash chaos. See
SCHEMA and operator notes on resize lag.

---

## Concurrent IBD / IO model

Roles and locks: [`docs/concurrency.md`](./concurrency.md). IO history and
host-pressure levers: [`docs/ibd-io-audit.md`](./ibd-io-audit.md). Process RAM
budgets: [`docs/ibd-memory.md`](./ibd-memory.md).

### Design principles

1. **Roles, not a global store mutex.** At most one Class A appender and one
   spend annotator per process; **N readers** of published ranges are free.
2. **Allocate-then-publish.** Write body → idx → count/HWM (Release); readers
   use Acquire. Incomplete records are invisible.
3. **Confirm pipeline** splits **load → scripts (CPU only) → write** so disk
   pin/load, script verify, and Class C publish overlap without pausing
   queries under a map lock.
4. **Request-bounded archive memory.** Soft archive queue budget limits new
   `getdata` assignment — **not** peer TCP read/decode of already-requested
   blocks (see ibd-memory).
5. **Bulk IO.** Linux prefers **io_uring** for multi-read / RMW paths (confirm
   bodies, head resolve, spend annotate, `tx.head` resize fill) with a
   **thread-local** bulk ring for batch pread/pwrite; `RBITCOIN_IO_URING=0`
   (or non-Linux) falls back to pread/pwrite workers. No separate IOCP/kqueue
   backend — same API surface, modality only.

### Map growth

Capacity grow = fallocate + map a **new epoch** on the same file, pointer swap;
old maps live until pins drop. Long-held “pause the world” mmap mutexes on the
IBD/read path are considered design bugs.

---

## Pure-Rust consensus (secp exception)

| Piece | Implementation |
|-------|----------------|
| Headers, structure, connect, BIP68, sigops, … | `rbitcoin-consensus` |
| Script interpreter + typed paths (P2PKH/WPKH/WSH/TR/…) | `rbitcoin-consensus::script` (**no** `bitcoinconsensus` / libbitcoinconsensus) |
| ECDSA / Schnorr primitives | **secp256k1** via the **rust-bitcoin** dependency stack only |
| Types / wire at edges | rust-bitcoin |

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
4. **Confirm scripts** verifies scripts in parallel (rayon) with **no store I/O**.
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
| [`libbitcoin-durable-archive-variant.md`](../libbitcoin-durable-archive-variant.md) | Epochs, wire ring, IBD vs steady durability |
| [`docs/concurrency.md`](./concurrency.md) | Who may write which table |
| [`docs/experimental-mainnet.md`](./experimental-mainnet.md) | Lab mainnet ops |
| [`OPERATOR.md`](../OPERATOR.md) | Knobs, logging, memory budgets |
| [`COMPAT.md`](../COMPAT.md) | Product surface vs Core / Electrum methods |
