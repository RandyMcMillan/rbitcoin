# Implementation Plan: Consensus Node + IBD + Block Relay

**Codename (working):** `rbitcoin-node`

**Near-term goal:** A fully **consensus-compatible** Bitcoin full node in Rust that can complete **IBD** and participate in **block relay**, using a libbitcoin-class relational mmap archive and the durability model in [`libbitcoin-durable-archive-variant.md`](./libbitcoin-durable-archive-variant.md).

**Deferred (explicitly out of the active roadmap):**

- Mempool, transaction relay, package policy, fee estimation
- Descriptor (and any) wallets, wallet RPC/CLI
- Mining template / GBT (unless needed as a thin later add-on)
- Full Core RPC/ZMQ surface beyond what ops need to run and verify IBD

Those deferred areas may return in a later product track. **Crates `rbitcoin-mempool` and `rbitcoin-wallet` are removed from the workspace.**

---

## 0. Code review of current tree (as of this revision)

### 0.1 What exists and works

| Area | Assessment |
|------|------------|
| Workspace layout | Multi-crate monorepo; good separation intent (`store` / `query` / `node`) |
| `rbitcoin-store` | Real mmap table files, hash heads, headers, framed txs/outputs, `point` multimap; allocate-then-publish for headers/points |
| `rbitcoin-query` | Thin facade — right direction for keeping consensus off raw mmap |
| `rbitcoin-node` | Config, datadir, open store, smoke CLI — lifecycle only |
| Tests | High-level scenarios preferred; coverage gate (0 HTML `uncovered-line`) |
| Docs | SCHEMA draft, durable-archive spec, COVERAGE/TESTING culture |

### 0.2 Gaps vs a consensus node (priority order)

1. **No rust-bitcoin types yet** — store uses raw bytes/`[u8;32]`; consensus encoding/script/PoW not integrated.
2. **Incomplete archive schema** — missing `input` body, `ins`/`outs`/`txs` linkage arrays, `confirmed` height index, `strong_tx` / Class C tip state; fixed hash-head capacity (will fill on mainnet-scale data); no epoch finalize / wire ring implementation.
3. **No chainstate machine** — no header tree, most-work selection, candidate vs confirmed, reorg unconfirm.
4. **No validation** — consensus crate is a milestone stub only.
5. **No P2P** — net crate is a constant stub; no handshake, headers sync, block download, or block serve.
6. **No IBD orchestration** — download ∥ store ∥ validate ∥ confirmability pipeline absent.
7. **Store production risks** — small fixed hash slots; var-table capacity caps; logical-length clamp on corrupt open; no transactor/exclusive finalize; no concurrent writer model beyond mutexes; no input index yet for prevout lookup during confirmability.
8. **CLI is hand-rolled** — fine for smoke; later prefer structured config file + fewer ad-hoc flags.

### 0.3 Keep / change decisions

| Decision | Action |
|----------|--------|
| Mempool / wallet crates | **Removed** from workspace; not reintroduced until a post-relay product phase |
| `rbitcoin-rpc` | Keep as **node-only** placeholder (blockchain/control RPCs later); no wallet routes |
| `rbitcoin-cli` | Keep for node control / smoke; no `-rpcwallet` as a product feature for now |
| Store model | Keep; extend schema and grow paths rather than rewrite |
| Test philosophy | Keep high-level scenarios + coverage gate |

---

## 1. Success criteria (active roadmap only)

| Criterion | Measure |
|-----------|---------|
| Consensus compatibility | Same accept/reject as Bitcoin Core on regtest, signet, and mainnet blocks under matched milestone/checkpoint settings |
| Full archival chain | Contiguous best chain from genesis; no pruning |
| IBD | Completes mainnet IBD with milestone; height/work matches network; concurrent download/store/validate path |
| Block relay | Serve and receive blocks (inv/getdata and/or compact blocks); follow tip after IBD |
| Query/index | Always-on structural tx and spend indexes (native store); reconstruct block for serve when needed |
| Durability (steady state) | Durable-archive epochs + tip wire ring after catch-up (may land immediately after IBD green) |
| Coverage | 100% executable line coverage via high-level tests (CI gate); branch coverage on nightly when available |
| Non-goals this track | Mempool, tx relay, wallets, fee estimation, GUI, prune |

---

## 2. Architecture (consensus + IBD track)

```text
┌──────────────────────────────────────────────────────────────────────┐
│                         rbitcoin-node                                │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────┐  ┌────────────┐  │
│  │  P2P net    │  │  Sync / IBD  │  │  Validation │  │ Confirmab. │  │
│  │  (blocks)   │──│  scheduler   │──│  (parallel) │──│ (ordered)  │  │
│  └──────┬──────┘  └──────┬───────┘  └──────┬──────┘  └─────┬──────┘  │
│         │                │                 │                │         │
│         │         ┌──────▼─────────────────▼────────────────▼──────┐  │
│         │         │         Query / Chain API                       │  │
│         │         │  header tree · confirmed · strong_tx · point    │  │
│         │         └──────────────────────┬─────────────────────────┘  │
│         │                                │                            │
│  ┌──────▼──────┐  ┌──────────────────────▼─────────────────────────┐  │
│  │ Block serve │  │ Store (mmap Class A/B/C) + wire ring + epochs  │  │
│  │ (getdata)   │  │ archive_mode gate (IBD vs steady state)        │  │
│  └─────────────┘  └────────────────────────────────────────────────┘  │
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────────────────────┐  │
│  │ Node RPC*   │  │ CLI          │  │ Metrics / logs               │  │
│  └─────────────┘  └──────────────┘  └──────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────┘
* Minimal blockchain/control RPC after IBD; not a Core wallet surface.
```

### 2.1 Design pillars (unchanged where relevant)

1. Relational mmap archive (not UTXO-primary chainstate).
2. Append-only outputs + `point` spend multimap.
3. Concurrent IBD: download ∥ store ∥ validation ∥ ordered confirmability; milestone skip.
4. Durability only after contiguous tip (`archive_mode`).
5. rust-bitcoin at protocol/consensus edges.
6. High-level tests drive coverage.

---

## 3. Crate layout (active)

| Crate | Role |
|-------|------|
| `rbitcoin-primitives` | Network, height, FKs; thin newtypes; re-export/adapt rust-bitcoin as needed |
| `rbitcoin-store` | mmap tables, heads, epochs, open/recover |
| `rbitcoin-query` | Archive write/read, navigate spenders, confirmed, strong_tx |
| `rbitcoin-wire-cache` | Tip wire block ring (node layer) |
| `rbitcoin-consensus` | Header/block/tx checks, scripts, milestone, confirmability orchestration |
| `rbitcoin-net` | P2P: peers, headers, **block** download/serve (no tx relay inventory) |
| `rbitcoin-rpc` | Optional node RPC (later phase); no wallet |
| `rbitcoin-cli` | Node CLI smoke / later RPC client |
| `rbitcoin-node` | Wiring, config, IBD state machine, archive_mode |
| `rbitcoin-test` | Functional/integration scenarios |

**Removed:** `rbitcoin-mempool`, `rbitcoin-wallet`.

**Dependency rule:** `store` ← `query` ← (`consensus`, `wire-cache`) ← `node`; `net` talks to node traits; no mmap in net/consensus.

---

## 4. Feature surface for this track

### 4.1 In scope

| Area | Scope |
|------|--------|
| Consensus | Full block/tx/script rules matching pinned Core; soft forks as activated |
| Chain | Headers-first, most-work, reorg, checkpoints, milestone/assumevalid |
| P2P | Version/handshake, addrman (basic), headers, block getdata, inv for **blocks**, disconnect/stall; compact blocks preferred once basic path works |
| IBD | Concurrent multi-peer block fetch; pipeline stages; milestone-accelerated |
| Indexes | Tx by id, spenders via `point`, height→header, block tx list |
| Durability | Epoch finalize + wire ring after catch-up |
| Ops RPC (thin) | e.g. `getblockchaininfo`, `getblock`, `getblockhash`, `getpeerinfo`, `stop` — after IBD works |

### 4.2 Out of scope (deferred product track)

| Area | Notes |
|------|--------|
| Mempool / tx relay | No `inv` tx, no `tx` message handling for mempool admission |
| Fee estimation | — |
| Wallet / PSBT / descriptors | Crates removed |
| Prune / GUI | Permanent non-goals |
| Mining RPC | Deferred |

### 4.3 Network policy for “no tx relay”

- Advertise and request **blocks/headers** only as needed for IBD and tip.
- Ignore or drop unsolicited tx inventory without building a mempool (document DoS stance: rate-limit / disconnect abusers).
- Do not claim Core tx-relay parity.

---

## 5. Store / query work remaining (feeds all later phases)

### 5.1 Schema completion

- `input` table + `ins`/`outs`/`txs` linkage (SCHEMA.md).
- Dense `confirmed` (height → header fk) and `strong_tx`.
- Growable hash heads (rehash or multi-level) — **blocking for mainnet**.
- Growable var-table capacity — **blocking for mainnet**.
- Optional: store wire bytes only in wire ring, not full history.

### 5.2 Chain operations API (`query`)

- Apply header, apply block body (txs/ins/outs/points).
- Set/unset strong + confirmed for a height (reorg).
- Prevout lookup for confirmability via outputs + `point` (no spender on output rows).
- Reconstruct serialized block for P2P serve (from relational data or wire ring).

### 5.3 Durability (after IBD path exists)

Implement durable-archive §3–5: `archive_mode`, bulk/incremental finalize, wire ring, recovery.

---

## 6. Phased delivery (manageable)

Each phase is independently mergeable, has explicit exit criteria, and must keep the coverage gate green.

### Phase 0 — Foundations ✅

**Done:** workspace, docs, store MVP, query facade, node lifecycle, high-level tests, CI/coverage, removal of mempool/wallet from active scope.

---

### Phase 1 — Chain-capable store + query ✅

**Done:**

1. Schema: inputs; growable `tx`/`input`/`output` (body+idx); `confirmed`, `strong_tx`, `block_txs` (SCHEMA.md).
2. Hash heads **rehash** on full; var tables grow via separate idx files (no fixed capacity footgun).
3. Query: `connect_block` / `disconnect_tip`, tip/header-at-height, strong-filtered `spenders` + `spenders_raw`.
4. Scenarios: 100-block chain reopen; tip reorg clears strong spenders; grow past 200+ headers / 300+ txs.

**Exit met:** reopen after reorg; spenders correct for strong chain; heads/tables grow.

---

---

### Phase 2 — rust-bitcoin integration + consensus validate ✅

**Done:**

1. `bitcoin` 0.32 + `bitcoinconsensus` for script verify; store still holds raw wire via consensus encode.
2. `accept_and_connect_block`: structure (merkle, weight, BIP34, coinbase order), header link/PoW, connect (prevout unspent + scripts), then store connect.
3. Milestone skips connect checks; `ChainParams` for main/test/signet/regtest; genesis hash check.
4. High-level regtest mine/validate scenarios (no lower-level unit wallpaper); pruned redundant store happy-path tests.
5. Anyone-can-spend (`OP_TRUE` / empty) short-circuit for fixtures; otherwise libbitcoinconsensus.

**Exit met (lab):** regtest genesis + mined chain validates and reopens; bad merkle/prev rejected; double-spend rejected; milestone path works.

**Follow-ups (later):** coinbase maturity, full difficulty adjustment, expanded checkpoints, bitcoind RPC differential in CI when available.

---

### Phase 3 — Header sync + block download P2P (2–4 weeks)

**Goal:** Talk to the network (or signet) enough to fetch headers and blocks; no tx relay.

**Work**

1. Async peer manager (tokio): outbound connect, version/verack, ping/pong.
2. Headers sync (getheaders / headers); maintain header tree + most-work tip candidate.
3. Block download window: `getdata` / `block`; peer scoring by download rate; stall drop.
4. Basic addr handling optional; fixed seed peers + DNS seeds per network.
5. DoS basics: message size limits, ignore tx inv or disconnect on flood.

**Exit**

- Signet or regtest P2P: headers to tip, download a contiguous range of blocks into the store **without** full validation pipeline (or with contextual-free checks only).
- Scenarios with mock peers for handshake + one block fetch.

---

### Phase 4 — Concurrent IBD pipeline (3–5 weeks)

**Goal:** Libbitcoin-style concurrent IBD with milestone; mainnet IBD experimental.

**Work**

1. Stages: download ∥ store ∥ validation ∥ confirmability (ordered).
2. Bounded height window (e.g. tens of thousands) for locality.
3. Milestone skips validation+confirmability below height (still commitment/malleation checks as required).
4. Progress metrics and `getblockchaininfo`-style logging.
5. No wire ring / epoch fsync on IBD path.

**Exit**

- Mainnet IBD under milestone reaches network tip (or within N blocks) on lab hardware.
- Crash mid-IBD: process restarts and continues without full wipe (best-effort; Core-class durability still off).
- Benchmark numbers recorded vs Core (and libbitcoin if available).

---

### Phase 5 — Block relay + tip following (2–3 weeks)

**Goal:** Steady-state block propagation without mempool.

**Work**

1. Announce new tip blocks to peers; serve `getdata` for blocks (reconstruct or wire ring).
2. Compact blocks (BIP152) high-bandwidth mode — preferred once basic inv/getdata works.
3. Tip connect path optimized (small batch validation, not full IBD window).
4. Reorg handling within reasonable depth.

**Exit**

- Node follows signet/mainnet tip for soak period with peers; serves blocks to a second peer/node.
- No tx relay required for this exit.

---

### Phase 6 — Durable archive + wire ring (2–3 weeks)

**Goal:** Post-IBD durability per durable-archive spec.

**Work**

1. `archive_mode` gate; bulk finalize on first enable.
2. Wire ring append/prune; recovery from epoch + wire/peers.
3. Incremental finalize batching; crash tests from durable-archive §9.

**Exit**

- Durable-archive acceptance criteria automated.
- IBD path still free of per-block fsync when `archive_mode == false`.

---

### Phase 7 — Hardening + minimal node RPC (2–3 weeks)

**Goal:** Operable consensus node for lab and early operators.

**Work**

1. Minimal RPC: chain info, getblock/getblockhash, peers, stop.
2. Config file, logging, metrics.
3. Peer DoS review, connection limits, Tor/proxy optional.
4. Docs: OPERATOR.md, PERF.md, COMPAT.md (honest about no mempool/wallet).
5. Release checklist for “consensus+IBD+block-relay” milestone (not full Core parity).

**Exit**

- Documented mainnet IBD + tip-follow runbook.
- CI + coverage green; soak test notes.

---

## 7. Phase dependency DAG

```text
Phase 0 ✅
   └─► Phase 1 store/query chain ops
         └─► Phase 2 consensus + rust-bitcoin
               ├─► Phase 3 P2P headers/blocks ──┐
               │                                 ▼
               └──────────────────────► Phase 4 concurrent IBD
                                            └─► Phase 5 block relay / tip
                                                  └─► Phase 6 durability
                                                        └─► Phase 7 ops RPC + harden
```

Phases 3 and 2 can overlap once Phase 1 APIs for “write raw block bytes + headers” exist (P2P can buffer blocks before full script verify is done).

---

## 8. Testing strategy (track-specific)

| Layer | Content |
|-------|---------|
| Store/query | Connect/disconnect scenarios; reopen; spender navigation |
| Consensus | Core vectors + bitcoind differential on regtest |
| Net | Mock peers; signet integration optional in CI |
| IBD | Scripted signet/mainnet lab; fsync count must stay ~0 in IBD |
| Relay | Two-node block serve/fetch |
| Durability | Kill mid-finalize; epoch recover |
| Coverage | High-level scenarios; no unit-test wallpaper |

**Still deferred from suite:** mempool, wallet, fee, tx relay.

---

## 9. Key decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Near-term product | Consensus + IBD + block relay only | Ship a useful validating peer before wallet/mempool scope explosion |
| Mempool / wallet | Deferred; crates removed | Clear dependency and review surface |
| Chainstate | Relational mmap + confirmability | Libbitcoin-class IBD/query |
| Tx relay | Not in this track | Avoid mempool/policy complexity |
| Script backend | Prefer `bitcoinconsensus` for 1.0 consensus gate; optimize later | Bit-exact vs Core |
| Coverage | HTML zero uncovered lines in CI | Enforce “every line runs” |

---

## 10. Risks

| Risk | Mitigation |
|------|------------|
| Hash head / capacity limits | Phase 1 grow-or-fail before mainnet IBD |
| Consensus drift | Differential tests; pin Core version |
| Tip latency vs Core | Phase 5 specialized tip path + wire ring |
| Scope creep (wallet/mempool) | This document’s non-goals; reject PRs that reintroduce early |
| P2P DoS without tx policy | Rate limits; ignore tx; connection slots |

---

## 11. Near-term execution (next 90 days)

| Window | Focus |
|--------|--------|
| Days 0–21 | Phase 1 complete; start Phase 2 (rust-bitcoin + header/block checks) |
| Days 21–45 | Phase 2 exit on regtest differential; Phase 3 handshake + headers |
| Days 45–75 | Phase 3 block download; Phase 4 pipeline on signet then mainnet experimental IBD |
| Days 75–90 | Stabilize IBD; start Phase 5 tip follow |

---

## 12. Later product track (not scheduled)

When consensus+IBD+block-relay is solid:

1. Mempool + tx relay + fee estimation (`rbitcoin-mempool` reborn).
2. Descriptor wallets + RPC/CLI (`rbitcoin-wallet`).
3. Full Core RPC/ZMQ parity (minus prune/GUI/legacy wallets).
4. Optional address index / filters.

Until then, do not re-add wallet/mempool crates “for convenience.”

---

## 13. North star (this track)

**Ship a Rust validating full node that stores the chain in a concurrent libbitcoin-style mmap archive, completes IBD with Core-compatible consensus, and relays blocks — without a mempool or wallet — then add post-IBD archive durability.**

---

## 14. Document control

| Item | Value |
|------|-------|
| Status | Active roadmap (consensus / IBD / block relay) |
| Supersedes | Earlier plan sections that treated wallet/mempool as v1.0-critical |
| Depends on | `libbitcoin-durable-archive-variant.md` |
| Next action | Phase 1 store/query chain ops |
