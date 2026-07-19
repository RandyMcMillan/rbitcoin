# Implementation Plan: Mainnet Blocks-Only Node + Electrum Serve

**Codename:** `rbitcoin-node`

## Product goal

A production Bitcoin full node in Rust that:

1. **Connects to Bitcoin mainnet** (and testnet/signet/regtest) in **blocks-only** mode (`relay=false`, no mempool product).
2. **Performs IBD** from the public network and **serves IBD** to other peers (full witness blocks).
3. **Follows the tip** and participates in **block relay** (headers/inv/getdata/compact blocks).
4. **Serves Electrum protocol clients** (JSON-RPC over TCP/SSL) from the same archival store—no separate indexer process required for v1.
5. Uses a **libbitcoin-class relational mmap archive** with post-IBD durability ([`libbitcoin-durable-archive-variant.md`](./libbitcoin-durable-archive-variant.md)).

**Deferred (separate product track later):**

- Full mempool policy, package/RBF complexity, fee estimation quality matching Core
- Descriptor wallets / Core wallet RPC
- Mining GBT, GUI, pruning

**Storage constraint (product):**

- **Historical blocks for getdata / Electrum:** **reconstruct wire from the relational archive** (and full witness `TxRecord.raw`).
- **On-disk wire bytes:** **tip / non-finalized soft zone only** (wire ring)—not a full historical block file corpus.

---

## 0. Code status (2026-07-16, post parallel-IBD refactor)

Living snapshot. Older Phase-3 gap tables are historical; see phase checklists below for what shipped.

### 0.1 Inventory (approx)

| Item | Value |
|------|--------|
| Workspace | store, query, consensus, net, node, electrum, wire-cache, rpc (stub), cli, test, log, primitives |
| Hot path | Parallel multi-peer IBD: archive-before-confirm, dedicated confirm + archive writer threads |
| Net IBD layout | `rbitcoin-net/src/ibd/` modularized (peer_io, archive, dial, confirm, state, assign_plan, …) |
| Query layout | `archive` / `connect` / `reconstruct` / `chain_view` / `scripthash` submodules |
| Tests | Unit + scenarios + multi-node integration; signet lab operator path |

| Crate | Role today |
|-------|------------|
| `rbitcoin-store` | mmap Class A/B/C tables, epoch finalize |
| `rbitcoin-query` | Domain API: archive mega-batch, confirm, reconstruct, Electrum joins |
| `rbitcoin-consensus` | Structure/header/connect; milestone = **scripts only** |
| `rbitcoin-net` | P2P handshake, parallel IBD, split-stream peer serve/tip, seeds/addrman |
| `rbitcoin-node` | Long-running node: IBD → tip follow, Electrum optional, cooperative SIGINT |
| `rbitcoin-electrum` | In-process Electrum TCP server |
| `rbitcoin-wire-cache` | Tip wire ring (soft zone) |
| `rbitcoin-rpc` | Minimal stub (ops later) |

### 0.2 What works

- Parallel IBD with windowed getdata, per-peer 16, archive lead vs tip confirm
- Store-backed reconstruct for getdata / Electrum after restart
- DNS/fixed seeds, multi-peer dial/redial, stall disconnect + cooldown
- Milestone assumevalid-style: **prevouts always**, scripts skipped ≤ height
- Split-stream peer IO (IBD download peers + post-IBD `peer_session`)
- Signet lab path documented in [`OPERATOR.md`](./OPERATOR.md)

### 0.3 Remaining gaps (honest)

| ID | Gap | Severity |
|----|-----|----------|
| G7 | Mainnet consensus parity (retarget edge cases, full policy) | experimental mainnet |
| G13 | Electrum tx broadcast / mempool product | deferred |
| G16 | Full node JSON-RPC surface | stub |
| — | Production-hardening, adversarial P2P, long soak | ongoing |

Concurrency write roles: [`docs/concurrency.md`](./docs/concurrency.md).

### 0.4 Consensus completeness (mainnet)

Core structure/connect/scripts path exists; treat public mainnet as **experimental** until retarget/MTP/subsidy parity is operator-validated. Historical checklist:


Present today vs needed for public-chain accept:

| Check | Today | Needed |
|-------|-------|--------|
| Structure / merkle / weight / BIP34 | yes | keep |
| Header prev + PoW ≤ limit | yes | keep |
| Difficulty retarget (2016) + testnet rules | **no** | yes |
| Median-time-past | **no** | yes |
| Coinbase maturity (100) | **no** | yes |
| Subsidy / coinbase value | **no** | yes |
| Witness commitment (segwit) | **no** | yes |
| Locktime / CSV / CLTV full context | partial (lib flags) | parity |
| Sigops / standardness for blocks | no | consensus sigops |
| BIP9 / taproot / deployment windows | no | yes |
| Checkpoints + assumevalid/milestone | hooks empty | populate |
| Header tree / most-work (not linear tip-only) | no | yes for reorg/IBD |

### 0.5 Reconstruct-serve (locked)

- Ingest **must** keep storing full witness consensus encoding (`TxRecord.raw`) — already true.
- Historical `getdata` MSG_BLOCK / MSG_WITNESS_BLOCK: rebuild `bitcoin::Block` from header record + ordered `block_txs` → each `TxRecord.raw` decode.
- After process restart with empty RAM cache and empty tip wire ring, peers must still IBD-sync deep history from us.
- Electrum `blockchain.transaction.get` / merkle use the same raw + `block_txs` order.
- Phase 4 must **prove** round-trips (byte or network acceptance); do not assume reconstruct is free.

### 0.6 Electrum data model mapping (design)

| Electrum need | Store source |
|---------------|--------------|
| Tip header / height | `confirmed` + header table |
| Block header(s) | header table (80-byte wire encode) |
| Tx hex | `TxRecord.raw` or reconstruct |
| Merkle proof | `block_txs` order → txids → merkle branch |
| Scripthash history | **new index**: `SHA256(scriptPubKey)` → create/spend events (strong only) |
| UTXO set for scripthash | index + strong spenders |
| Broadcast | P2P `tx` push to peers (no admission policy in v1) |
| Mempool methods | return empty / zeros until mempool track |

---

## 1. Success criteria

| Criterion | Measure |
|-----------|---------|
| Mainnet blocks-only interop | Connect, IBD, serve IBD, tip follow with Core peers (§1.1) |
| Reconstruct serve | After restart, empty tip ring: peers pull deep history via reconstruct |
| Electrum | Stock Electrum (or Electrum personal server client) syncs **confirmed** wallet history against us; send via best-effort broadcast |
| Consensus | Same accept/reject as Core under matched milestone/checkpoints for mainnet |
| Durability | Durable-archive epochs + tip wire ring only for non-finalized zone |
| Coverage | High-level tests; CI uncovered-line gate |

### 1.1 Mainnet blocks-only checklist

**A — Perform IBD**

| # | Requirement | Phase |
|---|-------------|-------|
| A1 | Network magic, genesis, params per chain | 4 |
| A2 | DNS seeds + fixed seeds + basic addrman | 4 |
| A3 | Multi-outbound concurrent download window; stall/score | 4 |
| A4 | Header tree + most-work selection | 4–5 |
| A5 | Witness block download; prefer `WITNESS` peers | 4 |
| A6 | Mainnet consensus completeness (§0.4) | 4 |
| A7 | Checkpoints + default milestone | 4 |
| A8 | Resume IBD after restart from store tip | 4 |

**B — Serve IBD**

| # | Requirement | Phase |
|---|-------------|-------|
| B1 | Advertise `NETWORK \| WITNESS` when reconstruct serve works | 4 |
| B2 | getheaders from **store-backed** locator (not RAM-only) | 4 |
| B3 | getdata → reconstruct (or tip wire ring if soft zone) | 4 |
| B4 | Full witness blocks on wire | 4 |
| B5 | Inbound connection limits / basic DoS | 8 |
| B6 | Serve after cold start with empty RAM cache | 4 |

**C — Tip**

| # | Requirement | Phase |
|---|-------------|-------|
| C1 | Unsolicited headers / inv → download → accept | 5 |
| C2 | Announce tip (`inv` / `sendheaders`) | 5 |
| C3 | Compact blocks (BIP152) | 5 |
| C4 | Reorg to most-work; soft zone + optional tip wire | 5–6 |

**D — Wire on disk**

| # | Requirement | Phase |
|---|-------------|-------|
| D1 | Tip soft zone wire ring only | 6 |
| D2 | No full historical block files | always |

### 1.2 Electrum protocol checklist (product requirement)

Target: **Electrum protocol 1.4+** (ElectrumX-style scripthash API). Transport: **TCP + TLS**.

| # | Requirement | Notes | Phase |
|---|-------------|-------|-------|
| E1 | JSON-RPC over TCP (+ SSL) | Line-delimited JSON-RPC 2.0 | 7 |
| E2 | `server.version` / `server.features` / `server.ping` / `server.banner` | genesis, hosts, protocol min/max, pruning=false | 7 |
| E3 | `blockchain.headers.subscribe` + push | Needs tip follow (Phase 5) | 7 |
| E4 | `blockchain.block.header` / `blockchain.block.headers` | From header table | 7 |
| E5 | **Script hash index** | `sha256(scriptPubKey)` → history; update on connect/disconnect | **6** (build), 7 (serve) |
| E6 | `blockchain.scripthash.get_history` / `get_balance` / `listunspent` / `subscribe` | **Confirmed first** | 7 |
| E7 | `blockchain.transaction.get` | From `TxRecord.raw` | 7 |
| E8 | `blockchain.transaction.get_merkle` | `block_txs` + merkle branch | 7 |
| E9 | `blockchain.transaction.broadcast` | Best-effort P2P push; no full mempool policy | 7 (+ minimal net path) |
| E10 | `blockchain.estimatefee` / `relayfee` | Config stub or clear error | 7 |
| E11 | Mempool scripthash / fee histogram | Empty arrays / documented | 7 |
| E12 | `blockchain.transaction.id_from_pos` (optional but useful) | From `block_txs` | 7 |
| E13 | Integration: protocol fixtures + wallet smoke on regtest/signet | | 7–8 |

**v1 explicit non-goals for Electrum:** full unconfirmed UX, RBF tracking, fee estimation quality, CashAddr, alternate coins.

---

## 2. Architecture

```text
┌──────────────────────────────────────────────────────────────────────────┐
│                            rbitcoin-node                                 │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────┐  ┌────────────────┐  │
│  │ P2P blocks  │  │ IBD / tip    │  │ Consensus   │  │ Confirmability │  │
│  │ only        │──│ scheduler    │──│ validate    │──│ ordered        │  │
│  └──────┬──────┘  └──────┬───────┘  └──────┬──────┘  └───────┬────────┘  │
│         │                │                 │                  │           │
│         │         ┌──────▼─────────────────▼──────────────────▼────────┐  │
│         │         │ Query: connect/disconnect · reconstruct · indexes  │  │
│         │         └──────────────────────┬─────────────────────────────┘  │
│  ┌──────▼──────┐  ┌──────────────────────▼─────────────────────────────┐  │
│  │ getdata /   │  │ Relational mmap archive (history + scripthash idx) │  │
│  │ getheaders  │  │ + tip wire ring (non-finalized only)               │  │
│  │ store-backed│  └────────────────────────────────────────────────────┘  │
│  └─────────────┘                                                          │
│  ┌─────────────────────┐  ┌──────────────┐  ┌──────────────────────────┐  │
│  │ Electrum TCP/SSL    │  │ Node RPC     │  │ Metrics / logs           │  │
│  │ (scripthash API)    │  │ (minimal)    │  │                          │  │
│  └─────────────────────┘  └──────────────┘  └──────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────┘
```

### 2.1 Design pillars

1. Relational mmap archive = historical truth (query, validate, **reconstruct wire**, Electrum history).
2. **No full historical block files**; tip wire ring only for non-finalized soft zone.
3. Blocks-only P2P interop with Core (`relay=false`; `NETWORK|WITNESS` when reconstruct serve is proven).
4. Concurrent IBD pipeline (libbitcoin-class).
5. Electrum as a **first-class face** of the same node (shared store/indexes)—not a bolt-on second database.
6. High-level tests; prune lower-level tests when superseded.

### 2.2 Crate layout (target)

| Crate | Role |
|-------|------|
| `rbitcoin-primitives` | Shared types |
| `rbitcoin-store` | mmap tables + **scripthash multimap** (Phase 6) |
| `rbitcoin-query` | Chain ops + **reconstruct_block** + electrum query helpers |
| `rbitcoin-wire-cache` | Tip wire ring only |
| `rbitcoin-consensus` | Validation / confirmability |
| `rbitcoin-net` | P2P blocks-only + minimal broadcast |
| `rbitcoin-electrum` | **New (Phase 7):** Electrum protocol server |
| `rbitcoin-rpc` | Minimal Core-like node RPC |
| `rbitcoin-cli` | CLI |
| `rbitcoin-node` | Process wiring: P2P + Electrum + RPC |
| `rbitcoin-test` | Scenarios + multi-node + electrum fixtures |

---

## 3. Phased delivery

Each phase is mergeable; coverage gate stays green; prefer high-level tests and delete obsolete lower ones in the same PR.

### Phase 0 — Foundations ✅

Workspace, docs, lifecycle, CI/coverage culture.

### Phase 1 — Chain-capable store + query ✅

Growable tables, inputs, confirmed/strong_tx/block_txs, connect/disconnect, reorg tip scenarios.

### Phase 2 — rust-bitcoin consensus (regtest-grade) ✅

`accept_and_connect_block`, structure/header/connect checks, regtest mine scenarios; not mainnet-complete.

### Phase 3 — P2P headers/blocks + multi-node regtest ✅

Handshake, getheaders/getdata, BlockCache, 2-/3-node tests + periodic mesh script. **Not** mainnet peer discovery. Serve is **RAM-cache only**.

---

### Phase 4 — Reconstruct serve + multi-peer IBD foundation — **core done**

**Goal:** Serve history without block files after cold start; perform IBD from real networks at signet (and mainnet-experimental) quality.

**Workstreams**

| # | Workstream | Status |
|---|------------|--------|
| 1 | **Reconstruct core** — `reconstruct_block_{at_height,by_hash}` + round-trip tests | ✅ |
| 2 | **Store-backed P2P serve** — getheaders/getdata from store; restart seeder proof | ✅ |
| 3 | **Service flags** — `NETWORK\|WITNESS` | ✅ |
| 4a | **Discovery** — DNS/fixed seed lists + `AddrMan`; multi-peer try list | ✅ foundation |
| 4b | Concurrent download window, stall/score | ✅ `parallel_ibd` (window 1024, 16/peer, stall reassign) |
| 5a | **Consensus** — MTP, nBits/retarget, maturity, subsidy, witness commitment, checkpoints; reject-path coverage in `docs/consensus-tests.md` | ✅ |
| 6 | **Long-running node** — `run_p2p`, `--listen`/`--connect`/`--smoke` | ✅ |
| 7 | Public signet IBD lab run | ⬜ ops — see OPERATOR.md (readiness ladder) |

See **§3.1 Consensus gaps** for deployment-window policy (not a separate “5b” workstream).

**Exit (core)**

- Reconstruct round-trips green; multi-node serve after **process restart** without historical block files. ✅
- Signet IBD lab + concurrent multi-peer polish: optional follow-ups, not blockers for Phase 5.

---

### 3.1 Consensus gaps (documented; not a Phase 5 dependency)

Policy: **do not implement BIP9 / version-bits “deployment windows” as a separate feature** beyond what is required so each historical block is accepted or rejected under the same rules Core would apply at that height. Prefer height/time-based activation already implied by the chain and script-rule flags over a full deployment-state machine.

| Gap | Risk if missing | Mitigation / when |
|-----|-----------------|-------------------|
| Explicit BIP9 state machine (CSV, segwit, taproot start/timeout/lockin) | Wrong accept/reject near activation boundaries | Enforce via height/time rules and script flags consistent with mainnet history; expand only if differential vs bitcoind fails on real blocks |
| Testnet min-difficulty / special retarget edges | Testnet IBD stalls or rejects | Add when dogfooding testnet; regtest/signet covered by `Params` |
| Full sigops / legacy limits parity | Rare historical edge blocks | Add when differential testing shows need |
| Assumevalid / dense checkpoint set | IBD speed / skip script range | Milestone exists; populate denser checkpoints as ops need |
| Header tree index beyond best-chain linear confirmed | Complex reorgs / multi-peer header races | Phase 5 adds most-work reorg on received branches; full parallel header tree later if needed |
| Concurrent multi-peer block download window | Slower IBD | Phase 4 foundation is sequential try-list; parallel window is performance polish |

**Not gaps for product v1:** fee estimation, mempool policy, BIP9 UI/RPC reporting.

---

### Phase 5 — Tip follow + block relay — **core done**

**Goal:** Steady-state blocks-only tip follow and block announce; enable Electrum header notifications later.

| # | Work | Status |
|---|------|--------|
| 1 | Unsolicited headers / block inv → download → accept (`peer_session`, `follow_from`) | ✅ |
| 2 | Announce tip (`inv` or `headers` after peer `sendheaders`) via `TipEvent` bus | ✅ |
| 3 | Compact blocks: send/recv `sendcmpct`; `cmpctblock` → full `getdata` (no mempool short-ids) | ✅ v1 |
| 4 | Most-work reorg (`accept_branch` / competing tip by work) | ✅ foundation |
| 5 | High-level tests: `tip_follow_after_ibd`, `reorg_to_longer_branch` | ✅ |
| 6 | Hours-scale soak / multi-peer tip race polish | ⬜ periodic later |

**Exit**

- Tip extension via inv/headers announce green; reorg foundation green; C1–C3 materially done. ✅
- Electrum `headers.subscribe` unblocked (hook = `ChainHub` / `TipEvent` bus). ✅

---

### Phase 6 — Durability + scripthash index — **core done**

**Goal:** Durable-archive soft/hard zones; **script hash index** required by Electrum (and generally useful).

| # | Work | Status |
|---|------|--------|
| 1 | `archive_mode` + `archive_epoch` finalize_through + reopen | ✅ |
| 2 | Tip `WireRing` (RAM + datadir/wire files, depth eviction, drop_through) | ✅ |
| 3 | Scripthash Class B multimap + connect/disconnect updates | ✅ |
| 4 | Query: history / balance / listunspent (strong-filtered) | ✅ |
| 5 | High-level tests (history+reorg, wire+epoch) | ✅ |
| 6 | Optional full backfill tool / crash soak | ❌ SH always-on (no backfill); crash soak later |
| 7 | Auto wire push on every tip accept in net path | ⬜ polish |

**Exit**

- Durability acceptance; scripthash confirmed history correct across reorg. ✅
- SCHEMA.md updated. ✅

---

### Phase 7 — Electrum protocol server — **core done**

**Goal:** Stock Electrum clients can use this node as their server (**confirmed-chain** UX).

| # | Work | Status |
|---|------|--------|
| 1 | `rbitcoin-electrum` TCP line-delimited JSON-RPC 1.4 | ✅ |
| 2 | server.* + scripthash history/balance/listunspent/subscribe; mempool empty | ✅ |
| 3 | transaction.get / get_merkle / id_from_pos; estimatefee stub | ✅ |
| 4 | headers.subscribe (tip bus ready; push when tip_tx fed) | ✅ foundation |
| 5 | `--electrum-listen` on node | ✅ |
| 6 | Protocol fixture test | ✅ |
| 7 | TLS listener | ⬜ later |
| 8 | Live peer broadcast for transaction.broadcast | ⬜ returns txid only for now |

**Exit**

- Confirmed Electrum API works against local chain. ✅
- Wallet smoke on signet / TLS / peer broadcast: Phase 8 polish.

---

### Phase 8 — Hardening + node RPC + “network ready” (2–3 weeks)

**Goal:** Operator-ready mainnet blocks-only + Electrum.

**Work**

1. Minimal Core-like RPC: blockchain info, getblock (reconstruct), getrawtransaction, peers, stop.
2. Inbound limits, DoS, logging, metrics, config polish.
3. OPERATOR.md: IBD, serving peers, Electrum ports/TLS, reconstruct model, limitations.
4. Sign-off §1.1 + §1.2 required rows; update COMPAT.md.
5. Release label: **“mainnet blocks-only + Electrum (confirmed)”**.

**Exit**

- Runbook + checklist complete; CI green; known limitations listed.

---

## 4. Phase DAG

```text
0–3 ✅ foundations → store chain → consensus regtest → p2p regtest (RAM serve)
         │
         ▼
    Phase 4  reconstruct + store-backed serve + multi-peer IBD
             + mainnet consensus + long-running node
         │
         ├──────────────► Phase 5 tip follow / block relay
         │                      │
         ▼                      ▼
    Phase 6 durability + scripthash index  ◄── may share store PRs with late 4
         │
         ▼
    Phase 7 Electrum server (+ minimal broadcast)
         │
         ▼
    Phase 8 hardening + network ready
```

**Parallelism notes**

- Scripthash **index schema/writes** may start late Phase 4 once connect is production-shaped; **must** be complete before Phase 7.
- Electrum **wire protocol** waits for tip notifications (Phase 5) and index (Phase 6).
- Reconstruct + store-backed serve is the critical path for both P2P IBD serve and Electrum tx/merkle.

---

## 5. Key decisions

| Decision | Choice |
|----------|--------|
| Historical block serve | **Reconstruct** from relational archive |
| Full historical block files | **No** |
| Tip wire | Non-finalized soft zone only |
| Tx relay product | Deferred; Electrum broadcast = minimal peer push |
| Electrum | In-process server; confirmed history first-class |
| Fee estimation | Stub/config until mempool track |
| Script backend | pure Rust in `rbitcoin-consensus::script` (no libbitcoinconsensus); milestone for IBD speed |
| Indexing | Scripthash always-on (or config flag) like libbitcoin address index |

---

## 6. Risks

| Risk | Mitigation |
|------|------------|
| Reconstruct not bit-exact / peer reject | Round-trip corpus; fix ingest raw; network acceptance tests |
| Reconstruct CPU for heavy getdata | Tip wire ring; short RAM LRU of reconstructed blocks |
| Serve headers wrong after restart | Store-backed locator tests (not only getdata) |
| Electrum without mempool | Document; empty mempool APIs; broadcast path only |
| Scripthash index size / IBD cost | Measure; optional build flag; batch writes |
| Strong-vs-raw point history confuses Electrum | Index only strong; tests on reorg |
| Mainnet consensus bugs | Differential vs bitcoind; checkpoints |
| Phase 4 scope overload | Land reconstruct/serve proof before multi-peer polish |
| Scope creep | Electrum after chain interop; no wallet/mempool product |

---

## 7. Near-term execution

| Window | Focus |
|--------|--------|
| **Done Phase 4 core** | Reconstruct + store-backed restart serve; WITNESS; consensus depth; seeds; long-running node |
| **Done Phase 5 core** | Tip follow (`follow_from` + announce); cmpct→getdata; most-work reorg foundation |
| **Done Phase 6 core** | Wire ring (multi-tip), archive epoch, scripthash index |
| **Done Phase 7 core** | Electrum TCP server + protocol fixtures |
| **Documented gaps** | §3.1; Electrum TLS + live broadcast deferred |
| **IBD ladder** | OPERATOR.md: signet lab → fix → mainnet experimental → full |
| **Next** | Signet lab gate, then Phase 8 hardening |

---

## 8. Testing strategy

| Layer | Content |
|-------|---------|
| Unit avoidance | Prefer scenarios; delete lower tests when higher cover paths |
| Consensus | Regtest mine; later bitcoind differential; mainnet fixtures |
| P2P | Multi-node (CI); periodic mesh (`scripts/integration.sh`) |
| Reconstruct | Round-trip after reopen; multi-node serve **after seeder restart** |
| Electrum | Protocol fixtures; wallet smoke on regtest/signet |
| Coverage | HTML uncovered-line = 0 gate |

---

## 9. North star

**Ship a Rust full node that joins Bitcoin mainnet in blocks-only mode, completes and serves IBD (history via relational reconstruct, wire ring only at the tip), follows the chain, and speaks Electrum so wallets can use it directly—libbitcoin-class storage and performance, Core-compatible block consensus, no mempool/wallet product in v1.**

---

## 10. Document control

| Item | Value |
|------|-------|
| Status | Living plan — Phase 0–7 **core done** |
| Depends on | [`libbitcoin-durable-archive-variant.md`](./libbitcoin-durable-archive-variant.md), [`SCHEMA.md`](./SCHEMA.md) |
| Next action | Phase 8 — hardening, minimal RPC, operator docs |
| Gaps policy | §3.1 — deployment windows only as needed for historical block rules |
