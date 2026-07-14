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

## 0. Code audit (2026-07-14, post Phase 3)

### 0.1 Inventory

| Item | Value |
|------|--------|
| Workspace | 10 crates; mempool/wallet **removed** |
| Production-ish LOC | ~3.6k (excluding `rbitcoin-test`) |
| Tests | ~650 lines scenarios + multi-node; HTML uncovered-line = 0 gate |
| HEAD (at audit) | Phase 0–3 landed; plan rewrite uncommitted vs `501794b` |

| Crate | LOC (approx) | Role today |
|-------|--------------|------------|
| `rbitcoin-store` | ~1.4k | mmap tables, growable heads/var, Class A/B/C chain tables |
| `rbitcoin-query` | ~230 | `connect_block` / `disconnect_tip`, tip/header/tx accessors |
| `rbitcoin-consensus` | ~530 | regtest-grade accept path + `block_to_apply` (stores full witness raw) |
| `rbitcoin-net` | ~680 | regtest multi-node P2P; RAM `BlockCache`; single-peer `sync_from` |
| `rbitcoin-node` | ~240 | open store + CLI smoke; **no** long-running P2P loop |
| `rbitcoin-wire-cache` | ~30 | `WireRing` depth placeholder only |
| `rbitcoin-rpc` | ~12 | stub |
| `rbitcoin-cli` / `primitives` / `test` | remainder | CLI, types, mine helpers, scenarios |

### 0.2 What works (verified in code)

**Store / query**

- Datadir mmap files: headers, tx/in/out, point multimap, `confirmed`, `strong_tx`, `block_txs` ([`SCHEMA.md`](./SCHEMA.md)).
- Growable hash heads (rehash) and var body+idx tables.
- `Query::connect_block` / `disconnect_tip`: tip-only linear connect; Class A/B rows remain on disconnect; strong filter for UTXO-style `spenders`.
- `TxRecord.raw` is full `consensus_encode` of the transaction (**includes witness**) via `block_to_apply` — **prerequisite for reconstruct is already on the write path**.

**Consensus (regtest-grade)**

- Structure: non-empty, coinbase first, weight ≤ 4M WU, merkle root, duplicate txids, BIP34 height.
- Header: prev link on confirmed chain, empty checkpoint list hook, PoW vs `pow_limit` (not retarget).
- Connect: prevouts exist, strong-spend double-spend check, value in≥out, scripts via `bitcoinconsensus` (OP_TRUE skipped for fixtures).
- Milestone can skip connect validation for fast IBD later.

**Net (regtest)**

- Handshake: `ServiceFlags::NETWORK` only, `relay: false`, `PROTOCOL_VERSION` = **70001** (rust-bitcoin constant).
- Serve: getheaders/getdata/ping from **in-RAM** `BlockCache` only; silently drop tx inv/mempool.
- Outbound: single-peer `sync_from` (getheaders → getdata WitnessBlock → `accept_and_connect_block` + cache).
- Integration: 2-node sync, 3-node hop serve (while process still holds RAM cache); optional ignored mesh.

**Node**

- `run_node`: ensure datadir, open `Query`, construct empty `WireRing` — process exits after smoke.

### 0.3 Gaps vs product goal

Legend: **blocker** for that product face.

| ID | Gap | Perform IBD | Serve IBD | Tip | Electrum |
|----|-----|-------------|-----------|-----|----------|
| G1 | No `reconstruct_block` / store-backed getdata | — | **blocker** | — | **blocker** (tx get, merkle) |
| G2 | Serve path uses **only** RAM `BlockCache`; not reloaded from store | — | **blocker** after restart | — | — |
| G3 | getheaders locator walk not store-backed | — | **blocker** after restart | weak | headers API |
| G4 | `WITNESS` not in service flags; proto stuck at 70001 | weak | weak | weak | — |
| G5 | No DNS seeds / fixed seeds / addrman | **blocker** | — | — | — |
| G6 | No multi-peer concurrent download / most-work header tree | **blocker** | — | **blocker** | — |
| G7 | Mainnet consensus incomplete (see §0.4) | **blocker** | — | **blocker** | correct history |
| G8 | No tip inv/headers announce / compact blocks | — | — | **blocker** | header subscribe |
| G9 | No long-running node binary (P2P + optional Electrum loop) | **blocker** | **blocker** | **blocker** | **blocker** |
| G10 | No scripthash (scriptPubKey hash) index | — | — | — | **blocker** |
| G11 | No Electrum TCP/SSL JSON-RPC server | — | — | — | **blocker** |
| G12 | Tip wire ring not implemented (durable soft zone) | soft recovery | soft zone | soft | — |
| G13 | No minimal tx broadcast path for Electrum | — | — | — | send UX |
| G14 | `disconnect_tip` leaves point rows; Electrum must use **strong** semantics | — | — | reorg | history correctness |
| G15 | Checkpoints empty; no default mainnet milestone set | **blocker** quality | — | — | — |
| G16 | Node RPC almost absent | ops | ops | ops | optional |

### 0.4 Consensus completeness (mainnet blockers)

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

### Phase 4 — Reconstruct serve + multi-peer IBD foundation (4–6 weeks) — **in progress**

**Goal:** Serve history without block files after cold start; perform IBD from real networks at signet (and mainnet-experimental) quality.

**Workstreams**

| # | Workstream | Status |
|---|------------|--------|
| 1 | **Reconstruct core** — `reconstruct_block_{at_height,by_hash}` + round-trip tests | ✅ |
| 2 | **Store-backed P2P serve** — getheaders/getdata from store; restart seeder proof | ✅ |
| 3 | **Service flags** — `NETWORK\|WITNESS` | ✅ |
| 4a | **Discovery** — DNS/fixed seed lists + `AddrMan`; multi-peer try list | ✅ foundation |
| 4b | Concurrent download window, stall/score, header tree / most-work | ⬜ remaining |
| 5a | **Consensus** — MTP, nBits/retarget, maturity, subsidy, witness commitment, checkpoints | ✅ |
| 5b | BIP9/taproot deployment windows, testnet min-diff edge cases | ⬜ remaining |
| 6 | **Long-running node** — `run_p2p`, `--listen`/`--connect`/`--smoke` | ✅ |
| 7 | Public signet IBD lab run | ⬜ remaining |

**Exit (full Phase 4)**

- Reconstruct round-trips green; multi-node serve after **process restart** without historical block files. ✅
- Signet IBD to tip (or N of tip) under milestone on lab hardware. ⬜
- §1.1 A2–A8, B1–B4, B6 materially done (A3 concurrent window / A4 most-work still open).

**Tests to prefer:** reconstruct + restart-serve multi-node; multi-peer try list; short `run_p2p`.

---

### Phase 5 — Tip follow + block relay (2–3 weeks)

**Goal:** Steady-state blocks-only on signet/mainnet; enable Electrum header notifications later.

**Work**

1. Unsolicited headers / block inv → download → accept.
2. Announce tip (`inv` / `sendheaders`).
3. Compact blocks (BIP152).
4. Reorg to most-work; soft zone may use tip wire ring when Phase 6 lands.
5. Soak tests (hours-scale tip follow).

**Exit**

- Tip follow soak green; C1–C3 done; Electrum `headers.subscribe` unblocked.

---

### Phase 6 — Durability + scripthash index (2–4 weeks)

**Goal:** Durable-archive soft/hard zones; **script hash index** required by Electrum (and generally useful).

**Work**

1. `archive_mode`, epoch finalize, tip wire ring (non-finalized only); recovery rules per durable-archive doc §9.
2. Crash tests; getdata for finalized heights still **reconstruct only**.
3. **Scripthash index (Class B multimap):**
   - Key = `SHA256(scriptPubKey)` (Electrum byte order: binary hash of script, then usually reversed hex for API).
   - Values: outpoint, value, create height/txid, spend height/txid when **strong**.
4. Update index on `connect_block` / `disconnect_tip` (strong-aware; reorg-safe).
5. Optional backfill tool for existing stores.
6. Query helpers: history, balance, listunspent for one scripthash.
7. High-level tests: connect chain → query history; reorg updates index; no historical full-block file growth.

**Exit**

- Durability acceptance; scripthash confirmed history correct across reorg.
- SCHEMA.md updated for new tables.

**Note:** Index **writes** can start as soon as connect is stable; shipping the index before Electrum wire (Phase 7) is intentional so IBD builds history once.

---

### Phase 7 — Electrum protocol server (3–5 weeks)

**Goal:** Stock Electrum clients can use this node as their server (**confirmed-chain** UX).

**Work**

1. New crate `rbitcoin-electrum`: TCP + TLS listeners, line-delimited JSON-RPC, protocol negotiation.
2. Implement E2–E12 (§1.2); mempool methods empty; broadcast = peer push without mempool admission (documented).
3. Merkle proofs from `block_txs` + txids.
4. Subscriptions: scripthash + headers (hook tip events from Phase 5).
5. Wire into `rbitcoin-node` config: ports, SSL certs, banner, optional donation address.
6. Minimal net path: `broadcast_tx(raw)` → send `tx` to connected peers (may land late Phase 4/5 if useful earlier).
7. Integration fixtures: protocol-level tests; optional `electrum` CLI / small Rust client against local node.
8. Extend `scripts/integration.sh` with electrum soak when practical.

**Exit**

- Electrum wallet can sync receive history and broadcast send on regtest/signet against this server.
- COMPAT.md / OPERATOR notes: differences vs ElectrumX (no mempool history, fee stubs).

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
| Script backend | bitcoinconsensus for parity; milestone for IBD speed |
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
| **Done (Phase 4 core)** | Reconstruct + store-backed restart serve; `NETWORK\|WITNESS`; MTP/bits/maturity/subsidy/witness commitment/checkpoints; seeds/AddrMan; multi-peer try list; long-running `run_p2p` |
| **Remaining Phase 4** | Concurrent multi-peer download window + stall/score; header tree / most-work (not linear tip-only); public signet IBD lab run; richer mainnet deployments (BIP9/taproot windows) |
| **Next phase** | Phase 5 tip follow / block relay |
| Then | Phase 6 durability + scripthash index |
| Then | Phase 7 Electrum |
| Then | Phase 8 network-ready sign-off |

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
| Status | Living plan — **re-audited post Phase 3** with code-level gaps; **Electrum** is a first-class requirement (Phases 6–7) |
| Depends on | [`libbitcoin-durable-archive-variant.md`](./libbitcoin-durable-archive-variant.md), [`SCHEMA.md`](./SCHEMA.md) |
| Next action | Phase 4 — reconstruct + store-backed serve + multi-peer IBD + mainnet consensus |
| Audit note | ~3.6k prod LOC; serve is RAM-only today; `TxRecord.raw` already full witness; no scripthash/Electrum crates yet |
