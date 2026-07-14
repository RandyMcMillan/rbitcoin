# Implementation Plan: Consensus Node + IBD + Block Relay

**Codename (working):** `rbitcoin-node`

**Near-term goal:** A fully **consensus-compatible** Bitcoin full node in Rust that can:

1. **Connect to Bitcoin mainnet** and stay connected to diverse Core (and other) peers in **blocks-only** mode (`relay=false`, no mempool).
2. **Perform IBD** from the public network (headers-first, multi-peer block download, full or milestone-accelerated validation).
3. **Serve IBD** to other peers (advertise full archival node services; answer `getheaders` / `getdata` for any historical block including witness data).
4. **Follow the tip** and participate in **block relay** after sync.

Storage remains a libbitcoin-class relational mmap archive plus durability from [`libbitcoin-durable-archive-variant.md`](./libbitcoin-durable-archive-variant.md).

**Deferred (explicitly out of the active roadmap):**

- Mempool, transaction relay, package policy, fee estimation
- Descriptor (and any) wallets, wallet RPC/CLI
- Mining template / GBT (unless needed as a thin later add-on)
- Full Core RPC/ZMQ surface beyond what ops need to run and verify IBD

Those deferred areas may return in a later product track. **Crates `rbitcoin-mempool` and `rbitcoin-wallet` are removed from the workspace.**

**Blocks-only is intentional and compatible:** Bitcoin Core’s `-blocksonly` / `relay=false` is a supported interop mode. Peers still exchange headers and blocks. We must not break block-path interop by advertising incomplete services or failing to serve witness blocks.

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
| Mainnet interop (blocks-only) | See **§1.1** — hard gate before calling the node “network ready” |

### 1.1 Mainnet blocks-only interoperability checklist

This is the acceptance bar for “works with the existing Bitcoin network.” Items are **required** unless marked optional. Status is as of after Phase 3.

#### A. Connect and stay online on mainnet

| # | Requirement | Why | Status | Phase |
|---|-------------|-----|--------|-------|
| A1 | Correct mainnet magic + genesis | Else immediate disconnect | Partial (params exist; node process not wired to long-running net) | 4–5 |
| A2 | Modern `version` (≥70015/70016 class) | Headers, compact blocks, witness norms | **Gap** — currently `PROTOCOL_VERSION` 70001 | 4 |
| A3 | Advertise `NODE_NETWORK \| NODE_WITNESS` (and not claim prune) | Core uses services to select IBD peers; without `NETWORK`+`WITNESS` peers may avoid us or send non-witness data | **Gap** — currently `NETWORK` only | 4 |
| A4 | `relay=false` in version | Blocks-only / no tx relay | Done (handshake) | 3 ✅ |
| A5 | DNS seeds + fixed seeds + `addr`/`getaddr` (basic addrman) | Cannot dial the network with only manual peers | **Gap** | 4 |
| A6 | Many outbound peers; inbound listen; feeler optional | IBD bandwidth + eclipse resistance | **Gap** (single-peer `sync_from` only) | 4 |
| A7 | Ping/pong keepalive, idle timeout, stall disconnect | Survive real peers | Partial | 4 |
| A8 | Message size limits; ignore/rate-limit tx inv without banning for `relay=false` | DoS + blocks-only | Partial | 4–7 |
| A9 | Optional BIP324 (v2 transport) | Increasing mainnet share; not strictly required day one if v1 still works | Optional later | 7+ |

#### B. Perform IBD (download chain from peers)

| # | Requirement | Why | Status | Phase |
|---|-------------|-----|--------|-------|
| B1 | Headers-first sync with locator | Core primary path | Partial (single-peer loop) | 3–4 |
| B2 | Multi-peer concurrent block download | Mainnet IBD time / robustness | **Gap** | 4 |
| B3 | Header tree + **most-work** selection (not only linear parent link) | Reorgs / competing tips during sync | **Gap** | 4 |
| B4 | Request `MSG_WITNESS_BLOCK` (or equivalent) and prefer WITNESS peers | SegWit+ history is invalid without witness | Partial (we request witness inv; peer selection missing) | 3–4 |
| B5 | Full mainnet consensus: difficulty adjustment, MTP, coinbase maturity, witness commitment, deployment/activation | Else we accept invalid tips or reject valid blocks | **Gap** (simplified Phase 2 rules) | 4–5 |
| B6 | Mainnet checkpoints + default milestone/assumevalid | Safety + IBD speed | **Gap** (empty checkpoint list) | 4 |
| B7 | Persist progress; resume after restart without full re-download | Operator requirement | Partial (store persists; header/block pipeline not durable mid-window) | 4–6 |
| B8 | Validate (or milestone-skip) then connect in order | Chainstate integrity | Partial (regtest) | 2–4 |
| B9 | Signet/mainnet lab IBD exit | Proof | Not yet | 4–5 |

#### C. Serve IBD (upload chain to other nodes)

| # | Requirement | Why | Status | Phase |
|---|-------------|-----|--------|-------|
| C1 | `NODE_NETWORK` (+ `WITNESS`) while archival | Core only IBD-syncs from peers advertising full service | **Gap** (flags) | 4 |
| C2 | Answer `getheaders` from genesis→tip in ≤2000 batches | Standard IBD | Partial (in-memory cache only) | 3–4 |
| C3 | Answer `getdata` for **any** historical block with **full witness** wire bytes | Serving IBD to other nodes | **Gap** — must **reconstruct** from relational archive (not full block files) | **4 (must)** |
| C4 | Bit-exact reconstruction proven (mainnet/signet samples + property tests) | Peers reject non-canonical wire | **Gap** — need reconstruct path + round-trip tests | **4 (must)** |
| C5 | Wire cache only for **recent non-finalized** tip window | Crash/corruption recovery of soft zone; **not** historical archive | Partial design (durable-archive wire ring); not productized | 3–6 |
| C6 | Handle `getblocks` (optional but useful) | Older clients | Optional | 5 |
| C7 | Not pruned; never serve empty for deep history | Archival product | Intentional non-prune | ✅ product |
| C8 | Inbound connection limits / resource caps | DoS when serving IBD | **Gap** | 7 |

#### D. Steady-state tip (after IBD)

| # | Requirement | Why | Status | Phase |
|---|-------------|-----|--------|-------|
| D1 | `headers` / block `inv` → fetch → validate → connect | Tip follow | **Gap** | 5 |
| D2 | Announce new tip to peers (`inv` or `headers` via sendheaders) | Help the network | **Gap** | 5 |
| D3 | Compact blocks (BIP152) | Mainnet efficiency | Preferred | 5 |
| D4 | Reorg to most-work within policy | Safety | Partial store disconnect only | 5 |

#### E. Explicit non-requirements for this track (still interop-safe)

| Item | Notes |
|------|--------|
| Tx relay / mempool | `relay=false`; ignore tx messages — Core interops fine |
| Addr gossip quality | Minimal addrman is enough to find peers; need not match Core |
| BIP324 day-one | Nice-to-have |
| Wallet / fee / GBT | Out of scope |

### 1.2 Verdict before Phase 4

| Question | Answer |
|----------|--------|
| Does the **plan direction** end in mainnet blocks-only interop with perform+serve IBD? | **Yes, if Phase 4–7 absorb the gaps in §1.1** (especially **C3/C4 bit-exact reconstruct-from-archive**, **B5 consensus completeness**, **A3 service flags**, **A5–A6 peer discovery/multi-peer**). |
| Are we there now (post Phase 3)? | **No.** Proven only on **regtest multi-node** with in-memory block cache. |
| How do we **serve** historical blocks? | **Reconstruct wire format from the relational store** (headers, txs, inputs, outputs, witnesses, order). **No full historical block file store.** |
| What wire bytes *do* we keep on disk? | **Only the recent non-finalized window** (durable-archive tip wire ring / soft zone) for crash recovery and tip reorg — not for IBD history. |
| Single biggest missing piece for *serving* IBD? | **Bit-exact `reconstruct_block(hash\|height) → wire bytes`**, with enough archive fields (including witness) and round-trip tests against real blocks. |
| Single biggest missing piece for *performing* mainnet IBD? | **Mainnet consensus (difficulty/MTP/activation) + multi-peer download + peer discovery.** |

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
│  │ Block serve │  │ Relational mmap archive = historical truth     │  │
│  │ getdata via │  │ reconstruct wire for IBD serve                 │  │
│  │ reconstruct │  │ + tip wire ring only (non-finalized soft zone) │  │
│  └─────────────┘  └────────────────────────────────────────────────┘  │
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────────────────────┐  │
│  │ Node RPC*   │  │ CLI          │  │ Metrics / logs               │  │
│  └─────────────┘  └──────────────┘  └──────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────┘
* Minimal blockchain/control RPC after IBD; not a Core wallet surface.
```

### 2.1 Design pillars (unchanged where relevant)

1. Relational mmap archive (not UTXO-primary chainstate) — **source of truth for history**.
2. **Serve historical blocks by reconstruction**, not by storing full block files.
3. Wire on disk only for **recent non-finalized** blocks (tip ring / soft zone).
4. Append-only outputs + `point` spend multimap.
5. Concurrent IBD: download ∥ store ∥ validation ∥ ordered confirmability; milestone skip.
6. Durability only after contiguous tip (`archive_mode`).
7. rust-bitcoin at protocol/consensus edges.
8. High-level tests drive coverage.

---

## 3. Crate layout (active)

| Crate | Role |
|-------|------|
| `rbitcoin-primitives` | Network, height, FKs; thin newtypes; re-export/adapt rust-bitcoin as needed |
| `rbitcoin-store` | mmap tables, heads, epochs, open/recover |
| `rbitcoin-query` | Archive write/read, navigate spenders, confirmed, strong_tx, **reconstruct_block** |
| `rbitcoin-wire-cache` | **Tip-only** wire ring for non-finalized soft zone (not historical archive) |
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

### 4.3 Network policy for “blocks-only” (mainnet-compatible)

- Set `relay=false` in `version` (done in Phase 3).
- Advertise **`NODE_NETWORK | NODE_WITNESS`** once we can serve full historical witness blocks (required for Core to treat us as a full archival peer).
- Prefer outbound peers with `NODE_NETWORK | NODE_WITNESS` when performing IBD.
- Request blocks as witness inventory (`Inventory::WitnessBlock`).
- Ignore tx `inv` / `tx` / `mempool` without disconnecting solely for offering txs (rate-limit floods).
- Do not claim Core tx-relay parity; do claim **block-path interop** for headers + blocks.

### 4.4 Storage for interop (decision — product constraint)

| Layer | Purpose | Retention |
|-------|---------|-----------|
| **Relational mmap archive** | Historical truth: query, validate, **reconstruct wire for serve-IBD** | Full chain (archival) |
| **Tip wire ring** | Soft zone only: crash recovery, tip reorg, corruption of recent non-finalized blocks | Last *N* blocks / byte budget behind tip; **not** full history |

**Hard rules:**

1. **Do not** store full historical `blocks*.dat`-style files for the entire chain.
2. **`getdata` for height ≤ finalized / outside wire window** → `reconstruct_block` from relational tables → consensus-encoded witness block.
3. **`getdata` for soft tip window** → prefer wire ring if present; else reconstruct.
4. Reconstruction must be **bit-exact** (or network-accepted equivalent where the protocol allows) for mainnet blocks — enforced by tests, not assumed.

### 4.5 Reconstruct-block requirements (schema + API)

To serve IBD without full block files, the archive must retain everything needed to rebuild a `bitcoin::Block`:

| Data | Notes |
|------|--------|
| Header fields | version, prev, merkle, time, bits, nonce (already) |
| Tx order in block | `block_txs` / height→tx fk list (Phase 1) |
| Full tx wire content | Prefer storing **consensus-serialized tx (with witness)** at ingest (`TxRecord.raw` already intended for this) and reassemble block as header + tx list |
| Coinbase & witness commitment | Present in stored txs / structure checks at connect |
| SegWit marker/flag | Preserved if `raw` is full witness serialization |

**Preferred v1 reconstruct algorithm (simple and exact):**

1. Load header → `bitcoin::block::Header`.
2. Load ordered tx fks for that block.
3. For each tx, decode `TxRecord.raw` via rust-bitcoin consensus decode (witness included).
4. Assemble `Block { header, txdata }`, consensus-encode for the wire.

**Do not** invent a second parallel full-block archive. If `raw` is incomplete for any historical path, **fix the relational write path** so `raw` is always full witness serialization at connect — still not a separate blocks directory.

**Proof obligations (Phase 4 exit):**

- Round-trip: accept real/signet/mainnet sample blocks → store → restart process → reconstruct → byte-identical (or equal under a documented, peer-accepted canonical form).
- Multi-node: peer B IBD-syncs from peer A **after A restarts** with **empty wire ring** for heights only available via reconstruct.
- Fuzz/property: random regtest chains reconstruct after reopen.

---

## 5. Store / query work remaining (feeds all later phases)

### 5.1 Schema completion

- Inputs / confirmed / strong_tx / growable tables — largely Phase 1 ✅.
- Ensure **every connected tx keeps full witness `raw`** (gate in connect path).
- Header + height indexes for locator/getheaders without relying on RAM `BlockCache` alone.

### 5.2 Chain operations API (`query`)

- Apply header/block body; set/unset strong + confirmed.
- Prevout lookup for confirmability.
- **`reconstruct_block(hash|height) -> bitcoin::Block`** and encode for P2P serve.
- Optional: compare reconstruct vs tip wire ring when both exist (debug/consistency).

### 5.3 Durability (after IBD path exists)

Implement durable-archive §3–5: `archive_mode`, finalize epochs, **tip wire ring only** for non-finalized heights, recovery replay from ring then peers. Historical serve always uses reconstruct, not retained full-block files.

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

### Phase 3 — Header sync + block download P2P ✅

**Done:**

1. Tokio P2P: version/verack handshake, ping/pong, listen + dial (`rbitcoin-net`).
2. Headers sync (`getheaders` / `headers`) + ordered block download (`getdata` / `block` with witness inventory).
3. `BlockCache` for wire serve + locator; integrate with `accept_and_connect_block` on ingest/sync.
4. No tx relay: ignore `tx` / mempool / tx inv; `relay=false` in version.
5. Multi-node integration tests (always-on 2- and 3-node meshes) + periodic ignored mesh (`scripts/integration.sh`).

**Exit met (regtest multi-node):** peer syncs headers+blocks from seeder; second hop serve-after-sync works.

**Follow-ups (Phase 4+):** peer scoring/stall eviction, DNS seeds, concurrent IBD pipeline, compact blocks.

---

### Phase 4 — Mainnet-capable IBD (perform + reconstruct-serve) (4–6 weeks)

**Goal:** Close §1.1 gaps so the node can **perform IBD from public peers** and **serve historical blocks via reconstruct-from-archive** after restart—without a full historical block file store.

**Work (ordered)**

1. **`reconstruct_block` / serve path:** assemble wire block from header + ordered txs’ full witness `raw`; use in `getdata` handler (**C3/C4**).
2. **Ingest guarantees:** connect path always stores complete witness serialization per tx; tests refuse incomplete `raw`.
3. **Round-trip proof:** regtest + signet/mainnet sample blocks → store → restart → reconstruct → compare to original wire; multi-node serve IBD with **empty tip wire ring** for deep heights.
4. **Service flags** `NETWORK | WITNESS` only after reconstruct serve works; bump protocol version; prefer WITNESS peers (**A2/A3/B4**).
5. **Peer discovery:** DNS seeds + fixed seeds + basic addrman; multi-outbound (**A5/A6**).
6. **Header tree + most-work**; multi-peer concurrent download; stall/score/evict (**B2/B3**).
7. **Consensus completeness for mainnet:** difficulty retarget, MTP, coinbase maturity, witness commitment, deployment flags (**B5**); checkpoints + default milestone (**B6**).
8. Pipeline: download ∥ relational store ∥ validate ∥ confirm; no per-block epoch fsync in IBD.
9. Tip wire ring remains **optional soft-zone** for crash recovery only (not a substitute for reconstruct).

**Exit**

- Signet (then mainnet experimental) IBD under milestone reaches network tip (or within N blocks).
- After **restart**, second process (or Core) can IBD from us for a range **served only by reconstruct** (wire ring disabled or pruned for that range).
- Reconstruct round-trip tests green on fixture corpus.

---

### Phase 5 — Block relay + tip following (2–3 weeks)

**Goal:** Steady-state blocks-only participation on mainnet/signet.

**Work**

1. Unsolicited `headers` / block `inv` → fetch → validate → connect (**D1**).
2. Announce new tip (`inv` / `sendheaders`) (**D2**).
3. Compact blocks (BIP152) high-bandwidth (**D3**).
4. Reorg to most-work within policy (**D4**); soft zone may use tip wire ring; history via reconstruct.
5. Soak: follow tip ≥ days with diverse peers.

**Exit**

- Mainnet/signet tip follow soak; we announce and receive new blocks; still `relay=false`.

---

### Phase 6 — Durable archive + tip wire ring only (2–3 weeks)

**Goal:** Post-IBD durability per durable-archive spec: epochs for sealed relational archive; **wire ring only for non-finalized tip**.

**Work**

1. `archive_mode` gate; bulk finalize on first enable (relational Class A/B HWMs — **not** full historical wire files).
2. Tip wire ring append/prune for soft zone; recovery: epoch + ring + peers.
3. Incremental finalize; crash tests from durable-archive §9.
4. Confirm: getdata for **finalized** heights uses **reconstruct only** (no dependency on wire files).

**Exit**

- Durable-archive §9 automated; IBD path free of mandatory per-block fsync when `archive_mode == false`; no growth of historical block-file corpus.

---

### Phase 7 — Hardening + minimal node RPC + “network ready” gate (2–3 weeks)

**Goal:** Operable mainnet blocks-only node for lab and early operators.

**Work**

1. Minimal RPC: chain info, getblock/getblockhash (via reconstruct), peers, stop.
2. Config file, logging, metrics; inbound limits (**C8**).
3. Peer DoS review; Tor/proxy optional; optional BIP324 note.
4. Docs: OPERATOR.md (IBD in/out; reconstruct-serve model), PERF.md, COMPAT.md.
5. **§1.1 checklist sign-off** — every required row Done or explicitly waived with reason.
6. Release checklist: “mainnet blocks-only: perform IBD + serve IBD via reconstruct + tip follow”.

**Exit**

- Documented mainnet runbook; §1.1 required items complete; CI + coverage green.

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

**Ship a Rust validating full node that speaks Bitcoin’s block protocol on mainnet in blocks-only mode: discovers peers, performs IBD, serves historical witness blocks by reconstructing them from the relational archive (no full historical block files), keeps wire bytes only for the non-finalized tip window, follows the tip — without a mempool or wallet — then hardens post-IBD durability.**

---

## 14. Document control

| Item | Value |
|------|-------|
| Status | Active roadmap (mainnet blocks-only interop) |
| Supersedes | Wallet/mempool-as-v1.0 plans; post–Phase 3 gap analysis; **full historical block-blob store rejected** in favor of reconstruct-from-archive |
| Depends on | `libbitcoin-durable-archive-variant.md` (tip wire ring = soft zone only) |
| Next action | Phase 4 — reconstruct serve + multi-peer IBD + mainnet consensus completeness |
