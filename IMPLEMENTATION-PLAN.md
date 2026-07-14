# Implementation Plan: Production Rust Bitcoin Node (libbitcoin-class store)

**Codename (working):** `rbitcoin-node` (crate/workspace name TBD)

**Purpose:** Build a fully production-quality Bitcoin full node in Rust that:

1. Uses the best of the **rust-bitcoin** ecosystem for types, consensus encoding, scripts, and P2P message primitives.
2. Implements a **libbitcoin-like relational mmap storage engine** with the durability extensions in [`libbitcoin-durable-archive-variant.md`](./libbitcoin-durable-archive-variant.md).
3. Reaches **feature parity with Bitcoin Core** except **pruning** and **GUI**, including **full descriptor-wallet** parity for Core’s RPC/CLI managed wallets.
4. Targets **libbitcoin-class IBD throughput** and **structural query performance** (tx, spenders, height, optional address/filters), not Core’s UTXO-centric chainstate.

**Non-goals (explicit):**

- Pruning (out of product scope; store remains archival).
- GUI / Qt / RPC-browser UI.
- SQL / LevelDB / RocksDB as primary chain archive.
- UTXO-set-as-primary-chainstate redesign.
- Soft-fork rule invention; consensus must track Bitcoin Core mainnet rules.
- **Legacy (pre-descriptor) Core wallets** — Berkeley DB / old `wallet.dat` keypool wallets that Core still can open but no longer recommends. No import, migration, or create path for those formats (see §4.7).

**Primary references:**

- This repo: `libbitcoin-durable-archive-variant.md` (durability, wire ring, epochs, Class A/B/C tables).
- Libbitcoin conceptual model: Delving Bitcoin *“Libbitcoin for Core people”* (download / validation / confirmability concurrency; milestone ≈ assumevalid; spend via `point` multimap, not mutated outputs).
- Bitcoin Core: consensus rules, P2P policy surface, JSON-RPC + CLI surface (minus prune/GUI; **descriptor wallets only**), mempool/policy, network DoS posture.
- rust-bitcoin org crates: `bitcoin`, `bitcoin_hashes`, `secp256k1`, `miniscript`, `bitcoinconsensus` / libbitcoinconsensus bindings as needed, encoding primitives.
- Wallet stack candidates: rust-bitcoin `miniscript` + descriptor support; BDK (`bdk_wallet` / `bdk_chain`) or equivalent where it accelerates descriptor/coin-selection work without blocking Core RPC shape.

---

## 1. Success criteria (definition of done)

### 1.1 Product

| Criterion | Measure |
|-----------|---------|
| Archival full node | Contiguous mainnet (and testnet/signet/regtest) chain from genesis; no pruning mode |
| Consensus parity | Same accept/reject as Core on mainnet tip for all historical + live blocks under matched settings (milestone/assumevalid, checkpoints) |
| Core feature parity (excl. prune/GUI) | P2P, mempool/policy, mining/template RPCs, ZMQ, indexes that Core has when non-pruned + `txindex`, **descriptor-wallet RPC/CLI**, config knobs that matter for operators |
| Descriptor wallet parity | Create/load/unload multi-wallet; full descriptor import/export; receive/send/PSBT; coin control; hardware/external signer hooks as Core exposes over RPC; encryption; rescan; balances/history — behavioral parity with Core descriptor wallets on regtest and mainnet workflows |
| IBD performance | Wall-clock IBD within noise of contemporary libbitcoin on comparable hardware/bandwidth with milestone enabled; no intentional fsync/wire tax during IBD |
| Query performance | Constant-time structural navigation (tx by hash, spenders of outpoint, confirmed height, block tx lists) competitive with libbitcoin; not “scan blocks.dat” |
| Durability (steady state) | Per durable-archive spec: sealed archive ≤ `finalized_height` survives crash without full resync; soft tip recovers from wire ring and/or peers |
| Test coverage | **100% line and 100% branch coverage** of first-party Rust code, measured in CI; achieved primarily via high-level functional/integration tests (see §10) |
| Production quality | Crash recovery, observability, config, packaging, docs, security review, long-run soak tests, reproducible builds |

### 1.2 Explicit out of scope forever (unless product revises)

- Pruned node modes and prune RPCs.
- Built-in GUI.
- Claiming sealed `point` heads mean “no future spends” (see durable-archive §5.3).
- Legacy Core wallet formats (non-descriptor BDB/`wallet.dat` keypool wallets): open, migrate-from, or create.

---

## 2. Architecture overview

```text
┌──────────────────────────────────────────────────────────────────────────┐
│                         rbitcoin-node (process)                          │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────┐  ┌────────────────┐  │
│  │  P2P net    │  │  Sync / IBD  │  │  Validation │  │  Confirmability│  │
│  │  (tokio)    │──│  scheduler   │──│  (parallel) │──│  (ordered)     │  │
│  └──────┬──────┘  └──────┬───────┘  └──────┬──────┘  └────────┬───────┘  │
│         │                │                 │                   │          │
│         │         ┌──────▼─────────────────▼───────────────────▼──────┐   │
│         │         │              Query / Chain API                     │   │
│         │         │   (header, tx, in/out, point, strong, confirmed)   │   │
│         │         └───────┬────────────────────────────▲──────────────┘   │
│         │                 │                            │                   │
│  ┌──────▼──────┐  ┌───────▼──────────────────┐  ┌──────┴───────────────┐  │
│  │ Mempool     │  │ Store (mmap Class A/B/C) │  │ Descriptor wallets   │  │
│  │ + policy    │  │ + archive_mode / epochs  │  │ (multi-wallet DB)    │  │
│  └──────┬──────┘  │ + wire ring (node layer) │  │ notify ← blocks/tx   │  │
│         │         └──────────────────────────┘  └──────────▲───────────┘  │
│  ┌──────▼──────┐  ┌──────────────┐  ┌──────────────────────┴──────────┐  │
│  │ JSON-RPC    │  │ CLI client   │  │ ZMQ / notify │ Observability    │  │
│  │ (node+wlt)  │  │ (rpcwallet)  │  └──────────────┴──────────────────┘  │
│  └─────────────┘  └──────────────┘                                        │
└──────────────────────────────────────────────────────────────────────────┘
```

### 2.1 Core design pillars

1. **Transaction-relational archive (not block-blob + UTXO DB).**  
   Headers, txs, inputs, outputs, linkage tables, spend multimap (`point`), confirmation indexes. Reconstruct wire blocks when needed; keep a **hot wire ring** only for the tip window (durable-archive §4).

2. **Spend model: append-only outputs + `point` multimap.**  
   Never mutate old output rows to record spenders. New spends append Class B entries and CAS/publish hash heads.

3. **Concurrent IBD pipeline** (libbitcoin-style stages, concurrent across height ranges):
   - **Download** — wide peer fan-out, redundant headers, spread block `getdata`.
   - **Store** — allocate-then-publish; lock-free heads where safe.
   - **Validation** — all checks *except* prevout existence/unspent (scripts, amounts, locktimes, size, …) with partial ordering.
   - **Confirmability** — ordered (or window-ordered) check that prevouts exist and are unspent; publish `strong_tx` / confirmed linkage.
   - **Milestone** — skip validation + confirmability below configured milestone (threat model aligned with Core `-assumevalid`, plus no ordered UTXO construction requirement).

4. **Durability split by mode** (durable-archive §3):
   - `archive_mode == false` (IBD/catch-up): no wire ring, no epoch fsync advancement; rebuildable store; max performance.
   - `archive_mode == true` (steady state): wire ring + incremental finalize epochs for Core-class buried durability.

5. **rust-bitcoin at the edges of consensus and wire.**  
   Use rust-bitcoin types and encoding for blocks, txs, scripts, network messages. Do **not** reimplement SHA256d, secp, or script interpreters from scratch unless a measured hotspot requires a carefully reviewed specialized path (still consensus-tested against Core vectors).

6. **Descriptor wallets as a peer of the node, not the chainstore.**  
   Full Core RPC/CLI descriptor-wallet parity; per-wallet UTXO/tx caches fed by chain+mempool notifications; wallet DB isolated from mmap archive; **no** legacy BDB wallet support.

7. **Coverage by high-level scenarios, not unit-test wallpaper.**  
   100% line and branch coverage is a merge gate; tests drive the stack from RPC/P2P/node/store public surfaces whenever possible (see §10).

---

## 3. Crate / workspace layout

Monorepo Cargo workspace. Names are provisional; keep crates small and dependency-direction acyclic.

| Crate | Responsibility |
|-------|----------------|
| `rbitcoin-primitives` | Thin re-exports / newtypes over `bitcoin` where store needs stable layout (height, fk ids, outpoint keys). Avoid forking consensus types. |
| `rbitcoin-store` | mmap tables, hash heads, array/blob bodies, transactor, snapshot, **epoch finalize**, open/recover. Pure storage; no P2P. |
| `rbitcoin-query` | Read/write query layer over store: archive put, navigate spenders, confirmed, strong_tx, height indexes. |
| `rbitcoin-wire-cache` | Node-layer wire block ring (flat files preferred per durable-archive §4.2). |
| `rbitcoin-consensus` | Block/tx validation and confirmability orchestration; milestone; checkpoints; script verify via best available path (`bitcoinconsensus` / rust-bitcoin script + secp). |
| `rbitcoin-net` | P2P: peer manager, handshake, inventory, headers/blocks sync protocol, DoS limits, addr man. |
| `rbitcoin-mempool` | Policy mempool (RBF, package limits, min relay, expiry); disk-backed optional later. |
| `rbitcoin-wallet` | Descriptor wallets only: multi-wallet manager, descriptor/miniscript, key material, SQLite (or equivalent) wallet DB, coin selection, tx construction, PSBT, rescan against chain/store, encryption. |
| `rbitcoin-rpc` | Bitcoin Core–compatible JSON-RPC (node + wallet methods); multi-wallet endpoint routing. |
| `rbitcoin-cli` | Bitcoin Core–compatible CLI for node and wallet RPCs (`bitcoin-cli` analogue). |
| `rbitcoin-node` | Binary: wire stages together, config, `archive_mode` state machine, wallet lifecycle, signals, shutdown. |
| `rbitcoin-test` / `rbitcoin-bench` | High-level functional/integration harness (in-process node + multi-process regtest), Core differential tests, coverage-driving scenario suites, IBD benches, crash/recovery. Prefer tests here over per-crate unit tests. |

**Dependency rule:** `store` ← `query` ← (`consensus`, `wire-cache`) ← `node`; `wallet` depends on chain notifications + query (and optionally mempool) via traits, **not** on raw mmap; `rpc` / `cli` sit above node + wallet.

---

## 4. Feature parity map (Bitcoin Core, minus prune/GUI)

Track against a pinned Core version (e.g. 28.x / 29.x) and bump deliberately.

### 4.1 Consensus & chain

| Feature | Target | Notes |
|---------|--------|-------|
| Full script + tx + block consensus | Required | Match Core on all soft forks active on network |
| Headers-first / most-work chain | Required | Candidate vs confirmed terminology per libbitcoin |
| Checkpoints | Required | Same security role as Core |
| Assumevalid / milestone | Required | Default milestone updated with releases |
| Reorg handling | Required | Unconfirm via Class C; deep reorg policy vs `wire_depth` documented |
| AssumeUTXO | Optional later | Not required for v1 parity narrative; store model differs |
| Chainparams main/test/signet/regtest | Required | |

### 4.2 Network (P2P)

| Feature | Target |
|---------|--------|
| Outbound + inbound peers, feeler, extra block-relay-only | Required |
| High outbound count during IBD; scale down after tip | Required (libbitcoin-class) |
| Headers sync, compact blocks, high-bandwidth mode | Required |
| Tx relay, inv/getdata, package-aware policy where Core has it | Required |
| Addr management, addrman persistence | Required |
| Ban/discouragement, stall detection, rate limits | Required |
| Tor / proxy / bind / whitelist | Required for production |
| BIP324 v2 transport | Required for modern parity (phased OK) |

### 4.3 Mempool & policy

| Feature | Target |
|---------|--------|
| Size-limited mempool, min relay fee, incremental relay | Required |
| RBF (BIP125 / full-RBF knobs as Core) | Required |
| Package relay / TRUC / policy limits as Core evolves | Track Core; phased |
| Fee estimation (`estimatesmartfee`) | Required |
| Persist mempool on shutdown | Required |

### 4.4 Indexes & query (archival advantages)

| Feature | Target |
|---------|--------|
| Always-on tx index (structural, not optional bolt-on) | Required — native to store |
| Spender index (`point`) | Required — native |
| Optional address index | Phase 2 (libbitcoin optional path) |
| Compact block filters (BIP157/158) serve | Required for Core parity |
| Coinstats / gettxoutsetinfo | Required (may be slower than Core’s UTXO walk; document or cache) |

### 4.5 RPC / CLI surface

Implement in waves matching operator priority. **v1.0 includes full descriptor-wallet RPC and CLI parity** with the pinned Core version (minus legacy-wallet-only methods).

1. **Control / network / blockchain read:** `getblockchaininfo`, `getnetworkinfo`, `getpeerinfo`, `getblock`, `getblockheader`, `getrawtransaction`, `gettxout`, `getbestblockhash`, `getblockhash`, `getblockcount`, …
2. **Raw tx / mempool:** `sendrawtransaction`, `testmempoolaccept`, `getrawmempool`, `getmempoolentry`, `estimaterawfee` / `estimatesmartfee`
3. **Mining:** `getblocktemplate`, `submitblock`, `getmininginfo`
4. **ZMQ notifications**
5. **Wallet lifecycle & multi-wallet:** `createwallet`, `loadwallet`, `unloadwallet`, `listwallets`, `listwalletdir`, `getwalletinfo`, wallet-scoped HTTP endpoints / `-rpcwallet` CLI routing — **descriptor wallets only** (`descriptors=true` is the only supported create mode; reject or no-op legacy create flags as documented)
6. **Descriptor management:** `importdescriptors`, `listdescriptors`, `getdescriptorinfo`, `deriveaddresses`, `getnewaddress`, `getrawchangeaddress`, `getaddressinfo`, label APIs as Core exposes for descriptors
7. **Balances, history, coins:** `getbalances`, `getbalance`, `getreceivedbyaddress` / `by label`, `listunspent`, `listtransactions`, `listsinceblock`, `gettransaction`, lockunspent / coin control
8. **Spending & PSBT:** `send`, `sendtoaddress`, `sendmany`, `walletcreatefundedpsbt`, `walletprocesspsbt`, `sendall`, fee rate controls, RBF bump (`bumpfee` / `psbtbumpfee` as Core)
9. **Signing & keys:** wallet encryption (`encryptwallet`, `walletpassphrase`, …), `signmessage` / `verifymessage`, `signrawtransactionwithwallet`, external signer RPCs if present in pinned Core
10. **Rescan / restore:** `rescanblockchain`, backup/restore of **descriptor** wallet files, `restorewallet` for descriptor wallets only
11. **CLI:** `rbitcoin-cli` (name TBD) with Core-compatible flags for node + wallet RPC, including multi-wallet selection

**Recommendation:** v1.0 = **full node + descriptor wallets + RPC + CLI**, matching Core with `deprecatedrpc` / legacy wallet paths **omitted**. State clearly in release notes and `COMPAT.md` that legacy BDB wallets are unsupported (users must use Core to migrate legacy → descriptor, then import descriptors here if needed).

### 4.6 RPC / interfaces intentionally absent

- Prune-related RPCs and `-prune`.
- GUI and anything Qt.
- Anything that requires mutating sealed Class A bodies.
- Legacy wallet RPCs and behaviors that exist only for pre-descriptor wallets, including but not limited to:
  - Creating or loading non-descriptor wallets
  - BDB `wallet.dat` format compatibility
  - Legacy dump/import key dumps as a primary workflow (`dumpprivkey` / `importprivkey` / `importwallet` / `dumpwallet` — **omit** unless a narrow descriptor-adjacent subset is required for Core test compatibility; prefer descriptor + PSBT flows)
  - Legacy `addmultisigaddress` / account APIs long removed or descriptor-superseded in modern Core
- Document each omitted method in `COMPAT.md` with “use Core to migrate to descriptors” where relevant.

### 4.7 Wallet stance (normative)

**In scope for 1.0:** Bitcoin Core **descriptor wallets** managed via **JSON-RPC and CLI**, including:

| Area | Requirement |
|------|-------------|
| Format | Descriptor wallet only (output script descriptors + miniscript where Core supports them) |
| Multi-wallet | Concurrent loaded wallets; per-wallet RPC routing; create/load/unload |
| Watch-only / solvable | Ranged and non-ranged descriptors; combo/tr/pkh/wpkh/sh/wsh/multi/sortedmulti/tr as Core |
| Private keys | Optional disabled-private-keys wallets; encrypted wallets at rest |
| Birth / rescan | Rescan from timestamp or height against archival chain (leverage native tx/script indexes; avoid Core-style full block scan where store allows faster matching) |
| Coin selection | Behavioral parity with Core’s knobs (avoid partial spends, consolidate, etc.) where exposed over RPC |
| PSBT | Full wallet PSBT lifecycle parity with Core descriptor wallets |
| External signer | Parity with Core’s external signer RPC integration if present in pinned Core version |
| Persistence | Durable wallet DB separate from chain mmap store (SQLite is the natural Core-aligned choice); atomic updates; backup guidance |
| Chain coupling | Block connected/disconnected + mempool notifications update wallet UTXO/tx state; reorg-safe |
| IBD interaction | Wallets may be loaded during IBD with progress-aware rescan; must not regress IBD hot path (no per-block fsync in chain store) |

**Out of scope:**

- Legacy Core wallet types (non-descriptor), including opening Core’s old BDB wallets in-place.
- Automatic in-process migrator from legacy Core wallets (operators use Core’s migrator, then `listdescriptors` / `importdescriptors` or file copy of a descriptor wallet if format-compatible — **format compatibility with Core’s descriptor SQLite is a Phase design choice**, see §18).
- GUI wallet.

**Design note:** Wallet UTXOs are a **per-wallet cache** derived from the relational archive + mempool, not the global chainstate. Global chain remains Class A/B/C mmap; wallet does not reintroduce a node-wide UTXO set as the authority for consensus.

---

## 5. Storage engine design (`rbitcoin-store`)

Implement the mental model from durable-archive §2 and libbitcoin-database v4-style tables.

### 5.1 Table classes

**Class A — write-once archive bodies**

- `header`, `tx`, `input`, `output`, `ins`, `outs`, `txs` (and filter bodies as needed).
- Append-oriented slab/blob + array maps.
- Output row = parent linkage + value + script only (**no spender field**).

**Class B — forever-open multimaps**

- `point` (key = outpoint → spending input linkage), optional `address`, `duplicate`, …
- Append body + **mutable hash heads** (CAS + release ordering).
- Finalization never claims “spend list complete forever.”

**Class C — tip / confirmation state**

- `strong_tx`, `confirmed` / candidate linkage, validation caches as needed.
- Reorg clears/sets strong without rewriting Class A.

### 5.2 Low-level primitives

| Primitive | Behavior |
|-----------|----------|
| mmap file regions | Bodies + heads; grow by remap or preallocate strategy (document platform limits) |
| Hash head | Lock-free or sharded CAS publish; memory order matches libbitcoin release-fence publish |
| Array table | Dense fk → record |
| Blob / slab | Variable-length scripts, witness, etc. |
| Transactor | Exclusive multi-table consistency for snapshot/finalize; readers stay concurrent |
| Allocate-then-publish | Writers allocate space, fill, then publish head/link so readers never see partial rows |

### 5.3 Durability: epochs + wire ring

Implement **exactly** the durable-archive variant:

1. **`archive_mode` gate** — false during IBD; true only when contiguous + current (§3.1).
2. **Bulk finalize on first enable** — exclusive transactor; fsync Class A/B bodies+heads; write epoch at `finalized_height = tip - N` (§3.3).
3. **Wire ring** — last N blocks (or byte budget) in canonical wire form at node layer (§4).
4. **Incremental finalize** — when tip − finalized_height > N; prefer global HWM strategy (B) in v1 (§5.4).
5. **Recovery** — trust ≤ epoch; replay wire / fetch peers for soft zone; rebuild Class C as needed (§5.2).
6. **Config knobs** — as in durable-archive §8.

**Acceptance tests** from durable-archive §9 are **release gates** for the store+node durability track.

### 5.4 What we deliberately do not build

- In-place spender fields on outputs.
- SQL backend.
- Full historical wire archive by default.
- Mandatory per-block fsync during IBD.

---

## 6. Validation & sync design

### 6.1 Pipeline stages

```text
 Peers ──► Header tree (most work)
              │
              ▼
     Block download window (concurrent, bounded e.g. ≤50k heights)
              │
              ├─► Contextual-free checks + store txs (concurrent)
              ├─► Validation (scripts etc., partial order)  [skip if h ≤ milestone]
              └─► Confirmability (prevout exist/unspent, chain order)
                        │
                        ▼
                 Publish confirmed / strong_tx
```

### 6.2 Concurrency rules

- Download, store, validation, and confirmability **overlap** on different height ranges.
- Confirmability advances in order (or on non-overlapping contiguous ranges) so spend checks are sound when milestone is off.
- Under milestone: still verify commitment structure (no malleation of tx/witness commitments as required); skip script + confirmability per documented threat model.
- Never block IBD on epoch fsync or wire writes.

### 6.3 Steady-state tip

After `archive_mode = true`:

- Prefer low-latency single-block (or small batch) validation path for new tips.
- Write wire ring on accept; incremental finalize in background/batches.
- Compact block reconstruction must meet network expectations (wire ring helps serve recent blocks).

### 6.4 Crypto performance

- Use hardware SHA (SHANI / ARM) via rust-bitcoin / hashes stack; verify enablement in release builds.
- secp256k1: official `secp256k1` crate with correct feature flags; multi-thread script verify.
- Merkle and batch hashing: vectorized where available; measure before custom asm.

---

## 7. Network design notes

- **IBD:** high outbound peer count (configurable; default ~100 class), concurrent handshake races, spread block requests, drop slow/stalled peers by download-rate deviation (libbitcoin-inspired).
- **Steady state:** fewer outbounds; block-relay-only peers; standard Core-like topology knobs.
- **Protocol:** implement against rust-bitcoin network message types; version negotiation; feeler; addrv2; compact blocks.
- **DoS:** unsolicited block/tx limits, response timeouts, per-peer byte/cpu budgets, inv queues — production requirement before public listen-by-default.

---

## 8. Mempool design notes

- In-memory primary structure with fee-ordered eviction.
- Conflict / RBF rules aligned with Core policy version N (document which).
- Optional: persist unconfirmed txs in store Class A/C hybrid (libbitcoin “transaction pool on disk”) as Phase 2; v1 can use Core-like `mempool.dat`.
- Interface cleanly to confirmability when connecting a block (remove confirmed; re-add reorged).

---

## 9. RPC, ZMQ, config, ops

### 9.1 Config

- Bitcoin Core–familiar names where possible (`datadir`, `rpcuser`, `bind`, `proxy`, `assumevalid` / milestone, `dbcache` analogue → store map budget, etc.).
- Durability knobs from durable-archive §8 under a clear prefix (`archive_*`) to avoid false Core compatibility claims.

### 9.2 Observability

- Structured logs (tracing).
- Metrics: IBD heights/stage rates, peer bandwidth, script verify rate, finalize lag, wire ring occupancy, mmap growth, RPC latency.
- `getblockchaininfo`-level progress fields during sync.

### 9.3 Lifecycle

- Fast shutdown: stop net → flush mempool → optional tip snapshot → exit. Target sub-minute shutdown after full sync (libbitcoin-class), not multi-minute UTXO flush pain.
- Crash recovery path fully specified for both `archive_mode` true/false.

---

## 10. Testing strategy

### 10.1 Coverage mandate (non-negotiable)

| Requirement | Rule |
|-------------|------|
| **Line coverage** | **100%** of first-party Rust lines in workspace crates that ship in release (and their `cfg(test)`-adjacent production paths). |
| **Branch coverage** | **100%** branch coverage of the same code (every `if`/`match` arm, `?` error path that is real code, loop entry/exit where the instrumenter counts branches). |
| **Enforcement** | CI **fails** the merge if coverage drops below 100% line **or** 100% branch on the measured set. Coverage reports are artifacts on every PR. |
| **Scope** | All workspace library/binary code we own. Exclude only: generated code (if any, minimize), third-party deps, and explicitly listed dead `#[cfg]` platform stubs that are proven unreachable on CI targets (document in `COVERAGE.md`; prefer deleting dead code over excluding it). |
| **Tools** | LLVM source-based coverage via `cargo llvm-cov` (or equivalent) with **branch** instrumentation enabled; HTML + LCOV artifacts; optional codecov/coveralls upload. |

**Definition of done for any PR that adds or changes production code:** the new paths are exercised by automated tests such that global 100%/100% still holds. “I’ll add tests later” is not acceptable.

### 10.2 Testing philosophy: high-level first

**Prefer functional / integration tests at the highest reasonable layer** so one scenario covers many crates and real wiring.

| Preference order (highest first) | Examples |
|----------------------------------|----------|
| 1. **Multi-process / full node** | Spawn node binary (or in-process `Node` with real store path), P2P peers, RPC/CLI clients; regtest mine, reorg, wallet send; compare to bitcoind |
| 2. **Subsystem integration** | Store+query+consensus without full P2P; wallet+chain notifications+mempool; net harness with mock peers feeding the real pipeline |
| 3. **Scenario suites with fault injection** | Process kill mid-finalize; corrupt epoch byte; stall peer; disk-full simulation — still through public node/store open APIs |
| 4. **True unit tests** | **Avoid by default.** Only when a branch cannot be reached through any higher-level API without absurd harness cost (document why in the test file). Prefer expanding the harness over adding `#[cfg(test)]` white-box tests of private helpers. |

**Implications:**

- Design production APIs so rare branches (error paths, reorg edge cases, `archive_mode` transitions) are **triggerable from the outside** (config, RPC, fault injectors, test-only knobs behind `#[cfg(feature = "integration-testing")]` if needed — never silent dead code).
- Do **not** structure code for easy private-unit-testing at the expense of clear module boundaries; structure code so **integration scenarios** can hit every branch.
- Shared test support lives in `rbitcoin-test` (fixtures, regtest miner, peer simulator, Core differential runner, coverage scenario registry).

### 10.3 How to reach 100% without a unit-test mountain

1. **Scenario matrix, not function matrix** — enumerate behaviors (IBD catch-up, enable archive_mode, incremental finalize, deep reorg within wire window, wallet rescan after import, RBF replace, ban peer, …) and implement each as one high-level test that asserts observable outcomes (RPC, on-disk epoch, peer messages).
2. **Error-path campaigns** — dedicated suites that force failures through public surfaces: invalid blocks from a peer, bad RPC args, unreadable datadir, passphrase wrong, descriptor checksum wrong, epoch checksum fail. Each failed `match` arm must appear in some scenario.
3. **Parametric / table-driven integration tests** — one harness, many cases (script vectors, policy packages, descriptor types), still running through validation/wallet entry points.
4. **Coverage-guided gap closure** — after the suite runs, open the branch report; any red branch gets a **new high-level scenario** (or a justified narrow test). No “cover with a trivial assert on a private fn” as the default fix.
5. **Feature flags for injectors** — e.g. crash points, artificial peer delay, finalize batch size 1; compiled into CI test builds so branches are reachable without rewriting production control flow for unit tests.
6. **Core differential** — where behavior must match Core, drive both nodes with the same RPC script; our coverage comes from executing our stack; correctness from comparing results.

### 10.4 Scenario catalog (coverage-bearing layers)

| Layer | High-level content (primary) |
|-------|------------------------------|
| Consensus / vectors | Feed Core script/tx/block vectors through the **node or consensus service API**, not isolated free functions when avoidable; bip341/342; historical block apply |
| Store / durability | Full open→write→finalize→kill→recover paths; durable-archive §9 acceptance; mid-publish crash via injector; corrupt sealed prefix detection on restart |
| Net / IBD | Multi-peer download, stall/slow eviction, compact blocks, headers races — real `rbitcoin-net` + pipeline |
| Mempool | RBF, packages, limits, reorg re-add — via RPC/`sendrawtransaction` and block connect |
| Wallet | Core functional test ports (descriptor subset); multi-wallet; encrypt; rescan; reorg; PSBT; external signer mocks — RPC/CLI driven |
| RPC / CLI | Every exposed method and major error code path exercised by client scripts; multi-wallet routing |
| Config / lifecycle | Startup/shutdown, signal handling, datadir layout, `archive_mode` auto flip |
| Soak (may be nightly) | Multi-day tip follow, restart loops — does not replace merge-gate coverage but catches timing bugs |

**Fuzzing:** optional supplement for parsers (P2P, RPC, descriptors). Fuzz hits do **not** replace the 100% structured-coverage gate unless integrated into the measured coverage run and deterministic enough for CI.

**Benchmarks:** IBD/query benches are performance gates, not coverage substitutes.

### 10.5 CI gates

| Gate | When | Rule |
|------|------|------|
| `fmt` + `clippy -D warnings` | Every PR | Clean |
| Full functional/integration suite | Every PR | Green |
| **Coverage 100% line + 100% branch** | Every PR | Hard fail if under |
| Core differential (regtest) | Every PR or required check | No divergences on pinned scenario pack |
| Slow soak / mainnet IBD bench | Nightly / lab | Non-blocking for trivial docs PRs; blocking for release |

### 10.6 Phase and PR policy

- **Phase 0:** wire `cargo llvm-cov` (branch), coverage bot, `COVERAGE.md`, empty-suite baseline; first production code lands only with scenarios that keep 100%.
- **Each phase exit:** coverage still 100%/100%; scenario catalog updated for that phase’s features.
- **PR review:** reject pure production logic without a high-level test; reject new `#[cfg(test)]` modules that re-implement integration paths as unit tests without justification.
- **1.0 release:** coverage gate green on release tag; document any exclusion list (should be empty or trivial).

### 10.7 Consensus correctness gate (orthogonal to coverage)

Continuous “compare-on-regtest / mainnet shadow” mode that validates the same blocks as a local Core and flags divergence. **Coverage ≠ consensus correctness**; both are required.

### 10.8 Risks of the 100% bar (accepted)

| Risk | Mitigation |
|------|------------|
| Pressure to write low-value unit tests | Explicit philosophy §10.2; review culture prefers scenarios |
| Unreachable defensive code | Delete it, or trigger via injectors; no “exclude to hit 100%” without design review |
| Flaky integration tests | Deterministic regtest clocks/seeds; retry budget only for known OS flakes; quarantine is a bug |
| Slow CI | Parallelize scenarios; in-process node where safe; keep multi-process for true crash/IPC paths |
| 100% bar slows delivery | Front-load harness in Phase 0; treat coverage as part of feature work, not a tax at the end |

---

## 11. Phased delivery plan

Each phase ends with **mergeable software** and **measurable exit criteria**. Phases map to multi-PR stacks; see §13.

**Standing exit criterion for every phase:** workspace remains at **100% line and 100% branch coverage**, with new behavior covered by **high-level functional/integration scenarios** (§10). Phase-specific exits below are additive.

### Phase 0 — Foundations (2–4 weeks)

**Scope**

- Repo bootstrap: workspace, CI (fmt, clippy, test), MSRV policy, licensing, CONTRIBUTING.
- **Coverage pipeline:** `cargo llvm-cov` with branch coverage; CI hard-fail at &lt;100% line or branch; `COVERAGE.md` policy; `rbitcoin-test` harness skeleton.
- Pin rust-bitcoin stack versions; feature matrix (mainnet params).
- Design freeze notes: table schemas (binary layouts), fk ID widths, endianness, file naming under `datadir`.
- Dev tooling: regtest bitcoin-core sidecar for differential tests (optional docker).
- First high-level scenarios: node starts, loads config, shuts down cleanly (establishes coverage culture before features land).

**Exit**

- Empty node binary runs, loads config, exits cleanly — covered by functional tests.
- Schema document reviewed.
- CI enforces 100% line + branch coverage on the workspace.

### Phase 1 — Store MVP (Class A/B basics) (4–8 weeks)

**Scope**

- mmap growable files; array + blob + hashhead primitives.
- Tables: header, tx, input, output, linkage, `point`.
- Transactor + basic snapshot (no epoch yet).
- High-level store scenarios: insert synthetic chain, concurrent readers, allocate-then-publish visibility, restart reopen — via public store/query APIs (not private unit tests).

**Exit**

- Can insert synthetic chain and query tx/out/spenders correctly after restart (best-effort mmap durability only).
- Coverage still 100% line + branch for all store code paths introduced.

### Phase 2 — Query + confirmability primitives (3–6 weeks)

**Scope**

- `strong_tx` / confirmed height indexes.
- Navigation APIs: `to_spenders`, confirmed height of tx, block tx list.
- Reorg unconfirm path (Class C only).

**Exit**

- Regtest chain build/reorg with full structural queries green.

### Phase 3 — Consensus validation (4–8 weeks)

**Scope**

- Header PoW/work; block contextual checks; tx checks; script verify path.
- Milestone + checkpoints.
- Parallel validation workers.
- Differential tests vs Core on regtest blocks.

**Exit**

- Independently validates regtest and signet tip (with network or block files).

### Phase 4 — P2P + concurrent IBD (6–12 weeks)

**Scope**

- Peer manager, headers sync, block download window, peer scoring/eviction.
- Wire IBD pipeline: download ∥ store ∥ validate ∥ confirm.
- Milestone-accelerated mainnet IBD path.
- No archive durability tax.

**Exit**

- Completes mainnet IBD under milestone; height matches network; benchmarks recorded vs libbitcoin/Core on fixed hardware profile.
- **Performance gate:** document numbers; iterate on download concurrency and store publish path until within target band of libbitcoin.

### Phase 5 — Durable archive variant (3–6 weeks)

**Scope**

- Full implementation of `libbitcoin-durable-archive-variant.md`:
  1. Epoch record + `finalize` + open/recover
  2. `archive_mode` state machine
  3. Wire ring + prune-on-finalize
  4. Incremental finalize + optional checksums
  5. Crash/corrupt tests

**Exit**

- Durable-archive §9 acceptance criteria pass.

### Phase 6 — Mempool, tip following, compact blocks (4–8 weeks)

**Scope**

- Mempool policy + persistence.
- Steady-state tip latency optimization.
- Compact blocks / high bandwidth.
- Wire ring used for serve/reorg/recovery.

**Exit**

- Node follows mainnet tip stably for soak period; mempool parity tests vs Core policy suite where applicable.

### Phase 7 — Node RPC / ZMQ / CLI foundation (3–6 weeks)

**Scope**

- JSON-RPC server; auth; batch; warmup; multi-wallet routing hooks (even before wallet is complete).
- Priority node RPC set (§4.5 waves 1–3).
- ZMQ.
- `rbitcoin-cli` skeleton with Core-compatible invocation patterns.
- Config compatibility notes; manpage/docs for node surface.

**Exit**

- Can replace Core for common non-wallet operator workflows (monitor, broadcast, GBT, explorers using txindex-like queries).

### Phase 8 — Descriptor wallet core (6–12 weeks)

**Scope**

- `rbitcoin-wallet` crate: descriptor/miniscript stack, wallet DB, multi-wallet manager.
- Key material + encryption; watch-only and full-key wallets.
- Chain/mempool notification interface; wallet UTXO and tx records; reorg handling.
- Rescan implementation optimized for archival store (script/outpoint matching via indexes where possible).
- Coin selection + tx construction + fee estimation integration.
- PSBT create/process/finalize paths aligned with Core descriptor wallets.
- Wallet RPC wave (§4.5 waves 5–10) + CLI wallet flags.
- Differential tests vs bitcoind descriptor wallet on regtest (create, receive, send, bump, encrypt, backup/restore of *our* format or agreed Core-compatible format).

**Exit**

- Core descriptor-wallet functional scenarios pass against this node on regtest.
- Documented `COMPAT.md` list of intentionally absent legacy-wallet RPCs.
- Multi-wallet load/unload under a synced node is stable.

### Phase 9 — Wallet hardening, external signer, long-tail (3–6 weeks)

**Scope**

- External signer RPC parity (as in pinned Core).
- Remaining wallet RPC edge cases; label APIs; send variants; avoid partial spends / consolidate flags.
- Backup/restore runbooks; corruption recovery for wallet DB (independent of chain epoch).
- Performance: rescan of large wallets on mainnet-sized archive; mempool-aware balance updates.
- Security review of key material handling (mlock considerations, zeroize, file permissions).

**Exit**

- Wallet track production checklist green; no known P0 divergences from Core descriptor behavior on agreed test suite.

### Phase 10 — Hardening & production (ongoing → 1.0)

**Scope**

- DoS review, dependency audit, sandbox notes.
- BIP324, Tor, binding, permissions.
- Packaging (deb/rpm/docker), reproducible builds.
- Address index / filters optional features.
- Remaining node + wallet RPC long-tail.
- Security disclosure process; versioning; assumevalid/milestone update procedure.

**Exit**

- Production checklist signed off; 1.0 release candidate (node **and** descriptor wallets).
- Coverage gate green at 100% line + 100% branch on the release tag; `COVERAGE.md` exclusion list empty or design-approved only.

---

## 12. Performance program (IBD & queries)

Treat performance as a **first-class workstream** from Phase 1, not a polish pass.

### 12.1 IBD levers (priority order)

1. Concurrent multi-peer download with large window (not Core-style sequential connect).
2. Append-only mmap store without per-block fsync in IBD.
3. Milestone skip of script + confirmability (no UTXO rebuild).
4. Parallel validation when milestone off / above milestone.
5. Hardware crypto (SHA-NI, fast secp).
6. Avoid wire reconstruction on the hot IBD path.
7. Allocator / page fault tuning; huge pages experiment (document, don’t require).

### 12.2 Query levers

1. O(1) hash → tx/header via heads.
2. Spender multimap without scanning.
3. Careful cache locality of fk arrays.
4. Optional read-ahead for sequential height scans (RPC `getblock` reconstruction).

### 12.3 Benchmark suite (automate early)

- `bench-ibd-signet` / `bench-ibd-mainnet` (nightly/hardware lab).
- `bench-query-random-tx`, `bench-spenders`, `bench-getblock-reconstruct`.
- Compare artifacts: wall time, CPU%, RSS, disk write GB, fsync count (must be ~0 extra in IBD).

### 12.4 Performance non-negotiables

- Enabling durability features **must not** regress IBD when `archive_mode` stays false (durable-archive §9.1).
- Reconstruction cost for historical `getblock` is acceptable; tip serve uses wire ring.

---

## 13. PR / workstream DAG (implementation order)

PRs are sized for review; names are illustrative.

```text
PR00 Workspace + CI + schema doc + coverage gate (100% line/branch) + rbitcoin-test harness
  └─► PR01 store: mmap file + array + blob
        └─► PR02 store: hashhead CAS + publish order
              └─► PR03 store: header/tx/in/out tables
                    └─► PR04 store: point multimap
                          └─► PR05 query: archive write path
                                └─► PR06 query: navigate + confirmed + strong
                                      ├─► PR07 consensus: headers + block checks
                                      │     └─► PR08 consensus: scripts + milestone
                                      └─► PR09 net: peers + headers
                                            └─► PR10 net: block download + IBD orchestration
                                                  └─► PR11 node: pipeline integration (IBD green)
                                                        ├─► PR12 store: epoch finalize + recover
                                                        ├─► PR13 node: archive_mode + bulk finalize
                                                        └─► PR14 wire-cache ring
                                                              └─► PR15 incremental finalize + crash tests
                                                        ├─► PR16 mempool
                                                        ├─► PR17 compact blocks + tip path
                                                        └─► PR18 rpc/cli node waves + ZMQ + multi-wallet routing shell
                                                              ├─► PR19 wallet: descriptors + DB + multi-wallet manager
                                                              ├─► PR20 wallet: chain notifications + rescan + UTXO cache
                                                              ├─► PR21 wallet: coin selection + send + PSBT
                                                              ├─► PR22 wallet: encryption + backup/restore + RPC/CLI wave
                                                              └─► PR23 wallet: external signer + long-tail parity
                                                                    └─► PR24 hardening / packaging / 1.0
```

**Parallelism after PR06:** consensus and net can proceed in parallel; durability (PR12–15) can start once IBD store writes are stable (PR11). Wallet (PR19+) can begin once regtest chain notifications and query APIs exist (PR06+PR11 minimum); full rescan/send needs mempool + tip (PR16–17).

---

## 14. Key decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Primary chainstate | Relational mmap archive + confirmation indexes | Libbitcoin IBD/query performance; no multi-GB ordered UTXO write bottleneck |
| Spend index | Append `point` multimap; immutable outputs | Matches durable-archive; reorg-friendly; concurrent writes |
| Consensus types | rust-bitcoin + tested script path | Correctness and ecosystem alignment; avoid dual type stacks |
| Async runtime | tokio for net/RPC; dedicated thread pools for validation/store | Separate IO from CPU-bound verify |
| Durability | Epoch finalize + wire ring **after** IBD only | Core-class buried data without killing IBD |
| Wallet | **Descriptor wallets only**, full RPC/CLI parity with Core | Matches modern Core recommendation; avoids BDB/legacy keypool surface |
| Legacy wallets | Unsupported | Core still opens old types; we do not — migrate in Core first if needed |
| Wallet storage | Separate durable DB (SQLite-class), not chain mmap | Key material isolation; independent backup; no IBD coupling |
| Prune / GUI | Never in this product line | Stated product boundary |
| RPC / CLI compatibility | Core-compatible for node + descriptor wallet; document legacy gaps | Operators and tooling reuse |
| Milestone default | On for IBD (updated each release) | Required for libbitcoin-class IBD; same practical trust as Core assumevalid |
| Wire history | Tip ring only by default | Sealed archive is long-term truth; ring is hot cache |
| Test coverage | **100% line + 100% branch**, CI-enforced | Production quality bar; no untested merge |
| Test style | High-level functional/integration preferred; unit tests discouraged | One scenario covers real wiring; avoids mock-heavy false confidence |
| Crate layout | Multi-crate monorepo (not multi-repo) | Libbitcoin-shaped libraries, Rust workspace packaging |

---

## 15. Risks and mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Consensus divergence from Core | Critical | High-level vector + shadow validation; careful soft-fork upgrades |
| mmap portability / 32-bit / Windows | High | Linux-first production target; abstract OS layer; document support matrix |
| Store format churn | High | Versioned magic; explicit migrations or reindex-only until 1.0 format freeze |
| Tip latency worse than Core | Medium | Wire ring + specialized steady-state path; benchmark tip connect |
| Historical getblock CPU | Medium | Optional cache; accept archival tradeoff; filters for light clients |
| Scope creep (assumeutxo, optional indexes, GUI-like features) | High | Phased plan; protect 1.0 definition in §4; wallet scope capped at descriptor RPC/CLI |
| Wallet / Core behavioral drift (fees, coin selection, PSBT) | High | Port Core wallet functional tests; differential regtest vs bitcoind; pin Core version in COMPAT.md |
| Key management bugs | Critical | Threat model review; encryption tests; no keys in logs; secure file perms; optional hardware signer path |
| Rescan performance on archival store | Medium | Index-assisted script matching; progress RPC; avoid full wire rebuild per block when possible |
| Underestimating P2P DoS surface | Critical | External review; default secure bind; rate limits before public advertising |
| rust-bitcoin API churn | Medium | Pin versions; thin facade in `rbitcoin-primitives` |
| Coverage gamed with low-level unit tests | Medium | §10.2 review norm; prefer scenario PRs; COVERAGE.md “unit test exception” requires justification |
| CI too slow from integration-only suite | Medium | Parallel jobs, in-process node, scenario sharding; keep multi-process for crash tests only |

---

## 16. Team / skill tracks (suggested ownership)

1. **Store & durability** — mmap, epochs, crash/recovery scenarios.
2. **Consensus** — validation, milestone, Core differential.
3. **Network & IBD** — peers, download, orchestration, benches.
4. **Mempool & policy** — RBF, packages, fee est.
5. **Wallet** — descriptors, multi-wallet, PSBT, rescan, encryption, Core wallet functional ports.
6. **RPC/CLI/ops** — compatibility, packaging, docs.
7. **Perf** — continuous benchmarking, crypto, allocation.
8. **Test harness / coverage** — `rbitcoin-test`, scenario catalog, CI coverage gate, gap closure without unit-test sprawl.

One tech lead owns cross-cutting consensus correctness and release gates; wallet lead owns key-handling security sign-off; harness owner protects the 100% coverage bar and high-level testing culture.

---

## 17. Documentation deliverables

- `SCHEMA.md` — on-disk table layouts, versioning (chain store).
- `WALLET.md` — descriptor wallet DB layout, encryption, backup/restore, multi-wallet layout under datadir.
- `CONSENSUS.md` — milestone/checkpoint policy; upgrade process.
- `OPERATOR.md` — datadir, config, RPC/CLI, durability knobs, recovery, wallet operations.
- `PERF.md` — how to reproduce IBD benches; wallet rescan benches.
- `COMPAT.md` — RPC/CLI and config matrix vs Core version N; explicit legacy-wallet omissions.
- `COVERAGE.md` — 100% line/branch policy, tool commands, exclusion rules (default: none), how to add a scenario for a red branch, when a narrow unit test is allowed.
- `TESTING.md` — harness overview, scenario catalog index, Core differential runner, fault injectors.
- Inline rustdoc for store primitives (publish order invariants are load-bearing).

---

## 18. Open questions (resolve before or during Phase 0–1)

1. **Binary/crate public name and license** (MIT/Apache-2.0 vs others; trademark care with “Bitcoin”).
2. **MSRV and target platforms** for 1.0 (recommend: Linux x86_64/aarch64 first).
3. **Script verification backend:** pure rust-bitcoin vs `bitcoinconsensus` (libbitcoinconsensus) for bit-exact Core matching — recommend consensus lib for 1.0 gate, optional faster path if proven equivalent.
4. **Address index in 1.0 or 1.1?** Default: 1.1 optional feature (wallet does not require a global address index if it tracks its own scripts).
5. **Descriptor wallet DB format:** byte-compatible with Core’s SQLite descriptor wallet vs intentional independent format with descriptor import/export only — decide in Phase 8 design note (compatibility eases `restorewallet`; independence eases iteration).
6. **Wallet implementation base:** build primarily on BDK vs rust-bitcoin/miniscript-first custom wallet with selective BDK reuse — choose by RPC-shape fit and PSBT parity cost.
7. **Deep reorg beyond wire_depth after finalize:** fail closed + peer rebuild vs explicit unfinalize tool.
8. **Whether Class C is included in epoch** or reconstructed (durable-archive §5.3) — pick one in Phase 5 design note and test it.

---

## 19. Suggested near-term execution (first 90 days)

**Days 0–30**

- Phase 0 complete; SCHEMA.md + COVERAGE.md + TESTING.md; CI coverage gate live.
- PR01–PR04 landed: can store headers/txs/outs/points in mmap tables with high-level scenarios keeping 100%/100%.

**Days 30–60**

- PR05–PR08: query + consensus on regtest via integration scenarios.
- Differential harness vs bitcoind; expand scenario catalog for every new branch.

**Days 60–90**

- PR09–PR11: P2P headers + block download; first signet/mainnet IBD experimental run.
- Establish nightly bench skeleton; first numbers (even if far from target).
- Coverage remains merge-blocking as net/IBD code lands.

**Parallel track from day 30**

- Draft epoch format and recovery state machine on paper; implement after IBD path exists (Phase 5), with crash scenarios planned alongside the feature.

---

## 20. One-sentence north star

**Ship a Rust full node that speaks rust-bitcoin at the protocol edge, stores the chain as a concurrent libbitcoin-style relational mmap archive with post-IBD epoch durability and a tip wire ring, matches Bitcoin Core’s non-prune non-GUI surface including descriptor-wallet RPC/CLI (not legacy wallets), wins on IBD and structural queries the way libbitcoin does, and proves every line and branch through high-level functional tests — without making a global UTXO set the center of consensus chainstate.**

---

## 21. Document control

| Item | Value |
|------|-------|
| Status | Living plan — implementors update phase exit criteria as measured |
| Depends on | `libbitcoin-durable-archive-variant.md` (normative for durability/store mutation rules) |
| Next action | Resolve §18 open questions; start Phase 0 workspace + SCHEMA.md |
