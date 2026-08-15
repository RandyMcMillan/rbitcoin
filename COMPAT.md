# Compatibility with Bitcoin Core

Pinned reference version: **target Core ≥27** for BIP324 v2 interop; package wire
tracks BIP331 when rust-bitcoin exposes the messages.

**Experimental 0.x** — not a production Core or Fulcrum replacement. Design
contrasts: [`docs/architecture.md`](./docs/architecture.md). Lab mainnet:
[`docs/experimental-mainnet.md`](./docs/experimental-mainnet.md).

## Active product track

Full **P2P participant** (blocks + tip-mode tx relay) and **wallet-client
backends**: in-process **Electrum** (confirmed + unconfirmed, libre-relay-class
admission) and optional **Esplora-compatible REST** for the same role (history,
UTXO, broadcast, block/tx fetch by id). Optional **Core-class JSON-RPC subset**
(see [`docs/rpc.md`](./docs/rpc.md)) — not full Core wallet / mining parity.
**Scripthash index (`--shindex`) defaults off**; Electrum/Esplora require it.

### Query surface intent: wallet clients, not graphical explorers

**Goal:** serve **wallet software** (Electrum, Sparrow, custom wallets, light
clients that already know their addresses/scripthashes or exact txids/block
ids).

**Non-goal:** power a **graphical block explorer** product (search boxes,
address-prefix autocomplete, “browse everything” UX, Liquid/mining template
surfaces). Those need reverse indexes and explorer-only APIs we deliberately
omit. Block/tx **by full id** and address/**exact** scripthash history exist so
wallets and APIs can verify and sync—not so we become mempool.space.

## Intentional differences

| Area | This node | Bitcoin Core |
|------|-----------|--------------|
| Chainstore | Relational **map-free** archive (fd tables; see [`docs/io-modality.md`](./docs/io-modality.md)) | blocks/undo + LevelDB chainstate |
| Historical blocks | Reconstruct from archive; tip via body queue + peer wire | `blocks/` blk*.dat |
| Transport | **BIP324 v2 only** | v1 + v2 |
| Mempool structure | Cluster graph + chunks | Cluster mempool (same lineage) |
| Admission policy | **Libre-relay-class** (0.1 sat/vB, no dust, full RBF) | Standardness + policy knobs |
| Compact blocks | BIP152 **v2** receive + reconstruct + `getblocktxn` serve | v1/v2 high-bandwidth |
| WTx inventory | BIP339 when peer also sends `wtxidrelay` | BIP339 |
| Package relay wire | `accept_package` + experimental `rbtpkg` | BIP331 |
| Pruning / GUI / mining | Not supported | Supported |
| Wallets | Electrum clients (requires `--shindex`) | Descriptor + legacy |
| Scripthash index | Optional (`--shindex`, default **off**); bulk at tip when on | External ElectrumX / Fulcrum; Core `-txindex` is different (txid→block) |
| JSON-RPC | Documented **subset** ([`docs/rpc.md`](./docs/rpc.md)); cookie/user-pass | Full Core RPC |

## Core-class JSON-RPC (subset)

| Method group | Status | Notes |
|--------------|--------|-------|
| Control (`help`, `uptime`, `stop`, `getrpcinfo`, `syncwithvalidationinterfacequeue`) | done | Queue RPC is a no-op `null` |
| Blockchain (`getblockchaininfo`, `getblockcount`, `getbestblockhash`, `getblockhash`, `getblock`/`header`, `getdifficulty`) | done | Archive reconstruct |
| Network (`getnetworkinfo`, `getconnectioncount`, `getpeerinfo`, `addnode`, `disconnectnode`, `addconnection`) | done | BIP324 v2-only; live session table |
| Mempool / rawtx (`getmempool*`, `getrawtransaction`, `sendrawtransaction`, `testmempoolaccept`) | done | Libre policy |
| Fee (`estimatesmartfee`) | done | **10-minute inclusion** product — not Core historical |
| Decode (`decoderawtransaction`, `decodescript`, `validateaddress`) | done | |
| Regtest `generatetoaddress` / `generateblock` / `generate` / `submitblock` | harness | **Regtest only.** Same confirm/accept path as P2P. Not a mining product. |
| Wallet / mining / GBT | **never** | Non-goal (no GBT / wallet keys) |
| `createrawtransaction` / `combinerawtransaction` | **never** | External tools |
| `scantxoutset` / `gettxoutsetinfo` | **never** | No UTXO-set coins DB |

Full method list, auth, and shindex matrix: **[`docs/rpc.md`](./docs/rpc.md)**.

## Electrum surface

| Method | Status | Notes |
|--------|--------|-------|
| server.version / banner / features | done | Banner: libre-relay-class. `server.version[0]` is `rbitcoin-electrs <ver>` so Cake `getNodeIsElectrs()` will probe tweaks |
| blockchain.tweaks.subscribe | done | Cake stream (first height as result, then notifies + `done`). Naive walk, or `--sptweaks` thin index (`len:tweak` only; outs from `txout`). Isolate may still hardcode `electrs.cakewallet.com` |
| headers / block headers | done | Tip push on subscribe |
| scripthash history / balance / listunspent | done | Unconf when mempool attached; `get_history` optional BCH-style `from_height` / exclusive `to_height` (`-1` = tip + mempool); 1-arg = full history; **subscribe status always full** |
| scripthash.get_mempool / subscribe | done | Status on mempool announce |
| transaction.get / get_merkle | done | get falls back to mempool |
| transaction.broadcast | done | Mempool accept + P2P inv |
| relayfee / estimatefee / histogram | done | Libre min + live median |
| TLS | external | terminate at reverse proxy; node is plain TCP |
| DoS floor | always on | max conn / line / idle / subs / broadcast hex (`ServeLimits`); public bind OK behind proxy |

## Esplora REST surface

Plain HTTP via `--esplora-listen` / conf `esplora_listen` (default **off**). TLS
via reverse proxy; app `ServeLimits` always on (same model as Electrum).

| Endpoint group | Status | Notes |
|----------------|--------|-------|
| Tip | done | `/blocks/tip/height`, `/blocks/tip/hash` |
| Blocks list | done | `/blocks`, `/blocks/:start_height` (10 summaries, newest-first) |
| Block | done | `/block/:hash` JSON, `/raw`, `/status`, `/header`, `/txids`, `/txid/:i`, `/txs[/:start]` |
| Tx | done | `/tx/:txid` full JSON, `/hex`, `/raw`, `/status`, Electrum `/merkle-proof`, BIP37 `/merkleblock-proof`, `/outspend(s)` |
| Address / scripthash | done | stats + `/utxo` + `/txs` + `/txs/mempool` + `/txs/chain[/:last_seen_txid]`; needs SH finalize |
| Mempool / fees | done | `/mempool`, `/mempool/txids`, `/mempool/recent` (accept-order ring), `/fee-estimates` |
| `POST /tx` | done | broadcast via mempool hub; **503** if hub absent |
| `POST /txs/package` | done | JSON array of hex txs → `accept_package`; **503** without hub; max 25 txs |
| Unknown path | 404 | plain body |
| **Non-goal / never** | — | Graphical explorer features: `address-prefix` search, Liquid/assets, mining `block-template`, explorer UI-only APIs |

## Esplora WebSocket (wallet live subset)

Same listen as REST (`--esplora-listen`). Paths: **`/v1/ws`** (preferred) and
**`/ws`** alias. Plain WS in-process; terminate **WSS** at the reverse proxy
(often public URL `wss://host/api/v1/ws` if the proxy strips `/api`).

**Product boundary:** wallet live updates only (tip, address watchlist, pending
txids, wallet-scoped RBF). **Not** a mempool.space explorer live backend.
Message *names* follow mempool.space where listed; **payloads use Esplora REST
shapes** (`build_tx_json` / `tx_status_json` / tip height+hash).

### Client → server (supported)

| Message | Behavior |
|---------|----------|
| `{ "action": "want", "data": ["blocks"] }` | Subscribe tip pushes; other `data` tokens **no-op** (no disconnect) |
| empty want / no `blocks` | Clear tip subscription |
| `{ "track-address": "<addr>" }` / `{ "track-addresses": [...] }` | Watchlist (network-checked); over-cap → `{ "error": "max_track_addresses exceeded" }` |
| `{ "stop-track-address": "…" }` / `stop-track-addresses` / empty track-address | Unsubscribe |
| `{ "track-tx": "<txid>" }` / `{ "track-txs": [...] }` | Pending set; over-cap → error |
| `{ "stop-track-tx": "…" }` / `stop-track-txs` | Unsubscribe |

No client API for global `track-mempool*`, `track-rbf`, or `want` stats/charts.

### Server → client (supported)

| Key | When |
|-----|------|
| `{ "block": { "height", "id", "timestamp" } }` | Tip advance after `want: blocks` |
| `{ "address-transactions": [ … ] }` | Mempool accept touching a tracked script (in/out when resolvable) |
| `{ "block-transactions": [ … ] }` | Tip height: confirmed history for tracked scripts at that height (SH index) |
| `{ "tx": { "txid", "status" } }` | Tracked txid status transition (mempool / confirmed) |
| `{ "replaced-transactions": [ { "txid", "replaced-by" } ] }` | Full-RBF replace **only if** old or new intersects this connection’s tracks |

Unknown client keys: ignored (or JSON error for bad JSON / oversize). Lagged
broadcast receivers drop (best-effort, like Electrum).

### Caps (`EsploraConfig`, defaults)

| Knob | Default |
|------|---------|
| max_ws_connections | 64 (separate from REST concurrency) |
| max_ws_message_bytes | 64 KiB |
| max_track_addresses | 64 / connection |
| max_track_txs | 64 / connection |

### Gap list (explorer-only — not supported)

| mempool.space-style feature | Status |
|-----------------------------|--------|
| `want`: `stats`, `mempool-blocks`, `live-2h-chart` | **No** |
| `track-mempool` / `track-mempool-txids` global firehose | **No** |
| `track-mempool-block` projected templates | **No** |
| Global `track-rbf` / `rbfLatest` trees | **No** (wallet-scoped replace only) |
| CPFP / `txPosition` / explorer fee-ladder fields | **No** |
| Durable resume / sequence cursors | **No** |

## BIP324 v2 short-ID surface (live paths)

Encode/decode uses Core’s `V2_MESSAGE_IDS` table (`crates/rbitcoin-net/src/v2.rs`).
**Live IBD + tip follow + tip tx relay** commands with short IDs:

| Short ID | Command | Role |
|----------|---------|------|
| 1 | addr | peers |
| 2 | block | IBD / tip body |
| 3 | blocktxn | compact fill |
| 4 | cmpctblock | tip HB |
| 5 | feefilter | tip policy |
| 9–15 | getblocks…mempool | headers/blocks/inv |
| 17–21 | notfound…tx | ping/pong/sendcmpct/tx |
| 28 | addrv2 | BIP155 |

Long-form (no short ID): `version`, `verack`, `wtxidrelay`, `sendheaders`,
`sendaddrv2`, and unknown/extension commands.

**Not implemented as product features** (short slots 22–27 compact filters, 29–36
placeholders, 37 `feature`): decode may reject unknown short IDs; peers that
only need the live set above interoperate. Full Core filter/light-client APIs
are deferred.

## Deferred surfaces

Core wallet RPC, mining GBT, fee-estimator research quality, BIP331 native wire
enum, durable orphans: **out of scope** for this plan.

**Permanent non-goals for Electrum/Esplora:** graphical explorer backends
(address-prefix autocomplete, global search, explorer-only catalogue APIs).
