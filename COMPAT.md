# Compatibility with Bitcoin Core

Pinned reference version: **target Core ≥27** for BIP324 v2 interop; package wire
tracks BIP331 when rust-bitcoin exposes the messages.

## Active product track

Full **P2P participant** (blocks + tip-mode tx relay) and **Electrum** backend
(confirmed + unconfirmed, libre-relay-class admission). Not full Core JSON-RPC /
wallet / mining parity.

## Intentional differences

| Area | This node | Bitcoin Core |
|------|-----------|--------------|
| Chainstore | Relational mmap archive | blocks/undo + LevelDB chainstate |
| Historical blocks | Reconstruct from archive; tip wire ring | `blocks/` blk*.dat |
| Transport | **BIP324 v2 only** | v1 + v2 |
| Mempool structure | Cluster graph + chunks | Cluster mempool (same lineage) |
| Admission policy | **Libre-relay-class** (0.1 sat/vB, no dust, full RBF) | Standardness + policy knobs |
| Package relay wire | `accept_package` + experimental `rbtpkg` | BIP331 |
| Pruning / GUI / mining | Not supported | Supported |
| Wallets | Electrum clients | Descriptor + legacy |
| Scripthash index | Native on confirm | External ElectrumX / Fulcrum |

## Electrum surface

| Method | Status | Notes |
|--------|--------|-------|
| server.version / banner / features | done | Banner: libre-relay-class |
| headers / block headers | done | Tip push on subscribe |
| scripthash history / balance / listunspent | done | Unconf when mempool attached |
| scripthash.get_mempool / subscribe | done | Status on mempool announce |
| transaction.get / get_merkle | done | get falls back to mempool |
| transaction.broadcast | done | Mempool accept + P2P inv |
| relayfee / estimatefee / histogram | done | Libre min + live median |
| TLS | external | terminate at reverse proxy; node is plain TCP |

## Deferred surfaces

Core wallet RPC, mining GBT, fee-estimator research quality, BIP331 native wire
enum, durable orphans: **out of scope** for this plan.
