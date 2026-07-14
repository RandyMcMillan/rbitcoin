# Compatibility with Bitcoin Core

Pinned reference version: **to be set** (target: recent major, e.g. 28.x / 29.x).

## Intentional differences

| Area | This node | Bitcoin Core |
|------|-----------|--------------|
| Chainstore | Relational mmap archive | blocks/undo + LevelDB chainstate UTXO |
| Pruning | Not supported | Supported |
| GUI | Not supported | Qt GUI |
| Legacy wallets | Not supported | BDB / non-descriptor still openable |
| Descriptor wallets | Required path for 1.0 | Recommended path |
| Always-on tx / spender index | Native | Optional `txindex` |

## RPC / CLI

Track implemented methods in this file as they land. Format:

```text
method_name | status (done/partial/absent) | notes
```

### Node (initial)

| Method / surface | Status | Notes |
|------------------|--------|-------|
| process start/stop | partial | Lifecycle only in Phase 0 |
| JSON-RPC server | absent | Phase 7 |

### Wallet

Legacy-only methods (create non-descriptor, BDB load, `importprivkey` primary flows, etc.): **absent by design**.

## Config knobs

Core-familiar names preferred where behavior matches. Durability knobs use `archive_*` prefix (see durable-archive §8).
