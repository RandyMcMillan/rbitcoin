# Compatibility with Bitcoin Core

Pinned reference version: **to be set** (target: recent major, e.g. 28.x / 29.x).

## Active product track

Consensus validation, IBD, and **block** relay. Not claiming full Core operator parity yet.

## Intentional differences

| Area | This node | Bitcoin Core |
|------|-----------|--------------|
| Chainstore | Relational mmap archive | blocks/undo + LevelDB chainstate UTXO |
| Pruning | Not supported | Supported |
| GUI | Not supported | Qt GUI |
| Mempool / tx relay | Deferred | Full |
| Fee estimation | Deferred | Full |
| Wallets | Deferred | Descriptor + legacy open |
| Always-on tx / spender index | Native (target) | Optional `txindex` |

## RPC / CLI

Track implemented methods as they land. Format:

```text
method_name | status (done/partial/absent) | notes
```

### Node (initial)

| Method / surface | Status | Notes |
|------------------|--------|-------|
| process start/stop | partial | Lifecycle / smoke |
| JSON-RPC server | absent | Phase 7 of active plan |

### Deferred surfaces

Mempool, wallet, fee, mining RPCs: **absent by design** on this track.
