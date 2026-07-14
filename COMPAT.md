# Compatibility with Bitcoin Core

Pinned reference version: **to be set** (target: recent major, e.g. 28.x / 29.x).

## Active product track

Consensus validation, IBD, **block** relay (blocks-only), and **Electrum protocol serve** (confirmed history). Not claiming full Core operator parity yet.

## Intentional differences

| Area | This node | Bitcoin Core |
|------|-----------|--------------|
| Chainstore | Relational mmap archive | blocks/undo + LevelDB chainstate UTXO |
| Historical block files | Reconstruct from archive; tip wire ring only | `blocks/` blk*.dat |
| Pruning | Not supported | Supported |
| GUI | Not supported | Qt GUI |
| Mempool / tx relay | Deferred (Electrum broadcast = best-effort peer push) | Full |
| Fee estimation | Stub / deferred | Full |
| Wallets | Deferred (clients use Electrum protocol) | Descriptor + legacy open |
| Always-on tx / spender index | Native (target) | Optional `txindex` |
| Scripthash / Electrum index | Native (target, Phase 6–7) | Via ElectrumX / Fulcrum external |

## RPC / CLI / Electrum

Track implemented methods as they land. Format:

```text
method_name | status (done/partial/absent) | notes
```

### Node (initial)

| Method / surface | Status | Notes |
|------------------|--------|-------|
| process start/stop | partial | Lifecycle / smoke |
| Core-like JSON-RPC | absent | Phase 8 of active plan |
| Electrum TCP/SSL | absent | Phase 7; scripthash index Phase 6 |

### Deferred surfaces

Full mempool policy, Core wallet RPC, fee estimator quality, mining GBT: **absent by design** on this track.
