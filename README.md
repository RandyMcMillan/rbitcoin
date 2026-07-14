# rbitcoin

Bitcoin full node in Rust (active track: **consensus + IBD + block relay**):

- **rust-bitcoin** types and protocol primitives at the edges (integrating next)
- **libbitcoin-class** concurrent relational mmap archive for chain storage
- **Post-IBD durability** (epoch finalize + tip wire ring) per [`libbitcoin-durable-archive-variant.md`](./libbitcoin-durable-archive-variant.md)
- **No pruning, no GUI**
- **Deferred:** mempool, transaction relay, fee estimation, wallets
- **100% executable line coverage** via high-level functional/integration tests

See [`IMPLEMENTATION-PLAN.md`](./IMPLEMENTATION-PLAN.md) for the roadmap and code review of the current tree.

## Status

Phase 0 complete / Phase 1 next: store chain ops (confirmed, strong_tx, growable indexes), then consensus + P2P IBD.

## Build

```bash
nix-shell
cargo build --workspace
cargo test --workspace
./scripts/coverage.sh
```

## Crate map

| Crate | Role |
|-------|------|
| `rbitcoin-primitives` | Shared types / newtypes |
| `rbitcoin-store` | mmap tables, heads, epochs |
| `rbitcoin-query` | Domain query API over the store |
| `rbitcoin-wire-cache` | Tip wire-format block ring |
| `rbitcoin-consensus` | Validation / confirmability |
| `rbitcoin-net` | P2P (headers + blocks) |
| `rbitcoin-rpc` | Node JSON-RPC (later; no wallet) |
| `rbitcoin-cli` | CLI client |
| `rbitcoin-node` | Node binary |
| `rbitcoin-test` | High-level test harness |

## License

MIT OR Apache-2.0 (see `LICENSE-MIT` and `LICENSE-APACHE`).
