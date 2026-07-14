# rbitcoin

Production-oriented Bitcoin full node in Rust:

- **rust-bitcoin** types and protocol primitives at the edges
- **libbitcoin-class** concurrent relational mmap archive for chain storage
- **Post-IBD durability** (epoch finalize + tip wire ring) per [`libbitcoin-durable-archive-variant.md`](./libbitcoin-durable-archive-variant.md)
- **Descriptor wallets** with Bitcoin Core–compatible RPC/CLI (no legacy BDB wallets)
- **No pruning, no GUI**
- **100% line and branch coverage**, driven by high-level functional/integration tests

See [`IMPLEMENTATION-PLAN.md`](./IMPLEMENTATION-PLAN.md) for the full roadmap.

## Status

Phase 0 / early Phase 1: workspace, documentation, node lifecycle, and store primitives.

## Build

This environment uses Nix for the toolchain:

```bash
nix-shell
cargo build --workspace
cargo test --workspace
./scripts/coverage.sh   # requires cargo-llvm-cov; enforces 100% line+branch
```

## Crate map

| Crate | Role |
|-------|------|
| `rbitcoin-primitives` | Shared types / newtypes |
| `rbitcoin-store` | mmap tables, heads, epochs |
| `rbitcoin-query` | Domain query API over the store |
| `rbitcoin-wire-cache` | Tip wire-format block ring |
| `rbitcoin-consensus` | Validation / confirmability |
| `rbitcoin-net` | P2P |
| `rbitcoin-mempool` | Policy mempool |
| `rbitcoin-wallet` | Descriptor wallets |
| `rbitcoin-rpc` | JSON-RPC |
| `rbitcoin-cli` | CLI client |
| `rbitcoin-node` | Node binary |
| `rbitcoin-test` | High-level test harness |

## License

MIT OR Apache-2.0 (see `LICENSE-MIT` and `LICENSE-APACHE`).
