# rbitcoin

Bitcoin full node in Rust (active track: **blocks-only consensus + IBD + block relay + Electrum serve**):

- **rust-bitcoin** types and protocol primitives at the edges
- **libbitcoin-class** concurrent relational mmap archive for chain storage
- **Historical blocks via reconstruct** from the archive (tip wire ring only for non-finalized soft zone)
- **Post-IBD durability** (epoch finalize + tip wire ring) per [`libbitcoin-durable-archive-variant.md`](./libbitcoin-durable-archive-variant.md)
- **Electrum protocol** clients (confirmed history; in-process server — planned)
- **No pruning, no GUI**
- **Deferred:** full mempool/policy, fee estimation quality, descriptor wallets, mining GBT
- **100% executable line coverage** via high-level functional/integration tests

See [`IMPLEMENTATION-PLAN.md`](./IMPLEMENTATION-PLAN.md) for the re-audited roadmap, gap table, and phases (including Electrum).

## Status

**Phases 0–3 complete** (store chain ops, regtest consensus, multi-node P2P headers/blocks).

**Next: Phase 4** — `reconstruct_block`, store-backed serve after restart, multi-peer IBD, mainnet consensus depth, long-running node. Then tip follow → durability + scripthash index → Electrum server → hardening.

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
| `rbitcoin-store` | mmap tables, heads (+ scripthash index later) |
| `rbitcoin-query` | Domain query API; reconstruct (Phase 4) |
| `rbitcoin-wire-cache` | Tip wire-format block ring |
| `rbitcoin-consensus` | Validation / confirmability |
| `rbitcoin-net` | P2P (headers + blocks, blocks-only) |
| `rbitcoin-electrum` | Electrum TCP/SSL server (Phase 7) |
| `rbitcoin-rpc` | Minimal node JSON-RPC (Phase 8) |
| `rbitcoin-cli` | CLI client |
| `rbitcoin-node` | Node binary |
| `rbitcoin-test` | High-level test harness |

## License

MIT OR Apache-2.0 (see `LICENSE-MIT` and `LICENSE-APACHE`).
