# rbitcoin

**Experimental** Bitcoin full node in Rust: multi-peer IBD, tip follow, block/tx relay (tip mode), and in-process Electrum.

- **rust-bitcoin** types and protocol primitives at the edges
- **libbitcoin-class** concurrent relational mmap archive for chain storage
- **Historical blocks via reconstruct** from the archive (tip wire ring for soft zone)
- **Post-IBD durability** (epoch finalize + tip wire ring) per [`libbitcoin-durable-archive-variant.md`](./libbitcoin-durable-archive-variant.md)
- **Electrum** TCP (confirmed + unconfirmed when mempool attached); TLS via reverse proxy
- **Libre-class mempool** (cluster, full RBF, 0.1 sat/vB min; **scripts verified on accept**)
- **BIP152 compact blocks (v2)** + **BIP339 wtxidrelay** on tip-follow sessions
- **No** pruning, GUI, wallet, or mining

**Not** a production Bitcoin Core or Fulcrum replacement. See [`docs/experimental-mainnet.md`](./docs/experimental-mainnet.md).

## Status

Core pipelines exist (store, consensus, P2P IBD, tip follow, scripthash, Electrum). **Mainnet is experimental** — signet lab first ([`OPERATOR.md`](./OPERATOR.md)).

**Milestone (default mainnet 840000):** at/below `--milestone`, **script/sig checks are skipped** on block connect (assumevalid-style speed tradeoff). Prevouts, double-spend, maturity, and fees still run. Use **`--milestone 0`** for full script validation.

```bash
# Signet lab (time-boxed)
cargo build -p rbitcoin-node --release
./target/release/rbitcoin-node --datadir ./datadir-signet --network signet \
  --listen 127.0.0.1:38333 --milestone 200000 --max-run-secs 120
```

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
| `rbitcoin-store` | mmap Class A/B/C tables, scripthash, epoch |
| `rbitcoin-query` | Domain API (archive, confirm, reconstruct, Electrum joins) |
| `rbitcoin-wire-cache` | Tip wire-format block ring |
| `rbitcoin-consensus` | Validation / confirm; milestone = scripts only |
| `rbitcoin-net` | P2P + IBD (modular `ibd/`), blocks-only |
| `rbitcoin-electrum` | Electrum TCP server |
| `rbitcoin-rpc` | Minimal node JSON-RPC (stub) |
| `rbitcoin-cli` | CLI client |
| `rbitcoin-node` | Node binary |
| `rbitcoin-test` | High-level test harness |

## License

MIT OR Apache-2.0 (see `LICENSE-MIT` and `LICENSE-APACHE`).
