# rbitcoin

Bitcoin **full node** in Rust aimed at **production server-side** use: multi-peer
IBD, tip follow, block/tx relay (tip mode), and in-process Electrum for **wallet
backends** and similar infrastructure — built around a **libbitcoin-class
relational archive** and a **pure-Rust consensus/script** path.

> **0.x:** on-disk format and APIs are **unstable until 1.0**. Prefer a **signet
> soak** before first mainnet cutover; treat early mainnet as high-scrutiny.
> Security contact and policy: [`SECURITY.md`](./SECURITY.md). Operator notes:
> [`docs/experimental-mainnet.md`](./docs/experimental-mainnet.md).

| | |
|--|--|
| **License** | MIT OR Apache-2.0 ([`LICENSE-MIT`](./LICENSE-MIT), [`LICENSE-APACHE`](./LICENSE-APACHE)) |
| **Version** | 0.1.0 experimental ([`CHANGELOG.md`](./CHANGELOG.md)) |
| **Security** | [`SECURITY.md`](./SECURITY.md) |
| **Design** | [`docs/architecture.md`](./docs/architecture.md) — why this node is different |

## Why this node is different

Most full nodes center a **UTXO set + block files** (Bitcoin Core). Most Electrum
backends are **external indexers** of another node. rbitcoin does neither:

1. **On-disk archive** — **map-free** Class A/B/C tables (pread/pwrite + fallocate
   grow; kernel page cache as L0): packed txs, keyless `tx.head`, spend
   annotations, native scripthash. Historical blocks are **reconstructed** from
   the archive; tip keeps a **wire ring** and Class C tip durability after catch-up.
   Layout: [`SCHEMA.md`](./SCHEMA.md); IO modality: [`docs/io-modality.md`](./docs/io-modality.md);
   concurrency: [`docs/concurrency.md`](./docs/concurrency.md).
2. **Concurrent IBD / IO** — fixed writer roles (one Class A appender),
   allocate-then-publish HWMs (no map epochs), confirm as **lookup → load →
   scripts → write**, bulk **io_uring** where available (pread/pwrite fallback).
   Map: [`docs/concurrency.md`](./docs/concurrency.md).
3. **Pure-Rust consensus** — structure, connect, and **script verification in
   Rust**; only **secp256k1** (via rust-bitcoin) as the crypto primitive — **no**
   `libbitcoinconsensus` dual-eval. Tests: [`docs/consensus-tests.md`](./docs/consensus-tests.md).

Full narrative and Core / Fulcrum contrasts: **[`docs/architecture.md`](./docs/architecture.md)**.
Product surface: [`COMPAT.md`](./COMPAT.md).

## Status

Core pipelines exist (store, consensus, P2P IBD, tip follow, scripthash,
Electrum, libre mempool) for the **server-side / wallet-backend** role. **0.x
mainnet** is early production: run **signet first**, then mainnet with
monitoring ([`OPERATOR.md`](./OPERATOR.md)). Finishing any one operator’s first
full mainnet sync is **not** a gate for using or packaging this tree.

**Authorship:** first-party code is **AI-written** (Grok / xAI) under
**Brandon Black** ([@reardencode](https://github.com/reardencode)) prompting —
details in [`SECURITY.md`](./SECURITY.md).

**Milestone (default mainnet 840000):** at/below `--milestone`, **script/sig
checks are skipped** on block connect (assumevalid-style speed tradeoff).
Prevouts, double-spend, maturity, and fees still run. Use **`--milestone 0`**
for full script validation.

```bash
# Portable static release (preferred)
nix build .#rbitcoin-musl
install -m 755 result/bin/rbitcoin-node result/bin/rbitcoin-cli target/release/

# Signet lab (time-boxed)
./target/release/rbitcoin-node --datadir ./datadir-signet --network signet \
  --listen 127.0.0.1:38333 --milestone 200000 --max-run-secs 120
```

## Build

### Portable static release (recommended)

Pinned **nixpkgs + Cargo.lock** produce a **fully static, portable**
`rbitcoin-node` / `rbitcoin-cli` (musl) that runs on ordinary Linux hosts without
Nix or a matching glibc. Byte-identical digests for a given revision + target.
Not NixOS-specific — any machine with [Nix](https://nixos.org/download/) + flakes:

```bash
nix build .#rbitcoin-musl          # default package; fully static
# or: ./scripts/repro-build.sh
install -m 755 result/bin/rbitcoin-node result/bin/rbitcoin-cli target/release/
./scripts/repro-build.sh           # day-to-day musl install (crane-layered)
./scripts/repro-check.sh           # release only: two clean rebuilds; compare digests
```

Do **not** use `cargo build --release` inside `nix-shell` / `nix develop` as the
operator binary — that links against the Nix store glibc and fails outside the
store (`No such file or directory` at exec). Details:
[`docs/reproducible-builds.md`](./docs/reproducible-builds.md).

### Dev / CI path

Requires a recent Rust toolchain (workspace `rust-version` 1.74+). Prefer the
**same pin** as release builds for tests and clippy:

```bash
nix develop   # or: nix-shell  (both use flake.lock, not floating <nixpkgs>)
cargo build --workspace
cargo test --workspace
./scripts/coverage.sh   # PR bar: see CONTRIBUTING.md
```

Operator binary: always the static install under `./target/release/` (or
`./result/bin/`). Operator knobs: [`OPERATOR.md`](./OPERATOR.md). Experimental
mainnet: [`docs/experimental-mainnet.md`](./docs/experimental-mainnet.md).

## Crate map

| Crate | Role |
|-------|------|
| `rbitcoin-primitives` | Shared types / newtypes |
| `rbitcoin-store` | Map-free Class A/B/C tables (fd pread/pwrite), scripthash, bulk IO |
| `rbitcoin-query` | Domain API (archive, confirm, reconstruct, Electrum joins) |
| `rbitcoin-wire-cache` | Tip wire-format block ring |
| `rbitcoin-consensus` | Validation / confirm; pure-Rust scripts; milestone = scripts only |
| `rbitcoin-net` | P2P + IBD (modular `ibd/`), tip follow, relay |
| `rbitcoin-mempool` | Cluster graph + libre admission |
| `rbitcoin-electrum` | Electrum TCP server |
| `rbitcoin-rpc` | Minimal node JSON-RPC (stub) |
| `rbitcoin-cli` | CLI client |
| `rbitcoin-node` | Node binary |
| `rbitcoin-test` | High-level test harness |

## Documentation index

| Doc | Audience |
|-----|----------|
| [`docs/architecture.md`](./docs/architecture.md) | Design uniqueness (start here) |
| [`docs/experimental-mainnet.md`](./docs/experimental-mainnet.md) | Lab mainnet runbook |
| [`OPERATOR.md`](./OPERATOR.md) | Day-to-day ops, env knobs |
| [`SCHEMA.md`](./SCHEMA.md) | On-disk schema |
| [`docs/concurrency.md`](./docs/concurrency.md) | Writer roles / lock-free publish |
| [`COMPAT.md`](./COMPAT.md) | Intentional differences vs Core / Electrum methods |
| [`CONTRIBUTING.md`](./CONTRIBUTING.md) | Dev workflow and coverage bar |
| [`SECURITY.md`](./SECURITY.md) | Vulnerability reporting |
| [`CHANGELOG.md`](./CHANGELOG.md) | Release notes |
| [`docs/reproducible-builds.md`](./docs/reproducible-builds.md) | Pinned Nix byte-identical builds |

## What this is not

- Production multi-tenant Electrum or “drop-in Core”
- Wallet, mining, GUI, or pruning
- Full Core JSON-RPC surface
- A claim of complete mainnet script validation under the **default** milestone

## License

Licensed under either of:

- Apache License, Version 2.0 ([`LICENSE-APACHE`](./LICENSE-APACHE) or
  http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([`LICENSE-MIT`](./LICENSE-MIT) or
  http://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions. See
[`CONTRIBUTING.md`](./CONTRIBUTING.md).
