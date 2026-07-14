# Testing guide

## Preference order

1. **Multi-process / full node** — binary + datadir + RPC/CLI/P2P
2. **Subsystem integration** — store+query+consensus; net harness with mock peers
3. **Fault injection** through public APIs / `integration-testing` features
4. **Unit tests** — last resort only

Shared helpers live in the `rbitcoin-test` crate.

## Running tests

```bash
nix-shell
cargo test --workspace
./scripts/coverage.sh
```

## Scenario catalog

Prefer **one high-level scenario** per behavior. Delete lower-level tests when a newer scenario covers the same production paths.

| ID | Layer | Description |
|----|-------|-------------|
| `node_lifecycle_and_networks` | Lifecycle | Networks + primitives smoke |
| `cli_and_node_entrypoints` | CLI | Flags, smoke, fault injector |
| `store_error_and_corrupt_paths` | Store | Error/corrupt surfaces (coverage) |
| `chain_connect_reorg_and_growth` | Query | Synthetic chain + disconnect |
| `consensus_regtest_genesis_and_mine_chain` | Consensus | rust-bitcoin mine + accept + reopen |
| `consensus_reject_bad_pow_and_merkle` | Consensus | Invalid block rejection |
| `consensus_spend_and_reject_double_spend` | Consensus | Prevout spend + double-spend |
| `consensus_milestone_skips_connect_checks` | Consensus | Milestone path |
| `two_node_header_and_block_sync` | P2P integration | Seeder → peer headers+blocks |
| `three_node_relay_path` | P2P integration | Sync via intermediate peer |
| `multinode_mesh_periodic` | P2P integration (ignored) | Larger mesh; `scripts/integration.sh` |

### Integration / multi-node

Default CI runs `integration_multinode` without `--ignored`.

Periodic / holistic suite:

```bash
./scripts/integration.sh
# or: cargo test -p rbitcoin-test --test integration_multinode -- --ignored --nocapture
```

New features: add a high-level scenario; remove obsolete lower-level tests in the same PR.

## Core differential

Not yet wired. Future: pin bitcoind version, run shared RPC scripts on regtest against both nodes.

## Fault injectors

Optional `integration-testing` cargo feature on crates that need crash points (e.g. mid-finalize). Off by default in release builds used for production packaging; **on** in CI test builds when needed for coverage.
