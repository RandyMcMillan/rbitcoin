# Testing guide

## Preference order

1. **Multi-process / full node** — binary + datadir + RPC/CLI/P2P
2. **Subsystem integration** — store+query+consensus; net harness with mock peers
3. **Fault injection** through public APIs / `integration-testing` features
4. **Unit tests** — last resort only

Shared helpers live in the `rbitcoin-test` crate (`mine`, `chain_fixture`).

## Running tests

```bash
nix-shell
cargo test --workspace
./scripts/coverage.sh
```

### Coverage notes

- Default coverage is **incremental** (no `llvm-cov clean`) so repeat runs reuse the instrumented target dir.
- Force a cold instrumented rebuild: `COVERAGE_CLEAN=1 ./scripts/coverage.sh`
- Gate: HTML report has **0** `uncovered-line` markers on first-party sources.

### Mature-chain fixtures

Do **not** re-mine a 100-block maturity pad in every scenario. Use:

```rust
use rbitcoin_test::build_mature_regtest_with_spend;
let chain = build_mature_regtest_with_spend(&query, &params);
// assert spend, reconstruct samples, etc. on the same chain
```

## Scenario catalog

Prefer **one high-level scenario** per behavior cluster. Delete lower-level tests when a newer scenario covers the same production paths.

| ID | Layer | Description |
|----|-------|-------------|
| `node_cli_and_surface_smoke` | Lifecycle/CLI | Networks, config errors, CLI flags, params, net surface |
| `store_error_and_corrupt_paths` | Store | Error/corrupt surfaces |
| `store_table_header_and_idx_corrupt` | Store | Table header/head corrupt open |
| `chain_connect_reorg_and_growth` | Query | Synthetic growth + disconnect (rehash) |
| `consensus_mature_chain_spend_and_reconstruct` | Consensus+query | **One** mature mine: spend, double-spend, reopen reconstruct |
| `consensus_reject_bad_structure_and_milestone` | Consensus | Bad merkle/prev + milestone skip |
| `scripthash_index_history_balance_and_reorg` | Query | Electrum index + reorg spend clear |
| `wire_ring_and_archive_epoch` | Wire/epoch | Multi-tip ring + finalize soft zone |
| `electrum_server_version_history_balance` | Electrum | Protocol fixture: version, history, balance, headers |
| `two_node_header_and_block_sync` | P2P | Seeder → peer |
| `serve_after_restart_via_reconstruct` | P2P | Cold serve via reconstruct |
| `three_node_relay_path` | P2P | Hop serve |
| `sync_from_peers_tries_list` | P2P | Multi-peer try list (fallback) |
| `parallel_ibd_two_peers` | P2P | Concurrent windowed download from 2 peers |
| `tip_follow_after_ibd` | P2P | Tip announce follow |
| `reorg_to_longer_branch` | P2P/chain | Most-work reorg |
| `node_run_p2p_short` | Node | Long-running entry short run |
| `multinode_mesh_periodic` | P2P (ignored) | Larger mesh; `scripts/integration.sh` |

### Integration / multi-node

Default CI runs `integration_multinode` without `--ignored`.

```bash
./scripts/integration.sh
# or: cargo test -p rbitcoin-test --test integration_multinode -- --ignored --nocapture
```

New features: add a high-level scenario; remove obsolete lower-level tests in the same PR.

## Core differential

Not yet wired. Future: pin bitcoind version, run shared RPC scripts on regtest against both nodes.

## Fault injectors

Optional `integration-testing` cargo feature on crates that need crash points (e.g. mid-finalize). Off by default in release builds used for production packaging; **on** in CI test builds when needed for coverage.
