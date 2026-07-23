# Testing guide

## Preference order

1. **Multi-process / full node** — binary + datadir + RPC/CLI/P2P
2. **Subsystem integration** — store+query+consensus; net harness with mock peers
3. **Fault injection** through public APIs / `integration-testing` features
4. **Unit tests** — last resort only

Shared helpers live in the `rbitcoin-test` crate (`mine`, `chain_fixture`).

## Diagnostic examples (ad-hoc, not CI)

One-off script/script-failure probes live as **crate examples** (run with
`cargo run -p CRATE --example NAME -- …`). Prefer these over pasting ad-hoc
main files into the tree.

| Example | Crate | Purpose |
|---------|-------|---------|
| `diag_tip182692` | `rbitcoin-test` | Signet tip stall / PrevoutSpent on a store path |
| `diag_block_script` | `rbitcoin-consensus` | Which script fails in a wire block (signet API prevouts) |
| `diag_mainnet_block` | `rbitcoin-consensus` | Mainnet block script probes (blockstream API) |
| `diag_fail` / `diag_fail_tx` / `diag_sh` | `rbitcoin-consensus` | Historical script failure forensics |
| `dump_wit` | `rbitcoin-consensus` | Dump witness stack from a local block bin |

IBD progress/rejects belong in node logs (`ibd: confirm reject`, `ibd: archive reject`);
do not re-home those into examples.

## Running tests

```bash
nix-shell
# Warnings are errors (workspace.lints + RUSTFLAGS=-Dwarnings in shell.nix)
cargo build --workspace --all-targets
cargo test --workspace
./scripts/coverage.sh
# Guard: no libbitcoinconsensus in the dependency graph
cargo tree -i bitcoinconsensus 2>&1 | grep -q 'package ID specification' || \
  (echo "FAIL: bitcoinconsensus still in dependency tree" && cargo tree -i bitcoinconsensus && exit 1)
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
| `node_cli_and_surface_smoke` | Lifecycle/CLI | Networks, config errors, CLI flags (incl. log-level/mempool/electrum/inhibit), params, net surface |
| `three_stage_confirm_and_parent_mlock_surface` | Consensus+query | Split load→scripts→write; parent pin/mlock; load ready timeout/cancel |
| `block_cache_and_mempool_hub_surface` | Net | BlockCache locator/eviction + MempoolHub accept/remove/reorg on mature chain |
| `store_error_and_corrupt_paths` | Store | Error/corrupt surfaces |
| `store_table_header_and_idx_corrupt` | Store | Table header/head corrupt open |
| `chain_connect_reorg_and_growth` | Query | Synthetic growth + disconnect (rehash) |
| `consensus_mature_chain_spend_and_reconstruct` | Consensus+query | **One** mature mine: spend, local prev_fk, double-spend, reopen reconstruct |
| `ibd_parallel_archive_idempotent_confirm_without_tx_head` | Query+consensus | Out-of-order archive, re-archive idempotent, head-off prevout+maturity |
| `resume_head_off_warms_cache_for_external_prev` | Query+consensus | Resume head-off: warm Class A cache fixes external-prev missing prevout |
| `consensus_reject_bad_structure_and_milestone` | Consensus | Bad merkle/prev + milestone skip |
| `consensus_rules` (test binary) | Consensus | Focused reject paths for structure/header/connect rules we own — see [`docs/consensus-tests.md`](./docs/consensus-tests.md) |
| `scripthash_index_history_balance_and_reorg` | Query | Electrum index + reorg spend clear |
| `wire_ring_and_archive_epoch` | Wire/epoch | Multi-tip ring + finalize soft zone |
| `electrum_server_version_history_balance` | Electrum | Protocol fixture: version, history, balance, headers |
| `electrum_more_methods_and_errors` | Electrum | ping/features/block headers/listunspent/tx get+merkle/fees + error paths |
| `two_node_header_and_block_sync` | P2P | Seeder → peer |
| `serve_after_restart_via_reconstruct` | P2P | Cold serve via reconstruct |
| `three_node_relay_path` | P2P | Hop serve |
| `ibd_skips_dead_peer` | P2P | IBD dial book skips dead address |
| `ibd_two_peers` | P2P | Multi-peer windowed IBD download |
| `tip_follow_after_ibd` | P2P | Tip announce follow |
| `reorg_to_longer_branch` | P2P/chain | Most-work reorg |
| `node_run_p2p_short` | Node | Long-running entry short run |
| `multinode_mesh_periodic` | P2P (ignored) | Larger mesh; `scripts/integration.sh` |

Removed (covered by the rows above): `confirm_cross_block_prevout_without_tx_head`,
`double_archive_keeps_tx_height_for_coinbase_maturity`, `mega_batch_duplicate_header_is_idempotent`,
`archive_local_prev_fk_and_reconstruct`.

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
