# Testing guide

## Preference order (dev-cycle aware)

| Prefer | Avoid |
|--------|--------|
| **Journey scenarios**: one `/tmp` store, one mature pad, then a **sequence** of asserts (spend, reject, reconstruct, scripthash, …) | Many skinny scenarios that each remine maturity and re-open the store |
| **Pure units** on pure helpers (scriptnum, bits, fuse8, open-hash) with **no store** | Units that re-implement confirm and only paint lines a journey already hits |
| **One entry** per production path (scenario **or** unit next to the shipped fn) | Twin unit + scenario for the same reject string |
| Core JSON corpora for **script engine** breadth | A second parallel script suite |

**Fewer scenario functions / store opens, not less coverage** — put more asserts on one carefully designed multi-stage journey.

Shared helpers live in the `rbitcoin-test` crate (`mine`, `chain_fixture`).

### Third-party deps and compile cost (2026-08)

| Change | Why it helps the cycle |
|--------|------------------------|
| **mimalloc** only on product bins (`rbitcoin-node`, `rbitcoin-cli`) | Store **lib** tests no longer compile `libmimalloc-sys`/`cc`. Production still uses mimalloc on node/cli. |
| **rayon removed** from consensus | Parallel scripts use in-crate `script_pool` (`rbtc-scripts` steal). Drops rayon + crossbeam from the consensus graph. |
| **xorf + bincode + serde** removed from store | Sealed fuse8 is in-tree (`binary_fuse8` + hand LE layout **v2**). Drops a serde-heavy path from store rebuilds. |
| **fuse8 v1 → v2 on open** | Legacy fuse files soft-migrate (always-probe + rewrite from Class A); **do not** wipe `tx.head` for fuse payload-only changes. |

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
# CARGO_TARGET_DIR defaults to target/dev (see Artifact silos below)
cargo build --workspace --all-targets
# Default suite: unit + scenarios + **fast** multi-node only (no --ignored)
cargo test --workspace
./scripts/coverage.sh   # uses target/cov — does not thrash target/dev
# Guard: no libbitcoinconsensus in the dependency graph
cargo tree -i bitcoinconsensus 2>&1 | grep -q 'package ID specification' || \
  (echo "FAIL: bitcoinconsensus still in dependency tree" && cargo tree -i bitcoinconsensus && exit 1)
```

### Artifact silos (do not mix)

Host **gnu** objects are not interchangeable with **musl** release or with
**llvm-cov**-instrumented objects (different triple / profile / RUSTFLAGS).

| Silo | Where | Used by |
|------|--------|---------|
| **Dev** | `target/dev` (`CARGO_TARGET_DIR` from `nix-shell` / `nix develop`) | fmt, clippy, `cargo test`, ad-hoc `cargo build` |
| **Coverage** | `target/cov` (forced in `scripts/coverage.sh`) | `./scripts/coverage.sh` only |
| **Musl release** | Nix store via crane (`cargoArtifacts` + app) | `nix build .#rbitcoin-musl` — **not** `./target` |

Override dev dir only when intentional: `CARGO_TARGET_DIR=… nix-shell`.  
Override coverage dir: `CARGO_TARGET_DIR_COV=… ./scripts/coverage.sh`.

**Default vs heavy tiers**

| Tier | Command | Contents |
|------|---------|----------|
| **Default** (CI / human local full suite) | `cargo test --workspace` | Crate unit tests + scenarios + electrum + consensus_rules + **tier A multi-node IBD** (8-block single-hop + cold reconstruct) + reorg + short IBD error-path smokes. Agents use targeted `-p` tests locally; this suite runs on the PR. |
| **CI multinode job** | same as tier A filters | Required job after fmt/clippy/test (coverage cadence) |
| **Heavy multi-node / IBD** | `./scripts/integration.sh` or `-- --ignored` on `integration_multinode` / `ibd_smoke` | Multi-hop, tip-follow, 48-block dual seeder, mesh, `run_p2p` |
| **Ignored benches** | `cargo test -p rbitcoin-net --test freeze_benches -- --ignored` etc. | Optional perf / contention probes |

### Suite speed budgets (default tier)

**Target:** warm default suite wall **≤3 min** (stretch **&lt;2 min**) on a Linux host comparable to CI / agent VM with a warm `target/`.

**Baseline (agent VM, warm test profile, 2026-08-07):** full `cargo test --workspace` was **~1000 s (~17 min)** before store fan-in scale fixes. After parameterizing fan-in targets and shrinking SH head default benches (`6588b62` era): `rbitcoin-store --lib` serial **~26 s** (was **~498 s**); `sorted_run` module **~1 s** (was **~191 s**). Re-measure package walls when claiming further suite-speed work — do **not** re-run multi-minute full-suite timing loops as a planning spike.

| Package / binary (warm, order-of-magnitude) | Budget | Notes |
|---------------------------------------------|-------:|-------|
| `rbitcoin-store --lib` | **&lt;45 s** | Fan-in reduce tests must use a **tiny** stream target, not production 4096 |
| `rbitcoin-consensus --lib` | **&lt;30 s** | Prefer pure unit over full-store loops when the branch allows |
| `rbitcoin-query --lib` | **&lt;20 s** | |
| `rbitcoin-test --test scenarios` | **&lt;15 s** | Prefer `pad_empty_from` / shared mature helpers |
| **Full** `cargo test --workspace` | **≤3 min** warm | Stretch **&lt;2 min**; ignore-tier IBD stays out |

**New default-suite test rule:** if a new or expanded default test routinely takes **&gt;2 s wall** on a warm tree, the PR must **justify** it (what contract needs that cost, why a smaller N / unit cannot hit the branch). Prefer `#[ignore]` + reason string for true microbenches / host-only forensics.

**Do not pin production-scale constants in default unit fixtures** when a smaller N still exercises the code path:

| Anti-pattern | Prefer |
|--------------|--------|
| `n = FANIN_TARGET_STREAM_RUNS + ε` (~4k run files) | Pass a tiny `target_stream_runs` into reduce; keep 4096 geometry in pure math tests only |
| Multi‑GiB / mainnet head scale under `cargo test` | `RBITCOIN_HEAD_SCALE=tiny` / `cfg(test)` default; force mainnet only for explicit scale tests |
| Remining 100-block maturity pads with `confirm_wire_run` | `pad_empty_from` / `build_mature_regtest_with_spend` once per store |
| Wall-time multi-round microbenches in default suite | Deterministic structure / chunk-load asserts; demote wall arms to `#[ignore]` |

**Tier A timeouts:** `two_node_header_and_block_sync` 60s wall; `serve_after_restart_via_reconstruct` 90s wall. Confirm pipeline queue depths use saturating counters so teardown races cannot panic on overflow. Heavier paths remain `#[ignore]` (`scripts/integration.sh`).

**Speed / reliability (default suite):** prefer `pad_empty_from` / `build_mature_regtest_with_spend` over remine pads; SH run-builder sleeps are 1 ms under `cfg(test)` (40 ms in production). `pin_compose_multi_pack_timed` keeps functional + layout/covered short-circuit gates (multi-ms floor); sticky vs cold assemble is log-only (not a hard timing assert). Schema-13 wire rebuild must stamp create identity from `txid.body` — zero batch identity is treated as missing (regression covered by `reconstruct_and_connect_error_arms` + multi-vout confirm scenarios). Coverage vs speed: prefer **one** scenario at the real entry over N micro-opens that only paint lines; when adding coverage for reduce/materialize, use a **tiny** target, not production stream depth.

### Coverage notes

- Coverage uses **`target/cov`**, separate from **`target/dev`**, so instrumented
  rebuilds do not invalidate plain test/clippy artifacts (and the reverse).
- Default coverage is **incremental** (no `llvm-cov clean`) so repeat runs reuse the instrumented tree.
- Force a cold instrumented rebuild: `COVERAGE_CLEAN=1 ./scripts/coverage.sh`
- Gate: LCOV line coverage **≥ 90%** (`LH`/`LF` from `./scripts/coverage.sh`).

### Mature-chain fixtures

Electrum hub tests and `MempoolHub` accept harnesses use
`rbitcoin_consensus::pad_empty_from` for coinbase-maturity pads (not a local
`1..=103` POW remine loop).

Do **not** re-mine a 100-block maturity pad with per-height `confirm_wire_run`. Use:

```rust
use rbitcoin_test::{build_mature_regtest_with_spend, pad_empty_from};
// Full mature chain + one spend (accept path):
let chain = build_mature_regtest_with_spend(&query, &params);
// Or pad heights from_h..=last with accept_and_connect only:
let (tip, tip_time) = pad_empty_from(&query, &params, tip, tip_time, 2, maturity);
```

## Scenario catalog

Prefer **one high-level scenario** per behavior cluster. Delete lower-level tests when a newer scenario covers the same production paths.

| ID | Layer | Description |
|----|-------|-------------|
| `node_cli_and_surface_smoke` | Lifecycle/CLI | Networks, config errors, CLI flags (incl. log-level/mempool/electrum/inhibit), params, net surface |
| `three_stage_confirm_and_parent_pin_surface` | Consensus+query | Split load→scripts→write; parent pin; load ready timeout/cancel |
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
| `electrum_server_version_history_balance` | Electrum | Protocol fixture: version, history, balance, headers |
| `electrum_more_methods_and_errors` | Electrum | ping/features/block headers/listunspent/tx get+merkle/fees + error paths |
| `two_node_header_and_block_sync` | P2P (**default / multinode CI**) | Seeder → peer 8-block IBD |
| `serve_after_restart_via_reconstruct` | P2P (**default / multinode CI**) | Cold serve via reconstruct |
| `reorg_to_longer_branch` | P2P/chain (default) | Most-work reorg (hub only — no IBD hang risk) |
| `three_node_relay_path` | P2P (**ignored**) | Hop serve — `scripts/integration.sh` |
| `ibd_skips_dead_peer` | P2P (**ignored**) | Dial book skips dead address |
| `ibd_two_peers` | P2P (**ignored**) | Dual-seeder 48-block IBD |
| `tip_follow_after_ibd` / `tip_follow_getheaders_*` / `ibd_to_tip_tracking_*` | P2P (**ignored**) | Tip follow / relay |
| `node_run_p2p_short` | Node (**ignored**) | Full `run_p2p` entry |
| `multinode_mesh_periodic` | P2P (**ignored**) | Larger mesh |

Removed (covered by the rows above): `confirm_cross_block_prevout_without_tx_head`,
`double_archive_keeps_tx_height_for_coinbase_maturity`, `mega_batch_duplicate_header_is_idempotent`,
`archive_local_prev_fk_and_reconstruct`.

### Integration / multi-node

Default CI + required **multinode** job run tier A `integration_multinode` cases (not `--ignored` heavies).
Heavy topology is `#[ignore]` and run periodically:

```bash
./scripts/integration.sh   # default multinode + --ignored
# or only heavy:
cargo test -p rbitcoin-test --test integration_multinode -- --ignored --nocapture
```

New features: add a high-level scenario; remove obsolete lower-level tests in the same PR.

## Core differential

Inventory for Bitcoin Core **v31.1** functional tests lives in
[`scripts/core-functional/`](scripts/core-functional/)
([`docs/core-functional.md`](docs/core-functional.md)).
`python3 scripts/core-functional/check_inventory.py` is the completeness
gate. `run.sh` may only invoke inventory `run` names (`feature_help.py`
and `feature_uacomment.py` today).
The nightly job (`.github/workflows/core-functional.yml` →
`scripts/core-functional/nightly.sh`) warns — it does not fail — when a
newer Bitcoin Core release exists than the inventory pin. Label
**`core-functional`** on harness PRs. Default `cargo test` does **not**
invoke Core’s Python suite.

```bash
python3 scripts/core-functional/check_inventory.py
./scripts/core-functional/check_inventory_test.sh
./scripts/core-functional/sync-core-fixtures.test.sh
./scripts/core-functional/run.sh.test.sh
./scripts/core-functional/run.sh --list
./scripts/core-functional/run.sh feature_help.py feature_uacomment.py
./scripts/core-functional/bitcoind.test.sh
./scripts/core-functional/check_core_release.test.sh
# cargo test stages Core JSON from the submodule:
./scripts/core-functional/init-submodule.sh
./scripts/core-functional/sync-core-fixtures.sh --check
```

## Fault injectors

Optional `integration-testing` cargo feature on crates that need crash points (e.g. mid-finalize). Off by default in release builds used for production packaging; **on** in CI test builds when needed for coverage.
