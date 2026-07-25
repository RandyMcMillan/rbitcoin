# Test coverage inventory (speed-suite consolidation)

Goal: reduce `cargo test --workspace` wall time by merging **internal-only**
duplicate unit surfaces into fewer tests, without dropping external-observable
or consensus-edge coverage. Prefer scenarios listed in [`TESTING.md`](../TESTING.md).

## Keep (do not merge away)

| Class | Why | Where |
|-------|-----|--------|
| Consensus rule matrix | Each rule must fail if inverted | `docs/consensus-tests.md`, `structure_rule_tests`, `consensus_rules` binary |
| Scenario catalog | External multi-crate paths | `rbitcoin-test` scenarios / electrum / multinode |
| Store structural | Layout/encoding/concurrency edges unique to Class A/C | `rbitcoin-store` unit tests |
| Mainnet/signet script fixtures | Real-block consensus | `rbitcoin-consensus/tests/signet_*`, `tx_core_vectors` |
| Diagnostic benches | Already `#[ignore]` | `reader_contention_*`, `freeze_bench_*` (except helpers smoke) |

## Merged (this pass) — successor map

| Removed / folded unit tests | Successor |
|-----------------------------|-----------|
| `out_fifo::{fifo_evicts_*, replace_in_place_*, pin_lookup_*}` | `ConfirmParentCache::out_fifo_pin_and_dense_surface` + scenario `three_stage_confirm_and_parent_pin_surface` |
| `BatchParents::{put_and_get_roundtrip, pin_covered_*, parent_entry_has_no_create_height_*}` | `BatchParents::insert_layout_coinbase_and_covered` |
| `ConfirmParentCache::{out_fifo_survives_*, body_create_resolves_*, out_fifo_keeps_*, out_fifo_cap_*, out_fifo_bounds_*, pin_batch_*, get_bodies_for_pin_batch_slims_*, put_dense_*}` | `out_fifo_pin_and_dense_surface` (cap eviction + dense pin layout) |
| `progress::{pct_basic, pct_tip_above_horizon, pct_zero_horizon, tip_hole_*, eta_*}` (8 → 2) | `pct_tip_hole_and_format_surface`, `tip_rate_tracker_eta_surface` |
| `confirm::{claim_feed_*, note/requeue/finish, queue_depth_*, caps}` | `claim_feed_wave_and_skip_confirmed`, `feed_note_requeue_finish_surface`, `queue_depth_log_and_caps_surface` |
| `coalesce::*` (5 → 1) | `batch_and_coalesce_wait_surface` |
| `exit::*` (5 → 1) | `exit_and_catchup_complete_surface` |
| `body::{pending_*, archived_*, missing_*, known_*, hygiene_*, unknown_*, mark_archived_*, demote_*, rejected_*, archive_charged_*}` (11 → 2) | `presence_lifecycle_surface`, `rejected_and_archive_charged_surface` |
| `assign_plan::{far_slots, zero_feed_scale, header_soft_cap, remove_*, compact_*}` (7 → 2) | `assign_policy_helpers_surface`, `ordered_set_remove_and_compact_surface` |
| `events::{zero_hash_reject, prevout_spent_reject, real_script_reject}` | `confirm_reject_blacklist_surface` |
| `confirm_run::{filter_keeps_*, three_stage_entry_points_exist, scripts_phase_is_pure_*}` | `three_stage_write_filter_and_scripts_surface` |

## Intentionally not merged

- **Consensus structure S1–S12 / header H* / connect C*** — one test per rule (matrix in `consensus-tests.md`).
- **Store encode/roundtrip/concurrency** — unique failure modes; not covered by scenarios.
- **IBD archive/assign budget regressions** — production log regressions with distinct messages.
- **Plan/tip watermark tests on `ConfirmParentCache`** (`ensure_plans_skips_*`, `advance_tip_prunes_*`) — distinct from FIFO pin path.

## Timing

Logs under `/tmp/grok-goal-d363ff7fb95d/implementer/` (`time -p` real/user/sys).

| Run | real (s) | Notes |
|-----|----------|--------|
| `test-baseline.log` | 440 | Mid-merge tree; aborted on store concurrent FD race (`rbitcoin-store --lib` SIGABRT) |
| `test-after.log` | 491 | Full tree after consolidations; same store concurrent abort mid-lib |
| `test-after-warm-nostore.log` | **61** | Warm `--workspace --exclude rbitcoin-store` — all **285** non-store tests ok |
| `test-store-serial.log` | 13 | store lib threads=1: flaky `put_batch_publish_visible_to_concurrent_readers` (passes on retry alone) |

### Unit-test count reduction (lib suites)

| Crate | Before (approx) | After |
|-------|-----------------|-------|
| `rbitcoin-net` | ~100 (+8 ignored) | **66** (+8 ignored) |
| `rbitcoin-query` | ~40 | **28** |
| `rbitcoin-consensus` | ~101 | **99** |

Consensus **structure/header/connect** matrix and scenario catalog unchanged.
Store concurrent flakiness is pre-existing (not touched this pass).
