# Test coverage inventory (speed-suite consolidation)

Goal: reduce `cargo test --workspace` wall time by merging **internal-only**
duplicate unit surfaces and consolidating integration **binaries** that share the
same link cost, without dropping external-observable or consensus-edge asserts.

Prefer scenarios listed in [`TESTING.md`](../TESTING.md).

## Keep (do not merge away)

| Class | Why | Where |
|-------|-----|--------|
| Consensus rule matrix | Each rule must fail if inverted | `docs/consensus-tests.md`, `structure_rule_tests`, `consensus_rules` binary |
| Scenario catalog | External multi-crate paths | `rbitcoin-test` scenarios / electrum / multinode |
| Store structural | Layout/encoding/concurrency edges unique to Class A/C | `rbitcoin-store` unit tests |
| Script/signet/mainnet fixtures | Real-block consensus edges (hashes, opcodes, verify) | `rbitcoin-consensus/tests/script_edge_fixtures.rs` + `tx_core_vectors.rs` |
| Diagnostic benches | Already `#[ignore]` | `reader_contention_*`, `freeze_bench_*` (except helpers smoke) |

## Merged — successor map

### Query pin / FIFO (public cache API observables)

| Removed / folded unit tests | Successor |
|-----------------------------|-----------|
| `out_fifo::{fifo_evicts_*, replace_in_place_*, pin_lookup_*}` | `ConfirmParentCache::out_fifo_pin_and_dense_surface` (hard eviction, replace-in-place via re-put same Fk, pin slim + later vout re-pin, multi-create **bounds** `total_outs ≤ cap`) + scenario `three_stage_confirm_and_parent_pin_surface` |
| `BatchParents::{put_and_get_roundtrip, pin_covered_*, parent_entry_has_no_create_height_*}` | `BatchParents::insert_layout_coinbase_and_covered` |
| `ConfirmParentCache::{out_fifo_survives_*, body_create_resolves_*, out_fifo_keeps_*, out_fifo_cap_*, out_fifo_bounds_*, pin_batch_*, get_bodies_for_pin_batch_slims_*, put_dense_*}` | `out_fifo_pin_and_dense_surface` — **restored**: slim then re-pin remaining vout; dense all-vouts still addressable after slim; bounds loop; replace-in-place; hard 2-create eviction |

### Net pure helpers / IBD policy (internal or log-token)

| Removed / folded | Successor |
|------------------|-----------|
| `progress::{pct_*, tip_hole_*, eta_*}` (8 → 2) | `pct_tip_hole_and_format_surface`, `tip_rate_tracker_eta_surface` |
| `confirm::{claim_feed_*, note/requeue/finish, queue_depth_*, caps}` | `claim_feed_wave_and_skip_confirmed`, `feed_note_requeue_finish_surface`, `queue_depth_log_and_caps_surface` |
| `coalesce::*` (5 → 1) | `batch_and_coalesce_wait_surface` |
| `exit::*` (5 → 1) | `exit_and_catchup_complete_surface` |
| `body::*` (11 → 2) | `presence_lifecycle_surface`, `rejected_and_archive_charged_surface` |
| `assign_plan::*` (7 → 2) | `assign_policy_helpers_surface`, `ordered_set_remove_and_compact_surface` |
| `events::{zero_hash_reject, prevout_spent_reject, real_script_reject}` | `confirm_reject_blacklist_surface` |

### Consensus

| Removed / folded | Successor |
|------------------|-----------|
| `confirm_run::{filter_keeps_*, three_stage_entry_points_exist, scripts_phase_is_pure_*}` | `three_stage_write_filter_and_scripts_surface` + scenario `three_stage_confirm_and_parent_pin_surface` |
| Separate integration binaries `signet_*.rs`, `mainnet_290329_*.rs` | **One** binary `script_edge_fixtures.rs` — **same** 10 tests / asserts (hashes, opcodes, verify_job). Build-link only consolidation. |
| `tx_core_vectors.rs` | **Kept** separate (serde_json parse smoke) |

## Intentionally not merged

- **Consensus structure S1–S12 / header H* / connect C*** — one test per rule.
- **Store encode/roundtrip/concurrency** — unique failure modes; not covered by scenarios.
- **IBD archive/assign budget regressions** — production log regressions.
- **Plan/tip watermark** on `ConfirmParentCache` (`ensure_plans_skips_*`, `advance_tip_prunes_*`).

## Pre-existing store flakiness (not introduced here)

Full `cargo test --workspace` can abort or fail on concurrent store tests under parallel load:

- `var_table::put_batch_publish_visible_to_concurrent_readers` (race / timeout)
- occasional `IO Safety violation: owned file descriptor already closed` (SIGABRT)

Timing runs that need a green workspace may pass
`-- --skip put_batch_publish_visible --skip concurrent_readers_during_append`
(or serial store lib). Documented in `{SCRATCH}/test-time-compare.txt`. These skips
are **not** used as an excuse to drop consensus fixtures.

## Timing

See `{SCRATCH}/test-time-compare.txt` and logs under
`/tmp/grok-goal-d363ff7fb95d/implementer/`.
