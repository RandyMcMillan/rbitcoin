# IBD memory: intentional caches vs process leaks

This document is the developer/AI contract for **process-owned** memory on the
IBD path. It is **not** about kernel page cache under store mmaps (those count
in RSS when faulted but are not Rust heap leaks).

## Intentional, bounded structures (do not gut)

| Structure | Cap / bound | Production clear / evict |
|-----------|-------------|---------------------------|
| **Archive queue budget** | default 512 MiB (`RBITCOIN_ARCHIVE_QUEUE_MB`) | `ArchiveQueueBudget::charge` on first body enqueue; **`release` only via** `apply_archive_result` on `ArchiveResult::{Ok,Err,Dropped}` |
| **ContigPark** | horizon `CONTIG_DENSIFY_AHEAD` (2048 heights) | `take_contiguous` → writer; abort via `drain_all` + Err results; skip already-Class-A via `force_advance` + **Dropped** |
| **`BodyPresence.archive_charged`** | one bit per in-flight archive body | **Only** `clear_archive_charged` on pipeline result — **never** hygiene-prune |
| **OutFifo (confirm outs)** | `RBITCOIN_CONFIRM_OUT_FIFO` (default 2²⁴ outs) | FIFO whole-create eviction on insert; tip GC does **not** free outs |
| **Archive txid sticky** | ~4M entries | FIFO + touch recency |
| **Confirm plans / headers** | offer-ahead window | `ConfirmParentCache::advance_tip` from write `post_commit` |
| **SH memtable / runs** | memtable env cap; runs on disk | spill + merge; bulk materialize at tip |
| **Ordered work path** | `MAX_ORDERED_HEADERS` | `IbdWorkState::hygiene` |

Tests that need a clean process must call these **same** entry points (or drop the
owning `Query` / pipeline), not a secret test-only free-all that masks production
leaks.

## Leak classes fixed

### A. Charged archive bodies dropped without `ArchiveResult`

Ownership chain:

1. `Block` handler: `archive_queued.charge(wire)` + `mark_archive_charged`
2. Job lives in ContigPark / prep / writer with decoded `Block` RAM
3. Release **only** when main loop applies `ArchiveResult`

If prep exits on writer death, or `force_advance` discards a parked job, without
emitting a result, **budget bytes and `archive_charged` stick** until process
restart. Mainnet logs showed `arch=1487/512MiB` after writer/probe stalls.

### B. Unbounded `arch_job` + decode wait during `tx.head` resize stall

While the Class A writer **sleeps on probe-exhaust** for online head resize,
prep/forwarder back up. An **unbounded** `arch_job` channel retained every
decoded `Block` for the multi-day stall. Separately, fire-and-forget block
decode acquired the decode semaphore *after* spawn, so multi-MB framed
payloads waited unboundedly for permits.

**Fix:** `ARCH_JOB_QUEUE_CAP` (256) + `try_send` Full → release charge /
`mark_missing`; block readers `acquire_block_decode_permit` before the next
TCP frame; tip-follow pending maps capped.

### Production rules (do not regress)

1. **Every charge has exactly one release path** tied to an `ArchiveResult` (or
   the failed `arch_job_tx.send` immediate release).
2. **Any drop of an `ArchiveJob` after charge** must send `Ok` / `Err` / `Dropped`
   (see `release_remaining_jobs` and `force_advance` → `Dropped`).
3. **`archive_charged` is not hygiene-pruned** — only `clear_archive_charged`.
4. **Do not** “fix” high RSS by shrinking intentional caps (OutFifo, sticky,
   archive budget) without measuring **charge residual** and ContigPark ownership.

### Production helpers (call these — do not re-implement release)

| Helper | Role |
|--------|------|
| `emit_archive_job_err` / `emit_archive_job_dropped` | One charged job → `ArchiveResult` |
| `emit_writer_dead_outcomes` | Writer channel dead: sticky clear + Err per outcome |
| `release_remaining_jobs` | ContigPark + pri/far drain as Err |
| `drain_job_rx_as_err` | Forwarder stop: drain job channel |
| `apply_archive_result` | Main loop: **only** place that `release`s the budget |
| `ARCH_JOB_QUEUE_CAP` + `try_send` Full | Bound decoded Blocks into pipeline; Full → release charge |
| `acquire_block_decode_permit` | Bound multi-MB frames waiting for decode |

### Regression tests (shipped path)

```text
cargo test -p rbitcoin-net --lib contig_park_tests
cargo test -p rbitcoin-net --lib presence_lifecycle
```

Honest coverage (reverting emit/apply / queue bound would fail):

- `arch_job_queue_full_releases_charge` — Full `try_send` releases budget
- `multi_block_ibd_like_growth_then_production_abort_plateau` — N=128 large charges, WriterDead path, plateau budget==0
- `multi_block_park_abort_releases_all_charges` — `emit_writer_dead_outcomes` + `release_remaining_jobs` + `apply_archive_result`
- `drain_job_rx_as_err_releases_via_apply` — forwarder stop drain + apply
- `force_advance_returns_parked_jobs_for_charge_release` — Dropped emit + apply
- `sticky_map_stays_at_cap_under_unique_flood`

## Process RSS vs true leak

| Observation | Interpretation |
|-------------|----------------|
| `arch=` climbs while ContigPark/pending hold bodies, then falls on Ok/Err | Working budget (may overshoot while getdata in flight) |
| `arch=` stays ≫ budget after pipeline stop / writer death | **Leak** — missing result/release |
| `bodies=` near OutFifo cap, oscillates | Intentional cache fill |
| `sh_runs` grows during Direct IBD | On-disk runs; bulk materialize at tip |
| High `RssFile` with stable anon heap | Mmap page cache — not a Rust leak |

Host check:

```bash
grep -E 'VmRSS|RssAnon|RssFile' /proc/$PID/smaps_rollup
# and IBD perf: arch=…/…MiB pending=… contig parked=…
```

## Agent / reviewer checklist

When changing IBD archive, assign, body presence, or confirm parent cache:

- [ ] New retain path has a **cap** or **tip/abort clear** using production APIs
- [ ] Charge/release pairs stay symmetric (add a unit test if you touch the meter)
- [ ] Abort / stop / WriterDead paths drain owned jobs with results
- [ ] Tests do not clear caches via a different code path than production
- [ ] Do not silence “high RSS” by deleting useful caches

See also: `AGENTS.md` (leak-prevention summary), `docs/store-efficiency-plan.md` §2.2.
