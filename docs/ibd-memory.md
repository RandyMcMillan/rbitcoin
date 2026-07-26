# IBD memory: intentional caches vs process leaks

This document is the developer/AI contract for **process-owned** memory on the
IBD path. It is **not** about kernel page cache under store mmaps (those count
in RSS when faulted but are not Rust heap leaks).

## Intentional, bounded structures (do not gut)

| Structure | Cap / bound | Production clear / evict |
|-----------|-------------|---------------------------|
| **Archive queue budget** | default 512 MiB (`RBITCOIN_ARCHIVE_QUEUE_MB`) | `ArchiveQueueBudget::charge` on first body enqueue; **`release` only via** `apply_archive_result` on `ArchiveResult::{Ok,Err,Dropped}` (or immediate release if pipeline send fails because the channel is **closed**) |
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

## Soft archive budget: request-limited only (invariant)

**We never stop reading or decoding block data a peer sends for a block we
already requested** just because the decoded body would push us over the soft
archive queue budget (or any other soft queue meter).

| Allowed | Forbidden |
|---------|-----------|
| Stop **new densify / cache getdata** when `!can_assign` (fill ≥ budget) | Await a decode permit / soft gate **before** the next TCP read on a peer |
| Scale far densify via `far_admission_scale` | `try_send` Full → drop a decoded `Block` we already received |
| Overshoot budget while in-flight getdata completes | Make healthy peers look stalled by parking the reader on archive backpressure |
| Unbounded `arch_job` channel (main → ContigPark) | Bound process RAM by refusing peer bytes already on the wire |

**Why this is safe:** when archive stops accepting new work, assign stops issuing
getdata. Outstanding requests are finite (per-peer in-flight window). Decoding
and enqueueing those bodies cannot create a truly unbounded leak; the backlog
drains when the writer resumes. Bound queue size by **not requesting**, not by
**not reading**.

Historical regression (do not reintroduce): `ARCH_JOB_QUEUE_CAP` + Full-drop and
`acquire_block_decode_permit` before the next frame made peers look dead while
TCP buffers filled, and was not the real memory-leak fix (charge release on
WriterDead / ContigPark abort was).

## Leak classes fixed

### A. Charged archive bodies dropped without `ArchiveResult`

Ownership chain:

1. `Block` handler: `archive_queued.charge(wire)` + `mark_archive_charged`
2. Job lives in ContigPark / prep / writer with decoded `Block` RAM
3. Release **only** when main loop applies `ArchiveResult`

If prep exits on writer death, or `force_advance` discards a parked job, without
emitting a result, **budget bytes and `archive_charged` stick** until process
restart. Mainnet logs showed `arch=1487/512MiB` after writer/probe stalls.

### Production rules (do not regress)

1. **Every charge has exactly one release path** tied to an `ArchiveResult` (or
   the failed `arch_job_tx.send` immediate release when the **pipeline is closed**).
2. **Any drop of an `ArchiveJob` after charge** must send `Ok` / `Err` / `Dropped`
   (see `release_remaining_jobs` and `force_advance` → `Dropped`).
3. **`archive_charged` is not hygiene-pruned** — only `clear_archive_charged`.
4. **Do not** “fix” high RSS by shrinking intentional caps (OutFifo, sticky,
   archive budget) without measuring **charge residual** and ContigPark ownership.
5. **Do not** reintroduce receive-side backpressure (bounded arch_job Full-drop,
   reader-side decode-permit wait) to “fix” soft-budget overshoot.

### Production helpers (call these — do not re-implement release)

| Helper | Role |
|--------|------|
| `emit_archive_job_err` / `emit_archive_job_dropped` | One charged job → `ArchiveResult` |
| `emit_writer_dead_outcomes` | Writer channel dead: sticky clear + Err per outcome |
| `release_remaining_jobs` | ContigPark + pri/far drain as Err |
| `drain_job_rx_as_err` | Forwarder stop: drain job channel |
| `apply_archive_result` | Main loop: **only** place that `release`s the budget (except closed-pipeline send fail) |
| `ArchiveQueueBudget::can_assign` | **Only** gate for new densify/cache block requests |

### Regression tests (shipped path)

```text
cargo test -p rbitcoin-net --lib contig_park_tests
cargo test -p rbitcoin-net --lib presence_lifecycle
```

Honest coverage (reverting emit/apply would fail):

- `multi_block_ibd_like_growth_then_production_abort_plateau` — WriterDead path, plateau budget==0
- `multi_block_park_abort_releases_all_charges` — `emit_writer_dead_outcomes` + `release_remaining_jobs` + `apply_archive_result`
- `drain_job_rx_as_err_releases_via_apply` — forwarder stop drain + apply
- `force_advance_returns_parked_jobs_for_charge_release` — Dropped emit + apply
- `can_assign_stops_at_budget_charge_may_overshoot` — request gate only; charge may overshoot
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
- [ ] Soft budget is enforced by **request** gates only — never by stalling TCP
      read/decode or Full-dropping already-received blocks

See also: `AGENTS.md` (leak-prevention summary), `docs/store-efficiency-plan.md` §2.2.
