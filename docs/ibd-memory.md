# IBD memory: intentional caches vs process leaks

This document is the developer/AI contract for **process-owned** memory on the
IBD path. It is **not** about kernel page cache under store mmaps (those count
in RSS when faulted but are not Rust heap leaks).

## Primary IBD wire path (current)

| Structure | Cap / bound | Production clear / evict |
|-----------|-------------|---------------------------|
| **Durable body queue** | **Soft time-depth** ~5 min of tip-rate blocks on disk (hysteresis resume &lt;4 min); optional absolute byte ceiling via `RBITCOIN_BLOCK_QUEUE_GB` / `_BYTES` (default unlimited) | Peer **offer** wire always to disk; confirm prep **reads** by height; confirm-write **dequeues** after tip advance. Files get `POSIX_FADV_DONTNEED` after durable write (idle on disk — not process heap). **Restart rehydrate uses index meta only** (`list_meta`) — never `load_all`. No process RAM overflow for BQ. Logs: `bq n=` + `disk=` + `soft=n/stop`. |
| **Body densify height horizon** | `CONTIG_DENSIFY_AHEAD` (64 k past tip) | Safety max walk/receive; primary stop is **soft time-depth**. Gaps inside on-disk max height always densify even under pressure. |
| **Confirm feed** | readiness (height/hash), no wire retain | Plan **packs** tip-contiguous runs by decoding BQ wire one height at a time until soft **input** budget (`RBITCOIN_CONFIRM_BATCH_INPUTS`, default **8000**, overshoot block included) or hard **144** blocks; requeue / finish on outcome |

## Soft budgets / fallback archive job (not the primary Class A path)

Unknown-height bodies and abort/charge release still use an archive-job
pipeline + ContigPark. On the **unified** path, peers do **not** dual-append
Class A far ahead of tip — confirm commit is the sole Class A appender.

| Structure | Cap / bound | Production clear / evict |
|-----------|-------------|---------------------------|
| **Archive queue budget** | default 512 MiB (`RBITCOIN_ARCHIVE_QUEUE_MB`) | Soft-charge **RAM overflow / arch jobs only** (not multi‑GiB disk body queue). `charge` on overflow/job; **`release` only via** `apply_archive_result` on `ArchiveResult::{Ok,Err,Dropped}` (or immediate release if pipeline send fails because the channel is **closed**) |
| **ContigPark** | horizon `CONTIG_DENSIFY_AHEAD` | Fallback contiguous park → prep/writer; abort via `drain_all` + Err; skip already-Class-A via `force_advance` + **Dropped** |
| **`BodyPresence.archive_charged`** | one bit per charged fallback body | **Only** `clear_archive_charged` on pipeline result — **never** hygiene-prune |
| **CreateResidency (sole pin map)** | **Default lean:** 256k creates / 1M outs (`NO_CACHE_*`). Override with `RBITCOIN_CREATE_RESIDENCY_CAP` / `_OUT_CAP` (legacy `RBITCOIN_CONFIRM_OUT_FIFO`). **`RBITCOIN_CONFIRM_CACHE=1`** restores multi‑GiB 8M/16M caps + denserels prewarm | **Insert-order FIFO** only (no read-LRU). Lean default keeps **in-flight** res_seed/pin; cold denserels use OS page cache. Late denserels_hit% may be low while tip rate holds — do not chase hit% with multi‑GiB caps |
| **ConfirmParentCache header plans** | tip-GC window | Always on — required for multi-block wire MTP; not controlled by `CONFIRM_CACHE` |
| **Confirm plans / headers** | offer-ahead window | `ConfirmParentCache::advance_tip` from write `post_commit` |
| **SH memtable / runs** | memtable env cap; runs on disk | spill + merge; bulk materialize at tip |
| **Ordered work path** | `MAX_ORDERED_HEADERS` | `IbdWorkState::hygiene` |

Tests that need a clean process must call these **same** entry points (or drop the
owning `Query` / pipeline), not a secret test-only free-all that masks production
leaks.

## Soft budgets: request-limited only (invariant)

**We never stop reading or decoding block data a peer sends for a block we
already requested** just because the decoded body would push us over the durable
body-queue byte budget, soft archive RAM budget, or any other soft meter.

| Allowed | Forbidden |
|---------|-----------|
| Stop **frontier densify getdata** when soft BQ depth &gt; ~5 min tip-rate blocks (or archive soft `!can_assign`) | Await a decode permit / soft gate **before** the next TCP read on a peer |
| Always fill **gaps** within on-disk height span under pressure | Drop a decoded `Block` we already received solely for soft budget |
| Overshoot soft depth while in-flight / gap fill completes | Make healthy peers look stalled by parking the reader on soft backpressure |
| Accept all in-flight bodies onto durable disk | Bound process RAM by refusing peer bytes already on the wire |

**Why this is safe:** when either budget stops accepting new densify work, assign
stops issuing getdata. Outstanding requests are finite (per-peer in-flight
window). Decoding and enqueueing those bodies cannot create a truly unbounded
leak; the backlog drains as confirm dequeues (or the fallback writer resumes).
Bound queue size by **not requesting**, not by **not reading**.

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
4. **Do not** “fix” high RSS by shrinking intentional caps (CreateResidency,
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

## Process RSS vs true leak

| Observation | Interpretation |
|-------------|----------------|
| `arch=` climbs while ContigPark/pending hold bodies, then falls on Ok/Err | Working budget (may overshoot while getdata in flight) |
| `arch=` stays ≫ budget after pipeline stop / writer death | **Leak** — missing result/release |
| `residency creates=` near create cap, oscillates | Intentional CreateResidency fill |
| `sh_runs` grows during Direct IBD | On-disk runs; bulk materialize at tip |
| High `RssFile` with stable anon heap | Mmap page cache — not a Rust leak |

Host check / in-process:

Every ~5s IBD emits **`ibd: sizes`** (INFO) with process RSS and occupancy of
known retain structures:

| Token group | What it meters |
|-------------|----------------|
| `rss=` `anon=` `file=` `hwm=` | `/proc` process RSS (anon vs mmap file pages) |
| `work` / `body` | IBD maps + body-presence sets |
| `body_soft` / `contig` | Soft archive RAM + ContigPark |
| `bq disk=` / `soft=n/stop` | **Disk** payload size + soft densify count target (do not equate disk MiB with RSS) |
| `residency` | **Sole** pin map: creates/outs vs caps + conf_plans + `cache=on|off` |
| `conf planq` / `prepq` / `writeq` | Confirm pipeline **queue contents** (batches, blocks, wire MiB, parents) + feed ready/inflight |
| `txhead` | Segmented `tx.head.*` (open head + sealed heads/fuses; logical sizes) |
| `sh` | SH runs / memtable / tip heads |

Grep:

```bash
grep 'ibd: sizes' mainnet.log
# Compare rss=/anon= growth to structure counts — RSS up while sizes flat ⇒
# untracked retain or RssFile (mmap page cache / dual head during resize).
grep -E 'VmRSS|RssAnon|RssFile' /proc/$PID/smaps_rollup
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
