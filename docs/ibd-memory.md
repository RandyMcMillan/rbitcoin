# IBD memory: intentional caches vs process leaks

This document is the developer/AI contract for **process-owned** memory on the
IBD path. It is **not** about kernel page cache under store mmaps (those count
in RSS when faulted but are not Rust heap leaks).

## Primary IBD wire path (current)

| Structure | Cap / bound | Production clear / evict |
|-----------|-------------|---------------------------|
| **In-RAM body queue** | Soft densify assign (no hysteresis): under ~100 MiB free densify ahead; over ~100 MiB only heights confirm will consume in the next ~1 min at tip rate. Optional absolute ceiling via `RBITCOIN_BLOCK_QUEUE_GB` / `_BYTES` (default unlimited) | Peer **BlockFramed** enqueues **raw** frame payload (no full Block decode on peer); confirm pack **decodes** by height; confirm-write **dequeues** after tip advance. **RAM-only by design** — avoids double-writing every block (queue + Class A); restart empties the queue (redownload). Logs: `bq soft=n/win RAM=`. |
| **Body densify height horizon** | `CONTIG_DENSIFY_AHEAD` (64 k past tip) | Safety max walk/receive; primary gate is soft assign (100 MiB free / 1 min confirm window). |
| **Confirm feed** | readiness (height/hash), no wire retain | Plan **packs** tip-contiguous runs by decoding BQ wire one height at a time until soft **input** budget (`RBITCOIN_CONFIRM_BATCH_INPUTS`, default **8000**, overshoot block included) or hard **144** blocks. At dense mainnet heights **8000 inputs ≈ a few blocks** (often 1–3; early chain can pack many tiny blocks up to 144). Requeue / finish on outcome |

## Soft budgets (unified body-queue path)

Peers enqueue **raw** framed block payloads into the **in-RAM** body queue (no
peer full-block decode). Confirm pack is the sole wire decode. Confirm commit is
the sole Class A appender (**no** dual-track archive-job / ContigPark pipeline).

**Why RAM (not disk):** writing peer wire to a durable queue and again into Class
A would **double disk write every block**. Process memory + redownload on restart
is the deliberate tradeoff. Accept stores raw wire only (block hash already known
from framing); full parse / txid calculation stays on the confirm pack path so we
do not hold both a decoded `Block` and the wire bytes.

| Structure | Cap / bound | Production clear / evict |
|-----------|-------------|---------------------------|
| **Archive queue budget** | default 512 MiB (`RBITCOIN_ARCHIVE_QUEUE_MB`) | Soft densify / far-scale meter only; **no** job charge/release on the unified path (without a charger it stays empty — in-RAM BQ soft depth is the primary densify gate) |
| **Pipeline pins (no process FIFO)** | Plan `batch_pin` / `BatchParents` / plan-local external parents only | Drop with batch. Cold denserels for ancient parents use Class A into plan-local maps |
| **ConfirmParentCache header plans** | tip-GC window | Always on — required for multi-block wire MTP |
| **Confirm plans / headers** | offer-ahead window | `ConfirmParentCache::advance_tip` from write `post_commit` |
| **SH memtable / runs** | memtable env cap; runs on disk | spill + merge; bulk materialize at tip |
| **Ordered work path** | `MAX_ORDERED_HEADERS` | `IbdWorkState::hygiene` |

Tests that need a clean process must call these **same** entry points (or drop the
owning `Query` / pipeline), not a secret test-only free-all that masks production
leaks.

## Soft budgets: request-limited only (invariant)

**We never stop accepting block data a peer sends for a block we already
requested** just because body-queue soft depth (or any other soft meter)
is over target.

| Allowed | Forbidden |
|---------|-----------|
| Limit **densify getdata assign** when BQ payload is over ~100 MiB to heights confirm will consume in the next ~1 min at tip rate | Await a soft gate **before** the next TCP read on a peer |
| Free densify ahead while BQ payload is under ~100 MiB | Drop a body we already received solely for soft budget |
| Overshoot soft limits while in-flight requests complete | Make healthy peers look stalled by parking the reader on soft backpressure |
| Accept all in-flight bodies into the RAM queue via `block_queue_offer` (soft assign limits are ignored on offer) | Bound process RAM by refusing peer bytes already on the wire |

**Why this is safe:** when soft assign restricts densify to the confirm-time
window, outstanding requests remain finite (per-peer in-flight window).
Enqueueing those bodies cannot create a truly unbounded leak; the backlog
drains as confirm dequeues. Bound queue size by **not requesting**, not by
**not reading**.

Historical regression (do not reintroduce): bounded arch_job Full-drop and
reader-side decode-permit wait before the next frame made peers look dead while
TCP buffers filled. Dual-track `ArchiveJob` + ContigPark charge/release is
**retired** — do not reintroduce a second Class A path for unknown-height bodies.

## Process RSS vs true leak

| Observation | Interpretation |
|-------------|----------------|
| `bq RAM=` climbs while tip lags, falls as confirm dequeues | Working in-RAM queue (counts toward RSS/anon) |
| `conf_plans=` grows with tip-ahead headers | Intentional ConfirmParentCache Arc header plans (tip GC) |
| `conf … parents=` | Sum of `BatchParents` entries in prepq + writeq (pipeline meter only; no writeq parent budget) |
| `sh_runs` grows during Direct IBD | On-disk runs; bulk materialize at tip |
| High `RssFile` with stable anon heap | Mmap page cache — not a Rust leak |

Host check / in-process:

Every ~5s IBD emits **`ibd: sizes`** (INFO) with process RSS and occupancy of
known retain structures:

| Token group | What it meters |
|-------------|----------------|
| `rss=` `anon=` `file=` `hwm=` | `/proc` process RSS (anon vs mmap file pages) |
| `work` / `body` | IBD maps + body-presence sets |
| `bq soft=n/win RAM=` | In-RAM body-queue count vs 1-min confirm window at tip rate + heap MiB |
| `conf_plans` / bq / conf pipe | Header plans + body-queue + confirm pipeline sizes (no process pin FIFO) |
| `conf planq` / `prepq` / `writeq` | Confirm pipeline **queue contents** (batches, blocks, wire MiB) + pipeline-wide `parents=` + feed ready/inflight |
| `txhead` | Segmented `tx.head.*` (open head + sealed heads/fuses; logical sizes) |
| `sh` | SH runs / memtable / tip heads |
| `heap … iflight= pstore= sh_mt= accounted= residual=` | Approx process heap: BQ + prep-ahead CreatePins + parent-store live pins + SH memtable + confirm wire; residual = anon − accounted |

Grep:

```bash
grep 'ibd: sizes' mainnet.log
```
