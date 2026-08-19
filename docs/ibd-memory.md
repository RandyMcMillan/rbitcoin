# IBD memory: intentional caches vs process leaks

This document is the developer/AI contract for **process-owned** memory on the
IBD path. It is **not** about kernel page cache under FdOnly store files
(those count in RSS when faulted but are not Rust heap leaks).

## Primary IBD wire path (current)

| Structure | Cap / bound | Production clear / evict |
|-----------|-------------|---------------------------|
| **In-RAM body queue** | Soft densify assign (no hysteresis): under ~100 MiB free densify ahead; over ~100 MiB only heights confirm will consume in the next ~1 min at tip rate. Optional absolute ceiling via `RBITCOIN_BLOCK_QUEUE_GB` / `_BYTES` (default unlimited). `bytes()` is **raw only** | Peer **BlockFramed** enqueues **raw** frame payload (no full Block decode on peer); lookup **takes** the row (dequeue) after decode. Decoded `Arc<Block>` + `TxPrecompute` live on **loadq** (cap 14), then scriptq/writeq. **Have-body** (hole / densify / receive) is confirmed ∨ BQ hash ∨ `H ≤ lookup_taken_hi`. **Never both** raw and decoded. **RAM-only by design**. Restart empties BQ+loadq. Logs: `bq soft=n/win RAM=` `loadq=n/14`. |
| **Body densify height horizon** | `CONTIG_DENSIFY_AHEAD` (64 k past tip) | Safety max walk/receive; primary gate is soft assign (100 MiB free / 1 min confirm window). |
| **Confirm feed** | readiness (height/hash), no wire retain | **Load** packs tip-contiguous runs by decoding BQ wire one height at a time until soft **input** budget (hardcoded **8000**, overshoot block included) or hard **144** blocks. At dense mainnet heights **8000 inputs ≈ a few blocks** (often 1–3; early chain can pack many tiny blocks up to 144). Do not treat ~32 as pack size. IBD **lookup** TipOnly-resolves at most **64000** inputs or **1080** BQ-ready heights per wave. Hard **min 8000** inputs when more unresolved heights remain (`ready=0` included). Also holds a short wave while `ready` is over half the 1-min BQ window, unless the first unresolved height is in the load-facing half of that window **and** the collect is already ≥8000. Requeue / finish on outcome |

## Soft budgets (unified body-queue path)

Peers enqueue **raw** framed block payloads into the **in-RAM** body queue (no
peer full-block decode). Lookup is the first decode: it runs `TxPrecompute::from_tx`,
**`take_raw`** (row gone), and sends `ResolvedWire` on loadq. Load stamp takes
that same `pres` Arc (no second `from_tx`; do not re-stash on the BQ).
Confirm commit is the sole Class A appender (**no** dual-track archive-job /
ContigPark pipeline).

**Why RAM (not disk):** writing peer wire to a durable queue and again into Class
A would **double disk write every block**. Process memory + redownload on restart
is the deliberate tradeoff. Accept stores raw wire only (block hash already known
from framing). After lookup processes a height we hold the decoded `Block` +
pres and **not** the raw bytes. Reorg gather that wants wire re-encodes.

| Structure | Cap / bound | Production clear / evict |
|-----------|-------------|---------------------------|
| **Published identity union** | Per-wave parent maps still on the BQ (`ArcSwap` of the layer-chain head; get walks, no union rebuild) | Lookup drops a layer once no height in its span remains on the BQ. Disconnect stores `None`. Not a process FIFO. |
| **Pipeline pins (no process FIFO)** | Plan `batch_pin` / `BatchParents` only | Drop with batch. Cold **outs** for ancient parents use `txout.body` into `BatchParents` (stamped range) |
| **RecentCreates identity ring** | `txid → fk+range` only; expire EWMA(`lookup_taken_hi − tip`)+25% (floor 32, cap 32×144) | Write notes per height then **one** `flush_recent_creates` (and expire) after Class A+idx; disconnect `drop_from` + flush. Sizes: `recent=… live=/pub=/ov= fifo=`. Not a pin/outs FIFO |
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
| `conf … parents=` | Sum of `BatchParents` entries in scriptq + writeq (pipeline meter only; no writeq parent budget) |
| `sh_runs` grows during Direct IBD | On-disk runs; bulk materialize at tip |
| High `RssFile` with stable anon heap | Mmap page cache — not a Rust leak |
| `fuse8=` ≈ 9 bits × sealed Class A | In-RAM sealed membership filters — intentional; not a leak |
| `class_c_l2=` ≈ creates/8 | Strong-tx bit image under the Class C in-RAM cap |

Host check / in-process:

Every ~5s IBD emits **`ibd: sizes`** (INFO) with process RSS and occupancy of
known retain structures:

| Token group | What it meters |
|-------------|----------------|
| `rss=` `anon=` `file=` `hwm=` | `/proc` process RSS (anon vs mmap file pages) |
| `work` / `body` | IBD maps + body-presence sets |
| `bq soft=n/win RAM=` | In-RAM body-queue count vs 1-min confirm window at tip rate + heap MiB (**raw only**) |
| `conf_plans` / bq / conf pipe | Header plans + body-queue + confirm pipeline sizes (no process pin FIFO) |
| `conf loadq=` / `scriptq` / `writeq` | Real queue contents (loadq cap 8) + pipeline-wide `parents=` + feed ready/inflight |
| `txhead` | Segmented `tx.head.*` (open head + sealed heads/fuses; logical sizes) |
| `sh` | SH runs / memtable / tip heads |
| `heap … iflight= pstore= recent= union= h2h= fence= sh_mt= fuse8= open_keys= class_c_l2= accounted= residual=` | Approx process heap: BQ + load-ahead CreatePins + parent-store live pins + **recent-create identity ring** (`recent=Nh live=/pub=/ov= fifo=≈NMiB`; one published Arc + overlay + fifo) + **PublishedIds/LiveUnion layers** (`union=NL/Nk`) + `height_by_hash` + height fence + SH memtable + confirm wire + **sealed `tx.head` fuse8 fingerprints** + open-segment fuse-key Vec + Class C L2 images; residual = anon − accounted |

## Residual heap audit (872k / ~1.42 B creates)

`ibd: sizes` at `class_a≈1.416B` (mainnet.log, 2026-08-13) showed
`anon≈2.2 GiB` vs `accounted≈13 MiB` (`residual≈2.2 GiB`). That gap was a
**meter hole**, not an unbounded leak. The missing retain is almost all
intentional:

| Retain | Approx at 1.42 B creates | Notes |
|--------|-------------------------:|-------|
| **Sealed `tx.head` fuse8** | **~1.5–1.6 GiB** | `open_file` loads every sealed `.fuse8` fingerprint array into process RAM (~9 bits/key). `file=` stays ~6 MiB because heads are FdOnly. |
| **Class C L2 `strong_tx`** | **~177 MiB** | 1 bit/create, under the 256 MiB in-RAM cap. |
| **Open-segment `open_keys`** | **~100–200 MiB** | `Vec<u64>` fuse keys for the unsealed tail. |
| **`height_by_hash`** | **~60 MiB** | Query comment; still unmetered. |
| **Process baseline** | **~90 MiB** | Visible at genesis (`class_a=476`, `residual≈93`). Allocator arenas, rustc runtime, net. |

Meters `fuse8=` / `open_keys=` / `class_c_l2=` now enter `accounted`. After a
host restart on this build, residual should drop to a few hundred MiB
(baseline + `height_by_hash` + allocator slack). **Do not add a 64–128 MiB
process txid→fk map until that post-meter residual is confirmed on the host.**
The fuse RAM is the real heap cost of segmented heads; a second map is only
justified if `head_fk` is still the pole after Steps 1–8.

Grep:

```bash
grep 'ibd: sizes' mainnet.log
```

## Hard RAM (page-cache working set)

Process heap (BQ + L2 Class C + pins + mempool) is **a few GiB**. The
**hard** requirement is kernel page cache for the files each mode actually
touches. Census: [`SCHEMA.md`](../SCHEMA.md) (tip 962298, 1.42 B creates).

| Mode | Must stay hot | Approx | Cold (fault OK) |
|------|---------------|--------|-----------------|
| **Tip follow / Electrum serve** | Open `tx.head` + recent `txout`/`spent`/`txid` tails + SH main idx + mempool | **8–16 GiB** page cache + **~2–3 GiB** process | `inwit` (except `getrawtransaction`), sealed `tx.head` older than fuse-skip, archive `txout` |
| **Comfortable serve** (busy wallets, Cake, RPC reconstruct) | Above + more `txout` + SH body slabs + `txid.body` | **16–32 GiB** | `inwit` except rawtx |
| **IBD pin+annotate (no thrash)** | **All** `txout` + **all** `spent` + three `*.idx` + `txid.body` + `tx.head` | **~227 GiB** | **`inwit` (~486 GiB)** — wire still holds witness |
| **IBD + reconstruct/getdata** | Previous + `inwit` | **~710 GiB** (same order as old packed `tx.body`) | — |
| **SH tip materialize** | Sliced k-way: n-cpu workers, 256 KiB double-buffered pages on the TLS completion session (submit ahead, wait on promote), no temp pack bodies; ingest OA **~128 MiB** (2²²×32 B) | **≪1 GiB** extra heap | No 0.5–1 GiB OA image per shard |

Packed schema 13/14 needed the whole **`tx.body` (~663 GiB)** hot for the same
pin/annotate work. Split Class A drops that to **~161 GiB** (`txout`+`spent`)
plus idx/identity. A **16 GiB** host can tip-follow (OPERATOR §16 GiB) but IBD
parent pin will be **disk-bound** on `txout`/`spent`.
