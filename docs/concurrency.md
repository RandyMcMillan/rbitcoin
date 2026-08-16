# Concurrency model (store / IBD / tip follow)

Short map of who may write which tables. **Format is unstable until 1.0.**

## Roles during IBD

| Role | Threading | Store writes |
|------|-----------|--------------|
| Peer IO (N tasks) | tokio multi-thread | none; decoded blocks **offer body queue only**; note height/hash readiness on confirm feed |
| Confirm **lookup** | 1 OS thread | load wire from **body queue**; structure + stamp create_fk (Class A planned only) |
| Confirm **load** | 1 OS thread | stamp from BQ hits + leftover TipOnly `tx.head`; pin `txout` + assemble |
| Confirm **scripts** | 1 OS thread + 2 coordinators + `rbtc-scripts` steal | **none** — pure CPU |
| Confirm **write** | 1 OS thread | **sole Class A appender** (`txout`+`inwit`+`spent`) + structural + Class C + spend annotate on **`spent.body`** + tip GC; **`block_queue_dequeue_height`**. Class A **never leads tip** (same commit era; no archive-ahead DONTNEED) |
| IBD main loop | 1 tokio task | none (orchestration only) |

**Height-ordered unified pipeline (current):** peer → **body queue** → **lookup** (BQ-ahead TipOnly `head_fk` wave) → **load** (structure + stamp from BQ hits + leftover pending-then-TipOnly + pin `txout` + assemble) → scripts → single commit era. **No** peer→confirm-feed wire retain. **No** hash-only / Class-A-only confirm (bq wire required). Load leftover order is in-flight → pins → BQ hits → **pending (no fence)** → TipOnly (fence-connected). Write drain inserts `tx.head` in parallel with Class C; pending snap stays until **fence extend**. In-flight prune is **leftover-ready** (`covers_fk_span` of the pack's fks). Bodies without a known height are marked missing and re-getdata after the height map is ready — there is **no** dual-track archive-job / ContigPark fallback.

**Load claim pack size:** soft **Σ `tx.input`** budget (hardcoded **8000**; include overshoot block) or hard **144** blocks. Dense mainnet blocks hit the input soft stop after **typically a few blocks** (often 1–3); early tiny blocks may pack many until the hard cap. Do **not** treat ~32 as pack size (that was 8000/250 mid-chain, not fat-era).

**IBD lookup resolve wave:** TipOnly `head_fk` over at most **8** BQ-ready heights (or 64 k keys), then mark those complete for load to claim. Small on purpose so complete slices arrive steadily; raise only after host A/B.

**Tip follow / reorg:** peer wire via `ChainHub::accept_block` / `accept_branch` → `accept_and_connect_block` (same wire load path with cold denserels allowed on the one-shot call). Disconnect keeps Class A archive; re-extension always supplies **wire** from the peer, not hash-only load. **IBD most-work reorg** calls `accept_branch` from the **IBD orchestration task only** — never from confirm lookup/load/scripts/write threads. See [`design-ibd-most-work-reorg.md`](./design-ibd-most-work-reorg.md).

**Wire retained on the pipeline batch only:** lookup/load pull `bitcoin::Block` from the body queue; that wire rides through scripts; **no Class-A wire rebuild**. Split Class A (`txout` / `inwit` / `spent`) is planned once and committed in the write stage.

**Body queue:** process-local **in-RAM** payload FIFO (same shape as the former on-disk queue: id / height / hash / header_fk / payload). **Why RAM:** avoid **double disk write** of every block (queue then Class A); accept **redownload on restart** and peak RAM of soft depth. **Primary capacity is soft densify assign** (no hysteresis): under ~100 MiB free densify ahead; over ~100 MiB only heights confirm will consume in the next ~1 min at tip rate. Optional absolute byte ceiling via `RBITCOIN_BLOCK_QUEUE_GB` / `_BYTES` (default unlimited). Height horizon (`CONTIG_DENSIFY_AHEAD`, 64 k past tip) caps densify/receive walk. **Offer** on peer Block → RAM; load **reads** by height; **dequeue** after confirm-commit. Restart starts empty (legacy `store/block_queue/` is best-effort removed).

**Pipeline pins:** plan `batch_pin` / `BatchParents` / plan-local **sparse** `external_parent_outs` only (no process create FIFO). External staging maps are **frozen/cleared after pin** (`ArchiveWritePlan::freeze_after_pin`) so write batch only concatenates commit halves. `SharedParentPin` publishes immutable outs/layout halves via `arc_swap::ArcSwap` (compose + RCU; no in-place mutation). `BatchParents` sticky-caches the last outs Arc for multi-input assemble. IBD `pin(... adopt= range_fill= contract= publish=)` names residual pin wall. ConfirmParentCache holds tip-ahead **Arc** header plans only (insert/replace/drop under tip GC).

**tx.head (segmented):** see [`heads.md`](./heads.md). Lookup: live pin by
txid → hot (open + ages ≤3) → ID/idx → cold (ages ≥4) if needed.
The 2-wave split is sealed age, not an IO flag.

**Datadir secret (schema 12):** `store/store.secret` CSPRNG at create. XOR scripts/witness at rest; keyed TXID mix for heads.

## Roles after IBD / tip follow

| Role | Notes |
|------|--------|
| `peer_session` (split read/write) | Serve reconstruct + accept tip blocks via **`accept_and_connect_block`** → **`confirm_wire_run`** (same load→scripts→commit) |
| Electrum | Read-mostly; joins Class A + scripthash under `Query` |
| Epoch finalize | Single-threaded control path; flushes table maps / fd durability |

## Index modes (`IndexMode`)

| Mode | When | Spentness | Durable `tx.head` / spends | SH |
|------|------|-----------|----------------------------|-----|
| **Direct** | IBD (`enter_direct_index_mode`) | confirmed-strong annotations | commit-stage head insert; spend annotate in same stage | append-only target-sized runs + SEAL → bulk at tip |
| **Tip** | after IBD (`enter_tip_mode`) | confirmed-strong annotations | live heads + confirm spends | durable write-through after bulk |

Do not enter Tip until tip ≈ peer height. Tip entry bulk-materializes SH
(runs → fan-in reduce → durable tables); it does **not** rebuild `tx.head` or spend annotations.

## Locks (exceptions only)

**Default is lock-free** on table hot paths (see `AGENTS.md`):

| Mechanism | What it replaces |
|-----------|------------------|
| Capacity grow (`TableFile`) | No map epochs; fallocate/`set_len` only; readers use published HWM (Acquire) |
| Atomic `count` / HWM | Publish barrier (Acquire readers / Release appender) |
| Role exclusivity | One appender, one annotator — not a global store mutex |
| `tx.head` insert | **Sole writer**: plain Release store empty→relative + SeqCst fence per batch (no CAS). Role exclusivity — not multi-inserter safe |
| `tx.head` segment seal | Synchronous on roll: build fuse8 + mark sealed + open new head (no shadow resize) |
| Process `rehash_gate` | Rare multi‑GiB open-hash rehash (host freeze prevention) |
| `ChainHub::confirmed` | `RwLock<HashSet>` for O(1) `has_block` (IBD assign path) |

There is **no** global “pause queries during confirm write.” Tip-as-commit +
`is_confirmed_strong` define query visibility ([`crash-recovery.md`](./crash-recovery.md)).

## Practical rules

1. Do **not** spawn a second Class A writer while IBD confirm write is running.
2. Pipeline depth: lookup(N+1) ∥ load(N) ∥ scripts(N−1) ∥ write(N−2) via BQ `ready=` + bounded scriptq/writeq.
3. Scripts for batch N may run while load does N+1 and write does N−1. Scripts never touch disk.
4. **Load ahead of store tip:** lookup may stamp batch N+1 while write has not advanced tip.
   Lookup holds a **reserved create-fk HWM** and **in-flight create/out maps** from
   uncommitted plans (`WireLoadPipeline` / `archive_plan_batch_from`). First height
   of a batch is the **pipeline path_lo** (tip+1 or last-loaded+1), not only store tip.
   Write still applies batches in height order; on permanent reject, lookup clears
   reserved state and re-syncs from `txs.count()`.
5. On SIGINT, IBD cancels cooperatively — do not drop nested runtimes mid-await.

## Host freezes / IO storms

Single Class A writer is intentional. Multi‑GiB **FdOnly grow** is fallocate-only
(no remap), but **hash-head rehash** (header / scripthash shards when materializing)
can still stall the **host** (page cache / disk). Class C tip tables use L2
write-behind (`flush_class_c_tip` before BQ dequeue); large tables stay L0.
are small. See **[io-modality.md](./io-modality.md)** for operator IO levers.

### Confirm load read pipeline

Cold parent `txout.idx` / `txout.body` on the **load** thread uses
**FdOnly idx + bulk body** (`idx_body_pipeline` → `bulk_io` uring/pread). Batch
creates come from **wire**, not a second Class A full-decode pass.
