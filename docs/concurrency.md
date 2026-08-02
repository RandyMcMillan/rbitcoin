# Concurrency model (store / IBD / tip follow)

Short map of who may write which tables. **Format is unstable until 1.0.**

## Roles during IBD

| Role | Threading | Store writes |
|------|-----------|--------------|
| Peer IO (N tasks) | tokio multi-thread | none; decoded blocks **offer body queue only**; note height/hash readiness on confirm feed |
| Confirm **plan** | 1 OS thread | load wire from **body queue**; structure + **plan** Class A (stamp create_fk only) |
| Confirm **prep** | 1 OS thread | pin denserels + assemble from owned stamped plan (no re-plan / no head resolve) |
| Confirm **scripts** | 1 OS thread + rayon | **none** — pure CPU |
| Confirm **commit** | 1 OS thread | **sole Class A appender** + structural + Class C + spend annotate + tip GC; **`block_queue_dequeue_height`** |
| IBD main loop | 1 tokio task | none (orchestration only) |

**Height-ordered unified pipeline (current):** peer → **body queue** → **plan** (structure + stamp create_fk) → **prep** (pin denserels + assemble) → scripts → single commit era. **No** peer→confirm-feed wire retain. **No** hash-only / Class-A-only confirm (bq wire required). Prep must **not** re-run plan head resolve; handoff is owned `ArchiveWritePlan` (not residency FIFO seed → Forbid). Bodies without a known height are marked missing and re-getdata after the height map is ready — there is **no** dual-track archive-job / ContigPark fallback.

**Tip follow / reorg:** peer wire via `ChainHub::accept_block` / `accept_branch` → `accept_and_connect_block` (same wire prep path with cold denserels allowed on the one-shot call). Disconnect keeps Class A archive; re-extension always supplies **wire** from the peer, not hash-only load.

**Wire retained on the pipeline batch only:** plan/prep pull `bitcoin::Block` from the body queue; that wire rides through scripts; **no Class-A wire rebuild**. Class A packed form is planned once and committed in the write stage.

**Body queue:** `store/block_queue/` on-disk payload FIFO (no process RAM overflow). **Primary capacity is soft time-depth**: stop frontier densify when on-disk count &gt; ~5 minutes of tip-rate blocks (same EWMA as ETA); resume when &lt; ~4 minutes. Gaps inside the on-disk height span always densify (overshoot OK). Optional absolute byte ceiling via `RBITCOIN_BLOCK_QUEUE_GB` / `_BYTES` (default unlimited). Height horizon (`CONTIG_DENSIFY_AHEAD`, 64 k past tip) caps densify/receive walk. **Offer** on peer Block → disk; prep **reads** by height; **dequeue** after confirm-commit. Restart re-notes feed readiness only (wire stays on disk until prep).

**CreateResidency:** sole process-local create map for wire plan + pin (txid→fk,
range, outs, denserels; raw FIFO). Class A commit seeds denserels offline so
prep(N+1) hits without body re-read. Sole process map is CreateResidency
(plan resolve + pin denserels + prewarm).

**tx.head (segmented):** fixed **25-bit** open-address head per segment with
**4 B relative** create ids; roll at `MIN(body soft span, 80% slots)`. On seal,
build **binary fuse8** (~9 bits/key). Open segment has no filter (always probed).
Lookup: open → sealed newest→oldest (fuse gate) → body verify. No mono-head
resize / overflow sidecar.

**Datadir secret (schema 12):** `store/store.secret` CSPRNG at create. XOR scripts/witness at rest; keyed TXID mix for heads.

## Roles after IBD / tip follow

| Role | Notes |
|------|--------|
| `peer_session` (split read/write) | Serve reconstruct + accept tip blocks via **`accept_and_connect_block`** → **`confirm_wire_run`** (same prep→scripts→commit) |
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
| Capacity **epochs** (`TableFile`) | No long map `Mutex` on read/write; MapFull = new map window + pointer swap; FdOnly (`tx.body`) = fallocate only |
| Atomic `count` / HWM | Publish barrier (Acquire readers / Release appender) |
| Role exclusivity | One appender, one annotator — not a global store mutex |
| `tx.head` insert | **Sole writer**: plain Release store empty→relative + SeqCst fence per batch (no CAS). Role exclusivity — not multi-inserter safe |
| `tx.head` segment seal | Synchronous on roll: build fuse8 + mark sealed + open new head (no shadow resize) |
| Process `rehash_gate` | Rare multi‑GiB open-hash rehash (host freeze prevention) |
| `ChainHub::confirmed` | `RwLock<HashSet>` for O(1) `has_block` (IBD assign path) |

There is **no** global “pause queries during confirm write.” Tip-as-commit +
`is_confirmed_strong` define query visibility ([`crash-recovery.md`](./crash-recovery.md)).

## Practical rules

1. Do **not** spawn a second Class A writer while IBD confirm commit is running.
2. Pipeline depth: prep(N+1) ∥ scripts(N) ∥ commit(N−1) via bounded load/write queues.
3. Scripts for batch N may run while prep does N+1 and commit does N−1. Scripts never touch disk.
4. **Prep ahead of store tip:** prep may plan batch N+1 while commit has not advanced tip.
   Prep holds a **reserved create-fk HWM** and **in-flight create/out maps** from
   uncommitted plans (`WirePrepPipeline` / `archive_plan_mega_from`). First height
   of a batch is the **pipeline path_lo** (tip+1 or last-prepped+1), not only store tip.
   Commit still applies batches in height order; on permanent reject, prep clears
   reserved state and re-syncs from `txs.count()`.
5. On SIGINT, IBD cancels cooperatively — do not drop nested runtimes mid-await.

## Host freezes / IO storms

Single Class A writer is intentional. Multi‑GiB **MapFull grow/remap** and **hash-head
rehash** (especially large **header** / scripthash head shards when materializing) can still stall the
**host** (page cache / disk). See **[ibd-io-audit.md](./ibd-io-audit.md)** and
**[io-modality.md](./io-modality.md)** for history, demap plan, and operator levers
(`ionice`, dedicated disk, rehash log lines).

### Confirm prep read pipeline

Cold parent `tx.idx` / `tx.body` on the **prep** thread uses
**table-map idx + bulk body** (`idx_body_pipeline` → `bulk_io` uring/pread). Batch
creates come from **wire**, not a second Class A full-decode pass.
