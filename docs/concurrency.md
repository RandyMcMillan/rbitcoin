# Concurrency model (store / IBD / tip follow)

Short map of who may write which tables. **Format is unstable until 1.0.**

## Roles during IBD

| Role | Threading | Store writes |
|------|-----------|--------------|
| Peer IO (N tasks) | tokio multi-thread | none; decoded blocks **offer body queue only**; note height/hash readiness on confirm feed |
| Confirm **plan** | 1 OS thread | load wire from **body queue**; structure + **plan** Class A (stamp create_fk only) |
| Confirm **prep** | 1 OS thread | pin denserels + assemble from owned stamped plan (no re-plan / no head resolve) |
| Confirm **scripts** | 1 OS thread + rayon | **none** — pure CPU |
| Confirm **commit** | 1 OS thread | **sole Class A appender** + structural + Class C + spend annotate + tip GC; **`block_queue_dequeue_height`**. Class A **never leads tip** (same commit era; no archive-ahead DONTNEED) |
| IBD main loop | 1 tokio task | none (orchestration only) |

**Height-ordered unified pipeline (current):** peer → **body queue** → **plan** (structure + stamp create_fk) → **prep** (pin denserels + assemble) → scripts → single commit era. **No** peer→confirm-feed wire retain. **No** hash-only / Class-A-only confirm (bq wire required). Prep must **not** re-run plan head resolve; handoff is owned `ArchiveWritePlan` (pipeline pins → Forbid cold denserels on prep). Bodies without a known height are marked missing and re-getdata after the height map is ready — there is **no** dual-track archive-job / ContigPark fallback.

**Plan pack size:** soft **Σ `tx.input`** budget (default **8000**, `RBITCOIN_CONFIRM_BATCH_INPUTS`; include overshoot block) or hard **144** blocks. Dense mainnet blocks hit the input soft stop after **typically a few blocks** (often 1–3); early tiny blocks may pack many until the hard cap. Do not assume ~32-block plan waves.

**Tip follow / reorg:** peer wire via `ChainHub::accept_block` / `accept_branch` → `accept_and_connect_block` (same wire prep path with cold denserels allowed on the one-shot call). Disconnect keeps Class A archive; re-extension always supplies **wire** from the peer, not hash-only load.

**Wire retained on the pipeline batch only:** plan/prep pull `bitcoin::Block` from the body queue; that wire rides through scripts; **no Class-A wire rebuild**. Class A packed form is planned once and committed in the write stage.

**Body queue:** process-local **in-RAM** payload FIFO (same shape as the former on-disk queue: id / height / hash / header_fk / payload). **Why RAM:** avoid **double disk write** of every block (queue then Class A); accept **redownload on restart** and peak RAM of soft depth. **Primary capacity is soft densify assign** (no hysteresis): under ~100 MiB free densify ahead; over ~100 MiB only heights confirm will consume in the next ~1 min at tip rate. Optional absolute byte ceiling via `RBITCOIN_BLOCK_QUEUE_GB` / `_BYTES` (default unlimited). Height horizon (`CONTIG_DENSIFY_AHEAD`, 64 k past tip) caps densify/receive walk. **Offer** on peer Block → RAM; prep **reads** by height; **dequeue** after confirm-commit. Restart starts empty (legacy `store/block_queue/` is best-effort removed).

**Pipeline pins:** plan `batch_pin` / `BatchParents` / plan-local `external_parent_outs` only (no process create FIFO). ConfirmParentCache holds tip-ahead header plans only.

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

Single Class A writer is intentional. Multi‑GiB **FdOnly grow** is fallocate-only
(no remap), but **hash-head rehash** (header / scripthash shards when materializing)
can still stall the **host** (page cache / disk). Class C tip tables use L2
write-behind (`flush_class_c_tip` before BQ dequeue); large tables stay L0.
are small. See **[ibd-io-audit.md](./ibd-io-audit.md)** and
**[io-modality.md](./io-modality.md)** for history and operator levers.

### Confirm prep read pipeline

Cold parent `tx.idx` / `tx.body` on the **prep** thread uses
**FdOnly idx + bulk body** (`idx_body_pipeline` → `bulk_io` uring/pread). Batch
creates come from **wire**, not a second Class A full-decode pass.
