# Concurrency model (store / IBD / tip follow)

Short map of who may write which tables. **Format is unstable until 1.0.**

## Roles during IBD

| Role | Threading | Store writes |
|------|-----------|--------------|
| Peer IO (N tasks) | tokio multi-thread | none; decoded blocks **enqueue durable `store/block_queue/`** + RAM archive job |
| Archive **prep** | 1 tokio task | **none** — parent fk resolve via **CreateResidency** then sticky/OutFifo/head |
| Archive **writer** | 1 OS thread | **Class A exclusive**; publishes creates into sticky **and CreateResidency** |
| Confirm **load** | 1 OS thread | **none** — create decode + pin_new via **`load_creates_once` → CreateResidency** (shared map) |
| Confirm **scripts** | 1 OS thread | **none** — pure CPU (rayon); no store |
| Confirm **write** | 1 OS thread | Class C + spend annotate; **`block_queue_dequeue_height`** after success |
| IBD main loop | 1 tokio task | none (orchestration only) |

**Single parent-body path:** confirm load seeds **CreateResidency** on create decode; pin hits residency outs/ranges before cold denserels IO. Archive commit dual-writes ranges into the same map.

**Durable queue:** `store/block_queue/` multi‑GiB payload FIFO (`RBITCOIN_BLOCK_QUEUE_GB` / `_BYTES`, default 8 GiB). **Offer** on peer Block: write to disk when under budget; if full, **RAM buffer** (no spam) until confirm-write **dequeue** frees space. **Assign** gates new densify getdata on durable fill with 90%/70% hysteresis (and archive RAM budget); already-requested bodies are always accepted. On IBD start, **`rehydrate_block_queue_into_archive`** loads disk entries into arch_job (no re-getdata).

**tx.head overflow:** depth-exhausted inserts → `tx.head.overflow` (overflow-first lookup).

**Datadir secret (schema 12):** `store/store.secret` CSPRNG at create. XOR scripts/witness at rest; keyed TXID mix for heads.

## Roles after IBD / tip follow

| Role | Notes |
|------|--------|
| `peer_session` (split read/write) | Serve reconstruct + accept tip blocks; tip connect is archive + `confirm_archived_run` (same load pin denserels path as IBD) via `accept_and_connect_block` / cache |
| Electrum | Read-mostly; joins Class A + scripthash under `Query` |
| Epoch finalize | Single-threaded control path; flushes mmap |

## Index modes (`IndexMode`)

| Mode | When | Spentness | Durable `tx.head` / spends | SH |
|------|------|-----------|----------------------------|-----|
| **Direct** | IBD (`enter_direct_index_mode`) | confirmed-strong annotations | archive live head; confirm spend batch | append-only target-sized runs + SEAL → bulk at tip |
| **Tip** | after IBD (`enter_tip_mode`) | confirmed-strong annotations | live heads + confirm spends (already written in Direct) | durable write-through after bulk |

Do not enter Tip until tip ≈ peer height. Tip entry bulk-materializes SH
(runs → fan-in reduce → durable tables); it does **not** rebuild `tx.head` or spend annotations.

## Locks (exceptions only)

**Default is lock-free** on table hot paths (see `AGENTS.md`):

| Mechanism | What it replaces |
|-----------|------------------|
| Map **epochs** (`TableFile`) | No map `Mutex` on read/write; capacity = new mmap window + pointer swap |
| Atomic `count` / HWM | Publish barrier (Acquire readers / Release appender) |
| Role exclusivity | One appender, one annotator — not a global store mutex |
| `tx.head` insert | **Sole writer**: plain Release store empty→fk + SeqCst fence per batch (no CAS). Role exclusivity — not multi-inserter safe |
| `tx.head` resize swap | Brief exclusive catch-up + rename (shadow fill unlocked; primary inserts pause via `head` write lock on final swap) |
| Process `rehash_gate` | Rare multi‑GiB open-hash rehash (host freeze prevention) |
| `ChainHub::confirmed` | `RwLock<HashSet>` for O(1) `has_block` (IBD assign path) |

There is **no** global “pause queries during confirm write.” Tip-as-commit +
`is_confirmed_strong` define query visibility ([`crash-recovery.md`](./crash-recovery.md)).

## Practical rules

1. Do **not** spawn a second Class A writer while IBD archive is running.
2. Confirm may lag archive; that is intentional (tip holes vs archive lead).
3. Scripts for batch N may run while load does N+1 and write does N−1 (two queues, cap 2 each). Scripts never touch disk.
4. On SIGINT, IBD cancels cooperatively — do not drop nested runtimes mid-await.

## Host freezes / IO storms

Single Class A writer is intentional. Multi‑GiB **mmap grow/remap** and **hash-head
rehash** (especially large **header** / scripthash head shards when materializing) can still stall the
**host** (page cache / disk). See **[ibd-io-audit.md](./ibd-io-audit.md)** for the
audit history, mitigations, and operator levers (`ionice`, dedicated disk, rehash
log lines). Spends no longer use a durable `point.head` (schema v5+).

### Confirm load + archive prep read pipelines

Cold `tx.idx` / `tx.body` on the **prep** and **confirm-load** threads use a
single **mmap idx + bulk body** path (`idx_body_pipeline` → `bulk_io`):

- **Idx:** sorted mmap `record_range_batch` on segmented u32-stride `tx.idx`
  (no scatter pread / no io_uring for idx — multi-file segments make multi-fd
  uring a worse dual path).
- **Body:** `bulk_io::pread_batch` — Linux io_uring with a **thread-local**
  `UringSession` reused across waves (no per-batch `io_uring_setup`); nested
  bulk_io on the same thread opens a temporary ring. Prep and load each have
  their own TL ring (role threads).
- Sticky / OutFifo range hits skip idx; same-batch pin skips store.
- Fallback: `RBITCOIN_IO_URING=0` or non-Linux → libc `pread`/`pwrite` (optional
  `RBITCOIN_BULK_IO_WORKERS` for parallel pread). Same API surface; callers do
  not branch.
- Dense sticky commit ranges stay sequential mmap (`record_ranges`).

Multi-stage streaming loops (archive **head-resolve**, **spend annotate** abs-meta
RMW, `tx.head` shadow fill) own **one** `UringSession` for the batch duration —
not short-lived per CQE.

### RAM-for-IO budgets (process-local, kill-safe)

Caches avoid disk only; they never replace publish order / SEAL / tip-as-commit:

| Cache | Default | Cap / knob | Avoids |
|-------|---------|------------|--------|
| Archive sticky (`txid→fk` + optional body range) | 8 M entries (~384 MiB planning) | `RBITCOIN_ARCHIVE_TXID_STICKY_CAP` clamp 100 k–20 M (≤~1 GiB-class); **raw FIFO** (lookup read-only, no touch/LRU) | `tx.head` probe + often `tx.idx` |
| Confirm OutFifo (outs + slim meta) | 2²⁴ outs | `RBITCOIN_CONFIRM_OUT_FIFO`; **raw FIFO** (no pin-hit touch) | re-decode Class A for pin hits |

### Cross-platform bulk IO

Linux keeps io_uring for high-depth pipelined body/annotate/head-fill. **No**
Windows IOCP or macOS kqueue bulk backend: those completion models do not match
Linux ring fill/harvest shape, would not make prep/load code paths more similar
across platforms, and would risk a second maintenance surface without a measured
Linux-safe win. Non-Linux uses the shared pread/pwrite batch API only.

For **TB-scale store + Electrum on 16 GiB RAM**, the architectural plan (slim IBD,
fat Electrum index, Class B redesign) is **[store-efficiency-plan.md](./store-efficiency-plan.md)**.
