# Concurrency model (store / IBD / tip follow)

Short map of who may write which tables. **Format is unstable until 1.0.**

## Roles during IBD

| Role | Threading | Store writes |
|------|-----------|--------------|
| Peer IO (N tasks) | tokio multi-thread | none (wire only) |
| Archive **prep** | 1 tokio task | **none** (CPU encode only) |
| Archive **writer** | 1 OS thread (`ibd-archive-writer`) | **Class A exclusive**: header body/head, tx body/idx/head, in/out runs; optional points when spend_index on |
| Confirm **load** | 1 OS thread (`ibd-confirm-load`) | none (reads Class A / parent cache; pin parents) |
| Confirm **scripts** | 1 OS thread (`ibd-confirm`) | **none** — pure CPU on `LoadedBatch` (rayon script verify only; **no store / Query reads or writes**) |
| Confirm **write** | 1 OS thread (`ibd-confirm-write`) | **structural** spentness/maturity/subsidy, then **Class C** (`strong_tx` / `tx_height` / SH creates / `confirmed[]`), then spend annotate (Direct). FIFO by height |
| IBD main loop | 1 tokio task | none (orchestration only) |

Prep never holds store write locks. The writer is the sole Class A producer for a process during IBD; multi-peer delivery is idempotent (`header_txs` body already present → skip re-append).

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
| Archive sticky (`txid→fk` + optional body range) | 8 M entries (~384 MiB planning) | `RBITCOIN_ARCHIVE_TXID_STICKY_CAP` clamp 100 k–20 M (≤~1 GiB-class) | `tx.head` probe + often `tx.idx` |
| Confirm OutFifo (outs + slim meta) | 2²⁴ outs | `RBITCOIN_CONFIRM_OUT_FIFO` | re-decode Class A for pin hits |

### Cross-platform bulk IO

Linux keeps io_uring for high-depth pipelined body/annotate/head-fill. **No**
Windows IOCP or macOS kqueue bulk backend: those completion models do not match
Linux ring fill/harvest shape, would not make prep/load code paths more similar
across platforms, and would risk a second maintenance surface without a measured
Linux-safe win. Non-Linux uses the shared pread/pwrite batch API only.

For **TB-scale store + Electrum on 16 GiB RAM**, the architectural plan (slim IBD,
fat Electrum index, Class B redesign) is **[store-efficiency-plan.md](./store-efficiency-plan.md)**.
