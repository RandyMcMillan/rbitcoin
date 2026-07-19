# IBD IO / concurrency audit (host freezes)

Observed during full mainnet validation (~20% tip): **periodic host UI freezes**.
That usually means the **kernel** is busy (disk reclaim, mmap page faults, TLB
shootdowns, journal commits)—not only that our process is CPU-bound.

Snapshot at audit time (`datadir-mainnet/store`):

| File | ~Size | Role |
|------|-------|------|
| `input.body` | **19 GiB** | Class A inputs (archive writes continuously) |
| `point.head` | **11 GiB** | Hash head for spends (rehash doubles) |
| `output.body` | ~5 GiB | Class A outputs |
| `tx.body` / `tx.head` | multi‑GiB | Class A txs |
| Total store | **~45 GiB+** | Growing through IBD |

Logs already show: `writer_busy%=100`, `arch=256/256MiB` (queue full),
`class_a_cache hit%≈67–72` with high **evict** rates.

---

## Architecture (who hits the disk)

```
Peers (tokio) ──► decode (blocking pool)
                      │
         ┌────────────┴────────────┐
         ▼                         ▼
   archive prep (OS thread)   confirm (OS thread)
         │                         │
         ▼                         ▼
   archive writer (OS)        Class C + scripts (rayon)
   exclusive Class A mmap     strong_tx / scripthash / tip
```

Documented roles: [`concurrency.md`](./concurrency.md). **One** Class A writer is
correct for locking; freezes come from **how much** that writer (and hash rehash)
does to multi‑GiB mmaps, not from N concurrent Class A writers.

---

## Top freeze suspects (ranked)

### 1. Hash-head rehash zero-fill (critical)

`HashHead::rehash` doubled the slot table and **wrote zeros across the entire
new table** with chunked `write_at`. For `point.head` ~11 GiB, the next rehash
targets ~22 GiB and used to **write ~11–22 GiB of zeros** in one burst.

- Full validation (`--milestone 0`) enables durable **points** → `point.head` grows
  and rehashes periodically.
- Symptom: multi-second (or longer) freezes that track “random” progress, not
  tip height alone.

**Mitigation (landed):** `TableFile::zero_range` uses `fallocate` punch-hole when
available; rehash logs `hash-head rehash start/done` with elapsed so freezes can
be correlated in logs.

### 2. Multi‑GiB mmap grow + remap (high)

`TableFile::ensure_capacity` extends files and **remaps** the whole region.
`input.body` at 19 GiB + 256 MiB headroom steps still remaps a huge mapping.

- `set_len` without fallocate can force the FS to zero-extend (write storm).
- Remap invalidates page tables; concurrent confirm reads fault pages back in.

**Mitigation (landed):** prefer `fallocate` for growth; **larger headroom/steps**
for multi‑GiB tables (512 MiB–1 GiB) to cut remap frequency.

### 3. Sustained archive write saturation (high)

Pipeline stats: writer 100% busy, archive queue at cap (default 256 MiB). That is
**continuous sequential write** of Class A. If the store shares a disk with the
desktop (root, browser, swap), the UI freezes even when our locks are fine.

**Operator levers:** put `datadir` on a dedicated disk/NVMe; `ionice -c3` /
`nice -n 10` the node; lower concurrency slightly (`--max-outbound`); raise
`RBITCOIN_ARCHIVE_QUEUE_MB` only if RAM allows (more buffering, not less IO).

### 4. Class A cache thrash (medium — process, not always host)

Single `Mutex` around a 256 MiB FIFO. Logs show high miss/evict rates while
archive leads tip. That causes allocator churn and more random mmap reads under
confirm (page cache thrash), which **amplifies** disk pressure from (3).

**Levers:** `RBITCOIN_CLASS_A_CACHE_MB` (too large can worsen thrash); keep
archive lead moderate (confirm catching up helps locality).

### 5. Full `store.flush()` (medium, rare)

`flush()` msync + `sync_data` **every table** including multi‑GiB bodies. IBD
writer uses **`flush_header_archive` every 2048 blocks** (headers only)—good.
Full flush is mainly shutdown / finalize. Avoid calling full flush on a timer
during IBD.

### 6. CPU oversubscription (medium)

Tokio multi-thread (~all cores) + rayon global pool for scripts + OS archive
threads. Can compete with the compositor. Usually less “hard freeze” than disk.

**Levers:** `RAYON_NUM_THREADS=N` (e.g. half cores); run node on a cgroup/CPU set.

### 7. Process locks (lower for host freezes)

| Lock | Risk |
|------|------|
| Per-`TableFile` map/file mutexes | Contended if confirm reads during Class A grow |
| `Query::txid_to_fk`, `spent_local` | Fine-grained; not multi-second |
| `ClassACache` mutex | Hot under confirm; stalls confirm, not usually whole host |
| `ChainHub::confirmed` RwLock | Cheap |

Locks explain **in-process** stalls; host freezes align better with **mmap + disk**.

---

## What we changed in code

1. **`ensure_capacity`:** fallocate-first; larger growth steps for multi‑GiB files.
2. **`zero_range` + hash rehash:** punch-hole clear instead of zero `write_at` loop.
3. **Rehash logging:** WARN with path, sizes, duration for correlation.
4. **Slot-sorted / chunk-buffered `HashHead::insert_many`:** batch upserts sort by
   primary hash slot and RMW through slot-aligned chunks (sequential mmap writeback).
5. **Write-behind overlay** on `point.head` / `tx.head` during full-validation IBD
   (`--milestone 0`): process-local map of pending upserts; spill sorted at cap
   (default 512k, `RBITCOIN_POINT_HEAD_OVERLAY` / `RBITCOIN_TX_HEAD_OVERLAY`), on
   `flush_header_archive`, and on full `flush`. Cuts continuous random head RMW.
   Per-spill lines are **TRACE**; a rolled-up **DEBUG** summary logs ~every 30s.
6. **256-way sharded hash heads** (new creates: `point.head/00`…`ff`): probes and
   rehashes stay per-shard; mega-batch `insert_many` groups by `key[0]`. Legacy
   single-file heads still open. **No** `ensure_min` blow-up on open.
7. **Larger archive mega-batches** (up to 1024 blocks, min batch 32–256 by lag)
   so each writer cycle deposits more keys per shard before cycling.
8. **`RLIMIT_NOFILE`**: process raises soft limit toward 16384 (capped by hard) on
   store open / node start. Four×256 shards alone need ~1k FDs. If hard is still
   1024, set `LimitNOFILE=` / `ulimit -n` in the environment — soft cannot exceed hard.

---

## Recommended operator recipe (smoother host)

```bash
# Dedicated disk if possible
export RBITCOIN_ARCHIVE_QUEUE_MB=256
export RBITCOIN_CLASS_A_CACHE_MB=256
export RAYON_NUM_THREADS=4   # example: leave cores for desktop

nice -n 10 ionice -c 3 \
  ./target/release/rbitcoin-node \
  --datadir /mnt/btc/datadir-mainnet \
  --network mainnet \
  --milestone 0 \
  --max-outbound 12 \
  --log-level info
```

Watch for rehash freezes:

```bash
grep 'hash-head rehash' mainnet-ibd.log
```

---

## Further work (not yet)

| Idea | Effect |
|------|--------|
| Async/background rehash on a side file + rename | Cap stall latency |
| Optional `MAP_POPULATE` / `madvise` hints | Trade RSS for fewer fault storms |
| Separate archive volume from OS disk | Best single operator fix |
| Confirm-aware archive throttle when page cache pressure high | Needs metrics |
| Shard point heads | Smaller rehashes |

---

## Bottom line

Concurrency design (single Class A writer, separate confirm) is sound. Host freezes
are most consistent with **multi‑GiB mmap growth**, **hash-head rehash write
storms** (points under full validation), and **100% archive disk bandwidth** on a
shared drive—not with a classic lock-order bug. Mitigations above cut the worst
write storms; dedicated storage + mild nice/ionice remain the practical host fix.
