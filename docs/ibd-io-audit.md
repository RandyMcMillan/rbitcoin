# IBD IO / concurrency audit (host freezes)

**Status:** historical audit + current mitigations. Snapshot numbers below are from
an early mainnet full-validation run that still used durable `point.head` and
split Class A I/O tables. **Current schema (v5–v7)** uses spend-on-output, packed
Class A, catch-up sorted runs + materialize, and **write-through** hash heads
(no process-local write-behind overlay).

Observed during full mainnet validation (~20% tip): **periodic host UI freezes**.
That usually means the **kernel** is busy (disk reclaim, mmap page faults, TLB
shootdowns, journal commits)—not only that our process is CPU-bound.

Historical snapshot (`datadir-mainnet/store` at audit time):

| File | ~Size | Role (then) |
|------|-------|-------------|
| `input.body` | **19 GiB** | Class A inputs (archive writes continuously) |
| `point.head` | **11 GiB** | Hash head for spends (rehash doubles) — **removed in schema v5** |
| `output.body` | ~5 GiB | Class A outputs |
| `tx.body` / `tx.head` | multi‑GiB | Class A txs |
| Total store | **~45 GiB+** | Growing through IBD |

Current layout differs: packed `tx.body` embeds I/O; spends are annotations on
create outputs + rare `spenders.body` multi-lists; catch-up defers `tx.head` /
scripthash via sorted runs.

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

Background: confirm load / cache / archive may run additional disk paths; confirm
stays on its own OS thread.

---

## Top freeze suspects (ranked; historical + residual)

### 1. Hash-head rehash zero-fill (critical — mitigated)

`HashHead::rehash` doubled the slot table and **wrote zeros across the entire
new table** with chunked `write_at`. For multi‑GiB heads, the next rehash could
**write many GiB of zeros** in one burst.

**Mitigation (landed):** `TableFile::zero_range` uses `fallocate` punch-hole when
available; rehash logs start/done with elapsed; process-wide rehash gate; sharded
heads so rehashes stay per-shard.

### 2. Multi‑GiB mmap grow + remap (high — mitigated)

`TableFile::ensure_capacity` extends files and **remaps** the whole region.

**Mitigation (landed):** prefer `fallocate` for growth; **larger headroom/steps**
for multi‑GiB tables to cut remap frequency.

### 3. Sustained archive write saturation (high)

Pipeline stats: writer busy, archive queue at cap. Continuous sequential write of
Class A. If the store shares a disk with the desktop, UI freezes even when locks
are fine.

**Operator levers:** dedicated disk/NVMe; `ionice -c3` / `nice -n 10`;
`RBITCOIN_ARCHIVE_QUEUE_MB` only if RAM allows.

### 4. Confirm / parent-load thrash (medium)

Confirm parent resolve under lag can amplify random reads. Prefer parent cache
+ light UTXO in catch-up; keep archive lead moderate.

### 5. Full `store.flush()` (medium, rare)

Full flush is mainly shutdown / finalize. IBD writer uses **`flush_header_archive`**
on a cadence (headers only). Avoid full flush on a timer during IBD.

### 6. CPU oversubscription (medium)

Tokio + rayon + OS archive/materialize threads. **Levers:** `RAYON_NUM_THREADS`;
cgroup/CPU set.

### 7. Process locks (lower for host freezes)

Per-`TableFile` mutexes, light UTXO mutex, `ChainHub` RwLock — explain
**in-process** stalls more than whole-host freezes.

---

## What we changed in code

1. **`ensure_capacity`:** fallocate-first; larger growth steps for multi‑GiB files.
2. **`zero_range` + hash rehash:** punch-hole clear instead of zero `write_at` loop.
3. **Rehash logging:** path, sizes, duration for correlation.
4. **Slot-sorted / chunk-buffered `HashHead::insert_many`:** batch upserts sort by
   primary hash slot and RMW through slot-aligned chunks (write-through).
5. **Schema v5+:** no durable `point.head`; spend annotations on create outputs.
6. **Packed Class A:** one `tx.body` payload per full tx (no 3-table get path).
7. **Catch-up runs + materialize:** defer head inserts; claim `*.run.mat`;
   write-through heads (overlay / write-behind **removed**).
8. **Sharded hash heads** (new creates: `header.head` 256-way; `tx.head` /
   scripthash 16-way): probes and rehashes stay per-shard.
9. **`RLIMIT_NOFILE`:** soft limit raised toward 16384 on store open / node start.

---

## Recommended operator recipe (smoother host)

```bash
# Dedicated disk if possible
export RBITCOIN_ARCHIVE_QUEUE_MB=512
export RAYON_NUM_THREADS=4   # example: leave cores for desktop

nice -n 10 ionice -c 3 \
  ./target/release/rbitcoin-node \
  --datadir /mnt/btc/datadir-mainnet \
  --network mainnet
```

See also [`store-efficiency-plan.md`](./store-efficiency-plan.md) and
[`OPERATOR.md`](../OPERATOR.md).
