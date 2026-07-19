# Store efficiency plan: mainnet TB-scale + Electrum on 16 GiB RAM

**Status:** design / sequencing (not all items implemented)  
**Audience:** operators hitting host freezes + agents changing the store  
**Related:** [`ibd-io-audit.md`](./ibd-io-audit.md), [`concurrency.md`](./concurrency.md), [`SCHEMA.md`](../SCHEMA.md), [`libbitcoin-durable-archive-variant.md`](../libbitcoin-durable-archive-variant.md)

---

## 1. Where we are (measured)

At ~20% mainnet full validation on this machine, `datadir-mainnet/store` is already **~50 GiB**:

| File | ~Size | Access pattern |
|------|-------|----------------|
| `input.body` | **21 GiB** | Sequential append (IBD); random read (confirm / reconstruct) |
| `point.head` | **11 GiB** | Open hash, linear probe, rehash-doubles |
| `tx.head` | **5 GiB** | Open hash (txid → fk) |
| `output.body` | **5 GiB** | Sequential append; random for Electrum enrich |
| `tx.body` | **3.7 GiB** | Sequential + random |
| `point.body` | **2.9 GiB** | Append multimap edges |
| `scripthash.head` | **~1 GiB** | Open hash |
| `scripthash.body` | **~0.4 GiB** | Thin create chains |

Naive linear scale to “full mainnet” (wire ~600 GiB, archival indexes often **1–2 TiB** total):

- `input.body` alone can approach **~100 GiB+**
- `point.head` + `tx.head` can be **tens of GiB each** of *random-access* structure
- On a **16 GiB RAM** box, **none** of these heads fit; every cold probe is a disk seek / page fault

Logs already show the live shape of pain: `writer_busy%=100`, archive queue full, Class A cache thrash, and multi‑GiB mmap grow / hash rehash storms ([`ibd-io-audit.md`](./ibd-io-audit.md)).

---

## 2. How Electrum will feel on 16 GiB + sluggish disk

### 2.1 Query path today (confirmed)

`scripthash_history` / `balance` / `listunspent` ([`scripthash.rs`](../crates/rbitcoin-query/src/scripthash.rs)):

1. Hash probe `scripthash.head` → walk thin create chain in `scripthash.body`
2. Per create: **`get_tx(create_tx_fk)`** + **`tx_output`** + `tx_height` (enrich)
3. Per create: **`spenders(outpoint)`** → point head probe + body walk + `get_tx(spend)` + height

So one quiet wallet with **N creates** is roughly:

```text
O(N) × (scripthash body + Class A tx + Class A output + point head + point body + Class A spend tx)
```

Each step is often a **different multi‑GiB mmap region**. On a cold cache / slow disk that is **dozens of random IOs per UTXO lifecycle**, not one index lookup.

ElectrumX / Fulcrum avoid this by storing **fat history rows** (txid, height, value, spentness) so a wallet query is largely sequential in one index file.

### 2.2 Steady-state memory budget (16 GiB machine)

| Consumer | Rough budget that still leaves room for OS + browser |
|----------|------------------------------------------------------|
| OS + page cache (must exist) | 4–6 GiB minimum for UI to stay alive |
| Process RSS (node + peers + rayon) | 4–6 GiB realistic target |
| Class A cache | **≤128–256 MiB** (already default 256; larger can thrash worse) |
| Archive queue | **≤128–256 MiB** |
| Mempool | **≤100–300 MiB** weight budget mapped to live set |
| Hash heads | **must not** be fully faulted in |

**Implication:** performance must come from **fewer random IOs** and **locality**, not from “cache the archive in RAM.” Page cache will only hold a thin working set of *recent* Class A + hot Electrum keys.

### 2.3 Worst Electrum cases on this store

| Query | Failure mode on slow disk |
|-------|---------------------------|
| Popular exchange scripthash | Huge create chains; walk + N× point probes |
| `listunspent` after long history | Same joins; latency multi-second |
| Concurrent wallets | Cache thrash; no per-scripthash pinning |
| `transaction.get` deep history | `tx.head` probe + body read (OK if head fits in cache; not if thrashing) |

**Bottom line for Electrum-on-16 GiB:** the current “thin create + join everything” model is **architecturally wrong for serve**, even if it is elegant for write-once archive. It will work, but it will feel like a cold HDD address index from 2015 unless we densify the Electrum path.

---

## 3. IBD vs serve: two jobs, one store (today)

| Job | What we need | What the store optimizes for today |
|-----|--------------|-------------------------------------|
| **IBD full validation** | Append Class A; resolve prevouts; mark spends; scripts | Sequential Class A mega-batch (good); points+heads on critical path (bad) |
| **Electrum serve** | Low-latency history/UTXO by scripthash | Thin index + random joins (bad) |
| **Tip follow** | Small random write + reconstruct | OK if heads not thrashing |

Libbitcoin’s product insight (see durable-archive note): **IBD is allowed to be rebuildable and non-durable**; concurrency and append throughput matter more than fsync. We already defer full flush. We do **not** yet defer or slim the indexes that explode random IO (`point` / fat open hash heads).

---

## 4. Comparison to libbitcoin (insights to steal)

### 4.1 What we already share (keep)

| Idea | Why it scales better than Core’s UTXO-as-backbone |
|------|-----------------------------------------------------|
| Class A write-once bodies | No rewrite of ancient outputs |
| Class B multimaps for spends | Append edge, don’t mutate UTXO row |
| Class C tip confirmation | Reorg without rewriting archive |
| Milestone / skip scripts | IBD speed valve |
| mmap tables | Zero-copy reads when pages are hot |

### 4.2 What libbitcoin does better for *throughput* (we only partially have)

| Libbitcoin-ish idea | Our gap |
|---------------------|---------|
| **IBD = no durability tax** | We still write **points + optional full tx.head** during full validation IBD → head rehash storms |
| **Allocate-then-publish / lock-free heads** (v4 era) | We use `Mutex` + mmap rewrite rehash; correct but stall-prone |
| **Structural queries for node internals**, not Electrum densification | We bolted Electrum onto thin structural indexes → join tax |
| **Optional address / filter tables** | Our scripthash is mandatory-thin; should become **serve-optimized densification** |
| **Candidate / validation caches near tip** | We have Class A FIFO cache; no tip UTXO/prevout slab sized for confirm |

### 4.3 What not to copy blindly

- Assuming “more RAM + big mmap = fine” — fails on 16 GiB + >1 TiB store  
- Full open-addressing hash tables for **every** outpoint at global scale without partitioning  
- Treating Electrum as a free projection of structural tables  

---

## 5. Concrete performance plan (phased)

Goal: **IO-efficient IBD** *and* **IO-efficient Electrum** on **16 GiB + mediocre disk**, with path to **>1 TiB** store.

### Phase S0 — Operator / config (days, no format break)

| Action | Effect |
|--------|--------|
| Datadir on **dedicated** disk (not OS/UI disk) | Largest single freeze/latency win |
| `nice` + `ionice -c3` | UI survives archive saturation |
| Full validation: accept slower; or **milestone IBD then reindex** spends/scripthash | Avoid points on critical path during catch-up |
| Cap rayon: `RAYON_NUM_THREADS` | Leave cores for OS |
| Keep Class A cache **≤256 MiB** unless dedicated big RAM | Avoid thrash |

Ship as OPERATOR “sluggish disk / 16 GiB” profile (checklist).

### Phase S1 — Stop write storms (format-compatible) ✅ partially landed

| Action | Status / next |
|--------|----------------|
| fallocate grow, larger remap steps | ✅ landed |
| Hash rehash without multi‑GiB zero writes (punch-hole) | ✅ landed |
| Log rehash duration | ✅ landed |
| Slot-sorted / chunk-buffered `insert_many` | ✅ landed |
| Process-local write-behind overlay on `point.head`/`tx.head` (full-validation IBD) | ✅ landed (spill at cap / archive flush / tip mode) |
| Background rehash to side file + atomic swap | **Next** if freezes remain |
| Never full `store.flush()` during IBD | Already mostly true; guardrails |

### Phase S2 — Split **IBD slim** vs **serve dense** (highest leverage)

**IBD slim mode (default for catch-up):**

```text
Write: Class A (headers, tx, in, out) + Class C as today
Defer or batch: point.head updates, optional tx.head, dense Electrum
Prevouts: process txid→fk cache + prev_tx_fk (already) until tip mode
```

**Post-tip / serve build (one pass or streaming):**

```text
Build / densify: point index, Electrum fat index, optional filters
May take hours on slow disk — but once, offline-friendly, sequential-friendly
```

This mirrors libbitcoin’s “IBD is a different mode” without abandoning the relational model.

**Exit criteria:**  
- IBD wall time dominated by network + scripts, not head rehash  
- After serve-build, Electrum `listunspent` for a normal wallet is **O(history)** sequential in **one** file region, not O(history)×6 random maps  

### Phase S3 — Fat Electrum index (format bump, required for 16 GiB serve)

Replace thin create-only rows with **serve rows** (illustrative):

```text
scripthash → linked list or sorted slab of:
  txid[32] | height:u32 | value:i64 | vout:u32 | spend_height:u32 | flags
```

Or dual structure:

- **Create slab** (append by confirmation order)  
- **Spend bit / height** updated once (or second multimap only for spends)

| Query | Target IO |
|-------|-----------|
| `get_balance` | One chain walk, **no** Class A join |
| `listunspent` | Filter unspent in same slab |
| `get_history` | Same slab; optional second walk for spends if split |

Keep thin structural tables for node internals if needed; **Electrum never joins Class A for the hot path.**

### Phase S4 — Class B hash redesign (TB-scale)

Open linear-probe tables of tens of GiB are hostile to sluggish disks.

| Option | Pros | Cons |
|--------|------|------|
| **A. Partitioned heads** (e.g. 256 shards by key prefix) | Smaller rehash, parallel build | Still open hash |
| **B. Robin Hood / Swiss-table on disk** | Better probe locality | Complex |
| **C. LSM / sorted string table for points & scripthash** | Sequential compaction, great for cold disk | Write amp, more code |
| **D. Hybrid:** tip window open-hash in RAM + cold SST | Best 16 GiB fit | Two paths |

**Recommendation:** **S3 fat Electrum first**; for points, **S2 defer during IBD** then **S4-C or partitioned heads** for spend index. Do not keep growing a single 10–50 GiB `point.head` as the forever design.

### Phase S5 — Working sets sized for 16 GiB

| Cache | Size | Content |
|-------|------|---------|
| Tip prevout / UTXO slab | 256–512 MiB | Recent outputs for confirm (not full set) |
| Class A FIFO | 128–256 MiB | Archive→confirm locality only |
| Electrum hot scripthash | 64–128 MiB MRU of fat slabs | Wallet sessions |
| Hash probe bloom (optional) | 32–64 MiB | Negative lookups on tx/point |

Never try to cache full `tx.head` / `point.head` in process RAM.

### Phase S6 — Read path IO discipline

| Technique | Where |
|-----------|--------|
| Prefetch next scripthash body links | Electrum walk |
| Batch `get_tx` by fk order (sort FKs, sequential body) | When joins remain |
| Avoid holding mmap write lock across huge encodes | Archive writer |
| Reconstruct serve from `TxRecord.raw` without point joins | getdata / `transaction.get` |

### Phase S7 — Durability / epochs (libbitcoin durable variant)

Keep: **no finalize during IBD**; epoch + wire ring post-tip ([durable archive variant](../libbitcoin-durable-archive-variant.md)).  
Do not add fsync-per-batch during catch-up.

---

## 6. Recommended sequence (what to build next)

```text
Now (ops)     S0  dedicated disk, nice/ionice, rehash logs, 16 GiB profile
Soon (code)   S2  IBD slim: defer point durable writes + optional tx.head
              S1+ background rehash if still freezing
Then (format) S3  Fat Electrum index — required for serious wallet serve
Then          S4  Point/scripthash storage that is not one giant open hash
Parallel      S5–S6 caches + read batching
Steady state  S7  epoch finalize when archive_mode true
```

### Success metrics

| Scenario | Target (16 GiB, HDD/SATA SSD) |
|----------|-------------------------------|
| IBD (milestone) | Disk sequential; rehash WARN rare / short |
| IBD (full validation) | Scripts + network bound; points not multi-minute freezes |
| Electrum balance (≤1k history) | **&lt;50 ms** warm, **&lt;500 ms** cold after S3 |
| Electrum listunspent same | Same order of magnitude |
| Host UI during IBD | Usable with S0 + S1; no multi-second freezes after S1/S2 |

---

## 7. What *not* to do

1. **Raise Class A cache to multi‑GiB** on 16 GiB hosts — worsens thrash and UI death.  
2. **Keep thin Electrum forever** and “optimize joins” only — asymptotic cost stays O(history × maps).  
3. **Full UTXO set in RAM** as the center of the design — Core’s scaling trap libbitcoin walked away from.  
4. **fsync every archive batch** — destroys IBD; contradicts durable-archive “IBD off.”  
5. **Assume 64 GiB+ servers** — product must work on mid-range ops hardware.

---

## 8. One-paragraph thesis

We inherited libbitcoin’s **write-once relational mmap archive**, which is the right *family* for multi‑TB chain data, but we currently run **serve-quality open hash heads and thin Electrum joins on the IBD write path**. At mainnet scale on 16 GiB + slow disk that means random IO dominates both catch-up freezes and wallet latency. The fix is not “more locks” or “bigger mmap cache”; it is **mode-split (slim IBD / dense serve)**, a **fat Electrum index**, and **Class B structures that compact and scan instead of rehashing multi‑ten‑GiB open tables**.

---

## 9. Immediate engineering tickets (checklist)

- [ ] OPERATOR: “16 GiB / sluggish disk” profile (S0)  
- [ ] IBD slim mode: `--spend-index later` / build points after tip (S2)  
- [ ] Background hash rehash or side-file rehash (S1+)  
- [ ] SCHEMA v4 sketch: fat scripthash slab (S3)  
- [ ] Electrum path: only fat index on hot methods (S3)  
- [ ] Benchmark: N-entry wallet listunspent cold/warm before/after S3  
- [ ] Cap process RSS documentation; cgroup sample unit file  
