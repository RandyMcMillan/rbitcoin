# IBD plan slowdown forensics (mmap → FdOnly / io_uring)

**Status:** root cause fixed in tree (remove dead far-ahead plan finish); host re-measure pending  
**Date:** 2026-08-03  
**Log:** `mainnet.log`  
**Audience:** operator re-measure finish @400k; residual `head_fk` is P1

---

## TL;DR

At tip ~400–425k, current run (R7) was **~0.65–0.70×** the block rate of the best prior full IBD (R6, Aug 1).

| Symptom | Cause class |
|---------|-------------|
| **~75% of plan regression** | Hot-path O(n) `header_txs.count_bodies()` inside plan `finish` (L2 `RwLock` per index) — **not** uring |
| **~25% of plan regression** | Real FdOnly + uring head-resolve cost (`head_fk` +16 ms/blk) |
| Script / write | Not the problem (write is faster on R7) |

**Fix shipped (code):** delete `archive_far_ahead_of_confirm` / plan `advise_dont_need` / write-side body DONTNEED lead. **Class A never leads tip** (unified confirm sole Class A appender), so the lead check was always false and the O(n) scan was pure dead work. Plan finish no longer calls `count_bodies`. Optional O(1) `count_bodies` for rare status APIs is **not** required for the plan regression.

Expected plan **~119 → ~76 ms/blk** at this height after fix; host must confirm `finish=` flat vs tip.

---

## Runs compared

Segmented `mainnet.log` by tip drops (wipe / restart). Focus:

| ID | Window | Tip span | Notes |
|----|--------|----------|-------|
| **R6** | 2026-08-01 13:20 → 2026-08-02 ~20:40 | ~2k → tip (~960k) | Best full IBD; already uring for spend meta/ann |
| **R7 (current)** | 2026-08-02 21:35 → (ongoing) | ~0 → ~425k+ | Post demap / FdOnly Class C L2; wipe restart |

Other historical runs (R0 Jul 28, R4/R5 late Jul) are slower than R7 at the same heights; R6 is the right ceiling.

### Block rates at same height (wall-clock segment)

| Segment | R6 blk/s | R7 blk/s | R7/R6 |
|---------|----------|----------|-------|
| 300–350k | 32.2 | 24.6 | 0.76× |
| 350–400k | 16.6 | 11.4 | 0.69× |
| **400–420k** | **11.7** | **7.8** | **0.67×** |
| 10k window @ ~425k | 11.0 | 7.2 | 0.65× |

Time to 420k: R6 **141 min**, R7 **195 min** (~38% slower wall).

---

## Phase breakdown (tip 400–425k, ms/blk)

From `INFO ibd: perf` `stamp_sub` + conf thr (aggregated):

| Phase | R6 | R7 | Δ |
|-------|----|----|---|
| **plan total** | **60.9** | **118.7** | **+58** |
| `head_fk` | 40.0 | 56.0 | +16 |
| `head_dens` | 6.6 | 8.8 | +2 |
| stamp | ~1.2 | ~0.9 | ~0 |
| **`finish`** | **0.8** | **43.8** | **+43** |
| script | 61 | 64 | ~0 |
| write | 47 | 36 | better |
| class_a head (write) | 2.1 | 4.6 | +2.5 (not bottleneck) |

Pipeline busy% (prep/script/write thr busy/(busy+wait)):

| | prep | script | write |
|--|------|--------|-------|
| R6 | 56% | 84% | 91% |
| R7 | **21%** | 51% | 46% |

Plan thread is starving the rest of the pipeline on R7.

### Finish scales with tip (R7 only)

| tip band | finish ms/blk |
|----------|---------------|
| 200k | 4.5 |
| 300k | 11 |
| 350k | 24 |
| 400k | 44 |
| 425k | 53 |

R6 stayed ~0.5–1.2 ms/blk until ~900k. R7 grows steadily from mid-chain.

On R7 every sampled window: **`dontneed=0`** — the flag guarded by this check never enables `advise_body_dont_need` during unified confirm IBD.

---

## Root cause 1 (primary): `finish` → `count_bodies` O(n) + per-get RwLock

### Call chain

1. `Query::archive_plan_mega_from` timer `t_finish`  
   (`crates/rbitcoin-query/src/archive.rs` ~445–463)
2. `archive_far_ahead_of_confirm()` (~844–854)
3. `HeaderTxsTable::count_bodies()`  
   (`crates/rbitcoin-store/src/chain.rs` ~1094–1107)
4. Loop `for i in 0..n { self.count.get(i)? }`
5. `ArrayTable::get` takes **`data.read()` RwLock every call**  
   (`crates/rbitcoin-store/src/array_table.rs` ~124–132)

At tip ~400k (historical, when this pathology was live): **~400k lock acquire/release pairs per plan batch** → ~1.5–1.7 s, matching log `finish=1500–2100ms`. Plan size then was often tens of blocks; **current** packing is soft **Σ inputs ≈ 8000** (typically **a few blocks** at dense heights — see `OPERATOR.md` / `RBITCOIN_CONFIRM_BATCH_INPUTS`). The far-ahead scan cost scaled with tip, not with pack length.

Comment on `count_bodies` says *"startup / resume accounting"* but it runs **every mega plan**.

`body_est` + `batch_creates` in the same timer are cheap; almost all `finish` is the far-ahead check.

### Why demap made this lethal

Phase 6 Class C L2 (`ArrayTable` InRam `Vec` + `RwLock`) replaced mmap-style cheap sequential scans. Per-index `get()` is correct for sparse access but catastrophic for full-array scans under lock.

### Why the check is useless on unified IBD

Unified path: Class A commit is the confirm tip. `arch_hi ≈ tip ≈ parent_cache`, so  
`arch_hi - cache > ARCHIVE_BODY_DONTNEED_LEAD` is **always false**.  
Confirmed: R7 log has **0** non-zero `dontneed=` samples in the current run window.

### Fix applied (not A/B/C keep-the-question)

**Invariant:** Class A **cannot** lead tip → far-ahead body DONTNEED is dead product.

**Code:** remove detector + plan finish call + `advise_dont_need` plumbing. Do **not** keep calling `count_bodies` with an O(1) counter on the plan path — that answers a deleted question faster. Status-only `archived_block_count` may still O(n) rarely (optional later hygiene).

### Verification after fix

- `rg archive_far_ahead|advise_dont_need` empty in crates (structural).
- Store/query/consensus tests green; plan finish has no `count_bodies` call.
- Operator: `finish=` flat vs tip; `dontneed=` absent or always 0.

### Expected impact after fix

| | plan ms/blk @400k | rough plan-bound rate |
|--|-------------------|------------------------|
| R7 now | ~119 | ~8.4 /s |
| finish → ~1 ms | ~76 | ~13 /s |
| R6 reference | ~61 | ~16 /s |

Observed rates are a bit lower than pure critical-path math (pipeline + network), but finish fix should close **most** of R7 vs R6 at this height.

---

## Root cause 2 (secondary): real FdOnly / uring `head_fk`

After finish is fixed, residual gap ≈ **`head_fk` 40 → 56 ms/blk** (+40%).

### Per-key head_rd (perf_dbg, tip 400–425k)

| Metric | R6 | R7 | Δ |
|--------|----|----|---|
| probe µs/key | 6.4 | 8.1 | +27% |
| idx µs/key | 9.2 | 13.3 | +45% |
| body µs/key | 10.6 | 13.3 | +25% |
| lookups/key | 4.1 | 3.9 | ~same |
| keys/blk | 919 | 1082 | +18% |
| denserels_hit% | 88 | **99** | better |
| dens_wave µs/fk | 8 | 8 | same |

Cache is **not** the problem. Identity resolve (probe + Prefix33 idx/body) is slower under FdOnly + uring.

### Code path

- Plan: `get_fk_and_outs_by_txid_batch`  
  (`crates/rbitcoin-store/src/tx_table.rs` ~1427+)
- Shape A: Prefix33 select + one denserels body per winner
- Probe: `SegmentedTxHead::probe_candidates_batch` (page-coalesced)  
  (`crates/rbitcoin-store/src/segmented_head.rs` ~401+)
- Streaming resolve (fk-only path): `head_resolve_stream.rs` — uring SM, ring depth **128**, `MAX_IN_FLIGHT_KEYS = 128`
- Env: `RBITCOIN_HEAD_RESOLVE_IO` / `RBITCOIN_IO` (`uring` \| `pread`)  
  See `docs/io-modality.md`, `OPERATOR.md`

Historical note in `docs/io-modality.md`: io_uring **head insert** was ~5× slower than mmap and was reverted; bulk uring kept for **reads**. Resolve may still prefer pread for some patterns — **host A/B required**.

### Optimization ideas (after finish fix)

1. Host A/B: `RBITCOIN_HEAD_RESOLVE_IO=pread` vs `uring` at same tip band.
2. Larger ring / in-flight cap for head resolve (128 vs ~40k keys/batch).
3. Hotter L1 page cache for open + sealed head segments (probe µs).
4. Fuse false-positive reduction (still ~2 miss_peeks/key, avg ~4 cands).
5. Confirm residency / parent mix: why keys/blk +18% vs R6 despite better denserels hit%.

Do **not** reintroduce long-held map mutexes or mmap for multi-GiB tables (see `AGENTS.md`, `docs/io-modality.md`).

---

## Non-causes / lesser items

| Item | Finding |
|------|---------|
| Script validation | ~same ms/blk |
| Write path overall | **Faster** on R7 (36 vs 47 ms/blk) |
| denserels / pin hit% | Better on R7 (99/99 vs 88/98) |
| dens_wave cost | ~8 µs/fk both |
| Spend ann/meta uring | ~1–2 µs/op; small ms/blk |
| RSS | R7 ~1.7 GiB vs R6 ~16 GiB @420k — demap working; don't reverse for rate |
| Body queue depth | R7 ~1.5 GiB vs R6 ~7.5 GiB — may matter **after** plan is fixed |
| class_a head insert | 2× slower (page RMW) but write not critical path yet |

---

## How to re-measure

From `mainnet.log` (or a fresh run):

```bash
# Progress tip + instant rate
rg "INFO ibd: progress" mainnet.log | tail

# Plan sub-phases (finish / head_fk)
rg "stamp_sub\\(struct=.*finish=" mainnet.log | tail

# head_rd µs/key
rg "head_rd\\(probe=" mainnet.log | tail

# dontneed should stay 0 during unified IBD; finish should collapse after fix
rg "dontneed=" mainnet.log | tail
```

Compare **same tip band** (e.g. 400–420k) wall-clock blk/s and ms/blk, not overall average from height 0.

**Perf A/B is operator-host only** with musl static binary (`AGENTS.md`). Agent VM 9p cannot open live mainnet store for meaningful timings.

```bash
nix build .#rbitcoin-musl --out-link result
mkdir -p target/release
install -m 755 result/bin/rbitcoin-node result/bin/rbitcoin-cli target/release/
```

---

## Suggested follow-on order

1. **[P0 — done in tree]** Remove far-ahead detector from plan finish (no hot-path `count_bodies`).
2. Operator re-measure: `finish` flat vs tip; tip rate @400k.
3. **[P1]** Host A/B head resolve uring vs pread; optional ring depth / probe cache knobs.
4. **[P2]** Body-queue soft depth / residency only if plan is no longer critical path.

---

## Key file references

| Area | Path |
|------|------|
| Plan finish timer + far-ahead | `crates/rbitcoin-query/src/archive.rs` |
| `count_bodies` | `crates/rbitcoin-store/src/chain.rs` |
| ArrayTable L2 get/RwLock | `crates/rbitcoin-store/src/array_table.rs` |
| Shape A head + denserels | `crates/rbitcoin-store/src/tx_table.rs` (`get_fk_and_outs_by_txid_batch`) |
| Streaming head resolve | `crates/rbitcoin-store/src/head_resolve_stream.rs` |
| Segmented head probe batch | `crates/rbitcoin-store/src/segmented_head.rs` |
| Phase stats / finish_ns | `crates/rbitcoin-query/src/lib.rs` (`archive_phase_stats`) |
| Perf log tokens | `crates/rbitcoin-net/src/ibd/perf_log.rs` |
| IO modality / demap history | `docs/io-modality.md` |
| Env knobs | `OPERATOR.md` |
| Concurrency rules | `AGENTS.md`, `docs/concurrency.md` |

---

## Context for the reviewing agent

- Project prefers **lock-free store hot path**, FdOnly tables, uring bulk reads — do not “fix” rate by remapping multi-GiB tables.
- Behavioral fixes need tests; commit + `nix build .#rbitcoin-musl` install after code changes.
- Do not open user `datadir-mainnet/` from the agent VM for store open/mmap diagnosis.
- This doc is forensics from `mainnet.log` comparison R6 vs R7; implement and verify on operator host when changing perf-sensitive paths.
