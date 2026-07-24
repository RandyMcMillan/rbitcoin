# Confirm load review (mainnet IBD)

**Date:** 2026-07-24  
**Log:** `mainnet-ibd.log` (tip ~374k, archive lead ~72k)  
**Context:** Operator noted `conf_q` load→scripts queue always empty with scripts waiting on load.

---

## What `conf_q load<0/8` means

`load<0/8` is intentional log syntax: **depth 0** on the load→scripts channel, so the **scripts worker is blocked waiting for the next loaded batch**. In the last ~80 status windows this is almost always true:

| conf_q | count |
|--------|------:|
| `load<0/8 write<0/2` | 67 |
| `load=1/8 …` | 3 |
| `write=1/2` | 1 |

So the pipeline is not “broken”; **load is the long pole**. Scripts (and often write) finish before load finishes the next 32-block batch, so the queue never fills.

Typical recent walls (32-block batches, slow logs):

| stage | p50 wall |
|-------|----------|
| load (`work_ms`) | ~3.2–4.0s |
| scripts | ~2.2–3.3s |
| write | ~2.1–2.3s |

Tip rate ~6–13 blk/s with archive 72k ahead — confirm is pacing the tip, not download.

---

## What load still does (one batch)

`confirm_load_phase` is four steps; only the first is what logs call `win=` / most of `load=`:

```
load_confirm_parents  →  resolve metas  →  wire rebuild  →  assemble jobs
     (LOAD_NS / win)      (resolve_ms)      (recon/wire)     (in work_ms)
```

### 1. `load_confirm_parents` (~95% of `LOAD_NS`)

From last ~80 windows (phase share of `win`):

| phase | share of win | p50 ms/window | role |
|-------|-------------:|--------------:|------|
| **pin** | **66%** | ~2.7s | unique parent outs into `BatchParents` |
| **dec** | **20%** | ~0.6s | full Class A decode of batch creates |
| **thin** | **10%** | ~0.4s | stamped `create_fk` edges |
| **spent** (subset of pin) | ~8.5% of win | ~0.3s | `unspent_create_vouts` |
| **hdr** | ~0.4% | ~13ms | header + `header_txs` |

Pin split (last 50 `perf_dbg`):

| pin_sub | p50 | meaning |
|---------|----:|---------|
| **new** | ~1.6s | store meta+outs for FIFO miss parents |
| **body** | ~0.7s | FIFO hit path (clone outs + spent filter) |
| spent filter alone | ~0.4s | body walk + strong-spender checks |

Pin hit rate is usually good (p50 **pin_hit% ≈ 86%**), but when it dips (40–60%) pin_new explodes (tens of thousands of parents).

Per load-scanned block (rough): **~7 ms decode, ~4 ms thin, ~32 ms pin**.

### 2. Wire rebuild (after pin) — **fixed 2026-07-24**

**Was:** OutFifo kept meta+outs only; wire re-`get_tx_full` every create → double decode
(`body_io` ≈ half of `wire_body store=`).

**Now:** load puts full Class A into batch-local [`BatchFullBodies`]; wire rebuilds
`bitcoin::Transaction` from that map. OutFifo still outs-only for pin. Store full
decode on wire only for RPC / unexpected miss.

### 3. Resolve + assemble

Cheap relative to pin/dec: resolve often single-digit ms when header plans hit; assemble builds script jobs from already-pinned prevouts + wire txs.

---

## Where the time actually goes (mental model)

For a 32-block mainnet batch around this height:

```
┌─────────────────────────────────────────────────────────────┐
│ load_confirm_parents                                        │
│  ┌ dec: full Class A (tx+ins+outs) for every create  ~20%  │
│  ┌ thin: walk inputs → parent fks                    ~10%  │
│  ┌ pin:                                                ~66% │
│  │   FIFO hit → clone outs + unspent filter (often)        │
│  │   miss     → bulk meta+outs + unspent filter            │
│  └ spent filter alone often ~10% of win                     │
└─────────────────────────────────────────────────────────────┘
┌ wire: re-get_tx_full for same creates again          ~5–10% of work_ms ┐
┌ assemble: jobs from BatchParents + wire              small             ┐
```

Scripts then verify; write does structural + Class C + spend annotate. Scripts and write are busy, but **they starve on empty load queue**.

---

## Wasted / duplicated work (ranked)

### 1. Double Class A decode — **done**

Batch-local `BatchFullBodies` from load → wire; OutFifo still outs-only.

### 2. Pin spent-filter on hot FIFO hits (largest wall share)

Even with pin_hit% 80–90%, non–same-batch FIFO hits still:

- re-`tx_body_range`
- `unspent_create_vouts` (packed spender metas + strong checks)

Same-batch parents correctly skip spent filter; external parents do not.

- **Fix directions:**
  - Parallelize pin_jobs (rayon over parents after bulk range resolve)
  - Cache body range on `CreateOuts` to avoid idx probes
  - Optional: epoch/tip-versioned “still unspent” bits on FIFO outs, invalidated when tip advances / spends annotate
  - Batch strong checks more tightly if multi-range walks dominate

### 3. pin_new cold path still heavy

When hit% dips, pin_new is ~1.5–4s alone. Bulk meta+outs already exists; remaining cost is likely sequential spent filter + cloning.

- Parallel pin_new after bulk decode
- Don’t clone full out vectors when only a few vouts are needed (slim already for FIFO hits; pin_new still indexes dense outs)

### 4. Full decode at load is partly justified, partly not

Need:

- outs → FIFO / pin
- inputs → thin edges (`create_fk`)
- full body → wire (if we stop double-decoding)

Could explore **decode once into a batch arena** and derive FIFO slim + thin + wire from that single materialization (one packed read, multiple consumers).

### 5. Pipeline optics vs capacity

`LOAD_QUEUE_CAP = 8` is fine; it stays empty because **producer is slower than consumer**, not because the cap is too small. Raising cap alone will not help until load is faster (or load is parallelized across batches — harder with tip ordering).

### 6. Minor / already OK

- hdr / plans: cheap
- thin: pure CPU walk of already-decoded inputs (~10%) — only worth micro-opts after pin/dec
- write slow ~2s: real work (Class C + annotate), not the reason scripts starve
- pin_hit% 86% mean OutFifo is doing its job when depth is healthy

---

## Optimization priority (if you want a follow-on PR plan)

| Prio | Change | Expected effect | Risk |
|-----:|--------|-----------------|------|
| **P0** | ~~Single-materialize bodies for load+wire~~ **landed** | — | — |
| **P1** | Parallelize pin spent-filter / pin_new over parents | Shrink the 66% pin wall on multi-core | Low–med — store concurrency |
| **P1** | Stash `body_range` (+ maybe unspent epoch) on FIFO entry | Less idx + repeated body walks | Low |
| **P2** | Batch-local body arena (dec → thin → pin → wire from one decode) | Cleaner than “decode then re-decode” | Med refactor |
| **P3** | Tune OutFifo / offer ahead only after P0–P1 | Secondary | Low |

**Do not chase first:**

- Raising `LOAD_QUEUE_CAP`
- More archive lead (already ~72k)
- Thin micro-opts before pin/dec

---

## Bottom line

Scripts wait on load because **load is systematically slower than scripts**, not because the queue accounting is wrong. Inside load, **parent pin (~⅔)** and **Class A decode (~⅕, then again for wire)** dominate. The sharpest structural waste is **decode creates fully for pin/FIFO, drop inputs, then decode the same bodies again for wire**. Fixing that plus pin parallelism is the main lever to actually fill `load→scripts` and push tip rate.

### Code map

| Area | Path |
|------|------|
| Load + pin + thin | `crates/rbitcoin-query/src/confirm_load.rs` |
| Load→scripts→write pipeline | `crates/rbitcoin-net/src/ibd/confirm.rs` |
| `confirm_load_phase` / wire / assemble | `crates/rbitcoin-consensus/src/confirm_run.rs` |
| Wire body re-read | `crates/rbitcoin-query/src/reconstruct.rs` |
| OutFifo (meta+outs only) | `crates/rbitcoin-query/src/out_fifo.rs` |
| Parent cache put/pin | `crates/rbitcoin-query/src/confirm_parent_cache.rs` |
| Spent filter | `crates/rbitcoin-store/src/store.rs` (`unspent_create_vouts`) |
| Queue depth log format | `format_conf_q` / `format_queue_depth` in `confirm.rs` |

---

*Saved from agent review session for later implementation planning.*
