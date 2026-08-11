# Confirm pipeline store start states + IO split

## Process open (before P2P / confirm)

| Step | Action |
|------|--------|
| 1 | `Store::open` + schema gates |
| 2 | `repair_class_c_above_tip` |
| 3 | Soft `tip_seal` clamp (if present) |
| 4 | Tip-window revalidate last **6** heights (structure + merkle); shrink/clear on fail |
| 5 | Then node may densify / extend tip |

See [`crash-recovery.md`](./crash-recovery.md).

## Stage IO invariants (hard)

| Stage | Allowed store IO | Forbidden |
|-------|------------------|-----------|
| **lookup** | `tx.head`, `txid.body`, `tx.idx` (fk + body_range), header tables | **`tx.body`** (no denserels decode) |
| **load** | **`tx.body` only** (denserels/outs by known range) | `tx.head`, `txid.body`, `tx.idx` |
| **scripts** | none | any store IO |
| **write** | Class A append + annotate + tip (own era) | re-resolve parents via head |

Warmup that needs body denserels before a pipeline start is **outside** the
pipeline (not on lookup). Lookup hands load: create_fk stamps + body ranges +
parent txids (RAM reverse map from wire / sidefile).

## Store start states (intake at confirm start)

| State | Class A for tip+1 | Headers | Parent head | Handler (lookup cleans) |
|-------|-------------------|---------|-------------|-------------------------|
| **S0 fresh tip+1** | absent | present | parents on head | plan=Some: plan_batch stamps fk+range+txid; load body denserels by range |
| **S1 already-archived** | body present (plan=None) | present | parents on head | lookup still stamps parent fk+range+txid (idx/head); load body denserels only |
| **S2 tip-ahead pack** | prior pack uncommitted | — | parents in in_flight | plan uses in_flight create_fk; **must** also stamp body_range (idx) when body exists, or use offline CreatePin denserels |
| **S3 short catch-up** | mixed S0/S1 over gap | present | mostly cold | ordered claim tip+1 only for write; lookup may plan ahead with reserved HWM |
| **S4 cascade fail** | tip+1 blacklisted or write failed | — | — | tip-ahead write may hit `fk mismatch` / `connect height not tip+1` → **soft requeue**, not permanent blacklist |

## Log failure map (2026-08-07)

| Error | State | Root | Fix |
|-------|-------|------|-----|
| `lookup stage miss (load cold denserels forbidden)` @961466 | S0/S3 | Load Forbid + parents without plan range (in_flight / last-chance head without idx range fill); denserels body was incorrectly gated | Lookup always fills `external_parent_ranges` for every stamped external create_fk; load denserels by range only (body); never idx cold on load |
| `put_full_batch fk mismatch` @961468 | S4 cascade | Tip-ahead plan after tip+1 reject | Soft requeue for fk mismatch / connect height not tip+1 |
| `parent create_fk unresolved` | S2 | Creates-only in_flight lag | Keep soft requeue + creates-only publish |
| false PrevoutSpent | identity | schema-13 zero pin id | plan reverse map / lookup txid.body only |

## Dual soft paths to collapse

- Spentness recovery for wrong pin identity → **removed** (invariant)
- Forbid-all cold denserels on load → **wrong**; body-by-range is load's job
- idx cold denserels on load → **removed** (violates load IO split)
- Soft-requeue of parent unresolved / fk mismatch → **removed** (hides store bugs;
  only wire corrupt body + missing retarget header stay soft re-getdata)

## Store corruption: tx.idx double-write (mainnet 2026-08-07)

One published Class A window was a **bit-identical copy** of the previous
3330 idx starts (fk 1412912844..6173 == 1412909514..2843). No live heal —
`end < start` is hard `Corrupt`. Offline compact repair removes the ghost
window; `append_starts` rejects starts that fall inside already-published body.
