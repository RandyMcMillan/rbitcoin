# Confirm pipeline store start states + IO split

## Process open (before P2P / confirm)

| Step | Action |
|------|--------|
| 1 | `Store::open` + schema gates |
| 2 | Soft `tip_seal` clamp (if present) |
| 3 | Trim trailing null `confirmed[]` slots, then tip-window revalidate last **6** heights (structure + merkle + last-6 strong bits); shrink/clear on fail; rebuild fence |
| 4 | One `repair_class_c_above_tip` (fence complement: holes + short suffix — not a full strong-bit walk) |
| 5 | Then node may densify / extend tip |

See [`crash-recovery.md`](./crash-recovery.md).

## Stage IO invariants (hard)

| Stage | Allowed store IO | Forbidden |
|-------|------------------|-----------|
| **lookup** | `tx.head`, `txid.body`, `txout.idx` / `spent.idx` (fk + ranges), header tables | **body decode** (`txout` / `inwit`) |
| **load** | **`txout.body` only** (outs by known range) | `tx.head`, `txid.body`, idx, `inwit` |
| **scripts** | none | any store IO |
| **write** | Class A append + annotate + tip (own era) | re-resolve parents via head |

Warmup that needs body outs before a pipeline start is **outside** the
pipeline (not on lookup). Lookup hands load: create_fk stamps + `txout` /
`spent` ranges + parent txids (RAM reverse map from wire / sidefile).

## Store start states (intake at confirm start)

| State | Class A for tip+1 | Headers | Parent head | Handler (lookup cleans) |
|-------|-------------------|---------|-------------|-------------------------|
| **S0 fresh tip+1** | absent | present | parents on head | plan=Some: plan_batch stamps fk+txout/spent range+txid; load `txout` by range |
| **S1 already-archived** | body present (plan=None) | present | parents on head | lookup still stamps parent fk+ranges+txid (idx/head); load `txout` only |
| **S2 tip-ahead pack** | prior pack uncommitted | — | parents in in_flight | plan uses in_flight create_fk; **must** also stamp ranges (idx) when body exists, or use offline CreatePin |
| **S3 short catch-up** | mixed S0/S1 over gap | present | mostly cold | ordered claim tip+1 only for write; lookup may plan ahead with reserved HWM |
| **S4 cascade fail** | tip+1 blacklisted or write failed | — | — | tip-ahead write may hit `fk mismatch` / `connect height not tip+1` → **soft requeue**, not permanent blacklist |

## Log failure map (2026-08-07)

| Error | State | Root | Fix |
|-------|-------|------|-----|
| `lookup stage miss (load cold denserels forbidden)` @961466 | S0/S3 | Load Forbid + parents without plan range (in_flight / last-chance head without idx range fill); outs body was incorrectly gated | Lookup always fills `external_parent_ranges` for every stamped external create_fk; load outs by `txout` range only; never idx cold on load |
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
`TxIdx::open` refuses a non-monotone tail (`IDX_OPEN_DOUBLE_APPEND`, names
`txout.idx` / siblings). Offline compact: `scripts/repair-idx-double-append.py`.
`append_starts` still rejects new clones into already-published body.
