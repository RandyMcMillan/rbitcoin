# Confirm-path invariants vs multi-path fallbacks

## Goal

On hot paths (especially Direct IBD confirm lookup → load → scripts → write), prefer
**one correct path** after lookup/load, not silent colder alternate paths when those
stages were supposed to guarantee a fact.

If lookup or load missed a fact that the normal path needs, that is a **bug**: fail hard
with `Err(…Corrupt("invariant: …"))` (and `debug_assert!` where useful). Do
**not** silently fall through to a slower store/idx walk that hides the bug.

## Classification

| Kind | Examples | Policy |
|------|----------|--------|
| **Load miss** | Spend annotate without `spent_range` abs; body decode without idx range; pin without outs for need_vouts; ensure without abs for a spend edge | Assert / hard Err; fix lookup/load |
| **Environment** | bulk IO backend uring vs pread/pwrite (single backend trait) | Keep modality only |
| **Protocol** | BIP30 multi-spender confirmed-strong walk; same-block spends; coinbase null create | Real branches (not soft recovery) |
| **Format migrate** | fuse8 v1 soft-open / always-probe with operator warn | Temporary dual-read only |
| **API / product** | RPC body from store; Electrum mempool after chain; compact → getdata | Keep |

**Killed dual paths (do not reintroduce):** soft spentness recovery for wrong/missing
pin identity; unpinned wire-corrected create_fk spentness; load-stage `txid.body`
identity fill after lookup promised stamp; `ColdPinMode` Allow/Forbid cold denserels
split on load (load is range **outs** only); denserels-as-spender-abs (schema 15
abs is `spent_off+9×vout` only).

## Failure style

- **Release / node:** `StoreError::Corrupt("invariant: …")` (or consensus wrap).
  No silent cold path. Operators get a log line; process need not abort.
- **Debug:** `debug_assert!` on the same facts when free.

Peer/wire corruption stays ordinary `Corrupt` / `BadBlock` — never assert on
untrusted input. Invariants apply only to **our** pipeline (load pin, denserels,
header plan after lookup planned it, stamped `create_fk`, etc.).

## Direct IBD stage table (enforced)

```
wire / body-queue
  → lookup (stamp create_fk + parent txout/spent ranges + parent txid;
            IO: tx.head, txout.idx/spent.idx, txid.body — NEVER body decode)
  → load / pin (BatchParents outs by known txout range only;
            IO: txout.body — NEVER head / idx / txid.body / inwit)
  → scripts (pure CPU — NEVER any store IO)
  → Class A commit (if ArchiveWritePlan present)
  → ensure abs (stamp spent_range; post-condition: every spend has abs)
  → structural spentness (pin abs bulk pread of spent.body; multi-list protocol cold only)
  → Class C tip
  → abs spend annotate (put_spend_batch_by_abs_meta on spent.body only)
```

| Stage | Allowed IO | Forbidden |
|-------|------------|-----------|
| **lookup** | `tx.head`, `txout.idx` / `spent.idx` (fk + ranges), `txid.body`, headers | **`txout`/`inwit` decode** |
| **load** | **`txout.body` outs by range** (from lookup stamp) | head, idx (`txout` / `spent`), `txid.body`, `inwit` |
| **scripts** | none | any store IO |

| Stage | Invariant | Soft path allowed? |
|-------|-----------|--------------------|
| Lookup parent stamp | Every external spent parent has create_fk + body_range (or offline in_flight CreatePin) + reverse txid | Missing → hard Err at stamp / pin contract |
| Parent create_fk union | **in-flight** (prune **after** pin + scripts handoff: drain inserted fk span **and** `fence.covers_fk_span`) → **published live_union** (lookup wave snapshot, `ArcSwap`) → **TipOnly** (connected **and** idx `body_range`). No leftover pending map. Dequeue enqueues a forget; lookup applies it at the next wave-end snapshot. Disconnect `store(None)` immediately and clears the lookup union. Header-cache GC polls store tip each load pack. One fk per txid — [`errata.md`](./errata.md). | **No** soft-requeue. Union miss → `Corrupt("parent create_fk unresolved")` (permanent). Identity without idx range → `Corrupt("invariant: idx range missing after identity")`, not a miss |
| io_uring harvest | Pending `user_data` set + kind/epoch. Unexpected CQE, undrained leftover, CQ overflow | **No** silent success. `Corrupt("invariant: io_uring …")`. Ring-unavailable still pread-fallback |
| Load body outs | By `txout` range only from lookup stamp; incomplete outs → hard Err | **No** idx cold outs on load; **no** `spent.idx` on pin; **no** `inwit` on pin |
| Ensure (write) | Every non-null spend edge has `spent_range` abs after ensure returns. **Sole** `spent.idx` stamper (`tx_spent_range_batch`) | Idx stamp of `spent.body` ranges; incomplete → `invariant:` |
| Structural spentness | Abs required for every non-null spend create_fk after load; multi-list → confirmed-strong walk (reorg protocol) | **No** unpinned “wire-corrected create_fk” soft spentness. Multi flag alone is **not** hard `Err` |
| Pin create identity | Pin must carry non-zero create txid from **lookup stamp** (plan reverse map / wire prev_txid / `txid.body`) | Soft zero-identity pin → assemble mismatch → cold recovery is **forbidden** |
| Tip already-archived | `plan=None`: lookup still stamps parent pin material; load `txout` by range | Soft spentness recovery for zero pin identity is **not** OK |
| Tip-ahead cascade | `fk mismatch` / `connect height not tip+1` after tip+1 fail | **Soft requeue** (not permanent blacklist) |
| Spend annotate | Abs-only `put_spend_batch_by_abs_meta`; cold OOB/IO is hard Err | No ranged/by_create annotate tiers |
| Tip scripts | Optional `ScriptPreverified` (mempool) | IBD empty set |
| Reorg | Disconnect outside confirm; connect tip+1 with normal pipeline | — |

RPC, Electrum, and standalone tools may still use store cold paths.
`validate_block_connect` remains a no-write unit-test helper only (empty pin →
structural cold spentness).

## Process open (before P2P / confirm)

Normative write-order and repair: [`crash-recovery.md`](./crash-recovery.md).
Short sequence:

| Step | Action |
|------|--------|
| 1 | `Store::open` + schema gates |
| 2 | Soft `tip_seal` clamp (if present) |
| 3 | Trim trailing null `confirmed[]` slots, then tip-window revalidate last **6** heights; shrink/clear on fail; rebuild fence |
| 4 | One `repair_class_c_above_tip` (fence complement: holes + short suffix) |
| 5 | Then node may densify / extend tip |

`TxIdx::open` refuses a non-monotone tail (`IDX_OPEN_DOUBLE_APPEND`). Offline
compact: `scripts/repair-idx-double-append.py`. No live heal of a cloned
published idx window.

## Store start states (intake at confirm start)

| State | Class A for tip+1 | Headers | Parent head | Handler (lookup cleans) |
|-------|-------------------|---------|-------------|-------------------------|
| **S0 fresh tip+1** | absent | present | parents on head | plan=Some: plan_batch stamps fk+txout/spent range+txid; load `txout` by range |
| **S1 already-archived** | body present (plan=None) | present | parents on head | lookup still stamps parent fk+ranges+txid (idx/head); load `txout` only |
| **S2 tip-ahead pack** | prior pack uncommitted | — | parents in in_flight | plan uses in_flight create_fk; **must** also stamp ranges (idx) when body exists, or use offline CreatePin |
| **S3 short catch-up** | mixed S0/S1 over gap | present | mostly cold | ordered claim tip+1 only for write; lookup may plan ahead with reserved HWM |
| **S4 cascade fail** | tip+1 blacklisted or write failed | — | — | tip-ahead write may hit `fk mismatch` / `connect height not tip+1` → **soft requeue**, not permanent blacklist |

| Error | State | Root | Fix |
|-------|-------|------|-----|
| `lookup stage miss (load cold denserels forbidden)` | S0/S3 | Load Forbid + parents without plan range | Lookup always fills `external_parent_ranges`; load outs by `txout` range only |
| `put_full_batch fk mismatch` | S4 cascade | Tip-ahead plan after tip+1 reject | Soft requeue for fk mismatch / connect height not tip+1 |
| `parent create_fk unresolved` | S2 | Leftover union miss | **Permanent.** Fix publish order. Do not soft-requeue. |
| false PrevoutSpent | identity | schema-13 zero pin id | plan reverse map / lookup `txid.body` only |

## Why there is no leftover pending map

In-flight is the only RAM `txid → create_fk` cache for planned creates
(plus `CreatePin` outs). Load prunes it **after** pin and scripts handoff,
when drain has inserted the layer (seal is inside that insert) **and**
the fence covers the span. Stamp skips `body_range` when in-flight still
has outs (n−1); pin needs those outs. TipOnly is the home once the layer
is gone.

Fence alone during drain/seal (67438 / 269204) does not drop the layer.
Drain alone does not drop it (TipOnly would reject unconnected).
Disconnect drops layers at/above the leaving height **before** the next
bind.

## Related code

- Confirm write annotate / ensure: `rbitcoin-consensus` `confirm_run::{post_commit,ensure_spend_abs_layouts,pin_for_wire_batch}`
- Structural: `rbitcoin-consensus` `block::structural_validate_spends`
- Pin / denserels: `rbitcoin-query` `confirm_load`, `BatchParents`, `pin_for_wire_batch` (cold range / adopt)
- Abs annotate: `rbitcoin-store` `put_spend_batch_by_abs_meta`

## Regression tests (shipped)

| Test | Entry |
|------|--------|
| `pin_for_wire_missing_parent_is_invariant_error` | `pin_for_wire_batch` missing spent parent |
| `pin_for_wire_incomplete_outs_is_invariant_error` | `pin_for_wire_batch` incomplete outs → cold miss |
| `post_commit_missing_denserels_is_invariant_error` | `post_commit` abs-only annotate |
| `ensure_spend_abs_incomplete_is_invariant_error` | `ensure_spend_abs_layouts` post-condition |
| `write_ensure_stamps_spent_range_after_load_pin` | pin-then-ensure fills abs; load pin has no `spent.idx` |
| `structural_pinned_without_abs_is_invariant_error` | `structural_validate_spends` pin without denserels |
| `already_archived_schema13_pin_identity_tip_follow` | archive then `confirm_wire_run` plan=None + rapid tip accept |
| `store_start_states_lookup_load_confirm` | S0 new Class A + S1 plan=None via lookup→load |
| `plan_inflight_creates_only_fills_parent_body_range` | creates-only in_flight still stamps body_range for load denserels |
| `confirm_engine_pins_spend_of_just_written_pack` | IBD load: child spend of just-written pack (187 denserels miss) |
| `confirm_reject_blacklist_surface` | fk mismatch / connect height not tip+1 soft requeue |
