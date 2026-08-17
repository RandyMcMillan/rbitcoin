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
| **load** | **`txout.body` outs by range** (from lookup stamp) | head, idx, `txid.body`, `inwit` |
| **scripts** | none | any store IO |

| Stage | Invariant | Soft path allowed? |
|-------|-----------|--------------------|
| Lookup parent stamp | Every external spent parent has create_fk + body_range (or offline in_flight CreatePin) + reverse txid | Missing → hard Err at stamp / pin contract |
| Parent create_fk union | **in-flight** (prune **after** pin + scripts handoff: drain inserted fk span **and** `fence.covers_fk_span`) → pin_txid → BQ hits → **TipOnly** (connected). No leftover pending map. Disconnect drops in-flight layers at that height. Header-cache GC polls store tip each load pack. One fk per txid — [`errata.md`](./errata.md). | **No** soft-requeue. Union miss → `Corrupt("parent create_fk unresolved")` (permanent) |
| Load body outs | By `txout` range only from lookup stamp; incomplete outs → hard Err | **No** idx cold outs on load; **no** `inwit` on pin |
| Ensure (write) | Every non-null spend edge has `spent_range` abs after ensure returns | Idx stamp of `spent.body` ranges; incomplete → `invariant:` |
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
- Pin / denserels: `rbitcoin-query` `confirm_load`, `BatchParents`, plan-local `external_parent_outs`
- Abs annotate: `rbitcoin-store` `put_spend_batch_by_abs_meta`

## Regression tests (shipped)

| Test | Entry |
|------|--------|
| `pin_for_wire_missing_parent_is_invariant_error` | `pin_for_wire_batch` missing spent parent |
| `pin_for_wire_incomplete_outs_is_invariant_error` | `pin_for_wire_batch` incomplete outs → cold miss |
| `post_commit_missing_denserels_is_invariant_error` | `post_commit` abs-only annotate |
| `ensure_spend_abs_incomplete_is_invariant_error` | `ensure_spend_abs_layouts` post-condition |
| `structural_pinned_without_abs_is_invariant_error` | `structural_validate_spends` pin without denserels |
| `already_archived_schema13_pin_identity_tip_follow` | archive then `confirm_wire_run` plan=None + rapid tip accept |
| `store_start_states_lookup_load_confirm` | S0 new Class A + S1 plan=None via lookup→load; structural IO split asserts |
| `plan_inflight_creates_only_fills_parent_body_range` | creates-only in_flight still stamps body_range for load denserels |
| `confirm_engine_pins_spend_of_just_written_pack` | IBD load: child spend of just-written pack (187 denserels miss) |
| `confirm_reject_blacklist_surface` | fk mismatch / connect height not tip+1 soft requeue |
