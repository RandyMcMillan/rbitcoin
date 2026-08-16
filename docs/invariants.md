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
| Leftover parent union | A committed create is leftover-visible via **in-flight** until `fence.covers_fk_span`, then **load-owned pending** (`txid → fk` notes, no fence) until insert-fk HWM **and** fence **and** `height < tip+1`, then **TipOnly head**. Write queued is insert-only. Forget is per-fk, after bind. Disconnect evicts that block’s txids. Noted fk without `body_range` is `Corrupt`. Header-cache GC polls store tip each load pack. One fk per txid — [`errata.md`](./errata.md). | **No** soft-requeue. Union miss → `Corrupt("parent create_fk unresolved")` (permanent) |
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

## Why leftover is not in-flight

In-flight and leftover are both RAM `txid → create_fk` (plus in-flight outs).
They are **not** the same lifetime or prune.

| | In-flight | Leftover pending |
|-|-----------|------------------|
| **When** | Pack planned, not yet leftover-ready | Class A published; `tx.head` may still be queued |
| **Who writes** | Load `InFlightLog::note` / `prune` | Write sends notes; load applies / forgets |
| **Prune** | `fence.covers_fk_span` of the **whole layer** | **Per fk**: fence **and** insert HWM **and** `height < tip+1` |
| **Payload** | Creates **and** `CreatePin` outs for tip-ahead pin | Identity only (`txid → fk`) |

In-flight drops a layer as soon as the fence covers its fk span. That is when
leftover TipOnly *would* accept those creates — **if insert has published**.
Class C extends the fence **during** drain (67438). If leftover used the same
prune, load would drop identity after fence and miss open-head TipOnly.

Making in-flight wait for drain too would:

- Hold full `CreatePin` outs until insert+fence+pack_lo (leftover only needs
  32+8 bytes per txid).
- Couple layer prune (`covers_fk_span` of a pack) to per-fk insert HWM.
- Put leftover bind on a structure load already snapshots for scripts/write
  (`InFlightView`) — different readers, different forget.

So: in-flight until leftover *could* TipOnly on a fenced span; leftover until
insert **and** fence **and** the next pack has bound n−1. Two maps, two
prunes, one union order.

**`pack_lo = tip+1`:** leftover holds creates at height ≥ tip until the next
leftover bind (n−1 insurance). Forget after bind. `plan=None` does not forget
before bind.

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
| `confirm_reject_blacklist_surface` | fk mismatch / connect height not tip+1 soft requeue |
