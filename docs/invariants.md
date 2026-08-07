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
| **Load miss** | Spend annotate without pin denserels; body decode without `tx.idx` range; pin without outs for need_vouts; ensure without abs for a spend edge | Assert / hard Err; fix lookup/load |
| **Environment** | `io_uring` off → pread/pwrite; `RBITCOIN_IO=mmap` demotes to pread; `RBITCOIN_FD_APPEND=0` | Keep modality fallback |
| **Protocol** | BIP30 multi-spender list; same-block spends; coinbase null create | Real branches |
| **API / product** | RPC body from store; Electrum mempool after chain; compact → getdata | Keep |

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
  → lookup (stamp create_fk + parent body_range + parent txid;
            IO: tx.head, tx.idx, txid.body — NEVER tx.body denserels)
  → load / pin (BatchParents denserels by known range only;
            IO: tx.body — NEVER head / idx / txid.body)
  → scripts (pure CPU — NEVER any store IO)
  → Class A commit (if ArchiveWritePlan present)
  → ensure abs (pin layout → Class A denserels body; post-condition: every spend has abs)
  → structural spentness (pin abs bulk pread; multi-list protocol cold only)
  → Class C tip
  → abs spend annotate (put_spend_batch_by_abs_meta only)
```

| Stage | Allowed IO | Forbidden |
|-------|------------|-----------|
| **lookup** | `tx.head`, `tx.idx` (fk + body_range), `txid.body`, headers | **`tx.body` denserels** |
| **load** | **`tx.body` denserels by range** (from lookup stamp) | head, idx, `txid.body` |
| **scripts** | none | any store IO |

| Stage | Invariant | Soft path allowed? |
|-------|-----------|--------------------|
| Lookup parent stamp | Every external spent parent has create_fk + body_range (or offline in_flight CreatePin) + reverse txid | Missing → hard Err at stamp / pin contract |
| Load body denserels | By range only from lookup stamp; incomplete denserels → hard Err | **No** idx cold denserels on load |
| Ensure (write) | Every non-null spend edge has denserels/abs after ensure returns | Residency then denserels body to **complete** load-ahead; incomplete → `invariant:` |
| Structural spentness | Abs required for every non-null spend create_fk after load; multi-list → confirmed-strong walk (reorg protocol) | **No** unpinned “wire-corrected create_fk” soft spentness. Multi flag alone is **not** hard `Err` |
| Pin create identity | Schema-13 denserels pin must carry non-zero create txid from **lookup stamp** (plan reverse map / wire prev_txid) | Soft zero-identity pin → assemble mismatch → cold recovery is **forbidden** |
| Tip already-archived | `plan=None`: lookup still stamps parent pin material; load denserels by range | Soft spentness recovery for zero pin identity is **not** OK |
| Tip-ahead cascade | `fk mismatch` / `connect height not tip+1` after tip+1 fail | **Soft requeue** (not permanent blacklist) |
| Spend annotate | Abs-only `put_spend_batch_by_abs_meta`; cold OOB/IO is hard Err | No ranged/by_create annotate tiers |
| Tip scripts | Optional `ScriptPreverified` (mempool) | IBD empty set |
| Reorg | Disconnect outside confirm; connect tip+1 with normal pipeline | — |

RPC, Electrum, and standalone tools may still use store cold paths.
`validate_block_connect` remains a no-write unit-test helper only (empty pin →
structural cold spentness).

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
| `pin_new_missing_parent_body_is_invariant_error` | `load_confirm_parents` pin_new ghost create_fk |
| `pin_new_incomplete_need_vouts_is_invariant_error` | `load_confirm_parents` pin_new OOB need_vouts |
