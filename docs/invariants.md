# Prep invariants vs multi-path fallbacks

## Goal

On hot paths (especially Direct IBD confirm load → scripts → write), prefer
**one correct path** after prep, not silent colder alternate paths when prep
was supposed to guarantee a fact.

If prep missed a fact that the normal path needs, that is a **bug**: fail hard
with `Err(…Corrupt("invariant: …"))` (and `debug_assert!` where useful). Do
**not** silently fall through to a slower store/idx walk that hides the bug.

## Classification

| Kind | Examples | Policy |
|------|----------|--------|
| **Prep miss** | Spend annotate without pin denserels; body decode without `tx.idx` range; FIFO pin without `body_range` | Assert / hard Err; fix prep |
| **Environment** | `io_uring` off → pread/mmap; `RBITCOIN_FD_APPEND=0` | Keep modality fallback |
| **Protocol** | BIP30 multi-spender list; same-block spends; coinbase null create | Real branches |
| **API / product** | RPC body from store; Electrum mempool after chain; compact → getdata | Keep |

## Failure style

- **Release / node:** `StoreError::Corrupt("invariant: …")` (or consensus wrap).
  No silent cold path. Operators get a log line; process need not abort.
- **Debug:** `debug_assert!` on the same facts when free.

Peer/wire corruption stays ordinary `Corrupt` / `BadBlock` — never assert on
untrusted input. Invariants apply only to **our** prep (load pin, denserels,
header plan after load planned it, stamped `create_fk`, etc.).

## Direct IBD strictness (current focus)

| Stage | Invariant (examples) |
|-------|----------------------|
| Load body | Every published create has a body range (no sequential `get_tx_full` fallback) |
| Load pin | Every needed parent has `body_range` + denserels for `need_vouts` (incomplete FIFO → re-pin) |
| Write spend annotate | Every non-null spend edge has abs meta; only `put_spend_batch_by_abs_meta` |
| Structural spentness | Abs required when parent was pin-loaded; cold only if no pin entry (unit-test `validate_block_connect`) or multi-list protocol |
| Tip connect | Same as IBD: archive + `confirm_archived_run` (not empty-pin connect) |
| Tip scripts | Optional `ScriptPreverified` (live mempool txids) skips re-verify; IBD empty set |
| Reorg | Disconnect outside confirm; then connect tip+1 with the normal pipeline |

RPC, Electrum, and standalone tools may still use store cold paths.
`validate_block_connect` remains a no-write unit-test helper only.
See the plan catalog (clusters A–K) for the full inventory.

## Related code

- Confirm write annotate: `rbitcoin-consensus` `confirm_run::post_commit`
- Pin / denserels: `rbitcoin-query` `confirm_load`, `BatchParents`, `CreateResidency`
- Abs annotate: `rbitcoin-store` `put_spend_batch_by_abs_meta`
