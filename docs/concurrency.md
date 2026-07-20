# Concurrency model (store / IBD / tip follow)

Short map of who may write which tables. **Format is unstable until 1.0.**

## Roles during parallel IBD

| Role | Threading | Store writes |
|------|-----------|--------------|
| Peer IO (N tasks) | tokio multi-thread | none (wire only) |
| Archive **prep** | 1 tokio task | **none** (CPU encode only) |
| Archive **writer** | 1 OS thread (`ibd-archive-writer`) | **Class A exclusive**: header body/head, tx body/idx/head, in/out runs; optional points when spend_index on |
| Confirm engine | 1 OS thread (`ibd-confirm`) | **Class C**: `strong_tx` / `tx_height`, thin scripthash **creates** (always, batched), then `confirmed[]` (tip = commit). Points at archive when spend_index on |
| IBD main loop | 1 tokio task | none (orchestration only) |

Prep never holds store write locks. The writer is the sole Class A producer for a process during IBD; multi-peer delivery is idempotent (`header_txs` body already present → skip re-append).

## Roles after IBD / tip follow

| Role | Notes |
|------|--------|
| `peer_session` (split read/write) | Serve reconstruct + accept tip blocks; may write Class A+C via `accept_and_connect_block` / cache |
| Electrum | Read-mostly; joins Class A + scripthash under `Query` |
| Epoch finalize | Single-threaded control path; flushes mmap |

## Locks

- Store tables use fine-grained `Mutex`es per file/head (see `rbitcoin-store`).
- Catch-up spentness / parent create_fk: `Query` light UTXO (`ibd_utxo.map` mmap under a mutex). No process-local spent HashSet.
- `ChainHub::confirmed` is `RwLock<HashSet>` for O(1) `has_block` during IBD.

## Practical rules

1. Do **not** spawn a second Class A writer while IBD archive is running.
2. Confirm may lag archive; that is intentional (tip holes vs archive lead).
3. On SIGINT, IBD cancels cooperatively — do not drop nested runtimes mid-await.

## Host freezes / IO storms

Single Class A writer is intentional. Multi‑GiB **mmap grow/remap** and **hash-head
rehash** (especially `point.head` under `--milestone 0`) can still stall the **host**
(page cache / disk). See **[ibd-io-audit.md](./ibd-io-audit.md)** for the full
audit, mitigations, and operator levers (`ionice`, dedicated disk, rehash log lines).

For **TB-scale store + Electrum on 16 GiB RAM**, the architectural plan (slim IBD,
fat Electrum index, Class B redesign) is **[store-efficiency-plan.md](./store-efficiency-plan.md)**.
