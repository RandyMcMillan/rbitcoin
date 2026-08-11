# Mempool fee estimation (rbitcoin)

## Product opinion: 10-minute inclusion is the standard API

The **default** fee estimate this node advertises answers:

> If current mempool pressure and admit/evict/relay flows continue, what feerate
> still gets a package into the next few blocks **after about 10 minutes**?

| Surface | Role |
|---------|------|
| Electrum `blockchain.estimatefee` (default / primary) | **10-minute inclusion** |
| Esplora fee endpoints (primary) | Same |
| Optional target-depth knobs | Secondary; same engine, different depth |

We intentionally do **not** present “90th percentile of live txs” as the long-term
story. The 10-minute inclusion question is clearer for wallets and is where we
intend to stand out versus Core-style historical estimators and crude percentiles.

## Engine v1 (this design / current implementation track)

**Inclusion frontier** from the live cluster mempool:

1. Per-cluster mining linearization + chunks (same graph used for eviction).
2. Best-first fill by chunk feerate; CPFP priced as one chunk rate.
3. Default 10-minute horizon maps to a fixed product depth `W_default`
   (documented in code/tests — e.g. next 1–2 blocks of weight at a simple cadence).
4. **Confirm-memory floor:** short ring of package feerates sampled when txs leave
   the pool on block connect (`remove_for_block`).  
   `recommended = max(frontier_default, memory_floor, min_relay)`.

Also exposed for transparency and future templates (not GBT):

- Fee histogram (prefer chunk-aware rates)
- `weight_above_feerate(r)`
- `mining_frontier_snapshot()` — ordered chunk summaries

Libre policy floor: never recommend below **0.1 sat/vB** when a rate is defined.

## Engine v2 (follow-up — same standard API)

**Relay-flow / temporal projection** powers the **same** 10-minute endpoint:

- If admit/evict/relay flows continue, what rate is still inside the inclusion set
  after 10 minutes.
- Not a second optional estimator — it becomes (or layers under) the standard answer.
- Implementation may consume append-only accept/remove/(optional) announce events
  (time, weight, feerate). v1 keeps the frontier pure and the default API stable
  so v2 does not require client changes.

## Template readiness (non-goal: full mining)

Frontier/chunk snapshots are shaped so a later block-template consumer can reuse
mining order. This document does **not** specify `getblocktemplate`, coinbase
construction, or witness nonces.

## Non-goals

- Core `estimatesmartfee` historical multi-horizon Bayesian parity
- Full multi-node flow aggregation / peer bandwidth models (v2 scope TBD)
- Changing Libre min relay, dust, or full-RBF defaults

## Related

- Mempool admission correctness: findings [010](./external_findings/010-mempool-confirmed-spentness.md),
  [011](./external_findings/011-mempool-structural-chain-context.md)
- Policy: `rbitcoin-consensus::policy`, OPERATOR Libre table
