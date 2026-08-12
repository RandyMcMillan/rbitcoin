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

## Non-blocking vs accept (published snapshot)

Fee APIs **do not** walk the live mempool graph on every Electrum/Esplora request.

| Path | Behavior |
|------|----------|
| Accept / remove | Marks fee cache **dirty** only (no recompute on the admit critical path). |
| Refresh | Singleflight: at most one recompute; **one** `mining_chunks_best_first` under a short hub read lock, then pure math off-lock for all depths. |
| Request | Loads a published `Arc` snapshot (≤ **~1 s** stale when dirty/max-age). |

This avoids fee-estimates holding the hub lock for multi-second full-pool linearizes (which previously blocked accepts and vice versa). Estimates may lag a short bound after fee spikes; they still apply min-relay and confirm-memory floors on publish.

## Engine v2 (shipped)

**Temporal flow projection** under the same APIs as v1:

1. **Clock is mempool flow, not last-block age.** Always plan block *k* at
   **T + 10 k minutes** (`capacity_wu = N × 4_000_000`). Never stretch by wall
   time since the last tip.
2. **Live stock:** mining-chunk weight strictly above candidate rate R
   (`weight_above_feerate` / frontier).
3. **Inflow EMA:** per feerate bucket, exponential moving average of admitted
   package/chunk weight per second (`FeeFlowMeter` on successful accept).
4. **Include at R** when
   `stock_above(R) + projected_inflow_above(R, N×600s) ≤ 0.95 × N × 4e6`.
5. **Recommend** `max(projected, frontier, confirm_memory_floor, min_relay)`.

**Cold start:** until the flow meter is warm (≥60 s wall and ≥32 admits), the
estimate is the **v1 inclusion frontier** + confirm-memory + min relay (zero
inflow ≡ frontier).

### Parameters (code constants, not env)

| Parameter | Value |
|-----------|--------|
| Block weight capacity | 4_000_000 WU |
| Seconds per planned block | 600 |
| Capacity safety margin | 95% |
| Admit EMA half-life | ~150 s |
| Confirm EMA half-life | ~420 s |
| Warm | 60 s + 32 admits |
| Bucket edges (sat/kvB) | 100…100000 (+ open top) |

### Confirm-memory floor

Short ring of package feerates sampled when txs leave the pool on block connect
(`remove_for_block`). Applied as a floor under both cold and warm paths.

### Histogram / relayfee

Live chunk histogram remains a **stock** snapshot (transparency). Libre min
relay (0.1 sat/vB) floors any defined number.

## Template readiness (non-goal: full mining)

Frontier/chunk snapshots are shaped so a later block-template consumer can reuse
mining order. This document does **not** specify `getblocktemplate`, coinbase
construction, or witness nonces.

## Non-goals

- Core `estimatesmartfee` historical multi-horizon Bayesian parity
- Full multi-node flow aggregation / peer bandwidth models
- Changing Libre min relay, dust, or full-RBF defaults
- Persisting flow meters across process restart (process-local)

## Related

- Mempool admission correctness: findings [010](./external_findings/010-mempool-confirmed-spentness.md),
  [011](./external_findings/011-mempool-structural-chain-context.md)
- Policy: `rbitcoin-consensus::policy`, OPERATOR Libre table
- Accept path: staged prepare / script_pool / commit; coalesced durable writes
