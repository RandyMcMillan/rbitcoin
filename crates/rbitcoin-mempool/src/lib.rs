//! Cluster mempool with **InRam** buffers + private sidecar durability under
//! `{datadir}/mempool/` — **not** Class A (`{datadir}/store/tx.body`).
//!
//! # Layout (private namespace)
//!
//! | File | Role |
//! |------|------|
//! | `meta` | Magic, schema, commit generation **G**, slot capacity, live count |
//! | `slots` | Fixed-size slot records (status + body range + txid) |
//! | `tx.body` | Unconfirmed payloads only: `fee(8)‖weight(8)‖raw_tx` per LIVE slot |
//!
//! **Commit model:** body complete → slot LIVE → RAM graph → no fsync per tx.
//! [`ActiveMempool::flush`] bumps `G` and `sync_data`s sidecars. Kill loses at
//! most the last unflushed batch; never claim incomplete bodies.
//!
//! **Memory rule:** graph + body buffers stay proportional to the live set.
//! Sidecars use process `Vec` + file write (no `memmap2`).
//!
//! # Phases (plan.md)
//!
//! - **P1:** open / flush / reopen empty skeleton  
//! - **P2:** TxGraph + linearization + Libre single-tx accept + durable commit  
//! - **P3:** package accept (CPFP), durable remove, block/reorg hooks  
//! - **P5:** full RBF + pure RBFR (1.25×) + package RBF + worst-chunk eviction  

mod accept;
mod error;
mod graph;
mod orphanage;
mod store;

pub use accept::{
    pure_rbfr_pays, rbf_allows_replacement, rbf_pays_for_replacement, AcceptError, AcceptResult,
    AcceptStageUs, ActiveMempool, ChainTipCtx, Coin, MapUtxoProvider, PreparedAdmit, UtxoProvider,
    DEFAULT_MAX_MEMPOOL_WEIGHT, INCREMENTAL_RELAY_FEE_RATE_SAT_PER_KVB, MAX_PACKAGE_COUNT,
    MAX_PACKAGE_WEIGHT, RBFR_RATIO_DEN, RBFR_RATIO_NUM,
};
pub use error::MempoolError;
pub use graph::{
    Chunk, Cluster, TxEntry, TxGraph, MAX_CLUSTER_COUNT, MAX_CLUSTER_VSIZE, MAX_CLUSTER_WEIGHT,
};
pub use orphanage::{
    Orphanage, DEFAULT_ORPHAN_MAX_COUNT, DEFAULT_ORPHAN_MAX_WEIGHT, MAX_ORPHAN_TX_WEIGHT,
    ORPHAN_PEER_BUDGET, ORPHAN_RESERVED_WEIGHT_PER_PEER,
};
pub use store::{Mempool, MempoolMeta, MEM_MAGIC, MEM_SCHEMA, PERSIST_COALESCE_OPS};

pub fn crate_name() -> &'static str {
    "rbitcoin-mempool"
}

#[cfg(test)]
mod tests {
    #[test]
    fn crate_name_stable() {
        assert_eq!(crate::crate_name(), "rbitcoin-mempool");
    }
}
