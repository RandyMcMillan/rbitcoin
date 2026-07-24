//! Thin create-fk edges for confirm load / assemble.
//!
//! Batch-local: load builds [`crate::confirm_load::BatchThin`], assemble
//! consumes it. Not stored on the process parent cache.

/// Thin input edge: create-tx Class A fk when known at load (UTXO / same-batch).
/// Coinbase / unknown → `create_fk = None`. Not stored on Class A disk.
#[derive(Clone, Copy, Debug)]
pub struct ThinInput {
    pub create_fk: Option<u64>,
    pub prev_index: u32,
}
