//! Thin create-fk edges for confirm load / assemble.
//!
//! Formerly also held [`WavePrevoutCache`] (batch-local UTXO map built by
//! "wave fill"). That second structure is gone: load pins bodies + sparse
//! parents into [`crate::confirm_parent_cache::ConfirmParentCache`], and
//! assemble/wire/structural resolve from there via stamped create_fk.

/// Thin input edge: create-tx Class A fk when known at load (UTXO / same-batch).
/// Coinbase / unknown → `create_fk = None`. Not stored on Class A disk.
///
/// Same layout as cache `StashedThinInput` (type alias there).
#[derive(Clone, Copy, Debug)]
pub struct ThinInput {
    pub create_fk: Option<u64>,
    pub prev_index: u32,
}
