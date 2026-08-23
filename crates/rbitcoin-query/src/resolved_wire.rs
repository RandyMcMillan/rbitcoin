//! Lookup-promoted body-queue wire (decoded `Block` + [`TxPrecompute`]).

use crate::TxPrecompute;
use bitcoin::Block;
use std::sync::Arc;

/// Decoded body held after lookup drops the raw frame.
#[derive(Clone, Debug)]
pub struct ResolvedWire {
    pub block: Arc<Block>,
    pub pres: Arc<[TxPrecompute]>,
}

/// One mutex snapshot of unresolved heights: still-raw vs already promoted.
///
/// `raw` is **(height, n_inputs)** — no payload clone. Lookup packs/holds
/// from the stamped count; decode clones via
/// [`crate::Query::block_queue_raw_payload`] only for heights it emits.
#[derive(Clone, Debug, Default)]
pub struct BlockQueueWaveIntake {
    pub raw: Vec<(u32, u32)>,
    pub resolved: Vec<(u32, ResolvedWire)>,
}
