//! Lookup-promoted body-queue wire (decoded `Block`; pres added with TxPrecompute).

use bitcoin::Block;
use std::sync::Arc;

/// Decoded body held after lookup drops the raw frame.
#[derive(Clone, Debug)]
pub struct ResolvedWire {
    pub block: Arc<Block>,
}

/// One mutex snapshot of unresolved heights: still-raw vs already promoted.
#[derive(Clone, Debug, Default)]
pub struct BlockQueueWaveIntake {
    pub raw: Vec<(u32, Vec<u8>)>,
    pub resolved: Vec<(u32, ResolvedWire)>,
}
