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
/// `raw` is **heights only** — no payload clone. Lookup decodes via
/// [`crate::Query::block_queue_raw_payload`] per height it will actually
/// process.
#[derive(Clone, Debug, Default)]
pub struct BlockQueueWaveIntake {
    pub raw: Vec<u32>,
    pub resolved: Vec<(u32, ResolvedWire)>,
}

/// One-lock load-pack view: stored hash + resolve-complete + promoted Arc.
#[derive(Clone, Debug)]
pub struct BlockQueuePackSnap {
    pub height: u32,
    pub hash: [u8; 32],
    pub resolve_complete: bool,
    pub block: Option<Arc<Block>>,
}
