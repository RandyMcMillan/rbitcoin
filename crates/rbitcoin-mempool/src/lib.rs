//! Policy mempool (Phase 6). Placeholder surface for workspace wiring.

pub fn crate_name() -> &'static str {
    "rbitcoin-mempool"
}

#[derive(Debug, Clone)]
pub struct MempoolConfig {
    pub max_size_bytes: usize,
    pub min_relay_fee_rate: u64,
}

impl Default for MempoolConfig {
    fn default() -> Self {
        Self {
            max_size_bytes: 300 * 1024 * 1024,
            min_relay_fee_rate: 1000,
        }
    }
}

impl MempoolConfig {
    pub fn is_sane(&self) -> bool {
        self.max_size_bytes > 0 && self.min_relay_fee_rate > 0
    }
}
