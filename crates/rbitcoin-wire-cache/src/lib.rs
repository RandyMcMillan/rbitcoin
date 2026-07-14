//! Tip wire-format block ring. Full implementation in Phase 5.

/// Crate identity for diagnostics.
pub fn crate_name() -> &'static str {
    "rbitcoin-wire-cache"
}

/// Placeholder ring that is always empty until archive_mode path lands.
#[derive(Debug, Default)]
pub struct WireRing {
    depth: u32,
}

impl WireRing {
    pub fn new(depth: u32) -> Self {
        Self { depth }
    }

    pub fn depth(&self) -> u32 {
        self.depth
    }

    pub fn len(&self) -> usize {
        0
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
