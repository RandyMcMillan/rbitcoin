//! Consensus validation (Phase 3). Placeholder surface for workspace wiring.

pub fn crate_name() -> &'static str {
    "rbitcoin-consensus"
}

/// Milestone policy placeholder (assumevalid analogue).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Milestone {
    pub height: u32,
}

impl Milestone {
    pub const NONE: Milestone = Milestone { height: 0 };

    pub fn skips_at(self, height: u32) -> bool {
        self.height > 0 && height <= self.height
    }
}
