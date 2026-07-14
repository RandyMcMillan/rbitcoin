/// Milestone policy (assumevalid analogue): skip script + confirmability at/below height.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Milestone {
    pub height: u32,
}

impl Milestone {
    pub const NONE: Milestone = Milestone { height: 0 };

    /// When height > 0, validation of scripts/prevouts is skipped for blocks with
    /// `block_height <= self.height`.
    pub fn skips_at(self, height: u32) -> bool {
        self.height > 0 && height <= self.height
    }
}

impl Default for Milestone {
    fn default() -> Self {
        Self::NONE
    }
}
