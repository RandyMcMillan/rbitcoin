/// Milestone policy (assumevalid-style): skip **script/sig** checks at/below height.
///
/// Prevout existence, double-spend, maturity, and fees are always checked on
/// contiguous tip confirm. Only pure-Rust script/signature verification is gated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Milestone {
    pub height: u32,
}

impl Milestone {
    pub const NONE: Milestone = Milestone { height: 0 };

    /// When height > 0, **script/signature** checks are skipped for
    /// `block_height <= self.height`. Prevouts/spends are still checked.
    pub fn skips_scripts_at(self, height: u32) -> bool {
        self.height > 0 && height <= self.height
    }
}

impl Default for Milestone {
    fn default() -> Self {
        Self::NONE
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn none_and_default_never_skip() {
        assert_eq!(Milestone::default(), Milestone::NONE);
        assert!(!Milestone::NONE.skips_scripts_at(0));
        assert!(!Milestone::NONE.skips_scripts_at(1_000_000));
    }

    #[test]
    fn height_gate_skips_at_and_below() {
        let m = Milestone { height: 100 };
        assert!(m.skips_scripts_at(1));
        assert!(m.skips_scripts_at(100));
        assert!(!m.skips_scripts_at(101));
    }
}
