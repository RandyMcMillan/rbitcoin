//! Shared open-address helpers for hash heads (`HashHead`, `ScriptHashHead`).
//!
//! Both tables use 16-byte key prefixes, linear probing, and the same FNV-1a
//! primary hash. Full strategy merge (one generic table) is deferred; this
//! module keeps probe math / load / rehash serialization from drifting.

use std::sync::{Mutex, OnceLock};

/// Rehash when occupied/slots ≥ 7/8.
pub const MAX_LOAD_NUM: u64 = 7;
pub const MAX_LOAD_DEN: u64 = 8;

/// FNV-1a 64-bit offset basis (same as historical HashHead / ScriptHashHead).
const FNV_OFFSET: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

/// Primary slot for a 16-byte open-address key (`slots` must be power of two).
#[inline]
pub fn primary_slot(key: &[u8; 16], slots: u64) -> u64 {
    debug_assert!(slots.is_power_of_two() && slots >= 2);
    let mut h = FNV_OFFSET;
    for b in key {
        h ^= u64::from(*b);
        h = h.wrapping_mul(FNV_PRIME);
    }
    h & (slots - 1)
}

/// Process-wide: at most one open-address rehash at a time (IBD materialize must
/// not stack multi-shard resizes into one host freeze). Shared by tx/header
/// heads and scripthash heads.
pub fn rehash_gate() -> &'static Mutex<()> {
    static GATE: OnceLock<Mutex<()>> = OnceLock::new();
    GATE.get_or_init(|| Mutex::new(()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn primary_slot_stable_and_in_range() {
        let k = [0xabu8; 16];
        let s = primary_slot(&k, 1024);
        assert!(s < 1024);
        assert_eq!(s, primary_slot(&k, 1024));
    }

    #[test]
    fn load_constants() {
        assert_eq!(MAX_LOAD_NUM, 7);
        assert_eq!(MAX_LOAD_DEN, 8);
    }
}
