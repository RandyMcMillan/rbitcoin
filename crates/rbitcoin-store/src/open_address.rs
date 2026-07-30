//! Shared open-address helpers for hash heads (`HashHead`, `ScriptHashHead`).
//!
//! Both tables use 16-byte key prefixes, linear probing, and the same FNV-1a
//! primary hash. Full strategy merge (one generic table) is deferred; this
//! module keeps probe math / load / rehash serialization from drifting.

use std::sync::{Mutex, OnceLock};

/// Rehash when occupied/slots ≥ 7/8.
pub const MAX_LOAD_NUM: u64 = 7;
pub const MAX_LOAD_DEN: u64 = 8;

/// FNV-1a 64-bit offset basis (HashHead / ScriptHashHead / overflow probe).
pub const FNV_OFFSET: u64 = 0xcbf29ce484222325;
pub const FNV_PRIME: u64 = 0x100000001b3;

/// FNV-1a 64 over arbitrary bytes (shared open-address / overflow probe math).
#[inline]
pub fn fnv1a_64(bytes: &[u8]) -> u64 {
    let mut h = FNV_OFFSET;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}

/// Primary slot for a 16-byte open-address key (`slots` must be power of two).
#[inline]
pub fn primary_slot(key: &[u8; 16], slots: u64) -> u64 {
    debug_assert!(slots.is_power_of_two() && slots >= 2);
    fnv1a_64(key) & (slots - 1)
}

/// Primary slot for a 32-byte key (e.g. mixed txid in `tx.head.overflow`).
#[inline]
pub fn primary_slot_32(key: &[u8; 32], slots: usize) -> usize {
    debug_assert!(slots.is_power_of_two() && slots >= 2);
    (fnv1a_64(key) as usize) & (slots - 1)
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
    fn fnv1a_and_primary_slot_32_match_legacy_overflow() {
        // Same FNV stream as historical head_overflow::primary (32-byte key).
        let k = [9u8; 32];
        let slots = 64usize;
        let mut h = FNV_OFFSET;
        for b in &k {
            h ^= u64::from(*b);
            h = h.wrapping_mul(FNV_PRIME);
        }
        assert_eq!(fnv1a_64(&k), h);
        assert_eq!(primary_slot_32(&k, slots), (h as usize) & (slots - 1));
        assert_eq!(primary_slot_32(&k, slots), primary_slot_32(&k, slots));
    }

    #[test]
    fn load_constants() {
        assert_eq!(MAX_LOAD_NUM, 7);
        assert_eq!(MAX_LOAD_DEN, 8);
    }
}
