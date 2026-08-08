//! Identity-hasher maps for dense sequential integer keys (create_fk, abs, heights).
//!
//! **Intended keys:** Class A append ids, body abs offsets, block heights — dense
//! sequential `u64` / `u32` / [`Fk`]. Those land on consecutive hashbrown buckets
//! under a power-of-two mask → even occupancy. **Avoid** for keys that share low
//! bits (aligned pointers) or for cryptographic hashes (`[u8; 32]` / txid).

use rbitcoin_primitives::Fk;
use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hasher};

/// Identity hasher: `finish()` is the key itself (no SipHash mix).
#[derive(Default, Clone, Copy)]
pub struct U64IdentityHasher(u64);

impl Hasher for U64IdentityHasher {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = self.0.wrapping_mul(0x1000_0000_01b3).wrapping_add(u64::from(b));
        }
    }
    #[inline]
    fn write_u64(&mut self, i: u64) {
        self.0 = i;
    }
    #[inline]
    fn write_u32(&mut self, i: u32) {
        self.0 = u64::from(i);
    }
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
}

/// `HashMap` with [`U64IdentityHasher`] for dense `u64` keys.
pub type U64Map<V> = HashMap<u64, V, BuildHasherDefault<U64IdentityHasher>>;

/// `HashSet` with [`U64IdentityHasher`] for dense `u64` keys.
pub type U64Set = HashSet<u64, BuildHasherDefault<U64IdentityHasher>>;

/// `HashMap` with [`U64IdentityHasher`] for dense `u32` keys (heights, etc.).
pub type U32Map<V> = HashMap<u32, V, BuildHasherDefault<U64IdentityHasher>>;

/// `HashMap` with [`U64IdentityHasher`] for [`Fk`] keys (single-field u64).
pub type FkMap<V> = HashMap<Fk, V, BuildHasherDefault<U64IdentityHasher>>;

/// `HashSet` with [`U64IdentityHasher`] for [`Fk`] keys.
pub type FkSet = HashSet<Fk, BuildHasherDefault<U64IdentityHasher>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn u64_identity_hasher_is_raw_key_and_map_roundtrips_pack_scale() {
        let mut h = U64IdentityHasher::default();
        h.write_u64(0xdead_beef_cafe_u64);
        assert_eq!(h.finish(), 0xdead_beef_cafe_u64);

        let n = 8_000u64;
        let mut m: U64Map<u32> = U64Map::with_capacity_and_hasher(n as usize, Default::default());
        for i in 1..=n {
            m.insert(i, (i % 1_000_000) as u32);
        }
        assert_eq!(m.len(), n as usize);
        for i in 1..=n {
            assert_eq!(m.get(&i).copied(), Some((i % 1_000_000) as u32));
        }
        assert_eq!(m.get(&0), None);
        assert_eq!(m.get(&(n + 1)), None);

        let mut s: U64Set = U64Set::with_capacity_and_hasher(n as usize, Default::default());
        for i in 1..=n {
            s.insert(i);
        }
        assert_eq!(s.len(), n as usize);
        assert!(s.contains(&1));
        assert!(!s.contains(&(n + 1)));

        let mut fm: FkMap<u8> = FkMap::default();
        fm.insert(Fk(42), 7);
        assert_eq!(fm.get(&Fk(42)).copied(), Some(7));

        let mut fs: FkSet = FkSet::default();
        assert!(fs.insert(Fk(1)));
        assert!(!fs.insert(Fk(1)));
        assert!(fs.contains(&Fk(1)));
    }
}
