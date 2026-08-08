//! Hybrid scripthash layout: 8 B create_tx_fk entries, **32 B head slots** (fixed).
//!
//! Head key = first 16 B of SHA256(spk). Value = two u64s.
//!
//! **Live (schema 13):** inline FKs or **slab** meta ([`SH_SLAB_MARKER`] on w0).
//!
//! **Target (schema 14 plan):** same 32 B slots; body uses **4 KiB page chains**
//! instead of relocating slabs. Head packing and page layout are pinned in
//! [`crate::scripthash_pages`] (Step 0). Live `ShHeadValue` still encodes slabs
//! until later plan steps rewire put/entries.

use crate::error::StoreError;
use rbitcoin_primitives::Fk;

/// Body / slab entry: create Class A fk only.
pub const SH_ENTRY_LEN: usize = 8;
/// Max create_tx_fks stored inline in the head value.
pub const SH_INLINE_CAP: usize = 2;
/// Size class 0 capacity; class `c` has capacity `SH_SLAB_BASE << c`.
pub const SH_SLAB_BASE: u32 = 4;
/// Max size class: cap = 4 << c (class 24 ≈ 2^26 entries).
pub const SH_MAX_CLASS: u8 = 24;
/// Head key length (prefix of Electrum SHA256(spk)).
pub const SH_HEAD_KEY_LEN: usize = 16;
/// Head value: two u64s.
pub const SH_HEAD_VALUE_LEN: usize = 16;
/// On-disk head slot size.
pub const SH_HEAD_SLOT_SIZE: usize = SH_HEAD_KEY_LEN + SH_HEAD_VALUE_LEN;
/// High bit of first value word marks slab mode (Class A fks stay < 2^63).
pub const SH_SLAB_MARKER: u64 = 1u64 << 63;
/// Alloc header magic after the RBT1 file header.
pub const SH_ALLOC_MAGIC: [u8; 4] = *b"SHAL";
pub const SH_ALLOC_VERSION: u16 = 1;
/// Fixed alloc control page (includes freelist heads).
pub const SH_ALLOC_HEADER_LEN: usize = 4096;

pub type ShHeadKey = [u8; SH_HEAD_KEY_LEN];

/// Truncate full Electrum scripthash (32 B) to head key (16 B).
#[inline]
pub fn head_key_from_full(full: &[u8; 32]) -> ShHeadKey {
    let mut k = [0u8; SH_HEAD_KEY_LEN];
    k.copy_from_slice(&full[0..SH_HEAD_KEY_LEN]);
    k
}

#[inline]
pub fn slab_cap(class: u8) -> u32 {
    SH_SLAB_BASE << class
}

#[inline]
pub fn slab_bytes(class: u8) -> u64 {
    u64::from(slab_cap(class)) * SH_ENTRY_LEN as u64
}

/// Smallest size class with `slab_cap(c) >= n`, or `None` if `n <= INLINE_CAP`
/// or `n` exceeds [`SH_MAX_CLASS`] capacity.
pub fn class_for_count(n: u32) -> Option<u8> {
    if n <= SH_INLINE_CAP as u32 {
        return None;
    }
    let mut c = 0u8;
    while c <= SH_MAX_CLASS {
        if slab_cap(c) >= n {
            return Some(c);
        }
        c += 1;
    }
    None
}

/// One thin create: create_tx_fk only (vout recovered from Class A).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ShEntry {
    pub create_tx_fk: Fk,
}

impl ShEntry {
    pub fn new(create_tx_fk: Fk) -> Self {
        Self { create_tx_fk }
    }

    pub fn encode(self) -> [u8; SH_ENTRY_LEN] {
        self.create_tx_fk.0.to_le_bytes()
    }

    pub fn decode(buf: &[u8]) -> Result<Self, StoreError> {
        if buf.len() < SH_ENTRY_LEN {
            return Err(StoreError::Corrupt("short scripthash entry"));
        }
        Ok(Self {
            create_tx_fk: Fk(u64::from_le_bytes(buf[0..8].try_into().unwrap())),
        })
    }

    pub fn is_null(self) -> bool {
        self.create_tx_fk.is_null()
    }
}

/// Durable head value for one scripthash key (16 B on disk).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ShHeadValue {
    Empty,
    Inline {
        entries: [ShEntry; SH_INLINE_CAP],
        used: u8,
    },
    Slab {
        class: u8,
        used: u32,
        /// File-absolute offset of packed entries.
        slab_off: u64,
    },
}

impl ShHeadValue {
    pub fn used(&self) -> u32 {
        match self {
            ShHeadValue::Empty => 0,
            ShHeadValue::Inline { used, .. } => u32::from(*used),
            ShHeadValue::Slab { used, .. } => *used,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.used() == 0
    }

    pub fn encode(&self) -> [u8; SH_HEAD_VALUE_LEN] {
        let mut out = [0u8; SH_HEAD_VALUE_LEN];
        match self {
            ShHeadValue::Empty => {}
            ShHeadValue::Inline { entries, used } => {
                let w0 = if *used >= 1 {
                    entries[0].create_tx_fk.0
                } else {
                    0
                };
                let w1 = if *used >= 2 {
                    entries[1].create_tx_fk.0
                } else {
                    0
                };
                debug_assert_eq!(w0 & SH_SLAB_MARKER, 0, "fk must not set slab marker");
                debug_assert_eq!(w1 & SH_SLAB_MARKER, 0, "fk must not set slab marker");
                out[0..8].copy_from_slice(&w0.to_le_bytes());
                out[8..16].copy_from_slice(&w1.to_le_bytes());
            }
            ShHeadValue::Slab {
                class,
                used,
                slab_off,
            } => {
                // w0: marker | class:u8 | used:u32 in low bits
                let mut w0 = SH_SLAB_MARKER;
                w0 |= u64::from(*class) << 32;
                w0 |= u64::from(*used) & 0xffff_ffff;
                out[0..8].copy_from_slice(&w0.to_le_bytes());
                out[8..16].copy_from_slice(&slab_off.to_le_bytes());
            }
        }
        out
    }

    pub fn decode(buf: &[u8]) -> Result<Self, StoreError> {
        if buf.len() < SH_HEAD_VALUE_LEN {
            return Err(StoreError::Corrupt("short scripthash head value"));
        }
        let w0 = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let w1 = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        if w0 == 0 && w1 == 0 {
            return Ok(ShHeadValue::Empty);
        }
        if w0 & SH_SLAB_MARKER != 0 {
            let class = ((w0 >> 32) & 0xff) as u8;
            if class > SH_MAX_CLASS {
                return Err(StoreError::Corrupt("bad slab class"));
            }
            let used = (w0 & 0xffff_ffff) as u32;
            let slab_off = w1;
            if used == 0 || used > slab_cap(class) {
                return Err(StoreError::Corrupt("bad slab used count"));
            }
            if slab_off == 0 {
                return Err(StoreError::Corrupt("null slab offset"));
            }
            return Ok(ShHeadValue::Slab {
                class,
                used,
                slab_off,
            });
        }
        // Inline
        let e0 = ShEntry::new(Fk(w0));
        if e0.is_null() {
            return Err(StoreError::Corrupt("inline null first fk"));
        }
        if w1 == 0 {
            return Ok(ShHeadValue::inline_one(e0));
        }
        let e1 = ShEntry::new(Fk(w1));
        if e1.is_null() {
            return Err(StoreError::Corrupt("inline null second fk"));
        }
        Ok(ShHeadValue::inline_two(e0, e1))
    }

    pub fn inline_one(e: ShEntry) -> Self {
        let mut entries = [ShEntry::new(Fk::NULL); SH_INLINE_CAP];
        entries[0] = e;
        ShHeadValue::Inline { entries, used: 1 }
    }

    pub fn inline_two(e0: ShEntry, e1: ShEntry) -> Self {
        ShHeadValue::Inline {
            entries: [e0, e1],
            used: 2,
        }
    }

    /// Collect live entries from an inline value (oldest→newest).
    pub fn inline_entries(&self) -> &[ShEntry] {
        match self {
            ShHeadValue::Inline { entries, used } => &entries[..*used as usize],
            _ => &[],
        }
    }

    /// All create_tx_fks in this value (inline only; slab needs body read).
    pub fn inline_fks(&self) -> Vec<Fk> {
        self.inline_entries()
            .iter()
            .map(|e| e.create_tx_fk)
            .collect()
    }
}

/// Payload region starts after RBT1 header + alloc page.
pub fn payload_start(file_header_len: usize) -> u64 {
    (file_header_len + SH_ALLOC_HEADER_LEN) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn class_for_count_ladder() {
        assert_eq!(class_for_count(0), None);
        assert_eq!(class_for_count(1), None);
        assert_eq!(class_for_count(2), None);
        assert_eq!(class_for_count(3), Some(0));
        assert_eq!(class_for_count(4), Some(0));
        assert_eq!(class_for_count(5), Some(1));
        assert_eq!(slab_bytes(0), 32); // 4 * 8
        assert_eq!(slab_cap(0), 4);
    }

    #[test]
    fn head_value_roundtrip() {
        let e0 = ShEntry::new(Fk(3));
        let e1 = ShEntry::new(Fk(4));
        let inline = ShHeadValue::inline_two(e0, e1);
        assert_eq!(ShHeadValue::decode(&inline.encode()).unwrap(), inline);

        let one = ShHeadValue::inline_one(e0);
        assert_eq!(ShHeadValue::decode(&one.encode()).unwrap(), one);

        let slab = ShHeadValue::Slab {
            class: 1,
            used: 5,
            slab_off: 4112,
        };
        assert_eq!(ShHeadValue::decode(&slab.encode()).unwrap(), slab);

        assert!(ShHeadValue::decode(&ShHeadValue::Empty.encode())
            .unwrap()
            .is_empty());
    }

    #[test]
    fn entry_roundtrip() {
        let e = ShEntry::new(Fk(0xdead_beef));
        assert_eq!(ShEntry::decode(&e.encode()).unwrap(), e);
    }

    #[test]
    fn head_key_prefix() {
        let full = [0xabu8; 32];
        let k = head_key_from_full(&full);
        assert_eq!(k.len(), 16);
        assert_eq!(&k[..], &full[..16]);
    }

    #[test]
    fn layout_error_and_edge_paths() {
        // class_for_count saturates at max class capacity.
        assert_eq!(class_for_count(slab_cap(SH_MAX_CLASS)), Some(SH_MAX_CLASS));
        assert_eq!(class_for_count(slab_cap(SH_MAX_CLASS) + 1), None);

        assert!(matches!(
            ShEntry::decode(&[0u8; 4]),
            Err(StoreError::Corrupt(_))
        ));
        assert!(ShEntry::new(Fk::NULL).is_null());
        assert!(!ShEntry::new(Fk(1)).is_null());

        assert!(matches!(
            ShHeadValue::decode(&[0u8; 8]),
            Err(StoreError::Corrupt(_))
        ));
        // Bad slab class
        let mut bad = [0u8; 16];
        let w0 = SH_SLAB_MARKER | (u64::from(SH_MAX_CLASS + 1) << 32) | 1;
        bad[0..8].copy_from_slice(&w0.to_le_bytes());
        bad[8..16].copy_from_slice(&4112u64.to_le_bytes());
        assert!(matches!(
            ShHeadValue::decode(&bad),
            Err(StoreError::Corrupt(_))
        ));
        // used == 0 with slab marker
        let mut bad = [0u8; 16];
        bad[0..8].copy_from_slice(&SH_SLAB_MARKER.to_le_bytes());
        bad[8..16].copy_from_slice(&4112u64.to_le_bytes());
        assert!(matches!(
            ShHeadValue::decode(&bad),
            Err(StoreError::Corrupt(_))
        ));
        // used > cap
        let mut bad = ShHeadValue::Slab {
            class: 0,
            used: 99,
            slab_off: 4112,
        }
        .encode();
        // force bad used via re-encode with hacked word
        let mut w0 = SH_SLAB_MARKER;
        w0 |= 99; // used
        bad[0..8].copy_from_slice(&w0.to_le_bytes());
        assert!(matches!(
            ShHeadValue::decode(&bad),
            Err(StoreError::Corrupt(_))
        ));
        // null slab offset
        let mut bad = ShHeadValue::Slab {
            class: 0,
            used: 1,
            slab_off: 1,
        }
        .encode();
        bad[8..16].copy_from_slice(&0u64.to_le_bytes());
        assert!(matches!(
            ShHeadValue::decode(&bad),
            Err(StoreError::Corrupt(_))
        ));
        // inline null first
        let mut bad = [0u8; 16];
        bad[0..8].copy_from_slice(&0u64.to_le_bytes());
        bad[8..16].copy_from_slice(&1u64.to_le_bytes());
        // w0==0 && w1!=0 is not Empty — first is null → corrupt
        // Actually Empty is only when both zero. So this hits inline null first.
        assert!(matches!(
            ShHeadValue::decode(&bad),
            Err(StoreError::Corrupt(_))
        ));
        // inline used paths: one entry
        let one = ShHeadValue::inline_one(ShEntry::new(Fk(9)));
        assert_eq!(one.used(), 1);
        assert!(!one.is_empty());
        assert_eq!(one.inline_fks(), vec![Fk(9)]);
        // empty / slab inline_entries empty
        assert!(ShHeadValue::Empty.inline_entries().is_empty());
        assert!(ShHeadValue::Empty.inline_fks().is_empty());
        let slab = ShHeadValue::Slab {
            class: 0,
            used: 3,
            slab_off: 4112,
        };
        assert!(slab.inline_entries().is_empty());
        assert_eq!(slab.used(), 3);
        // Inline encode with used < 2 leaves w1=0
        let enc = one.encode();
        assert_eq!(ShHeadValue::decode(&enc).unwrap(), one);
        assert_eq!(payload_start(16), 16 + SH_ALLOC_HEADER_LEN as u64);
    }
}
