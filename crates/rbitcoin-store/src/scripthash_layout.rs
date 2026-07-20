//! Hybrid scripthash layout: thin create entries, inline head or geometric slab.
//!
//! Entry = `create_tx_fk:u64 | vout:u32` (12 B). Head holds either ≤2 inline entries
//! or a pointer to one body slab (cap 4, 8, 16, …).

use crate::error::StoreError;
use rbitcoin_primitives::Fk;

/// Packed create outpoint (no `next` pointer).
pub const SH_ENTRY_LEN: usize = 12;
/// Max creates stored in the head value without a body slab.
pub const SH_INLINE_CAP: usize = 2;
/// Size class 0 capacity; class `c` has capacity `SH_SLAB_BASE << c`.
pub const SH_SLAB_BASE: u32 = 4;
/// Max size class: cap = 4 << c.
///
/// Class 18 = 2^20 entries (~12 MiB) is not enough for a few mainnet exchange
/// deposit scripts (migrate hit “entry count too large” past ~16.7M keys).
/// Class 24 = 2^26 entries (~805 MiB slab) covers pathological histories.
pub const SH_MAX_CLASS: u8 = 24;
/// Head value payload (tag + data); slot = key[32] + value[32] = 64 B.
pub const SH_HEAD_VALUE_LEN: usize = 32;
/// On-disk head slot size.
pub const SH_HEAD_SLOT_SIZE: usize = 32 + SH_HEAD_VALUE_LEN;
/// Alloc header magic after the RBT1 file header.
pub const SH_ALLOC_MAGIC: [u8; 4] = *b"SHAL";
pub const SH_ALLOC_VERSION: u16 = 1;
/// Fixed alloc control page (includes freelist heads).
pub const SH_ALLOC_HEADER_LEN: usize = 4096;

/// Legacy v3 linked-list body row length (migrate only).
pub const SH_V3_RECORD_LEN: usize = 20;

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
    // 4 << c >= n  ⇒  c >= ceil(log2(n)) - 2
    let mut c = 0u8;
    while c <= SH_MAX_CLASS {
        if slab_cap(c) >= n {
            return Some(c);
        }
        c += 1;
    }
    None
}

/// Max creates storable in one slab under current [`SH_MAX_CLASS`].
pub fn max_slab_entries() -> u32 {
    slab_cap(SH_MAX_CLASS)
}

/// One thin create: `create_tx_fk` + `vout`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ShEntry {
    pub create_tx_fk: Fk,
    pub vout: u32,
}

impl ShEntry {
    pub fn new(create_tx_fk: Fk, vout: u32) -> Self {
        Self {
            create_tx_fk,
            vout,
        }
    }

    pub fn encode(self) -> [u8; SH_ENTRY_LEN] {
        let mut out = [0u8; SH_ENTRY_LEN];
        out[0..8].copy_from_slice(&self.create_tx_fk.0.to_le_bytes());
        out[8..12].copy_from_slice(&self.vout.to_le_bytes());
        out
    }

    pub fn decode(buf: &[u8]) -> Result<Self, StoreError> {
        if buf.len() < SH_ENTRY_LEN {
            return Err(StoreError::Corrupt("short scripthash entry"));
        }
        Ok(Self {
            create_tx_fk: Fk(u64::from_le_bytes(buf[0..8].try_into().unwrap())),
            vout: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
        })
    }

    pub fn is_null(self) -> bool {
        self.create_tx_fk.is_null()
    }
}

const TAG_EMPTY: u8 = 0;
const TAG_INLINE: u8 = 1;
const TAG_SLAB: u8 = 2;

/// Durable head value for one scripthash key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ShHeadValue {
    /// No creates (soft-deleted head slot or absent).
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
                out[0] = TAG_INLINE;
                out[1] = *used;
                if *used >= 1 {
                    out[4..16].copy_from_slice(&entries[0].encode());
                }
                if *used >= 2 {
                    out[16..28].copy_from_slice(&entries[1].encode());
                }
            }
            ShHeadValue::Slab {
                class,
                used,
                slab_off,
            } => {
                out[0] = TAG_SLAB;
                out[1] = *class;
                out[2..6].copy_from_slice(&used.to_le_bytes());
                out[6..14].copy_from_slice(&slab_off.to_le_bytes());
            }
        }
        out
    }

    pub fn decode(buf: &[u8]) -> Result<Self, StoreError> {
        if buf.len() < SH_HEAD_VALUE_LEN {
            return Err(StoreError::Corrupt("short scripthash head value"));
        }
        match buf[0] {
            TAG_EMPTY => Ok(ShHeadValue::Empty),
            TAG_INLINE => {
                let used = buf[1];
                if used == 0 || used as usize > SH_INLINE_CAP {
                    return Err(StoreError::Corrupt("bad inline used count"));
                }
                let mut entries = [ShEntry::new(Fk::NULL, 0); SH_INLINE_CAP];
                entries[0] = ShEntry::decode(&buf[4..16])?;
                if used >= 2 {
                    entries[1] = ShEntry::decode(&buf[16..28])?;
                }
                Ok(ShHeadValue::Inline { entries, used })
            }
            TAG_SLAB => {
                let class = buf[1];
                if class > SH_MAX_CLASS {
                    return Err(StoreError::Corrupt("bad slab class"));
                }
                let used = u32::from_le_bytes(buf[2..6].try_into().unwrap());
                let slab_off = u64::from_le_bytes(buf[6..14].try_into().unwrap());
                if used == 0 || used > slab_cap(class) {
                    return Err(StoreError::Corrupt("bad slab used count"));
                }
                if slab_off == 0 {
                    return Err(StoreError::Corrupt("null slab offset"));
                }
                Ok(ShHeadValue::Slab {
                    class,
                    used,
                    slab_off,
                })
            }
            _ => Err(StoreError::Corrupt("unknown scripthash head tag")),
        }
    }

    pub fn inline_one(e: ShEntry) -> Self {
        let mut entries = [ShEntry::new(Fk::NULL, 0); SH_INLINE_CAP];
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
        assert_eq!(class_for_count(8), Some(1));
        assert_eq!(class_for_count(9), Some(2));
        assert_eq!(class_for_count(100), Some(5)); // 4<<5 = 128
        assert_eq!(class_for_count(1_048_576), Some(18)); // 4<<18 = 2^20
        assert_eq!(class_for_count(1_048_577), Some(19));
        assert_eq!(class_for_count(max_slab_entries()), Some(SH_MAX_CLASS));
        assert!(class_for_count(max_slab_entries().saturating_add(1)).is_none());
        assert_eq!(slab_cap(0), 4);
        assert_eq!(slab_cap(1), 8);
        assert_eq!(slab_bytes(0), 48);
    }

    #[test]
    fn head_value_roundtrip() {
        let e0 = ShEntry::new(Fk(3), 0);
        let e1 = ShEntry::new(Fk(4), 1);
        let inline = ShHeadValue::inline_two(e0, e1);
        assert_eq!(ShHeadValue::decode(&inline.encode()).unwrap(), inline);

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
        let e = ShEntry::new(Fk(0xdead_beef), 42);
        assert_eq!(ShEntry::decode(&e.encode()).unwrap(), e);
    }
}
