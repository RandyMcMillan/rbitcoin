//! Scripthash layout: 8 B create_tx_fk entries, **32 B head slots** (fixed).
//!
//! Head key = first 16 B of SHA256(spk). Value = two u64s.
//!
//! **Schema 14:** Empty / Inline (≤2 FKs) / **Paged** (first+last 4 KiB page offs).
//! Bit 63 of each value word is a flag; payload in low 63 bits. See
//! [`crate::scripthash_pages`] for page buffer layout.
//!
//! Schema-13 **slab** encoding is rejected on decode (rebuild SH on upgrade).

use crate::error::StoreError;
use crate::scripthash_pages::{
    sh_decode_paged_head, sh_encode_paged_head, sh_head_value_mode, sh_word_payload,
    ShHeadValueMode, SH_FLAG_BIT,
};
use rbitcoin_primitives::Fk;

/// Body / page entry: create Class A fk only.
pub const SH_ENTRY_LEN: usize = 8;
/// Max create_tx_fks stored inline in the head value.
pub const SH_INLINE_CAP: usize = 2;
/// Head key length (prefix of Electrum SHA256(spk)).
pub const SH_HEAD_KEY_LEN: usize = 16;
/// Head value: two u64s.
pub const SH_HEAD_VALUE_LEN: usize = 16;
/// On-disk head slot size.
pub const SH_HEAD_SLOT_SIZE: usize = SH_HEAD_KEY_LEN + SH_HEAD_VALUE_LEN;
/// High bit marks non-inline head value (paged). Same bit as legacy slab marker.
pub const SH_SLAB_MARKER: u64 = SH_FLAG_BIT;
/// Alloc header magic after the RBT1 file header.
pub const SH_ALLOC_MAGIC: [u8; 4] = *b"SHAL";
pub const SH_ALLOC_VERSION: u16 = 2;
/// Fixed alloc control page (includes freelist heads).
pub const SH_ALLOC_HEADER_LEN: usize = 4096;

/// Legacy size-class constants (page freelist reuses class index for 4 KiB pages).
/// Class 7: `4 << 7` entries × 8 B = 4096.
pub const SH_SLAB_BASE: u32 = 4;
pub const SH_MAX_CLASS: u8 = 24;
/// Slab class whose byte size equals one SH page ([`crate::scripthash_pages::SH_PAGE_SIZE`]).
pub const SH_PAGE_SLAB_CLASS: u8 = 7;

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
    /// 4 KiB page chain; head stores first and last page file offsets only.
    Paged {
        first_page: u64,
        last_page: u64,
    },
}

impl ShHeadValue {
    pub fn used(&self) -> u32 {
        match self {
            ShHeadValue::Empty => 0,
            ShHeadValue::Inline { used, .. } => u32::from(*used),
            // Count not stored in head; callers that need n walk pages.
            ShHeadValue::Paged { .. } => u32::MAX,
        }
    }

    pub fn is_empty(&self) -> bool {
        matches!(self, ShHeadValue::Empty)
    }

    pub fn is_paged(&self) -> bool {
        matches!(self, ShHeadValue::Paged { .. })
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
                debug_assert_eq!(w0 & SH_FLAG_BIT, 0, "fk must not set flag bit");
                debug_assert_eq!(w1 & SH_FLAG_BIT, 0, "fk must not set flag bit");
                out[0..8].copy_from_slice(&w0.to_le_bytes());
                out[8..16].copy_from_slice(&w1.to_le_bytes());
            }
            ShHeadValue::Paged {
                first_page,
                last_page,
            } => {
                out = sh_encode_paged_head(*first_page, *last_page)
                    .expect("paged head encode (offs validated at write)");
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
        // Schema-13 slab: w0 = SH_SLAB_MARKER | class<<32 | used (bits 40..62 clear), w1 = off.
        // Paged first_page is a file offset (typically ≥ payload_start ≈ 4KiB+); refuse
        // the historical packing shape so old heads never decode as paged.
        if (w0 & SH_SLAB_MARKER) != 0 {
            let bits40_62_clear = (w0 & 0x7fff_ff00_0000_0000) == 0;
            let class = ((w0 >> 32) & 0xff) as u8;
            let used = (w0 & 0xffff_ffff) as u32;
            // Historical slab: used ≤ class capacity (small). Real page offs are ≥4KiB.
            if bits40_62_clear && class <= SH_MAX_CLASS && used > 0 && used <= slab_cap(class) {
                return Err(StoreError::Corrupt(
                    "scripthash: legacy slab head value (rebuild scripthash index)",
                ));
            }
            let arr: &[u8; 16] = buf.try_into().map_err(|_| {
                StoreError::Corrupt("short scripthash head value")
            })?;
            let (first, last) = sh_decode_paged_head(arr)?;
            return Ok(ShHeadValue::Paged {
                first_page: first,
                last_page: last,
            });
        }
        match sh_head_value_mode(w0, w1)? {
            ShHeadValueMode::Empty => Ok(ShHeadValue::Empty),
            ShHeadValueMode::Inline => {
                let e0 = ShEntry::new(Fk(sh_word_payload(w0)));
                if e0.is_null() {
                    return Err(StoreError::Corrupt("inline null first fk"));
                }
                if sh_word_payload(w1) == 0 {
                    return Ok(ShHeadValue::inline_one(e0));
                }
                let e1 = ShEntry::new(Fk(sh_word_payload(w1)));
                if e1.is_null() {
                    return Err(StoreError::Corrupt("inline null second fk"));
                }
                Ok(ShHeadValue::inline_two(e0, e1))
            }
            ShHeadValueMode::Paged => {
                let (first, last) = sh_decode_paged_head(buf.try_into().map_err(|_| {
                    StoreError::Corrupt("short scripthash head value")
                })?)?;
                Ok(ShHeadValue::Paged {
                    first_page: first,
                    last_page: last,
                })
            }
        }
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

    pub fn paged(first_page: u64, last_page: u64) -> Self {
        ShHeadValue::Paged {
            first_page,
            last_page,
        }
    }

    /// Collect live entries from an inline value (oldest→newest).
    pub fn inline_entries(&self) -> &[ShEntry] {
        match self {
            ShHeadValue::Inline { entries, used } => &entries[..*used as usize],
            _ => &[],
        }
    }

    /// All create_tx_fks in this value (inline only; paged needs body read).
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
    fn head_value_roundtrip_inline_paged() {
        let e0 = ShEntry::new(Fk(3));
        let e1 = ShEntry::new(Fk(4));
        let inline = ShHeadValue::inline_two(e0, e1);
        assert_eq!(ShHeadValue::decode(&inline.encode()).unwrap(), inline);

        let one = ShHeadValue::inline_one(e0);
        assert_eq!(ShHeadValue::decode(&one.encode()).unwrap(), one);

        let paged = ShHeadValue::paged(4096, 8192);
        assert_eq!(ShHeadValue::decode(&paged.encode()).unwrap(), paged);

        assert!(ShHeadValue::decode(&ShHeadValue::Empty.encode())
            .unwrap()
            .is_empty());
    }

    #[test]
    fn legacy_slab_bytes_rejected() {
        // Schema-13 slab: FLAG | class<<32 | used, w1 = slab_off
        let mut bad = [0u8; 16];
        let w0 = SH_SLAB_MARKER | (1u64 << 32) | 5;
        bad[0..8].copy_from_slice(&w0.to_le_bytes());
        bad[8..16].copy_from_slice(&4112u64.to_le_bytes());
        let err = ShHeadValue::decode(&bad).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("legacy slab") || msg.contains("rebuild"),
            "{msg}"
        );
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
    fn page_class_is_4k() {
        assert_eq!(slab_bytes(SH_PAGE_SLAB_CLASS), 4096);
    }

    #[test]
    fn layout_error_paths() {
        assert!(matches!(
            ShEntry::decode(&[0u8; 4]),
            Err(StoreError::Corrupt(_))
        ));
        assert!(ShEntry::new(Fk::NULL).is_null());
        assert!(matches!(
            ShHeadValue::decode(&[0u8; 8]),
            Err(StoreError::Corrupt(_))
        ));
        let one = ShHeadValue::inline_one(ShEntry::new(Fk(9)));
        assert_eq!(one.inline_fks(), vec![Fk(9)]);
        assert!(ShHeadValue::Empty.inline_entries().is_empty());
        assert_eq!(payload_start(16), 16 + SH_ALLOC_HEADER_LEN as u64);
    }
}
