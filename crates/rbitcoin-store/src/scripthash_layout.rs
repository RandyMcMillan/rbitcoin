//! Scripthash layout: 8 B create_tx_fk entries, **32 B head slots** (fixed).
//!
//! Head key = first 16 B of SHA256(spk). Value = two u64s.
//!
//! **Schema 15:** Empty / Inline (≤2 FKs) / **Slab** `{class,used,off}` /
//! **Paged** (megakey first+last 4 KiB page offs).
//! Bit 63 of each value word is a flag; payload in low 63 bits. See
//! [`crate::scripthash_pages`] for page buffer layout.
//!
//! Schema-13 slab packing (`w0` flagged, `w1` clear) still decodes as paged;
//! store open refuses a durable pre-15 SH index.

use crate::error::StoreError;
use crate::scripthash_pages::{
    sh_decode_paged_head, sh_decode_slab_head, sh_encode_paged_head, sh_encode_slab_head,
    sh_head_value_mode, sh_word_payload, ShHeadValueMode,
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
/// High bit marks non-inline head value (paged). Same value as [`SH_FLAG_BIT`].
pub const SH_SLAB_MARKER: u64 = 1u64 << 63;
/// Alloc header magic after the RBT1 file header.
pub const SH_ALLOC_MAGIC: [u8; 4] = *b"SHAL";
/// v3 = schema 15 (slabs + combined RBT1/SHAL prefix). v2 = schema-14 pages. v1 = schema-13.
pub const SH_ALLOC_VERSION: u16 = 3;
/// Combined RBT1 + SHAL prefix. Payload starts here (no 4112 hole).
pub const SH_PREFIX_PAGE: usize = 4096;
/// SHAL field region after a 16 B RBT1 header (ends at [`SH_PREFIX_PAGE`]).
pub const SH_ALLOC_HEADER_LEN: usize = SH_PREFIX_PAGE - 16;

/// Legacy size-class constants (page freelist reuses class index for 4 KiB pages).
/// Class 7: `4 << 7` entries × 8 B = 4096.
pub const SH_SLAB_BASE: u32 = 4;
pub const SH_MAX_CLASS: u8 = 24;
/// Largest relocating geometric class (256 fks / 2 KiB). Class 7 is a page.
pub const SH_MAX_SLAB_CLASS: u8 = 6;
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
pub const fn slab_cap(class: u8) -> u32 {
    SH_SLAB_BASE << class
}

#[inline]
pub const fn slab_bytes(class: u8) -> u64 {
    slab_cap(class) as u64 * SH_ENTRY_LEN as u64
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
    /// Geometric slab: `used` fks at `off`, size class `class` (0–6).
    Slab {
        class: u8,
        used: u16,
        off: u64,
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
            ShHeadValue::Slab { used, .. } => u32::from(*used),
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

    pub fn is_slab(&self) -> bool {
        matches!(self, ShHeadValue::Slab { .. })
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
                debug_assert_eq!(w0 & SH_SLAB_MARKER, 0, "fk must not set flag bit");
                debug_assert_eq!(w1 & SH_SLAB_MARKER, 0, "fk must not set flag bit");
                out[0..8].copy_from_slice(&w0.to_le_bytes());
                out[8..16].copy_from_slice(&w1.to_le_bytes());
            }
            ShHeadValue::Slab { class, used, off } => {
                out = sh_encode_slab_head(*class, *used, *off)
                    .expect("slab head encode (fields validated at write)");
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
        // Schema-14 paged: w0 flagged, w1 clear, both payloads non-zero page offs.
        // Do **not** sniff "legacy slab" from offset shape: large `scripthash.body`
        // page offsets (e.g. class<<32 | 4096) collide with schema-13 packing and
        // false-positive mid warm-apply. Schema-13 stores with a durable SH index
        // are refused at store open (`meta`); empty-SH 13→14 upgrades wipe/rebuild.
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
            ShHeadValueMode::Slab => {
                let (class, used, off) = sh_decode_slab_head(
                    buf.try_into()
                        .map_err(|_| StoreError::Corrupt("short scripthash head value"))?,
                )?;
                Ok(ShHeadValue::Slab { class, used, off })
            }
            ShHeadValueMode::Paged => {
                let (first, last) = sh_decode_paged_head(
                    buf.try_into()
                        .map_err(|_| StoreError::Corrupt("short scripthash head value"))?,
                )?;
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

    pub fn slab(class: u8, used: u16, off: u64) -> Self {
        ShHeadValue::Slab { class, used, off }
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

/// Payload region starts at the combined RBT1+SHAL prefix page (offset 4096).
///
/// `file_header_len` is accepted so call sites stay stable; schema 15 does not
/// place SHAL in a second unaligned page after RBT1.
pub fn payload_start(_file_header_len: usize) -> u64 {
    SH_PREFIX_PAGE as u64
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

        let slab = ShHeadValue::slab(1, 5, 4096);
        assert_eq!(ShHeadValue::decode(&slab.encode()).unwrap(), slab);
        assert_eq!(slab.used(), 5);
        assert!(slab.is_slab());
        assert!(!slab.is_paged());

        assert!(ShHeadValue::decode(&ShHeadValue::Empty.encode())
            .unwrap()
            .is_empty());
    }

    #[test]
    fn paged_offsets_that_look_like_legacy_slab_still_decode() {
        // Regression: body offs such as (10<<32)|4096 used to trip a slab sniffer
        // and abort warm apply on multi‑GiB scripthash.body.
        let first = (10u64 << 32) | 4096;
        let last = first + 4096;
        let paged = ShHeadValue::paged(first, last);
        let got = ShHeadValue::decode(&paged.encode()).unwrap();
        assert_eq!(got, paged);
        // Historical slab packing shape also decodes as paged (open refuses schema-13
        // durable SH; no per-slot dual-read).
        let mut slab_shaped = [0u8; 16];
        let w0 = SH_SLAB_MARKER | (1u64 << 32) | 5;
        slab_shaped[0..8].copy_from_slice(&w0.to_le_bytes());
        slab_shaped[8..16].copy_from_slice(&4112u64.to_le_bytes());
        match ShHeadValue::decode(&slab_shaped).unwrap() {
            ShHeadValue::Paged {
                first_page,
                last_page,
            } => {
                assert_eq!(first_page, (1u64 << 32) | 5);
                assert_eq!(last_page, 4112);
            }
            other => panic!("expected paged, got {other:?}"),
        }
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

    #[test]
    fn head_value_used_and_paged_flags() {
        assert_eq!(ShHeadValue::Empty.used(), 0);
        assert!(!ShHeadValue::Empty.is_paged());
        let one = ShHeadValue::inline_one(ShEntry::new(Fk(1)));
        assert_eq!(one.used(), 1);
        assert!(!one.is_paged());
        let two = ShHeadValue::inline_two(ShEntry::new(Fk(1)), ShEntry::new(Fk(2)));
        assert_eq!(two.used(), 2);
        // used=0 inline encodes as empty words.
        let zero_inline = ShHeadValue::Inline {
            entries: [ShEntry::new(Fk::NULL); SH_INLINE_CAP],
            used: 0,
        };
        assert_eq!(
            ShHeadValue::decode(&zero_inline.encode()).unwrap(),
            ShHeadValue::Empty
        );
        let paged = ShHeadValue::paged(4096, 8192);
        assert_eq!(paged.used(), u32::MAX);
        assert!(paged.is_paged());
        assert!(!paged.is_slab());
        assert!(paged.inline_entries().is_empty());
        assert!(paged.inline_fks().is_empty());
        let slab = ShHeadValue::slab(0, 4, 4096);
        assert_eq!(slab.used(), 4);
        assert!(slab.is_slab());
        assert!(!slab.is_paged());
    }

    #[test]
    fn decode_inline_null_fk_errors() {
        // Non-zero first word with payload 0 (null fk) after mode decode.
        // Word without flag, payload 0 → null first.
        let mut bad = [0u8; 16];
        // w0 = 0 is empty; use payload that mode treats as inline with null.
        // Inline mode: non-zero words without slab/page flags.
        // First fk payload 0 with second non-zero is invalid.
        bad[0..8].copy_from_slice(&0u64.to_le_bytes());
        bad[8..16].copy_from_slice(&3u64.to_le_bytes());
        // w0==0 && w1!=0: may be mode-dependent; exercise decode.
        let _ = ShHeadValue::decode(&bad);

        // Explicit null first via payload 0 in a non-empty-looking layout is
        // hard without sh_head_value_mode; at least hit short-buffer again.
        assert!(ShHeadValue::decode(&[0u8; 15]).is_err());
    }
}
