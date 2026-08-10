//! Schema-14 scripthash **page-chain** layout (wired to `put_create` / `entries`).
//!
//! # Head value (16 B, full slot stays **32 B** = key16 + value16)
//!
//! Two little-endian `u64` words `w0`, `w1`. **Bit 63** of each word is a flag;
//! payload lives in bits `0..62` ([`SH_PAYLOAD_MASK`]). Create FKs and body
//! offsets keep high bits zero for many years — same rule as
//! [`SH_SLAB_MARKER`](crate::scripthash_layout::SH_SLAB_MARKER).
//!
//! | Mode | `w0` | `w1` |
//! |------|------|------|
//! | **Empty** | `0` | `0` |
//! | **Inline** (1–2 FKs) | bit63=0, low63 = fk0 | bit63=0, low63 = fk1 or `0` |
//! | **Paged** | bit63=1, low63 = **first** page off | bit63=0, low63 = **last** page off |
//!
//! - No `used` / count in the head for paged mode (append RMW last page).
//! - Inline never sets bit63 (FK payload must be `< 2^63`).
//! - Paged always sets bit63 on `w0` only; `w1` bit63 reserved **0**.
//! - Schema-13 **slab** encoding also set bit63 on `w0` but packed class/used
//!   into the low half — schema 14+ refuses slab on decode; rebuild SH.
//!
//! # Body page (exactly [`SH_PAGE_SIZE`] = 4096, disk-aligned)
//!
//! ```text
//! offset  len   field
//! 0       8     next_page_off  (0 = end of chain); bit63 must be 0
//! 8       2     n_fks          (u16 LE); FKs stored in this page
//! 10      6     reserved       (zero)
//! 16      8*n   create_tx_fk[] (u64 LE each), n = n_fks
//! ```
//!
//! Max FKs per page: [`SH_PAGE_FK_CAP`] = (4096 − 16) / 8 = **510**.
//!
//! Chain is **singly linked** first → … → last. Head stores first+last for O(1)
//! walk start and O(1) append target.

use crate::error::StoreError;
use crate::scripthash_layout::{ShEntry, SH_ENTRY_LEN};
use rbitcoin_primitives::Fk;

/// Disk page size for SH FK chains (aligned allocations).
pub const SH_PAGE_SIZE: usize = 4096;
/// Bytes before the FK array in a page.
pub const SH_PAGE_HEADER_LEN: usize = 16;
/// Max create_tx_fks that fit in one page after the header.
pub const SH_PAGE_FK_CAP: usize = (SH_PAGE_SIZE - SH_PAGE_HEADER_LEN) / SH_ENTRY_LEN;

/// Bit 63: mode / reserved flag on head words and (must be clear on) offsets.
pub const SH_FLAG_BIT: u64 = 1u64 << 63;
/// Payload bits for FK or page offset (`value & SH_PAYLOAD_MASK`).
pub const SH_PAYLOAD_MASK: u64 = !SH_FLAG_BIT;

/// Offset of `next_page_off` within a page.
pub const SH_PAGE_OFF_NEXT: usize = 0;
/// Offset of `n_fks` (u16 LE) within a page.
pub const SH_PAGE_OFF_N_FKS: usize = 8;
/// Offset of first FK within a page.
pub const SH_PAGE_OFF_FKS: usize = SH_PAGE_HEADER_LEN;

const _: () = assert!(SH_PAGE_SIZE == 4096);
const _: () = assert!(SH_PAGE_HEADER_LEN == 16);
const _: () = assert!(SH_ENTRY_LEN == 8);
const _: () = assert!(SH_PAGE_FK_CAP == 510);
const _: () = assert!(SH_PAGE_OFF_FKS + SH_PAGE_FK_CAP * SH_ENTRY_LEN == SH_PAGE_SIZE);

/// Require a full 4 KiB page buffer (rejects unaligned / short slices).
#[inline]
pub fn sh_page_as_array(buf: &[u8]) -> Result<&[u8; SH_PAGE_SIZE], StoreError> {
    buf.try_into()
        .map_err(|_| StoreError::Corrupt("scripthash page: buffer must be exactly 4096 bytes"))
}

/// Mutable full-page view (rejects wrong length).
#[inline]
pub fn sh_page_as_array_mut(buf: &mut [u8]) -> Result<&mut [u8; SH_PAGE_SIZE], StoreError> {
    buf.try_into()
        .map_err(|_| StoreError::Corrupt("scripthash page: buffer must be exactly 4096 bytes"))
}

/// True if bit 63 is set.
#[inline]
pub fn sh_word_flagged(word: u64) -> bool {
    word & SH_FLAG_BIT != 0
}

/// Low 63 bits (FK or page offset payload).
#[inline]
pub fn sh_word_payload(word: u64) -> u64 {
    word & SH_PAYLOAD_MASK
}

/// Pack a payload into a word with bit 63 clear (inline FK or last-page off).
#[inline]
pub fn sh_pack_clear(payload: u64) -> Result<u64, StoreError> {
    if payload & SH_FLAG_BIT != 0 {
        return Err(StoreError::Corrupt(
            "scripthash: payload must have bit63 clear (fk/offset < 2^63)",
        ));
    }
    Ok(payload)
}

/// Pack a payload with bit 63 set (paged-mode `w0` / first-page word).
#[inline]
pub fn sh_pack_flagged(payload: u64) -> Result<u64, StoreError> {
    if payload & SH_FLAG_BIT != 0 {
        return Err(StoreError::Corrupt(
            "scripthash: payload must have bit63 clear before flagging",
        ));
    }
    Ok(payload | SH_FLAG_BIT)
}

/// Head value mode from raw `w0`/`w1` (schema-14).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShHeadValueMode {
    Empty,
    /// One or two inline FKs (bit63 clear on both words).
    Inline,
    /// Page chain: first in w0 payload, last in w1 payload; w0 flagged.
    Paged,
}

/// Classify a 16-byte head value without allocating (schema-14 rules).
///
/// **Note:** schema-13 slab bytes also have w0 flagged; [`ShHeadValue::decode`](crate::scripthash_layout::ShHeadValue::decode)
/// refuses historical slab packing before treating flagged words as paged.
#[inline]
pub fn sh_head_value_mode(w0: u64, w1: u64) -> Result<ShHeadValueMode, StoreError> {
    if w0 == 0 && w1 == 0 {
        return Ok(ShHeadValueMode::Empty);
    }
    if sh_word_flagged(w0) {
        // Paged: w1 must not be flagged; both payloads non-zero page offs.
        if sh_word_flagged(w1) {
            return Err(StoreError::Corrupt(
                "scripthash paged head: w1 flag bit reserved clear",
            ));
        }
        let first = sh_word_payload(w0);
        let last = sh_word_payload(w1);
        if first == 0 || last == 0 {
            return Err(StoreError::Corrupt(
                "scripthash paged head: null first/last page off",
            ));
        }
        return Ok(ShHeadValueMode::Paged);
    }
    if sh_word_flagged(w1) {
        return Err(StoreError::Corrupt(
            "scripthash inline head: w1 must not set flag bit",
        ));
    }
    // Inline: w0 is first fk (non-zero).
    if sh_word_payload(w0) == 0 {
        return Err(StoreError::Corrupt("scripthash inline head: null first fk"));
    }
    Ok(ShHeadValueMode::Inline)
}

/// Encode paged head words (first/last page file offsets).
#[inline]
pub fn sh_encode_paged_head(
    first_page_off: u64,
    last_page_off: u64,
) -> Result<[u8; 16], StoreError> {
    let w0 = sh_pack_flagged(first_page_off)?;
    let w1 = sh_pack_clear(last_page_off)?;
    if first_page_off == 0 || last_page_off == 0 {
        return Err(StoreError::Corrupt("scripthash paged head: null page off"));
    }
    let mut out = [0u8; 16];
    out[0..8].copy_from_slice(&w0.to_le_bytes());
    out[8..16].copy_from_slice(&w1.to_le_bytes());
    Ok(out)
}

/// Decode paged first/last from a 16-byte value (errors if not paged mode).
#[inline]
pub fn sh_decode_paged_head(buf: &[u8; 16]) -> Result<(u64, u64), StoreError> {
    let w0 = u64::from_le_bytes(buf[0..8].try_into().unwrap());
    let w1 = u64::from_le_bytes(buf[8..16].try_into().unwrap());
    match sh_head_value_mode(w0, w1)? {
        ShHeadValueMode::Paged => Ok((sh_word_payload(w0), sh_word_payload(w1))),
        _ => Err(StoreError::Corrupt("scripthash: expected paged head value")),
    }
}

/// Zero a page buffer and write header (`next=0`, `n_fks=0`).
#[inline]
pub fn sh_page_init_empty(page: &mut [u8; SH_PAGE_SIZE]) {
    // Ensure callers that pass unaligned slices go through mut view first.
    let _ = sh_page_as_array_mut(page);
    page.fill(0);
}

/// Read `next_page_off` from a page (0 = end).
#[inline]
pub fn sh_page_next(page: &[u8; SH_PAGE_SIZE]) -> Result<u64, StoreError> {
    let next = u64::from_le_bytes(
        page[SH_PAGE_OFF_NEXT..SH_PAGE_OFF_NEXT + 8]
            .try_into()
            .unwrap(),
    );
    if next & SH_FLAG_BIT != 0 {
        return Err(StoreError::Corrupt(
            "scripthash page next_off has flag bit set",
        ));
    }
    Ok(next)
}

/// Set `next_page_off` (bit63 must be clear).
#[inline]
pub fn sh_page_set_next(page: &mut [u8; SH_PAGE_SIZE], next_off: u64) -> Result<(), StoreError> {
    let w = sh_pack_clear(next_off)?;
    page[SH_PAGE_OFF_NEXT..SH_PAGE_OFF_NEXT + 8].copy_from_slice(&w.to_le_bytes());
    Ok(())
}

/// Number of FKs stored in this page.
#[inline]
pub fn sh_page_n_fks(page: &[u8; SH_PAGE_SIZE]) -> Result<u16, StoreError> {
    let n = u16::from_le_bytes(
        page[SH_PAGE_OFF_N_FKS..SH_PAGE_OFF_N_FKS + 2]
            .try_into()
            .unwrap(),
    );
    if n as usize > SH_PAGE_FK_CAP {
        return Err(StoreError::Corrupt("scripthash page n_fks > capacity"));
    }
    Ok(n)
}

#[inline]
fn sh_page_set_n_fks(page: &mut [u8; SH_PAGE_SIZE], n: u16) {
    page[SH_PAGE_OFF_N_FKS..SH_PAGE_OFF_N_FKS + 2].copy_from_slice(&n.to_le_bytes());
}

/// Entries currently stored in the page (oldest → newest within page).
///
/// Uses [`ShEntry`] encode/decode (same 8 B layout as slab body entries).
pub fn sh_page_entries(page: &[u8; SH_PAGE_SIZE]) -> Result<Vec<ShEntry>, StoreError> {
    let n = sh_page_n_fks(page)? as usize;
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let off = SH_PAGE_OFF_FKS + i * SH_ENTRY_LEN;
        let e = ShEntry::decode(&page[off..off + SH_ENTRY_LEN])?;
        if e.is_null() {
            return Err(StoreError::Corrupt("scripthash page null fk"));
        }
        if e.create_tx_fk.0 & SH_FLAG_BIT != 0 {
            return Err(StoreError::Corrupt("scripthash page fk has flag bit set"));
        }
        out.push(e);
    }
    Ok(out)
}

/// Number of 4 KiB pages needed for `n` create entries (`0` → `0`).
#[inline]
pub fn sh_page_count_for_entries(n: usize) -> usize {
    if n == 0 {
        0
    } else {
        n.div_ceil(SH_PAGE_FK_CAP)
    }
}

/// Pack up to [`SH_PAGE_FK_CAP`] entries into a fresh page with `next_page_off` already set.
///
/// Cold bulk and new-chain writers use this so each page is written **once** with its
/// next link known up front — no read-modify-write of the previous page.
pub fn sh_page_pack(
    page: &mut [u8; SH_PAGE_SIZE],
    entries: &[ShEntry],
    next_off: u64,
) -> Result<(), StoreError> {
    if entries.len() > SH_PAGE_FK_CAP {
        return Err(StoreError::Corrupt(
            "scripthash page pack: entries exceed page capacity",
        ));
    }
    sh_page_init_empty(page);
    sh_page_set_next(page, next_off)?;
    for e in entries {
        if !sh_page_try_append_entry(page, *e)? {
            return Err(StoreError::Corrupt(
                "scripthash page pack: page full unexpectedly",
            ));
        }
    }
    Ok(())
}

/// Append one create FK to the page. Returns `Ok(true)` if appended, `Ok(false)` if full.
pub fn sh_page_try_append(page: &mut [u8; SH_PAGE_SIZE], fk: Fk) -> Result<bool, StoreError> {
    sh_page_try_append_entry(page, ShEntry::new(fk))
}

/// Append one [`ShEntry`] to the page. Returns `Ok(true)` if appended, `Ok(false)` if full.
pub fn sh_page_try_append_entry(
    page: &mut [u8; SH_PAGE_SIZE],
    entry: ShEntry,
) -> Result<bool, StoreError> {
    if entry.is_null() {
        return Err(StoreError::InvalidFk);
    }
    if entry.create_tx_fk.0 & SH_FLAG_BIT != 0 {
        return Err(StoreError::Corrupt(
            "scripthash: create_fk must have bit63 clear",
        ));
    }
    let n = sh_page_n_fks(page)? as usize;
    if n >= SH_PAGE_FK_CAP {
        return Ok(false);
    }
    let off = SH_PAGE_OFF_FKS + n * SH_ENTRY_LEN;
    let enc = entry.encode();
    page[off..off + SH_ENTRY_LEN].copy_from_slice(&enc);
    sh_page_set_n_fks(page, (n + 1) as u16);
    Ok(true)
}

/// Decode page fields from an arbitrary slice (rejects len ≠ 4096).
pub fn sh_page_decode_slice(buf: &[u8]) -> Result<(u64, Vec<ShEntry>), StoreError> {
    let page = sh_page_as_array(buf)?;
    Ok((sh_page_next(page)?, sh_page_entries(page)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_constants_pin_32b_slot_and_4k_page() {
        // Full OA slot stays 32 B (documented contract; head module owns key len).
        assert_eq!(crate::scripthash_layout::SH_HEAD_SLOT_SIZE, 32);
        assert_eq!(crate::scripthash_layout::SH_HEAD_VALUE_LEN, 16);
        assert_eq!(SH_PAGE_SIZE, 4096);
        assert_eq!(SH_PAGE_FK_CAP, 510);
        assert_eq!(SH_FLAG_BIT, 1u64 << 63);
        assert_eq!(SH_PAYLOAD_MASK, u64::MAX >> 1);
        // Flag bit matches historical slab marker bit position.
        assert_eq!(SH_FLAG_BIT, crate::scripthash_layout::SH_SLAB_MARKER);
    }

    #[test]
    fn pack_flag_roundtrip_and_reject_high_bit_payload() {
        assert_eq!(sh_pack_clear(0x1234).unwrap(), 0x1234);
        assert_eq!(sh_pack_flagged(0x1234).unwrap(), 0x1234 | SH_FLAG_BIT);
        assert!(sh_word_flagged(sh_pack_flagged(1).unwrap()));
        assert!(!sh_word_flagged(sh_pack_clear(1).unwrap()));
        assert_eq!(sh_word_payload(SH_FLAG_BIT | 99), 99);
        assert!(sh_pack_clear(SH_FLAG_BIT).is_err());
        assert!(sh_pack_flagged(SH_FLAG_BIT | 1).is_err());
    }

    #[test]
    fn head_mode_empty_inline_paged() {
        assert_eq!(sh_head_value_mode(0, 0).unwrap(), ShHeadValueMode::Empty);
        assert_eq!(sh_head_value_mode(3, 0).unwrap(), ShHeadValueMode::Inline);
        assert_eq!(sh_head_value_mode(3, 4).unwrap(), ShHeadValueMode::Inline);
        let enc = sh_encode_paged_head(4096, 8192).unwrap();
        let (f, l) = sh_decode_paged_head(&enc).unwrap();
        assert_eq!((f, l), (4096, 8192));
        let w0 = u64::from_le_bytes(enc[0..8].try_into().unwrap());
        let w1 = u64::from_le_bytes(enc[8..16].try_into().unwrap());
        assert_eq!(sh_head_value_mode(w0, w1).unwrap(), ShHeadValueMode::Paged);
        assert!(sh_word_flagged(w0));
        assert!(!sh_word_flagged(w1));
        // w1 flagged illegal (inline)
        assert!(sh_head_value_mode(3, SH_FLAG_BIT | 1).is_err());
        // null first inline
        assert!(sh_head_value_mode(0, 5).is_err());
        // Paged: both words flagged → error (line 135-138)
        assert!(sh_head_value_mode(SH_FLAG_BIT | 4096, SH_FLAG_BIT | 8192).is_err());
        // Paged: null first or last payload
        assert!(sh_head_value_mode(SH_FLAG_BIT | 0, 8192).is_err());
        assert!(sh_head_value_mode(SH_FLAG_BIT | 4096, 0).is_err());
        // encode rejects null page offs (after pack)
        assert!(sh_encode_paged_head(0, 8192).is_err());
        assert!(sh_encode_paged_head(4096, 0).is_err());
        // decode non-paged → error
        let inline = {
            let mut b = [0u8; 16];
            b[0..8].copy_from_slice(&3u64.to_le_bytes());
            b
        };
        assert!(sh_decode_paged_head(&inline).is_err());
        // page next with flag bit
        let mut page = [0u8; SH_PAGE_SIZE];
        sh_page_init_empty(&mut page);
        page[SH_PAGE_OFF_NEXT..SH_PAGE_OFF_NEXT + 8]
            .copy_from_slice(&(SH_FLAG_BIT | 100).to_le_bytes());
        assert!(sh_page_next(&page).is_err());
    }

    #[test]
    fn page_append_fill_and_next_link() {
        let mut page = [0u8; SH_PAGE_SIZE];
        sh_page_init_empty(&mut page);
        assert_eq!(sh_page_n_fks(&page).unwrap(), 0);
        assert_eq!(sh_page_next(&page).unwrap(), 0);
        assert!(sh_page_try_append(&mut page, Fk(1)).unwrap());
        assert!(sh_page_try_append_entry(&mut page, ShEntry::new(Fk(2))).unwrap());
        assert_eq!(
            sh_page_entries(&page).unwrap(),
            vec![ShEntry::new(Fk(1)), ShEntry::new(Fk(2))]
        );
        sh_page_set_next(&mut page, 8192).unwrap();
        assert_eq!(sh_page_next(&page).unwrap(), 8192);

        // Fill to capacity then refuse one more (needs new page).
        let mut full = [0u8; SH_PAGE_SIZE];
        sh_page_init_empty(&mut full);
        for i in 1..=SH_PAGE_FK_CAP as u64 {
            assert!(sh_page_try_append(&mut full, Fk(i)).unwrap(), "append {i}");
        }
        assert_eq!(sh_page_n_fks(&full).unwrap() as usize, SH_PAGE_FK_CAP);
        assert!(!sh_page_try_append(&mut full, Fk(99_999)).unwrap());
        assert_eq!(sh_page_entries(&full).unwrap().len(), SH_PAGE_FK_CAP);
        // ShEntry bytes match layout encode.
        let e = ShEntry::new(Fk(0xabc));
        assert_eq!(e.encode(), e.create_tx_fk.0.to_le_bytes());
    }

    #[test]
    fn page_rejects_flagged_fk_and_null() {
        let mut page = [0u8; SH_PAGE_SIZE];
        sh_page_init_empty(&mut page);
        assert!(sh_page_try_append(&mut page, Fk::NULL).is_err());
        assert!(sh_page_try_append(&mut page, Fk(SH_FLAG_BIT | 1)).is_err());
    }

    #[test]
    fn page_count_for_entries_and_pack_sets_next_before_write() {
        assert_eq!(sh_page_count_for_entries(0), 0);
        assert_eq!(sh_page_count_for_entries(1), 1);
        assert_eq!(sh_page_count_for_entries(SH_PAGE_FK_CAP), 1);
        assert_eq!(sh_page_count_for_entries(SH_PAGE_FK_CAP + 1), 2);
        // Two full pages + 3 → three pages (contiguous layout at base).
        let n = SH_PAGE_FK_CAP * 2 + 3;
        assert_eq!(sh_page_count_for_entries(n), 3);
        let ents: Vec<_> = (1..=n as u64).map(|i| ShEntry::new(Fk(i))).collect();
        let base = 4096u64;
        let mut pages = Vec::new();
        for pi in 0..3 {
            let start = pi * SH_PAGE_FK_CAP;
            let end = (start + SH_PAGE_FK_CAP).min(ents.len());
            let off = base + (pi as u64) * (SH_PAGE_SIZE as u64);
            let next = if pi + 1 < 3 {
                off + SH_PAGE_SIZE as u64
            } else {
                0
            };
            let mut page = [0u8; SH_PAGE_SIZE];
            sh_page_pack(&mut page, &ents[start..end], next).unwrap();
            assert_eq!(sh_page_next(&page).unwrap(), next);
            assert_eq!(sh_page_n_fks(&page).unwrap() as usize, end - start);
            pages.push(page);
        }
        assert_eq!(sh_page_next(&pages[0]).unwrap(), base + SH_PAGE_SIZE as u64);
        assert_eq!(
            sh_page_next(&pages[1]).unwrap(),
            base + 2 * SH_PAGE_SIZE as u64
        );
        assert_eq!(sh_page_next(&pages[2]).unwrap(), 0);
        // Reconstruct FK order from packed pages.
        let mut got = Vec::new();
        for p in &pages {
            got.extend(sh_page_entries(p).unwrap());
        }
        assert_eq!(got, ents);
        // Over-capacity pack must fail (no silent truncate).
        let too_many: Vec<_> = (1..=SH_PAGE_FK_CAP as u64 + 1)
            .map(|i| ShEntry::new(Fk(i)))
            .collect();
        let mut page = [0u8; SH_PAGE_SIZE];
        assert!(sh_page_pack(&mut page, &too_many, 0).is_err());
    }

    #[test]
    fn page_slice_must_be_exactly_4k() {
        let short = [0u8; 100];
        assert!(sh_page_as_array(&short).is_err());
        assert!(sh_page_decode_slice(&short).is_err());
        let mut long = vec![0u8; SH_PAGE_SIZE + 1];
        assert!(sh_page_as_array(&long).is_err());
        assert!(sh_page_as_array_mut(&mut long[..SH_PAGE_SIZE - 1]).is_err());

        let mut page = [0u8; SH_PAGE_SIZE];
        sh_page_init_empty(&mut page);
        sh_page_try_append(&mut page, Fk(7)).unwrap();
        let (next, ents) = sh_page_decode_slice(&page).unwrap();
        assert_eq!(next, 0);
        assert_eq!(ents, vec![ShEntry::new(Fk(7))]);
    }

    #[test]
    fn page_corrupt_n_fks_over_capacity() {
        let mut page = [0u8; SH_PAGE_SIZE];
        sh_page_init_empty(&mut page);
        // Force illegal n_fks
        page[SH_PAGE_OFF_N_FKS..SH_PAGE_OFF_N_FKS + 2]
            .copy_from_slice(&((SH_PAGE_FK_CAP + 1) as u16).to_le_bytes());
        assert!(sh_page_n_fks(&page).is_err());
        assert!(sh_page_entries(&page).is_err());
    }

    #[test]
    fn page_entries_reject_null_and_flagged_fk_bytes() {
        let mut page = [0u8; SH_PAGE_SIZE];
        sh_page_init_empty(&mut page);
        // n_fks=1 with null fk body
        page[SH_PAGE_OFF_N_FKS..SH_PAGE_OFF_N_FKS + 2].copy_from_slice(&1u16.to_le_bytes());
        page[SH_PAGE_OFF_FKS..SH_PAGE_OFF_FKS + 8].copy_from_slice(&0u64.to_le_bytes());
        assert!(sh_page_entries(&page).is_err());
        // flagged fk body
        page[SH_PAGE_OFF_FKS..SH_PAGE_OFF_FKS + 8]
            .copy_from_slice(&(SH_FLAG_BIT | 9).to_le_bytes());
        assert!(sh_page_entries(&page).is_err());
    }
}
