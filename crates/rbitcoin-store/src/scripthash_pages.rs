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
//! | **Slab** (schema 15) | bit63=1, low63 = body off | bit63=1, low16 = used, bits 16–23 = class |
//!
//! - No `used` / count in the head for paged mode (append RMW last page).
//! - Inline never sets bit63 (FK payload must be `< 2^63`).
//! - Paged always sets bit63 on `w0` only; `w1` bit63 reserved **0**.
//! - Schema-15 **slab** sets bit63 on **both** words (schema 14 never wrote that).
//! - Schema-13 slab packed class/used into `w0` with `w1` clear — still decodes
//!   as paged; store open refuses a durable schema-13 SH index.
//!
//! # Body page (exactly [`SH_PAGE_SIZE`] = 4096, disk-aligned)
//!
//! ```text
//! offset  len   field
//! 0       8     next_page_off  (0 = end of chain); bit63 must be 0
//! 8       2     n_fks          (u16 LE); FKs stored in this page
//! 10      1     ver            (1 = uleb fk0+deltas; 0 + n>0 = leftover raw)
//! 11      5     reserved       (zero)
//! 16      …     delta stream   (uleb fk0 + uleb gaps), n = n_fks
//! ```
//!
//! [`SH_PAGE_FK_CAP`] = 510 is the raw-u64 historical fill. Delta pages hold
//! more when gaps are small (page full when the next uleb does not fit).
//!
//! Chain is **singly linked** first → … → last. Head stores first+last for O(1)
//! walk start and O(1) append target.

use crate::compact::{read_uleb128, uleb128_len, write_uleb128};
use crate::error::StoreError;
use crate::scripthash_layout::{ShEntry, SH_ENTRY_LEN};
use crate::scripthash_slabs::{decode_fk_delta_stream, encode_fk_delta_stream};
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
/// Offset of delta-stream version byte (`reserved[0]`).
pub const SH_PAGE_OFF_VER: usize = 10;
/// Schema-17 megakey pages: uleb fk0 + uleb deltas (not raw u64 slots).
pub const SH_PAGE_DELTA_VER: u8 = 1;
/// Max FKs if every stream byte is a 1-byte uleb.
pub const SH_PAGE_STREAM_MAX: usize = SH_PAGE_SIZE - SH_PAGE_HEADER_LEN;
/// Offset of first FK / delta stream within a page.
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

/// Head value mode from raw `w0`/`w1` (schema-15).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShHeadValueMode {
    Empty,
    /// One or two inline FKs (bit63 clear on both words).
    Inline,
    /// Page chain: first in w0 payload, last in w1 payload; w0 flagged.
    Paged,
    /// Geometric slab: both words flagged; off / class+used in payloads.
    Slab,
}

/// Classify a 16-byte head value without allocating (schema-15 rules).
///
/// Both flags set is **slab**. Flagged `w0` only is **paged**. Schema-13 slab
/// packing used flagged `w0` + clear `w1`; store open refuses a durable
/// schema-13 SH index, so decode does not sniff slab-vs-paged from offset shape.
#[inline]
pub fn sh_head_value_mode(w0: u64, w1: u64) -> Result<ShHeadValueMode, StoreError> {
    if w0 == 0 && w1 == 0 {
        return Ok(ShHeadValueMode::Empty);
    }
    if sh_word_flagged(w0) && sh_word_flagged(w1) {
        let off = sh_word_payload(w0);
        if off == 0 {
            return Err(StoreError::Corrupt("scripthash slab head: null body off"));
        }
        return Ok(ShHeadValueMode::Slab);
    }
    if sh_word_flagged(w0) {
        // Paged: w1 is not flagged (both-flag case already returned Slab).
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

/// Encode slab head: both words flagged; `w0` = off, `w1` = used | class<<16.
#[inline]
pub fn sh_encode_slab_head(class: u8, used: u16, off: u64) -> Result<[u8; 16], StoreError> {
    if off == 0 {
        return Err(StoreError::Corrupt("scripthash slab head: null body off"));
    }
    if class > crate::scripthash_layout::SH_MAX_SLAB_CLASS {
        return Err(StoreError::Corrupt("scripthash slab head: class overflow"));
    }
    if used < 3 {
        return Err(StoreError::Corrupt(
            "scripthash slab head: used < 3 (inline)",
        ));
    }
    let w0 = sh_pack_flagged(off)?;
    let packed = u64::from(used) | (u64::from(class) << 16);
    let w1 = sh_pack_flagged(packed)?;
    let mut out = [0u8; 16];
    out[0..8].copy_from_slice(&w0.to_le_bytes());
    out[8..16].copy_from_slice(&w1.to_le_bytes());
    Ok(out)
}

/// Decode slab `(class, used, off)` from a 16-byte value (errors if not slab).
#[inline]
pub fn sh_decode_slab_head(buf: &[u8; 16]) -> Result<(u8, u16, u64), StoreError> {
    let w0 = u64::from_le_bytes(buf[0..8].try_into().unwrap());
    let w1 = u64::from_le_bytes(buf[8..16].try_into().unwrap());
    match sh_head_value_mode(w0, w1)? {
        ShHeadValueMode::Slab => {
            let off = sh_word_payload(w0);
            let packed = sh_word_payload(w1);
            if packed >> 24 != 0 {
                return Err(StoreError::Corrupt(
                    "scripthash slab head: reserved bits set",
                ));
            }
            let used = (packed & 0xffff) as u16;
            let class = ((packed >> 16) & 0xff) as u8;
            if class > crate::scripthash_layout::SH_MAX_SLAB_CLASS {
                return Err(StoreError::Corrupt("scripthash slab head: class overflow"));
            }
            if used < 3 {
                return Err(StoreError::Corrupt(
                    "scripthash slab head: used < 3 (inline)",
                ));
            }
            Ok((class, used, off))
        }
        _ => Err(StoreError::Corrupt("scripthash: expected slab head value")),
    }
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
    page[SH_PAGE_OFF_VER] = SH_PAGE_DELTA_VER;
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
    if n as usize > SH_PAGE_STREAM_MAX {
        return Err(StoreError::Corrupt("scripthash page n_fks > capacity"));
    }
    Ok(n)
}

fn sh_page_require_delta(page: &[u8; SH_PAGE_SIZE]) -> Result<(), StoreError> {
    let ver = page[SH_PAGE_OFF_VER];
    let n = sh_page_n_fks(page)?;
    if ver == SH_PAGE_DELTA_VER {
        return Ok(());
    }
    if ver == 0 && n == 0 {
        return Ok(());
    }
    Err(StoreError::Corrupt(
        "scripthash page leftover raw-u64; rematerialize",
    ))
}

fn sh_page_stream(page: &[u8; SH_PAGE_SIZE]) -> &[u8] {
    &page[SH_PAGE_OFF_FKS..]
}

fn sh_page_stream_used(page: &[u8; SH_PAGE_SIZE]) -> Result<usize, StoreError> {
    let n = sh_page_n_fks(page)? as usize;
    if n == 0 {
        return Ok(0);
    }
    let mut off = 0usize;
    let buf = sh_page_stream(page);
    for _ in 0..n {
        let (_, used) = read_uleb128(buf.get(off..).unwrap_or(&[]))?;
        off += used;
        if off > SH_PAGE_STREAM_MAX {
            return Err(StoreError::Corrupt("scripthash page stream overrun"));
        }
    }
    Ok(off)
}

#[inline]
fn sh_page_set_n_fks(page: &mut [u8; SH_PAGE_SIZE], n: u16) {
    page[SH_PAGE_OFF_N_FKS..SH_PAGE_OFF_N_FKS + 2].copy_from_slice(&n.to_le_bytes());
}

/// Entries currently stored in the page (**strictly increasing** create_tx_fk).
///
/// Uses [`ShEntry`] encode/decode (same 8 B layout as slab body entries).
/// Refuses equal/decreasing consecutive FKs (durable corruption or bad pack).
pub fn sh_page_entries(page: &[u8; SH_PAGE_SIZE]) -> Result<Vec<ShEntry>, StoreError> {
    sh_page_require_delta(page)?;
    let n = sh_page_n_fks(page)? as usize;
    let fks = decode_fk_delta_stream(sh_page_stream(page), n)?;
    Ok(fks.into_iter().map(ShEntry::new).collect())
}

/// Last create_tx_fk on this page (`None` if empty).
#[inline]
pub fn sh_page_last_fk(page: &[u8; SH_PAGE_SIZE]) -> Result<Option<Fk>, StoreError> {
    let ents = sh_page_entries(page)?;
    Ok(ents.last().map(|e| e.create_tx_fk))
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

/// Split strictly increasing FKs into page-sized delta-stream chunks.
pub fn sh_page_chunk_ranges(fks: &[Fk]) -> Result<Vec<(usize, usize)>, StoreError> {
    if fks.is_empty() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut used = 0usize;
    for (i, fk) in fks.iter().enumerate() {
        let add = if i == start {
            uleb128_len(fk.0)
        } else {
            let d = fk.0.saturating_sub(fks[i - 1].0);
            uleb128_len(d)
        };
        if i > start && used + add > SH_PAGE_STREAM_MAX {
            out.push((start, i));
            start = i;
            used = uleb128_len(fk.0);
        } else {
            used += add;
        }
    }
    out.push((start, fks.len()));
    Ok(out)
}

/// Pack up to [`SH_PAGE_FK_CAP`] **strictly increasing** entries into a fresh page
/// with `next_page_off` already set.
///
/// Cold bulk and new-chain writers use this so each page is written **once** with its
/// next link known up front — no read-modify-write of the previous page.
pub fn sh_page_pack(
    page: &mut [u8; SH_PAGE_SIZE],
    entries: &[ShEntry],
    next_off: u64,
) -> Result<(), StoreError> {
    let stream =
        encode_fk_delta_stream(&entries.iter().map(|e| e.create_tx_fk).collect::<Vec<_>>())?;
    if stream.len() > SH_PAGE_STREAM_MAX {
        return Err(StoreError::Corrupt(
            "scripthash page pack: entries exceed page capacity",
        ));
    }
    // Pre-check strictly increasing so pack fails cleanly (not mid-page).
    for w in entries.windows(2) {
        if w[1].create_tx_fk.0 <= w[0].create_tx_fk.0 {
            return Err(StoreError::Corrupt(
                "invariant: scripthash page pack create_fks not strictly increasing",
            ));
        }
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
///
/// Requires `entry.create_tx_fk` **strictly greater** than the page's last FK when
/// non-empty (sorted page chain invariant). Equal/lower is a pack/encode bug —
/// durable re-queue of lower FKs is filtered **before** append by table writers.
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
    sh_page_require_delta(page)?;
    let n = sh_page_n_fks(page)? as usize;
    let used = sh_page_stream_used(page)?;
    let add = if n == 0 {
        uleb128_len(entry.create_tx_fk.0)
    } else {
        let last = sh_page_last_fk(page)?.expect("n>0 has last fk");
        if entry.create_tx_fk.0 <= last.0 {
            return Err(StoreError::Corrupt(
                "invariant: scripthash page append create_fk not strictly increasing",
            ));
        }
        uleb128_len(entry.create_tx_fk.0 - last.0)
    };
    if used + add > SH_PAGE_STREAM_MAX {
        return Ok(false);
    }
    let mut tmp = Vec::with_capacity(add);
    if n == 0 {
        write_uleb128(&mut tmp, entry.create_tx_fk.0);
    } else {
        let last = sh_page_last_fk(page)?.expect("n>0 has last fk");
        write_uleb128(&mut tmp, entry.create_tx_fk.0 - last.0);
    }
    debug_assert_eq!(tmp.len(), add);
    let off = SH_PAGE_OFF_FKS + used;
    page[off..off + tmp.len()].copy_from_slice(&tmp);
    page[SH_PAGE_OFF_VER] = SH_PAGE_DELTA_VER;
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
    fn sh_page_delta_packs_600_sequential_in_one_page() {
        let ents: Vec<_> = (1u64..=600).map(|i| ShEntry::new(Fk(i))).collect();
        let mut page = [0u8; SH_PAGE_SIZE];
        sh_page_pack(&mut page, &ents, 0).unwrap();
        assert_eq!(sh_page_n_fks(&page).unwrap(), 600);
        assert_eq!(sh_page_last_fk(&page).unwrap(), Some(Fk(600)));
        assert_eq!(sh_page_entries(&page).unwrap(), ents);
        assert_eq!(page[SH_PAGE_OFF_VER], SH_PAGE_DELTA_VER);
    }

    #[test]
    fn sh_page_delta_append_opens_second_page_when_stream_full() {
        let mut page = [0u8; SH_PAGE_SIZE];
        sh_page_init_empty(&mut page);
        let mut n = 0u64;
        loop {
            n += 1;
            if !sh_page_try_append(&mut page, Fk(n)).unwrap() {
                break;
            }
        }
        assert!(
            n > SH_PAGE_FK_CAP as u64,
            "delta must beat 510 raw slots, n={n}"
        );
        assert!(sh_page_n_fks(&page).unwrap() as u64 >= SH_PAGE_FK_CAP as u64);
        assert_eq!(sh_page_last_fk(&page).unwrap(), Some(Fk(n - 1)));
        assert!(!sh_page_try_append(&mut page, Fk(n)).unwrap());
    }

    #[test]
    fn sh_page_delta_refuses_raw_u64_leftover() {
        let mut page = [0u8; SH_PAGE_SIZE];
        sh_page_init_empty(&mut page);
        page[SH_PAGE_OFF_VER] = 0;
        page[SH_PAGE_OFF_N_FKS..SH_PAGE_OFF_N_FKS + 2].copy_from_slice(&1u16.to_le_bytes());
        page[SH_PAGE_OFF_FKS..SH_PAGE_OFF_FKS + 8].copy_from_slice(&1u64.to_le_bytes());
        match sh_page_entries(&page) {
            Err(StoreError::Corrupt(m)) => {
                assert!(m.contains("rematerialize") || m.contains("raw"), "{m}");
            }
            other => panic!("expected leftover Corrupt, got {other:?}"),
        }
    }

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
        // Both words flagged → slab mode (payloads checked on encode/decode).
        assert_eq!(
            sh_head_value_mode(SH_FLAG_BIT | 4096, SH_FLAG_BIT | 5 | (1u64 << 16)).unwrap(),
            ShHeadValueMode::Slab
        );
        let slab = sh_encode_slab_head(1, 5, 4096).unwrap();
        assert_eq!(sh_decode_slab_head(&slab).unwrap(), (1, 5, 4096));
        assert!(sh_encode_slab_head(1, 5, 0).is_err());
        assert!(sh_encode_slab_head(1, 2, 4096).is_err());
        assert!(sh_head_value_mode(SH_FLAG_BIT | 0, SH_FLAG_BIT | 5).is_err());
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

        // Fill until the next sequential fk does not fit.
        let mut full = [0u8; SH_PAGE_SIZE];
        sh_page_init_empty(&mut full);
        let mut i = 1u64;
        while sh_page_try_append(&mut full, Fk(i)).unwrap() {
            i += 1;
        }
        assert!(i > SH_PAGE_FK_CAP as u64);
        assert!(!sh_page_try_append(&mut full, Fk(i)).unwrap());
        assert_eq!(sh_page_entries(&full).unwrap().len() as u64, i - 1);
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
        let n = SH_PAGE_STREAM_MAX + 200;
        let ents: Vec<_> = (1..=n as u64).map(|i| ShEntry::new(Fk(i))).collect();
        let fks: Vec<Fk> = ents.iter().map(|e| e.create_tx_fk).collect();
        let chunks = sh_page_chunk_ranges(&fks).unwrap();
        assert!(chunks.len() >= 2, "stream overflow must split pages");
        let base = 4096u64;
        let mut pages = Vec::new();
        for (pi, &(start, end)) in chunks.iter().enumerate() {
            let off = base + (pi as u64) * (SH_PAGE_SIZE as u64);
            let next = if pi + 1 < chunks.len() {
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
        assert_eq!(sh_page_next(pages.last().unwrap()).unwrap(), 0);
        let mut got = Vec::new();
        for p in &pages {
            got.extend(sh_page_entries(p).unwrap());
        }
        assert_eq!(got, ents);
        // One page cannot hold STREAM_MAX+1 one-byte deltas (first fk + N gaps).
        let too_many: Vec<_> = (1..=SH_PAGE_STREAM_MAX as u64 + 2)
            .map(|i| ShEntry::new(Fk(i)))
            .collect();
        let mut page = [0u8; SH_PAGE_SIZE];
        assert!(sh_page_pack(&mut page, &too_many, 0).is_err());
    }

    #[test]
    fn page_fks_must_be_strictly_increasing() {
        let mut page = [0u8; SH_PAGE_SIZE];
        sh_page_init_empty(&mut page);
        assert!(sh_page_try_append(&mut page, Fk(10)).unwrap());
        assert!(sh_page_try_append(&mut page, Fk(20)).unwrap());
        // Equal / lower rejected at append (encode bug), not silent.
        assert!(sh_page_try_append(&mut page, Fk(20)).is_err());
        assert!(sh_page_try_append(&mut page, Fk(5)).is_err());
        assert_eq!(sh_page_last_fk(&page).unwrap(), Some(Fk(20)));

        // Pack rejects unsorted input.
        let unsorted = vec![
            ShEntry::new(Fk(3)),
            ShEntry::new(Fk(1)),
            ShEntry::new(Fk(2)),
        ];
        assert!(sh_page_pack(&mut page, &unsorted, 0).is_err());

        // Decode refuses durable unsorted bytes (plant equal consecutive).
        sh_page_init_empty(&mut page);
        sh_page_try_append(&mut page, Fk(1)).unwrap();
        sh_page_try_append(&mut page, Fk(2)).unwrap();
        // Zero the second-stream uleb (delta) without going through append.
        page[SH_PAGE_OFF_FKS + 1] = 0;
        assert!(sh_page_entries(&page).is_err());
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
            .copy_from_slice(&((SH_PAGE_STREAM_MAX + 1) as u16).to_le_bytes());
        assert!(sh_page_n_fks(&page).is_err());
        assert!(sh_page_entries(&page).is_err());
    }

    #[test]
    fn page_entries_reject_null_and_flagged_fk_bytes() {
        let mut page = [0u8; SH_PAGE_SIZE];
        sh_page_init_empty(&mut page);
        page[SH_PAGE_OFF_N_FKS..SH_PAGE_OFF_N_FKS + 2].copy_from_slice(&1u16.to_le_bytes());
        page[SH_PAGE_OFF_FKS] = 0;
        assert!(sh_page_entries(&page).is_err());
        let mut stream = Vec::new();
        crate::compact::write_uleb128(&mut stream, SH_FLAG_BIT | 9);
        page[SH_PAGE_OFF_FKS..SH_PAGE_OFF_FKS + stream.len()].copy_from_slice(&stream);
        assert!(sh_page_entries(&page).is_err());
    }

    #[test]
    fn page_last_fk_rejects_null_and_flagged() {
        let mut page = [0u8; SH_PAGE_SIZE];
        sh_page_init_empty(&mut page);
        assert_eq!(sh_page_last_fk(&page).unwrap(), None);
        page[SH_PAGE_OFF_N_FKS..SH_PAGE_OFF_N_FKS + 2].copy_from_slice(&1u16.to_le_bytes());
        page[SH_PAGE_OFF_FKS] = 0;
        assert!(sh_page_last_fk(&page).is_err());
        let mut flagged = Vec::new();
        crate::compact::write_uleb128(&mut flagged, SH_FLAG_BIT | 42);
        page[SH_PAGE_OFF_FKS..SH_PAGE_OFF_FKS + flagged.len()].copy_from_slice(&flagged);
        assert!(sh_page_last_fk(&page).is_err());
        sh_page_init_empty(&mut page);
        assert!(sh_page_try_append(&mut page, Fk(42)).unwrap());
        assert_eq!(sh_page_last_fk(&page).unwrap(), Some(Fk(42)));
        assert_eq!(sh_page_count_for_entries(0), 0);
        assert_eq!(sh_page_count_for_entries(1), 1);
        assert_eq!(sh_page_count_for_entries(SH_PAGE_FK_CAP), 1);
        assert_eq!(sh_page_count_for_entries(SH_PAGE_FK_CAP + 1), 2);
    }
}
