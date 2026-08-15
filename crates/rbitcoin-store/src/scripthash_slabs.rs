//! Schema-15 scripthash body packing: geometric slabs + ULEB128 fk deltas.
//!
//! Occupancy helpers compare schema-14 4 KiB page allocation against
//! schema-15 size-class slabs (megakeys still use pages). Encode/decode of
//! the fk stream is shared by slab payloads and megakey pages
//! ([`encode_fk_delta_stream`]).

use crate::compact::{read_uleb128, uleb128_len, write_uleb128};
use crate::error::StoreError;
use crate::scripthash_layout::{slab_bytes, slab_cap, SH_INLINE_CAP, SH_MAX_SLAB_CLASS};
use crate::scripthash_pages::{sh_page_count_for_entries, SH_FLAG_BIT, SH_PAGE_SIZE};
use rbitcoin_primitives::Fk;

/// First fk count that freezes into a megakey page chain (class 6 cap + 1).
pub const SH_MEGAKEY_MIN_FKS: u32 = 257;

const _: () = assert!(slab_cap(SH_MAX_SLAB_CLASS) == 256);
const _: () = assert!(SH_MEGAKEY_MIN_FKS == slab_cap(SH_MAX_SLAB_CLASS) + 1);

/// Smallest class whose slot cap is `≥ n` (`None` if inline or megakey).
///
/// `n ≤ 2` is head-inline (no body). `n ≥ 257` is a page chain.
pub fn slab_class_for_n_fks(n: u32) -> Option<u8> {
    if n as usize <= SH_INLINE_CAP || n > slab_cap(SH_MAX_SLAB_CLASS) {
        return None;
    }
    (0..=SH_MAX_SLAB_CLASS).find(|&c| slab_cap(c) >= n)
}

/// Tip-grow picker: hold `n` with one spare slot when a larger class exists.
///
/// A 4-fk list therefore lands in class 1 (cap 8), not a full class 0.
/// At the class-6 ceiling the spare is impossible — exact class 6, then pages.
pub fn slab_class_for_n_fks_with_slack(n: u32) -> Option<u8> {
    slab_class_for_n_fks(n.saturating_add(1)).or_else(|| slab_class_for_n_fks(n))
}

/// Schema-14 body bytes for one key with `n` create fks (inline → 0).
pub fn page_alloc_bytes_for_n_fks(n: u32) -> u64 {
    if n as usize <= SH_INLINE_CAP {
        0
    } else {
        sh_page_count_for_entries(n as usize) as u64 * SH_PAGE_SIZE as u64
    }
}

/// Schema-15 allocated body bytes for one key with `n` create fks.
///
/// Exact class (cold pack). Tip grow uses [`slab_class_for_n_fks_with_slack`].
pub fn slab_alloc_bytes_for_n_fks(n: u32) -> u64 {
    if n as usize <= SH_INLINE_CAP {
        0
    } else if let Some(c) = slab_class_for_n_fks(n) {
        slab_bytes(c)
    } else {
        page_alloc_bytes_for_n_fks(n)
    }
}

/// Encode strictly increasing create fks as ULEB128 `fk0` + ULEB128 deltas.
///
/// Does not prefix `used` — slabs write `u16` first; pages keep `n_fks` in the
/// page header. Empty input → empty stream.
pub fn encode_fk_delta_stream(fks: &[Fk]) -> Result<Vec<u8>, StoreError> {
    if fks.is_empty() {
        return Ok(Vec::new());
    }
    if fks[0].is_null() {
        return Err(StoreError::Corrupt("scripthash fk stream null first fk"));
    }
    if fks[0].0 & SH_FLAG_BIT != 0 {
        return Err(StoreError::Corrupt("scripthash fk stream flag bit set"));
    }
    for w in fks.windows(2) {
        if w[1].0 <= w[0].0 {
            return Err(StoreError::Corrupt(
                "invariant: scripthash fk stream not strictly increasing",
            ));
        }
        if w[1].0 & SH_FLAG_BIT != 0 {
            return Err(StoreError::Corrupt("scripthash fk stream flag bit set"));
        }
    }
    let mut out = Vec::with_capacity(uleb128_len(fks[0].0) + fks.len());
    write_uleb128(&mut out, fks[0].0);
    for w in fks.windows(2) {
        write_uleb128(&mut out, w[1].0 - w[0].0);
    }
    Ok(out)
}

/// Decode `n` strictly increasing fks from a delta stream (stops after `n`).
pub fn decode_fk_delta_stream(buf: &[u8], n: usize) -> Result<Vec<Fk>, StoreError> {
    if n == 0 {
        return Ok(Vec::new());
    }
    let mut out = Vec::with_capacity(n);
    let mut off = 0usize;
    let (first, used) = read_uleb128(buf.get(off..).unwrap_or(&[]))?;
    if first == 0 {
        return Err(StoreError::Corrupt("scripthash fk stream null first fk"));
    }
    if first & SH_FLAG_BIT != 0 {
        return Err(StoreError::Corrupt("scripthash fk stream flag bit set"));
    }
    off += used;
    out.push(Fk(first));
    for _ in 1..n {
        let (d, used) = read_uleb128(buf.get(off..).unwrap_or(&[]))?;
        off += used;
        if d == 0 {
            return Err(StoreError::Corrupt(
                "invariant: scripthash fk stream zero delta",
            ));
        }
        let next = out
            .last()
            .unwrap()
            .0
            .checked_add(d)
            .ok_or(StoreError::Corrupt("scripthash fk stream delta overflow"))?;
        if next & SH_FLAG_BIT != 0 {
            return Err(StoreError::Corrupt("scripthash fk stream flag bit set"));
        }
        out.push(Fk(next));
    }
    Ok(out)
}

/// Slab payload: `used:u16` LE + [`encode_fk_delta_stream`].
pub fn encode_slab_payload(fks: &[Fk]) -> Result<Vec<u8>, StoreError> {
    if fks.len() > u16::MAX as usize {
        return Err(StoreError::Corrupt("scripthash slab used overflow"));
    }
    let stream = encode_fk_delta_stream(fks)?;
    let mut out = Vec::with_capacity(2 + stream.len());
    out.extend_from_slice(&(fks.len() as u16).to_le_bytes());
    out.extend_from_slice(&stream);
    Ok(out)
}

/// Decode a slab payload (`used` + stream). Extra padding after the stream is ignored.
pub fn decode_slab_payload(buf: &[u8]) -> Result<Vec<Fk>, StoreError> {
    if buf.len() < 2 {
        return Err(StoreError::Corrupt("scripthash slab payload short"));
    }
    let used = u16::from_le_bytes([buf[0], buf[1]]) as usize;
    decode_fk_delta_stream(&buf[2..], used)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Measured mainnet page histogram (share of 4 KiB pages, per 1000).
    ///
    /// Representative `n_fks` sits in the middle of each bucket from the live
    /// `scripthash.body` sample (61.8 % pages hold 3–7 fks, …).
    const MAINNET_PAGE_MIX: &[(u32, u32)] = &[
        (5, 618),  // 3–7
        (11, 172), // 8–15
        (23, 92),  // 16–31
        (47, 50),  // 32–63
        (95, 27),  // 64–127
        (191, 15), // 128–255
        (380, 6),  // 256–509
        (510, 20), // full page
    ];

    #[test]
    fn slab_pack_beats_pages_on_mainnet_mix() {
        let mut page_bytes = 0u64;
        let mut slab_bytes_tot = 0u64;
        for &(n, weight) in MAINNET_PAGE_MIX {
            page_bytes += page_alloc_bytes_for_n_fks(n) * u64::from(weight);
            slab_bytes_tot += slab_alloc_bytes_for_n_fks(n) * u64::from(weight);
        }
        assert!(page_bytes > 0, "page packer must charge body bytes");
        let ratio = slab_bytes_tot as f64 / page_bytes as f64;
        assert!(
            ratio < 0.12,
            "slab packer must use <12% of page bytes on the measured mix; \
             got {ratio:.4} (slab={slab_bytes_tot} page={page_bytes})"
        );
    }

    #[test]
    fn slab_class_picks_smallest_fit_and_slack() {
        assert_eq!(slab_class_for_n_fks(0), None);
        assert_eq!(slab_class_for_n_fks(1), None);
        assert_eq!(slab_class_for_n_fks(2), None);
        assert_eq!(slab_class_for_n_fks(3), Some(0));
        assert_eq!(slab_class_for_n_fks(4), Some(0));
        assert_eq!(slab_class_for_n_fks(5), Some(1));
        assert_eq!(slab_class_for_n_fks(8), Some(1));
        assert_eq!(slab_class_for_n_fks(9), Some(2));
        assert_eq!(slab_class_for_n_fks(256), Some(6));
        assert_eq!(slab_class_for_n_fks(257), None);
        assert_eq!(slab_class_for_n_fks_with_slack(4), Some(1));
        assert_eq!(slab_class_for_n_fks_with_slack(256), Some(6));
        assert_eq!(slab_alloc_bytes_for_n_fks(1), 0);
        assert_eq!(page_alloc_bytes_for_n_fks(1), 0);
        assert_eq!(slab_alloc_bytes_for_n_fks(5), 64);
        assert_eq!(slab_alloc_bytes_for_n_fks(257), 4096);
        assert_eq!(page_alloc_bytes_for_n_fks(5), 4096);
    }

    #[test]
    fn fk_delta_stream_roundtrip_and_packed_len() {
        let cases: &[&[u64]] = &[
            &[],
            &[1],
            &[3, 4, 5, 7],
            &[1_000_000_000, 1_000_000_003, 1_000_000_010],
            &[10, 11, 12, 13, 14, 15, 16, 20],
        ];
        for raw in cases {
            let fks: Vec<Fk> = raw.iter().copied().map(Fk).collect();
            let stream = encode_fk_delta_stream(&fks).unwrap();
            assert!(
                stream.len() <= 8 * fks.len() || fks.is_empty(),
                "packed length {} > 8×n={}",
                stream.len(),
                fks.len()
            );
            let got = decode_fk_delta_stream(&stream, fks.len()).unwrap();
            assert_eq!(got, fks);
            let slab = encode_slab_payload(&fks).unwrap();
            assert_eq!(decode_slab_payload(&slab).unwrap(), fks);
            assert_eq!(slab.len(), 2 + stream.len());
        }
        assert!(encode_fk_delta_stream(&[Fk(5), Fk(5)]).is_err());
        assert!(encode_fk_delta_stream(&[Fk(0)]).is_err());
        assert!(encode_fk_delta_stream(&[Fk(SH_FLAG_BIT | 1)]).is_err());
        assert!(encode_fk_delta_stream(&[Fk(1), Fk(SH_FLAG_BIT | 2)]).is_err());
        assert!(decode_fk_delta_stream(&[0x00], 1).is_err());
        let mut flagged = Vec::new();
        crate::compact::write_uleb128(&mut flagged, SH_FLAG_BIT);
        assert!(decode_fk_delta_stream(&flagged, 1).is_err());
        assert!(decode_fk_delta_stream(&[0x01, 0x00], 2).is_err());
        assert!(decode_slab_payload(&[0]).is_err());
        assert!(decode_fk_delta_stream(&[], 0).unwrap().is_empty());
    }
}
