//! Schema-15 scripthash body packing: geometric slabs + ULEB128 fk deltas.
//!
//! Occupancy helpers compare schema-14 4 KiB page allocation against
//! schema-15 size-class slabs (megakeys still use pages). Encode/decode of
//! the fk stream is shared by slab payloads and megakey pages
//! ([`encode_fk_delta_stream_into`]).

use crate::compact::{read_uleb128, write_uleb128_into};
use crate::error::StoreError;
use crate::scripthash_layout::{slab_bytes, slab_cap, SH_INLINE_CAP, SH_MAX_SLAB_CLASS};
use crate::scripthash_pages::SH_FLAG_BIT;
use rbitcoin_primitives::Fk;

/// First fk count that freezes into a megakey page chain (class 6 cap + 1).
pub const SH_MEGAKEY_MIN_FKS: u32 = 257;

const _: () = assert!(slab_bytes(0) == 16);
const _: () = assert!(slab_bytes(SH_MAX_SLAB_CLASS) == 2048);
const _: () = assert!(SH_MEGAKEY_MIN_FKS == 257);

/// Smallest relocating class whose **byte** size holds `packed_len`.
///
/// Bulk pack uses this so a tight ULEB stream does not inherit the raw-u64
/// `n × 8` class. `None` if empty or larger than class 7 (2 KiB).
pub fn slab_class_for_packed_len(packed_len: usize) -> Option<u8> {
    if packed_len == 0 {
        return None;
    }
    (0..=SH_MAX_SLAB_CLASS).find(|&c| slab_bytes(c) as usize >= packed_len)
}

/// Smallest class whose slot cap is `≥ n` (`None` if inline or megakey).
///
/// Tip-grow still uses fk-count so a spare slot exists for the next append.
/// `n ≤ 1` is head-inline (no body). `n ≥ 257` is a page chain.
pub fn slab_class_for_n_fks(n: u32) -> Option<u8> {
    if n as usize <= SH_INLINE_CAP || n >= SH_MEGAKEY_MIN_FKS {
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

/// Encode strictly increasing create fks as ULEB128 `fk0` + ULEB128 deltas.
///
/// Does not prefix `used` — slabs write `u16` first; pages keep `n_fks` in the
/// page header. Empty input → empty stream.
pub fn encode_fk_delta_stream_into(out: &mut [u8], fks: &[u64]) -> Result<usize, StoreError> {
    if fks.is_empty() {
        return Ok(0);
    }
    if fks[0] == 0 {
        return Err(StoreError::Corrupt("scripthash fk stream null first fk"));
    }
    if fks[0] & SH_FLAG_BIT != 0 {
        return Err(StoreError::Corrupt("scripthash fk stream flag bit set"));
    }
    for w in fks.windows(2) {
        if w[1] <= w[0] {
            return Err(StoreError::Corrupt(
                "invariant: scripthash fk stream not strictly increasing",
            ));
        }
        if w[1] & SH_FLAG_BIT != 0 {
            return Err(StoreError::Corrupt("scripthash fk stream flag bit set"));
        }
    }
    let mut n = write_uleb128_into(out, fks[0])?;
    for w in fks.windows(2) {
        n += write_uleb128_into(out.get_mut(n..).unwrap_or(&mut []), w[1] - w[0])?;
    }
    Ok(n)
}

/// Decode `n` strictly increasing fks from a delta stream into `out` (stops after `n`).
pub fn decode_fk_delta_stream_into(
    buf: &[u8],
    n: usize,
    out: &mut Vec<Fk>,
) -> Result<(), StoreError> {
    if n == 0 {
        return Ok(());
    }
    out.reserve(n);
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
            return Err(StoreError::Corrupt("invariant: scripthash fk stream zero delta"));
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
    Ok(())
}

/// Slab payload: `used:u16` LE + [`encode_fk_delta_stream_into`].
pub fn encode_slab_payload_into(out: &mut [u8], fks: &[u64]) -> Result<usize, StoreError> {
    if fks.len() > u16::MAX as usize {
        return Err(StoreError::Corrupt("scripthash slab used overflow"));
    }
    if out.len() < 2 {
        return Err(StoreError::Corrupt("scripthash slab dest short"));
    }
    let n = encode_fk_delta_stream_into(&mut out[2..], fks)?;
    out[..2].copy_from_slice(&(fks.len() as u16).to_le_bytes());
    Ok(2 + n)
}

/// Decode a slab payload (`used` + stream) into `out`. Extra padding after the stream is ignored.
pub fn decode_slab_payload_into(buf: &[u8], out: &mut Vec<Fk>) -> Result<(), StoreError> {
    if buf.len() < 2 {
        return Err(StoreError::Corrupt("scripthash slab payload short"));
    }
    let used = u16::from_le_bytes([buf[0], buf[1]]) as usize;
    decode_fk_delta_stream_into(&buf[2..], used, out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_stream(fks: &[u64]) -> Result<Vec<u8>, StoreError> {
        let mut out = vec![0u8; fks.len().saturating_mul(10).max(1)];
        let n = encode_fk_delta_stream_into(&mut out, fks)?;
        out.truncate(n);
        Ok(out)
    }

    fn decode_stream(buf: &[u8], n: usize) -> Result<Vec<Fk>, StoreError> {
        let mut out = Vec::new();
        decode_fk_delta_stream_into(buf, n, &mut out)?;
        Ok(out)
    }

    fn encode_slab(fks: &[u64]) -> Result<Vec<u8>, StoreError> {
        let mut out = vec![0u8; 2 + fks.len().saturating_mul(10).max(1)];
        let n = encode_slab_payload_into(&mut out, fks)?;
        out.truncate(n);
        Ok(out)
    }

    fn decode_slab(buf: &[u8]) -> Result<Vec<Fk>, StoreError> {
        let mut out = Vec::new();
        decode_slab_payload_into(buf, &mut out)?;
        Ok(out)
    }

    #[test]
    fn slab_class_picks_smallest_fit_and_slack() {
        assert_eq!(slab_class_for_n_fks(0), None);
        assert_eq!(slab_class_for_n_fks(1), None);
        assert_eq!(slab_class_for_n_fks(2), Some(0));
        assert_eq!(slab_class_for_n_fks(3), Some(0));
        assert_eq!(slab_class_for_n_fks(4), Some(0));
        assert_eq!(slab_class_for_n_fks(5), Some(1));
        assert_eq!(slab_class_for_n_fks(8), Some(1));
        assert_eq!(slab_class_for_n_fks(9), Some(2));
        assert_eq!(slab_class_for_n_fks(256), Some(6));
        assert_eq!(slab_class_for_n_fks(257), None);
        assert_eq!(slab_class_for_n_fks_with_slack(4), Some(1));
        assert_eq!(slab_class_for_n_fks_with_slack(256), Some(6));
        let five: Vec<u64> = (1..=5u64).collect();
        let packed = encode_slab(&five).unwrap();
        assert_eq!(slab_class_for_packed_len(packed.len()), Some(0));
        let two = [1u64, 2];
        let packed2 = encode_slab(&two).unwrap();
        assert_eq!(slab_bytes(0), 16);
        assert_eq!(slab_class_for_packed_len(packed2.len()), Some(0));
        assert!(
            packed2.len() as u64 <= slab_bytes(0),
            "2-fk ULEB must fit class 0 16 B (len={})",
            packed2.len()
        );
        let three_hundred: Vec<u64> = (1..=300u64).collect();
        let packed = encode_slab(&three_hundred).unwrap();
        let class = slab_class_for_packed_len(packed.len()).expect("300 tight deltas fit a slab");
        assert!(class <= 5, "class={class} packed={}", packed.len());
        assert_eq!(slab_class_for_packed_len(0), None);
        assert_eq!(slab_class_for_packed_len(2049), None);
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
            let stream = encode_stream(raw).unwrap();
            assert!(
                stream.len() <= 8 * fks.len() || fks.is_empty(),
                "packed length {} > 8×n={}",
                stream.len(),
                fks.len()
            );
            let got = decode_stream(&stream, fks.len()).unwrap();
            assert_eq!(got, fks);
            let mut into = Vec::new();
            decode_fk_delta_stream_into(&stream, fks.len(), &mut into).unwrap();
            assert_eq!(into, got);
            let slab = encode_slab(raw).unwrap();
            assert_eq!(decode_slab(&slab).unwrap(), fks);
            let mut slab_got = Vec::new();
            decode_slab_payload_into(&slab, &mut slab_got).unwrap();
            assert_eq!(slab_got, fks);
            assert_eq!(slab.len(), 2 + stream.len());
            let mut stream_into = vec![0u8; raw.len().saturating_mul(10).max(1)];
            let sn = encode_fk_delta_stream_into(&mut stream_into, raw).unwrap();
            assert_eq!(&stream_into[..sn], stream.as_slice());
            let mut slab_into = vec![0u8; 2 + raw.len().saturating_mul(10).max(1)];
            let pn = encode_slab_payload_into(&mut slab_into, raw).unwrap();
            assert_eq!(&slab_into[..pn], slab.as_slice());
        }
        assert!(encode_stream(&[5, 5]).is_err());
        assert!(encode_stream(&[0]).is_err());
        assert!(encode_stream(&[SH_FLAG_BIT | 1]).is_err());
        assert!(encode_stream(&[1, SH_FLAG_BIT | 2]).is_err());
        assert!(decode_stream(&[0x00], 1).is_err());
        let mut flagged = Vec::new();
        crate::compact::write_uleb128(&mut flagged, SH_FLAG_BIT);
        assert!(decode_stream(&flagged, 1).is_err());
        assert!(decode_stream(&[0x01, 0x00], 2).is_err());
        assert!(decode_slab(&[0]).is_err());
        assert!(decode_stream(&[], 0).unwrap().is_empty());
        let mut keep = vec![Fk(99)];
        decode_fk_delta_stream_into(&[], 0, &mut keep).unwrap();
        assert_eq!(keep, vec![Fk(99)]);
        assert!(decode_slab_payload_into(&[0], &mut Vec::new()).is_err());
    }
}
