//! Binary fuse8 membership for sealed `tx.head` segments.
//!
//! ~9 bits/key, no false negatives, FP ≈ 2⁻⁸. Built once at seal from mixed
//! probe keys (as u64). Open segments have no filter.
//!
//! On-disk: `BF8R` | u32 version LE | u64 body_len LE | body
//! body (v1 bincode-compatible / v2 explicit) =
//!   seed u64 | segment_length u32 | segment_length_mask u32 |
//!   segment_count_length u32 | fingerprints_len u64 | fingerprints

use crate::binary_fuse8::BinaryFuse8;
use crate::error::StoreError;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

const MAGIC: &[u8; 4] = b"BF8R";
/// Historical files written with bincode of xorf BinaryFuse8 (same field order).
const VERSION_V1: u32 = 1;
/// Explicit LE body (no bincode).
const VERSION_V2: u32 = 2;

/// On-disk / in-memory sealed fuse for one head segment.
#[derive(Clone)]
pub struct SealedFuse8 {
    filter: BinaryFuse8,
}

impl SealedFuse8 {
    /// Build from distinct mixed-key u64s (caller must dedupe).
    pub fn build(keys: &[u64]) -> Result<Self, StoreError> {
        if keys.is_empty() {
            // Empty sealed segment: dummy key so construction succeeds; callers
            // never insert 0 as a real fuse key for empty segments in practice
            // (mixed keys from txids). Membership of real keys stays false.
            return Ok(Self {
                filter: BinaryFuse8::try_from_keys(&[0u64])
                    .map_err(|_| StoreError::Corrupt("binary fuse8 empty build"))?,
            });
        }
        let filter = BinaryFuse8::try_from_keys(keys)
            .map_err(|_| StoreError::Corrupt("binary fuse8 build failed (dup keys?)"))?;
        Ok(Self { filter })
    }

    /// Membership test (no FN for keys passed to [`Self::build`]).
    #[inline]
    pub fn contains(&self, key: u64) -> bool {
        self.filter.contains(key)
    }

    /// Fingerprint array length (bytes).
    pub fn fingerprint_bytes(&self) -> usize {
        self.filter.len()
    }

    pub fn write_to(&self, path: &Path) -> Result<(), StoreError> {
        let body = encode_body(&self.filter);
        let mut f = File::create(path).map_err(|e| StoreError::io(path, e))?;
        f.write_all(MAGIC).map_err(|e| StoreError::io(path, e))?;
        f.write_all(&VERSION_V2.to_le_bytes())
            .map_err(|e| StoreError::io(path, e))?;
        f.write_all(&(body.len() as u64).to_le_bytes())
            .map_err(|e| StoreError::io(path, e))?;
        f.write_all(&body).map_err(|e| StoreError::io(path, e))?;
        f.sync_all().map_err(|e| StoreError::io(path, e))?;
        Ok(())
    }

    pub fn read_from(path: &Path) -> Result<Self, StoreError> {
        let mut f = File::open(path).map_err(|e| StoreError::io(path, e))?;
        let mut hdr = [0u8; 16];
        f.read_exact(&mut hdr)
            .map_err(|e| StoreError::io(path, e))?;
        if &hdr[0..4] != MAGIC {
            return Err(StoreError::Corrupt("tx.head fuse magic"));
        }
        let ver = u32::from_le_bytes(hdr[4..8].try_into().unwrap());
        if ver != VERSION_V1 && ver != VERSION_V2 {
            return Err(StoreError::Corrupt("tx.head fuse version"));
        }
        let len = u64::from_le_bytes(hdr[8..16].try_into().unwrap()) as usize;
        let mut payload = vec![0u8; len];
        f.read_exact(&mut payload)
            .map_err(|e| StoreError::io(path, e))?;
        let filter = decode_body(&payload)?;
        Ok(Self { filter })
    }
}

fn encode_body(filter: &BinaryFuse8) -> Vec<u8> {
    let fp = filter.fingerprints.as_ref();
    let mut body = Vec::with_capacity(8 + 4 + 4 + 4 + 8 + fp.len());
    body.extend_from_slice(&filter.seed.to_le_bytes());
    body.extend_from_slice(&filter.segment_length.to_le_bytes());
    body.extend_from_slice(&filter.segment_length_mask.to_le_bytes());
    body.extend_from_slice(&filter.segment_count_length.to_le_bytes());
    body.extend_from_slice(&(fp.len() as u64).to_le_bytes());
    body.extend_from_slice(fp);
    body
}

fn decode_body(payload: &[u8]) -> Result<BinaryFuse8, StoreError> {
    if payload.len() < 8 + 4 + 4 + 4 + 8 {
        return Err(StoreError::Corrupt("fuse8 body short"));
    }
    let mut o = 0usize;
    let seed = u64::from_le_bytes(payload[o..o + 8].try_into().unwrap());
    o += 8;
    let segment_length = u32::from_le_bytes(payload[o..o + 4].try_into().unwrap());
    o += 4;
    let segment_length_mask = u32::from_le_bytes(payload[o..o + 4].try_into().unwrap());
    o += 4;
    let segment_count_length = u32::from_le_bytes(payload[o..o + 4].try_into().unwrap());
    o += 4;
    let fp_len = u64::from_le_bytes(payload[o..o + 8].try_into().unwrap()) as usize;
    o += 8;
    if payload.len() < o + fp_len {
        return Err(StoreError::Corrupt("fuse8 fingerprints truncated"));
    }
    let fingerprints = payload[o..o + fp_len].to_vec().into_boxed_slice();
    Ok(BinaryFuse8 {
        seed,
        segment_length,
        segment_length_mask,
        segment_count_length,
        fingerprints,
    })
}

/// Fold a 32-byte mixed head key into a u64 fuse key (stable, keyed via mix).
#[inline]
pub fn fuse_key_from_mixed(mixed: &[u8; 32]) -> u64 {
    u64::from_le_bytes(mixed[0..8].try_into().unwrap())
        ^ u64::from_le_bytes(mixed[8..16].try_into().unwrap())
        ^ u64::from_le_bytes(mixed[16..24].try_into().unwrap())
        ^ u64::from_le_bytes(mixed[24..32].try_into().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn tmp() -> std::path::PathBuf {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let p = std::env::temp_dir().join(format!("rbitcoin-fuse8-{n}"));
        let _ = std::fs::create_dir_all(&p);
        p
    }

    #[test]
    fn no_false_negatives_and_roundtrip() {
        let keys: Vec<u64> = (0..10_000u64)
            .map(|i| i.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(7))
            .collect();
        let f = SealedFuse8::build(&keys).unwrap();
        for &k in &keys {
            assert!(f.contains(k), "FN on {k}");
        }
        let dir = tmp();
        let path = dir.join("seg.fuse8");
        f.write_to(&path).unwrap();
        let f2 = SealedFuse8::read_from(&path).unwrap();
        for &k in &keys {
            assert!(f2.contains(k));
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn fuse_key_stable() {
        let m = [1u8; 32];
        assert_eq!(fuse_key_from_mixed(&m), fuse_key_from_mixed(&m));
    }

    #[test]
    fn empty_build_and_bad_header_errors() {
        let empty = SealedFuse8::build(&[]).unwrap();
        let _ = empty.contains(0);
        assert!(empty.fingerprint_bytes() > 0);

        let dir = tmp();
        let path = dir.join("bad.fuse8");
        std::fs::write(
            &path,
            b"XXXX\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
        )
        .unwrap();
        assert!(matches!(
            SealedFuse8::read_from(&path),
            Err(StoreError::Corrupt(_))
        ));
        let mut bad_ver = Vec::from(*MAGIC);
        bad_ver.extend_from_slice(&99u32.to_le_bytes());
        bad_ver.extend_from_slice(&0u64.to_le_bytes());
        std::fs::write(&path, &bad_ver).unwrap();
        assert!(matches!(
            SealedFuse8::read_from(&path),
            Err(StoreError::Corrupt(_))
        ));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
