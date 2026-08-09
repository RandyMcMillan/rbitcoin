//! Binary fuse8 membership for sealed `tx.head` / SH overflow segments.
//!
//! ~9 bits/key, no false negatives, FP ≈ 2⁻⁸. Built once at seal from mixed
//! probe keys (as u64). Open segments have no filter.
//!
//! On-disk: `BF8R` | u32 version LE | u64 body_len LE | body
//!
//! | Version | Body |
//! |--------:|------|
//! | **1** | Historical: bincode of xorf `BinaryFuse8` (pre in-tree port) — **not** decoded |
//! | **2** | Explicit LE: seed u64 · seg_len u32 · seg_mask u32 · seg_count_len u32 · fp_len u64 · fps |
//!
//! Opening a v1 file does **not** fail the whole store: callers get
//! [`SealedFuse8::always_probe`] + a rewrite flag so the operator can migrate
//! fuse payloads without wiping `tx.head`.

use crate::binary_fuse8::BinaryFuse8;
use crate::error::StoreError;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

const MAGIC: &[u8; 4] = b"BF8R";
/// Historical: bincode of xorf BinaryFuse8. Do not decode with the v2 algorithm.
pub const VERSION_V1: u32 = 1;
/// Explicit LE body (in-tree BinaryFuse8).
pub const VERSION_V2: u32 = 2;

/// Result of opening a sealed fuse file.
#[derive(Clone, Debug)]
pub enum FuseFileOpen {
    /// Current format; safe to use as a membership gate.
    Ready(SealedFuse8),
    /// Legacy or unreadable BF8R body. `gate` always returns true (probe the head).
    /// Caller should rebuild fuse keys and [`SealedFuse8::write_to`] as v2.
    NeedsRewrite {
        gate: SealedFuse8,
        reason: &'static str,
    },
}

/// On-disk / in-memory sealed fuse for one head segment.
#[derive(Clone, Debug)]
pub struct SealedFuse8 {
    /// `None` ⇒ always probe (migration placeholder; no false negatives).
    filter: Option<BinaryFuse8>,
}

impl SealedFuse8 {
    /// Build from distinct mixed-key u64s (caller must dedupe).
    pub fn build(keys: &[u64]) -> Result<Self, StoreError> {
        if keys.is_empty() {
            // Empty sealed segment: dummy key so construction succeeds.
            return Ok(Self {
                filter: Some(
                    BinaryFuse8::try_from_keys(&[0u64])
                        .map_err(|_| StoreError::Corrupt("binary fuse8 empty build"))?,
                ),
            });
        }
        let filter = BinaryFuse8::try_from_keys(keys)
            .map_err(|_| StoreError::Corrupt("binary fuse8 build failed (dup keys?)"))?;
        Ok(Self {
            filter: Some(filter),
        })
    }

    /// Temporary gate during fuse format migration: never skip a sealed probe.
    #[inline]
    pub fn always_probe() -> Self {
        Self { filter: None }
    }

    #[inline]
    pub fn is_always_probe(&self) -> bool {
        self.filter.is_none()
    }

    /// Membership test (no FN for keys passed to [`Self::build`]).
    /// Always-probe gates return true for every key.
    #[inline]
    pub fn contains(&self, key: u64) -> bool {
        match &self.filter {
            Some(f) => f.contains(key),
            None => true,
        }
    }

    /// Fingerprint array length (bytes); 0 for always-probe.
    pub fn fingerprint_bytes(&self) -> usize {
        self.filter.as_ref().map(|f| f.len()).unwrap_or(0)
    }

    /// Write current (v2) layout. Panics not used — always-probe must not be written.
    pub fn write_to(&self, path: &Path) -> Result<(), StoreError> {
        let filter = self
            .filter
            .as_ref()
            .ok_or_else(|| StoreError::Corrupt("fuse8 write always-probe placeholder"))?;
        let body = encode_body(filter);
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

    /// Strict open: only Ready v2. Prefer [`open_file`] when migration is allowed.
    pub fn read_from(path: &Path) -> Result<Self, StoreError> {
        match open_file(path)? {
            FuseFileOpen::Ready(f) => Ok(f),
            FuseFileOpen::NeedsRewrite { reason, .. } => Err(StoreError::Corrupt(reason)),
        }
    }
}

/// Open a BF8R fuse file, classifying legacy v1 for soft migration.
pub fn open_file(path: &Path) -> Result<FuseFileOpen, StoreError> {
    let mut f = File::open(path).map_err(|e| StoreError::io(path, e))?;
    let mut hdr = [0u8; 16];
    f.read_exact(&mut hdr)
        .map_err(|e| StoreError::io(path, e))?;
    if &hdr[0..4] != MAGIC {
        return Err(StoreError::Corrupt("tx.head fuse magic"));
    }
    let ver = u32::from_le_bytes(hdr[4..8].try_into().unwrap());
    let len = u64::from_le_bytes(hdr[8..16].try_into().unwrap()) as usize;
    let mut payload = vec![0u8; len];
    f.read_exact(&mut payload)
        .map_err(|e| StoreError::io(path, e))?;

    match ver {
        VERSION_V1 => Ok(FuseFileOpen::NeedsRewrite {
            gate: SealedFuse8::always_probe(),
            reason: "fuse8 v1 (xorf/bincode) — rewrite as v2",
        }),
        VERSION_V2 => match decode_body(&payload) {
            Ok(filter) => Ok(FuseFileOpen::Ready(SealedFuse8 {
                filter: Some(filter),
            })),
            Err(_) => Ok(FuseFileOpen::NeedsRewrite {
                gate: SealedFuse8::always_probe(),
                reason: "fuse8 v2 body unreadable — rewrite",
            }),
        },
        _ => Err(StoreError::Corrupt("tx.head fuse version")),
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
    // Sanity: refuse absurd lengths that would OOM or never match a real seal.
    if fp_len > 512 * 1024 * 1024 {
        return Err(StoreError::Corrupt("fuse8 fingerprints too large"));
    }
    if segment_length == 0 || segment_length_mask != segment_length.saturating_sub(1) {
        return Err(StoreError::Corrupt("fuse8 segment_length invalid"));
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
        assert!(matches!(open_file(&path), Err(StoreError::Corrupt(_))));
        let mut bad_ver = Vec::from(*MAGIC);
        bad_ver.extend_from_slice(&99u32.to_le_bytes());
        bad_ver.extend_from_slice(&0u64.to_le_bytes());
        std::fs::write(&path, &bad_ver).unwrap();
        assert!(matches!(open_file(&path), Err(StoreError::Corrupt(_))));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn v1_opens_as_needs_rewrite_always_probe() {
        let dir = tmp();
        let path = dir.join("legacy.fuse8");
        // Minimal v1 envelope: magic + version 1 + empty body (historical xorf path).
        let mut raw = Vec::from(*MAGIC);
        raw.extend_from_slice(&VERSION_V1.to_le_bytes());
        raw.extend_from_slice(&0u64.to_le_bytes());
        std::fs::write(&path, &raw).unwrap();
        match open_file(&path).unwrap() {
            FuseFileOpen::NeedsRewrite { gate, reason } => {
                assert!(gate.is_always_probe());
                assert!(gate.contains(0xdead));
                assert!(reason.contains("v1"));
            }
            FuseFileOpen::Ready(_) => panic!("v1 must not decode as ready"),
        }
        // Strict read_from still fails so accidental uses stay loud.
        assert!(SealedFuse8::read_from(&path).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn always_probe_must_not_write() {
        let dir = tmp();
        let path = dir.join("nope.fuse8");
        assert!(SealedFuse8::always_probe().write_to(&path).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
