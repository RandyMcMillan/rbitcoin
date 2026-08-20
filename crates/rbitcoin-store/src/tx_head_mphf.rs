//! Sealed `tx.head` shard: BDZ MPHF + dense `u32` rels (one candidate).
//!
//! `base.mphf` is BDZ. `base.rel` is `n × 4` (1-based relative fk). Optional
//! `base.mlt` holds extra older rels for BIP30 (same mixed key, newer first
//! in `.rel`, rest here). A miss is fuse-gated by the caller.

use crate::bdz::BdzMphf;
use crate::bulk_io::{pread_batch, pread_batch_on_ctx, ReadOp};
use crate::error::StoreError;
use crate::io_handle::IoHandle;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

pub struct TxHeadMphf {
    base: PathBuf,
    mphf: BdzMphf,
    rel: File,
    mlt: HashMap<u32, Vec<u32>>,
}

pub fn mphf_path(base: &Path) -> PathBuf {
    sidecar(base, ".mphf")
}

pub fn rel_path(base: &Path) -> PathBuf {
    sidecar(base, ".rel")
}

fn mlt_path(base: &Path) -> PathBuf {
    sidecar(base, ".mlt")
}

fn sidecar(base: &Path, ext: &str) -> PathBuf {
    let mut s = base.as_os_str().to_os_string();
    s.push(ext);
    PathBuf::from(s)
}

impl TxHeadMphf {
    pub fn exists(base: &Path) -> bool {
        mphf_path(base).is_file() && rel_path(base).is_file()
    }

    pub fn write(base: impl AsRef<Path>, pairs: &[(u64, u32)]) -> Result<Self, StoreError> {
        let base = base.as_ref();
        if let Some(parent) = base.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let mut by_key: HashMap<u64, Vec<u32>> = HashMap::new();
        for &(k, rel) in pairs {
            if rel == 0 {
                return Err(StoreError::Corrupt("tx.head mphf: rel 0"));
            }
            by_key.entry(k).or_default().push(rel);
        }
        let keys: Vec<u64> = by_key.keys().copied().collect();
        let mphf = BdzMphf::build(&keys)?;
        let n = keys.len();
        let mut rels = vec![0u32; n];
        let mut mlt: HashMap<u32, Vec<u32>> = HashMap::new();
        for (k, mut rs) in by_key {
            let newest = rs.pop().unwrap();
            let slot = mphf.index(k)?;
            rels[slot as usize] = newest;
            if !rs.is_empty() {
                rs.reverse();
                mlt.insert(slot, rs);
            }
        }
        mphf.write_to(&mphf_path(base))?;
        {
            let rp = rel_path(base);
            let mut buf = Vec::with_capacity(n.saturating_mul(4));
            for r in &rels {
                buf.extend_from_slice(&r.to_le_bytes());
            }
            std::fs::write(&rp, &buf).map_err(|e| StoreError::io(&rp, e))?;
            let f = OpenOptions::new()
                .write(true)
                .open(&rp)
                .map_err(|e| StoreError::io(&rp, e))?;
            f.sync_all().map_err(|e| StoreError::io(&rp, e))?;
        }
        write_mlt(&mlt_path(base), &mlt)?;
        Self::open(base)
    }

    pub fn open(base: impl AsRef<Path>) -> Result<Self, StoreError> {
        let base = base.as_ref().to_path_buf();
        let mp = mphf_path(&base);
        let rp = rel_path(&base);
        let mphf = BdzMphf::read_from(&mp)?;
        let rel = OpenOptions::new()
            .read(true)
            .open(&rp)
            .map_err(|e| StoreError::io(&rp, e))?;
        let n = mphf.n() as u64;
        let meta = rel.metadata().map_err(|e| StoreError::io(&rp, e))?;
        if meta.len() != n.saturating_mul(4) {
            return Err(StoreError::Corrupt("tx.head mphf: rel length"));
        }
        let mlt = read_mlt(&mlt_path(&base))?;
        Ok(Self {
            base,
            mphf,
            rel,
            mlt,
        })
    }

    pub fn slots_for(&self, mixed_u64: &[u64]) -> Result<Vec<u32>, StoreError> {
        let mut out = Vec::with_capacity(mixed_u64.len());
        for &k in mixed_u64 {
            out.push(self.mphf.index(k)?);
        }
        Ok(out)
    }

    pub fn read_rels_batch(
        &self,
        slots: &[u32],
        ctx: &mut crate::IoCtx<'_>,
    ) -> Result<Vec<Vec<u32>>, StoreError> {
        let fd = IoHandle::from_file(&self.rel);
        let mut bufs = vec![[0u8; 4]; slots.len()];
        let mut out = vec![Vec::new(); slots.len()];
        {
            let mut ops: Vec<ReadOp<'_>> = bufs
                .iter_mut()
                .zip(slots.iter())
                .map(|(buf, &slot)| ReadOp {
                    fd,
                    offset: u64::from(slot) * 4,
                    buf: &mut buf[..],
                    result: 0,
                })
                .collect();
            if !ops.is_empty() {
                if !pread_batch_on_ctx(ctx, &mut ops) {
                    pread_batch(&mut ops);
                }
                for op in &ops {
                    if op.result < 4 {
                        return Err(StoreError::io(
                            &rel_path(&self.base),
                            std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                "tx.head rel pread short",
                            ),
                        ));
                    }
                }
            }
        }
        for (i, buf) in bufs.iter().enumerate() {
            let rel = u32::from_le_bytes(*buf);
            if rel != 0 {
                out[i].push(rel);
            }
            if let Some(extra) = self.mlt.get(&slots[i]) {
                out[i].extend_from_slice(extra);
            }
        }
        Ok(out)
    }
}

fn write_mlt(path: &Path, mlt: &HashMap<u32, Vec<u32>>) -> Result<(), StoreError> {
    if mlt.is_empty() {
        let _ = std::fs::remove_file(path);
        return Ok(());
    }
    let mut buf = Vec::new();
    buf.extend_from_slice(&(mlt.len() as u32).to_le_bytes());
    for (&slot, rels) in mlt {
        buf.extend_from_slice(&slot.to_le_bytes());
        buf.extend_from_slice(&(rels.len() as u16).to_le_bytes());
        for r in rels {
            buf.extend_from_slice(&r.to_le_bytes());
        }
    }
    std::fs::write(path, &buf).map_err(|e| StoreError::io(path, e))?;
    Ok(())
}

fn read_mlt(path: &Path) -> Result<HashMap<u32, Vec<u32>>, StoreError> {
    if !path.is_file() {
        return Ok(HashMap::new());
    }
    let buf = std::fs::read(path).map_err(|e| StoreError::io(path, e))?;
    if buf.len() < 4 {
        return Err(StoreError::Corrupt("tx.head mlt short"));
    }
    let n = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
    let mut i = 4usize;
    let mut out = HashMap::new();
    for _ in 0..n {
        if i + 6 > buf.len() {
            return Err(StoreError::Corrupt("tx.head mlt truncated"));
        }
        let slot = u32::from_le_bytes(buf[i..i + 4].try_into().unwrap());
        i += 4;
        let nrel = u16::from_le_bytes(buf[i..i + 2].try_into().unwrap()) as usize;
        i += 2;
        if i + nrel * 4 > buf.len() {
            return Err(StoreError::Corrupt("tx.head mlt rels"));
        }
        let mut rels = Vec::with_capacity(nrel);
        for _ in 0..nrel {
            rels.push(u32::from_le_bytes(buf[i..i + 4].try_into().unwrap()));
            i += 4;
        }
        out.insert(slot, rels);
    }
    Ok(out)
}
