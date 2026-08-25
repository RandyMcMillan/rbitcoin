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

    #[cfg(test)]
    pub fn slots_for(&self, mixed_u64: &[u64]) -> Result<Vec<u32>, StoreError> {
        self.slots_for_ctx(mixed_u64, &mut crate::IoCtx::none())
    }

    pub fn slots_for_ctx(
        &self,
        mixed_u64: &[u64],
        ctx: &mut crate::IoCtx<'_>,
    ) -> Result<Vec<u32>, StoreError> {
        self.mphf.index_batch(mixed_u64, ctx)
    }

    #[cfg(test)]
    pub fn take_g_page_preads(&self) -> u64 {
        self.mphf.take_g_page_preads()
    }

    pub fn g_bytes_resident(&self) -> u64 {
        self.mphf.g_bytes_resident() as u64
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
                match pread_batch_on_ctx(ctx, &mut ops)? {
                    true => {}
                    false => pread_batch(&mut ops),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bdz::BdzMphf;
    use crate::uring_session::{IoCtx, SessionKind, UringSession};

    fn tmp(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "rbitcoin-tx-mphf-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[test]
    fn shared_g_page_is_one_pread() {
        let dir = tmp("share");
        std::fs::create_dir_all(&dir).unwrap();
        let keys: Vec<u64> = (0..4_000u64)
            .map(|i| i.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(3))
            .collect();
        let ram = BdzMphf::build(&keys).unwrap();
        let p = dir.join("t.mphf");
        ram.write_to(&p).unwrap();
        let fd = BdzMphf::read_from(&p).unwrap();
        let page_of = |k: u64| {
            fd.vertices(k)
                .into_iter()
                .map(|v| v / 1024)
                .collect::<Vec<_>>()
        };
        let k0 = keys[0];
        let p0 = page_of(k0);
        let k1 = keys
            .iter()
            .copied()
            .find(|&k| k != k0 && page_of(k).iter().any(|p| p0.contains(p)))
            .expect("two keys sharing a g page");
        let _ = fd.take_g_page_preads();
        let a = fd.index(k0).unwrap();
        let b = fd.index(k1).unwrap();
        let serial_pages = fd.take_g_page_preads();
        let batch = fd.index_batch(&[k0, k1], &mut IoCtx::none()).unwrap();
        let batch_pages = fd.take_g_page_preads();
        assert_eq!(batch, vec![a, b]);
        let mut uniq = page_of(k0);
        uniq.extend(page_of(k1));
        uniq.sort_unstable();
        uniq.dedup();
        assert_eq!(batch_pages, uniq.len() as u64);
        assert!(batch_pages <= serial_pages);
        assert!(batch_pages >= 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn index_batch_held_session_submits_g_pages() {
        let dir = tmp("held");
        std::fs::create_dir_all(&dir).unwrap();
        let keys: Vec<u64> = (0..200u64).map(|i| i * 17 + 3).collect();
        let ram = BdzMphf::build(&keys).unwrap();
        let p = dir.join("t.mphf");
        ram.write_to(&p).unwrap();
        let fd = BdzMphf::read_from(&p).unwrap();
        let serial = fd.index_batch(&keys[..8], &mut IoCtx::none()).unwrap();
        let mut session = UringSession::try_open_kind(SessionKind::Pool, 32).expect("pool");
        let _ = crate::uring_session::test_take_last_sqe_lens();
        let mut ctx = IoCtx::held(&mut session);
        let batch = fd.index_batch(&keys[..8], &mut ctx).unwrap();
        session.drain_all().unwrap();
        assert_eq!(batch, serial);
        let sqes = crate::uring_session::test_take_last_sqe_lens();
        assert!(
            !sqes.is_empty(),
            "index_batch(held) must submit g pages on the held session"
        );
        assert!(sqes.iter().all(|&len| len > 0));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Held-session leftover `KIND_BULK_PREAD` must not be harvested as a BDZ
    /// g-page CQE (`bdz g page bad slot`). Drain the foreign SQE first.
    #[test]
    fn index_batch_held_session_drains_foreign_kind_leftover() {
        use std::io::Write;
        let dir = tmp("held-leftover");
        std::fs::create_dir_all(&dir).unwrap();
        let keys: Vec<u64> = (0..200u64).map(|i| i * 17 + 3).collect();
        let ram = BdzMphf::build(&keys).unwrap();
        let p = dir.join("t.mphf");
        ram.write_to(&p).unwrap();
        let fd_mphf = BdzMphf::read_from(&p).unwrap();
        let serial = fd_mphf.index_batch(&keys[..8], &mut IoCtx::none()).unwrap();

        let leftover_path = dir.join("leftover.bin");
        let mut leftover_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&leftover_path)
            .unwrap();
        leftover_file.write_all(&[0xAAu8; 8]).unwrap();
        leftover_file.sync_all().unwrap();
        let leftover_fd = crate::io_handle::IoHandle::from_file(&leftover_file);

        let mut session = UringSession::try_open_kind(SessionKind::Pool, 32).expect("pool");
        session.begin_batch().unwrap();
        let mut leftover_buf = [0u8; 8];
        let ud = crate::uring_session::pack_ud(
            crate::uring_session::KIND_BULK_PREAD,
            session.epoch(),
            0,
        );
        session
            .push_pread(leftover_fd, 0, &mut leftover_buf, ud)
            .unwrap();
        session.submit().unwrap();
        assert!(
            session.in_flight() > 0,
            "foreign SQE must still be pending when BDZ starts"
        );

        let mut ctx = IoCtx::held(&mut session);
        let batch = fd_mphf
            .index_batch(&keys[..8], &mut ctx)
            .unwrap_or_else(|e| {
                panic!("held index_batch with leftover KIND_BULK_PREAD must drain, not {e}")
            });
        drop(ctx);
        session.drain_all().unwrap();
        assert_eq!(batch, serial);
        assert_eq!(session.in_flight(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn tx_head_mphf_open_is_header_only() {
        let dir = tmp("open");
        std::fs::create_dir_all(&dir).unwrap();
        let base = dir.join("000000");
        let pairs: Vec<(u64, u32)> = (1..64u32)
            .map(|i| (u64::from(i).wrapping_mul(0x9e37_79b9_7f4a_7c15), i))
            .collect();
        let h = TxHeadMphf::write(&base, &pairs).unwrap();
        assert_eq!(h.mphf.g_bytes_resident(), 0);
        let slots = h.slots_for(&[pairs[0].0]).unwrap();
        assert_eq!(slots.len(), 1);
        let rels = h
            .read_rels_batch(&slots, &mut crate::IoCtx::none())
            .unwrap();
        assert_eq!(rels[0][0], pairs[0].1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Held `.rel` pread must fail closed on a poisoned TLS ring (f07415b5).
    /// Falling through to `pread_batch` nests `with_thread_local` and panics
    /// lookup (`ibd-confirm-lookup` nested thread-local io_uring).
    #[test]
    #[cfg(target_os = "linux")]
    fn held_rel_pread_poisoned_session_does_not_nest_tls() {
        if !crate::bulk_io::io_uring_enabled() {
            return;
        }
        let dir = tmp("held-rel-poison");
        std::fs::create_dir_all(&dir).unwrap();
        let base = dir.join("000000");
        let pairs = vec![(0x1111_u64, 1u32), (0x2222, 2)];
        let h = TxHeadMphf::write(&base, &pairs).unwrap();
        let slots = h.slots_for(&[pairs[0].0]).unwrap();

        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            crate::uring_session::with_thread_local(
                crate::uring_session::DEFAULT_ENTRIES,
                |session| {
                    session.poison();
                    h.read_rels_batch(&slots, &mut IoCtx::held(session))
                },
            )
        }));
        let _ = std::fs::remove_dir_all(&dir);
        match caught {
            Ok(Ok(Err(StoreError::Corrupt(msg))))
                if msg.contains("poisoned") || msg.contains("held") => {}
            Ok(Ok(Ok(_))) => panic!("poisoned held rel must fail closed, not succeed"),
            Ok(Err(e)) => panic!("with_thread_local setup failed: {e}"),
            Err(_) => panic!("nested thread-local io_uring: held rel must not open a second ring"),
            Ok(Ok(Err(e))) => panic!("unexpected held rel error: {e}"),
        }
    }
}
