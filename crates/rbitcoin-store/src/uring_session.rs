//! Owned io_uring session for completion-driven pipelines.
//!
//! Used by online `tx.head` shadow fill and streaming archive head-resolve.
//! Distinct from [`crate::bulk_io::pread_batch`]'s thread-local "submit all,
//! wait for all" ring — this session is **owned** by one pipeline and must not
//! be shared across concurrent call stacks on the same thread.

use crate::error::StoreError;
use std::os::fd::RawFd;
use std::path::Path;

/// Default SQ/CQ depth (matches bulk_io / shadow fill).
pub const DEFAULT_ENTRIES: u32 = 1024;

/// Owned io_uring for multi-stage submit/complete loops.
pub struct UringSession {
    #[cfg(target_os = "linux")]
    ring: io_uring::IoUring,
    entries: u32,
    in_flight: usize,
}

impl UringSession {
    /// Open a private ring. Returns `Err` if io_uring is disabled or setup fails.
    pub fn new(entries: u32) -> Result<Self, StoreError> {
        if !crate::bulk_io::io_uring_enabled() {
            return Err(StoreError::Corrupt("io_uring unavailable"));
        }
        let entries = entries.max(32).min(4096);
        #[cfg(target_os = "linux")]
        {
            let ring = io_uring::IoUring::new(entries).map_err(|e| {
                StoreError::io(
                    Path::new("io_uring"),
                    std::io::Error::new(std::io::ErrorKind::Other, format!("io_uring: {e}")),
                )
            })?;
            Ok(Self {
                ring,
                entries,
                in_flight: 0,
            })
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = entries;
            Err(StoreError::Corrupt("io_uring is Linux-only"))
        }
    }

    pub fn in_flight(&self) -> usize {
        self.in_flight
    }

    pub fn free_sq(&self) -> usize {
        (self.entries as usize).saturating_sub(self.in_flight)
    }

    /// Push a pread SQE. Buffer must stay live until the CQE is harvested.
    pub fn push_pread(
        &mut self,
        fd: RawFd,
        offset: u64,
        buf: &mut [u8],
        user_data: u64,
    ) -> Result<(), StoreError> {
        #[cfg(target_os = "linux")]
        {
            use io_uring::{opcode, types};
            if buf.is_empty() {
                return Ok(());
            }
            if self.in_flight >= self.entries as usize {
                return Err(StoreError::Corrupt("io_uring SQ full (in_flight cap)"));
            }
            let sqe = opcode::Read::new(types::Fd(fd), buf.as_mut_ptr(), buf.len() as u32)
                .offset(offset)
                .build()
                .user_data(user_data);
            // SAFETY: caller keeps `buf` alive until matching CQE is harvested.
            unsafe {
                self.ring
                    .submission()
                    .push(&sqe)
                    .map_err(|_| StoreError::Corrupt("io_uring SQ full"))?;
            }
            self.in_flight += 1;
            Ok(())
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (fd, offset, buf, user_data);
            Err(StoreError::Corrupt("io_uring is Linux-only"))
        }
    }

    /// Push a pwrite SQE. Buffer must stay live until the CQE is harvested.
    pub fn push_pwrite(
        &mut self,
        fd: RawFd,
        offset: u64,
        buf: &[u8],
        user_data: u64,
    ) -> Result<(), StoreError> {
        #[cfg(target_os = "linux")]
        {
            use io_uring::{opcode, types};
            if buf.is_empty() {
                return Ok(());
            }
            if self.in_flight >= self.entries as usize {
                return Err(StoreError::Corrupt("io_uring SQ full (in_flight cap)"));
            }
            let sqe = opcode::Write::new(types::Fd(fd), buf.as_ptr(), buf.len() as u32)
                .offset(offset)
                .build()
                .user_data(user_data);
            // SAFETY: caller keeps `buf` alive until matching CQE is harvested.
            unsafe {
                self.ring
                    .submission()
                    .push(&sqe)
                    .map_err(|_| StoreError::Corrupt("io_uring SQ full"))?;
            }
            self.in_flight += 1;
            Ok(())
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (fd, offset, buf, user_data);
            Err(StoreError::Corrupt("io_uring is Linux-only"))
        }
    }

    pub fn sync_submission(&mut self) {
        #[cfg(target_os = "linux")]
        {
            self.ring.submission().sync();
        }
    }

    /// Submit pending SQEs and wait for at least one CQE. Does not harvest.
    pub fn submit_and_wait_one(&mut self) -> Result<(), StoreError> {
        #[cfg(target_os = "linux")]
        {
            self.ring
                .submit_and_wait(1)
                .map_err(|_| StoreError::Corrupt("io_uring submit_and_wait failed"))?;
            Ok(())
        }
        #[cfg(not(target_os = "linux"))]
        {
            Err(StoreError::Corrupt("io_uring is Linux-only"))
        }
    }

    /// Kick submission without waiting (non-blocking).
    pub fn submit(&mut self) -> Result<(), StoreError> {
        #[cfg(target_os = "linux")]
        {
            self.ring
                .submit()
                .map_err(|_| StoreError::Corrupt("io_uring submit failed"))?;
            Ok(())
        }
        #[cfg(not(target_os = "linux"))]
        {
            Err(StoreError::Corrupt("io_uring is Linux-only"))
        }
    }

    /// Harvest all currently ready CQEs (non-blocking). Each item is
    /// `(user_data, result)` where `result` is bytes transferred or negated errno.
    pub fn harvest_ready(&mut self) -> Vec<(u64, i32)> {
        #[cfg(target_os = "linux")]
        {
            self.ring.completion().sync();
            let mut out = Vec::new();
            for cqe in self.ring.completion() {
                self.in_flight = self.in_flight.saturating_sub(1);
                out.push((cqe.user_data(), cqe.result()));
            }
            out
        }
        #[cfg(not(target_os = "linux"))]
        {
            Vec::new()
        }
    }

    /// Drain any leftover CQEs (e.g. on error unwind).
    pub fn drain_all(&mut self) {
        #[cfg(target_os = "linux")]
        {
            let _ = self.ring.submit();
            self.ring.completion().sync();
            for _ in self.ring.completion() {
                self.in_flight = self.in_flight.saturating_sub(1);
            }
        }
    }
}

impl Drop for UringSession {
    fn drop(&mut self) {
        self.drain_all();
    }
}

/// Pack `(kind, slot)` into `user_data` (high 2 bits = kind).
#[inline]
pub fn pack_ud(kind: u64, slot: u32) -> u64 {
    const KIND_SHIFT: u64 = 62;
    (kind << KIND_SHIFT) | (slot as u64 & ((1u64 << KIND_SHIFT) - 1))
}

/// Unpack [`pack_ud`].
#[inline]
pub fn unpack_ud(ud: u64) -> (u64, u32) {
    const KIND_SHIFT: u64 = 62;
    let kind = ud >> KIND_SHIFT;
    let slot = (ud & ((1u64 << KIND_SHIFT) - 1)) as u32;
    (kind, slot)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pack_unpack_roundtrip() {
        for kind in [0u64, 1, 2, 3] {
            for slot in [0u32, 1, 42, 0xffff, u32::MAX] {
                let (k, s) = unpack_ud(pack_ud(kind, slot));
                assert_eq!(k, kind);
                assert_eq!(s, slot);
            }
        }
    }

    #[test]
    fn new_respects_io_uring_gate() {
        if !crate::bulk_io::io_uring_enabled() {
            assert!(UringSession::new(32).is_err());
            return;
        }
        let s = UringSession::new(32).expect("uring");
        assert_eq!(s.in_flight(), 0);
        assert_eq!(s.free_sq(), 32);
    }
}
