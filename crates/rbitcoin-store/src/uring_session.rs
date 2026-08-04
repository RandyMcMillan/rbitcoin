//! Owned io_uring session — **the** ring abstraction for this crate.
//!
//! All production io_uring work goes through [`UringSession`] via
//! [`with_thread_local`]:
//! - plan head-resolve (confirm stamp / denserels)
//! - spend annotate abs-meta RMW / pure pwrite
//! - [`crate::bulk_io`] pread/pwrite batches and page RMW
//!
//! **Lifetime:** one long-lived ring per OS thread (TLS). Nested uring on the
//! same thread opens a temporary ring so re-entrancy never shares mid-wave.
//! Do **not** call [`UringSession::new`] on hot paths — that setup/teardowns
//! every batch.

use crate::error::StoreError;
use std::os::fd::RawFd;
use std::path::Path;

/// Default SQ/CQ depth for all store io_uring sessions (bulk, plan head-resolve,
/// spend annotate). TLS rings open at this size; [`with_thread_local`] may grow
/// if a caller requests more (none currently do).
pub const DEFAULT_ENTRIES: u32 = 128;

/// Linux `RWF_DONTCACHE` — drop pages after IO (kernel 6.14+; ignored if unsupported).
///
/// Set on SQE `rw_flags` for Class A body and cold head/idx/sidefile peeks.
#[cfg(target_os = "linux")]
pub const RWF_DONTCACHE: i32 = 0x0000_0080;
#[cfg(not(target_os = "linux"))]
pub const RWF_DONTCACHE: i32 = 0;

/// Owned io_uring for multi-stage submit/complete loops.
pub struct UringSession {
    #[cfg(target_os = "linux")]
    ring: io_uring::IoUring,
    entries: u32,
    in_flight: usize,
}

impl UringSession {
    /// Open a private ring. Returns `Err` if io_uring is disabled or setup fails.
    ///
    /// Prefer [`with_thread_local`] on production paths (avoids setup/teardown).
    /// Kept for unit tests / one-shot probes only.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn new(entries: u32) -> Result<Self, StoreError> {
        if !crate::bulk_io::io_uring_enabled() {
            return Err(StoreError::Corrupt("io_uring unavailable"));
        }
        Self::try_open(entries)
    }

    /// Create a ring without consulting [`crate::bulk_io::io_uring_enabled`].
    ///
    /// Used for capability probe and by TLS open after the gate has already
    /// passed (avoids recursion through `io_uring_enabled` → probe → here).
    pub(crate) fn try_open(entries: u32) -> Result<Self, StoreError> {
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

    pub fn entries(&self) -> u32 {
        self.entries
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
        self.push_pread_flags(fd, offset, buf, user_data, 0)
    }

    /// Like [`push_pread`] with optional `rw_flags` (e.g. [`RWF_DONTCACHE`]).
    pub fn push_pread_flags(
        &mut self,
        fd: RawFd,
        offset: u64,
        buf: &mut [u8],
        user_data: u64,
        rw_flags: i32,
    ) -> Result<(), StoreError> {
        #[cfg(test)]
        test_note_sqe_rw_flags(rw_flags);
        #[cfg(target_os = "linux")]
        {
            use io_uring::{opcode, types};
            if buf.is_empty() {
                return Ok(());
            }
            if self.in_flight >= self.entries as usize {
                return Err(StoreError::Corrupt("io_uring SQ full (in_flight cap)"));
            }
            let mut b = opcode::Read::new(types::Fd(fd), buf.as_mut_ptr(), buf.len() as u32)
                .offset(offset);
            if rw_flags != 0 {
                b = b.rw_flags(rw_flags);
            }
            let sqe = b.build().user_data(user_data);
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
            let _ = (fd, offset, buf, user_data, rw_flags);
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
        self.push_pwrite_flags(fd, offset, buf, user_data, 0)
    }

    /// Like [`push_pwrite`] with optional `rw_flags` (e.g. [`RWF_DONTCACHE`]).
    pub fn push_pwrite_flags(
        &mut self,
        fd: RawFd,
        offset: u64,
        buf: &[u8],
        user_data: u64,
        rw_flags: i32,
    ) -> Result<(), StoreError> {
        #[cfg(test)]
        test_note_sqe_rw_flags(rw_flags);
        #[cfg(target_os = "linux")]
        {
            use io_uring::{opcode, types};
            if buf.is_empty() {
                return Ok(());
            }
            if self.in_flight >= self.entries as usize {
                return Err(StoreError::Corrupt("io_uring SQ full (in_flight cap)"));
            }
            let mut b = opcode::Write::new(types::Fd(fd), buf.as_ptr(), buf.len() as u32)
                .offset(offset);
            if rw_flags != 0 {
                b = b.rw_flags(rw_flags);
            }
            let sqe = b.build().user_data(user_data);
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
            let _ = (fd, offset, buf, user_data, rw_flags);
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
/// Run `f` with this **OS thread's** long-lived io_uring session.
///
/// - Opens once on first use; reopens only if `min_entries` exceeds the current
///   ring size (grows in place, never shrinks).
/// - Nested calls open a **temporary** ring (re-entrancy safe; no mid-wave share).
/// - Drains stray in-flight CQEs before handing the session to `f`.
///
/// Returns `Err` if io_uring is unavailable or setup fails. Callers that prefer
/// a silent fallback can map `Err` themselves (bulk_io does).
pub fn with_thread_local<R>(
    min_entries: u32,
    f: impl FnOnce(&mut UringSession) -> R,
) -> Result<R, StoreError> {
    let min_entries = min_entries.max(32).min(4096);

    #[cfg(not(target_os = "linux"))]
    {
        let _ = min_entries;
        let _ = f;
        return Err(StoreError::Corrupt("io_uring is Linux-only"));
    }

    #[cfg(target_os = "linux")]
    {
        use std::cell::{Cell, RefCell};

        thread_local! {
            static SESSION: RefCell<Option<UringSession>> = const { RefCell::new(None) };
            static DEPTH: Cell<u32> = const { Cell::new(0) };
        }

        // Gate once; TLS open uses try_open to avoid recursive enabled() probe.
        if !crate::bulk_io::io_uring_enabled() {
            return Err(StoreError::Corrupt("io_uring unavailable"));
        }

        DEPTH.with(|depth| {
            let d = depth.get();
            depth.set(d.saturating_add(1));
            let out = if d == 0 {
                SESSION.with(|cell| -> Result<R, StoreError> {
                    let mut slot = cell.borrow_mut();
                    let need_open = match slot.as_ref() {
                        None => true,
                        Some(s) => s.entries() < min_entries,
                    };
                    if need_open {
                        if let Some(mut old) = slot.take() {
                            if old.in_flight() != 0 {
                                old.drain_all();
                            }
                            drop(old);
                        }
                        let open_n = min_entries.max(DEFAULT_ENTRIES);
                        *slot = Some(UringSession::try_open(open_n)?);
                    }
                    let session = slot.as_mut().expect("session just ensured");
                    if session.in_flight() != 0 {
                        session.drain_all();
                    }
                    Ok(f(session))
                })
            } else {
                let mut s = UringSession::try_open(min_entries.max(DEFAULT_ENTRIES))?;
                Ok(f(&mut s))
            };
            depth.set(d);
            out
        })
    }
}



// Test hook: last SQE rw_flags values from push_*_flags.
#[cfg(test)]
thread_local! {
    static LAST_SQE_RW_FLAGS: std::cell::RefCell<Vec<i32>> =
        std::cell::RefCell::new(Vec::new());
}

#[cfg(test)]
fn test_note_sqe_rw_flags(rw_flags: i32) {
    LAST_SQE_RW_FLAGS.with(|c| c.borrow_mut().push(rw_flags));
}

/// Drain recorded SQE rw_flags (tests only).
#[cfg(test)]
pub fn test_take_last_sqe_rw_flags() -> Vec<i32> {
    LAST_SQE_RW_FLAGS.with(|c| std::mem::take(&mut *c.borrow_mut()))
}

/// Pack `(kind, slot)` into `user_data` (high 2 bits = kind).
///
/// Used by multi-stage io_uring machines (head resolve, spend annotate).
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
