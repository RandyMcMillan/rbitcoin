//! Growable mmap-backed table files with a common header.

use crate::error::StoreError;
use memmap2::MmapMut;
use rbitcoin_primitives::{TableKind, SCHEMA_VERSION, STORE_MAGIC};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Mutex;

pub const FILE_HEADER_LEN: usize = 16;

pub struct TableFile {
    path: PathBuf,
    file: Mutex<File>,
    map: Mutex<MmapMut>,
    /// Logical length including header.
    len: Mutex<u64>,
}

impl TableFile {
    pub fn create(path: impl Into<PathBuf>, kind: TableKind) -> Result<Self, StoreError> {
        let path = path.into();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|e| StoreError::io(&path, e))?;

        let mut header = [0u8; FILE_HEADER_LEN];
        header[0..4].copy_from_slice(&STORE_MAGIC);
        header[4..6].copy_from_slice(&SCHEMA_VERSION.to_le_bytes());
        header[6..8].copy_from_slice(&kind.as_u16().to_le_bytes());
        file.write_all(&header)
            .map_err(|e| StoreError::io(&path, e))?;
        file.flush().map_err(|e| StoreError::io(&path, e))?;

        // Start with a small mapped region; grows on write.
        let initial = FILE_HEADER_LEN as u64 + 64;
        file.set_len(initial)
            .map_err(|e| StoreError::io(&path, e))?;
        // SAFETY: exclusive file we just created; length set above.
        let map = unsafe { MmapMut::map_mut(&file) }.map_err(|e| StoreError::io(&path, e))?;

        let _ = kind;
        Ok(Self {
            path,
            file: Mutex::new(file),
            map: Mutex::new(map),
            len: Mutex::new(FILE_HEADER_LEN as u64),
        })
    }

    pub fn open(path: impl Into<PathBuf>, kind: TableKind) -> Result<Self, StoreError> {
        let path = path.into();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|e| StoreError::io(&path, e))?;

        let mut header = [0u8; FILE_HEADER_LEN];
        file.read_exact(&mut header)
            .map_err(|e| StoreError::io(&path, e))?;
        if header[0..4] != STORE_MAGIC {
            return Err(StoreError::BadMagic);
        }
        let ver = u16::from_le_bytes([header[4], header[5]]);
        if ver != SCHEMA_VERSION {
            return Err(StoreError::BadSchema(ver));
        }
        let got = u16::from_le_bytes([header[6], header[7]]);
        if got != kind.as_u16() {
            return Err(StoreError::BadKind {
                expected: kind.as_u16(),
                got,
            });
        }

        let file_len = file.metadata().map_err(|e| StoreError::io(&path, e))?.len();
        // Header was fully read; file_len is at least FILE_HEADER_LEN.

        // v0: reserved bytes 8..16 store logical length (including header).
        let mut logical = u64::from_le_bytes(header[8..16].try_into().unwrap());
        if logical < FILE_HEADER_LEN as u64 {
            logical = FILE_HEADER_LEN as u64;
        }
        if logical > file_len {
            // Clamp corrupt HWMs instead of refusing open (rebuildable store).
            logical = file_len;
        }

        let map = unsafe { MmapMut::map_mut(&file) }.map_err(|e| StoreError::io(&path, e))?;
        Ok(Self {
            path,
            file: Mutex::new(file),
            map: Mutex::new(map),
            len: Mutex::new(logical),
        })
    }

    pub fn logical_len(&self) -> u64 {
        *self.len.lock().unwrap()
    }

    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    /// Shrink or set logical length (must be ≥ header size). Does not zero freed bytes.
    ///
    /// Updates the in-memory HWM and mmap header immediately; durable file HWM
    /// is written on [`flush`] (avoids seek+write per append during IBD).
    pub fn set_logical_len(&self, logical: u64) -> Result<(), StoreError> {
        if logical < FILE_HEADER_LEN as u64 {
            return Err(StoreError::Corrupt("logical length below header"));
        }
        self.ensure_capacity(logical)?;
        let mut len = self.len.lock().unwrap();
        *len = logical;
        self.write_hwm_mmap(logical);
        Ok(())
    }

    pub fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<(), StoreError> {
        let end = offset.saturating_add(buf.len() as u64);
        let len = *self.len.lock().unwrap();
        if end > len {
            return Err(StoreError::Corrupt("read past logical end"));
        }
        let map = self.map.lock().unwrap();
        buf.copy_from_slice(&map[offset as usize..end as usize]);
        Ok(())
    }

    pub fn write_at(&self, offset: u64, bytes: &[u8]) -> Result<(), StoreError> {
        let end = offset.saturating_add(bytes.len() as u64);
        self.ensure_capacity(end)?;
        {
            let mut map = self.map.lock().unwrap();
            map[offset as usize..end as usize].copy_from_slice(bytes);
        }
        let mut len = self.len.lock().unwrap();
        if end > *len {
            *len = end;
            drop(len);
            // Defer durable HWM seek to flush; only update mapped header.
            self.write_hwm_mmap(end);
        }
        Ok(())
    }

    /// Ensure the mmap covers at least `need` bytes (pre-grow for mega-batches).
    ///
    /// Large grows are the main host-stall risk during IBD: remapping multi‑GiB
    /// tables forces TLB/page-table work. We (1) grow in large steps so remaps
    /// are rare, (2) prefer `fallocate` so the FS preallocates without writing
    /// zeros, (3) drop the mmap before `set_len` so we do not hold a live
    /// mapping across the size change.
    pub fn ensure_capacity(&self, need: u64) -> Result<(), StoreError> {
        // Fast path: capacity already sufficient (short map lock).
        {
            let map = self.map.lock().unwrap();
            if need <= map.len() as u64 {
                return Ok(());
            }
        }
        // Small files: geometric growth (cheap remaps). Large files: do **not**
        // double (doubling a 50 GiB table wastes tens of GiB of disk). Use need
        // plus headroom; step size grows with file size so remap frequency falls.
        const DOUBLE_UNTIL: u64 = 64 * 1024 * 1024;
        let cur = {
            let map = self.map.lock().unwrap();
            map.len() as u64
        };
        if need <= cur {
            return Ok(());
        }
        let (headroom, step) = if cur >= 8 * 1024 * 1024 * 1024 {
            // ≥8 GiB tables: 1 GiB headroom / 512 MiB steps — fewer remaps.
            (1024 * 1024 * 1024u64, 512 * 1024 * 1024u64)
        } else if cur >= 1024 * 1024 * 1024 {
            (512 * 1024 * 1024u64, 256 * 1024 * 1024u64)
        } else {
            (256 * 1024 * 1024u64, 64 * 1024 * 1024u64)
        };
        let new_cap = if cur < DOUBLE_UNTIL {
            let mut c = cur.max(64);
            while c < need {
                c = c.saturating_mul(2).max(need);
            }
            c
        } else {
            need.saturating_add(headroom)
                .div_ceil(step)
                .saturating_mul(step)
                .max(need)
        };

        let mut map = self.map.lock().unwrap();
        // Re-check under lock (another grow may have raced — rare; exclusive writer in IBD).
        if need <= map.len() as u64 {
            return Ok(());
        }
        let file = self.file.lock().unwrap();
        // Prefer fallocate (prealloc without zero-fill write storm); then ensure size.
        if try_fallocate(&file, new_cap).is_err() {
            file.set_len(new_cap)
                .map_err(|e| StoreError::io(&self.path, e))?;
        } else if file.metadata().map(|m| m.len()).unwrap_or(0) < new_cap {
            file.set_len(new_cap)
                .map_err(|e| StoreError::io(&self.path, e))?;
        }
        // SAFETY: exclusive map+file locks; length ≥ new_cap.
        let new_map =
            unsafe { MmapMut::map_mut(&*file) }.map_err(|e| StoreError::io(&self.path, e))?;
        *map = new_map;
        Ok(())
    }

    /// Punch a hole over `[offset, offset+len)` so the range reads as zeros
    /// without writing a zero-fill. Used by hash-head rehash to clear multi‑GiB
    /// tables without an IO storm. Best-effort: falls back to write zeros.
    pub fn zero_range(&self, offset: u64, len: u64) -> Result<(), StoreError> {
        if len == 0 {
            return Ok(());
        }
        self.ensure_capacity(offset.saturating_add(len))?;
        let file = self.file.lock().unwrap();
        if try_punch_hole(&file, offset, len).is_ok() {
            return Ok(());
        }
        drop(file);
        // Fallback: chunked zero writes (slow on multi‑GiB — avoid when possible).
        let zero = vec![0u8; 1024 * 1024];
        let mut written = 0u64;
        while written < len {
            let chunk = ((len - written) as usize).min(zero.len());
            self.write_at(offset + written, &zero[..chunk])?;
            written += chunk as u64;
        }
        Ok(())
    }

    /// Update logical length in the mapped header only (no file seek).
    fn write_hwm_mmap(&self, logical: u64) {
        let mut map = self.map.lock().unwrap();
        if map.len() >= 16 {
            map[8..16].copy_from_slice(&logical.to_le_bytes());
        }
    }

    /// Write durable logical length into the file header (bytes 8..16).
    fn persist_logical_len(&self, logical: u64) -> Result<(), StoreError> {
        self.write_hwm_mmap(logical);
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(8))
            .map_err(|e| StoreError::io(&self.path, e))?;
        file.write_all(&logical.to_le_bytes())
            .map_err(|e| StoreError::io(&self.path, e))?;
        Ok(())
    }

    /// Persist HWM to the file header, flush dirty mmap pages, and `sync_data`.
    ///
    /// **Costly on multi‑GiB tables** — prefer rare header-only flushes during IBD.
    /// When [`crate::ibd_io_policy::defer_durable_flush`] is set (normally false;
    /// window), only the mmap HWM is updated — no msync/fdatasync.
    pub fn flush(&self) -> Result<(), StoreError> {
        let logical = *self.len.lock().unwrap();
        self.persist_logical_len(logical)?;
        if crate::ibd_io_policy::defer_durable_flush() {
            return Ok(());
        }
        self.map
            .lock()
            .unwrap()
            .flush()
            .map_err(|e| StoreError::io(&self.path, e))?;
        self.file
            .lock()
            .unwrap()
            .sync_data()
            .map_err(|e| StoreError::io(&self.path, e))?;
        Ok(())
    }

    /// Persist HWM + `msync(MS_ASYNC)` — schedules writeback without waiting.
    ///
    /// Prefer this (or HWM-only) on multi‑GiB Class A tables at process exit so the
    /// host UI is not frozen by multi-minute `MS_SYNC`/`fdatasync` storms.
    /// Skipped entirely when [`crate::ibd_io_policy::defer_durable_flush`] is set.
    pub fn flush_async(&self) -> Result<(), StoreError> {
        let logical = *self.len.lock().unwrap();
        self.persist_logical_len(logical)?;
        if crate::ibd_io_policy::defer_durable_flush() {
            return Ok(());
        }
        self.map
            .lock()
            .unwrap()
            .flush_async()
            .map_err(|e| StoreError::io(&self.path, e))?;
        Ok(())
    }

    /// Walk a byte range under the mmap lock without copying (caller must not
    /// hold other table locks that could deadlock).
    pub fn with_bytes<R>(
        &self,
        offset: u64,
        len: u64,
        f: impl FnOnce(&[u8]) -> R,
    ) -> Result<R, StoreError> {
        let end = offset.saturating_add(len);
        let logical = *self.len.lock().unwrap();
        if end > logical {
            return Err(StoreError::Corrupt("with_bytes past logical end"));
        }
        let map = self.map.lock().unwrap();
        if end as usize > map.len() {
            return Err(StoreError::Corrupt("with_bytes past map end"));
        }
        Ok(f(&map[offset as usize..end as usize]))
    }

    /// `mlock` page-aligned coverage of `[offset, offset+len)`.
    ///
    /// Expands to whole pages so subsequent reads never soft-fault. Requires a
    /// sufficient `RLIMIT_MEMLOCK` (see [`ensure_memlock_budget`]). Returns the
    /// locked page range `(page_start, page_len)` for later [`munlock_range`].
    /// Empty range is a no-op (`0, 0`).
    pub fn mlock_range(&self, offset: u64, len: u64) -> Result<(u64, u64), StoreError> {
        if len == 0 {
            return Ok((0, 0));
        }
        #[cfg(unix)]
        {
            let page = page_size() as u64;
            if page == 0 {
                return Ok((0, 0));
            }
            let end = offset.saturating_add(len);
            let start_pg = offset & !(page - 1);
            let end_pg = end.saturating_add(page - 1) & !(page - 1);
            let map = self.map.lock().unwrap();
            let map_len = map.len() as u64;
            if start_pg >= map_len {
                return Ok((0, 0));
            }
            let lock_end = end_pg.min(map_len);
            let lock_len = lock_end.saturating_sub(start_pg);
            if lock_len == 0 {
                return Ok((0, 0));
            }
            // SAFETY: range is within the live mmap; mlock faults pages in.
            let ptr = unsafe { map.as_ptr().add(start_pg as usize) } as *const libc::c_void;
            let rc = unsafe { libc::mlock(ptr, lock_len as usize) };
            if rc != 0 {
                let err = std::io::Error::last_os_error();
                rbitcoin_log::warn!(
                    "store: mlock failed path={} off={start_pg} len={lock_len}: {err} \
                     (raise RLIMIT_MEMLOCK / LimitMEMLOCK; soft budget may be exhausted)",
                    self.path.display()
                );
                return Err(StoreError::io(&self.path, err));
            }
            return Ok((start_pg, lock_len));
        }
        #[cfg(not(unix))]
        {
            let _ = (offset, len);
            Ok((0, 0))
        }
    }

    /// Best-effort `munlock` for a prior [`mlock_range`] page range.
    pub fn munlock_range(&self, page_start: u64, page_len: u64) {
        if page_len == 0 {
            return;
        }
        #[cfg(unix)]
        {
            let map = self.map.lock().unwrap();
            let map_len = map.len() as u64;
            if page_start >= map_len {
                return;
            }
            let unlock_len = page_len.min(map_len.saturating_sub(page_start)) as usize;
            if unlock_len == 0 {
                return;
            }
            // SAFETY: range was previously mlocked within this map (best-effort).
            let ptr = unsafe { map.as_ptr().add(page_start as usize) } as *const libc::c_void;
            let rc = unsafe { libc::munlock(ptr, unlock_len) };
            if rc != 0 {
                rbitcoin_log::trace!(
                    "store: munlock failed path={} off={page_start} len={unlock_len}: {}",
                    self.path.display(),
                    std::io::Error::last_os_error()
                );
            }
        }
        #[cfg(not(unix))]
        {
            let _ = (page_start, page_len);
        }
    }

    /// Best-effort: tell the kernel we do not need `[offset, offset+len)` in the
    /// page cache soon (archive far ahead of confirm/runway).
    ///
    /// Uses `posix_fadvise(POSIX_FADV_DONTNEED)` on the file (exact range) and
    /// `madvise(MADV_DONTNEED)` only on **whole pages strictly inside** the range
    /// so we do not drop a partial page that still holds an older live record.
    /// No-op on empty range / non-Linux / syscall failure (never fails the caller).
    ///
    /// **Does not** drop mlocked pages (kernel keeps them resident).
    pub fn advise_dont_need(&self, offset: u64, len: u64) {
        if len == 0 {
            return;
        }
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::io::AsRawFd;
            let end = offset.saturating_add(len);
            {
                let file = self.file.lock().unwrap();
                let fd = file.as_raw_fd();
                // Exact file range — kernel may write back dirty pages then drop.
                let rc = unsafe {
                    libc::posix_fadvise(
                        fd,
                        offset as libc::off_t,
                        len as libc::off_t,
                        libc::POSIX_FADV_DONTNEED,
                    )
                };
                if rc != 0 {
                    rbitcoin_log::trace!(
                        "store: posix_fadvise(DONTNEED) failed path={} off={offset} len={len}: {}",
                        self.path.display(),
                        std::io::Error::from_raw_os_error(rc)
                    );
                }
            }
            // Whole pages strictly inside [offset, end).
            let page = page_size() as u64;
            if page == 0 {
                return;
            }
            let start_pg = offset.saturating_add(page - 1) & !(page - 1);
            let end_pg = end & !(page - 1);
            if end_pg <= start_pg {
                return;
            }
            let map = self.map.lock().unwrap();
            let map_len = map.len() as u64;
            if start_pg >= map_len {
                return;
            }
            let adv_end = end_pg.min(map_len);
            let adv_len = (adv_end - start_pg) as usize;
            if adv_len == 0 {
                return;
            }
            // SAFETY: range is within the live mmap; MADV_DONTNEED is best-effort.
            let ptr = unsafe { map.as_ptr().add(start_pg as usize) } as *mut libc::c_void;
            let rc = unsafe { libc::madvise(ptr, adv_len, libc::MADV_DONTNEED) };
            if rc != 0 {
                rbitcoin_log::trace!(
                    "store: madvise(DONTNEED) failed path={} off={start_pg} len={adv_len}: {}",
                    self.path.display(),
                    std::io::Error::last_os_error()
                );
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (offset, len);
        }
    }
}

#[cfg(unix)]
fn page_size() -> usize {
    // SAFETY: sysconf is thread-safe for _SC_PAGESIZE.
    let n = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if n > 0 {
        n as usize
    } else {
        4096
    }
}

#[cfg(not(unix))]
fn page_size() -> usize {
    4096
}

#[cfg(test)]
mod advise_tests {
    use super::*;
    use rbitcoin_primitives::TableKind;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn advise_dont_need_is_best_effort() {
        static N: AtomicU64 = AtomicU64::new(0);
        let id = N.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("rbitcoin-advise-{id}"));
        let _ = std::fs::remove_file(&path);
        let f = TableFile::create(&path, TableKind::Tx).unwrap();
        let payload = vec![0xabu8; 16 * 1024];
        f.write_at(FILE_HEADER_LEN as u64, &payload).unwrap();
        // Must not panic or return error (void best-effort).
        f.advise_dont_need(FILE_HEADER_LEN as u64, payload.len() as u64);
        f.advise_dont_need(0, 0);
        let mut buf = vec![0u8; payload.len()];
        f.read_at(FILE_HEADER_LEN as u64, &mut buf).unwrap();
        assert_eq!(buf, payload);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn mlock_range_roundtrip_or_soft_fail() {
        static N: AtomicU64 = AtomicU64::new(0);
        let id = N.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("rbitcoin-mlock-{id}"));
        let _ = std::fs::remove_file(&path);
        let f = TableFile::create(&path, TableKind::Tx).unwrap();
        let payload = vec![0xcd_u8; 8 * 1024];
        f.write_at(FILE_HEADER_LEN as u64, &payload).unwrap();
        // Soft MEMLOCK may be 8 MiB (or less) in CI — success or clear error only.
        match f.mlock_range(FILE_HEADER_LEN as u64, payload.len() as u64) {
            Ok((start, len)) => {
                assert!(len > 0 || payload.is_empty());
                f.munlock_range(start, len);
            }
            Err(_) => {
                // RLIMIT_MEMLOCK exhausted / unsupported — still a valid outcome.
            }
        }
        let mut buf = vec![0u8; payload.len()];
        f.read_at(FILE_HEADER_LEN as u64, &mut buf).unwrap();
        assert_eq!(buf, payload);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn ensure_memlock_budget_is_callable() {
        let (soft, hard) = ensure_memlock_budget();
        // Soft should be ≤ hard (or both infinite).
        let _ = (soft, hard);
    }
}

/// Best-effort: set calling thread to Linux I/O priority idle (like `ionice -c3`).
///
/// No-op on non-Linux or if the syscall fails. Reduces desktop UI freezes when the
/// process runs as default niceness but still hammers the page cache.
pub fn try_set_io_idle() {
    #[cfg(target_os = "linux")]
    {
        // IOPRIO_WHO_PROCESS = 1, IOPRIO_CLASS_IDLE = 3, class shift 13.
        const IOPRIO_WHO_PROCESS: libc::c_int = 1;
        const IOPRIO_CLASS_IDLE: libc::c_int = 3;
        let prio = (IOPRIO_CLASS_IDLE << 13) as libc::c_int;
        // SAFETY: ioprio_set is a Linux syscall; args are integers.
        let rc = unsafe { libc::syscall(libc::SYS_ioprio_set, IOPRIO_WHO_PROCESS, 0, prio) };
        if rc == 0 {
            rbitcoin_log::debug!("store: set IOPRIO_CLASS_IDLE on thread");
        }
    }
}

/// Best-effort: best-effort I/O class, highest priority within class (shutdown spill).
pub fn try_set_io_best_effort() {
    #[cfg(target_os = "linux")]
    {
        const IOPRIO_WHO_PROCESS: libc::c_int = 1;
        const IOPRIO_CLASS_BE: libc::c_int = 2;
        // class BE, level 0 (highest within BE)
        let prio = (IOPRIO_CLASS_BE << 13) as libc::c_int;
        let rc = unsafe { libc::syscall(libc::SYS_ioprio_set, IOPRIO_WHO_PROCESS, 0, prio) };
        if rc == 0 {
            rbitcoin_log::debug!("store: set IOPRIO_CLASS_BE (high) on thread");
        }
    }
}

/// Soft floor for process open-file limit with 256-way sharded hash heads.
///
/// Four heads × 256 shards = 1024 FDs before bodies, wire, peers, etc.
/// Default Linux soft `nofile` is often 1024 — too low. We raise the **soft**
/// limit up to the **hard** limit (no root required for that).
pub const NOFILE_SOFT_TARGET: u64 = 16_384;

/// Raise `RLIMIT_MEMLOCK` soft limit to the hard limit (no root required for that).
///
/// Confirm runway `mlock`s Class A body pages so wave/connect do not soft-fault.
/// Default Linux hard memlock is often **8 MiB** — raise the **hard** limit via
/// NixOS `security.pam.loginLimits` / systemd `LimitMEMLOCK=` (e.g. 8 GiB) so this
/// can actually take effect. Returns `(soft, hard)` bytes after the attempt
/// (`u64::MAX` means unlimited / infinity).
pub fn ensure_memlock_budget() -> (u64, u64) {
    #[cfg(unix)]
    {
        let mut rlim = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: getrlimit with valid rlimit pointer.
        if unsafe { libc::getrlimit(libc::RLIMIT_MEMLOCK, &mut rlim) } != 0 {
            rbitcoin_log::warn!(
                "store: getrlimit(MEMLOCK) failed: {}",
                std::io::Error::last_os_error()
            );
            return (0, 0);
        }
        let hard = rlim.rlim_max as u64;
        let soft = rlim.rlim_cur as u64;
        let inf = libc::RLIM_INFINITY as u64;
        let hard_is_inf = rlim.rlim_max == libc::RLIM_INFINITY || hard == u64::MAX || hard == inf;
        let soft_is_inf = rlim.rlim_cur == libc::RLIM_INFINITY || soft == u64::MAX || soft == inf;
        if soft_is_inf || soft == hard {
            if !soft_is_inf && soft < 64 * 1024 * 1024 {
                rbitcoin_log::warn!(
                    "store: RLIMIT_MEMLOCK soft=hard={soft} bytes (~{} MiB); \
                     confirm mlock may fail — raise hard LimitMEMLOCK (e.g. 8G)",
                    soft / (1024 * 1024)
                );
            } else {
                rbitcoin_log::debug!(
                    "store: RLIMIT_MEMLOCK soft={soft} hard={hard} (already at hard)"
                );
            }
            return (soft, hard);
        }
        // Raise soft to hard (hard may be infinity).
        rlim.rlim_cur = rlim.rlim_max;
        // SAFETY: setrlimit soft ≤ hard.
        if unsafe { libc::setrlimit(libc::RLIMIT_MEMLOCK, &rlim) } != 0 {
            rbitcoin_log::warn!(
                "store: setrlimit(MEMLOCK) soft {soft}→{hard} failed: {}",
                std::io::Error::last_os_error()
            );
            return (soft, hard);
        }
        let new_soft = rlim.rlim_cur as u64;
        rbitcoin_log::info!(
            "store: raised RLIMIT_MEMLOCK soft {soft}→{new_soft} (hard={hard})"
        );
        if !hard_is_inf && hard < 64 * 1024 * 1024 {
            rbitcoin_log::warn!(
                "store: RLIMIT_MEMLOCK hard={hard} bytes (~{} MiB) is low for body mlock; \
                 set hard LimitMEMLOCK=8G (NixOS loginLimits / systemd)",
                hard / (1024 * 1024)
            );
        }
        return (new_soft, hard);
    }
    #[cfg(not(unix))]
    {
        (0, 0)
    }
}

/// Raise `RLIMIT_NOFILE` soft limit toward [`NOFILE_SOFT_TARGET`] (capped by hard).
///
/// Returns `(soft, hard)` after the attempt. If soft is still below what a
/// mainnet sharded store needs (~2k+), the operator must raise the hard limit
/// (`ulimit -n`, systemd `LimitNOFILE=`, container `--ulimit nofile=`).
pub fn ensure_nofile_budget() -> (u64, u64) {
    ensure_nofile_budget_at_least(NOFILE_SOFT_TARGET)
}

/// Like [`ensure_nofile_budget`] with an explicit soft target.
pub fn ensure_nofile_budget_at_least(want_soft: u64) -> (u64, u64) {
    #[cfg(unix)]
    {
        let mut rlim = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: getrlimit with valid rlimit pointer.
        if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) } != 0 {
            rbitcoin_log::warn!(
                "store: getrlimit(NOFILE) failed: {}",
                std::io::Error::last_os_error()
            );
            return (0, 0);
        }
        let hard = rlim.rlim_max as u64;
        let soft = rlim.rlim_cur as u64;
        // RLIM_INFINITY is !0 on Linux — treat as "plenty".
        let hard_cap = if hard == u64::MAX || rlim.rlim_max == libc::RLIM_INFINITY {
            want_soft.max(soft)
        } else {
            hard
        };
        let target = want_soft.min(hard_cap).max(soft);
        if target > soft {
            rlim.rlim_cur = target as libc::rlim_t;
            // SAFETY: setrlimit with filled rlimit; soft ≤ hard.
            if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &rlim) } != 0 {
                rbitcoin_log::warn!(
                    "store: setrlimit(NOFILE) soft {soft}→{target} failed (hard={hard}): {}",
                    std::io::Error::last_os_error()
                );
                return (soft, hard);
            }
            rbitcoin_log::debug!(
                "store: raised RLIMIT_NOFILE soft {soft}→{target} (hard={hard})"
            );
            return (target, hard);
        }
        if soft < want_soft {
            rbitcoin_log::warn!(
                "store: RLIMIT_NOFILE soft={soft} hard={hard} below target {want_soft}; \
                 sharded heads need ~1k+ FDs — raise hard limit (ulimit -n / LimitNOFILE) \
                 if open fails with EMFILE"
            );
        }
        return (soft, hard);
    }
    #[cfg(not(unix))]
    {
        let _ = want_soft;
        (0, 0)
    }
}

/// Extend file to at least `len` without zero-fill when the FS supports it.
fn try_fallocate(file: &File, len: u64) -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        // FALLOC_FL_KEEP_SIZE = 0x01 — allocate without changing size if already large enough.
        // We want size extended: pass mode 0.
        let rc = unsafe { libc::fallocate(file.as_raw_fd(), 0, 0, len as i64) };
        if rc == 0 {
            return Ok(());
        }
        Err(std::io::Error::last_os_error())
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (file, len);
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "fallocate unavailable",
        ))
    }
}

fn try_punch_hole(file: &File, offset: u64, len: u64) -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        // FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE
        const PUNCH: i32 = 0x02 | 0x01;
        let rc = unsafe {
            libc::fallocate(file.as_raw_fd(), PUNCH, offset as i64, len as i64)
        };
        if rc == 0 {
            return Ok(());
        }
        Err(std::io::Error::last_os_error())
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (file, offset, len);
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "punch hole unavailable",
        ))
    }
}
