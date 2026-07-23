//! Growable mmap-backed table files with a common header.
//!
//! # Concurrency (lock-free hot path)
//!
//! - **Published logical length** is an `AtomicU64` (Acquire/Release).
//! - **Map capacity** uses epochs: grow fallocates + maps a new window on the
//!   same file, then swaps an `Arc` pointer. Readers pin the epoch they load;
//!   old epochs live until the last pin drops (same idea as `tx.head` shadow
//!   swap — no reader pause for capacity).
//! - Steady-state **read / write / mlock** do **not** take a map mutex.
//! - `File` is only locked for grow (`fallocate`/`set_len`), fsync, and fadvise.
//!
//! Roles (see `AGENTS.md` / `docs/concurrency.md`): at most one appender and one
//! annotator; N concurrent readers of published ranges.

use crate::error::StoreError;
use memmap2::MmapMut;
use rbitcoin_primitives::{TableKind, SCHEMA_VERSION, STORE_MAGIC};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

pub const FILE_HEADER_LEN: usize = 16;

/// One mmap window over the table file. Shared via [`Arc`]; bytes are accessed
/// with role-disciplined pointer IO (not `DerefMut` under shared refs).
struct MapEpoch {
    map: MmapMut,
}

// SAFETY: concurrent access is only via `as_ptr()` memcpy of non-overlapping
// regions under the store role protocol (one appender, one annotator, N readers
// of published data). Capacity growth publishes a new epoch instead of mutating
// this map's extent in place.
unsafe impl Send for MapEpoch {}
unsafe impl Sync for MapEpoch {}

impl MapEpoch {
    fn cap(&self) -> u64 {
        self.map.len() as u64
    }

    fn as_ptr(&self) -> *const u8 {
        self.map.as_ptr()
    }
}

/// Pin of the current (or a still-live prior) map epoch.
struct EpochPin {
    epoch: Arc<MapEpoch>,
}

pub struct TableFile {
    path: PathBuf,
    /// Grow / fsync / fadvise only — not on the read/write memcpy path.
    file: Mutex<File>,
    /// Always non-null after construction. Loaded with Arc refcount bump.
    epoch: AtomicPtr<MapEpoch>,
    /// Logical length including header (published HWM).
    published_len: AtomicU64,
}

impl Drop for TableFile {
    fn drop(&mut self) {
        let p = self.epoch.load(Ordering::Acquire);
        if !p.is_null() {
            // SAFETY: we own the last "stored" strong ref installed at create/open/grow.
            unsafe {
                drop(Arc::from_raw(p));
            }
        }
    }
}

impl TableFile {
    fn install_epoch(epoch: Arc<MapEpoch>) -> AtomicPtr<MapEpoch> {
        let ptr = Arc::into_raw(epoch) as *mut MapEpoch;
        AtomicPtr::new(ptr)
    }

    /// Load a pin of the current epoch (lock-free).
    fn pin(&self) -> EpochPin {
        loop {
            let p = self.epoch.load(Ordering::Acquire);
            if p.is_null() {
                std::hint::spin_loop();
                continue;
            }
            // SAFETY: p is a live Arc allocation; we bump then re-check.
            unsafe {
                Arc::increment_strong_count(p);
                if self.epoch.load(Ordering::Acquire) != p {
                    Arc::decrement_strong_count(p);
                    continue;
                }
                return EpochPin {
                    epoch: Arc::from_raw(p),
                };
            }
        }
    }

    /// Publish a new map epoch; old epoch freed when last pin drops.
    fn publish_epoch(&self, new: Arc<MapEpoch>) {
        let new_ptr = Arc::into_raw(new) as *mut MapEpoch;
        let old = self.epoch.swap(new_ptr, Ordering::AcqRel);
        if !old.is_null() {
            // SAFETY: drops the stored strong ref on the previous epoch.
            unsafe {
                drop(Arc::from_raw(old));
            }
        }
    }

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

        let initial = FILE_HEADER_LEN as u64 + 64;
        file.set_len(initial)
            .map_err(|e| StoreError::io(&path, e))?;
        // SAFETY: exclusive file we just created; length set above.
        let map = unsafe { MmapMut::map_mut(&file) }.map_err(|e| StoreError::io(&path, e))?;
        let epoch = Arc::new(MapEpoch { map });

        let _ = kind;
        Ok(Self {
            path,
            file: Mutex::new(file),
            epoch: Self::install_epoch(epoch),
            published_len: AtomicU64::new(FILE_HEADER_LEN as u64),
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
        let mut logical = u64::from_le_bytes(header[8..16].try_into().unwrap());
        if logical < FILE_HEADER_LEN as u64 {
            logical = FILE_HEADER_LEN as u64;
        }
        if logical > file_len {
            logical = file_len;
        }

        let map = unsafe { MmapMut::map_mut(&file) }.map_err(|e| StoreError::io(&path, e))?;
        let epoch = Arc::new(MapEpoch { map });
        Ok(Self {
            path,
            file: Mutex::new(file),
            epoch: Self::install_epoch(epoch),
            published_len: AtomicU64::new(logical),
        })
    }

    pub fn logical_len(&self) -> u64 {
        self.published_len.load(Ordering::Acquire)
    }

    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    /// Shrink or set logical length (must be ≥ header size). Does not zero freed bytes.
    pub fn set_logical_len(&self, logical: u64) -> Result<(), StoreError> {
        if logical < FILE_HEADER_LEN as u64 {
            return Err(StoreError::Corrupt("logical length below header"));
        }
        self.ensure_capacity(logical)?;
        self.published_len.store(logical, Ordering::Release);
        self.write_hwm_mmap(logical);
        Ok(())
    }

    pub fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<(), StoreError> {
        let end = offset.saturating_add(buf.len() as u64);
        let len = self.published_len.load(Ordering::Acquire);
        if end > len {
            return Err(StoreError::Corrupt("read past logical end"));
        }
        let pin = self.pin();
        if end > pin.epoch.cap() {
            return Err(StoreError::Corrupt("read past map end"));
        }
        // SAFETY: range within published_len and epoch capacity; exclusive of
        // concurrent append past HWM by publish order.
        unsafe {
            let src = pin.epoch.as_ptr().add(offset as usize);
            ptr::copy_nonoverlapping(src, buf.as_mut_ptr(), buf.len());
        }
        Ok(())
    }

    pub fn write_at(&self, offset: u64, bytes: &[u8]) -> Result<(), StoreError> {
        let end = offset.saturating_add(bytes.len() as u64);
        self.ensure_capacity(end)?;
        let pin = self.pin();
        if end > pin.epoch.cap() {
            // Race with concurrent grow: re-pin once.
            let pin = self.pin();
            if end > pin.epoch.cap() {
                return Err(StoreError::Corrupt("write past map end after grow"));
            }
            unsafe {
                let dst = pin.epoch.as_ptr().add(offset as usize) as *mut u8;
                ptr::copy_nonoverlapping(bytes.as_ptr(), dst, bytes.len());
            }
        } else {
            unsafe {
                let dst = pin.epoch.as_ptr().add(offset as usize) as *mut u8;
                ptr::copy_nonoverlapping(bytes.as_ptr(), dst, bytes.len());
            }
        }
        // Publish HWM after bytes are visible.
        let mut cur = self.published_len.load(Ordering::Relaxed);
        while end > cur {
            match self.published_len.compare_exchange_weak(
                cur,
                end,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.write_hwm_mmap(end);
                    break;
                }
                Err(c) => cur = c,
            }
        }
        Ok(())
    }

    /// Ensure the mmap covers at least `need` bytes.
    ///
    /// Capacity growth: fallocate (or set_len) then map a **new** epoch over the
    /// same file and publish the pointer. Readers holding the old pin keep a valid
    /// mapping of the shared file — no catch-up, no long map lock.
    pub fn ensure_capacity(&self, need: u64) -> Result<(), StoreError> {
        {
            let pin = self.pin();
            if need <= pin.epoch.cap() {
                return Ok(());
            }
        }

        const DOUBLE_UNTIL: u64 = 64 * 1024 * 1024;
        let cur = self.pin().epoch.cap();
        if need <= cur {
            return Ok(());
        }
        let (headroom, step) = if cur >= 8 * 1024 * 1024 * 1024 {
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

        // Single grower (appender role): exclusive file metadata ops only.
        let file = self.file.lock().unwrap();
        // Re-check under file lock (another grow may have finished).
        if need <= self.pin().epoch.cap() {
            return Ok(());
        }
        if try_fallocate(&file, new_cap).is_err() {
            file.set_len(new_cap)
                .map_err(|e| StoreError::io(&self.path, e))?;
        } else if file.metadata().map(|m| m.len()).unwrap_or(0) < new_cap {
            file.set_len(new_cap)
                .map_err(|e| StoreError::io(&self.path, e))?;
        }
        // SAFETY: file length ≥ new_cap; new map is a larger window on same file.
        let new_map =
            unsafe { MmapMut::map_mut(&*file) }.map_err(|e| StoreError::io(&self.path, e))?;
        drop(file);
        self.publish_epoch(Arc::new(MapEpoch { map: new_map }));
        Ok(())
    }

    /// Punch a hole over `[offset, offset+len)`.
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
        let zero = vec![0u8; 1024 * 1024];
        let mut written = 0u64;
        while written < len {
            let chunk = ((len - written) as usize).min(zero.len());
            self.write_at(offset + written, &zero[..chunk])?;
            written += chunk as u64;
        }
        Ok(())
    }

    fn write_hwm_mmap(&self, logical: u64) {
        let pin = self.pin();
        if pin.epoch.cap() < 16 {
            return;
        }
        let bytes = logical.to_le_bytes();
        unsafe {
            let dst = pin.epoch.as_ptr().add(8) as *mut u8;
            ptr::copy_nonoverlapping(bytes.as_ptr(), dst, 8);
        }
    }

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
    pub fn flush(&self) -> Result<(), StoreError> {
        let logical = self.published_len.load(Ordering::Acquire);
        self.persist_logical_len(logical)?;
        if crate::ibd_io_policy::defer_durable_flush() {
            return Ok(());
        }
        let pin = self.pin();
        // SAFETY: MmapMut::flush needs &self via interior — use raw through owned map.
        // We only have shared Arc; memmap2 flush takes &self on MmapMut.
        pin.epoch
            .map
            .flush()
            .map_err(|e| StoreError::io(&self.path, e))?;
        self.file
            .lock()
            .unwrap()
            .sync_data()
            .map_err(|e| StoreError::io(&self.path, e))?;
        Ok(())
    }

    pub fn flush_async(&self) -> Result<(), StoreError> {
        let logical = self.published_len.load(Ordering::Acquire);
        self.persist_logical_len(logical)?;
        if crate::ibd_io_policy::defer_durable_flush() {
            return Ok(());
        }
        let pin = self.pin();
        pin.epoch
            .map
            .flush_async()
            .map_err(|e| StoreError::io(&self.path, e))?;
        Ok(())
    }

    /// Walk a byte range without copying.
    pub fn with_bytes<R>(
        &self,
        offset: u64,
        len: u64,
        f: impl FnOnce(&[u8]) -> R,
    ) -> Result<R, StoreError> {
        let end = offset.saturating_add(len);
        let logical = self.published_len.load(Ordering::Acquire);
        if end > logical {
            return Err(StoreError::Corrupt("with_bytes past logical end"));
        }
        let pin = self.pin();
        if end > pin.epoch.cap() {
            return Err(StoreError::Corrupt("with_bytes past map end"));
        }
        // SAFETY: range within published + capacity; pin keeps map alive.
        let slice = unsafe {
            std::slice::from_raw_parts(pin.epoch.as_ptr().add(offset as usize), len as usize)
        };
        Ok(f(slice))
    }

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
            let pin = self.pin();
            let map_len = pin.epoch.cap();
            if start_pg >= map_len {
                return Ok((0, 0));
            }
            let lock_end = end_pg.min(map_len);
            let lock_len = lock_end.saturating_sub(start_pg);
            if lock_len == 0 {
                return Ok((0, 0));
            }
            let ptr = unsafe { pin.epoch.as_ptr().add(start_pg as usize) } as *const libc::c_void;
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

    pub fn munlock_range(&self, page_start: u64, page_len: u64) {
        if page_len == 0 {
            return;
        }
        #[cfg(unix)]
        {
            let pin = self.pin();
            let map_len = pin.epoch.cap();
            if page_start >= map_len {
                return;
            }
            let unlock_len = page_len.min(map_len.saturating_sub(page_start)) as usize;
            if unlock_len == 0 {
                return;
            }
            let ptr =
                unsafe { pin.epoch.as_ptr().add(page_start as usize) } as *const libc::c_void;
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
            let page = page_size() as u64;
            if page == 0 {
                return;
            }
            let start_pg = offset.saturating_add(page - 1) & !(page - 1);
            let end_pg = end & !(page - 1);
            if end_pg <= start_pg {
                return;
            }
            let pin = self.pin();
            let map_len = pin.epoch.cap();
            if start_pg >= map_len {
                return;
            }
            let adv_end = end_pg.min(map_len);
            let adv_len = (adv_end - start_pg) as usize;
            if adv_len == 0 {
                return;
            }
            let ptr =
                unsafe { pin.epoch.as_ptr().add(start_pg as usize) } as *mut libc::c_void;
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
        match f.mlock_range(FILE_HEADER_LEN as u64, payload.len() as u64) {
            Ok((start, len)) => {
                assert!(len > 0 || payload.is_empty());
                f.munlock_range(start, len);
            }
            Err(_) => {}
        }
        let mut buf = vec![0u8; payload.len()];
        f.read_at(FILE_HEADER_LEN as u64, &mut buf).unwrap();
        assert_eq!(buf, payload);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn ensure_memlock_budget_is_callable() {
        let (soft, hard) = ensure_memlock_budget();
        let _ = (soft, hard);
    }

    #[test]
    fn concurrent_readers_during_append_and_grow() {
        use std::sync::Barrier;
        use std::thread;

        static N: AtomicU64 = AtomicU64::new(0);
        let id = N.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("rbitcoin-epoch-stress-{id}"));
        let _ = std::fs::remove_file(&path);
        let f = Arc::new(TableFile::create(&path, TableKind::Tx).unwrap());

        // Seed a first published record so early readers always have a range.
        let seed = vec![0x11u8; 64];
        f.write_at(FILE_HEADER_LEN as u64, &seed).unwrap();
        assert_eq!(
            f.logical_len(),
            FILE_HEADER_LEN as u64 + seed.len() as u64
        );

        let barrier = Arc::new(Barrier::new(5));
        let mut handles = Vec::new();

        // Appender: many chunks, force capacity grows.
        {
            let f = Arc::clone(&f);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                let mut off = f.logical_len();
                for i in 0..200u32 {
                    let chunk = vec![(i % 251) as u8; 4096];
                    f.write_at(off, &chunk).unwrap();
                    off += chunk.len() as u64;
                }
            }));
        }

        // Annotator-style: rewrite seed bytes (published range only).
        {
            let f = Arc::clone(&f);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                for i in 0..500u32 {
                    let b = [((i % 200) + 1) as u8; 8];
                    f.write_at(FILE_HEADER_LEN as u64, &b).unwrap();
                }
            }));
        }

        // Readers: always read published prefix; never torn length vs body.
        for _ in 0..3 {
            let f = Arc::clone(&f);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                for _ in 0..1000 {
                    let len = f.logical_len();
                    if len <= FILE_HEADER_LEN as u64 {
                        continue;
                    }
                    let n = (len - FILE_HEADER_LEN as u64).min(64) as usize;
                    let mut buf = vec![0u8; n];
                    f.read_at(FILE_HEADER_LEN as u64, &mut buf).unwrap();
                    // After any annotate, first bytes are non-zero once writer ran;
                    // just ensure read succeeded for published len.
                    let _ = buf[0];
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
        let final_len = f.logical_len();
        assert!(final_len > FILE_HEADER_LEN as u64 + 64);
        let mut head = [0u8; 8];
        f.read_at(FILE_HEADER_LEN as u64, &mut head).unwrap();
        // Annotator left non-zero pattern.
        assert_ne!(head, [0u8; 8]);
        let _ = std::fs::remove_file(&path);
    }
}

/// Best-effort: set calling thread to Linux I/O priority idle (like `ionice -c3`).
pub fn try_set_io_idle() {
    #[cfg(target_os = "linux")]
    {
        const IOPRIO_WHO_PROCESS: libc::c_int = 1;
        const IOPRIO_CLASS_IDLE: libc::c_int = 3;
        let prio = (IOPRIO_CLASS_IDLE << 13) as libc::c_int;
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
        let prio = (IOPRIO_CLASS_BE << 13) as libc::c_int;
        let rc = unsafe { libc::syscall(libc::SYS_ioprio_set, IOPRIO_WHO_PROCESS, 0, prio) };
        if rc == 0 {
            rbitcoin_log::debug!("store: set IOPRIO_CLASS_BE (high) on thread");
        }
    }
}

pub const NOFILE_SOFT_TARGET: u64 = 16_384;

pub fn ensure_memlock_budget() -> (u64, u64) {
    #[cfg(unix)]
    {
        let mut rlim = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
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
        rlim.rlim_cur = rlim.rlim_max;
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

pub fn ensure_nofile_budget() -> (u64, u64) {
    ensure_nofile_budget_at_least(NOFILE_SOFT_TARGET)
}

pub fn ensure_nofile_budget_at_least(want_soft: u64) -> (u64, u64) {
    #[cfg(unix)]
    {
        let mut rlim = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) } != 0 {
            rbitcoin_log::warn!(
                "store: getrlimit(NOFILE) failed: {}",
                std::io::Error::last_os_error()
            );
            return (0, 0);
        }
        let hard = rlim.rlim_max as u64;
        let soft = rlim.rlim_cur as u64;
        let hard_cap = if hard == u64::MAX || rlim.rlim_max == libc::RLIM_INFINITY {
            want_soft.max(soft)
        } else {
            hard
        };
        let target = want_soft.min(hard_cap).max(soft);
        if target > soft {
            rlim.rlim_cur = target as libc::rlim_t;
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

fn try_fallocate(file: &File, len: u64) -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
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
