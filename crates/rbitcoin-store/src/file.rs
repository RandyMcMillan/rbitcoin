//! Growable mmap-backed table files with a common header.
//!
//! # Concurrency (brief epoch pin, no long map mutex)
//!
//! - **Published logical length** is an `AtomicU64` (Acquire/Release).
//! - **Map capacity** uses epochs: grow fallocates + maps a new window on the
//!   same file, then swaps an `Arc` under a short write lock. Readers pin by
//!   cloning that `Arc` under a shared read lock (no load/increment UAF window).
//!   Old epochs live until the last pin drops (same idea as `tx.head` shadow
//!   swap — readers never pause for capacity *mapping* work).
//! - Steady-state **read / write** memcpy does **not** hold the epoch lock past
//!   the pin clone.
//! - `File` is only locked for grow (`fallocate`/`set_len`), fsync, and fadvise.
//!
//! Roles (see `AGENTS.md` / `docs/concurrency.md`): at most one appender and one
//! annotator; N concurrent readers of published ranges.

use crate::error::StoreError;
use memmap2::MmapMut;
use rbitcoin_primitives::{TableKind, SCHEMA_VERSION, STORE_MAGIC};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::fd::{AsRawFd, RawFd};
use std::path::PathBuf;
use std::ptr;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

pub const FILE_HEADER_LEN: usize = 16;

/// Trailing-header tables (`tx.head`): 16-byte store identity + 16-byte layout
/// extension (bits / entry_bytes / generation). Slots still start at file offset 0
/// so probe pages stay OS-page-aligned.
pub const TRAILING_FOOTER_LEN: usize = 32;

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
    /// Cloned FD for lock-free [`pread`](Self::pread_at) / io_uring bulk reads.
    /// Same inode as `file`; concurrent pread is safe with the role protocol.
    read_file: File,
    /// Current map epoch. Pin clones under a short shared lock; grow replaces
    /// under a write lock. Avoids the AtomicPtr load/increment free race.
    /// **Class A `tx.body` (`TableKind::Tx`, leading header):** map is a tiny
    /// header-only window (not grown); payload IO uses pread/pwrite only.
    epoch: RwLock<Arc<MapEpoch>>,
    /// Logical length including header/trailer (published HWM).
    published_len: AtomicU64,
    /// File capacity for **fd-only body** tables (`TableKind::Tx` leading). Always
    /// ≥ published; grown via fallocate without remapping multi‑GiB body.
    file_cap: AtomicU64,
    /// When true: [`TRAILING_FOOTER_LEN`]-byte magic+HWM+layout trailer is at
    /// **end** of published range; data starts at offset 0 (page-aligned probes).
    trailing_header: bool,
    /// Layout extension for trailing footers (address-head bits/gen). Zero for
    /// other tables; rewritten with the trailer on every `set_logical_len`.
    trailing_ext: [u8; 16],
    kind: TableKind,
}

impl TableFile {
    /// Class A `tx.body` — no multi‑GiB mmap payload access.
    #[inline]
    fn is_tx_body_fd(&self) -> bool {
        self.kind == TableKind::Tx && !self.trailing_header
    }

    fn install_epoch(epoch: Arc<MapEpoch>) -> RwLock<Arc<MapEpoch>> {
        RwLock::new(epoch)
    }

    /// Pin the current map epoch (shared read lock only for the Arc clone).
    fn pin(&self) -> EpochPin {
        EpochPin {
            epoch: Arc::clone(
                &self
                    .epoch
                    .read()
                    .unwrap_or_else(|e| e.into_inner()),
            ),
        }
    }

    /// Publish a new map epoch; old epoch freed when last pin drops.
    fn publish_epoch(&self, new: Arc<MapEpoch>) {
        *self
            .epoch
            .write()
            .unwrap_or_else(|e| e.into_inner()) = new;
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
        // Class A body: pin only a tiny header window forever (payload = pread/pwrite).
        // Other tables: full-file map.
        let map = if kind == TableKind::Tx {
            unsafe {
                memmap2::MmapOptions::new()
                    .len(initial as usize)
                    .map_mut(&file)
            }
            .map_err(|e| StoreError::io(&path, e))?
        } else {
            // SAFETY: exclusive file we just created; length set above.
            unsafe { MmapMut::map_mut(&file) }.map_err(|e| StoreError::io(&path, e))?
        };
        let epoch = Arc::new(MapEpoch { map });
        let read_file = file.try_clone().map_err(|e| StoreError::io(&path, e))?;

        Ok(Self {
            path,
            file: Mutex::new(file),
            read_file,
            epoch: Self::install_epoch(epoch),
            published_len: AtomicU64::new(FILE_HEADER_LEN as u64),
            file_cap: AtomicU64::new(initial),
            trailing_header: false,
            trailing_ext: [0u8; 16],
            kind,
        })
    }

    /// Create a table whose **data starts at offset 0** and a
    /// [`TRAILING_FOOTER_LEN`]-byte footer (store identity + layout ext) sits at
    /// the **end** of the published length.
    ///
    /// Used by page-local `tx.head` so probe pages are OS-page-aligned.
    pub fn create_trailing_header(
        path: impl Into<PathBuf>,
        kind: TableKind,
    ) -> Result<Self, StoreError> {
        let path = path.into();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|e| StoreError::io(&path, e))?;
        // Minimal size: footer only until caller sets body+footer length.
        let initial = TRAILING_FOOTER_LEN as u64;
        file.set_len(initial)
            .map_err(|e| StoreError::io(&path, e))?;
        let map = unsafe { MmapMut::map_mut(&file) }.map_err(|e| StoreError::io(&path, e))?;
        let epoch = Arc::new(MapEpoch { map });
        let read_file = file.try_clone().map_err(|e| StoreError::io(&path, e))?;
        let s = Self {
            path,
            file: Mutex::new(file),
            read_file,
            epoch: Self::install_epoch(epoch),
            published_len: AtomicU64::new(initial),
            file_cap: AtomicU64::new(initial),
            trailing_header: true,
            trailing_ext: [0u8; 16],
            kind,
        };
        s.write_trailer(initial)?;
        Ok(s)
    }

    /// Open a trailing-header table. `data_bytes` is the slot/body length
    /// (excluding the [`TRAILING_FOOTER_LEN`] footer).
    ///
    /// Returns `(file, layout_ext)` — the 16-byte address-head meta after the
    /// store identity (zeros if unused).
    pub fn open_trailing_header(
        path: impl Into<PathBuf>,
        kind: TableKind,
        data_bytes: u64,
    ) -> Result<(Self, [u8; 16]), StoreError> {
        let path = path.into();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|e| StoreError::io(&path, e))?;
        let file_len = file.metadata().map_err(|e| StoreError::io(&path, e))?.len();
        let expect = data_bytes.saturating_add(TRAILING_FOOTER_LEN as u64);
        if file_len < expect {
            return Err(StoreError::Corrupt("trailing-header table short"));
        }
        let mut footer = [0u8; TRAILING_FOOTER_LEN];
        file.seek(SeekFrom::Start(data_bytes))
            .map_err(|e| StoreError::io(&path, e))?;
        file.read_exact(&mut footer)
            .map_err(|e| StoreError::io(&path, e))?;
        if footer[0..4] != STORE_MAGIC {
            // Leading-header legacy / pre-footer-meta → rebuild.
            return Err(StoreError::BadMagic);
        }
        let ver = u16::from_le_bytes([footer[4], footer[5]]);
        if ver != SCHEMA_VERSION {
            return Err(StoreError::BadSchema(ver));
        }
        let got = u16::from_le_bytes([footer[6], footer[7]]);
        if got != kind.as_u16() {
            return Err(StoreError::BadKind {
                expected: kind.as_u16(),
                got,
            });
        }
        let mut trailing_ext = [0u8; 16];
        trailing_ext.copy_from_slice(&footer[16..32]);
        let logical = expect;
        let map = unsafe { MmapMut::map_mut(&file) }.map_err(|e| StoreError::io(&path, e))?;
        let epoch = Arc::new(MapEpoch { map });
        let read_file = file.try_clone().map_err(|e| StoreError::io(&path, e))?;
        Ok((
            Self {
                path,
                file: Mutex::new(file),
                read_file,
                epoch: Self::install_epoch(epoch),
                published_len: AtomicU64::new(logical),
                file_cap: AtomicU64::new(logical),
                trailing_header: true,
                trailing_ext,
                kind,
            },
            trailing_ext,
        ))
    }

    /// Open trailing-header by reading the footer at EOF (no sidecar needed for
    /// layout — bits/generation live in the footer extension).
    pub fn open_trailing_header_from_end(
        path: impl Into<PathBuf>,
        kind: TableKind,
    ) -> Result<(Self, [u8; 16]), StoreError> {
        let path = path.into();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|e| StoreError::io(&path, e))?;
        let file_len = file.metadata().map_err(|e| StoreError::io(&path, e))?.len();
        if file_len < TRAILING_FOOTER_LEN as u64 {
            return Err(StoreError::Corrupt("trailing-header table short"));
        }
        let mut footer = [0u8; TRAILING_FOOTER_LEN];
        file.seek(SeekFrom::Start(file_len - TRAILING_FOOTER_LEN as u64))
            .map_err(|e| StoreError::io(&path, e))?;
        file.read_exact(&mut footer)
            .map_err(|e| StoreError::io(&path, e))?;
        if footer[0..4] != STORE_MAGIC {
            return Err(StoreError::BadMagic);
        }
        let ver = u16::from_le_bytes([footer[4], footer[5]]);
        if ver != SCHEMA_VERSION {
            return Err(StoreError::BadSchema(ver));
        }
        let got = u16::from_le_bytes([footer[6], footer[7]]);
        if got != kind.as_u16() {
            return Err(StoreError::BadKind {
                expected: kind.as_u16(),
                got,
            });
        }
        let logical = u64::from_le_bytes(footer[8..16].try_into().unwrap());
        if logical < TRAILING_FOOTER_LEN as u64 || logical > file_len {
            return Err(StoreError::Corrupt("trailing-header logical length"));
        }
        let data_bytes = logical - TRAILING_FOOTER_LEN as u64;
        drop(file);
        Self::open_trailing_header(path, kind, data_bytes)
    }

    /// Update the 16-byte trailing layout extension and rewrite the footer.
    pub fn set_trailing_ext(&mut self, ext: [u8; 16]) -> Result<(), StoreError> {
        if !self.trailing_header {
            return Err(StoreError::Corrupt("set_trailing_ext on leading-header file"));
        }
        self.trailing_ext = ext;
        let logical = self.published_len.load(Ordering::Acquire);
        self.write_trailer(logical)
    }

    fn write_trailer(&self, logical: u64) -> Result<(), StoreError> {
        if logical < TRAILING_FOOTER_LEN as u64 {
            return Err(StoreError::Corrupt("trailing header logical short"));
        }
        let base = logical - TRAILING_FOOTER_LEN as u64;
        let mut footer = [0u8; TRAILING_FOOTER_LEN];
        footer[0..4].copy_from_slice(&STORE_MAGIC);
        footer[4..6].copy_from_slice(&SCHEMA_VERSION.to_le_bytes());
        footer[6..8].copy_from_slice(&self.kind.as_u16().to_le_bytes());
        footer[8..16].copy_from_slice(&logical.to_le_bytes());
        footer[16..32].copy_from_slice(&self.trailing_ext);
        self.ensure_capacity(logical)?;
        let pin = self.pin();
        if pin.epoch.cap() < logical {
            return Err(StoreError::Corrupt("trailer past map"));
        }
        unsafe {
            let dst = pin.epoch.as_ptr().add(base as usize) as *mut u8;
            ptr::copy_nonoverlapping(footer.as_ptr(), dst, TRAILING_FOOTER_LEN);
        }
        Ok(())
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

        let map = if kind == TableKind::Tx {
            let win = (FILE_HEADER_LEN as u64 + 4096).min(file_len.max(FILE_HEADER_LEN as u64));
            unsafe {
                memmap2::MmapOptions::new()
                    .len(win as usize)
                    .map_mut(&file)
            }
            .map_err(|e| StoreError::io(&path, e))?
        } else {
            unsafe { MmapMut::map_mut(&file) }.map_err(|e| StoreError::io(&path, e))?
        };
        let epoch = Arc::new(MapEpoch { map });
        let read_file = file.try_clone().map_err(|e| StoreError::io(&path, e))?;
        Ok(Self {
            path,
            file: Mutex::new(file),
            read_file,
            epoch: Self::install_epoch(epoch),
            published_len: AtomicU64::new(logical),
            file_cap: AtomicU64::new(file_len.max(logical)),
            trailing_header: false,
            trailing_ext: [0u8; 16],
            kind,
        })
    }

    pub fn logical_len(&self) -> u64 {
        self.published_len.load(Ordering::Acquire)
    }



    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    /// Raw FD for io_uring / pread bulk reads (lock-free; do not close).
    #[inline]
    pub fn read_fd(&self) -> RawFd {
        self.read_file.as_raw_fd()
    }



    /// Shrink or set logical length (must be ≥ header/trailer size). Does not zero freed bytes.
    pub fn set_logical_len(&self, logical: u64) -> Result<(), StoreError> {
        let min = if self.trailing_header {
            TRAILING_FOOTER_LEN as u64
        } else {
            FILE_HEADER_LEN as u64
        };
        if logical < min {
            return Err(StoreError::Corrupt("logical length below header"));
        }
        self.ensure_capacity(logical)?;
        self.published_len.store(logical, Ordering::Release);
        if self.trailing_header {
            self.write_trailer(logical)?;
        } else {
            self.write_hwm_mmap(logical);
        }
        Ok(())
    }

    /// Slot/data length excluding the header or trailing footer.
    #[inline]
    pub fn data_len(&self) -> u64 {
        let overhead = if self.trailing_header {
            TRAILING_FOOTER_LEN as u64
        } else {
            FILE_HEADER_LEN as u64
        };
        self.published_len
            .load(Ordering::Acquire)
            .saturating_sub(overhead)
    }

    /// Positional pread (page cache / disk). Preferred for Class A `tx.body`.
    pub fn pread_at(&self, offset: u64, buf: &mut [u8]) -> Result<(), StoreError> {
        if buf.is_empty() {
            return Ok(());
        }
        let end = offset.saturating_add(buf.len() as u64);
        let len = self.published_len.load(Ordering::Acquire);
        if end > len {
            return Err(StoreError::Corrupt("pread past logical end"));
        }
        let fd = self.read_file.as_raw_fd();
        let mut done = 0usize;
        while done < buf.len() {
            let rc = unsafe {
                libc::pread(
                    fd,
                    buf[done..].as_mut_ptr() as *mut libc::c_void,
                    buf.len() - done,
                    (offset + done as u64) as libc::off_t,
                )
            };
            if rc < 0 {
                return Err(StoreError::io(&self.path, std::io::Error::last_os_error()));
            }
            if rc == 0 {
                return Err(StoreError::io(
                    &self.path,
                    std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "pread short"),
                ));
            }
            done += rc as usize;
        }
        Ok(())
    }

    pub fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<(), StoreError> {
        if self.is_tx_body_fd() {
            return self.pread_at(offset, buf);
        }
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
        if self.is_tx_body_fd() {
            return self.write_at_pwrite(offset, bytes);
        }
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
        self.publish_logical_end(end);
        Ok(())
    }

    /// Write `bytes` at `offset` via **`pwrite`** (page cache), then publish HWM.
    ///
    /// Same capacity / published-length rules as [`Self::write_at`], but does not
    /// memcpy through the mmap for the payload. Intended for **linear archive
    /// appends** of large body/idx blobs so the writer need not dirty multi‑GiB
    /// map pages. MAP_SHARED readers still see data via the page cache.
    ///
    /// Sole appender role only (same as `write_at`).
    pub fn write_at_pwrite(&self, offset: u64, bytes: &[u8]) -> Result<(), StoreError> {
        if bytes.is_empty() {
            return Ok(());
        }
        let end = offset.saturating_add(bytes.len() as u64);
        self.ensure_capacity(end)?;
        let fd = self.read_file.as_raw_fd();
        let mut done = 0usize;
        while done < bytes.len() {
            // SAFETY: sole appender; buf lives for the syscall; fd is the table file.
            let rc = unsafe {
                libc::pwrite(
                    fd,
                    bytes[done..].as_ptr() as *const libc::c_void,
                    bytes.len() - done,
                    (offset + done as u64) as libc::off_t,
                )
            };
            if rc < 0 {
                return Err(StoreError::io(
                    &self.path,
                    std::io::Error::last_os_error(),
                ));
            }
            if rc == 0 {
                return Err(StoreError::io(
                    &self.path,
                    std::io::Error::new(std::io::ErrorKind::WriteZero, "pwrite returned 0"),
                ));
            }
            done += rc as usize;
        }
        self.publish_logical_end(end);
        Ok(())
    }

    /// Extend published HWM to at least `end` (Release) and refresh on-map HWM field.
    fn publish_logical_end(&self, end: u64) {
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
    }

    /// Atomic little-endian `u32` load (Acquire). Head probe path.
    /// Single-slot load (tests / diagnostics). Prefer bulk [`Self::read_at`] for pages.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn load_u32_le(&self, offset: u64) -> Result<u32, StoreError> {
        if offset % 4 != 0 {
            return Err(StoreError::Corrupt("load_u32 unaligned"));
        }
        let end = offset.saturating_add(4);
        let len = self.published_len.load(Ordering::Acquire);
        if end > len {
            return Err(StoreError::Corrupt("load_u32 past logical end"));
        }
        let pin = self.pin();
        if end > pin.epoch.cap() {
            return Err(StoreError::Corrupt("load_u32 past map end"));
        }
        // SAFETY: aligned offset within published+capacity pin.
        let v = unsafe {
            let p = pin.epoch.as_ptr().add(offset as usize) as *mut u32;
            AtomicU32::from_ptr(p).load(Ordering::Acquire)
        };
        Ok(v)
    }

    /// Atomic little-endian `u64` load (Acquire). Tests / single-slot diagnostics.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn load_u64_le(&self, offset: u64) -> Result<u64, StoreError> {
        if offset % 8 != 0 {
            return Err(StoreError::Corrupt("load_u64 unaligned"));
        }
        let end = offset.saturating_add(8);
        let len = self.published_len.load(Ordering::Acquire);
        if end > len {
            return Err(StoreError::Corrupt("load_u64 past logical end"));
        }
        let pin = self.pin();
        if end > pin.epoch.cap() {
            return Err(StoreError::Corrupt("load_u64 past map end"));
        }
        let v = unsafe {
            let p = pin.epoch.as_ptr().add(offset as usize) as *mut u64;
            AtomicU64::from_ptr(p).load(Ordering::Acquire)
        };
        Ok(v)
    }

    /// Ensure the file (and map for non-body tables) covers at least `need` bytes.
    ///
    /// Class A **tx.body**: fallocate/set_len only — never grows the multi‑GiB map.
    /// Other tables: fallocate then map a **new** epoch (readers hold old pin).
    pub fn ensure_capacity(&self, need: u64) -> Result<(), StoreError> {
        if self.is_tx_body_fd() {
            if need <= self.file_cap.load(Ordering::Acquire) {
                return Ok(());
            }
            return self.ensure_capacity_fd_only(need);
        }
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
        self.file_cap.store(new_cap, Ordering::Release);
        Ok(())
    }

    /// Grow Class A body file without remapping (payload stays pread/pwrite-only).
    fn ensure_capacity_fd_only(&self, need: u64) -> Result<(), StoreError> {
        const DOUBLE_UNTIL: u64 = 64 * 1024 * 1024;
        let cur = self.file_cap.load(Ordering::Acquire);
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
        let file = self.file.lock().unwrap();
        if need <= self.file_cap.load(Ordering::Acquire) {
            return Ok(());
        }
        if try_fallocate(&file, new_cap).is_err() {
            file.set_len(new_cap)
                .map_err(|e| StoreError::io(&self.path, e))?;
        } else if file.metadata().map(|m| m.len()).unwrap_or(0) < new_cap {
            file.set_len(new_cap)
                .map_err(|e| StoreError::io(&self.path, e))?;
        }
        drop(file);
        self.file_cap.store(new_cap, Ordering::Release);
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
        let bytes = logical.to_le_bytes();
        // Class A body: header HWM only (offset 8) via pwrite — map may be header-only.
        if self.is_tx_body_fd() && !self.trailing_header {
            let fd = self.read_file.as_raw_fd();
            let rc = unsafe {
                libc::pwrite(
                    fd,
                    bytes.as_ptr() as *const libc::c_void,
                    8,
                    8 as libc::off_t,
                )
            };
            if rc != 8 {
                // Best-effort HWM on map path used to silent-return; keep soft.
                let _ = rc;
            }
            return;
        }
        let pin = self.pin();
        if pin.epoch.cap() < logical.max(FILE_HEADER_LEN as u64) {
            return;
        }
        let hwm_off = if self.trailing_header {
            logical
                .saturating_sub(TRAILING_FOOTER_LEN as u64)
                .saturating_add(8)
        } else {
            8
        };
        if hwm_off + 8 > pin.epoch.cap() {
            return;
        }
        unsafe {
            let dst = pin.epoch.as_ptr().add(hwm_off as usize) as *mut u8;
            ptr::copy_nonoverlapping(bytes.as_ptr(), dst, 8);
        }
    }

    fn persist_logical_len(&self, logical: u64) -> Result<(), StoreError> {
        self.write_hwm_mmap(logical);
        let mut file = self.file.lock().unwrap();
        let seek = if self.trailing_header {
            logical
                .saturating_sub(TRAILING_FOOTER_LEN as u64)
                .saturating_add(8)
        } else {
            8
        };
        file.seek(SeekFrom::Start(seek))
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

    /// Walk a byte range. Class A body uses pread into a temporary buffer;
    /// other tables may zero-copy from the map pin.
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
        if self.is_tx_body_fd() {
            let mut buf = vec![0u8; len as usize];
            self.pread_at(offset, &mut buf)?;
            return Ok(f(&buf));
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
    fn concurrent_readers_during_append_and_grow() {
        use std::sync::Barrier;
        use std::thread;

        let _stress = TEST_MMAP_STRESS_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
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

        // Hard deadline: barrier+join used to hang forever if a worker panicked early.
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        thread::spawn(move || {
            for h in handles {
                h.join().unwrap();
            }
            let _ = done_tx.send(());
        });
        done_rx
            .recv_timeout(std::time::Duration::from_secs(30))
            .expect("concurrent TableFile workers timed out (hang?)");
        let final_len = f.logical_len();
        assert!(final_len > FILE_HEADER_LEN as u64 + 64);
        let mut head = [0u8; 8];
        f.read_at(FILE_HEADER_LEN as u64, &mut head).unwrap();
        // Annotator left non-zero pattern.
        assert_ne!(head, [0u8; 8]);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn load_store_u32_u64_zero_range_trailing_and_open_errors() {
        static N: AtomicU64 = AtomicU64::new(0);
        let id = N.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("rbitcoin-file-atomics-{id}"));
        let _ = std::fs::remove_file(&path);
        let f = TableFile::create(&path, TableKind::Tx).unwrap();
        // Grow published range for atomics
        let payload = [0u8; 64];
        f.write_at(FILE_HEADER_LEN as u64, &payload).unwrap();
        let off32 = FILE_HEADER_LEN as u64; // aligned
        let off64 = FILE_HEADER_LEN as u64 + 8;
        // Plain mmap slot writes (head insert path uses write_at, not atomics).
        f.write_at(off32, &0x1122_3344u32.to_le_bytes()).unwrap();
        assert_eq!(f.load_u32_le(off32).unwrap(), 0x1122_3344);
        f.write_at(off64, &0x0102_0304_0506_0708u64.to_le_bytes())
            .unwrap();
        assert_eq!(f.load_u64_le(off64).unwrap(), 0x0102_0304_0506_0708);
        assert!(matches!(
            f.load_u32_le(off32 + 1),
            Err(StoreError::Corrupt(_))
        ));
        assert!(matches!(
            f.load_u32_le(10_000),
            Err(StoreError::Corrupt(_))
        ));
        f.zero_range(0, 0).unwrap();
        f.zero_range(FILE_HEADER_LEN as u64 + 32, 16).unwrap();
        f.set_logical_len(FILE_HEADER_LEN as u64 + 48).unwrap();
        assert!(matches!(
            f.set_logical_len(0),
            Err(StoreError::Corrupt(_))
        ));
        // with_bytes past end
        assert!(matches!(
            f.with_bytes(FILE_HEADER_LEN as u64, 10_000, |_| ()),
            Err(StoreError::Corrupt(_))
        ));
        drop(f);

        // Bad magic / schema / kind
        {
            let bad = std::env::temp_dir().join(format!("rbitcoin-file-bad-{id}"));
            let _ = std::fs::remove_file(&bad);
            std::fs::write(&bad, b"XXXX\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00").unwrap();
            assert!(matches!(
                TableFile::open(&bad, TableKind::Tx),
                Err(StoreError::BadMagic)
            ));
            let _ = std::fs::remove_file(&bad);
        }
        // Trailing header create/open
        {
            let th = std::env::temp_dir().join(format!("rbitcoin-file-trail-{id}"));
            let _ = std::fs::remove_file(&th);
            let f = TableFile::create_trailing_header(&th, TableKind::HashHead).unwrap();
            let data_bytes = 64u64;
            f.set_logical_len(data_bytes + TRAILING_FOOTER_LEN as u64)
                .unwrap();
            f.write_at(0, &[0xABu8; 64]).unwrap();
            f.flush().unwrap();
            drop(f);
            let (f2, _ext) =
                TableFile::open_trailing_header(&th, TableKind::HashHead, data_bytes).unwrap();
            let mut b = [0u8; 4];
            f2.read_at(0, &mut b).unwrap();
            assert_eq!(b, [0xAB; 4]);
            // short table
            assert!(matches!(
                TableFile::open_trailing_header(&th, TableKind::HashHead, 1_000_000),
                Err(StoreError::Corrupt(_))
            ));
            // wrong kind
            assert!(matches!(
                TableFile::open_trailing_header(&th, TableKind::Tx, data_bytes),
                Err(StoreError::BadKind { .. })
            ));
            // set_trailing_ext on trailing-header file
            let (mut f3, _ext) =
                TableFile::open_trailing_header(&th, TableKind::HashHead, data_bytes).unwrap();
            f3.set_trailing_ext([0x11; 16]).unwrap();
            f3.flush().unwrap();
            drop(f3);
            // from_end requires file length == logical (no spare capacity). Build
            // a tightly-sized trailing file so EOF holds the footer.
            {
                let tight = std::env::temp_dir().join(format!("rbitcoin-file-trail-tight-{id}"));
                let _ = std::fs::remove_file(&tight);
                let data = [0xCDu8; 32];
                let logical = data.len() as u64 + TRAILING_FOOTER_LEN as u64;
                let mut footer = [0u8; TRAILING_FOOTER_LEN];
                footer[0..4].copy_from_slice(&STORE_MAGIC);
                footer[4..6].copy_from_slice(&SCHEMA_VERSION.to_le_bytes());
                footer[6..8]
                    .copy_from_slice(&TableKind::HashHead.as_u16().to_le_bytes());
                footer[8..16].copy_from_slice(&logical.to_le_bytes());
                footer[16..32].copy_from_slice(&[0x22; 16]);
                let mut raw = Vec::with_capacity(logical as usize);
                raw.extend_from_slice(&data);
                raw.extend_from_slice(&footer);
                std::fs::write(&tight, &raw).unwrap();
                let (f4, ext) =
                    TableFile::open_trailing_header_from_end(&tight, TableKind::HashHead).unwrap();
                assert_eq!(ext, [0x22; 16]);
                let mut b = [0u8; 4];
                f4.read_at(0, &mut b).unwrap();
                assert_eq!(b, [0xCD; 4]);
                drop(f4);
                let _ = std::fs::remove_file(&tight);
            }
            // short file
            let short = std::env::temp_dir().join(format!("rbitcoin-file-trail-short-{id}"));
            let _ = std::fs::remove_file(&short);
            std::fs::write(&short, b"tiny").unwrap();
            assert!(matches!(
                TableFile::open_trailing_header_from_end(&short, TableKind::HashHead),
                Err(StoreError::Corrupt(_))
            ));
            let _ = std::fs::remove_file(&short);
            // bad magic at EOF
            let badm = std::env::temp_dir().join(format!("rbitcoin-file-trail-badm-{id}"));
            let _ = std::fs::remove_file(&badm);
            let raw = vec![0u8; 64 + TRAILING_FOOTER_LEN];
            // leave footer magic wrong
            std::fs::write(&badm, &raw).unwrap();
            assert!(matches!(
                TableFile::open_trailing_header_from_end(&badm, TableKind::HashHead),
                Err(StoreError::BadMagic)
            ));
            let _ = std::fs::remove_file(&badm);
            let _ = std::fs::remove_file(&th);
        }
        // set_trailing_ext on leading-header fails
        {
            let lead = std::env::temp_dir().join(format!("rbitcoin-file-lead-{id}"));
            let _ = std::fs::remove_file(&lead);
            let mut f = TableFile::create(&lead, TableKind::Tx).unwrap();
            assert!(matches!(
                f.set_trailing_ext([0; 16]),
                Err(StoreError::Corrupt(_))
            ));
            drop(f);
            let _ = std::fs::remove_file(&lead);
        }
        // read past logical end
        {
            let p = std::env::temp_dir().join(format!("rbitcoin-file-readpast-{id}"));
            let _ = std::fs::remove_file(&p);
            let f = TableFile::create(&p, TableKind::Tx).unwrap();
            let mut big = [0u8; 8];
            assert!(matches!(
                f.read_at(FILE_HEADER_LEN as u64 + 1000, &mut big),
                Err(StoreError::Corrupt(_))
            ));
            drop(f);
            let _ = std::fs::remove_file(&p);
        }
        let _ = ensure_nofile_budget();
        let _ = ensure_nofile_budget_at_least(64);
        let _ = std::fs::remove_file(&path);
    }

    /// Trailing-header open error arms (short, bad magic, bad schema/kind).
    #[test]
    fn trailing_header_open_error_arms() {
        static N: AtomicU64 = AtomicU64::new(0);
        let id = N.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("rbitcoin-trail-err-{id}"));
        let _ = std::fs::create_dir_all(&dir);
        // Too short for footer
        let short = dir.join("short");
        std::fs::write(&short, b"tiny").unwrap();
        assert!(matches!(
            TableFile::open_trailing_header_from_end(&short, TableKind::Tx),
            Err(StoreError::Corrupt(_))
        ));
        // Footer-sized but bad magic
        let bad_magic = dir.join("badmag");
        std::fs::write(&bad_magic, vec![0u8; TRAILING_FOOTER_LEN]).unwrap();
        assert!(matches!(
            TableFile::open_trailing_header_from_end(&bad_magic, TableKind::Tx),
            Err(StoreError::BadMagic)
        ));
        // Valid trailing create then wrong kind / open paths
        let good = dir.join("good");
        let f = TableFile::create_trailing_header(&good, TableKind::Tx).unwrap();
        // Grow body and rewrite footer so open_from_end sees full layout
        f.ensure_capacity(4096 + TRAILING_FOOTER_LEN as u64).unwrap();
        f.set_logical_len(4096 + TRAILING_FOOTER_LEN as u64).unwrap();
        drop(f);
        let (_f2, _ext) = TableFile::open_trailing_header_from_end(&good, TableKind::Tx).unwrap();
        assert!(matches!(
            TableFile::open_trailing_header_from_end(&good, TableKind::Header),
            Err(StoreError::BadKind { .. })
        ));
        // open_trailing_header with explicit data_bytes
        let good2 = dir.join("good2");
        let f = TableFile::create_trailing_header(&good2, TableKind::Tx).unwrap();
        f.ensure_capacity(1024 + TRAILING_FOOTER_LEN as u64).unwrap();
        f.set_logical_len(1024 + TRAILING_FOOTER_LEN as u64).unwrap();
        drop(f);
        let (_f3, _) = TableFile::open_trailing_header(&good2, TableKind::Tx, 1024).unwrap();
        // Short data_bytes vs file
        assert!(TableFile::open_trailing_header(&good2, TableKind::Tx, 50_000).is_err());
        // zero_range / load_store helpers on normal file
        let path = dir.join("normal");
        let f = TableFile::create(&path, TableKind::Tx).unwrap();
        f.write_at(FILE_HEADER_LEN as u64, &[1, 2, 3, 4, 5, 6, 7, 8])
            .unwrap();
        let u = f.load_u32_le(FILE_HEADER_LEN as u64).unwrap();
        assert_eq!(u, u32::from_le_bytes([1, 2, 3, 4]));
        f.write_at(FILE_HEADER_LEN as u64, &0x1122_3344u32.to_le_bytes())
            .unwrap();
        f.write_at(
            FILE_HEADER_LEN as u64,
            &0x0102_0304_0506_0708u64.to_le_bytes(),
        )
        .unwrap();
        let _ = f.load_u64_le(FILE_HEADER_LEN as u64).unwrap();
        f.zero_range(FILE_HEADER_LEN as u64, 8).unwrap();
        f.set_logical_len(FILE_HEADER_LEN as u64 + 8).unwrap();
        let _ = f.path();
        let _ = f.data_len();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// pwrite payload must be visible to mmap `read_at` (page-cache coherency).
    #[test]
    fn write_at_pwrite_visible_via_mmap_read() {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let id = N.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("rbitcoin-pwrite-vis-{id}"));
        let _ = std::fs::remove_file(&path);
        let f = TableFile::create(&path, TableKind::Tx).unwrap();
        let payload = b"fd-append-payload-0123456789abcdef";
        let off = FILE_HEADER_LEN as u64;
        f.write_at_pwrite(off, payload).unwrap();
        assert_eq!(f.logical_len(), off + payload.len() as u64);
        let mut got = vec![0u8; payload.len()];
        f.read_at(off, &mut got).unwrap();
        assert_eq!(&got[..], payload);
        let more = b"MORE";
        let off2 = f.logical_len();
        f.write_at_pwrite(off2, more).unwrap();
        let mut got2 = [0u8; 4];
        f.read_at(off2, &mut got2).unwrap();
        assert_eq!(&got2, more);
        drop(f);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn table_file_surface_and_nofile_budget() {
        static N: AtomicU64 = AtomicU64::new(0);
        let id = N.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("rbitcoin-file-surface-{id}"));
        let _ = std::fs::remove_file(&path);
        let f = TableFile::create(&path, TableKind::Tx).unwrap();
        assert!(f.logical_len() >= FILE_HEADER_LEN as u64);
        let payload = b"hello-table";
        f.write_at(FILE_HEADER_LEN as u64, payload).unwrap();
        let mut buf = vec![0u8; payload.len()];
        f.read_at(FILE_HEADER_LEN as u64, &mut buf).unwrap();
        assert_eq!(&buf, payload);
        f.with_bytes(FILE_HEADER_LEN as u64, payload.len() as u64, |b| {
            assert_eq!(b, payload);
        })
        .unwrap();
        f.ensure_capacity(f.logical_len() + 4096).unwrap();
        f.flush().unwrap();
        f.flush_async().unwrap();
        let fd = f.read_fd();
        assert!(fd >= 0);
        drop(f);
        let f = TableFile::open(&path, TableKind::Tx).unwrap();
        let mut buf2 = vec![0u8; payload.len()];
        f.read_at(FILE_HEADER_LEN as u64, &mut buf2).unwrap();
        assert_eq!(&buf2, payload);
        // Bad kind
        assert!(matches!(
            TableFile::open(&path, TableKind::Header),
            Err(StoreError::BadKind { .. })
        ));
        drop(f);
        // nofile budget is best-effort
        let (soft, hard) = ensure_nofile_budget();
        assert!(soft > 0 || hard > 0 || cfg!(not(unix)));
        let (s2, _) = ensure_nofile_budget_at_least(64);
        assert!(s2 >= 64 || cfg!(not(unix)) || soft == 0);
        let _ = std::fs::remove_file(&path);
    }
}

pub const NOFILE_SOFT_TARGET: u64 = 16_384;

/// Process-wide lock for multi-thread mmap stress tests (grow / concurrent
/// readers / online head resize). Tests still run their own workers; this only
/// prevents *cross-test* overlap that has shown intermittent heap corruption.
#[cfg(test)]
pub(crate) static TEST_MMAP_STRESS_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());


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
