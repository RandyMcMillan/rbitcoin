//! Read-only mmap of an immutable sealed file (not [`crate::file::TableFile`]).
//!
//! Used for `.fuse8` fingerprints and BDZ3 occupancy. Kernel reclaim drops
//! clean file pages (no swap write).

use crate::error::StoreError;
use std::fs::File;
use std::path::Path;
use std::ptr;

/// Process mapping of a sealed file prefix. Fingerprints are a subrange.
pub struct ReadonlyMap {
    ptr: *mut u8,
    map_len: usize,
    fp_off: usize,
    fp_len: usize,
    _file: File,
}

// SAFETY: mapping is read-only and immutable after [`ReadonlyMap::map_path`].
unsafe impl Send for ReadonlyMap {}
unsafe impl Sync for ReadonlyMap {}

impl std::fmt::Debug for ReadonlyMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadonlyMap")
            .field("map_len", &self.map_len)
            .field("fp_off", &self.fp_off)
            .field("fp_len", &self.fp_len)
            .finish()
    }
}

impl ReadonlyMap {
    pub fn map_path(path: &Path) -> Result<Self, StoreError> {
        let file = File::open(path).map_err(|e| StoreError::io(path, e))?;
        let map_len = file.metadata().map_err(|e| StoreError::io(path, e))?.len() as usize;
        if map_len == 0 {
            return Err(StoreError::Corrupt("fuse8 file empty"));
        }
        Self::map_file(path, file, map_len)
    }

    /// Map the first `len` bytes. Does not include the file tail (SH tags).
    pub fn map_prefix(path: &Path, len: usize) -> Result<Self, StoreError> {
        if len == 0 {
            return Err(StoreError::Corrupt("mapped file empty"));
        }
        let file = File::open(path).map_err(|e| StoreError::io(path, e))?;
        let file_len = file.metadata().map_err(|e| StoreError::io(path, e))?.len() as usize;
        if file_len < len {
            return Err(StoreError::Corrupt("mapped file truncated"));
        }
        Self::map_file(path, file, len)
    }

    fn map_file(path: &Path, file: File, map_len: usize) -> Result<Self, StoreError> {
        let ptr = map_readonly(&file, map_len).map_err(|e| StoreError::io(path, e))?;
        Ok(Self { ptr, map_len, fp_off: 0, fp_len: map_len, _file: file })
    }

    pub fn as_file_bytes(&self) -> &[u8] {
        // SAFETY: ptr is a live mapping of map_len bytes for `_file`.
        unsafe { std::slice::from_raw_parts(self.ptr, self.map_len) }
    }

    pub fn set_fingerprint_range(&mut self, off: usize, len: usize) -> Result<(), StoreError> {
        if off.saturating_add(len) > self.map_len {
            return Err(StoreError::Corrupt("fuse8 fingerprints truncated"));
        }
        self.fp_off = off;
        self.fp_len = len;
        Ok(())
    }

    pub fn fingerprints(&self) -> &[u8] {
        // SAFETY: fp_off..fp_off+fp_len is inside the mapping (set_fingerprint_range).
        unsafe { std::slice::from_raw_parts(self.ptr.add(self.fp_off), self.fp_len) }
    }
}

impl Drop for ReadonlyMap {
    fn drop(&mut self) {
        unmap(self.ptr, self.map_len);
        self.ptr = ptr::null_mut();
    }
}

#[cfg(unix)]
fn map_readonly(file: &File, len: usize) -> std::io::Result<*mut u8> {
    use std::os::fd::AsRawFd;
    let ptr = unsafe {
        libc::mmap(ptr::null_mut(), len, libc::PROT_READ, libc::MAP_SHARED, file.as_raw_fd(), 0)
    };
    if ptr == libc::MAP_FAILED {
        return Err(std::io::Error::last_os_error());
    }
    Ok(ptr as *mut u8)
}

#[cfg(unix)]
fn unmap(ptr: *mut u8, len: usize) {
    if ptr.is_null() || len == 0 {
        return;
    }
    unsafe {
        libc::munmap(ptr as *mut libc::c_void, len);
    }
}

#[cfg(windows)]
fn map_readonly(file: &File, len: usize) -> std::io::Result<*mut u8> {
    use std::os::windows::io::AsRawHandle;
    let handle = file.as_raw_handle() as *mut std::ffi::c_void;
    let mapping =
        unsafe { CreateFileMappingW(handle, ptr::null_mut(), PAGE_READONLY, 0, 0, ptr::null()) };
    if mapping.is_null() {
        return Err(std::io::Error::last_os_error());
    }
    let view = unsafe { MapViewOfFile(mapping, FILE_MAP_READ, 0, 0, len) };
    unsafe {
        CloseHandle(mapping);
    }
    if view.is_null() {
        return Err(std::io::Error::last_os_error());
    }
    Ok(view as *mut u8)
}

#[cfg(windows)]
fn unmap(ptr: *mut u8, _len: usize) {
    if ptr.is_null() {
        return;
    }
    unsafe {
        UnmapViewOfFile(ptr as *mut std::ffi::c_void);
    }
}

#[cfg(windows)]
const PAGE_READONLY: u32 = 0x02;
#[cfg(windows)]
const FILE_MAP_READ: u32 = 0x0004;

#[cfg(windows)]
extern "system" {
    fn CreateFileMappingW(
        file: *mut std::ffi::c_void,
        sec: *mut std::ffi::c_void,
        protect: u32,
        max_high: u32,
        max_low: u32,
        name: *const u16,
    ) -> *mut std::ffi::c_void;
    fn MapViewOfFile(
        mapping: *mut std::ffi::c_void,
        access: u32,
        off_high: u32,
        off_low: u32,
        size: usize,
    ) -> *mut std::ffi::c_void;
    fn UnmapViewOfFile(view: *mut std::ffi::c_void) -> i32;
    fn CloseHandle(handle: *mut std::ffi::c_void) -> i32;
}

#[cfg(not(any(unix, windows)))]
fn map_readonly(_file: &File, _len: usize) -> std::io::Result<*mut u8> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "readonly mmap requires unix or windows",
    ))
}

#[cfg(not(any(unix, windows)))]
fn unmap(_ptr: *mut u8, _len: usize) {}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp() -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!(
            "rbitcoin-romap-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn map_prefix_excludes_file_tail() {
        let dir = tmp();
        let path = dir.join("t.bin");
        let mut body = vec![0u8; 64];
        body[0..4].copy_from_slice(b"HEAD");
        body[32..36].copy_from_slice(b"OCC!");
        body[48..52].copy_from_slice(b"TAIL");
        std::fs::write(&path, &body).unwrap();
        let map = ReadonlyMap::map_prefix(&path, 40).unwrap();
        let b = map.as_file_bytes();
        assert_eq!(b.len(), 40);
        assert_eq!(&b[0..4], b"HEAD");
        assert_eq!(&b[32..36], b"OCC!");
        assert!(!b.windows(4).any(|w| w == b"TAIL"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn map_prefix_longer_than_file_is_corrupt() {
        let dir = tmp();
        let path = dir.join("short.bin");
        std::fs::write(&path, b"abcd").unwrap();
        match ReadonlyMap::map_prefix(&path, 16) {
            Err(StoreError::Corrupt(m)) => assert_eq!(m, "mapped file truncated"),
            other => panic!("expected truncated, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
