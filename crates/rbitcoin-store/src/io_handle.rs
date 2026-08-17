//! Portable positional file handle for completion-driven store IO.
//!
//! Unix: raw fd. Windows: `HANDLE`. The session never closes the handle.

/// Borrowed OS handle for one positional pread/pwrite. Copy, not an owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct IoHandle {
    #[cfg(unix)]
    fd: std::os::fd::RawFd,
    #[cfg(windows)]
    handle: isize,
}

impl IoHandle {
    #[cfg(unix)]
    #[inline]
    pub fn from_raw_fd(fd: std::os::fd::RawFd) -> Self {
        Self { fd }
    }

    #[cfg(unix)]
    #[inline]
    pub fn as_raw_fd(self) -> std::os::fd::RawFd {
        self.fd
    }

    #[cfg(windows)]
    #[inline]
    pub fn from_raw_handle(handle: isize) -> Self {
        Self { handle }
    }

    #[cfg(windows)]
    #[inline]
    pub fn as_raw_handle(self) -> isize {
        self.handle
    }

    /// Borrow a positional handle from an open file (does not take ownership).
    pub fn from_file(file: &std::fs::File) -> Self {
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            Self::from_raw_fd(file.as_raw_fd())
        }
        #[cfg(windows)]
        {
            use std::os::windows::io::AsRawHandle;
            Self::from_raw_handle(file.as_raw_handle() as isize)
        }
    }

    /// One-shot positional read. Returns bytes transferred or negated errno.
    pub fn pread(self, offset: u64, buf: &mut [u8]) -> i32 {
        if buf.is_empty() {
            return 0;
        }
        #[cfg(unix)]
        {
            let n = unsafe {
                libc::pread(
                    self.fd,
                    buf.as_mut_ptr() as *mut libc::c_void,
                    buf.len(),
                    offset as libc::off_t,
                )
            };
            if n < 0 {
                -std::io::Error::last_os_error().raw_os_error().unwrap_or(5)
            } else {
                n as i32
            }
        }
        #[cfg(windows)]
        {
            win_pread(self.handle, offset, buf)
        }
    }

    /// One-shot positional write. Returns bytes transferred or negated errno.
    pub fn pwrite(self, offset: u64, buf: &[u8]) -> i32 {
        if buf.is_empty() {
            return 0;
        }
        #[cfg(unix)]
        {
            let n = unsafe {
                libc::pwrite(
                    self.fd,
                    buf.as_ptr() as *const libc::c_void,
                    buf.len(),
                    offset as libc::off_t,
                )
            };
            if n < 0 {
                -std::io::Error::last_os_error().raw_os_error().unwrap_or(5)
            } else {
                n as i32
            }
        }
        #[cfg(windows)]
        {
            win_pwrite(self.handle, offset, buf)
        }
    }
}

#[cfg(unix)]
impl From<std::os::fd::RawFd> for IoHandle {
    fn from(fd: std::os::fd::RawFd) -> Self {
        Self::from_raw_fd(fd)
    }
}

#[cfg(windows)]
fn win_pread(handle: isize, offset: u64, buf: &mut [u8]) -> i32 {
    win_xfer(handle, offset, buf.as_mut_ptr(), buf.len(), false)
}

#[cfg(windows)]
fn win_pwrite(handle: isize, offset: u64, buf: &[u8]) -> i32 {
    win_xfer(handle, offset, buf.as_ptr() as *mut u8, buf.len(), true)
}

#[cfg(windows)]
fn win_xfer(handle: isize, offset: u64, ptr: *mut u8, len: usize, write: bool) -> i32 {
    use std::ptr::null_mut;
    #[repr(C)]
    struct Overlapped {
        internal: usize,
        internal_high: usize,
        off: u32,
        off_high: u32,
        event: *mut core::ffi::c_void,
    }
    extern "system" {
        fn ReadFile(
            h: *mut core::ffi::c_void,
            buf: *mut u8,
            n: u32,
            got: *mut u32,
            ov: *mut Overlapped,
        ) -> i32;
        fn WriteFile(
            h: *mut core::ffi::c_void,
            buf: *const u8,
            n: u32,
            got: *mut u32,
            ov: *mut Overlapped,
        ) -> i32;
        fn GetOverlappedResult(
            h: *mut core::ffi::c_void,
            ov: *mut Overlapped,
            got: *mut u32,
            wait: i32,
        ) -> i32;
        fn GetLastError() -> u32;
    }
    const ERROR_IO_PENDING: u32 = 997;
    let mut ov = Overlapped {
        internal: 0,
        internal_high: 0,
        off: offset as u32,
        off_high: (offset >> 32) as u32,
        event: null_mut(),
    };
    let mut got: u32 = 0;
    let h = handle as *mut core::ffi::c_void;
    let ok = if write {
        unsafe { WriteFile(h, ptr, len as u32, &mut got, &mut ov) }
    } else {
        unsafe { ReadFile(h, ptr, len as u32, &mut got, &mut ov) }
    };
    if ok == 0 {
        let err = unsafe { GetLastError() };
        if err != ERROR_IO_PENDING {
            return -(err as i32);
        }
        if unsafe { GetOverlappedResult(h, &mut ov, &mut got, 1) } == 0 {
            return -(unsafe { GetLastError() } as i32);
        }
    }
    got as i32
}
