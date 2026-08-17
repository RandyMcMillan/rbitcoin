//! Windows IOCP completion session.
//!
//! Associates file handles with one completion port. `push_*` issues
//! overlapped ReadFile/WriteFile; `harvest_ready` drains
//! `GetQueuedCompletionStatus`.

use crate::error::StoreError;
use crate::io_handle::IoHandle;
use std::collections::VecDeque;
use std::path::Path;
use std::ptr::null_mut;

pub(crate) struct IocpEngine {
    port: *mut core::ffi::c_void,
    ready: VecDeque<(u64, i32)>,
}

// IOCP handle is used only from the machine thread (same as uring TLS).
unsafe impl Send for IocpEngine {}

extern "system" {
    fn CreateIoCompletionPort(
        file: *mut core::ffi::c_void,
        existing: *mut core::ffi::c_void,
        key: usize,
        threads: u32,
    ) -> *mut core::ffi::c_void;
    fn GetQueuedCompletionStatus(
        port: *mut core::ffi::c_void,
        transferred: *mut u32,
        key: *mut usize,
        ov: *mut *mut Overlapped,
        timeout_ms: u32,
    ) -> i32;
    fn CloseHandle(h: *mut core::ffi::c_void) -> i32;
    fn ReadFile(
        h: *mut core::ffi::c_void,
        buf: *mut u8,
        n: u32,
        got: *mut u32,
        ov: *mut core::ffi::c_void,
    ) -> i32;
    fn WriteFile(
        h: *mut core::ffi::c_void,
        buf: *const u8,
        n: u32,
        got: *mut u32,
        ov: *mut core::ffi::c_void,
    ) -> i32;
    fn GetLastError() -> u32;
}

#[repr(C)]
struct Overlapped {
    internal: usize,
    internal_high: usize,
    off: u32,
    off_high: u32,
    event: *mut core::ffi::c_void,
    user_data: u64,
}

const ERROR_IO_PENDING: u32 = 997;
const INVALID: *mut core::ffi::c_void = -1isize as *mut core::ffi::c_void;

impl IocpEngine {
    pub(crate) fn open(_entries: u32) -> Result<Self, StoreError> {
        let port = unsafe { CreateIoCompletionPort(INVALID, null_mut(), 0, 1) };
        if port.is_null() {
            return Err(StoreError::io(
                Path::new("iocp"),
                std::io::Error::from_raw_os_error(unsafe { GetLastError() } as i32),
            ));
        }
        Ok(Self {
            port,
            ready: VecDeque::new(),
        })
    }

    fn associate(&self, handle: IoHandle) -> Result<(), StoreError> {
        let h = handle.as_raw_handle() as *mut core::ffi::c_void;
        let p = unsafe { CreateIoCompletionPort(h, self.port, 0, 0) };
        if p.is_null() {
            // Already associated is often ERROR_INVALID_PARAMETER; ignore if port matches.
            let err = unsafe { GetLastError() };
            if err != 0 && err != 87 {
                return Err(StoreError::io(
                    Path::new("iocp"),
                    std::io::Error::from_raw_os_error(err as i32),
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn push_pread(
        &mut self,
        handle: IoHandle,
        offset: u64,
        buf: &mut [u8],
        user_data: u64,
    ) -> Result<(), StoreError> {
        self.issue(
            handle,
            offset,
            buf.as_mut_ptr(),
            buf.len(),
            user_data,
            false,
        )
    }

    pub(crate) fn push_pwrite(
        &mut self,
        handle: IoHandle,
        offset: u64,
        buf: &[u8],
        user_data: u64,
    ) -> Result<(), StoreError> {
        self.issue(
            handle,
            offset,
            buf.as_ptr() as *mut u8,
            buf.len(),
            user_data,
            true,
        )
    }

    fn issue(
        &mut self,
        handle: IoHandle,
        offset: u64,
        ptr: *mut u8,
        len: usize,
        user_data: u64,
        write: bool,
    ) -> Result<(), StoreError> {
        self.associate(handle)?;
        let ov = Box::into_raw(Box::new(Overlapped {
            internal: 0,
            internal_high: 0,
            off: offset as u32,
            off_high: (offset >> 32) as u32,
            event: null_mut(),
            user_data,
        }));
        let h = handle.as_raw_handle() as *mut core::ffi::c_void;
        let mut got = 0u32;
        let ovp = ov as *mut core::ffi::c_void;
        let ok = if write {
            unsafe { WriteFile(h, ptr, len as u32, &mut got, ovp) }
        } else {
            unsafe { ReadFile(h, ptr, len as u32, &mut got, ovp) }
        };
        if ok == 0 {
            let err = unsafe { GetLastError() };
            if err != ERROR_IO_PENDING {
                unsafe {
                    drop(Box::from_raw(ov));
                }
                return Err(StoreError::io(
                    Path::new("iocp"),
                    std::io::Error::from_raw_os_error(err as i32),
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn harvest_ready(&mut self) -> Vec<(u64, i32)> {
        self.poll(0);
        self.ready.drain(..).collect()
    }

    pub(crate) fn wait_one_cqe(&mut self) -> Result<(), StoreError> {
        if self.ready.is_empty() {
            self.poll(u32::MAX);
        }
        Ok(())
    }

    pub(crate) fn wait_idle(&mut self) -> Result<(), StoreError> {
        // Caller harvests until pending is empty; one long wait then drain.
        self.poll(50);
        Ok(())
    }

    fn poll(&mut self, timeout_ms: u32) {
        loop {
            let mut xfer = 0u32;
            let mut key = 0usize;
            let mut ov: *mut Overlapped = null_mut();
            let ok = unsafe {
                GetQueuedCompletionStatus(self.port, &mut xfer, &mut key, &mut ov, timeout_ms)
            };
            if ov.is_null() {
                return;
            }
            let boxed = unsafe { Box::from_raw(ov) };
            let res = if ok == 0 {
                -(unsafe { GetLastError() } as i32)
            } else {
                xfer as i32
            };
            self.ready.push_back((boxed.user_data, res));
            if timeout_ms == 0 {
                // drain ready without blocking again
                continue;
            }
            return;
        }
    }
}

impl Drop for IocpEngine {
    fn drop(&mut self) {
        if !self.port.is_null() {
            unsafe {
                CloseHandle(self.port);
            }
            self.port = null_mut();
        }
    }
}
