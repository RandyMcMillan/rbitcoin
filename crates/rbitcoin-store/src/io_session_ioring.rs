//! Windows IoRing probe (Win11 / Server 2022+).
//!
//! Probe only. Default Windows session is IOCP until a real submit engine
//! exists. Do not treat `ioring_available()` as “we are on IoRing.”

use std::sync::atomic::{AtomicU8, Ordering};

static PROBE: AtomicU8 = AtomicU8::new(0); // 0 unknown, 1 yes, 2 no

/// True when `CreateIoRing` can be resolved (not a guarantee of perf).
pub fn ioring_available() -> bool {
    match PROBE.load(Ordering::Relaxed) {
        1 => true,
        2 => false,
        _ => {
            let ok = probe();
            PROBE.store(if ok { 1 } else { 2 }, Ordering::Relaxed);
            ok
        }
    }
}

fn probe() -> bool {
    // `CreateIoRing` lives in kernel32 / onecore on recent Windows.
    // Missing export → IOCP default.
    extern "system" {
        fn GetModuleHandleA(name: *const u8) -> *mut core::ffi::c_void;
        fn GetProcAddress(m: *mut core::ffi::c_void, name: *const u8) -> *mut core::ffi::c_void;
    }
    unsafe {
        let k32 = GetModuleHandleA(b"kernel32.dll\0".as_ptr());
        if k32.is_null() {
            return false;
        }
        !GetProcAddress(k32, b"CreateIoRing\0".as_ptr()).is_null()
    }
}
