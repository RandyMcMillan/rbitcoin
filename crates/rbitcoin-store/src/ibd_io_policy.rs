//! Process-wide IBD IO policy: defer costly durable flushes while catch-up runs
//! are materializing into open-hash heads.
//!
//! When deferred, [`crate::file::TableFile::flush`] updates the in-mmap HWM only
//! (no `msync` / `fdatasync`). Callers should [`set_defer_durable_flush(false)`]
//! and flush Class B tables when leaving materialize mode so dirty pages land
//! before archive/getdata resume.

use std::sync::atomic::{AtomicBool, Ordering};

static DEFER_DURABLE_FLUSH: AtomicBool = AtomicBool::new(false);

/// While true, skip msync/fdatasync in [`crate::file::TableFile::flush`].
pub fn set_defer_durable_flush(defer: bool) {
    DEFER_DURABLE_FLUSH.store(defer, Ordering::Relaxed);
}

#[inline]
pub fn defer_durable_flush() -> bool {
    DEFER_DURABLE_FLUSH.load(Ordering::Relaxed)
}
