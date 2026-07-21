//! Process-wide IBD IO policy for table flushes.
//!
//! Historical note: catch-up progressive materialize deferred `msync`/`fdatasync`
//! while applying runs into open-hash heads. That mode is gone; the flag is kept
//! so flush paths stay centralized and can be re-armed if needed. Default is
//! **never defer** (always durable flush).

use std::sync::atomic::{AtomicBool, Ordering};

static DEFER_DURABLE_FLUSH: AtomicBool = AtomicBool::new(false);

/// While true, skip msync/fdatasync in [`crate::file::TableFile::flush`].
///
/// Production IBD leaves this false.
pub fn set_defer_durable_flush(defer: bool) {
    DEFER_DURABLE_FLUSH.store(defer, Ordering::Relaxed);
}

#[inline]
pub fn defer_durable_flush() -> bool {
    DEFER_DURABLE_FLUSH.load(Ordering::Relaxed)
}
