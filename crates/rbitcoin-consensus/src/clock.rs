//! Node clock for consensus / generate. Mock time is **not** a process `time()` hook.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Seconds since epoch. `mock == 0` means wall clock.
#[derive(Debug)]
pub struct NodeClock {
    mock: AtomicI64,
}

impl NodeClock {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            mock: AtomicI64::new(0),
        })
    }

    pub fn now_secs(&self) -> u64 {
        let m = self.mock.load(Ordering::Relaxed);
        if m > 0 {
            m as u64
        } else {
            wall_now()
        }
    }

    /// `0` restores wall clock. Negative is rejected by RPC.
    pub fn set_mock(&self, t: i64) {
        self.mock.store(t.max(0), Ordering::SeqCst);
    }
}

pub fn wall_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

thread_local! {
    static NOW_OVERRIDE: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
}

/// Scoped override for consensus header/time checks (does not affect log stamps).
pub fn with_now<T>(now: u64, f: impl FnOnce() -> T) -> T {
    NOW_OVERRIDE.with(|c| {
        let prev = c.replace(Some(now));
        let out = f();
        c.set(prev);
        out
    })
}

pub fn current_now() -> u64 {
    NOW_OVERRIDE.with(|c| c.get()).unwrap_or_else(wall_now)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mock_then_wall() {
        let c = NodeClock::new();
        c.set_mock(1_700_000_000);
        assert_eq!(c.now_secs(), 1_700_000_000);
        c.set_mock(0);
        assert!(c.now_secs() >= 1_700_000_000 || c.now_secs() > 1_600_000_000);
    }

    #[test]
    fn with_now_scopes() {
        assert!(current_now() > 0);
        with_now(42, || assert_eq!(current_now(), 42));
        assert_ne!(current_now(), 42);
    }
}
