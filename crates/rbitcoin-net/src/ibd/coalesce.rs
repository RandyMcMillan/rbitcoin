//! Archive writer coalesce policy (how long to wait for a fuller mega-batch).

use std::time::Duration;

/// Target minimum batch size given how far Class A leads confirmed tip.
///
/// Deep confirm lag → pack larger batches (fewer writer syscalls). Near tip →
/// flush sooner so tip+1 is confirmable.
pub(crate) fn min_batch_for_lag(confirm_lag: u32) -> usize {
    // Larger mega-batches improve per-shard head write locality (256-way
    // partitioned hash heads): more keys land in each shard before the writer
    // cycles back. Near tip still flush sooner so confirm is not starved.
    if confirm_lag >= 512 {
        256
    } else if confirm_lag >= 128 {
        128
    } else if confirm_lag >= 32 {
        64
    } else {
        32
    }
}

/// How long the writer should wait for more prepared bodies before flushing.
pub(crate) fn coalesce_wait(
    batch_len: usize,
    write_q: usize,
    arch_q: usize,
    confirm_lag: u32,
) -> Duration {
    let min_batch = min_batch_for_lag(confirm_lag);
    if batch_len >= min_batch {
        return Duration::ZERO;
    }

    let deep = confirm_lag >= 128;
    let fill_ms = if deep { 16 } else { 8 };
    let heavy_ms = if deep { 20 } else { 12 };
    let mid_ms = if deep { 12 } else { 4 };
    let dry_ms = if deep { 4 } else { 1 };

    if write_q > 0 && batch_len + write_q >= min_batch {
        return Duration::from_millis(fill_ms);
    }
    if write_q >= 32 || arch_q >= 2048 {
        return Duration::from_millis(heavy_ms);
    }
    if write_q >= 8 || arch_q >= 128 {
        return Duration::from_millis(mid_ms);
    }
    if arch_q >= 32 || write_q > 0 {
        return Duration::from_millis(mid_ms.min(4).max(2));
    }
    Duration::from_millis(dry_ms)
}

#[cfg(test)]
mod tests {
    use super::{coalesce_wait, min_batch_for_lag};
    use std::time::Duration;

    #[test]
    fn no_wait_when_batch_already_large() {
        assert_eq!(coalesce_wait(32, 0, 0, 0), Duration::ZERO);
        assert_eq!(coalesce_wait(64, 0, 0, 40), Duration::ZERO);
        assert_eq!(coalesce_wait(256, 500, 5000, 600), Duration::ZERO);
    }

    #[test]
    fn dry_pipeline_near_tip_is_short() {
        assert_eq!(coalesce_wait(1, 0, 0, 0), Duration::from_millis(1));
    }

    #[test]
    fn confirm_lag_raises_min_batch() {
        assert_eq!(min_batch_for_lag(0), 32);
        assert_eq!(min_batch_for_lag(32), 64);
        assert_eq!(min_batch_for_lag(128), 128);
        assert_eq!(min_batch_for_lag(512), 256);
        assert_eq!(coalesce_wait(256, 0, 0, 600), Duration::ZERO);
        assert!(coalesce_wait(1, 0, 0, 600) > Duration::ZERO);
    }

    #[test]
    fn write_q_fill_to_min_batch() {
        // lag=0 min_batch=32; need write_q to help reach min
        assert_eq!(coalesce_wait(1, 40, 0, 0), Duration::from_millis(8));
        assert_eq!(coalesce_wait(100, 200, 0, 600), Duration::from_millis(16));
    }
}
