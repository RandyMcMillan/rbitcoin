//! Archive writer coalesce policy (fixed batch sizes — not tip-lead dependent).
//!
//! Larger fixed quanta amortize head insert / create_fk resolve and raise
//! same-batch parent hit rate. Queue RAM / short waits still bound latency.

use std::time::Duration;

/// Default max blocks per `archive_prepared` call.
pub const DEFAULT_MAX_BATCH: usize = 128;
/// Default min blocks before flush (unless timeout / queue pressure).
pub const DEFAULT_MIN_BATCH: usize = 32;

fn max_batch_from_env() -> usize {
    std::env::var("RBITCOIN_ARCHIVE_MAX_BATCH")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_MAX_BATCH)
        .clamp(8, 512)
}

fn min_batch_from_env() -> usize {
    let max = max_batch_from_env();
    std::env::var("RBITCOIN_ARCHIVE_MIN_BATCH")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_MIN_BATCH)
        .clamp(1, max)
}

/// Cap on blocks per far-lane write (**fixed**; lag ignored).
pub(crate) fn max_batch_for_lag(_confirm_lag: u32) -> usize {
    max_batch_from_env()
}

/// Target minimum batch size before flushing (**fixed**; lag ignored).
pub(crate) fn min_batch_for_lag(_confirm_lag: u32) -> usize {
    min_batch_from_env()
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

    // Fixed short waits — not a function of tip lead.
    let fill_ms = 8;
    let heavy_ms = 12;
    let mid_ms = 4;
    let dry_ms = 1;

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
        return Duration::from_millis(2);
    }
    Duration::from_millis(dry_ms)
}

#[cfg(test)]
mod tests {
    use super::{coalesce_wait, max_batch_for_lag, min_batch_for_lag};
    use std::time::Duration;

    #[test]
    fn batch_sizes_ignore_confirm_lag() {
        assert_eq!(max_batch_for_lag(0), max_batch_for_lag(100_000));
        assert_eq!(min_batch_for_lag(0), min_batch_for_lag(100_000));
        assert!(max_batch_for_lag(0) >= 64);
        assert!(min_batch_for_lag(0) >= 1);
        assert!(min_batch_for_lag(0) <= max_batch_for_lag(0));
    }

    #[test]
    fn no_wait_when_batch_already_large() {
        let min = min_batch_for_lag(0);
        assert_eq!(coalesce_wait(min, 0, 0, 0), Duration::ZERO);
        assert_eq!(coalesce_wait(min, 0, 0, 600), Duration::ZERO);
    }

    #[test]
    fn dry_pipeline_is_short() {
        assert_eq!(coalesce_wait(1, 0, 0, 0), Duration::from_millis(1));
    }
}
