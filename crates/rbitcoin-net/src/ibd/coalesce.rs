//! Archive writer coalesce policy (how long to wait / how large a mega-batch).
//!
//! **When Class A leads tip a lot**, old policy packed 256–1024 blocks into one
//! `archive_prepared` call. That monopolized mmap/heads for minutes: tip sat on
//! `confirm_live` while `arch_q` froze and `write_blks=0` between mega dumps.
//!
//! **Now:** deep lag → **smaller write quanta** so the writer finishes often and
//! confirm can interleave page faults. Near tip still uses moderate batches for
//! throughput.

use std::time::Duration;

/// Cap on blocks per far-lane write when confirm lag is high.
///
/// Logs showed ~300-block batches with `writer_busy%=100` and multi-minute tip
/// freezes; 32–64 block quanta keep arch draining without multi-minute locks.
pub(crate) fn max_batch_for_lag(confirm_lag: u32) -> usize {
    if confirm_lag >= 8192 {
        32
    } else if confirm_lag >= 2048 {
        48
    } else if confirm_lag >= 512 {
        64
    } else if confirm_lag >= 128 {
        128
    } else if confirm_lag >= 32 {
        192
    } else {
        256
    }
}

/// Target minimum batch size before flushing a far-lane write.
///
/// Deep lag: low min so we do **not** wait to pack a huge batch (tip needs
/// interleaving). Near tip: slightly higher still OK for syscall amortization.
pub(crate) fn min_batch_for_lag(confirm_lag: u32) -> usize {
    if confirm_lag >= 512 {
        8
    } else if confirm_lag >= 128 {
        16
    } else if confirm_lag >= 32 {
        24
    } else {
        16
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

    // Deep lag: short waits only — prefer small frequent writes over packing.
    let deep = confirm_lag >= 512;
    let fill_ms = if deep { 4 } else { 8 };
    let heavy_ms = if deep { 6 } else { 12 };
    let mid_ms = if deep { 3 } else { 4 };
    let dry_ms = if deep { 1 } else { 1 };

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
    use super::{coalesce_wait, max_batch_for_lag, min_batch_for_lag};
    use std::time::Duration;

    #[test]
    fn no_wait_when_batch_already_large() {
        assert_eq!(coalesce_wait(16, 0, 0, 0), Duration::ZERO);
        assert_eq!(coalesce_wait(8, 0, 0, 600), Duration::ZERO);
        assert_eq!(coalesce_wait(64, 500, 5000, 600), Duration::ZERO);
    }

    #[test]
    fn dry_pipeline_near_tip_is_short() {
        assert_eq!(coalesce_wait(1, 0, 0, 0), Duration::from_millis(1));
    }

    #[test]
    fn deep_lag_prefers_small_quanta() {
        // Far ahead of tip: small max, small min — not 256/1024 mega-dumps.
        assert!(max_batch_for_lag(10_000) <= 32);
        assert!(max_batch_for_lag(600) <= 64);
        assert!(min_batch_for_lag(600) <= 8);
        assert!(max_batch_for_lag(0) >= max_batch_for_lag(600));
        assert!(min_batch_for_lag(0) >= min_batch_for_lag(600));
    }

    #[test]
    fn confirm_lag_batch_shape() {
        assert_eq!(min_batch_for_lag(0), 16);
        assert_eq!(min_batch_for_lag(32), 24);
        assert_eq!(min_batch_for_lag(128), 16);
        assert_eq!(min_batch_for_lag(512), 8);
        assert_eq!(max_batch_for_lag(0), 256);
        assert_eq!(max_batch_for_lag(512), 64);
        assert_eq!(max_batch_for_lag(8192), 32);
    }

    #[test]
    fn write_q_fill_to_min_batch() {
        // lag=0 min_batch=16; write_q helps reach min
        assert_eq!(coalesce_wait(1, 40, 0, 0), Duration::from_millis(8));
        // deep lag: shorter fill wait
        assert_eq!(coalesce_wait(1, 40, 0, 600), Duration::from_millis(4));
    }
}
