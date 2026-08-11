//! Permanent RWF_DONTCACHE policy: **spend-annotate `tx.body` pwrites only**.
//!
//! | Target | DONTCACHE |
//! |--------|-----------|
//! | Spend-annotate body **pwrite** | **yes** (when capability allows) |
//! | Spend-annotate RMW body **pread** | no |
//! | Class A body append | no |
//! | Confirm load / meta body reads | no |
//! | Generic body reads | no |
//! | Head / idx / sidefile peeks | no |
//!
//! Capability: kernel may reject the flag (ENOTSUP); see
//! [`crate::bulk_io::rwf_dontcache_ok`]. No env multi-mode selection.

use crate::uring_session::RWF_DONTCACHE;

/// Spend-annotate body **pwrite** wants DONTCACHE when the SQE path supports it.
#[inline]
pub fn body_write_spend() -> bool {
    crate::bulk_io::rwf_dontcache_ok()
}

/// `rw_flags` for spend-annotate body **pwrite** SQEs.
#[inline]
pub fn body_sqe_write_flags() -> i32 {
    if body_write_spend() {
        RWF_DONTCACHE
    } else {
        0
    }
}

/// Confirm / load / Class A / cold head-idx / sidefile: never request DONTCACHE.
#[inline]
pub fn body_write() -> bool {
    false
}

/// Historical alias of [`body_write`] (always false under permanent spend-only).
#[inline]
pub fn body_always() -> bool {
    false
}

#[inline]
pub fn body_read_confirm() -> bool {
    false
}

#[inline]
pub fn body_read_spend_rmw() -> bool {
    false
}

#[inline]
pub fn body_read_generic() -> bool {
    false
}

#[inline]
pub fn head_or_idx_segment(_sealed_age_from_tip: u32) -> bool {
    false
}

/// Sealed age from tip for segment index `si` in a vec of `n_segs` (last = tip).
/// Used by winner-age stats; not a DONTCACHE gate.
#[inline]
pub fn sealed_age_from_index(si: usize, n_segs: usize) -> u32 {
    n_segs.saturating_sub(1).saturating_sub(si) as u32
}

#[inline]
pub fn head_or_idx_segment_index(_si: usize, _n_segs: usize) -> bool {
    false
}

/// Annotate RMW pread: never DONTCACHE (pages stay for the following pwrite).
#[inline]
pub fn body_sqe_read_flags() -> i32 {
    if body_read_spend_rmw() {
        RWF_DONTCACHE
    } else {
        0
    }
}

#[inline]
pub fn txid_sidefile_entry(_fk: u64, _published_count: u64) -> bool {
    false
}

#[inline]
pub fn sidefile_sqe_rw_flags(_fk: u64, _published_count: u64) -> i32 {
    0
}

#[inline]
pub fn idx_sqe_rw_flags(_si: usize, _n_segs: usize) -> i32 {
    0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::uring_session::RWF_DONTCACHE;
    use rbitcoin_primitives::Fk;

    #[test]
    fn permanent_spend_write_only() {
        assert!(!body_write());
        assert!(!body_always());
        assert!(!body_read_confirm());
        assert!(!body_read_generic());
        assert!(!body_read_spend_rmw());
        assert!(!head_or_idx_segment(0));
        assert!(!head_or_idx_segment(100));
        assert!(!head_or_idx_segment_index(0, 10));
        assert!(!txid_sidefile_entry(1, 1_000_000_000));
        assert_eq!(body_sqe_read_flags(), 0);
        assert_eq!(sidefile_sqe_rw_flags(1, 1_000_000_000), 0);
        assert_eq!(idx_sqe_rw_flags(0, 10), 0);
        if crate::bulk_io::rwf_dontcache_ok() {
            assert!(body_write_spend());
            assert_eq!(body_sqe_write_flags(), RWF_DONTCACHE);
        } else {
            assert!(!body_write_spend());
            assert_eq!(body_sqe_write_flags(), 0);
        }
    }

    #[test]
    fn all_public_flags_and_sqe_helpers_surface() {
        // Drive every public policy helper so LCOV counts the thin wrappers.
        let _ = body_write_spend();
        let _ = body_sqe_write_flags();
        let _ = body_write();
        let _ = body_always();
        let _ = body_read_confirm();
        let _ = body_read_spend_rmw();
        let _ = body_read_generic();
        assert!(!head_or_idx_segment(0));
        assert!(!head_or_idx_segment(u32::MAX));
        assert_eq!(sealed_age_from_index(0, 0), 0);
        assert_eq!(sealed_age_from_index(0, 1), 0);
        assert_eq!(sealed_age_from_index(0, 3), 2);
        assert_eq!(sealed_age_from_index(2, 3), 0);
        assert!(!head_or_idx_segment_index(0, 4));
        assert!(!head_or_idx_segment_index(3, 4));
        let _ = body_sqe_read_flags();
        assert!(!txid_sidefile_entry(0, 0));
        assert!(!txid_sidefile_entry(1, 100));
        let _ = sidefile_sqe_rw_flags(0, 0);
        let _ = sidefile_sqe_rw_flags(50, 100);
        let _ = idx_sqe_rw_flags(0, 1);
        let _ = idx_sqe_rw_flags(5, 10);
    }

    #[test]
    fn sealed_age_from_index_math() {
        assert_eq!(sealed_age_from_index(5, 6), 0);
        assert_eq!(sealed_age_from_index(0, 6), 5);
        assert_eq!(sealed_age_from_index(2, 6), 3);
    }

    #[test]
    fn class_a_append_never_sets_write_dontcache() {
        use crate::bulk_io;
        use crate::tx_table::{InputRecord, OutputRecord, TxRecord, TxTable};
        use std::sync::atomic::{AtomicU64, Ordering};

        static N: AtomicU64 = AtomicU64::new(0);
        let id = N.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("rbitcoin-dc-append-{id}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let t = TxTable::create(&dir).unwrap();
        let _ = bulk_io::test_take_last_write_dontcache();
        let tx = TxRecord {
            txid: [9u8; 32],
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 1,
            output_start_fk: Fk::NULL,
            output_count: 1,
        };
        let inputs = vec![InputRecord::coinbase(u32::MAX, vec![0x01], vec![])];
        let outs = vec![OutputRecord::unspent(1, vec![0x51])];
        t.put_full_batch_indexed(&[(tx, inputs, outs)], false)
            .unwrap();
        let flags = bulk_io::test_take_last_write_dontcache();
        assert!(!flags.is_empty(), "Class A append must issue bulk WriteOp");
        assert!(
            flags.iter().all(|&d| !d),
            "Class A append must not request DONTCACHE; got {flags:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn generic_body_get_raw_never_dontcache() {
        use crate::bulk_io;
        use crate::tx_table::{InputRecord, OutputRecord, TxRecord, TxTable};
        use std::sync::atomic::{AtomicU64, Ordering};

        static N: AtomicU64 = AtomicU64::new(0);
        let id = N.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("rbitcoin-dc-get-{id}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let t = TxTable::create(&dir).unwrap();
        let tx = TxRecord {
            txid: [7u8; 32],
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 1,
            output_start_fk: Fk::NULL,
            output_count: 1,
        };
        let inputs = vec![InputRecord::coinbase(u32::MAX, vec![0x01], vec![])];
        let outs = vec![OutputRecord::unspent(1, vec![0x51])];
        let fks = t
            .put_full_batch_indexed(&[(tx, inputs, outs)], false)
            .unwrap();
        let _ = bulk_io::test_take_last_read_dontcache();
        let raw = t.body.get_raw(fks[0]).unwrap();
        assert!(!raw.is_empty());
        let flags = bulk_io::test_take_last_read_dontcache();
        assert!(!flags.is_empty());
        assert!(
            flags.iter().all(|&d| !d),
            "get_raw must not DONTCACHE; got {flags:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn confirm_load_and_meta_never_dontcache() {
        use crate::bulk_io;
        use crate::idx_body_pipeline::{run_idx_body_pipeline, BodyMode, IdxBodyJob};
        use crate::tx_table::{InputRecord, OutputRecord, TxRecord, TxTable};
        use std::sync::atomic::{AtomicU64, Ordering};

        static N: AtomicU64 = AtomicU64::new(0);
        let id = N.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("rbitcoin-dc-confirm-{id}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let t = TxTable::create(&dir).unwrap();
        let tx = TxRecord {
            txid: [0xcd; 32],
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 1,
            output_start_fk: Fk::NULL,
            output_count: 1,
        };
        let inputs = vec![InputRecord::coinbase(u32::MAX, vec![0x01], vec![])];
        let outs = vec![OutputRecord::unspent(50, vec![0x51])];
        let fks = t
            .put_full_batch_indexed(&[(tx, inputs, outs)], false)
            .unwrap();

        let mut jobs = vec![IdxBodyJob::new(fks[0].0, None)];
        let _ = bulk_io::test_take_last_read_dontcache();
        run_idx_body_pipeline(&t.body, &mut jobs, BodyMode::Full).unwrap();
        assert!(jobs[0].ok);
        let flags = bulk_io::test_take_last_read_dontcache();
        assert!(!flags.is_empty() && flags.iter().all(|&d| !d));

        let (off, len) = t.body_range(fks[0]).unwrap();
        let decoded = t.get_meta_and_outputs_batch_at(&[(off, len)]).unwrap();
        let abs = off + u64::from(decoded[0].as_ref().unwrap().2[0]);
        let _ = bulk_io::test_take_last_read_dontcache();
        let _ = t.get_spender_meta_at_abs_batch(&[abs]).unwrap();
        let flags = bulk_io::test_take_last_read_dontcache();
        assert!(!flags.is_empty() && flags.iter().all(|&d| !d));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
