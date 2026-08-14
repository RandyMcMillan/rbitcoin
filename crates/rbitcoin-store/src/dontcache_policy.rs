//! Permanent RWF_DONTCACHE policy: **spend-annotate `spent.body` pwrites only**.
//!
//! | Target | DONTCACHE |
//! |--------|-----------|
//! | Spend-annotate body **pwrite** | **yes** (when capability allows) |
//! | Everything else (append, load, head, idx, sidefile) | no |
//!
//! Capability: kernel may reject the flag (ENOTSUP); see
//! [`crate::bulk_io::rwf_dontcache_ok`]. No env multi-mode selection.
//! Wave split is sealed age ([`crate::head_resolve_stats::sealed_age_from_index`]),
//! not this flag.

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

/// Annotate RMW pread: never DONTCACHE (pages stay for the following pwrite).
#[inline]
pub fn body_sqe_read_flags() -> i32 {
    0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::uring_session::RWF_DONTCACHE;

    #[test]
    fn permanent_spend_write_only() {
        assert_eq!(body_sqe_read_flags(), 0);
        if crate::bulk_io::rwf_dontcache_ok() {
            assert!(body_write_spend());
            assert_eq!(body_sqe_write_flags(), RWF_DONTCACHE);
        } else {
            assert!(!body_write_spend());
            assert_eq!(body_sqe_write_flags(), 0);
        }
    }

    #[test]
    fn class_a_append_never_sets_write_dontcache() {
        use crate::bulk_io;
        use crate::tx_table::{InputRecord, OutputRecord, TxRecord, TxTable};
        use rbitcoin_primitives::Fk;
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
        use rbitcoin_primitives::Fk;
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
        use rbitcoin_primitives::Fk;
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

        let (off, _len) = t.spent_range(fks[0]).unwrap();
        let abs = crate::tx_table::spent_abs(off, 0);
        let _ = bulk_io::test_take_last_read_dontcache();
        let _ = t.get_spender_meta_at_abs_batch(&[abs]).unwrap();
        let flags = bulk_io::test_take_last_read_dontcache();
        assert!(!flags.is_empty() && flags.iter().all(|&d| !d));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
