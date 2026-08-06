//! RWF_DONTCACHE / page-cache policy for Class A / head / idx / sidefile IO.
//!
//! | Target | When DONTCACHE |
//! |--------|----------------|
//! | `tx.body` **writes** (Class A append, spend annotate pwrite) | **always** — drop after IO |
//! | `tx.body` **reads** on confirm pipeline (load pin + write-stage meta) | **never** — leave pages for pure-write RMW |
//! | `tx.body` **reads** generic (`get_raw` / off-pipeline) | **always** |
//! | `tx.idx` / `tx.head` reads | segment older than **open + past 3 sealed** |
//! | `txid.body` reads | entry more than **100_000_000** from tail |
//!
//! Confirm reuse: load reads parent bodies → structural meta preads → annotate
//! pwrites hit the same pages. If load/meta DONTCACHE, every 9 B pure-write pays
//! a kernel page RMW fault. Write-side DONTCACHE (uring `RWF_DONTCACHE`) is the
//! fd-only equivalent of “drop after use” — not `madvise` (body is not mmap’d).
//!
//! Bool helpers feed [`crate::bulk_io::ReadOp::dontcache`] /
//! [`crate::bulk_io::WriteOp::dontcache`]. Direct-session SQE helpers
//! (`*_sqe_rw_flags`) also gate on [`crate::bulk_io::rwf_dontcache_ok`].

use crate::txid_body::TXID_DONTCACHE_FROM_TAIL;
use crate::uring_session::RWF_DONTCACHE;

/// Class A body **writes**: always request DONTCACHE after the IO completes.
///
/// Covers Class A append and spend-annotate pure-write pwrites. Same intent as
/// `posix_fadvise(DONTNEED)` on the written pages; for io_uring we set
/// `RWF_DONTCACHE` on the SQE (body tables are fd-only, not `madvise`).
#[inline]
pub fn body_write() -> bool {
    true
}

/// Historical alias: “body always DONTCACHE” meant writes + all reads.
/// Prefer [`body_write`] / [`body_read_confirm`] / [`body_read_generic`].
#[inline]
pub fn body_always() -> bool {
    body_write()
}

/// Confirm-pipeline body **reads** (load pin `idx_body` + write-stage meta preads).
///
/// **Do not** DONTCACHE: the same pages are re-touched for pure-write annotate
/// (kernel RMW). Dropping here forces a cold page fault per annotate edge.
#[inline]
pub fn body_read_confirm() -> bool {
    false
}

/// Off-pipeline / generic body **reads** (`get_raw`, RPC reconstruct, etc.).
#[inline]
pub fn body_read_generic() -> bool {
    true
}

/// Head/idx segment: `sealed_age` is how many segments are **newer** than this
/// one (0 = open or newest). DONTCACHE when age > 3 (open + past 3 sealed stay
/// cacheable).
#[inline]
pub fn head_or_idx_segment(sealed_age_from_tip: u32) -> bool {
    sealed_age_from_tip > 3
}

/// Sealed age from tip for segment index `si` in a vec of `n_segs` (last = tip).
#[inline]
pub fn sealed_age_from_index(si: usize, n_segs: usize) -> u32 {
    n_segs.saturating_sub(1).saturating_sub(si) as u32
}

/// Whether a head/idx segment at index `si` should set DONTCACHE.
#[inline]
pub fn head_or_idx_segment_index(si: usize, n_segs: usize) -> bool {
    head_or_idx_segment(sealed_age_from_index(si, n_segs))
}

/// `rw_flags` for Class A body **write** SQEs (annotate / append).
#[inline]
pub fn body_sqe_rw_flags() -> i32 {
    if body_write() && crate::bulk_io::rwf_dontcache_ok() {
        RWF_DONTCACHE
    } else {
        0
    }
}

/// Sidefile entry for create_fk vs published count.
#[inline]
pub fn txid_sidefile_entry(fk: u64, published_count: u64) -> bool {
    if fk == 0 || published_count == 0 {
        return false;
    }
    let tail_lo = published_count
        .saturating_sub(TXID_DONTCACHE_FROM_TAIL)
        .saturating_add(1);
    fk < tail_lo
}

/// `rw_flags` for `txid.body` identity SQEs (0 when unsupported or near tail).
#[inline]
pub fn sidefile_sqe_rw_flags(fk: u64, published_count: u64) -> i32 {
    if txid_sidefile_entry(fk, published_count) && crate::bulk_io::rwf_dontcache_ok() {
        RWF_DONTCACHE
    } else {
        0
    }
}

/// `rw_flags` for plan head-resolve `STAGE_IDX` page SQEs (direct session).
#[inline]
pub fn idx_sqe_rw_flags(si: usize, n_segs: usize) -> i32 {
    if head_or_idx_segment_index(si, n_segs) && crate::bulk_io::rwf_dontcache_ok() {
        RWF_DONTCACHE
    } else {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::uring_session::RWF_DONTCACHE;
    use rbitcoin_primitives::Fk;

    #[test]
    fn body_write_yes_confirm_read_no() {
        assert!(body_write());
        assert!(body_always());
        assert!(!body_read_confirm());
        assert!(body_read_generic());
    }

    #[test]
    fn head_idx_age_cutoff() {
        assert!(!head_or_idx_segment(0));
        assert!(!head_or_idx_segment(3));
        assert!(head_or_idx_segment(4));
        assert!(head_or_idx_segment(100));
    }

    #[test]
    fn sidefile_tail_window() {
        let n = TXID_DONTCACHE_FROM_TAIL + 50;
        assert!(txid_sidefile_entry(1, n));
        assert!(!txid_sidefile_entry(n, n));
        assert!(!txid_sidefile_entry(n - 10, n));
    }

    #[test]
    fn rwf_dontcache_constant_nonzero_on_linux() {
        #[cfg(target_os = "linux")]
        assert_eq!(RWF_DONTCACHE, 0x80);
        #[cfg(not(target_os = "linux"))]
        assert_eq!(RWF_DONTCACHE, 0);
        assert!(body_write());
        assert!(head_or_idx_segment(4));
        assert!(txid_sidefile_entry(1, TXID_DONTCACHE_FROM_TAIL + 10));
    }

    #[test]
    fn policy_matches_sidefile_helper() {
        use crate::txid_body::TxidBody;
        let n = TXID_DONTCACHE_FROM_TAIL + 3;
        assert_eq!(txid_sidefile_entry(1, n), true);
        assert_eq!(txid_sidefile_entry(n, n), false);
        let _ = TxidBody::entry_offset(1).unwrap();
    }

    /// Op construction: body write + generic read DONTCACHE; confirm read does not.
    #[test]
    fn read_write_op_flags_follow_policy() {
        use crate::bulk_io::{ReadOp, WriteOp};
        let mut rb = [0u8; 8];
        let conf_ro = ReadOp {
            fd: 0,
            offset: 0,
            buf: &mut rb,
            result: i32::MIN,
            dontcache: body_read_confirm(),
        };
        assert!(!conf_ro.dontcache);
        let gen_ro = ReadOp {
            fd: 0,
            offset: 0,
            buf: &mut rb,
            result: i32::MIN,
            dontcache: body_read_generic(),
        };
        assert!(gen_ro.dontcache);
        let wb = [0u8; 8];
        let body_wo = WriteOp {
            fd: 0,
            offset: 0,
            buf: &wb,
            result: i32::MIN,
            dontcache: body_write(),
        };
        assert!(body_wo.dontcache);
        assert!(!head_or_idx_segment(0));
        assert!(!head_or_idx_segment(3));
        assert!(head_or_idx_segment(4));
        assert!(!head_or_idx_segment_index(4, 5));
        assert!(head_or_idx_segment_index(0, 5));
        assert!(txid_sidefile_entry(1, TXID_DONTCACHE_FROM_TAIL + 1));
        assert!(!txid_sidefile_entry(
            TXID_DONTCACHE_FROM_TAIL + 1,
            TXID_DONTCACHE_FROM_TAIL + 1
        ));
        #[cfg(target_os = "linux")]
        assert_eq!(RWF_DONTCACHE, 0x80);
    }

    /// Production head probe path builds bulk ReadOps with sealed-age DONTCACHE.
    #[test]
    fn head_probe_load_page_sets_dontcache_via_bulk_io() {
        use crate::address_head::AddressHead;
        use crate::bulk_io;
        use std::sync::atomic::{AtomicU64, Ordering};

        static N: AtomicU64 = AtomicU64::new(0);
        let id = N.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("rbitcoin-head-dc-{id}"));
        let _ = std::fs::remove_file(&path);
        let h = AddressHead::create_with_bits(&path, 12).unwrap();
        let mut txid = [0u8; 32];
        txid[0] = 0xab;
        h.insert(&txid, Fk(1)).unwrap();

        let _ = bulk_io::test_take_last_read_dontcache();
        let _ = h.probe_fks_batch_dontcache(&[txid], false).unwrap();
        let flags = bulk_io::test_take_last_read_dontcache();
        assert!(!flags.is_empty(), "probe must issue bulk page load");
        assert!(flags.iter().all(|&d| !d), "open/tip head must not DONTCACHE");

        let _ = bulk_io::test_take_last_read_dontcache();
        let _ = h.probe_fks_batch_dontcache(&[txid], true).unwrap();
        let flags = bulk_io::test_take_last_read_dontcache();
        assert!(!flags.is_empty());
        assert!(
            flags.iter().all(|&d| d),
            "old head segment reads must set ReadOp.dontcache"
        );

        drop(h);
        crate::address_head::remove_legacy_meta_sidecar(&path);
        let _ = std::fs::remove_file(&path);
    }

    /// Class A body append builds WriteOp with body_write DONTCACHE.
    #[test]
    fn body_append_write_op_sets_dontcache() {
        use crate::bulk_io;
        use crate::tx_table::{InputRecord, OutputRecord, TxRecord, TxTable};
        use std::sync::atomic::{AtomicU64, Ordering};

        static N: AtomicU64 = AtomicU64::new(0);
        let id = N.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("rbitcoin-body-dc-write-{id}"));
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
        t.put_full_batch_indexed(&[(tx, inputs, outs)], false).unwrap();
        let flags = bulk_io::test_take_last_write_dontcache();
        assert!(
            flags.iter().any(|&d| d),
            "Class A body write must set WriteOp.dontcache; got {flags:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn idx_bulk_read_sets_dontcache_by_segment_age() {
        let n = 6usize;
        assert!(!head_or_idx_segment_index(5, n));
        assert!(!head_or_idx_segment_index(n - 1 - 3, n));
        assert!(head_or_idx_segment_index(n - 1 - 4, n));
        assert_eq!(sealed_age_from_index(0, n), 5);
        assert_eq!(sealed_age_from_index(5, n), 0);
    }

    #[test]
    fn body_sqe_flags_match_constant_when_ok() {
        let f = body_sqe_rw_flags();
        if crate::bulk_io::rwf_dontcache_ok() {
            assert_eq!(f, RWF_DONTCACHE);
        } else {
            assert_eq!(f, 0);
        }
    }

    #[test]
    fn sidefile_sqe_flags_follow_policy_and_capability() {
        let n = TXID_DONTCACHE_FROM_TAIL + 10;
        let far = sidefile_sqe_rw_flags(1, n);
        let near = sidefile_sqe_rw_flags(n, n);
        assert_eq!(near, 0);
        assert!(head_or_idx_segment_index(0, 6));
        assert!(!head_or_idx_segment_index(5, 6));
        if crate::bulk_io::rwf_dontcache_ok() {
            assert_eq!(far, RWF_DONTCACHE);
        } else {
            assert_eq!(far, 0);
        }
    }

    /// Serial Class A body get_raw still DONTCACHE (generic / off-pipeline).
    #[test]
    fn body_get_raw_sets_dontcache_via_bulk_io() {
        use crate::bulk_io;
        use crate::tx_table::{InputRecord, OutputRecord, TxRecord, TxTable};
        use std::sync::atomic::{AtomicU64, Ordering};

        static N: AtomicU64 = AtomicU64::new(0);
        let id = N.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("rbitcoin-body-dc-get-{id}"));
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
        assert!(
            flags.iter().any(|&d| d),
            "get_raw body must set ReadOp.dontcache; got {flags:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Confirm load pin path (`idx_body`) must **not** DONTCACHE body reads.
    #[test]
    fn idx_body_pipeline_confirm_load_no_dontcache() {
        use crate::bulk_io;
        use crate::idx_body_pipeline::{run_idx_body_pipeline, BodyMode, IdxBodyJob};
        use crate::tx_table::{InputRecord, OutputRecord, TxRecord, TxTable};
        use std::sync::atomic::{AtomicU64, Ordering};

        static N: AtomicU64 = AtomicU64::new(0);
        let id = N.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("rbitcoin-idx-body-dc-{id}"));
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
        assert!(
            !flags.is_empty() && flags.iter().all(|&d| !d),
            "confirm load body reads must not DONTCACHE; got {flags:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Write-stage spender meta preads must **not** DONTCACHE (same pages as load).
    #[test]
    fn spender_meta_batch_confirm_read_no_dontcache() {
        use crate::bulk_io;
        use crate::tx_table::{InputRecord, OutputRecord, TxRecord, TxTable};
        use std::sync::atomic::{AtomicU64, Ordering};

        static N: AtomicU64 = AtomicU64::new(0);
        let id = N.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("rbitcoin-meta-dc-{id}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let t = TxTable::create(&dir).unwrap();
        let tx = TxRecord {
            txid: [0xee; 32],
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
        let (off, len) = t.body_range(fks[0]).unwrap();
        let decoded = t.get_meta_and_outputs_batch_at(&[(off, len)]).unwrap();
        let abs = off + u64::from(decoded[0].as_ref().unwrap().2[0]);
        let _ = bulk_io::test_take_last_read_dontcache();
        let _ = t.get_spender_meta_at_abs_batch(&[abs]).unwrap();
        let flags = bulk_io::test_take_last_read_dontcache();
        assert!(
            !flags.is_empty() && flags.iter().all(|&d| !d),
            "confirm meta body reads must not DONTCACHE; got {flags:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Sidefile get always builds a bulk ReadOp with per-fk policy flag.
    #[test]
    fn sidefile_get_sets_dontcache_flag_on_read_op() {
        use crate::bulk_io;
        use crate::tx_table::{InputRecord, OutputRecord, TxRecord, TxTable};
        use std::sync::atomic::{AtomicU64, Ordering};

        static N: AtomicU64 = AtomicU64::new(0);
        let id = N.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("rbitcoin-side-dc-get-{id}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let t = TxTable::create(&dir).unwrap();
        let tx = TxRecord {
            txid: [3u8; 32],
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
        let tid = t.txid_sidefile().get(fks[0]).unwrap();
        assert_eq!(tid, [3u8; 32]);
        let flags = bulk_io::test_take_last_read_dontcache();
        assert!(!flags.is_empty(), "sidefile get must issue bulk ReadOp");
        assert!(
            flags.iter().all(|&d| !d),
            "near-tail sidefile must not DONTCACHE; got {flags:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
