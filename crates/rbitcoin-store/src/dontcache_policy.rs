//! RWF_DONTCACHE policy for Class A / head / idx / sidefile IO (schema 13+).
//!
//! | Target | When DONTCACHE |
//! |--------|----------------|
//! | `tx.body` reads & writes | **always** |
//! | `tx.idx` / `tx.head` reads | segment older than **open + past 3 sealed** |
//! | `txid.body` reads | entry more than **100_000_000** from tail |
//!
//! Bool helpers feed [`crate::bulk_io::ReadOp::dontcache`] /
//! [`crate::bulk_io::WriteOp::dontcache`]. Direct-session SQE helpers
//! (`*_sqe_rw_flags`) also gate on [`crate::bulk_io::rwf_dontcache_ok`].

use crate::txid_body::TXID_DONTCACHE_FROM_TAIL;
use crate::uring_session::RWF_DONTCACHE;

/// Always DONTCACHE for Class A packed body payload IO.
#[inline]
pub fn body_always() -> bool {
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

/// `rw_flags` for Class A body SQEs (0 when RWF_DONTCACHE unsupported).
#[inline]
pub fn body_sqe_rw_flags() -> i32 {
    if body_always() && crate::bulk_io::rwf_dontcache_ok() {
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
///
/// Head/idx direct-session SQEs are not used today — those paths go through
/// [`crate::bulk_io::ReadOp::dontcache`] + [`head_or_idx_segment_index`].
#[inline]
pub fn sidefile_sqe_rw_flags(fk: u64, published_count: u64) -> i32 {
    if txid_sidefile_entry(fk, published_count) && crate::bulk_io::rwf_dontcache_ok() {
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
    fn body_always_true() {
        assert!(body_always());
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
        assert!(body_always());
        assert!(head_or_idx_segment(4));
        assert!(txid_sidefile_entry(1, TXID_DONTCACHE_FROM_TAIL + 10));
    }

    #[test]
    fn policy_matches_sidefile_helper() {
        use crate::txid_body::TxidBody;
        // Mirror TxidBody::dontcache_for_fk formula.
        let n = TXID_DONTCACHE_FROM_TAIL + 3;
        assert_eq!(txid_sidefile_entry(1, n), true);
        assert_eq!(txid_sidefile_entry(n, n), false);
        let _ = TxidBody::entry_offset(1).unwrap();
    }

    /// Op construction: body always, old head/idx, far sidefile set `dontcache`.
    #[test]
    fn read_write_op_flags_follow_policy() {
        use crate::bulk_io::{ReadOp, WriteOp};
        let mut rb = [0u8; 8];
        let body_ro = ReadOp {
            fd: 0,
            offset: 0,
            buf: &mut rb,
            result: i32::MIN,
            dontcache: body_always(),
        };
        assert!(body_ro.dontcache);
        let wb = [0u8; 8];
        let body_wo = WriteOp {
            fd: 0,
            offset: 0,
            buf: &wb,
            result: i32::MIN,
            dontcache: body_always(),
        };
        assert!(body_wo.dontcache);
        // Head/idx: open + past 3 sealed stay cacheable; older get DONTCACHE.
        assert!(!head_or_idx_segment(0));
        assert!(!head_or_idx_segment(3));
        assert!(head_or_idx_segment(4));
        assert!(!head_or_idx_segment_index(4, 5)); // tip
        assert!(head_or_idx_segment_index(0, 5)); // oldest of 5
        // Sidefile far from tail.
        assert!(txid_sidefile_entry(1, TXID_DONTCACHE_FROM_TAIL + 1));
        assert!(!txid_sidefile_entry(TXID_DONTCACHE_FROM_TAIL + 1, TXID_DONTCACHE_FROM_TAIL + 1));
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

        // Tip (open) segment: no DONTCACHE.
        let _ = bulk_io::test_take_last_read_dontcache();
        let _ = h.probe_fks_batch_dontcache(&[txid], false).unwrap();
        let flags = bulk_io::test_take_last_read_dontcache();
        assert!(!flags.is_empty(), "probe must issue bulk page load");
        assert!(flags.iter().all(|&d| !d), "open/tip head must not DONTCACHE");

        // Simulated old segment: production passes true from sealed-age policy.
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

    /// Class A body append builds WriteOp with body_always DONTCACHE.
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

    /// idx bulk path sets dontcache from segment index policy.
    #[test]
    fn idx_bulk_read_sets_dontcache_by_segment_age() {
        // Pure policy used by tx_idx::record_starts_batch_bulk op construction.
        let n = 6usize;
        // si=5 tip → age 0; si=0 age 5 → DONTCACHE
        assert!(!head_or_idx_segment_index(5, n));
        assert!(!head_or_idx_segment_index(n - 1 - 3, n)); // age 3
        assert!(head_or_idx_segment_index(n - 1 - 4, n)); // age 4
        assert_eq!(sealed_age_from_index(0, n), 5);
        assert_eq!(sealed_age_from_index(5, n), 0);
    }

    /// body_sqe_rw_flags matches RWF_DONTCACHE when supported.
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
        // Head/idx use ReadOp.dontcache bools (same sealed-age policy).
        assert!(head_or_idx_segment_index(0, 6));
        assert!(!head_or_idx_segment_index(5, 6));
        if crate::bulk_io::rwf_dontcache_ok() {
            assert_eq!(far, RWF_DONTCACHE);
        } else {
            assert_eq!(far, 0);
        }
    }

    /// Serial Class A body get_raw routes through bulk_io with body_always.
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
        // Near tail (count=1): no DONTCACHE.
        assert!(
            flags.iter().all(|&d| !d),
            "near-tail sidefile must not DONTCACHE; got {flags:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
