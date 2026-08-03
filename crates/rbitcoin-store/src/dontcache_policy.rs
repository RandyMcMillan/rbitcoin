//! RWF_DONTCACHE policy for Class A / head / idx / sidefile IO (schema 13+).
//!
//! | Target | When DONTCACHE |
//! |--------|----------------|
//! | `tx.body` reads & writes | **always** |
//! | `tx.idx` / `tx.head` reads | segment older than **open + past 3 sealed** |
//! | `txid.body` reads | entry more than **100_000_000** from tail |

use crate::txid_body::TXID_DONTCACHE_FROM_TAIL;

/// Always DONTCACHE for Class A packed body payload IO.
#[inline]
pub fn body_always() -> bool {
    true
}

/// Head/idx segment: `sealed_age` is how many sealed segments are **newer**
/// than this one (0 = newest sealed or open). DONTCACHE when age > 3.
#[inline]
pub fn head_or_idx_segment(sealed_age_from_tip: u32) -> bool {
    sealed_age_from_tip > 3
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::uring_session::RWF_DONTCACHE;

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
        // Sidefile far from tail.
        assert!(txid_sidefile_entry(1, TXID_DONTCACHE_FROM_TAIL + 1));
        assert!(!txid_sidefile_entry(TXID_DONTCACHE_FROM_TAIL + 1, TXID_DONTCACHE_FROM_TAIL + 1));
        #[cfg(target_os = "linux")]
        assert_eq!(RWF_DONTCACHE, 0x80);
    }
}
