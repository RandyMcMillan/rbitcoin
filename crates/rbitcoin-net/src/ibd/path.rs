//! Ordered work-path seeding and header locator tips.

use super::state::IbdWorkState;
use super::MAX_ORDERED_HEADERS;
use crate::chain::ChainHub;
use bitcoin::hashes::Hash;
use bitcoin::BlockHash;
use rbitcoin_log::{info, warn};
use std::time::Instant;

pub(crate) fn seed_work_path_from_store(st: &mut IbdWorkState, hub: &ChainHub) {
    let Some(tip_hash) = hub.tip_hash() else {
        return;
    };
    let tip_h = hub.tip_height().unwrap_or(0);
    let t0 = Instant::now();
    let path = match hub.query.resume_work_path_after_tip(
        tip_hash.to_byte_array(),
        tip_h,
        MAX_ORDERED_HEADERS,
    ) {
        Ok(p) => p,
        Err(e) => {
            warn!("ibd: resume seed from store failed: {e}");
            return;
        }
    };
    if path.is_empty() {
        return;
    }
    let mut with_body = 0u32;
    let mut contiguous_arch = tip_h;
    let mut arch_prefix = true;
    for e in &path {
        let hash = BlockHash::from_byte_array(e.hash);
        st.known_headers.insert(hash);
        st.record_height(hash, e.height);
        st.header_fks.insert(hash, e.header_fk);
        st.max_ordered_height = st.max_ordered_height.max(e.height);
        if st.ordered_set.insert(hash) {
            st.ordered.push_back(hash);
        }
        if e.has_body {
            st.body.mark_archived(hash);
            with_body = with_body.saturating_add(1);
            if arch_prefix {
                contiguous_arch = e.height;
            }
        } else {
            arch_prefix = false;
        }
    }
    st.max_archived_height = st.max_archived_height.max(contiguous_arch);
    // Peers may still advertise a higher tip; keep header sync open.
    st.headers_done = false;
    info!(
        "ibd: resume seed ordered={} archived_bodies={} archived_to={} (store walk {:?})",
        st.ordered.len(),
        with_body,
        contiguous_arch,
        t0.elapsed()
    );
}

/// Highest hashes on the download path (newest first) for getheaders locators.
pub(crate) fn work_path_tips(st: &IbdWorkState) -> Vec<BlockHash> {
    let mut tips = Vec::with_capacity(8);
    // ordered is tip→far; the back is the highest known header on the path.
    for h in st.ordered.iter().rev().take(4) {
        if st.ordered_set.contains(h) {
            tips.push(*h);
        }
    }
    // Also sample by max height in hash_height if ordered is empty/ghosty.
    if tips.is_empty() {
        if let Some((&h, _)) = st
            .hash_height
            .iter()
            .max_by_key(|(_, &ht)| ht)
        {
            tips.push(h);
        }
    }
    tips
}

#[cfg(test)]
mod tests {
    use super::work_path_tips;
    use super::super::state::IbdWorkState;
    use bitcoin::hashes::Hash;
    use bitcoin::BlockHash;

    fn h(n: u8) -> BlockHash {
        let mut b = [0u8; 32];
        b[0] = n;
        BlockHash::from_byte_array(b)
    }

    #[test]
    fn work_path_tips_from_ordered_newest_first() {
        let mut st = IbdWorkState::new(Vec::new(), None, Some(10));
        // ordered is tip→far (front near tip); tips take from the back (highest).
        for n in 1u8..=6 {
            let hash = h(n);
            st.ordered.push_back(hash);
            st.ordered_set.insert(hash);
            st.record_height(hash, 10 + u32::from(n));
        }
        let tips = work_path_tips(&st);
        assert_eq!(tips.len(), 4);
        assert_eq!(tips[0], h(6));
        assert_eq!(tips[1], h(5));
        assert_eq!(tips[2], h(4));
        assert_eq!(tips[3], h(3));
    }

    #[test]
    fn work_path_tips_skips_ghosts_and_falls_back_to_hash_height() {
        let mut st = IbdWorkState::new(Vec::new(), None, Some(0));
        // Ghost: in deque but not ordered_set.
        st.ordered.push_back(h(1));
        st.ordered.push_back(h(2));
        // No live ordered members → fall back to max height in hash_height.
        st.record_height(h(9), 99);
        st.record_height(h(8), 50);
        let tips = work_path_tips(&st);
        assert_eq!(tips, vec![h(9)]);

        // Empty everything → empty tips.
        let empty = IbdWorkState::new(Vec::new(), None, None);
        assert!(work_path_tips(&empty).is_empty());
    }

    #[test]
    fn work_path_tips_respects_live_set_only() {
        let mut st = IbdWorkState::new(Vec::new(), None, Some(1));
        st.ordered.push_back(h(1));
        st.ordered.push_back(h(2));
        st.ordered.push_back(h(3));
        st.ordered_set.insert(h(1));
        st.ordered_set.insert(h(3)); // h(2) is a middle ghost
        let tips = work_path_tips(&st);
        // rev walk: 3 (live), 2 (ghost skip), 1 (live) — only set members.
        assert_eq!(tips, vec![h(3), h(1)]);
    }

    #[test]
    fn seed_work_path_from_empty_and_genesis_store() {
        use super::seed_work_path_from_store;
        use rbitcoin_consensus::{ChainParams, Milestone};
        use rbitcoin_query::Query;

        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-path-seed-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::create_dir_all(&dir);
        let q = Query::open_or_create(dir.join("store")).unwrap();
        let hub = crate::chain::ChainHub::new(q, ChainParams::regtest(), Milestone::NONE);
        // Empty store: tip_hash is None → seed returns immediately.
        let mut st = IbdWorkState::new(Vec::new(), None, None);
        seed_work_path_from_store(&mut st, &hub);
        assert!(st.ordered.is_empty());

        hub.ensure_genesis().unwrap();
        let mut st2 = IbdWorkState::new(
            Vec::new(),
            hub.tip_hash(),
            hub.tip_height(),
        );
        seed_work_path_from_store(&mut st2, &hub);
        // Resume path after tip may be empty (no headers beyond tip).
        assert!(!st2.headers_done); // always left open for peer tip
        let _ = std::fs::remove_dir_all(dir);
    }
}

