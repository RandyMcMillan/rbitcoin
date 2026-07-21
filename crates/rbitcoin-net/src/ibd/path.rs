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

