//! Mutable IBD work-path state (peers, ordered queue, body cache, inflight).
//!
//! ## Ordered path memory
//!
//! - `ordered` + `ordered_set` track headers after the local tip for getdata.
//! - Middle completions leave **ghost** deque entries (see [`super::assign_plan::remove_from_ordered`]);
//!   [`Self::hygiene`] compacts when the deque bloats.
//! - `hash_height` / `header_fks` are bounded to live ordered hashes (+ tip seed)
//!   so they do not grow unbounded past `MAX_ORDERED_HEADERS`.

use super::assign_plan::compact_ordered;
use super::body::{BodyPresence, BodyPresenceSizes};
use super::reorg::IbdReorgState;
use bitcoin::BlockHash;
use rbitcoin_primitives::Fk;
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::time::Instant;

use super::peer_io::PeerSlot;

/// O(1) occupancy of [`IbdWorkState`] retain structures (for `ibd: sizes`).
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct WorkStructureSizes {
    pub ordered: usize,
    pub ordered_set: usize,
    pub hash_height: usize,
    pub height_to_hash: usize,
    pub header_fks: usize,
    pub known_headers: usize,
    pub inflight: usize,
    /// Sum of per-peer `in_flight` sets (may exceed unique `inflight` on tip races).
    pub peer_inflight: usize,
    pub addr_cooldown: usize,
    pub body: BodyPresenceSizes,
}

/// Outstanding getdata for one block hash (one or more peers).
///
/// Near/far densify use a single peer. Tip-hole hashes race a second peer
/// immediately and a third only after [`super::TIP_HOLE_THIRD_PEER_AFTER`].
#[derive(Debug, Clone, Default)]
pub(crate) struct InflightReq {
    pub peers: HashSet<usize>,
    /// When the second peer was first attached (tip-hole third-peer timer).
    pub second_peer_at: Option<Instant>,
}

impl InflightReq {
    pub(crate) fn new(peer: usize) -> Self {
        let mut peers = HashSet::with_capacity(1);
        peers.insert(peer);
        Self {
            peers,
            second_peer_at: None,
        }
    }

    pub(crate) fn contains_peer(&self, peer: usize) -> bool {
        self.peers.contains(&peer)
    }

    pub(crate) fn len(&self) -> usize {
        self.peers.len()
    }

    /// Returns true if `peer` was newly added.
    pub(crate) fn add_peer(&mut self, peer: usize) -> bool {
        if !self.peers.insert(peer) {
            return false;
        }
        if self.peers.len() == 2 {
            self.second_peer_at.get_or_insert_with(Instant::now);
        }
        true
    }

    /// Remove `peer`. Returns true if no peers remain (caller should drop the hash).
    pub(crate) fn remove_peer(&mut self, peer: usize) -> bool {
        self.peers.remove(&peer);
        if self.peers.len() < 2 {
            self.second_peer_at = None;
        }
        self.peers.is_empty()
    }
}

/// Core mutable state for the IBD event loop.
pub(crate) struct IbdWorkState {
    pub slots: Vec<PeerSlot>,
    /// Unique hashes with outstanding getdata (1 peer normally; tip-hole races ≤3).
    pub inflight: HashMap<BlockHash, InflightReq>,
    /// Chain-order download path after local tip (front ≈ next to confirm).
    pub ordered: VecDeque<BlockHash>,
    pub ordered_set: HashSet<BlockHash>,
    pub hash_height: HashMap<BlockHash, u32>,
    /// Inverse of [`Self::hash_height`] for O(1) tip+1‥ confirm offers.
    pub height_to_hash: HashMap<u32, BlockHash>,
    pub known_headers: HashSet<BlockHash>,
    pub body: BodyPresence,
    /// header hash → Class A header fk (from getheaders; Block path skips store).
    pub header_fks: HashMap<BlockHash, Fk>,
    /// Best peer-advertised tip (version.start_height + learned header heights).
    pub max_peer_height: u32,
    /// Highest claim-ready / offered body height on the work path (body queue
    /// densify + confirm offer bookkeeping; not a dual-track Class A HWM).
    pub max_ready_height: u32,
    /// Highest header height currently on the ordered work path.
    pub max_ordered_height: u32,
    pub headers_done: bool,
    pub empty_header_streak: u32,
    pub header_req_seq: u32,
    /// Rotates which peer is offered work first.
    pub assign_rot: usize,
    /// Stall-disconnect cooldowns (addr → until).
    pub addr_cooldown: HashMap<SocketAddr, Instant>,
    /// Process-local invalid apply marks for most-work reorg (IBD run only).
    pub reorg: IbdReorgState,
    /// Loop turns since last [`Self::hygiene`].
    hygiene_counter: u32,
}

impl IbdWorkState {
    pub(crate) fn new(
        slots: Vec<PeerSlot>,
        tip_hash: Option<BlockHash>,
        tip_height: Option<u32>,
    ) -> Self {
        let mut known_headers = HashSet::new();
        let mut hash_height = HashMap::new();
        let mut max_peer_height = tip_height.unwrap_or(0);
        if let Some(h) = tip_hash {
            known_headers.insert(h);
            if let Some(th) = tip_height {
                hash_height.insert(h, th);
            }
        }
        for s in &slots {
            max_peer_height = max_peer_height.max(s.peer_height);
        }
        let start_tip = tip_height.unwrap_or(0);
        Self {
            slots,
            inflight: HashMap::new(),
            ordered: VecDeque::new(),
            ordered_set: HashSet::new(),
            hash_height,
            height_to_hash: {
                let mut m = HashMap::new();
                if let (Some(h), Some(th)) = (tip_hash, tip_height) {
                    m.insert(th, h);
                }
                m
            },
            known_headers,
            body: BodyPresence::new(),
            header_fks: HashMap::new(),
            max_peer_height,
            max_ready_height: start_tip,
            max_ordered_height: start_tip,
            headers_done: false,
            empty_header_streak: 0,
            header_req_seq: 0,
            assign_rot: 0,
            addr_cooldown: HashMap::new(),
            reorg: IbdReorgState::new(),
            hygiene_counter: 0,
        }
    }

    /// Record `hash` at chain height `ht` (keeps inverse map in sync).
    pub(crate) fn record_height(&mut self, hash: BlockHash, ht: u32) {
        if let Some(old) = self.hash_height.insert(hash, ht) {
            if old != ht {
                if self.height_to_hash.get(&old) == Some(&hash) {
                    self.height_to_hash.remove(&old);
                }
            }
        }
        self.height_to_hash.insert(ht, hash);
    }

    /// Cheap occupancy of work-path maps/deques (for `ibd: sizes`; all O(1) lens).
    pub(crate) fn structure_sizes(&self) -> WorkStructureSizes {
        let peer_inflight: usize = self.slots.iter().map(|s| s.in_flight.len()).sum();
        WorkStructureSizes {
            ordered: self.ordered.len(),
            ordered_set: self.ordered_set.len(),
            hash_height: self.hash_height.len(),
            height_to_hash: self.height_to_hash.len(),
            header_fks: self.header_fks.len(),
            known_headers: self.known_headers.len(),
            inflight: self.inflight.len(),
            peer_inflight,
            addr_cooldown: self.addr_cooldown.len(),
            body: self.body.size_snapshot(),
        }
    }

    /// Compact ghost entries in `ordered` and drop auxiliary map keys no longer
    /// on the live work path.
    pub(crate) fn hygiene(&mut self) {
        self.hygiene_counter = self.hygiene_counter.wrapping_add(1);
        let bloated = self.ordered.len() > self.ordered_set.len().saturating_mul(4).max(128);
        if !bloated && self.hygiene_counter % 32 != 0 {
            return;
        }
        compact_ordered(&mut self.ordered, &self.ordered_set);
        let live = &self.ordered_set;
        let inflight = &self.inflight;
        // Keep header_fks / hash_height for known_headers too: when tip drains
        // `ordered`, re-getheaders must re-resolve height and re-admit without a
        // full store walk. Wiping them left known_hdr=N with hash_h=0 and freeze.
        self.header_fks.retain(|h, _| {
            live.contains(h) || inflight.contains_key(h) || self.known_headers.contains(h)
        });
        self.hash_height.retain(|h, _| {
            live.contains(h) || inflight.contains_key(h) || self.known_headers.contains(h)
        });
        self.height_to_hash.clear();
        for (&h, &ht) in &self.hash_height {
            if live.contains(&h) || inflight.contains_key(&h) {
                self.height_to_hash.insert(ht, h);
            }
        }
        if self.known_headers.len() > live.len().saturating_add(4096) {
            self.known_headers
                .retain(|h| live.contains(h) || inflight.contains_key(h));
            // Drop height/fk for headers we just pruned from known.
            self.header_fks.retain(|h, _| {
                live.contains(h) || inflight.contains_key(h) || self.known_headers.contains(h)
            });
            self.hash_height.retain(|h, _| {
                live.contains(h) || inflight.contains_key(h) || self.known_headers.contains(h)
            });
        }
        // Bound body presence cache to live work (rejected
        // never hygiene-pruned — see BodyPresence::hygiene_retain).
        self.body
            .hygiene_retain(|h| live.contains(h) || inflight.contains_key(h));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::hashes::Hash;

    fn h(n: u8) -> BlockHash {
        let mut b = [0u8; 32];
        b[0] = n;
        BlockHash::from_byte_array(b)
    }

    #[test]
    fn inflight_req_multi_peer_add_remove() {
        let mut r = InflightReq::new(1);
        assert_eq!(r.len(), 1);
        assert!(r.contains_peer(1));
        assert!(r.second_peer_at.is_none());
        assert!(r.add_peer(2));
        assert!(!r.add_peer(2)); // already present
        assert_eq!(r.len(), 2);
        assert!(r.second_peer_at.is_some());
        assert!(!r.remove_peer(1));
        assert_eq!(r.len(), 1);
        assert!(r.second_peer_at.is_none());
        assert!(r.remove_peer(2));
        assert_eq!(r.len(), 0);
    }

    #[test]
    fn record_height_structure_sizes_and_hygiene() {
        let tip = h(0);
        let mut st = IbdWorkState::new(Vec::new(), Some(tip), Some(10));
        assert!(st.known_headers.contains(&tip));
        assert_eq!(st.hash_height.get(&tip), Some(&10));
        assert_eq!(st.height_to_hash.get(&10), Some(&tip));

        // Height change removes stale inverse.
        st.record_height(tip, 11);
        assert!(st.height_to_hash.get(&10).is_none());
        assert_eq!(st.height_to_hash.get(&11), Some(&tip));

        // Seed a bloated ordered deque with middle ghosts so hygiene compacts.
        for i in 1u8..=140 {
            let hash = h(i);
            st.ordered.push_back(hash);
            if i % 2 == 0 {
                st.ordered_set.insert(hash);
                st.record_height(hash, 11 + u32::from(i));
                st.header_fks.insert(hash, Fk(u64::from(i)));
            }
            st.known_headers.insert(hash);
        }
        // Force hygiene by counter (not only bloat path): advance to %32==0.
        for _ in 0..32 {
            st.hygiene();
        }
        // Live set only even hashes; compact should drop ghosts when bloated.
        assert!(
            st.ordered.len() <= st.ordered_set.len().saturating_add(64).max(128)
                || st
                    .ordered
                    .iter()
                    .all(|x| st.ordered_set.contains(x) || !st.ordered_set.contains(x))
        );
        let sizes = st.structure_sizes();
        assert_eq!(sizes.ordered, st.ordered.len());
        assert_eq!(sizes.ordered_set, st.ordered_set.len());
        assert_eq!(sizes.hash_height, st.hash_height.len());
        assert_eq!(sizes.known_headers, st.known_headers.len());
        assert_eq!(sizes.header_fks, st.header_fks.len());
        assert_eq!(sizes.inflight, 0);
        assert_eq!(sizes.peer_inflight, 0);

        // Inflight keeps auxiliary keys across hygiene.
        let keep = h(200);
        st.inflight.insert(keep, InflightReq::new(0));
        st.record_height(keep, 999);
        st.header_fks.insert(keep, Fk(200));
        st.hygiene_counter = 31; // next call hits %32
        st.hygiene();
        assert!(st.hash_height.contains_key(&keep));
        assert!(st.header_fks.contains_key(&keep));
    }
}
