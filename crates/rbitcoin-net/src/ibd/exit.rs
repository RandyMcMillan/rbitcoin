//! IBD exit / catch-up-complete decisions.
//!
//! Pure helpers so the main loop stays readable and mid-chain peer death
//! cannot be mistaken for tip mode (mainnet tip≈161k / horizon≈958k bug).

use super::state::IbdWorkState;

/// Max empty redial rounds while fully dark before giving up mid catch-up.
pub const MAX_DARK_EMPTY_REDIALS: u32 = 4;

/// How far our known work path lags peer-advertised height.
pub(crate) fn header_lag_behind_peers(st: &IbdWorkState, tip_h: u32) -> u32 {
    let known_hi = st
        .max_ready_height
        .max(st.hash_height.values().copied().max().unwrap_or(0))
        .max(tip_h);
    st.max_peer_height.saturating_sub(known_hi)
}

/// WARN cadence for empty `headers` while still lagging peer horizon.
///
/// Streak must **not** be reset after log: a reset every N empties re-fires
/// `streak == 1` and floods the log when many peers reply empty in parallel.
#[inline]
pub(crate) fn should_log_empty_headers_lag(streak: u32) -> bool {
    streak == 1 || (streak > 0 && streak % 64 == 0)
}

/// Re-`getheaders` cadence while lagging (one peer, round-robin) after empty replies.
#[inline]
pub(crate) fn should_rerequest_headers_on_empty_lag(streak: u32) -> bool {
    streak == 1 || (streak > 0 && streak % 8 == 0)
}

/// Work path idle: no ordered hashes, no inflight getdata, archive queue empty.
#[inline]
pub fn path_drained(st: &IbdWorkState, archive_q_count: usize) -> bool {
    st.ordered.is_empty() && st.inflight.is_empty() && archive_q_count == 0
}

/// True when tip is within 2 of peer horizon and archive is not ahead of tip.
#[inline]
pub fn peer_caught_up(st: &IbdWorkState, tip_h: u32) -> bool {
    let lag = header_lag_behind_peers(st, tip_h);
    tip_h > 0 && lag <= 2 && tip_h >= st.max_ready_height
}

/// Success exit after path drain: headers_done or tip near max_peer_height.
#[inline]
pub fn catchup_complete_after_drain(st: &IbdWorkState, tip_h: u32, archive_q_count: usize) -> bool {
    if !path_drained(st, archive_q_count) {
        return false;
    }
    let lag = header_lag_behind_peers(st, tip_h);
    if lag > 2 {
        return false;
    }
    st.headers_done || tip_h >= st.max_peer_height.saturating_sub(2)
}

/// Outcome when every peer slot is dead (or retained empty).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AllPeersDead {
    /// Tip truly caught up — safe to exit IBD as success.
    CatchupComplete,
    /// Still mid-chain; redial in flight — keep looping.
    WaitRedial,
    /// Mid-chain and no redial hope — return Err (do **not** enter tip mode).
    GiveUpMidCatchup,
}

/// Decide what to do when `slots` has no live peers.
pub fn all_peers_dead_action(
    st: &IbdWorkState,
    tip_h: u32,
    archive_q_count: usize,
    redial_in_flight: bool,
    dark_redial_empty: u32,
) -> AllPeersDead {
    let lag = header_lag_behind_peers(st, tip_h);
    let path_ok = path_drained(st, archive_q_count);
    let caught_up = path_ok
        && tip_h > 0
        && lag <= 2
        && (st.headers_done || tip_h >= st.max_peer_height.saturating_sub(2));
    if caught_up {
        return AllPeersDead::CatchupComplete;
    }
    if !redial_in_flight || dark_redial_empty >= MAX_DARK_EMPTY_REDIALS {
        return AllPeersDead::GiveUpMidCatchup;
    }
    AllPeersDead::WaitRedial
}

#[cfg(test)]
mod tests {
    use super::*;

    /// all_peers_dead + catchup_complete edge matrix (mid-chain / caught-up / tip-0).
    #[test]
    fn exit_and_catchup_complete_surface() {
        // Mid-chain, all dead, no redial → give up.
        let mut mid = IbdWorkState::new(Vec::new(), None, Some(161_249));
        mid.max_peer_height = 958_820;
        mid.max_ready_height = 161_000;
        assert_eq!(
            all_peers_dead_action(&mid, 161_249, 0, false, 0),
            AllPeersDead::GiveUpMidCatchup
        );
        assert_eq!(
            all_peers_dead_action(&mid, 161_249, 0, true, 0),
            AllPeersDead::WaitRedial
        );

        // Caught up with no peers → complete.
        let mut done = IbdWorkState::new(Vec::new(), None, Some(100));
        done.max_peer_height = 100;
        done.max_ready_height = 100;
        done.headers_done = true;
        assert_eq!(
            all_peers_dead_action(&done, 100, 0, false, 0),
            AllPeersDead::CatchupComplete
        );

        // Path drain complete requires near peer tip (headers_done alone is not enough).
        let mut path = IbdWorkState::new(Vec::new(), None, Some(2000));
        path.max_peer_height = 313_000;
        path.max_ready_height = 2000;
        path.headers_done = true;
        assert!(!catchup_complete_after_drain(&path, 2000, 0));
        path.max_peer_height = 2001;
        assert!(catchup_complete_after_drain(&path, 2000, 0));

        // Regression: tip=0 + peer horizon must not look complete (false tip mode).
        let mut zero = IbdWorkState::new(Vec::new(), None, Some(0));
        zero.max_peer_height = 958_900;
        zero.max_ready_height = 0;
        zero.headers_done = false;
        assert!(!catchup_complete_after_drain(&zero, 0, 0));
        assert!(!peer_caught_up(&zero, 0));
        assert_eq!(
            all_peers_dead_action(&zero, 0, 0, false, 0),
            AllPeersDead::GiveUpMidCatchup
        );

        // Dark redial budget exhausted while redial still marked in-flight.
        assert_eq!(
            all_peers_dead_action(&mid, 161_249, 0, true, MAX_DARK_EMPTY_REDIALS),
            AllPeersDead::GiveUpMidCatchup
        );

        // path_drained false when archive queue non-empty or ordered non-empty.
        let mut busy = IbdWorkState::new(Vec::new(), None, Some(10));
        busy.max_peer_height = 10;
        busy.max_ready_height = 10;
        busy.headers_done = true;
        assert!(!path_drained(&busy, 1));
        assert!(!catchup_complete_after_drain(&busy, 10, 1));
        use bitcoin::hashes::Hash;
        use bitcoin::BlockHash;
        busy.ordered
            .push_back(BlockHash::from_byte_array([1u8; 32]));
        assert!(!path_drained(&busy, 0));

        // peer_caught_up: tip near horizon and not behind archive.
        let mut near = IbdWorkState::new(Vec::new(), None, Some(100));
        near.max_peer_height = 101;
        near.max_ready_height = 100;
        assert!(peer_caught_up(&near, 100));
        near.max_ready_height = 105;
        assert!(!peer_caught_up(&near, 100));
        assert_eq!(header_lag_behind_peers(&near, 100), 0); // archived ≥ peer
    }

    /// Empty-headers lag WARN/reget cadence (mainnet log flood regression).
    #[test]
    fn empty_headers_lag_rate_limits() {
        assert!(should_log_empty_headers_lag(1));
        assert!(!should_log_empty_headers_lag(2));
        assert!(!should_log_empty_headers_lag(8));
        assert!(!should_log_empty_headers_lag(16));
        assert!(should_log_empty_headers_lag(64));
        assert!(should_log_empty_headers_lag(128));
        // 21 peers × 8 empties would have logged ~5× if streak reset every 8.
        let logs: u32 = (1..=200)
            .filter(|&s| should_log_empty_headers_lag(s))
            .count() as u32;
        assert!(logs <= 5, "expected sparse logs in 200 empties, got {logs}");

        assert!(should_rerequest_headers_on_empty_lag(1));
        assert!(!should_rerequest_headers_on_empty_lag(2));
        assert!(should_rerequest_headers_on_empty_lag(8));
        assert!(should_rerequest_headers_on_empty_lag(16));
        let regets: u32 = (1..=64)
            .filter(|&s| should_rerequest_headers_on_empty_lag(s))
            .count() as u32;
        assert_eq!(regets, 9); // 1,8,16,...,64
    }
}
