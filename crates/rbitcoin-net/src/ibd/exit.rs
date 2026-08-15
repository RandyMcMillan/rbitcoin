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
    streak == 1 || (streak > 0 && streak.is_multiple_of(64))
}

/// Re-`getheaders` cadence while lagging (one peer, round-robin) after empty replies.
#[inline]
pub(crate) fn should_rerequest_headers_on_empty_lag(streak: u32) -> bool {
    streak == 1 || (streak > 0 && streak.is_multiple_of(8))
}

/// After a non-empty `headers` that added nothing to `ordered`.
///
/// Tip chatter is 1–3 already-known hashes from every peer. `live==0` must
/// **not** re-issue getheaders on each of those (that loop was ~1k/s at
/// mainnet tip). Full 2000-header windows and real lag still advance.
pub(crate) fn should_advance_locator_after_known_batch(
    live: usize,
    lag: u32,
    full_window: bool,
    need_headroom: bool,
) -> bool {
    if live == 0 {
        return false;
    }
    full_window || lag > 2 || need_headroom
}

/// How many peers to `getheaders` when the ordered path is empty.
///
/// Mid-sync (`lag > 2`): fan a few so one zombie cannot stall. Near tip
/// (`lag ≤ 2`): never fan — remaining work is inflight/BQ/confirm, not
/// another locator walk. Caller marks `headers_done` when fan is 0.
pub(crate) fn empty_path_header_fan(lag: u32, inflight: usize, alive: usize) -> usize {
    if lag <= 2 {
        return 0;
    }
    if inflight > 0 {
        return 1.min(alive);
    }
    alive.min(4).max(1)
}

/// Full `seed_work_path_from_store` (O(header_count) walk) while empty-lagging.
///
/// Must stay rare: mainnet ~1M headers ≈ 200–300ms per call. Same cadence as
/// [`should_log_empty_headers_lag`] so getheaders can still fan out every 8.
#[inline]
pub(crate) fn should_reseed_work_path_on_empty_lag(streak: u32) -> bool {
    should_log_empty_headers_lag(streak)
}

/// Work path idle: no ordered hashes, no inflight getdata.
#[inline]
pub fn path_drained(st: &IbdWorkState) -> bool {
    st.ordered.is_empty() && st.inflight.is_empty()
}

/// True when tip is within 2 of peer horizon and archive is not ahead of tip.
#[inline]
pub fn peer_caught_up(st: &IbdWorkState, tip_h: u32) -> bool {
    let lag = header_lag_behind_peers(st, tip_h);
    tip_h > 0 && lag <= 2 && tip_h >= st.max_ready_height
}

/// Success exit after path drain: headers_done or tip near max_peer_height.
#[inline]
pub fn catchup_complete_after_drain(st: &IbdWorkState, tip_h: u32) -> bool {
    if !path_drained(st) {
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
    redial_in_flight: bool,
    dark_redial_empty: u32,
) -> AllPeersDead {
    let lag = header_lag_behind_peers(st, tip_h);
    let path_ok = path_drained(st);
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
            all_peers_dead_action(&mid, 161_249, false, 0),
            AllPeersDead::GiveUpMidCatchup
        );
        assert_eq!(
            all_peers_dead_action(&mid, 161_249, true, 0),
            AllPeersDead::WaitRedial
        );

        // Caught up with no peers → complete.
        let mut done = IbdWorkState::new(Vec::new(), None, Some(100));
        done.max_peer_height = 100;
        done.max_ready_height = 100;
        done.headers_done = true;
        assert_eq!(
            all_peers_dead_action(&done, 100, false, 0),
            AllPeersDead::CatchupComplete
        );

        // Path drain complete requires near peer tip (headers_done alone is not enough).
        let mut path = IbdWorkState::new(Vec::new(), None, Some(2000));
        path.max_peer_height = 313_000;
        path.max_ready_height = 2000;
        path.headers_done = true;
        assert!(!catchup_complete_after_drain(&path, 2000));
        path.max_peer_height = 2001;
        assert!(catchup_complete_after_drain(&path, 2000));

        // Regression: tip=0 + peer horizon must not look complete (false tip mode).
        let mut zero = IbdWorkState::new(Vec::new(), None, Some(0));
        zero.max_peer_height = 958_900;
        zero.max_ready_height = 0;
        zero.headers_done = false;
        assert!(!catchup_complete_after_drain(&zero, 0));
        assert!(!peer_caught_up(&zero, 0));
        assert_eq!(
            all_peers_dead_action(&zero, 0, false, 0),
            AllPeersDead::GiveUpMidCatchup
        );

        // Dark redial budget exhausted while redial still marked in-flight.
        assert_eq!(
            all_peers_dead_action(&mid, 161_249, true, MAX_DARK_EMPTY_REDIALS),
            AllPeersDead::GiveUpMidCatchup
        );

        // path_drained false when ordered is non-empty.
        let mut busy = IbdWorkState::new(Vec::new(), None, Some(10));
        busy.max_peer_height = 10;
        busy.max_ready_height = 10;
        busy.headers_done = true;
        assert!(path_drained(&busy));
        use bitcoin::hashes::Hash;
        use bitcoin::BlockHash;
        busy.ordered
            .push_back(BlockHash::from_byte_array([1u8; 32]));
        assert!(!path_drained(&busy));
        assert!(!catchup_complete_after_drain(&busy, 10));

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

        // Full store reseed is sparser than getheaders (O(headers) walk).
        assert!(should_reseed_work_path_on_empty_lag(1));
        assert!(!should_reseed_work_path_on_empty_lag(8));
        assert!(should_reseed_work_path_on_empty_lag(64));
        let reseeds: u32 = (1..=64)
            .filter(|&s| should_reseed_work_path_on_empty_lag(s))
            .count() as u32;
        assert!(
            reseeds < regets,
            "reseed={reseeds} must be rarer than reget={regets}"
        );

        // Already-known 1-header at tip must not re-getheaders (storm).
        assert!(!should_advance_locator_after_known_batch(0, 0, false, true));
        assert!(!should_advance_locator_after_known_batch(
            0, 0, false, false
        ));
        // Live path + full window still advances.
        assert!(should_advance_locator_after_known_batch(
            8_000, 0, true, false
        ));
        assert!(should_advance_locator_after_known_batch(
            8_000, 10, false, false
        ));

        // Empty path near tip: no fan (finish sync / SH / tip follow).
        assert_eq!(empty_path_header_fan(0, 0, 29), 0);
        assert_eq!(empty_path_header_fan(2, 3, 29), 0);
        // Mid-sync empty: fan (cap 4); one peer if getdata already inflight.
        assert_eq!(empty_path_header_fan(100, 0, 29), 4);
        assert_eq!(empty_path_header_fan(100, 5, 29), 1);
        assert_eq!(empty_path_header_fan(100, 0, 2), 2);
    }
}
