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
        .max_archived_height
        .max(st.hash_height.values().copied().max().unwrap_or(0))
        .max(tip_h);
    st.max_peer_height.saturating_sub(known_hi)
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
    tip_h > 0 && lag <= 2 && tip_h >= st.max_archived_height
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
        mid.max_archived_height = 161_000;
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
        done.max_archived_height = 100;
        done.headers_done = true;
        assert_eq!(
            all_peers_dead_action(&done, 100, 0, false, 0),
            AllPeersDead::CatchupComplete
        );

        // Path drain complete requires near peer tip (headers_done alone is not enough).
        let mut path = IbdWorkState::new(Vec::new(), None, Some(2000));
        path.max_peer_height = 313_000;
        path.max_archived_height = 2000;
        path.headers_done = true;
        assert!(!catchup_complete_after_drain(&path, 2000, 0));
        path.max_peer_height = 2001;
        assert!(catchup_complete_after_drain(&path, 2000, 0));

        // Regression: tip=0 + peer horizon must not look complete (false tip mode).
        let mut zero = IbdWorkState::new(Vec::new(), None, Some(0));
        zero.max_peer_height = 958_900;
        zero.max_archived_height = 0;
        zero.headers_done = false;
        assert!(!catchup_complete_after_drain(&zero, 0, 0));
        assert!(!peer_caught_up(&zero, 0));
        assert_eq!(
            all_peers_dead_action(&zero, 0, 0, false, 0),
            AllPeersDead::GiveUpMidCatchup
        );
    }
}
