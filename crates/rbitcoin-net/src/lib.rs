//! Bitcoin P2P: BIP324 v2 transport, headers/blocks, tip follow, tip-mode **tx relay**.

mod cache;
mod chain;
mod codec;
mod compact;
mod error;
mod ibd;
mod msg_decode;
mod peer;
mod peer_dos;
mod seeds;
mod service;
mod tx_relay;
mod v2;

pub use cache::BlockCache;
pub use chain::{log_update_tip, AcceptOutcome, ChainHub, TipEvent};
pub use codec::{MAX_HEADERS_RESULTS, MAX_INV_SIZE, MAX_LOCATOR_SZ, MAX_PROTOCOL_MESSAGE_LENGTH};
pub use error::NetError;
pub use ibd::{
    ibd, ibd_cancellable, IbdConfig, DEFAULT_BLOCKS_IN_TRANSIT_PER_PEER, DEFAULT_IBD_WINDOW,
};
pub use peer::local_service_flags;
pub use peer_dos::{
    inbound_semaphore, max_inbound_from_env, PeerRateLimiter, DEFAULT_MAX_BYTES_PER_SEC,
    DEFAULT_MAX_INBOUND, DEFAULT_MAX_MSGS_PER_SEC, OVERSIZE_BAN_SCORE, RATE_LIMIT_BAN_SCORE,
};
pub use seeds::{
    default_port, dns_seeds, fixed_seed_hosts, resolve_all_seeds, resolve_dns_seeds,
    resolve_fixed_seeds, AddrMan, PeerEntry, PeerFlags,
};
pub use service::{magic_for, NetConfig, P2PHandle, P2PNode};
pub use tx_relay::{
    decode_len_prefixed_package, ElectrumMempoolItem, MempoolAnnounce, MempoolHub,
    QueryUtxoProvider,
};

pub fn crate_name() -> &'static str {
    "rbitcoin-net"
}

/// Default number of **live download peers** during IBD (`IbdConfig::target_peers`
/// and node `--max-outbound` default).
///
/// This is **not** the seed candidate pool size. The node dials a larger sample
/// of seed addresses (typically `2 × target`, clamped) so failed connects still
/// leave enough live peers.
pub const DEFAULT_IBD_TARGET_PEERS: u32 = 16;

/// Alias kept for older call sites; same as [`DEFAULT_IBD_TARGET_PEERS`].
///
/// Historically this was 100 and was easy to confuse with “how many seeds we
/// resolve.” Prefer [`DEFAULT_IBD_TARGET_PEERS`].
pub const DEFAULT_IBD_OUTBOUND: u32 = DEFAULT_IBD_TARGET_PEERS;

/// Suggested live outbound count: IBD target peers vs post-IBD tip-follow budget.
pub fn outbound_for_ibd(ibd: bool) -> u32 {
    if ibd {
        DEFAULT_IBD_TARGET_PEERS
    } else {
        8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crate_name_and_outbound_defaults() {
        assert_eq!(crate_name(), "rbitcoin-net");
        assert_eq!(DEFAULT_IBD_TARGET_PEERS, 16);
        assert_eq!(DEFAULT_IBD_OUTBOUND, DEFAULT_IBD_TARGET_PEERS);
        assert_eq!(outbound_for_ibd(true), 16);
        assert_eq!(outbound_for_ibd(false), 8);
    }
}
