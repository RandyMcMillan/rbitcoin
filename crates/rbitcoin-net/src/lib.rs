//! Bitcoin P2P: BIP324 v2 transport, headers/blocks, tip follow, tip-mode **tx relay**.

mod cache;
mod chain;
mod codec;
mod error;
mod ibd;
mod msg_decode;
mod peer;
mod seeds;
mod service;
mod tx_relay;
mod v2;

#[cfg(test)]
mod reader_contention;

pub use cache::BlockCache;
pub use chain::{AcceptOutcome, ChainHub, TipEvent};
pub use error::NetError;
pub use ibd::{
    ibd, ibd_cancellable, IbdConfig, DEFAULT_BLOCKS_IN_TRANSIT_PER_PEER,
    DEFAULT_IBD_WINDOW,
};
pub use peer::local_service_flags;
pub use seeds::{
    default_port, dns_seeds, fixed_seed_hosts, resolve_all_seeds, resolve_dns_seeds,
    resolve_fixed_seeds, AddrMan, PeerEntry, PeerFlags,
};
pub use codec::{
    MAX_HEADERS_RESULTS, MAX_INV_SIZE, MAX_LOCATOR_SZ, MAX_PROTOCOL_MESSAGE_LENGTH,
};
pub use service::{magic_for, NetConfig, P2PHandle, P2PNode};
pub use tx_relay::{
    decode_len_prefixed_package, ElectrumMempoolItem, MempoolHub, QueryUtxoProvider,
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
