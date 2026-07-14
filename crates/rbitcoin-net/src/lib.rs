//! Bitcoin P2P: handshake, headers sync, block download/serve, tip follow (no tx relay).

mod cache;
mod chain;
mod codec;
mod error;
mod ibd;
mod peer;
mod seeds;
mod service;

pub use cache::BlockCache;
pub use chain::{AcceptOutcome, ChainHub, TipEvent};
pub use error::NetError;
pub use ibd::{parallel_ibd, parallel_ibd_cancellable, IbdConfig};
pub use peer::local_service_flags;
pub use seeds::{
    default_port, dns_seeds, fixed_seed_hosts, resolve_all_seeds, resolve_dns_seeds,
    resolve_fixed_seeds, AddrMan,
};
pub use codec::{
    MAX_HEADERS_RESULTS, MAX_INV_SIZE, MAX_LOCATOR_SZ, MAX_PROTOCOL_MESSAGE_LENGTH,
};
pub use service::{magic_for, NetConfig, P2PHandle, P2PNode};

pub fn crate_name() -> &'static str {
    "rbitcoin-net"
}

/// Default outbound peer count during IBD (libbitcoin-class concurrent window).
pub const DEFAULT_IBD_OUTBOUND: u32 = 100;

pub fn outbound_for_ibd(ibd: bool) -> u32 {
    if ibd {
        DEFAULT_IBD_OUTBOUND
    } else {
        8
    }
}
