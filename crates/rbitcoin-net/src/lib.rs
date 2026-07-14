//! Bitcoin P2P: handshake, headers sync, block download/serve (no tx relay).

mod cache;
mod codec;
mod error;
mod peer;
mod seeds;
mod service;

pub use cache::BlockCache;
pub use error::NetError;
pub use peer::local_service_flags;
pub use seeds::{
    default_port, dns_seeds, fixed_seed_hosts, resolve_fixed_seeds, AddrMan,
};
pub use service::{magic_for, NetConfig, P2PHandle, P2PNode};

pub fn crate_name() -> &'static str {
    "rbitcoin-net"
}

/// Default outbound peer count during IBD (libbitcoin-class).
pub const DEFAULT_IBD_OUTBOUND: u32 = 100;

pub fn outbound_for_ibd(ibd: bool) -> u32 {
    if ibd {
        DEFAULT_IBD_OUTBOUND
    } else {
        8
    }
}
