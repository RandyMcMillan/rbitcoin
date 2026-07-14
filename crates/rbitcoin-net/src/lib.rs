//! P2P networking (Phase 4). Placeholder surface for workspace wiring.

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
