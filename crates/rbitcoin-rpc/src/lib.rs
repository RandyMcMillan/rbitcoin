//! JSON-RPC server (node blockchain/control methods — later phase).
//!
//! Wallet RPC is out of scope for the consensus/IBD track.

pub fn crate_name() -> &'static str {
    "rbitcoin-rpc"
}

/// Root HTTP path for the node RPC endpoint.
pub fn node_rpc_path() -> &'static str {
    "/"
}
