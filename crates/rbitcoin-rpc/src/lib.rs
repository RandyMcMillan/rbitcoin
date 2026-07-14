//! JSON-RPC server (Phase 7). Placeholder surface for workspace wiring.

pub fn crate_name() -> &'static str {
    "rbitcoin-rpc"
}

/// Multi-wallet RPC path segment helper.
pub fn wallet_rpc_path(wallet_name: &str) -> String {
    if wallet_name.is_empty() {
        "/".to_string()
    } else {
        format!("/wallet/{}", wallet_name)
    }
}
