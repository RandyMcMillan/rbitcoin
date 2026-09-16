//! Core-class JSON-RPC HTTP server (documented subset — not full Core parity).
//!
//! See `docs/rpc.md` for methods, auth, and permanent gaps.

mod auth;
mod blockstats;
mod methods;
mod server;

pub use auth::{default_socket_path, default_token_path, read_cookie_file, RpcAuth};
pub use blockstats::{
    is_unspendable, percentiles_by_weight, truncated_median, txout_serialized_size,
    PER_UTXO_OVERHEAD,
};
pub use methods::{
    difficulty_rpc_f64, gbt_template, submit_received_block, RpcActive, RpcContext, RpcRegtest,
    SubmitBlockOutcome,
};
pub use server::{run_rpc, RpcConfig, RpcHandle, DEFAULT_RPC_WORK_QUEUE, RPC_MAX_HTTP_BODY};

/// Root HTTP path for the node RPC endpoint.
pub fn node_rpc_path() -> &'static str {
    "/"
}

#[cfg(test)]
mod tests {
    #[test]
    fn node_rpc_path_is_root() {
        assert_eq!(crate::node_rpc_path(), "/");
    }
}
