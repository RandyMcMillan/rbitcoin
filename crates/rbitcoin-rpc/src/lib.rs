//! Core-class JSON-RPC HTTP server (documented subset — not full Core parity).
//!
//! See `docs/rpc.md` for methods, auth, and permanent gaps.

mod auth;
mod methods;
mod server;

pub use auth::{parse_basic_auth, read_cookie_file, resolve_rpc_auth, write_cookie_file, RpcAuth};
pub use methods::{
    dispatch, handle_request, rpc_error, RpcContext, ERR_INVALID_PARAMS, ERR_METHOD_NOT_FOUND,
    ERR_MISC,
};
pub use server::{basic_auth_header, post_rpc, run_rpc, RpcConfig, RpcHandle};

pub fn crate_name() -> &'static str {
    "rbitcoin-rpc"
}

/// Root HTTP path for the node RPC endpoint.
pub fn node_rpc_path() -> &'static str {
    "/"
}

#[cfg(test)]
mod tests {
    #[test]
    fn crate_name_stable() {
        assert_eq!(crate::crate_name(), "rbitcoin-rpc");
        assert_eq!(crate::node_rpc_path(), "/");
    }
}
