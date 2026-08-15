//! Node lifecycle, configuration, and process orchestration.

mod cli;
mod config;
mod error;
mod inhibit;
mod regtest_rpc;
mod run;

pub use cli::cli_main;
pub use config::NodeConfig;
pub use error::NodeError;
pub use inhibit::SuspendInhibit;
pub use regtest_rpc::HubRegtest;
pub use run::{run_node, run_p2p, NodeHandle};

pub fn crate_name() -> &'static str {
    "rbitcoin-node"
}

#[cfg(test)]
mod tests {
    #[test]
    fn crate_name_stable() {
        assert_eq!(crate::crate_name(), "rbitcoin-node");
    }
}
