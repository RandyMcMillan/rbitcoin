//! Node lifecycle, configuration, and process orchestration.

mod cli;
mod config;
mod error;
mod inhibit;
mod run;

pub use cli::cli_main;
pub use config::NodeConfig;
pub use error::NodeError;
pub use inhibit::SuspendInhibit;
pub use run::{run_node, run_p2p, NodeHandle};

pub fn crate_name() -> &'static str {
    "rbitcoin-node"
}
