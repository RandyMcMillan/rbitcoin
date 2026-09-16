//! Node lifecycle, configuration, and process orchestration.

mod cli;
mod config;
mod error;
mod inhibit;
mod regtest_rpc;
mod run;

pub use cli::cli_main;
pub use config::{
    inbound_from_maxconnections, parse_minimum_chain_work, ConfApply, DatadirOpts, ListenOpts,
    MempoolOpts, NodeConfig, RpcOpts, CORE_MAXCONNECTIONS_OUTBOUND_RESERVE, DEFAULT_MAX_INBOUND,
};
pub use error::NodeError;
pub use run::{run_node, run_p2p, NodeHandle};
