//! Electrum protocol 1.4+ server (confirmed history first; empty mempool).

mod server;

pub use server::{
    electrum_scripthash_hex, run_electrum, ElectrumConfig, ElectrumHandle, TipNotify,
};

pub fn crate_name() -> &'static str {
    "rbitcoin-electrum"
}
