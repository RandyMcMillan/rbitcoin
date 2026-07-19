//! Electrum protocol 1.4+ server (confirmed + optional mempool / libre-relay-class).

mod server;

pub use server::{
    electrum_scripthash_hex, run_electrum, run_electrum_tls, ElectrumConfig, ElectrumHandle,
    TipNotify,
};

pub fn crate_name() -> &'static str {
    "rbitcoin-electrum"
}
