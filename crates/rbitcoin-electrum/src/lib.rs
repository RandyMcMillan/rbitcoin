//! Electrum protocol 1.4+ server for **wallet clients** (confirmed + optional
//! mempool / libre-relay-class).
//!
//! Not a graphical block-explorer backend: clients are expected to already
//! know their scripthashes / txids.

mod server;

pub use server::{
    electrum_scripthash_hex, read_line_capped, run_electrum, ElectrumConfig, ElectrumHandle,
    ServeLimits, TipNotify, DEFAULT_IDLE_TIMEOUT_SECS, DEFAULT_MAX_BROADCAST_HEX,
    DEFAULT_MAX_CONNECTIONS, DEFAULT_MAX_LINE_BYTES, DEFAULT_MAX_REQUEST_BYTES,
    DEFAULT_MAX_SCRIPTHASH_SUBS,
};

pub fn crate_name() -> &'static str {
    "rbitcoin-electrum"
}

#[cfg(test)]
mod tests {
    #[test]
    fn crate_name_stable() {
        assert_eq!(crate::crate_name(), "rbitcoin-electrum");
    }
}
