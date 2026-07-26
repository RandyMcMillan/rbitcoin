//! Electrum protocol 1.4+ server (confirmed + optional mempool / libre-relay-class).

mod server;

pub use server::{
    electrum_scripthash_hex, run_electrum, ElectrumConfig, ElectrumHandle, TipNotify,
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
