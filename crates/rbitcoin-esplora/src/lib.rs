//! Esplora-compatible REST HTTP server (plain HTTP; TLS via reverse proxy).
//!
//! Tip, block header, and tx projection (incl. documented asm/type/address).

mod handlers;
mod script_fields;
mod server;
mod tx_json;

pub use script_fields::{esplora_script_fields, EsploraScriptFields};
pub use server::{run_esplora, EsploraConfig, EsploraHandle};
pub use tx_json::{build_tx_json, tx_status_json};

pub fn crate_name() -> &'static str {
    "rbitcoin-esplora"
}

#[cfg(test)]
mod tests {
    #[test]
    fn crate_name_stable() {
        assert_eq!(crate::crate_name(), "rbitcoin-esplora");
    }
}
