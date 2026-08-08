//! Esplora-compatible REST HTTP for **wallet clients and APIs** (plain HTTP;
//! TLS via reverse proxy).
//!
//! Serves exact address/scripthash history, tx/block by id, and broadcast—not a
//! graphical block-explorer product (no address-prefix search / explorer UI
//! catalogue APIs).

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
