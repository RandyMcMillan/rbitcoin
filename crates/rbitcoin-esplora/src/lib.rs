//! Esplora-compatible REST HTTP server (plain HTTP; TLS via reverse proxy).
//!
//! Phase 1 surface: tip height/hash. Block/tx/address routes land in later steps.

mod server;

pub use server::{run_esplora, EsploraConfig, EsploraHandle};

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
