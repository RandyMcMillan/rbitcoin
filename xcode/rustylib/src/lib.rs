uniffi::setup_scaffolding!();

use bitcoin::{Address, Network};
use bitcoin::hashes::{sha256d, Hash};
use std::str::FromStr;

#[derive(Debug, uniffi::Error)]
pub enum RustyError {
    InvalidInput,
}

impl std::fmt::Display for RustyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RustyError::InvalidInput => write!(f, "invalid input"),
        }
    }
}

#[uniffi::export]
fn rust_hello() -> String {
    "Hello from Rust!".to_string()
}

#[uniffi::export]
pub fn rust_add(a: u32, b: u32) -> u32 {
    a + b
}

#[uniffi::export]
pub fn rbitcoin_version() -> String {
    rbitcoin_primitives::VERSION.to_string()
}

#[uniffi::export]
pub fn rbitcoin_subversion(comments: Vec<String>) -> Result<String, RustyError> {
    let refs: Vec<&str> = comments.iter().map(|s| s.as_str()).collect();
    rbitcoin_primitives::rbitcoin_subversion(rbitcoin_primitives::VERSION, &refs)
        .map_err(|_| RustyError::InvalidInput)
}

#[uniffi::export]
pub fn hex_encode(bytes: Vec<u8>) -> String {
    rbitcoin_primitives::hex_encode(&bytes)
}

#[uniffi::export]
pub fn hex_decode(hex: String) -> Result<Vec<u8>, RustyError> {
    rbitcoin_primitives::hex_decode(&hex).map_err(|_| RustyError::InvalidInput)
}

#[uniffi::export]
pub fn script_sigops(script_hex: String, accurate: bool) -> Result<u64, RustyError> {
    let script = rbitcoin_primitives::hex_decode(&script_hex).map_err(|_| RustyError::InvalidInput)?;
    Ok(rbitcoin_primitives::script_sigop_count(&script, accurate))
}

#[uniffi::export]
pub fn validate_address(address: String) -> bool {
    Address::from_str(&address).is_ok()
}

#[uniffi::export]
pub fn address_network(address: String) -> Result<String, RustyError> {
    let unchecked = Address::from_str(&address).map_err(|_| RustyError::InvalidInput)?;
    if unchecked.is_valid_for_network(Network::Bitcoin) {
        Ok("mainnet".to_string())
    } else if unchecked.is_valid_for_network(Network::Testnet) {
        Ok("testnet".to_string())
    } else if unchecked.is_valid_for_network(Network::Signet) {
        Ok("signet".to_string())
    } else if unchecked.is_valid_for_network(Network::Regtest) {
        Ok("regtest".to_string())
    } else {
        Ok("unknown".to_string())
    }
}

#[uniffi::export]
pub fn hash256(bytes: Vec<u8>) -> String {
    let hash = sha256d::Hash::hash(&bytes);
    hash.to_string()
}
