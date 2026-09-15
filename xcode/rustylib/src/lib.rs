uniffi::setup_scaffolding!();

use bitcoin::{Address, Network};
use bitcoin::block::Header as BlockHeader;
use bitcoin::consensus::encode::deserialize_hex;
use bitcoin::hashes::{sha256d, Hash};
use rbitcoin_consensus::ChainParams;
use rbitcoin_primitives::Height;
use std::str::FromStr;

#[derive(Debug, uniffi::Error)]
pub enum RustyError {
    InvalidInput,
    ConsensusError,
}

impl std::fmt::Display for RustyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RustyError::InvalidInput => write!(f, "invalid input"),
            RustyError::ConsensusError => write!(f, "consensus error"),
        }
    }
}

fn chain_params_for_network(network: &str) -> Result<ChainParams, RustyError> {
    match network {
        "mainnet" => Ok(ChainParams::mainnet()),
        "testnet" => Ok(ChainParams::testnet()),
        "regtest" => Ok(ChainParams::regtest()),
        "signet" => Ok(ChainParams::signet()),
        _ => Err(RustyError::InvalidInput),
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

// --- rbitcoin-consensus FFI ---

#[uniffi::export]
pub fn check_block_wire(block_hex: String) -> Result<(), RustyError> {
    let bytes = rbitcoin_primitives::hex_decode(&block_hex).map_err(|_| RustyError::InvalidInput)?;
    rbitcoin_consensus::check_block_wire(&bytes).map_err(|_| RustyError::ConsensusError)
}

#[uniffi::export]
pub fn validate_header_on_parent(
    header_hex: String,
    network: String,
    height: u32,
    parent_mtp: u32,
    expected_bits: u32,
) -> Result<(), RustyError> {
    let header: BlockHeader = deserialize_hex(&header_hex).map_err(|_| RustyError::InvalidInput)?;
    let params = chain_params_for_network(&network)?;
    let bits = bitcoin::CompactTarget::from_consensus(expected_bits);
    rbitcoin_consensus::validate_header_on_parent(
        &params,
        Height(height),
        &header,
        parent_mtp,
        bits,
    )
    .map_err(|_| RustyError::ConsensusError)
}

#[uniffi::export]
pub fn block_subsidy(height: u32, network: String) -> Result<u64, RustyError> {
    let params = chain_params_for_network(&network)?;
    let subsidy = rbitcoin_consensus::block_subsidy(height, &params);
    Ok(subsidy as u64)
}

#[uniffi::export]
pub fn verify_tx_scripts(prevouts_hex: Vec<String>, tx_hex: String) -> Result<(), RustyError> {
    let prevouts: Vec<bitcoin::TxOut> = prevouts_hex
        .iter()
        .map(|h| deserialize_hex(h).map_err(|_| RustyError::InvalidInput))
        .collect::<Result<Vec<_>, _>>()?;
    let tx: bitcoin::Transaction = deserialize_hex(&tx_hex).map_err(|_| RustyError::InvalidInput)?;
    rbitcoin_consensus::verify_tx_scripts_detached(prevouts, tx)
        .map_err(|_| RustyError::ConsensusError)
}
