uniffi::setup_scaffolding!();

use bitcoin::{Address, Network};
use bitcoin::block::Header as BlockHeader;
use bitcoin::consensus::encode::deserialize_hex;
use bitcoin::hashes::{sha256d, Hash};
use rbitcoin_consensus::ChainParams;
use rbitcoin_primitives::Height;
use std::str::FromStr;
use std::sync::Arc;

#[derive(Debug, uniffi::Error)]
pub enum RustyError {
    InvalidInput,
    ConsensusError,
    StoreError,
}

impl std::fmt::Display for RustyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RustyError::InvalidInput => write!(f, "invalid input"),
            RustyError::ConsensusError => write!(f, "consensus error"),
            RustyError::StoreError => write!(f, "store error"),
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

fn parse_hash32(hex: &str) -> Result<[u8; 32], RustyError> {
    rbitcoin_primitives::parse_display_hash32(hex).map_err(|_| RustyError::InvalidInput)
}

// --- Primitives FFI ---

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

// --- Consensus FFI ---

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

// --- Store FFI ---

#[derive(uniffi::Record)]
pub struct FfiHeaderRecord {
    pub prev_fk: u64,
    pub version: i32,
    pub timestamp: u32,
    pub bits: u32,
    pub nonce: u32,
    pub merkle_root: String,
    pub hash: String,
    pub size: u32,
    pub weight: u32,
}

impl From<rbitcoin_store::HeaderRecord> for FfiHeaderRecord {
    fn from(h: rbitcoin_store::HeaderRecord) -> Self {
        Self {
            prev_fk: h.prev_fk.0,
            version: h.version,
            timestamp: h.timestamp,
            bits: h.bits,
            nonce: h.nonce,
            merkle_root: rbitcoin_primitives::hex_encode(&h.merkle_root),
            hash: rbitcoin_primitives::hex_encode(&h.hash),
            size: h.size,
            weight: h.weight,
        }
    }
}

#[derive(uniffi::Record)]
pub struct FfiTxRecord {
    pub txid: String,
    pub version: i32,
    pub locktime: u32,
    pub input_count: u32,
    pub output_count: u32,
}

impl From<rbitcoin_store::TxRecord> for FfiTxRecord {
    fn from(t: rbitcoin_store::TxRecord) -> Self {
        Self {
            txid: rbitcoin_primitives::hex_encode(&t.txid),
            version: t.version,
            locktime: t.locktime,
            input_count: t.input_count,
            output_count: t.output_count,
        }
    }
}

#[derive(uniffi::Object)]
pub struct FfiStore {
    inner: rbitcoin_store::Store,
}

#[uniffi::export]
impl FfiStore {
    #[uniffi::constructor]
    pub fn open(path: String) -> Result<Arc<Self>, RustyError> {
        let store = rbitcoin_store::Store::open(&path).map_err(|_| RustyError::StoreError)?;
        Ok(Arc::new(Self { inner: store }))
    }

    #[uniffi::constructor]
    pub fn create(path: String) -> Result<Arc<Self>, RustyError> {
        let store = rbitcoin_store::Store::create(&path).map_err(|_| RustyError::StoreError)?;
        Ok(Arc::new(Self { inner: store }))
    }

    pub fn tip_height(&self) -> Option<u64> {
        self.inner.tip_height().map(|h| h.0 as u64)
    }

    pub fn header_count(&self) -> u64 {
        self.inner.header_count()
    }

    pub fn get_header_by_hash(&self, hash_hex: String) -> Result<Option<FfiHeaderRecord>, RustyError> {
        let hash = parse_hash32(&hash_hex)?;
        let rec = self.inner.get_header_by_hash(&hash).map_err(|_| RustyError::StoreError)?;
        Ok(rec.map(|(_fk, h)| h.into()))
    }

    pub fn get_tx_by_txid(&self, txid_hex: String) -> Result<Option<FfiTxRecord>, RustyError> {
        let txid = parse_hash32(&txid_hex)?;
        let rec = self.inner.get_tx_by_txid(&txid).map_err(|_| RustyError::StoreError)?;
        Ok(rec.map(|(_, t)| t.into()))
    }
}

// --- Query FFI ---

#[derive(uniffi::Object)]
pub struct FfiQuery {
    inner: rbitcoin_query::Query,
}

#[uniffi::export]
impl FfiQuery {
    #[uniffi::constructor]
    pub fn open_or_create(path: String) -> Result<Arc<Self>, RustyError> {
        let query = rbitcoin_query::Query::open_or_create(&path).map_err(|_| RustyError::StoreError)?;
        Ok(Arc::new(Self { inner: query }))
    }

    pub fn tip_height(&self) -> Option<u64> {
        self.inner.tip_height().map(|h| h.0 as u64)
    }

    pub fn get_tx_by_txid(&self, txid_hex: String) -> Result<Option<FfiTxRecord>, RustyError> {
        let txid = parse_hash32(&txid_hex)?;
        let rec = self.inner.get_tx_by_txid(&txid).map_err(|_| RustyError::StoreError)?;
        Ok(rec.map(|(_, t)| t.into()))
    }

    pub fn is_outpoint_spent(&self, txid_hex: String, vout: u32) -> Result<bool, RustyError> {
        let txid = parse_hash32(&txid_hex)?;
        self.inner.is_outpoint_spent(&txid, vout).map_err(|_| RustyError::StoreError)
    }

    pub fn block_queue_count(&self) -> u64 {
        self.inner.block_queue_count() as u64
    }

    pub fn block_queue_max_height(&self) -> Option<u64> {
        self.inner.block_queue_max_height().map(|h| h as u64)
    }
}

// --- Tests ---

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rust_add() {
        assert_eq!(rust_add(2, 3), 5);
    }

    #[test]
    fn test_hex_roundtrip() {
        let bytes = vec![0xde, 0xad, 0xbe, 0xef];
        let hex = hex_encode(bytes.clone());
        assert_eq!(hex, "deadbeef");
        let decoded = hex_decode(hex).unwrap();
        assert_eq!(decoded, bytes);
    }

    #[test]
    fn test_validate_address_mainnet() {
        assert!(validate_address("bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh".to_string()));
        assert_eq!(
            address_network("bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh".to_string()).unwrap(),
            "mainnet"
        );
    }

    #[test]
    fn test_hash256() {
        let result = hash256(b"hello".to_vec());
        assert!(!result.is_empty());
    }

    #[test]
    fn test_block_subsidy_halving() {
        let pre = block_subsidy(839999, "mainnet".to_string()).unwrap();
        let post = block_subsidy(840000, "mainnet".to_string()).unwrap();
        assert_eq!(pre, 625_000_000);
        assert_eq!(post, 312_500_000);
    }

    #[test]
    fn test_store_create_open() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("store").to_str().unwrap().to_string();
        {
            let store = FfiStore::create(path.clone()).unwrap();
            assert_eq!(store.tip_height(), None);
            assert_eq!(store.header_count(), 0);
        }
        {
            let store = FfiStore::open(path).unwrap();
            assert_eq!(store.tip_height(), None);
        }
    }
}
