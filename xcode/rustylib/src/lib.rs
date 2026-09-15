uniffi::setup_scaffolding!();

use bitcoin::block::Header as BlockHeader;
use bitcoin::consensus::encode::deserialize_hex;
use bitcoin::hashes::{sha256d, Hash};
use bitcoin::{Address, Network};
use rbitcoin_consensus::ChainParams;
use rbitcoin_primitives::Height;
use std::str::FromStr;
use std::sync::Arc;

#[derive(Debug, uniffi::Error)]
pub enum RustyError {
    InvalidInput,
    ConsensusError,
    StoreError,
    MempoolError,
}

impl std::fmt::Display for RustyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RustyError::InvalidInput => write!(f, "invalid input"),
            RustyError::ConsensusError => write!(f, "consensus error"),
            RustyError::StoreError => write!(f, "store error"),
            RustyError::MempoolError => write!(f, "mempool error"),
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
    let script =
        rbitcoin_primitives::hex_decode(&script_hex).map_err(|_| RustyError::InvalidInput)?;
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
    let bytes =
        rbitcoin_primitives::hex_decode(&block_hex).map_err(|_| RustyError::InvalidInput)?;
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
    let tx: bitcoin::Transaction =
        deserialize_hex(&tx_hex).map_err(|_| RustyError::InvalidInput)?;
    rbitcoin_consensus::verify_tx_scripts_detached(prevouts, tx)
        .map_err(|_| RustyError::ConsensusError)
}

#[uniffi::export]
pub fn virtual_size(weight: u64) -> u64 {
    rbitcoin_consensus::policy::get_virtual_size(weight)
}

#[uniffi::export]
pub fn meets_min_relay_fee(fee_sat: u64, weight: u64) -> bool {
    rbitcoin_consensus::policy::meets_min_relay_fee(fee_sat, weight)
}

#[uniffi::export]
pub fn fee_rate_sat_per_kvb(fee_sat: u64, weight: u64) -> u64 {
    rbitcoin_consensus::policy::fee_rate_sat_per_kvb(fee_sat, weight)
}

#[uniffi::export]
pub fn is_annex_standard(annex_hex: String) -> Result<bool, RustyError> {
    let annex =
        rbitcoin_primitives::hex_decode(&annex_hex).map_err(|_| RustyError::InvalidInput)?;
    Ok(rbitcoin_consensus::policy::is_annex_standard(&annex))
}

#[uniffi::export]
pub fn median_time_past_times(times: Vec<u32>) -> u32 {
    rbitcoin_primitives::median_time_past_times(&times)
}

#[derive(uniffi::Record)]
pub struct FfiTxInfo {
    pub txid: String,
    pub wtxid: String,
    pub version: i32,
    pub locktime: u32,
    pub input_count: u32,
    pub output_count: u32,
    pub weight: u64,
    pub vsize: u64,
}

#[uniffi::export]
pub fn txid_from_hex(tx_hex: String) -> Result<String, RustyError> {
    let tx: bitcoin::Transaction =
        deserialize_hex(&tx_hex).map_err(|_| RustyError::InvalidInput)?;
    Ok(tx.compute_txid().to_string())
}

#[uniffi::export]
pub fn wtxid_from_hex(tx_hex: String) -> Result<String, RustyError> {
    let tx: bitcoin::Transaction =
        deserialize_hex(&tx_hex).map_err(|_| RustyError::InvalidInput)?;
    Ok(tx.compute_wtxid().to_string())
}

#[uniffi::export]
pub fn parse_tx(tx_hex: String) -> Result<FfiTxInfo, RustyError> {
    let tx: bitcoin::Transaction =
        deserialize_hex(&tx_hex).map_err(|_| RustyError::InvalidInput)?;
    let weight = tx.weight().to_wu() as u64;
    Ok(FfiTxInfo {
        txid: tx.compute_txid().to_string(),
        wtxid: tx.compute_wtxid().to_string(),
        version: tx.version.0,
        locktime: tx.lock_time.to_consensus_u32(),
        input_count: tx.input.len() as u32,
        output_count: tx.output.len() as u32,
        weight,
        vsize: weight.div_ceil(4),
    })
}

#[uniffi::export]
pub fn block_hash_from_header(header_hex: String) -> Result<String, RustyError> {
    let header: BlockHeader = deserialize_hex(&header_hex).map_err(|_| RustyError::InvalidInput)?;
    Ok(header.block_hash().to_string())
}

// --- Network FFI ---

fn rbitcoin_network(network: &str) -> Result<rbitcoin_primitives::Network, RustyError> {
    match network {
        "mainnet" => Ok(rbitcoin_primitives::Network::Mainnet),
        "testnet" => Ok(rbitcoin_primitives::Network::Testnet),
        "regtest" => Ok(rbitcoin_primitives::Network::Regtest),
        "signet" => Ok(rbitcoin_primitives::Network::Signet),
        _ => Err(RustyError::InvalidInput),
    }
}

#[uniffi::export]
pub fn p2p_default_port(network: String) -> Result<u16, RustyError> {
    let net = rbitcoin_network(&network)?;
    Ok(rbitcoin_net::default_port(net))
}

#[uniffi::export]
pub fn p2p_dns_seeds(network: String) -> Result<Vec<String>, RustyError> {
    let net = rbitcoin_network(&network)?;
    Ok(rbitcoin_net::dns_seeds(net)
        .iter()
        .map(|s| s.to_string())
        .collect())
}

#[uniffi::export]
pub fn p2p_fixed_seed_hosts(network: String) -> Result<Vec<String>, RustyError> {
    let net = rbitcoin_network(&network)?;
    Ok(rbitcoin_net::fixed_seed_hosts(net)
        .iter()
        .map(|s| s.to_string())
        .collect())
}

#[uniffi::export]
pub fn p2p_target_peers() -> u32 {
    rbitcoin_net::DEFAULT_IBD_TARGET_PEERS
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
            merkle_root: rbitcoin_primitives::hex_encode(h.merkle_root),
            hash: rbitcoin_primitives::hex_encode(h.hash),
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
            txid: rbitcoin_primitives::hex_encode(t.txid),
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

    pub fn get_header_by_hash(
        &self,
        hash_hex: String,
    ) -> Result<Option<FfiHeaderRecord>, RustyError> {
        let hash = parse_hash32(&hash_hex)?;
        let rec = self
            .inner
            .get_header_by_hash(&hash)
            .map_err(|_| RustyError::StoreError)?;
        Ok(rec.map(|(_fk, h)| h.into()))
    }

    pub fn get_tx_by_txid(&self, txid_hex: String) -> Result<Option<FfiTxRecord>, RustyError> {
        let txid = parse_hash32(&txid_hex)?;
        let rec = self
            .inner
            .get_tx_by_txid(&txid)
            .map_err(|_| RustyError::StoreError)?;
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
        let query =
            rbitcoin_query::Query::open_or_create(&path).map_err(|_| RustyError::StoreError)?;
        Ok(Arc::new(Self { inner: query }))
    }

    pub fn tip_height(&self) -> Option<u64> {
        self.inner.tip_height().map(|h| h.0 as u64)
    }

    pub fn get_tx_by_txid(&self, txid_hex: String) -> Result<Option<FfiTxRecord>, RustyError> {
        let txid = parse_hash32(&txid_hex)?;
        let rec = self
            .inner
            .get_tx_by_txid(&txid)
            .map_err(|_| RustyError::StoreError)?;
        Ok(rec.map(|(_, t)| t.into()))
    }

    pub fn is_outpoint_spent(&self, txid_hex: String, vout: u32) -> Result<bool, RustyError> {
        let txid = parse_hash32(&txid_hex)?;
        self.inner
            .is_outpoint_spent(&txid, vout)
            .map_err(|_| RustyError::StoreError)
    }

    pub fn block_queue_count(&self) -> u64 {
        self.inner.block_queue_count() as u64
    }

    pub fn block_queue_max_height(&self) -> Option<u64> {
        self.inner.block_queue_max_height().map(|h| h as u64)
    }
}

// --- Mempool FFI ---

#[derive(uniffi::Record)]
pub struct FfiMempoolMeta {
    pub generation: u64,
    pub slot_cap: u32,
    pub live_count: u32,
}

impl From<rbitcoin_mempool::MempoolMeta> for FfiMempoolMeta {
    fn from(m: rbitcoin_mempool::MempoolMeta) -> Self {
        Self {
            generation: m.generation,
            slot_cap: m.slot_cap,
            live_count: m.live_count,
        }
    }
}

#[derive(uniffi::Record)]
pub struct FfiMempoolSlotStats {
    pub free: u32,
    pub live: u32,
    pub dead: u32,
}

#[derive(uniffi::Object)]
pub struct FfiMempool {
    inner: std::sync::Mutex<rbitcoin_mempool::Mempool>,
}

#[uniffi::export]
impl FfiMempool {
    #[uniffi::constructor]
    pub fn open_or_create(path: String) -> Result<Arc<Self>, RustyError> {
        let mempool = rbitcoin_mempool::Mempool::open_or_create(&path)
            .map_err(|_| RustyError::MempoolError)?;
        Ok(Arc::new(Self {
            inner: std::sync::Mutex::new(mempool),
        }))
    }

    pub fn generation(&self) -> u64 {
        self.inner.lock().unwrap().generation()
    }

    pub fn live_count(&self) -> u32 {
        self.inner.lock().unwrap().live_count()
    }

    pub fn meta(&self) -> FfiMempoolMeta {
        self.inner.lock().unwrap().meta().into()
    }

    pub fn slot_stats(&self) -> FfiMempoolSlotStats {
        let (free, live, dead) = self.inner.lock().unwrap().slot_stats();
        FfiMempoolSlotStats { free, live, dead }
    }

    pub fn flush(&self) -> Result<(), RustyError> {
        self.inner
            .lock()
            .unwrap()
            .flush()
            .map_err(|_| RustyError::MempoolError)
    }
}

// --- Fee Estimation FFI ---

#[uniffi::export]
pub fn fee_bucket_edges() -> Vec<u64> {
    rbitcoin_mempool::FEE_BUCKET_EDGES_SAT_PER_KVB.to_vec()
}

#[uniffi::export]
pub fn fee_bucket_index(rate_sat_per_kvb: u64) -> u32 {
    rbitcoin_mempool::bucket_index(rate_sat_per_kvb) as u32
}

#[uniffi::export]
pub fn fee_bucket_count() -> u32 {
    rbitcoin_mempool::bucket_count() as u32
}

#[uniffi::export]
pub fn fee_capacity_wu(n_blocks: u32) -> u64 {
    rbitcoin_mempool::capacity_wu(n_blocks)
}

#[uniffi::export]
pub fn fee_effective_capacity_wu(n_blocks: u32) -> u64 {
    rbitcoin_mempool::effective_capacity_wu(n_blocks)
}

#[uniffi::export]
pub fn fee_horizon_secs(n_blocks: u32) -> u64 {
    rbitcoin_mempool::horizon_secs(n_blocks)
}

#[uniffi::export]
pub fn fee_default_candidate_rates() -> Vec<u64> {
    rbitcoin_mempool::default_candidate_rates()
}

#[uniffi::export]
pub fn fee_projected_inflow_wu_above(
    inflow_wu_per_s_by_bucket: Vec<u64>,
    rate_sat_per_kvb: u64,
    horizon_secs: u64,
) -> u64 {
    rbitcoin_mempool::projected_inflow_wu_above(
        &inflow_wu_per_s_by_bucket,
        rate_sat_per_kvb,
        horizon_secs,
    )
}

#[uniffi::export]
pub fn fee_min_rate_for_capacity_simple(
    stock_above: u64,
    inflow_wu_per_s_by_bucket: Vec<u64>,
    n_blocks: u32,
    candidate_rates: Vec<u64>,
) -> Option<u64> {
    rbitcoin_mempool::min_rate_for_capacity(
        |_r| stock_above,
        &inflow_wu_per_s_by_bucket,
        n_blocks,
        &candidate_rates,
    )
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
        assert!(validate_address(
            "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh".to_string()
        ));
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

    #[test]
    fn test_mempool_open_or_create() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("mempool").to_str().unwrap().to_string();
        {
            let mempool = FfiMempool::open_or_create(path.clone()).unwrap();
            assert_eq!(mempool.live_count(), 0);
            let meta = mempool.meta();
            assert_eq!(meta.live_count, 0);
            let stats = mempool.slot_stats();
            assert_eq!(stats.live, 0);
        }
        {
            let mempool = FfiMempool::open_or_create(path).unwrap();
            assert_eq!(mempool.live_count(), 0);
        }
    }

    #[test]
    fn test_fee_estimation_basics() {
        let edges = fee_bucket_edges();
        assert!(!edges.is_empty());
        assert_eq!(fee_bucket_count() as usize, edges.len() + 1);
        assert_eq!(fee_bucket_index(100), 0);
        assert_eq!(fee_capacity_wu(1), 4_000_000);
        assert_eq!(fee_effective_capacity_wu(1), 3_800_000);
        assert_eq!(fee_horizon_secs(1), 600);
        let rates = fee_default_candidate_rates();
        assert!(!rates.is_empty());
        let inflow = vec![0u64; fee_bucket_count() as usize];
        let projected = fee_projected_inflow_wu_above(inflow.clone(), 1000, 600);
        assert_eq!(projected, 0);
        let min_rate = fee_min_rate_for_capacity_simple(0, inflow, 1, rates.clone());
        assert!(min_rate.is_some());
    }

    #[test]
    fn test_consensus_policy() {
        assert_eq!(virtual_size(4_000_000), 1_000_000);
        assert!(meets_min_relay_fee(3000, 1000));
        // vsize(1000) = 250; min_relay = 100 sat/kvB
        // 100 * 1000 >= 250 * 100 → true (fee of 100 is enough for 250 vbytes at 100 sat/kvB)
        assert!(meets_min_relay_fee(100, 1000));
        // vsize(4000) = 1000; need 1000 * 100 / 1000 = 100 sat minimum
        assert!(!meets_min_relay_fee(99, 4000));
        let rate = fee_rate_sat_per_kvb(3000, 1000);
        assert!(rate > 0);
    }

    #[test]
    fn test_median_time_past() {
        let times = vec![1000u32, 2000, 1500];
        assert_eq!(median_time_past_times(times), 1500);
    }

    #[test]
    fn test_txid_from_hex() {
        // Coinbase tx from block 0 (genesis)
        let tx_hex = "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff4d04ffff001d0104455468652054696d65732030332f4a616e2f32303039204368616e63656c6c6f72206f6e206272696e6b206f66207365636f6e64206261696c6f757420666f722062616e6b73ffffffff0100f2052a01000000434104678afdb0fe5548271967f1a67130b7105cd6a828e03909a67962e0ea1f61deb649f6bc3f4cef38c4f35504e51ec112de5c384df7ba0b8d578a4c702b6bf11d5fac00000000";
        let txid = txid_from_hex(tx_hex.to_string()).unwrap();
        assert_eq!(
            txid,
            "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b"
        );
    }

    #[test]
    fn test_parse_tx() {
        let tx_hex = "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff4d04ffff001d0104455468652054696d65732030332f4a616e2f32303039204368616e63656c6c6f72206f6e206272696e6b206f66207365636f6e64206261696c6f757420666f722062616e6b73ffffffff0100f2052a01000000434104678afdb0fe5548271967f1a67130b7105cd6a828e03909a67962e0ea1f61deb649f6bc3f4cef38c4f35504e51ec112de5c384df7ba0b8d578a4c702b6bf11d5fac00000000";
        let info = parse_tx(tx_hex.to_string()).unwrap();
        assert_eq!(
            info.txid,
            "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b"
        );
        assert_eq!(info.version, 1);
        assert_eq!(info.input_count, 1);
        assert_eq!(info.output_count, 1);
    }

    #[test]
    fn test_block_hash_from_header() {
        // Genesis block header hex
        let header_hex = "0100000000000000000000000000000000000000000000000000000000000000000000003ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4a29ab5f49ffff001d1dac2b7c";
        let hash = block_hash_from_header(header_hex.to_string()).unwrap();
        assert_eq!(
            hash,
            "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
        );
    }

    #[test]
    fn test_p2p_network_info() {
        assert_eq!(p2p_default_port("mainnet".to_string()).unwrap(), 8333);
        assert_eq!(p2p_default_port("testnet".to_string()).unwrap(), 18333);
        assert_eq!(p2p_default_port("regtest".to_string()).unwrap(), 18444);
        assert_eq!(p2p_default_port("signet".to_string()).unwrap(), 38333);
        let seeds = p2p_dns_seeds("mainnet".to_string()).unwrap();
        assert!(!seeds.is_empty());
        let hosts = p2p_fixed_seed_hosts("mainnet".to_string()).unwrap();
        assert!(!hosts.is_empty());
        assert_eq!(p2p_target_peers(), 16);
    }
}
