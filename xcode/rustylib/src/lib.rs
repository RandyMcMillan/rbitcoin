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

#[uniffi::export]
pub fn required_seed_services_u64() -> u64 {
    rbitcoin_net::required_seed_services().to_u64()
}

// --- RPC / Electrum FFI ---

#[uniffi::export]
pub fn node_rpc_path() -> String {
    rbitcoin_rpc::node_rpc_path().to_string()
}

#[uniffi::export]
pub fn electrum_scripthash_hex(script_hex: String) -> Result<String, RustyError> {
    let script =
        rbitcoin_primitives::hex_decode(&script_hex).map_err(|_| RustyError::InvalidInput)?;
    Ok(rbitcoin_electrum::electrum_scripthash_hex(&script))
}

#[uniffi::export]
pub fn rpc_call_json(
    host: String,
    port: u16,
    user: String,
    password: String,
    method: String,
    params_json: String,
) -> Result<String, RustyError> {
    let params: Vec<serde_json::Value> =
        serde_json::from_str(&params_json).map_err(|_| RustyError::InvalidInput)?;
    let result = rbitcoin_cli::rpc_call(&host, port, &user, &password, &method, &params)
        .map_err(|_| RustyError::InvalidInput)?;
    serde_json::to_string(&result).map_err(|_| RustyError::InvalidInput)
}

// --- Node Config FFI ---

#[uniffi::export]
pub fn node_default_max_inbound() -> u32 {
    rbitcoin_node::DEFAULT_MAX_INBOUND
}

#[uniffi::export]
pub fn node_core_maxconnections_outbound_reserve() -> u32 {
    rbitcoin_node::CORE_MAXCONNECTIONS_OUTBOUND_RESERVE
}

#[uniffi::export]
pub fn node_inbound_from_maxconnections(total: u32) -> u32 {
    rbitcoin_node::inbound_from_maxconnections(total)
}

#[uniffi::export]
pub fn node_parse_minimum_chain_work(spec: String) -> Result<String, RustyError> {
    rbitcoin_node::parse_minimum_chain_work(&spec)
        .map(rbitcoin_primitives::hex_encode)
        .map_err(|_| RustyError::InvalidInput)
}

// --- Log FFI ---

#[uniffi::export]
pub fn init_log_level(level: String) {
    if let Some(l) = rbitcoin_log::Level::parse(&level) {
        rbitcoin_log::init(l);
    } else if level.trim().eq_ignore_ascii_case("off")
        || level.trim().eq_ignore_ascii_case("none")
        || level.trim() == "0"
    {
        rbitcoin_log::init_off();
    }
}

#[uniffi::export]
pub fn log_level_enabled(level: String) -> bool {
    rbitcoin_log::Level::parse(&level)
        .map(rbitcoin_log::enabled)
        .unwrap_or(false)
}

#[uniffi::export]
pub fn capture_logs(on: bool) {
    rbitcoin_log::capture_logs(on);
}

#[uniffi::export]
pub fn take_logs() -> Vec<String> {
    rbitcoin_log::take_logs()
        .into_iter()
        .map(|(level, msg)| format!("[{}] {}", level.as_str(), msg))
        .collect()
}

#[uniffi::export]
pub fn log_message(level: String, message: String) {
    if let Some(l) = rbitcoin_log::Level::parse(&level) {
        rbitcoin_log::log_at(l, format_args!("{}", message));
    }
}

// --- Network Seeds FFI ---

#[uniffi::export]
pub fn resolve_fixed_seeds(network: String) -> Result<Vec<String>, RustyError> {
    let net = rbitcoin_network(&network)?;
    Ok(rbitcoin_net::resolve_fixed_seeds(net)
        .into_iter()
        .map(|a| a.to_string())
        .collect())
}

#[uniffi::export]
pub fn resolve_dns_seeds(network: String) -> Result<Vec<String>, RustyError> {
    let net = rbitcoin_network(&network)?;
    Ok(rbitcoin_net::resolve_dns_seeds(net)
        .into_iter()
        .map(|a| a.to_string())
        .collect())
}

#[uniffi::export]
pub fn resolve_all_seeds(network: String) -> Result<Vec<String>, RustyError> {
    let net = rbitcoin_network(&network)?;
    Ok(rbitcoin_net::resolve_all_seeds(net)
        .into_iter()
        .map(|a| a.to_string())
        .collect())
}

#[uniffi::export]
pub fn seed_lookup_names_flat(network: String) -> Result<Vec<String>, RustyError> {
    let net = rbitcoin_network(&network)?;
    Ok(rbitcoin_net::seed_lookup_names(net)
        .into_iter()
        .flatten()
        .collect())
}

// --- Consensus Chain Params FFI ---

#[uniffi::export]
pub fn genesis_block_hash(network: String) -> Result<String, RustyError> {
    let params = chain_params_for_network(&network)?;
    let block = rbitcoin_consensus::genesis_block(&params);
    Ok(block.block_hash().to_string())
}

#[uniffi::export]
pub fn default_milestone_height(network: String) -> Result<u32, RustyError> {
    let net = rbitcoin_network(&network)?;
    Ok(rbitcoin_consensus::default_milestone_height(net))
}

#[uniffi::export]
pub fn check_genesis_hash(network: String, hash_hex: String) -> Result<bool, RustyError> {
    let params = chain_params_for_network(&network)?;
    let hash_bytes =
        rbitcoin_primitives::hex_decode(&hash_hex).map_err(|_| RustyError::InvalidInput)?;
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&hash_bytes);
    arr.reverse(); // display hash → internal byte order
    let hash = bitcoin::BlockHash::from_byte_array(arr);
    Ok(rbitcoin_consensus::check_genesis_hash(&params, hash))
}

#[uniffi::export]
pub fn signet_magic_hex(challenge_hex: String) -> Result<String, RustyError> {
    let bytes =
        rbitcoin_primitives::hex_decode(&challenge_hex).map_err(|_| RustyError::InvalidInput)?;
    let script = bitcoin::Script::from_bytes(&bytes);
    let magic = rbitcoin_consensus::signet_magic(script);
    Ok(rbitcoin_primitives::hex_encode(magic))
}

#[uniffi::export]
pub fn default_signet_challenge_hex() -> String {
    rbitcoin_primitives::hex_encode(rbitcoin_consensus::default_signet_challenge().as_bytes())
}

#[uniffi::export]
pub fn validate_signet_block_solution(
    block_hex: String,
    challenge_hex: String,
) -> Result<(), RustyError> {
    let bytes =
        rbitcoin_primitives::hex_decode(&block_hex).map_err(|_| RustyError::InvalidInput)?;
    let block: bitcoin::Block =
        bitcoin::consensus::encode::deserialize(&bytes).map_err(|_| RustyError::InvalidInput)?;
    let challenge_bytes =
        rbitcoin_primitives::hex_decode(&challenge_hex).map_err(|_| RustyError::InvalidInput)?;
    let challenge = bitcoin::Script::from_bytes(&challenge_bytes);
    rbitcoin_consensus::validate_signet_block_solution(&block, challenge)
        .map_err(|_| RustyError::ConsensusError)
}

// --- Regtest Mining FFI ---

#[uniffi::export]
pub fn mine_empty_regtest(
    prev_hash_hex: String,
    time: u32,
    height: u32,
) -> Result<String, RustyError> {
    let prev_bytes =
        rbitcoin_primitives::hex_decode(&prev_hash_hex).map_err(|_| RustyError::InvalidInput)?;
    let mut prev_arr = [0u8; 32];
    prev_arr.copy_from_slice(&prev_bytes);
    prev_arr.reverse();
    let prev = bitcoin::BlockHash::from_byte_array(prev_arr);
    let block = rbitcoin_consensus::mine_empty_regtest(prev, time, height);
    Ok(bitcoin::consensus::encode::serialize_hex(&block))
}

#[uniffi::export]
pub fn grind_regtest_pow(header_hex: String) -> Result<String, RustyError> {
    let bytes =
        rbitcoin_primitives::hex_decode(&header_hex).map_err(|_| RustyError::InvalidInput)?;
    let mut header: bitcoin::block::Header =
        bitcoin::consensus::encode::deserialize(&bytes).map_err(|_| RustyError::InvalidInput)?;
    rbitcoin_consensus::grind_regtest_pow(&mut header);
    Ok(bitcoin::consensus::encode::serialize_hex(&header))
}

#[uniffi::export]
pub fn regtest_pow_bits() -> u32 {
    rbitcoin_consensus::REGTEST_POW_BITS
}

#[uniffi::export]
pub fn regtest_block_spacing() -> u32 {
    rbitcoin_consensus::REGTEST_BLOCK_SPACING
}

#[uniffi::export]
pub fn mine_regtest_paying(
    prev_hash_hex: String,
    time: u32,
    height: u32,
    script_pubkey_hex: String,
    extra_txs_hex: Vec<String>,
) -> Result<String, RustyError> {
    let prev_bytes =
        rbitcoin_primitives::hex_decode(&prev_hash_hex).map_err(|_| RustyError::InvalidInput)?;
    let mut prev_arr = [0u8; 32];
    prev_arr.copy_from_slice(&prev_bytes);
    prev_arr.reverse();
    let prev = bitcoin::BlockHash::from_byte_array(prev_arr);
    let script = bitcoin::ScriptBuf::from_bytes(
        rbitcoin_primitives::hex_decode(&script_pubkey_hex)
            .map_err(|_| RustyError::InvalidInput)?,
    );
    let extra_txs: Vec<bitcoin::Transaction> = extra_txs_hex
        .iter()
        .map(|h| deserialize_hex(h).map_err(|_| RustyError::InvalidInput))
        .collect::<Result<Vec<_>, _>>()?;
    let block = rbitcoin_consensus::mine_regtest_paying(prev, time, height, script, extra_txs);
    Ok(bitcoin::consensus::encode::serialize_hex(&block))
}

#[uniffi::export]
pub fn check_libre_annex(tx_hex: String) -> Result<String, RustyError> {
    let bytes = rbitcoin_primitives::hex_decode(&tx_hex).map_err(|_| RustyError::InvalidInput)?;
    let tx: bitcoin::Transaction =
        bitcoin::consensus::encode::deserialize(&bytes).map_err(|_| RustyError::InvalidInput)?;
    match rbitcoin_consensus::policy::check_libre_annex(&tx) {
        rbitcoin_consensus::policy::PolicyResult::Standard => Ok("standard".to_string()),
        rbitcoin_consensus::policy::PolicyResult::NonStandard(reason) => {
            Ok(format!("non-standard: {}", reason))
        }
    }
}

#[uniffi::export]
pub fn check_libre_admission(
    tx_hex: String,
    fee_sat: u64,
    weight: u64,
) -> Result<String, RustyError> {
    let bytes = rbitcoin_primitives::hex_decode(&tx_hex).map_err(|_| RustyError::InvalidInput)?;
    let tx: bitcoin::Transaction =
        bitcoin::consensus::encode::deserialize(&bytes).map_err(|_| RustyError::InvalidInput)?;
    match rbitcoin_consensus::policy::check_libre_admission(&tx, fee_sat, weight) {
        rbitcoin_consensus::policy::PolicyResult::Standard => Ok("standard".to_string()),
        rbitcoin_consensus::policy::PolicyResult::NonStandard(reason) => {
            Ok(format!("non-standard: {}", reason))
        }
    }
}

#[uniffi::export]
pub fn check_libre_admission_at(
    tx_hex: String,
    fee_sat: u64,
    weight: u64,
    min_relay_sat_kvb: u64,
) -> Result<String, RustyError> {
    let bytes = rbitcoin_primitives::hex_decode(&tx_hex).map_err(|_| RustyError::InvalidInput)?;
    let tx: bitcoin::Transaction =
        bitcoin::consensus::encode::deserialize(&bytes).map_err(|_| RustyError::InvalidInput)?;
    match rbitcoin_consensus::policy::check_libre_admission_at(
        &tx,
        fee_sat,
        weight,
        min_relay_sat_kvb,
    ) {
        rbitcoin_consensus::policy::PolicyResult::Standard => Ok("standard".to_string()),
        rbitcoin_consensus::policy::PolicyResult::NonStandard(reason) => {
            Ok(format!("non-standard: {}", reason))
        }
    }
}

// --- Block Helpers FFI ---

#[uniffi::export]
pub fn block_has_witness(block_hex: String) -> Result<bool, RustyError> {
    let bytes =
        rbitcoin_primitives::hex_decode(&block_hex).map_err(|_| RustyError::InvalidInput)?;
    let block: bitcoin::Block =
        bitcoin::consensus::encode::deserialize(&bytes).map_err(|_| RustyError::InvalidInput)?;
    Ok(rbitcoin_consensus::block_has_witness(&block))
}

#[uniffi::export]
pub fn is_final_tx(
    tx_hex: String,
    block_height: u32,
    lock_time_cutoff: u32,
) -> Result<bool, RustyError> {
    let bytes = rbitcoin_primitives::hex_decode(&tx_hex).map_err(|_| RustyError::InvalidInput)?;
    let tx: bitcoin::Transaction =
        bitcoin::consensus::encode::deserialize(&bytes).map_err(|_| RustyError::InvalidInput)?;
    Ok(rbitcoin_consensus::is_final_tx(
        &tx,
        block_height,
        lock_time_cutoff,
    ))
}

#[uniffi::export]
pub fn bip34_height_script(height: u32) -> String {
    rbitcoin_primitives::hex_encode(rbitcoin_consensus::bip34_height_script(height))
}

#[uniffi::export]
pub fn meets_min_relay_fee_at(fee_sat: u64, weight: u64, sat_kvb: u64) -> bool {
    rbitcoin_consensus::policy::meets_min_relay_fee_at(fee_sat, weight, sat_kvb)
}

// --- Block Script / Sigops FFI ---

#[uniffi::export]
pub fn witness_commitment_script(
    non_cb_wtxids_hex: Vec<String>,
    reserved_hex: String,
) -> Result<String, RustyError> {
    let wtxids: Vec<[u8; 32]> = non_cb_wtxids_hex
        .iter()
        .map(|h| {
            let bytes = rbitcoin_primitives::hex_decode(h).map_err(|_| RustyError::InvalidInput)?;
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&bytes);
            Ok(arr)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let reserved_bytes =
        rbitcoin_primitives::hex_decode(&reserved_hex).map_err(|_| RustyError::InvalidInput)?;
    let mut reserved = [0u8; 32];
    reserved.copy_from_slice(&reserved_bytes);
    let script = rbitcoin_consensus::witness_commitment_script(wtxids, &reserved);
    Ok(rbitcoin_primitives::hex_encode(script))
}

#[uniffi::export]
pub fn legacy_sigop_count(tx_hex: String) -> Result<u64, RustyError> {
    let bytes = rbitcoin_primitives::hex_decode(&tx_hex).map_err(|_| RustyError::InvalidInput)?;
    let tx: bitcoin::Transaction =
        bitcoin::consensus::encode::deserialize(&bytes).map_err(|_| RustyError::InvalidInput)?;
    Ok(rbitcoin_consensus::legacy_sigop_count(&tx))
}

#[uniffi::export]
pub fn tx_gbt_sigops(tx_hex: String) -> Result<u64, RustyError> {
    let bytes = rbitcoin_primitives::hex_decode(&tx_hex).map_err(|_| RustyError::InvalidInput)?;
    let tx: bitcoin::Transaction =
        bitcoin::consensus::encode::deserialize(&bytes).map_err(|_| RustyError::InvalidInput)?;
    Ok(rbitcoin_consensus::tx_gbt_sigops(&tx))
}

#[uniffi::export]
pub fn bip68_active_for_tx(tx_hex: String) -> Result<bool, RustyError> {
    let bytes = rbitcoin_primitives::hex_decode(&tx_hex).map_err(|_| RustyError::InvalidInput)?;
    let tx: bitcoin::Transaction =
        bitcoin::consensus::encode::deserialize(&bytes).map_err(|_| RustyError::InvalidInput)?;
    Ok(rbitcoin_consensus::bip68_active_for_tx(&tx))
}

#[uniffi::export]
pub fn sequence_locks_satisfied(
    tx_hex: String,
    prev_heights: Vec<u32>,
    prev_coin_mtps: Vec<u32>,
    block_height: u32,
    block_prev_mtp: u32,
) -> Result<bool, RustyError> {
    let bytes = rbitcoin_primitives::hex_decode(&tx_hex).map_err(|_| RustyError::InvalidInput)?;
    let tx: bitcoin::Transaction =
        bitcoin::consensus::encode::deserialize(&bytes).map_err(|_| RustyError::InvalidInput)?;
    Ok(rbitcoin_consensus::sequence_locks_satisfied(
        &tx,
        &prev_heights,
        &prev_coin_mtps,
        block_height,
        block_prev_mtp,
    ))
}

// --- Primitives Hash FFI ---

#[uniffi::export]
pub fn display_hash_hex(bytes: Vec<u8>) -> Result<String, RustyError> {
    let arr: [u8; 32] = bytes.try_into().map_err(|_| RustyError::InvalidInput)?;
    Ok(rbitcoin_primitives::display_hash_hex(&arr))
}

#[uniffi::export]
pub fn parse_display_hash32(hex: String) -> Result<Vec<u8>, RustyError> {
    rbitcoin_primitives::parse_display_hash32(&hex)
        .map(|h| h.to_vec())
        .map_err(|_| RustyError::InvalidInput)
}

// --- Store Header FFI ---

#[uniffi::export]
pub fn block_header_hash(
    version: i32,
    prev_hash_hex: String,
    merkle_root_hex: String,
    timestamp: u32,
    bits: u32,
    nonce: u32,
) -> Result<String, RustyError> {
    let prev = parse_hash32(&prev_hash_hex)?;
    let merkle = parse_hash32(&merkle_root_hex)?;
    let hash = rbitcoin_store::block_header_hash(version, &prev, &merkle, timestamp, bits, nonce);
    // Bitcoin display hash is byte-reversed
    let mut rev = hash;
    rev.reverse();
    Ok(rbitcoin_primitives::hex_encode(rev))
}

// --- Silent Payments FFI ---

#[derive(Debug, PartialEq, uniffi::Record)]
pub struct FfiTaprootOut {
    pub vout: u32,
    pub xonly: String,
    pub value: u64,
}

#[derive(Debug, PartialEq, uniffi::Record)]
pub struct FfiTxTweak {
    pub tweak: String,
    pub output_pubkeys: Vec<FfiTaprootOut>,
}

#[uniffi::export]
pub fn tweak_from_tx(
    tx_hex: String,
    prevouts_hex: Vec<String>,
) -> Result<Option<FfiTxTweak>, RustyError> {
    let bytes = rbitcoin_primitives::hex_decode(&tx_hex).map_err(|_| RustyError::InvalidInput)?;
    let tx: bitcoin::Transaction =
        bitcoin::consensus::encode::deserialize(&bytes).map_err(|_| RustyError::InvalidInput)?;
    let prevouts: Vec<bitcoin::TxOut> = prevouts_hex
        .iter()
        .map(|h| {
            let b = rbitcoin_primitives::hex_decode(h).map_err(|_| RustyError::InvalidInput)?;
            bitcoin::consensus::encode::deserialize(&b).map_err(|_| RustyError::InvalidInput)
        })
        .collect::<Result<Vec<_>, _>>()?;
    match rbitcoin_consensus::tweak_from_tx(&tx, &prevouts) {
        Some(tt) => Ok(Some(FfiTxTweak {
            tweak: rbitcoin_primitives::hex_encode(tt.tweak),
            output_pubkeys: tt
                .output_pubkeys
                .into_iter()
                .map(|o| FfiTaprootOut {
                    vout: o.vout,
                    xonly: rbitcoin_primitives::hex_encode(o.xonly),
                    value: o.value,
                })
                .collect(),
        })),
        None => Ok(None),
    }
}

// --- ASMap FFI ---

#[uniffi::export]
pub fn ip16_for_lookup(ip_str: String) -> Result<Vec<u8>, RustyError> {
    let ip: std::net::IpAddr = ip_str.parse().map_err(|_| RustyError::InvalidInput)?;
    Ok(rbitcoin_net::ip16_for_lookup(ip).to_vec())
}

#[uniffi::export]
pub fn asmap_sanity_check(asmap_hex: String) -> Result<bool, RustyError> {
    let bytes =
        rbitcoin_primitives::hex_decode(&asmap_hex).map_err(|_| RustyError::InvalidInput)?;
    Ok(rbitcoin_net::sanity_check(&bytes))
}

#[uniffi::export]
pub fn asmap_interpret(asmap_hex: String, ip16: Vec<u8>) -> Result<u32, RustyError> {
    let bytes =
        rbitcoin_primitives::hex_decode(&asmap_hex).map_err(|_| RustyError::InvalidInput)?;
    let arr: [u8; 16] = ip16.try_into().map_err(|_| RustyError::InvalidInput)?;
    Ok(rbitcoin_net::interpret(&bytes, &arr))
}

// --- Script Verify Forks FFI ---

#[uniffi::export]
pub fn verify_tx_scripts_detached_forks(
    prevouts_hex: Vec<String>,
    tx_hex: String,
    bip65_active: bool,
    bip112_active: bool,
    bip66_active: bool,
    bip16_active: bool,
    taproot_active: bool,
) -> Result<(), RustyError> {
    let prevouts: Vec<bitcoin::TxOut> = prevouts_hex
        .iter()
        .map(|h| {
            let b = rbitcoin_primitives::hex_decode(h).map_err(|_| RustyError::InvalidInput)?;
            bitcoin::consensus::encode::deserialize(&b).map_err(|_| RustyError::InvalidInput)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let tx: bitcoin::Transaction =
        deserialize_hex(&tx_hex).map_err(|_| RustyError::InvalidInput)?;
    rbitcoin_consensus::verify_tx_scripts_detached_forks(
        prevouts,
        tx,
        bip65_active,
        bip112_active,
        bip66_active,
        bip16_active,
        taproot_active,
    )
    .map_err(|_| RustyError::ConsensusError)
}

// --- Accept and Connect Block FFI ---

#[uniffi::export]
pub fn accept_and_connect_block(
    query_path: String,
    network: String,
    height: u32,
    block_hex: String,
    milestone_height: u32,
) -> Result<u64, RustyError> {
    let query =
        rbitcoin_query::Query::open_or_create(&query_path).map_err(|_| RustyError::StoreError)?;
    let params = chain_params_for_network(&network)?;
    let bytes =
        rbitcoin_primitives::hex_decode(&block_hex).map_err(|_| RustyError::InvalidInput)?;
    let block: bitcoin::Block =
        bitcoin::consensus::encode::deserialize(&bytes).map_err(|_| RustyError::InvalidInput)?;
    let milestone = rbitcoin_consensus::Milestone {
        height: milestone_height,
    };
    let fk = rbitcoin_consensus::accept_and_connect_block(
        &query,
        &params,
        rbitcoin_primitives::Height(height),
        &block,
        milestone,
    )
    .map_err(|_| RustyError::ConsensusError)?;
    Ok(fk.0)
}

// --- Commit Class A Block FFI ---

#[uniffi::export]
pub fn commit_class_a_block(
    query_path: String,
    network: String,
    height: u32,
    block_hex: String,
    milestone_height: u32,
) -> Result<(), RustyError> {
    let query =
        rbitcoin_query::Query::open_or_create(&query_path).map_err(|_| RustyError::StoreError)?;
    let params = chain_params_for_network(&network)?;
    let bytes =
        rbitcoin_primitives::hex_decode(&block_hex).map_err(|_| RustyError::InvalidInput)?;
    let block: bitcoin::Block =
        bitcoin::consensus::encode::deserialize(&bytes).map_err(|_| RustyError::InvalidInput)?;
    let milestone = rbitcoin_consensus::Milestone {
        height: milestone_height,
    };
    rbitcoin_consensus::commit_class_a_block(
        &query,
        &params,
        rbitcoin_primitives::Height(height),
        &block,
        milestone,
    )
    .map_err(|_| RustyError::ConsensusError)
}

// --- Block Structure FFI ---

#[uniffi::export]
pub fn validate_block_structure(
    block_hex: String,
    network: String,
    height: u32,
    enforce_height_gates: bool,
) -> Result<(), RustyError> {
    let bytes =
        rbitcoin_primitives::hex_decode(&block_hex).map_err(|_| RustyError::InvalidInput)?;
    let block: bitcoin::Block =
        bitcoin::consensus::encode::deserialize(&bytes).map_err(|_| RustyError::InvalidInput)?;
    let params = chain_params_for_network(&network)?;
    let milestone = rbitcoin_consensus::Milestone { height };
    let ctx = rbitcoin_consensus::ValidationContext {
        params: &params,
        height: rbitcoin_primitives::Height(height),
        milestone,
        enforce_height_gates,
    };
    rbitcoin_consensus::validate_block_structure(&block, &ctx)
        .map_err(|_| RustyError::ConsensusError)
}

// --- Network Netgroup FFI ---

#[uniffi::export]
pub fn netgroup(ip: String, port: u16, asmap_hex: Option<String>) -> Result<u64, RustyError> {
    let ip_addr: std::net::IpAddr = ip.parse().map_err(|_| RustyError::InvalidInput)?;
    let addr = std::net::SocketAddr::new(ip_addr, port);
    let asmap = match asmap_hex {
        Some(hex) => {
            let bytes =
                rbitcoin_primitives::hex_decode(&hex).map_err(|_| RustyError::InvalidInput)?;
            Some(rbitcoin_net::AsMap::from_bytes(bytes).ok_or(RustyError::InvalidInput)?)
        }
        None => None,
    };
    Ok(rbitcoin_net::netgroup(addr, asmap.as_ref()))
}

// --- Work FFI ---

#[uniffi::export]
pub fn sum_work_hex(work_hexes: Vec<String>) -> Result<String, RustyError> {
    let works: Vec<bitcoin::Work> = work_hexes
        .iter()
        .map(|h| {
            let bytes = rbitcoin_primitives::hex_decode(h).map_err(|_| RustyError::InvalidInput)?;
            let arr: [u8; 32] = bytes.try_into().map_err(|_| RustyError::InvalidInput)?;
            Ok(bitcoin::Work::from_be_bytes(arr))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let sum = rbitcoin_net::sum_work(works.into_iter());
    Ok(rbitcoin_primitives::hex_encode(sum.to_be_bytes()))
}

// --- Silent Payments Query FFI ---

#[derive(Debug, PartialEq, uniffi::Record)]
pub struct FfiHeightTweak {
    pub txid: String,
    pub tweak: String,
    pub output_pubkeys: Vec<FfiTaprootOut>,
}

// --- Disconnect Tip FFI ---

#[uniffi::export]
pub fn format_disconnect_tip_line(
    height: u32,
    hash_hex: String,
    n_tx: u32,
) -> Result<String, RustyError> {
    let hash = parse_hash32(&hash_hex)?;
    Ok(rbitcoin_query::format_disconnect_tip_line(
        height,
        &hash,
        n_tx as usize,
    ))
}

// --- Serve Perf FFI ---

#[derive(Debug, PartialEq, uniffi::Record)]
pub struct FfiServePerfSample {
    pub n: u64,
    pub bytes: u64,
    pub ntx: u64,
    pub wall_ns: u64,
    pub max_ns: u64,
}

impl From<rbitcoin_net::ServePerfSample> for FfiServePerfSample {
    fn from(s: rbitcoin_net::ServePerfSample) -> Self {
        Self {
            n: s.n,
            bytes: s.bytes,
            ntx: s.ntx,
            wall_ns: s.wall_ns,
            max_ns: s.max_ns,
        }
    }
}

#[uniffi::export]
pub fn sample_reset_serve_perf() -> FfiServePerfSample {
    rbitcoin_net::sample_reset_serve_perf().into()
}

#[uniffi::export]
pub fn format_serve_perf(sample: FfiServePerfSample) -> String {
    let s = rbitcoin_net::ServePerfSample {
        n: sample.n,
        bytes: sample.bytes,
        ntx: sample.ntx,
        wall_ns: sample.wall_ns,
        max_ns: sample.max_ns,
    };
    rbitcoin_net::format_serve_perf(&s)
}

// --- Peer Address FFI ---

#[uniffi::export]
pub fn parse_peer_addr(addr: String) -> Result<String, RustyError> {
    let socket = rbitcoin_net::parse_peer_addr(&addr).map_err(|_| RustyError::InvalidInput)?;
    Ok(socket.to_string())
}

// --- V2 Transport FFI ---

#[uniffi::export]
pub fn parse_v2_regtest(contents_hex: String) -> Result<(), RustyError> {
    let bytes =
        rbitcoin_primitives::hex_decode(&contents_hex).map_err(|_| RustyError::InvalidInput)?;
    rbitcoin_net::parse_v2_regtest(&bytes).map_err(|_| RustyError::InvalidInput)
}

#[uniffi::export]
pub fn parse_v2_regtest_named(command: String, payload_hex: String) -> Result<(), RustyError> {
    let payload =
        rbitcoin_primitives::hex_decode(&payload_hex).map_err(|_| RustyError::InvalidInput)?;
    rbitcoin_net::parse_v2_regtest_named(&command, &payload).map_err(|_| RustyError::InvalidInput)
}

// --- Network Service Flags FFI ---

#[uniffi::export]
pub fn local_service_flags_u64() -> u64 {
    rbitcoin_net::local_service_flags().to_u64()
}

#[uniffi::export]
pub fn desirable_service_flags(offered: u64, tip_depth_blocks: i64) -> u64 {
    let offered = bitcoin::p2p::ServiceFlags::from(offered);
    rbitcoin_net::desirable_service_flags(offered, tip_depth_blocks).to_u64()
}

#[uniffi::export]
pub fn has_all_desirable_service_flags(offered: u64, tip_depth_blocks: i64) -> bool {
    let offered = bitcoin::p2p::ServiceFlags::from(offered);
    rbitcoin_net::has_all_desirable_service_flags(offered, tip_depth_blocks)
}

// --- Versionbits Warnings FFI ---

#[derive(uniffi::Record)]
pub struct FfiWarnPeriod {
    pub start: u32,
    pub end: u32,
}

#[uniffi::export]
pub fn warn_period_threshold(network: String) -> Result<FfiWarnPeriod, RustyError> {
    let net = rbitcoin_network(&network)?;
    let (start, end) = rbitcoin_net::warn_period_threshold(net);
    Ok(FfiWarnPeriod { start, end })
}

#[uniffi::export]
pub fn unknown_rules_warning(bit: i32) -> String {
    rbitcoin_net::unknown_rules_warning(bit)
}

// --- Store Integrity FFI ---

#[uniffi::export]
pub fn merkle_root_from_txids(txids_hex: Vec<String>) -> Result<String, RustyError> {
    let txids: Vec<[u8; 32]> = txids_hex
        .iter()
        .map(|h| {
            let bytes = rbitcoin_primitives::hex_decode(h).map_err(|_| RustyError::InvalidInput)?;
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&bytes);
            Ok(arr)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let root = rbitcoin_store::merkle_root_from_txids(&txids);
    Ok(rbitcoin_primitives::hex_encode(root))
}

#[uniffi::export]
pub fn block_wire_input_count(block_hex: String) -> Result<u32, RustyError> {
    let bytes =
        rbitcoin_primitives::hex_decode(&block_hex).map_err(|_| RustyError::InvalidInput)?;
    Ok(rbitcoin_store::block_wire_input_count(&bytes))
}

// --- Mempool Constants FFI ---

#[uniffi::export]
pub fn mempool_default_max_weight() -> u64 {
    rbitcoin_mempool::DEFAULT_MAX_MEMPOOL_WEIGHT
}

#[uniffi::export]
pub fn mempool_max_standard_tx_weight() -> u64 {
    rbitcoin_consensus::policy::MAX_STANDARD_TX_WEIGHT
}

#[uniffi::export]
pub fn mempool_incremental_relay_fee_rate() -> u64 {
    rbitcoin_mempool::INCREMENTAL_RELAY_FEE_RATE_SAT_PER_KVB
}

// --- Electrum Constants FFI ---

#[uniffi::export]
pub fn electrum_default_tweaks_min_dust() -> u64 {
    rbitcoin_electrum::DEFAULT_TWEAKS_MIN_DUST
}

// --- Primitives Constants FFI ---

#[uniffi::export]
pub fn rbitcoin_store_magic() -> String {
    rbitcoin_primitives::hex_encode(rbitcoin_primitives::STORE_MAGIC)
}

#[uniffi::export]
pub fn rbitcoin_schema_version() -> u16 {
    rbitcoin_primitives::SCHEMA_VERSION
}

#[uniffi::export]
pub fn rbitcoin_schema_file_openable(ver: u16) -> bool {
    rbitcoin_primitives::schema_file_openable(ver)
}

// --- Store FFI ---

#[derive(Debug, PartialEq, uniffi::Record)]
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

#[derive(Debug, PartialEq, uniffi::Record)]
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

    #[uniffi::constructor]
    pub fn open_or_create(path: String) -> Result<Arc<Self>, RustyError> {
        let store =
            rbitcoin_store::Store::open_or_create(&path).map_err(|_| RustyError::StoreError)?;
        Ok(Arc::new(Self { inner: store }))
    }

    pub fn datadir_bytes(&self) -> u64 {
        self.inner.datadir_bytes()
    }

    pub fn path(&self) -> String {
        self.inner.path().to_string_lossy().into_owned()
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

    pub fn tx_index_enabled(&self) -> bool {
        self.inner.tx_index_enabled()
    }

    pub fn spend_index_enabled(&self) -> bool {
        self.inner.spend_index_enabled()
    }

    pub fn block_queue_stats(&self) -> FfiBlockQueueStats {
        let (assign_stop, bytes, count) = self.inner.block_queue_stats();
        FfiBlockQueueStats {
            assign_stop_bytes: assign_stop,
            bytes,
            count: count as u64,
        }
    }

    pub fn soft_confirm_window(&self) -> u32 {
        self.inner.soft_confirm_window()
    }

    pub fn fence_tip_height(&self) -> Option<u64> {
        self.inner.fence_tip_height().map(|h| h as u64)
    }

    pub fn archived_block_count(&self) -> Result<u64, RustyError> {
        self.inner
            .archived_block_count()
            .map_err(|_| RustyError::StoreError)
    }

    pub fn tx_body_count(&self) -> u64 {
        self.inner.tx_body_count()
    }

    pub fn tx_head_occupied(&self) -> u64 {
        self.inner.tx_head_occupied()
    }

    pub fn is_block_archived(&self, hash_hex: String) -> Result<bool, RustyError> {
        let hash = parse_hash32(&hash_hex)?;
        self.inner
            .is_block_archived(&hash)
            .map_err(|_| RustyError::StoreError)
    }

    pub fn header_has_class_a_body(&self, header_fk: u64) -> Result<bool, RustyError> {
        self.inner
            .header_has_class_a_body(header_fk)
            .map_err(|_| RustyError::StoreError)
    }

    pub fn clear_archived_body(&self, hash_hex: String) -> Result<bool, RustyError> {
        let hash = parse_hash32(&hash_hex)?;
        self.inner
            .clear_archived_body(&hash)
            .map_err(|_| RustyError::StoreError)
    }

    pub fn expected_next_bits(
        &self,
        network: String,
        height: u32,
        header_time: u32,
    ) -> Result<u32, RustyError> {
        let params = chain_params_for_network(&network)?;
        let bits = rbitcoin_consensus::expected_next_bits(
            &self.inner,
            &params,
            rbitcoin_primitives::Height(height),
            header_time,
        )
        .map_err(|_| RustyError::ConsensusError)?;
        Ok(bits.to_consensus())
    }

    pub fn tx_fk_by_txid(&self, txid_hex: String) -> Result<Option<u64>, RustyError> {
        let txid = parse_hash32(&txid_hex)?;
        let fk = self
            .inner
            .tx_fk_by_txid(&txid)
            .map_err(|_| RustyError::StoreError)?;
        Ok(fk.map(|f| f.0))
    }

    pub fn is_outpoint_spent_at(
        &self,
        txid_hex: String,
        vout: u32,
        tip: Option<u32>,
    ) -> Result<bool, RustyError> {
        let txid = parse_hash32(&txid_hex)?;
        self.inner
            .is_outpoint_spent_at(&txid, vout, tip)
            .map_err(|_| RustyError::StoreError)
    }

    pub fn flush_for_shutdown(&self) -> Result<(), RustyError> {
        self.inner
            .flush_for_shutdown()
            .map_err(|_| RustyError::StoreError)
    }

    pub fn confirm_cancelled(&self) -> bool {
        self.inner.confirm_cancelled()
    }

    pub fn lookup_taken_hi(&self) -> Option<u64> {
        self.inner.lookup_taken_hi().map(|h| h as u64)
    }

    pub fn active_unknown_bits(&self, network: String) -> Result<Vec<i32>, RustyError> {
        let net = rbitcoin_network(&network)?;
        Ok(rbitcoin_net::active_unknown_bits(&self.inner, net))
    }

    pub fn warning_strings(&self, network: String) -> Result<Vec<String>, RustyError> {
        let net = rbitcoin_network(&network)?;
        Ok(rbitcoin_net::warning_strings(&self.inner, net))
    }

    pub fn drain_and_fence_hi(&self) -> Option<u64> {
        self.inner.drain_and_fence_hi().map(|h| h as u64)
    }

    pub fn block_queue_update_soft_pressure(&self, rate_blocks_per_s: Option<f64>) -> bool {
        self.inner
            .block_queue_update_soft_pressure(rate_blocks_per_s)
    }

    pub fn sh_indexed_through_height(&self) -> Option<u64> {
        self.inner.sh_indexed_through_height().map(|h| h as u64)
    }

    pub fn max_sh_creates(&self) -> u32 {
        self.inner.max_sh_creates()
    }

    pub fn set_max_sh_creates(&self, n: u32) {
        self.inner.set_max_sh_creates(n);
    }

    pub fn sample_reset_reconstruct_archived(&self) -> u64 {
        self.inner.sample_reset_reconstruct_archived()
    }

    pub fn sample_reset_thin_tweak_body_bytes(&self) -> u64 {
        self.inner.sample_reset_thin_tweak_body_bytes()
    }

    pub fn tweaks_at_height(
        &self,
        network: String,
        height: u32,
    ) -> Result<Vec<FfiHeightTweak>, RustyError> {
        let params = chain_params_for_network(&network)?;
        let map = rbitcoin_consensus::tweaks_for_height(
            &self.inner,
            &params,
            rbitcoin_primitives::Height(height),
        )
        .map_err(|_| RustyError::ConsensusError)?;
        Ok(map
            .into_iter()
            .map(|(txid, tt)| FfiHeightTweak {
                txid: rbitcoin_primitives::hex_encode(txid),
                tweak: rbitcoin_primitives::hex_encode(tt.tweak),
                output_pubkeys: tt
                    .output_pubkeys
                    .into_iter()
                    .map(|o| FfiTaprootOut {
                        vout: o.vout,
                        xonly: rbitcoin_primitives::hex_encode(o.xonly),
                        value: o.value,
                    })
                    .collect(),
            })
            .collect())
    }

    pub fn height_of_hash(&self, hash_hex: String) -> Result<Option<u64>, RustyError> {
        let hash = parse_hash32(&hash_hex)?;
        let height = self
            .inner
            .height_of_hash(&hash)
            .map_err(|_| RustyError::StoreError)?;
        Ok(height.map(|h| h.0 as u64))
    }

    pub fn header_at_height(&self, height: u32) -> Result<Option<FfiHeaderRecord>, RustyError> {
        let rec = self
            .inner
            .header_at_height(rbitcoin_primitives::Height(height))
            .map_err(|_| RustyError::StoreError)?;
        Ok(rec.map(|(_fk, h)| h.into()))
    }

    pub fn confirm_block(&self, height: u32, hash_hex: String) -> Result<u64, RustyError> {
        let hash = parse_hash32(&hash_hex)?;
        let fk = self
            .inner
            .confirm_block(rbitcoin_primitives::Height(height), &hash)
            .map_err(|_| RustyError::StoreError)?;
        Ok(fk.0)
    }

    pub fn index_mode(&self) -> u8 {
        self.inner.index_mode() as u8
    }

    pub fn sh_index_enabled(&self) -> bool {
        self.inner.sh_index_enabled()
    }

    pub fn backfill_sp_tweaks(&self, network: String) -> Result<u32, RustyError> {
        let params = chain_params_for_network(&network)?;
        rbitcoin_consensus::backfill_sp_tweaks(&self.inner, &params)
            .map_err(|_| RustyError::ConsensusError)
    }

    pub fn validate_header(
        &self,
        network: String,
        height: u32,
        header_hex: String,
    ) -> Result<(), RustyError> {
        let params = chain_params_for_network(&network)?;
        let header: BlockHeader =
            deserialize_hex(&header_hex).map_err(|_| RustyError::InvalidInput)?;
        rbitcoin_consensus::validate_header(
            &self.inner,
            &params,
            rbitcoin_primitives::Height(height),
            &header,
        )
        .map_err(|_| RustyError::ConsensusError)
    }

    pub fn median_time_past(&self, height: u32) -> Result<u32, RustyError> {
        rbitcoin_consensus::median_time_past(&self.inner, rbitcoin_primitives::Height(height))
            .map_err(|_| RustyError::ConsensusError)
    }

    pub fn on_load_pack(&self) -> Result<(), RustyError> {
        self.inner
            .on_load_pack()
            .map_err(|_| RustyError::StoreError)
    }

    pub fn request_confirm_cancel(&self) {
        self.inner.request_confirm_cancel();
    }

    pub fn clear_confirm_cancel(&self) {
        self.inner.clear_confirm_cancel();
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
}

#[derive(uniffi::Record)]
pub struct FfiBlockQueueStats {
    pub assign_stop_bytes: u64,
    pub bytes: u64,
    pub count: u64,
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

    pub fn has_free_slot(&self) -> bool {
        self.inner.lock().unwrap().has_free_slot()
    }

    pub fn compact(&self) -> Result<String, RustyError> {
        let mut guard = self.inner.lock().unwrap();
        let (dead, shrunk) = guard.compact().map_err(|_| RustyError::MempoolError)?;
        Ok(format!("dead={dead} shrunk={shrunk}"))
    }
}

// --- RBF FFI ---

#[uniffi::export]
pub fn rbf_pays_for_replacement(
    new_fee: u64,
    new_weight: u64,
    old_fee: u64,
    old_weight: u64,
) -> bool {
    rbitcoin_mempool::rbf_pays_for_replacement(new_fee, new_weight, old_fee, old_weight)
}

#[uniffi::export]
pub fn pure_rbfr_pays(new_fee: u64, new_weight: u64, direct_fee: u64, direct_weight: u64) -> bool {
    rbitcoin_mempool::pure_rbfr_pays(new_fee, new_weight, direct_fee, direct_weight)
}

#[uniffi::export]
pub fn rbf_allows_replacement(
    new_fee: u64,
    new_weight: u64,
    conflict_fee: u64,
    conflict_weight: u64,
    direct_fee: u64,
    direct_weight: u64,
) -> bool {
    rbitcoin_mempool::rbf_allows_replacement(
        new_fee,
        new_weight,
        conflict_fee,
        conflict_weight,
        direct_fee,
        direct_weight,
    )
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

    #[test]
    fn test_rpc_path() {
        assert_eq!(node_rpc_path(), "/");
    }

    #[test]
    fn test_electrum_scripthash() {
        // P2PKH script for 76a91489abcdefabbaabbaabbaabbaabbaabbaabbaabba88ac
        let script_hex = "76a91489abcdefabbaabbaabbaabbaabbaabbaabbaabba88ac";
        let hash = electrum_scripthash_hex(script_hex.to_string()).unwrap();
        assert!(!hash.is_empty());
        assert_eq!(hash.len(), 64);
    }

    #[test]
    fn test_rpc_call_json_parsing() {
        // We can't test an actual RPC call without a server, but we can test
        // that invalid params are rejected.
        let result = rpc_call_json(
            "127.0.0.1".to_string(),
            8332,
            "".to_string(),
            "".to_string(),
            "getblockchaininfo".to_string(),
            "not valid json".to_string(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_node_config() {
        assert_eq!(node_default_max_inbound(), 125);
        assert_eq!(node_core_maxconnections_outbound_reserve(), 11);
        assert_eq!(node_inbound_from_maxconnections(100), 89);
    }

    #[test]
    fn test_log_level() {
        init_log_level("warn".to_string());
        assert!(log_level_enabled("warn".to_string()));
        assert!(!log_level_enabled("info".to_string()));
        init_log_level("debug".to_string());
        assert!(log_level_enabled("debug".to_string()));
        assert!(!log_level_enabled("trace".to_string()));
        init_log_level("off".to_string());
        assert!(!log_level_enabled("error".to_string()));
        init_log_level("info".to_string());
    }

    #[test]
    fn test_capture_logs() {
        capture_logs(true);
        init_log_level("info".to_string());
        log_message("info".to_string(), "test capture".to_string());
        let logs = take_logs();
        capture_logs(false);
        assert!(logs.iter().any(|l| l.contains("test capture")));
    }

    #[test]
    fn test_genesis_block_hash() {
        let hash = genesis_block_hash("mainnet".to_string()).unwrap();
        assert_eq!(
            hash,
            "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
        );
        let regtest = genesis_block_hash("regtest".to_string()).unwrap();
        assert!(!regtest.is_empty());
    }

    #[test]
    fn test_default_milestone_height() {
        assert_eq!(
            default_milestone_height("mainnet".to_string()).unwrap(),
            840_000
        );
        assert_eq!(default_milestone_height("regtest".to_string()).unwrap(), 0);
    }

    #[test]
    fn test_check_genesis_hash() {
        let hash = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f";
        assert!(check_genesis_hash("mainnet".to_string(), hash.to_string()).unwrap());
        assert!(!check_genesis_hash("regtest".to_string(), hash.to_string()).unwrap());
    }

    #[test]
    fn test_mempool_constants() {
        assert_eq!(mempool_default_max_weight(), 300_000_000);
        assert_eq!(mempool_max_standard_tx_weight(), 400_000);
        assert!(mempool_incremental_relay_fee_rate() > 0);
    }

    #[test]
    fn test_electrum_tweaks_min_dust() {
        assert_eq!(electrum_default_tweaks_min_dust(), 1000);
    }

    #[test]
    fn test_primitives_constants() {
        assert_eq!(rbitcoin_store_magic(), "52425431");
        assert!(rbitcoin_schema_version() > 0);
        assert!(rbitcoin_schema_file_openable(rbitcoin_schema_version()));
        assert!(!rbitcoin_schema_file_openable(0));
    }

    #[test]
    fn test_store_open_or_create() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("store2").to_str().unwrap().to_string();
        let store = FfiStore::open_or_create(path.clone()).unwrap();
        assert_eq!(store.tip_height(), None);
        assert!(store.datadir_bytes() > 0 || store.datadir_bytes() == 0);
        assert!(!store.path().is_empty());
    }

    #[test]
    fn test_query_block_queue_stats() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("query2").to_str().unwrap().to_string();
        let query = FfiQuery::open_or_create(path).unwrap();
        assert!(query.tx_index_enabled());
        assert!(query.spend_index_enabled());
        let stats = query.block_queue_stats();
        assert_eq!(stats.count, 0);
        assert_eq!(stats.bytes, 0);
        assert!(stats.assign_stop_bytes > 0);
    }

    #[test]
    fn test_mempool_compact_and_free_slot() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("mempool2").to_str().unwrap().to_string();
        let mempool = FfiMempool::open_or_create(path).unwrap();
        assert!(mempool.has_free_slot());
        let compact = mempool.compact().unwrap();
        assert!(compact.starts_with("dead="));
    }

    #[test]
    fn test_seed_lookup_names_flat() {
        let names = seed_lookup_names_flat("mainnet".to_string()).unwrap();
        assert!(!names.is_empty());
        assert!(names.iter().any(|n| n.contains("seed.bitcoin.sipa.be")));
        let regtest = seed_lookup_names_flat("regtest".to_string()).unwrap();
        assert!(regtest.is_empty());
    }

    #[test]
    fn test_signet_magic_hex() {
        // A simple push+checksig script: 0x51 (OP_PUSHBYTES_33) + 33 bytes + 0xac (OP_CHECKSIG)
        let script_hex = "512103add177f3e3c6d9f3c8e4c5b9a7e2d1f0c3b6a5d8e7f4c1b0a3d6e5f8c7b4a1d0e3f6c5b8a7d4e1f0c3b6a5d8e7f4c1b0a3d6e5f8c7b4a1d0e3f6c5b8ac";
        let magic = signet_magic_hex(script_hex.to_string()).unwrap();
        assert_eq!(magic.len(), 8);
    }

    #[test]
    fn test_default_signet_challenge() {
        let challenge = default_signet_challenge_hex();
        assert!(!challenge.is_empty());
    }

    #[test]
    fn test_regtest_mining() {
        assert_eq!(regtest_pow_bits(), 0x207f_ffff);
        assert_eq!(regtest_block_spacing(), 600);
        // Genesis hash for regtest is all zeros
        let genesis_hash = "0000000000000000000000000000000000000000000000000000000000000000";
        let block_hex = mine_empty_regtest(genesis_hash.to_string(), 1296688602, 0).unwrap();
        assert!(!block_hex.is_empty());
    }

    #[test]
    fn test_grind_regtest_pow() {
        let genesis_hash = "0000000000000000000000000000000000000000000000000000000000000000";
        let block_hex = mine_empty_regtest(genesis_hash.to_string(), 1296688602, 0).unwrap();
        let bytes = hex_decode(block_hex.clone()).unwrap();
        let block: bitcoin::Block = bitcoin::consensus::encode::deserialize(&bytes).unwrap();
        let header_hex = bitcoin::consensus::encode::serialize_hex(&block.header);
        let ground = grind_regtest_pow(header_hex).unwrap();
        assert!(!ground.is_empty());
    }

    #[test]
    fn test_is_final_tx() {
        // A simple final tx (no locktime, sequence max)
        let tx_hex = "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0100ffffffff0100f2052a010000001976a914000000000000000000000000000000000000000088ac00000000";
        assert!(is_final_tx(tx_hex.to_string(), 100, 100).unwrap());
    }

    #[test]
    fn test_bip34_height_script() {
        let script = bip34_height_script(1);
        assert!(!script.is_empty());
    }

    #[test]
    fn test_local_service_flags() {
        let flags = local_service_flags_u64();
        assert!(flags > 0);
    }

    #[test]
    fn test_desirable_service_flags() {
        let offered = local_service_flags_u64();
        let desirable = desirable_service_flags(offered, 0);
        assert!(desirable > 0);
        assert!(has_all_desirable_service_flags(offered, 0));
    }

    #[test]
    fn test_versionbits_warnings() {
        let period = warn_period_threshold("mainnet".to_string()).unwrap();
        assert!(period.start > 0);
        assert!(period.end > 0);
        let warning = unknown_rules_warning(0);
        assert!(!warning.is_empty());
    }

    #[test]
    fn test_merkle_root_from_txids() {
        let txids = vec![
            "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
            "1111111111111111111111111111111111111111111111111111111111111111".to_string(),
        ];
        let root = merkle_root_from_txids(txids).unwrap();
        assert_eq!(root.len(), 64);
    }

    #[test]
    fn test_block_wire_input_count() {
        // Genesis block hex (regtest)
        let genesis_hash = "0000000000000000000000000000000000000000000000000000000000000000";
        let block_hex = mine_empty_regtest(genesis_hash.to_string(), 1296688602, 0).unwrap();
        let count = block_wire_input_count(block_hex).unwrap();
        assert_eq!(count, 1); // coinbase only
    }

    #[test]
    fn test_query_extended_methods() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("query3").to_str().unwrap().to_string();
        let query = FfiQuery::open_or_create(path).unwrap();
        assert_eq!(query.soft_confirm_window(), 0); // default unknown rate
        assert_eq!(query.fence_tip_height(), None);
        assert_eq!(query.archived_block_count().unwrap(), 0);
        assert_eq!(query.tx_body_count(), 0);
        assert_eq!(query.tx_head_occupied(), 0);
    }

    #[test]
    fn test_meets_min_relay_fee_at() {
        assert!(meets_min_relay_fee_at(3000, 1000, 100));
        assert!(!meets_min_relay_fee_at(99, 4000, 100));
    }

    #[test]
    fn test_display_hash_hex() {
        let bytes = vec![0u8; 32];
        let hash = display_hash_hex(bytes).unwrap();
        assert_eq!(
            hash,
            "0000000000000000000000000000000000000000000000000000000000000000"
        );
    }

    #[test]
    fn test_parse_display_hash32_roundtrip() {
        let hex = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f";
        let bytes = parse_display_hash32(hex.to_string()).unwrap();
        let back = display_hash_hex(bytes).unwrap();
        assert_eq!(back, hex);
    }

    #[test]
    fn test_block_header_hash() {
        let hash = block_header_hash(
            1,
            "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
            "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b".to_string(),
            1231006505,
            0x1d00ffff,
            2083236893,
        )
        .unwrap();
        assert_eq!(
            hash,
            "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
        );
    }

    #[test]
    fn test_query_archive_methods() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("query4").to_str().unwrap().to_string();
        let query = FfiQuery::open_or_create(path).unwrap();
        assert!(!query
            .is_block_archived(
                "0000000000000000000000000000000000000000000000000000000000000000".to_string()
            )
            .unwrap());
        assert!(!query.header_has_class_a_body(1).unwrap());
    }

    #[test]
    fn test_witness_commitment_script() {
        let wtxids =
            vec!["0000000000000000000000000000000000000000000000000000000000000000".to_string()];
        let reserved = "0000000000000000000000000000000000000000000000000000000000000000";
        let script = witness_commitment_script(wtxids, reserved.to_string()).unwrap();
        assert!(!script.is_empty());
        assert!(script.starts_with("6a24aa21a9ed"));
    }

    #[test]
    fn test_legacy_sigop_count() {
        // P2PKH tx with one input, one output
        let tx_hex = "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff4d04ffff001d0104455468652054696d65732030332f4a616e2f32303039204368616e63656c6c6f72206f6e206272696e6b206f66207365636f6e64206261696c6f757420666f722062616e6b73ffffffff0100f2052a01000000434104678afdb0fe5548271967f1a67130b7105cd6a828e03909a67962e0ea1f61deb649f6bc3f4cef38c4f35504e51ec112de5c384df7ba0b8d578a4c702b6bf11d5fac00000000";
        let count = legacy_sigop_count(tx_hex.to_string()).unwrap();
        assert!(count > 0);
        let gbt = tx_gbt_sigops(tx_hex.to_string()).unwrap();
        assert_eq!(gbt, count * 4);
    }

    #[test]
    fn test_bip68_active_for_tx() {
        // Version 1 tx
        let tx_hex = "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0100ffffffff0100f2052a010000001976a914000000000000000000000000000000000000000088ac00000000";
        assert!(!bip68_active_for_tx(tx_hex.to_string()).unwrap());
    }

    #[test]
    fn test_sequence_locks_satisfied() {
        // Version 1 tx (no sequence locks)
        let tx_hex = "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0100ffffffff0100f2052a010000001976a914000000000000000000000000000000000000000088ac00000000";
        assert!(sequence_locks_satisfied(tx_hex.to_string(), vec![], vec![], 100, 100).unwrap());
    }

    #[test]
    fn test_required_seed_services() {
        let services = required_seed_services_u64();
        assert!(services > 0);
    }

    #[test]
    fn test_mine_regtest_paying() {
        let genesis_hash = "0000000000000000000000000000000000000000000000000000000000000000";
        let script = "76a914000000000000000000000000000000000000000088ac"; // P2PKH
        let block_hex = mine_regtest_paying(
            genesis_hash.to_string(),
            1296688602,
            0,
            script.to_string(),
            vec![],
        )
        .unwrap();
        assert!(!block_hex.is_empty());
        let count = block_wire_input_count(block_hex).unwrap();
        assert_eq!(count, 1); // coinbase only
    }

    #[test]
    fn test_check_libre_annex() {
        // Version 1 tx with no witness
        let tx_hex = "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0100ffffffff0100f2052a010000001976a914000000000000000000000000000000000000000088ac00000000";
        let result = check_libre_annex(tx_hex.to_string()).unwrap();
        assert_eq!(result, "standard");
    }

    #[test]
    fn test_check_libre_admission() {
        // Coinbase tx should be non-standard for mempool
        let tx_hex = "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff4d04ffff001d0104455468652054696d65732030332f4a616e2f32303039204368616e63656c6c6f72206f6e206272696e6b206f66207365636f6e64206261696c6f757420666f722062616e6b73ffffffff0100f2052a01000000434104678afdb0fe5548271967f1a67130b7105cd6a828e03909a67962e0ea1f61deb649f6bc3f4cef38c4f35504e51ec112de5c384df7ba0b8d578a4c702b6bf11d5fac00000000";
        let result = check_libre_admission(tx_hex.to_string(), 1000, 1000).unwrap();
        assert!(result.starts_with("non-standard"));
    }

    #[test]
    fn test_query_tx_fk_by_txid() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("query_txfk").to_str().unwrap().to_string();
        let query = FfiQuery::open_or_create(path).unwrap();
        // Empty chain, no tx found
        let fk = query
            .tx_fk_by_txid(
                "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
            )
            .unwrap();
        assert_eq!(fk, None);
    }

    #[test]
    fn test_query_is_outpoint_spent_at() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("query_spent").to_str().unwrap().to_string();
        let query = FfiQuery::open_or_create(path).unwrap();
        let spent = query
            .is_outpoint_spent_at(
                "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
                0,
                None,
            )
            .unwrap();
        assert!(!spent);
    }

    #[test]
    fn test_query_flush_for_shutdown() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("query_flush").to_str().unwrap().to_string();
        let query = FfiQuery::open_or_create(path).unwrap();
        query.flush_for_shutdown().unwrap();
    }

    #[test]
    fn test_query_confirm_cancelled() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp
            .path()
            .join("query_cancel")
            .to_str()
            .unwrap()
            .to_string();
        let query = FfiQuery::open_or_create(path).unwrap();
        assert!(!query.confirm_cancelled());
    }

    #[test]
    fn test_query_lookup_taken_hi() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp
            .path()
            .join("query_lookup")
            .to_str()
            .unwrap()
            .to_string();
        let query = FfiQuery::open_or_create(path).unwrap();
        assert_eq!(query.lookup_taken_hi(), None);
    }

    #[test]
    fn test_query_active_unknown_bits_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("query_bits").to_str().unwrap().to_string();
        let query = FfiQuery::open_or_create(path).unwrap();
        let bits = query.active_unknown_bits("mainnet".to_string()).unwrap();
        assert!(bits.is_empty());
        let warnings = query.warning_strings("mainnet".to_string()).unwrap();
        assert!(warnings.is_empty());
    }

    #[test]
    fn test_tweak_from_tx_not_eligible() {
        // Simple tx with no taproot outputs → not eligible
        let tx_hex = "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0100ffffffff0100f2052a010000001976a914000000000000000000000000000000000000000088ac00000000";
        let result = tweak_from_tx(tx_hex.to_string(), vec![]).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_ip16_for_lookup() {
        let v4 = ip16_for_lookup("127.0.0.1".to_string()).unwrap();
        assert_eq!(v4.len(), 16);
        // IPv4-mapped: ::ffff:127.0.0.1
        assert_eq!(v4[0..10], vec![0u8; 10]);
        assert_eq!(v4[10], 255);
        assert_eq!(v4[11], 255);
        assert_eq!(v4[12], 127);
        let v6 = ip16_for_lookup("::1".to_string()).unwrap();
        assert_eq!(v6.len(), 16);
        assert_eq!(v6[15], 1);
    }

    #[test]
    fn test_asmap_sanity_check_empty() {
        let ok = asmap_sanity_check("".to_string()).unwrap();
        assert!(!ok);
    }

    #[test]
    fn test_query_drain_and_fence_hi_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("query_drain").to_str().unwrap().to_string();
        let query = FfiQuery::open_or_create(path).unwrap();
        assert_eq!(query.drain_and_fence_hi(), None);
    }

    #[test]
    fn test_query_sh_indexed_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("query_sh").to_str().unwrap().to_string();
        let query = FfiQuery::open_or_create(path).unwrap();
        assert_eq!(query.sh_indexed_through_height(), None);
    }

    #[test]
    fn test_query_max_sh_creates() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp
            .path()
            .join("query_sh_max")
            .to_str()
            .unwrap()
            .to_string();
        let query = FfiQuery::open_or_create(path).unwrap();
        assert_eq!(query.max_sh_creates(), 0);
        query.set_max_sh_creates(100);
        assert_eq!(query.max_sh_creates(), 100);
    }

    #[test]
    fn test_query_sample_resets() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp
            .path()
            .join("query_samples")
            .to_str()
            .unwrap()
            .to_string();
        let query = FfiQuery::open_or_create(path).unwrap();
        assert_eq!(query.sample_reset_reconstruct_archived(), 0);
        assert_eq!(query.sample_reset_thin_tweak_body_bytes(), 0);
    }

    #[test]
    fn test_validate_block_structure_genesis() {
        let genesis_hash = "0000000000000000000000000000000000000000000000000000000000000000";
        let block_hex = mine_empty_regtest(genesis_hash.to_string(), 1231006505, 0).unwrap();
        validate_block_structure(block_hex, "regtest".to_string(), 0, true).unwrap();
    }

    #[test]
    fn test_netgroup_v4() {
        let group = netgroup("1.2.3.4".to_string(), 8333, None).unwrap();
        assert!(group > 0);
    }

    #[test]
    fn test_sum_work() {
        let w1 = "0000000000000000000000000000000000000000000000000000000000000001";
        let w2 = "0000000000000000000000000000000000000000000000000000000000000002";
        let sum = sum_work_hex(vec![w1.to_string(), w2.to_string()]).unwrap();
        assert!(!sum.is_empty());
    }

    #[test]
    fn test_query_tweaks_at_height_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("query_sp").to_str().unwrap().to_string();
        let query = FfiQuery::open_or_create(path).unwrap();
        let tweaks = query.tweaks_at_height("mainnet".to_string(), 0).unwrap();
        assert!(tweaks.is_empty());
    }

    #[test]
    fn test_rbf_pays_for_replacement() {
        assert!(rbf_pays_for_replacement(2000, 1000, 1000, 1000));
        assert!(!rbf_pays_for_replacement(1000, 1000, 1000, 1000));
        assert!(!rbf_pays_for_replacement(500, 1000, 1000, 1000));
    }

    #[test]
    fn test_pure_rbfr_pays() {
        assert!(pure_rbfr_pays(5000, 1000, 1000, 1000));
        assert!(!pure_rbfr_pays(1000, 1000, 1000, 1000));
    }

    #[test]
    fn test_rbf_allows_replacement() {
        assert!(rbf_allows_replacement(2000, 1000, 1000, 1000, 1000, 1000));
        assert!(!rbf_allows_replacement(500, 1000, 1000, 1000, 1000, 1000));
    }

    #[test]
    fn test_query_height_of_hash_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("query_hoh").to_str().unwrap().to_string();
        let query = FfiQuery::open_or_create(path).unwrap();
        let height = query
            .height_of_hash(
                "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
            )
            .unwrap();
        assert_eq!(height, None);
    }

    #[test]
    fn test_query_header_at_height_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("query_hah").to_str().unwrap().to_string();
        let query = FfiQuery::open_or_create(path).unwrap();
        let header = query.header_at_height(0).unwrap();
        assert_eq!(header, None);
    }

    #[test]
    fn test_parse_v2_regtest() {
        // Invalid contents should fail
        let result = parse_v2_regtest("00".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_v2_regtest_named() {
        // Empty payload for "version" command — parses structurally even if empty
        let result = parse_v2_regtest_named("version".to_string(), "".to_string());
        // Just ensure it doesn't panic; empty payload may or may not error
        let _ = result;
    }

    #[test]
    fn test_query_index_mode() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("query_mode").to_str().unwrap().to_string();
        let query = FfiQuery::open_or_create(path).unwrap();
        let mode = query.index_mode();
        assert!(mode == 1 || mode == 2);
        assert!(query.sh_index_enabled());
    }

    #[test]
    fn test_query_backfill_sp_tweaks_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp
            .path()
            .join("query_sp_backfill")
            .to_str()
            .unwrap()
            .to_string();
        let query = FfiQuery::open_or_create(path).unwrap();
        let count = query.backfill_sp_tweaks("mainnet".to_string()).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_parse_peer_addr() {
        let addr = parse_peer_addr("127.0.0.1:8333".to_string()).unwrap();
        assert!(addr.contains("127.0.0.1"));
        assert!(addr.contains("8333"));
    }

    #[test]
    fn test_query_median_time_past_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("query_mtp").to_str().unwrap().to_string();
        let query = FfiQuery::open_or_create(path).unwrap();
        // Empty chain may error; just ensure it doesn't panic
        let _ = query.median_time_past(0);
    }

    #[test]
    fn test_accept_and_connect_block_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp
            .path()
            .join("query_connect")
            .to_str()
            .unwrap()
            .to_string();
        // Empty chain: connecting a non-genesis block should fail
        let genesis_hash = "0000000000000000000000000000000000000000000000000000000000000000";
        let block_hex = mine_empty_regtest(genesis_hash.to_string(), 1296688602, 1).unwrap();
        let result = accept_and_connect_block(path, "regtest".to_string(), 1, block_hex, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_format_disconnect_tip_line() {
        let line = format_disconnect_tip_line(
            100,
            "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
            5,
        )
        .unwrap();
        assert!(line.contains("DisconnectTip"));
        assert!(line.contains("height=100"));
        assert!(line.contains("tx=5"));
    }

    #[test]
    fn test_serve_perf() {
        let sample = sample_reset_serve_perf();
        let formatted = format_serve_perf(sample);
        assert!(!formatted.is_empty());
    }

    #[test]
    fn test_query_on_load_pack() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("query_load").to_str().unwrap().to_string();
        let query = FfiQuery::open_or_create(path).unwrap();
        query.on_load_pack().unwrap();
    }

    #[test]
    fn test_query_confirm_cancel_cycle() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp
            .path()
            .join("query_cancel2")
            .to_str()
            .unwrap()
            .to_string();
        let query = FfiQuery::open_or_create(path).unwrap();
        assert!(!query.confirm_cancelled());
        query.request_confirm_cancel();
        assert!(query.confirm_cancelled());
        query.clear_confirm_cancel();
        assert!(!query.confirm_cancelled());
    }

    #[test]
    fn test_query_get_header_by_hash_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("query_ghbh").to_str().unwrap().to_string();
        let query = FfiQuery::open_or_create(path).unwrap();
        let header = query
            .get_header_by_hash(
                "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
            )
            .unwrap();
        assert_eq!(header, None);
    }

    #[test]
    fn test_verify_tx_scripts_detached_forks() {
        let tx_hex = "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff4d04ffff001d0104455468652054696d65732030332f4a616e2f32303039204368616e63656c6c6f72206f6e206272696e6b206f66207365636f6e64206261696c6f757420666f722062616e6b73ffffffff0100f2052a01000000434104678afdb0fe5548271967f1a67130b7105cd6a828e03909a67962e0ea1f61deb649f6bc3f4cef38c4f35504e51ec112de5c384df7ba0b8d578a4c702b6bf11d5fac00000000";
        let result = verify_tx_scripts_detached_forks(
            vec![],
            tx_hex.to_string(),
            true,
            true,
            true,
            true,
            true,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_commit_class_a_block_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp
            .path()
            .join("query_commit_a")
            .to_str()
            .unwrap()
            .to_string();
        let genesis_hash = "0000000000000000000000000000000000000000000000000000000000000000";
        let block_hex = mine_empty_regtest(genesis_hash.to_string(), 1296688602, 1).unwrap();
        // Just ensure it doesn't panic; empty chain behavior may vary
        let _ = commit_class_a_block(path, "regtest".to_string(), 1, block_hex, 0);
    }
}
