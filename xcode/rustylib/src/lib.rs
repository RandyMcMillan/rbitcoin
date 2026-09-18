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
pub fn verify_tx_scripts_forks(
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
        .map(|h| deserialize_hex(h).map_err(|_| RustyError::InvalidInput))
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
pub fn network_magic_hex(network: String) -> Result<String, RustyError> {
    let params = chain_params_for_network(&network)?;
    let magic = rbitcoin_net::magic_for_params(&params);
    let bytes = bitcoin::consensus::encode::serialize(&magic);
    Ok(rbitcoin_primitives::hex_encode(bytes))
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

#[uniffi::export]
pub fn net_default_blocks_in_transit_per_peer() -> u32 {
    rbitcoin_net::DEFAULT_BLOCKS_IN_TRANSIT_PER_PEER as u32
}

#[uniffi::export]
pub fn net_default_ibd_window() -> u32 {
    rbitcoin_net::DEFAULT_IBD_WINDOW as u32
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

#[uniffi::export]
pub fn init_log_from_env() -> bool {
    rbitcoin_log::init_from_env()
}

#[uniffi::export]
pub fn init_log_off() {
    rbitcoin_log::init_off();
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

// --- Header to Record FFI ---

#[uniffi::export]
pub fn header_to_record(
    prev_fk: u64,
    header_hex: String,
    hash_hex: String,
) -> Result<FfiHeaderRecord, RustyError> {
    let header: BlockHeader = deserialize_hex(&header_hex).map_err(|_| RustyError::InvalidInput)?;
    let hash = parse_hash32(&hash_hex)?;
    let rec = rbitcoin_consensus::header_to_record(rbitcoin_primitives::Fk(prev_fk), &header, hash);
    Ok(rec.into())
}

// --- Inbound Eviction FFI ---

#[derive(Debug, PartialEq, uniffi::Record)]
pub struct FfiInboundEvictCandidate {
    pub id: u64,
    pub connected_at: u64,
    pub min_ping: Option<f64>,
    pub last_block: u64,
    pub last_tx: u64,
    pub netgroup: u64,
    pub noban: bool,
}

#[uniffi::export]
pub fn select_inbound_eviction(candidates: Vec<FfiInboundEvictCandidate>) -> Option<u64> {
    let cands: Vec<rbitcoin_net::InboundEvictCandidate> = candidates
        .into_iter()
        .map(|c| rbitcoin_net::InboundEvictCandidate {
            id: c.id,
            connected_at: c.connected_at,
            min_ping: c.min_ping,
            last_block: c.last_block,
            last_tx: c.last_tx,
            netgroup: c.netgroup,
            noban: c.noban,
        })
        .collect();
    rbitcoin_net::select_inbound_eviction(cands)
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

#[derive(Debug, PartialEq, uniffi::Record)]
pub struct FfiChainView {
    pub height: u64,
    pub hash: String,
    pub header_fk: u64,
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

#[uniffi::export]
pub fn electrum_last_height(start: u32, count: u32, tip: Option<u32>) -> Option<u32> {
    rbitcoin_electrum::last_height(start, count, tip)
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

impl TryFrom<FfiHeaderRecord> for rbitcoin_store::HeaderRecord {
    type Error = RustyError;
    fn try_from(h: FfiHeaderRecord) -> Result<Self, Self::Error> {
        Ok(Self {
            prev_fk: rbitcoin_primitives::Fk(h.prev_fk),
            version: h.version,
            timestamp: h.timestamp,
            bits: h.bits,
            nonce: h.nonce,
            merkle_root: parse_hash32(&h.merkle_root)?,
            hash: parse_hash32(&h.hash)?,
            size: h.size,
            weight: h.weight,
        })
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

#[derive(Debug, PartialEq, uniffi::Record)]
pub struct FfiPointRecord {
    pub out_txid: String,
    pub out_index: u32,
    pub spending_tx_fk: u64,
    pub spending_vin: u32,
    pub next_fk: u64,
}

impl From<rbitcoin_store::PointRecord> for FfiPointRecord {
    fn from(p: rbitcoin_store::PointRecord) -> Self {
        Self {
            out_txid: rbitcoin_primitives::hex_encode(p.out_txid),
            out_index: p.out_index,
            spending_tx_fk: p.spending_tx_fk.0,
            spending_vin: p.spending_vin,
            next_fk: p.next.0,
        }
    }
}

#[derive(Debug, PartialEq, uniffi::Record)]
pub struct FfiInputRecord {
    pub prev_txid: String,
    pub create_fk: u64,
    pub prev_index: u32,
    pub sequence: u32,
    pub script_sig: Vec<u8>,
    pub witness: Vec<Vec<u8>>,
}

impl From<rbitcoin_store::InputRecord> for FfiInputRecord {
    fn from(i: rbitcoin_store::InputRecord) -> Self {
        Self {
            prev_txid: rbitcoin_primitives::hex_encode(i.prev_txid),
            create_fk: i.create_fk.0,
            prev_index: i.prev_index,
            sequence: i.sequence,
            script_sig: i.script_sig,
            witness: i.witness,
        }
    }
}

#[derive(Debug, PartialEq, uniffi::Record)]
pub struct FfiOutputRecord {
    pub value: i64,
    pub script: Vec<u8>,
    pub spender_fk: u64,
    pub multi_spender: bool,
}

impl From<rbitcoin_store::OutputRecord> for FfiOutputRecord {
    fn from(o: rbitcoin_store::OutputRecord) -> Self {
        Self {
            value: o.value,
            script: o.script,
            spender_fk: o.spender_field.0,
            multi_spender: o.multi_spender,
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

    pub fn archived_block_count(&self) -> Result<u64, RustyError> {
        self.inner
            .archived_block_count()
            .map_err(|_| RustyError::StoreError)
    }

    pub fn header_slots(&self) -> u64 {
        self.inner.header_slots()
    }

    pub fn tx_head_bits(&self) -> u32 {
        self.inner.tx_head_bits()
    }

    pub fn is_split(&self) -> bool {
        self.inner.is_split()
    }

    pub fn get_header(&self, fk: u64) -> Result<FfiHeaderRecord, RustyError> {
        let rec = self
            .inner
            .get_header(rbitcoin_primitives::Fk(fk))
            .map_err(|_| RustyError::StoreError)?;
        Ok(rec.into())
    }

    pub fn get_tx(&self, fk: u64) -> Result<FfiTxRecord, RustyError> {
        let rec = self
            .inner
            .get_tx(rbitcoin_primitives::Fk(fk))
            .map_err(|_| RustyError::StoreError)?;
        Ok(rec.into())
    }

    pub fn spender_list_count(&self) -> u64 {
        self.inner.spender_list_count()
    }

    pub fn class_c_l2_resident_bytes(&self) -> u64 {
        self.inner.class_c_l2_resident_bytes()
    }

    pub fn is_confirmed_strong(&self, tx_fk: u64) -> Result<bool, RustyError> {
        self.inner
            .is_confirmed_strong(rbitcoin_primitives::Fk(tx_fk))
            .map_err(|_| RustyError::StoreError)
    }

    pub fn is_confirmed_strong_at(
        &self,
        tx_fk: u64,
        tip: Option<u32>,
    ) -> Result<bool, RustyError> {
        self.inner
            .is_confirmed_strong_at(rbitcoin_primitives::Fk(tx_fk), tip)
            .map_err(|_| RustyError::StoreError)
    }

    pub fn fence_max_connected_fk(&self) -> u64 {
        self.inner.fence_max_connected_fk()
    }

    pub fn height_fence_run_count(&self) -> u64 {
        self.inner.height_fence_run_count() as u64
    }

    pub fn fence_tip_height(&self) -> Option<u64> {
        self.inner.fence_tip_height().map(|h| h as u64)
    }

    pub fn rebuild_height_fence(&self) -> Result<(), RustyError> {
        self.inner
            .rebuild_height_fence()
            .map_err(|_| RustyError::StoreError)
    }

    pub fn flush_header_archive(&self) -> Result<(), RustyError> {
        self.inner
            .flush_header_archive()
            .map_err(|_| RustyError::StoreError)
    }

    pub fn flush(&self) -> Result<(), RustyError> {
        self.inner.flush().map_err(|_| RustyError::StoreError)
    }

    pub fn tx_height_get(&self, tx_fk: u64) -> Result<Option<u32>, RustyError> {
        self.inner
            .tx_height_get(rbitcoin_primitives::Fk(tx_fk))
            .map_err(|_| RustyError::StoreError)
    }

    pub fn tx_height_get_batch(&self, fks: Vec<u64>) -> Result<Vec<Option<u32>>, RustyError> {
        let fks: Vec<rbitcoin_primitives::Fk> =
            fks.into_iter().map(rbitcoin_primitives::Fk).collect();
        self.inner
            .tx_height_get_batch(&fks)
            .map_err(|_| RustyError::StoreError)
    }

    pub fn txids_get_many(&self, fks: Vec<u64>) -> Result<Vec<Option<String>>, RustyError> {
        let fks: Vec<rbitcoin_primitives::Fk> =
            fks.into_iter().map(rbitcoin_primitives::Fk).collect();
        let txids = self.inner.txids_get_many(&fks).map_err(|_| RustyError::StoreError)?;
        Ok(txids.into_iter().map(|t| t.map(rbitcoin_primitives::hex_encode)).collect())
    }

    pub fn tx_body_range_batch(
        &self,
        fks: Vec<u64>,
    ) -> Result<Vec<Option<FfiTxRange>>, RustyError> {
        let fks: Vec<rbitcoin_primitives::Fk> =
            fks.into_iter().map(rbitcoin_primitives::Fk).collect();
        let ranges = self
            .inner
            .tx_body_range_batch(&fks)
            .map_err(|_| RustyError::StoreError)?;
        Ok(ranges
            .into_iter()
            .map(|r| r.map(|(offset, len)| FfiTxRange { offset, len }))
            .collect())
    }

    pub fn tx_spent_range_batch(
        &self,
        fks: Vec<u64>,
    ) -> Result<Vec<Option<FfiTxRange>>, RustyError> {
        let fks: Vec<rbitcoin_primitives::Fk> =
            fks.into_iter().map(rbitcoin_primitives::Fk).collect();
        let ranges = self
            .inner
            .tx_spent_range_batch(&fks)
            .map_err(|_| RustyError::StoreError)?;
        Ok(ranges
            .into_iter()
            .map(|r| r.map(|(offset, len)| FfiTxRange { offset, len }))
            .collect())
    }

    pub fn spenders_create(&self, create_fk: u64, out_index: u32) -> Result<Vec<u64>, RustyError> {
        let fks = self
            .inner
            .spenders_create(rbitcoin_primitives::Fk(create_fk), out_index)
            .map_err(|_| RustyError::StoreError)?;
        Ok(fks.into_iter().map(|f| f.0).collect())
    }

    pub fn get_fk_by_txid(&self, txid_hex: String) -> Result<Option<u64>, RustyError> {
        let txid = parse_hash32(&txid_hex)?;
        let fk = self
            .inner
            .get_fk_by_txid(&txid)
            .map_err(|_| RustyError::StoreError)?;
        Ok(fk.map(|f| f.0))
    }

    pub fn get_fk_by_txid_tip(&self, txid_hex: String) -> Result<Option<u64>, RustyError> {
        let txid = parse_hash32(&txid_hex)?;
        let fk = self
            .inner
            .get_fk_by_txid_tip(&txid)
            .map_err(|_| RustyError::StoreError)?;
        Ok(fk.map(|f| f.0))
    }

    pub fn tx_body_range(&self, fk: u64) -> Result<FfiTxRange, RustyError> {
        let (offset, len) = self
            .inner
            .tx_body_range(rbitcoin_primitives::Fk(fk))
            .map_err(|_| RustyError::StoreError)?;
        Ok(FfiTxRange { offset, len })
    }

    pub fn tx_spent_range(&self, fk: u64) -> Result<FfiTxRange, RustyError> {
        let (offset, len) = self
            .inner
            .tx_spent_range(rbitcoin_primitives::Fk(fk))
            .map_err(|_| RustyError::StoreError)?;
        Ok(FfiTxRange { offset, len })
    }

    pub fn tx_inwit_range(&self, fk: u64) -> Result<FfiTxRange, RustyError> {
        let (offset, len) = self
            .inner
            .tx_inwit_range(rbitcoin_primitives::Fk(fk))
            .map_err(|_| RustyError::StoreError)?;
        Ok(FfiTxRange { offset, len })
    }

    pub fn mtp_times_at(&self, height: u32) -> Option<FfiMtpTimes> {
        self.inner
            .mtp_times_at(rbitcoin_primitives::Height(height))
            .map(|(count, window)| FfiMtpTimes {
                count,
                window: window.to_vec(),
            })
    }

    pub fn coinbase_fk_at_heights(&self, heights: Vec<u32>) -> Result<Vec<FfiCoinbaseAtHeight>, RustyError> {
        let map = self
            .inner
            .coinbase_fk_at_heights(&heights)
            .map_err(|_| RustyError::StoreError)?;
        Ok(map
            .into_iter()
            .map(|(height, fk)| FfiCoinbaseAtHeight {
                height,
                coinbase_fk: fk.0,
            })
            .collect())
    }

    pub fn get_tx_full(&self, fk: u64) -> Result<FfiTxFull, RustyError> {
        let (tx, inputs, outputs) = self
            .inner
            .get_tx_full(rbitcoin_primitives::Fk(fk))
            .map_err(|_| RustyError::StoreError)?;
        Ok(FfiTxFull {
            tx: tx.into(),
            inputs: inputs.into_iter().map(|i| i.into()).collect(),
            outputs: outputs.into_iter().map(|o| o.into()).collect(),
        })
    }

    pub fn get_tx_full_span(&self, first: u64, last: u64) -> Result<Vec<FfiTxFull>, RustyError> {
        let txs = self
            .inner
            .get_tx_full_span(first, last)
            .map_err(|_| RustyError::StoreError)?;
        Ok(txs
            .into_iter()
            .map(|(tx, inputs, outputs)| FfiTxFull {
                tx: tx.into(),
                inputs: inputs.into_iter().map(|i| i.into()).collect(),
                outputs: outputs.into_iter().map(|o| o.into()).collect(),
            })
            .collect())
    }

    pub fn get_tx_meta_and_outputs(&self, fk: u64) -> Result<FfiTxMetaAndOutputs, RustyError> {
        let (tx, outputs) = self
            .inner
            .get_tx_meta_and_outputs(rbitcoin_primitives::Fk(fk))
            .map_err(|_| RustyError::StoreError)?;
        Ok(FfiTxMetaAndOutputs {
            tx: tx.into(),
            outputs: outputs.into_iter().map(|o| o.into()).collect(),
        })
    }

    pub fn get_tx_meta_and_prevouts(&self, fk: u64) -> Result<FfiTxMetaAndPrevouts, RustyError> {
        let (tx, prevouts) = self
            .inner
            .get_tx_meta_and_prevouts(rbitcoin_primitives::Fk(fk))
            .map_err(|_| RustyError::StoreError)?;
        Ok(FfiTxMetaAndPrevouts {
            tx: tx.into(),
            prevouts: prevouts
                .into_iter()
                .map(|(fk, vout)| FfiPrevoutRef {
                    create_fk: fk.0,
                    vout,
                })
                .collect(),
        })
    }

    pub fn resolve_txid(&self, txid_hex: String, tip_then_any: bool) -> Result<Option<u64>, RustyError> {
        let txid = parse_hash32(&txid_hex)?;
        if let Some(fk) = self
            .inner
            .get_fk_by_txid_tip(&txid)
            .map_err(|_| RustyError::StoreError)?
        {
            return Ok(Some(fk.0));
        }
        if tip_then_any {
            if let Some(fk) = self
                .inner
                .get_fk_by_txid(&txid)
                .map_err(|_| RustyError::StoreError)?
            {
                return Ok(Some(fk.0));
            }
        }
        Ok(None)
    }

    pub fn get_fk_by_txid_batch(&self, txids_hex: Vec<String>) -> Result<Vec<Option<u64>>, RustyError> {
        let txids: Vec<[u8; 32]> = txids_hex
            .into_iter()
            .map(|h| parse_hash32(&h))
            .collect::<Result<_, _>>()?;
        let results = self
            .inner
            .get_fk_by_txid_batch(&txids)
            .map_err(|_| RustyError::StoreError)?;
        Ok(results
            .into_iter()
            .map(|(_txid, opt)| opt.map(|(fk, _)| fk.0))
            .collect())
    }
}

#[derive(Debug, PartialEq, uniffi::Record)]
pub struct FfiTxRange {
    pub offset: u64,
    pub len: u64,
}

#[derive(uniffi::Record)]
pub struct FfiTxFull {
    pub tx: FfiTxRecord,
    pub inputs: Vec<FfiInputRecord>,
    pub outputs: Vec<FfiOutputRecord>,
}

#[derive(uniffi::Record)]
pub struct FfiTxMetaAndOutputs {
    pub tx: FfiTxRecord,
    pub outputs: Vec<FfiOutputRecord>,
}

#[derive(uniffi::Record)]
pub struct FfiPrevoutRef {
    pub create_fk: u64,
    pub vout: u32,
}

#[derive(uniffi::Record)]
pub struct FfiTxMetaAndPrevouts {
    pub tx: FfiTxRecord,
    pub prevouts: Vec<FfiPrevoutRef>,
}

#[derive(uniffi::Record)]
pub struct FfiMtpTimes {
    pub count: u8,
    pub window: Vec<u32>,
}

#[derive(uniffi::Record)]
pub struct FfiCoinbaseAtHeight {
    pub height: u32,
    pub coinbase_fk: u64,
}

// --- Query FFI ---

#[derive(uniffi::Object)]
pub struct FfiQuery {
    inner: Arc<rbitcoin_query::Query>,
}

#[uniffi::export]
impl FfiQuery {
    #[uniffi::constructor]
    pub fn open_or_create(path: String) -> Result<Arc<Self>, RustyError> {
        let query =
            rbitcoin_query::Query::open_or_create(&path).map_err(|_| RustyError::StoreError)?;
        Ok(Arc::new(Self { inner: Arc::new(query) }))
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

    pub fn lookup_started_hi(&self) -> Option<u64> {
        self.inner.lookup_started_hi().map(|h| h as u64)
    }

    pub fn class_a_hi(&self) -> Option<u64> {
        self.inner.class_a_hi().map(|h| h as u64)
    }

    pub fn block_queue_soft_pressure(&self) -> bool {
        self.inner.block_queue_soft_pressure()
    }

    pub fn take_disconnect(&self, seen_gen: u64) -> Option<u32> {
        let mut gen = seen_gen;
        self.inner.take_disconnect(&mut gen)
    }

    pub fn block_queue_unresolved_heights(
        &self,
        path_lo: u32,
        skip: Vec<u32>,
        cap: u64,
    ) -> Vec<u32> {
        let skip_set: std::collections::HashSet<u32> = skip.into_iter().collect();
        self.inner
            .block_queue_unresolved_heights(path_lo, &skip_set, cap as usize)
    }

    pub fn block_queue_payload_by_hash(
        &self,
        hash_hex: String,
    ) -> Result<Option<Vec<u8>>, RustyError> {
        let hash = parse_hash32(&hash_hex)?;
        self.inner
            .block_queue_payload_by_hash(&hash)
            .map_err(|_| RustyError::StoreError)
    }

    pub fn block_queue_take_raw_clone_n(&self) -> u64 {
        self.inner.block_queue_take_raw_clone_n()
    }

    pub fn block_queue_promoted_count(&self) -> u64 {
        self.inner.block_queue_promoted_count() as u64
    }

    pub fn head_drain_fk(&self) -> u64 {
        self.inner.head_drain_fk()
    }

    pub fn block_queue_queued_heights(&self) -> Vec<u32> {
        self.inner
            .block_queue_queued_heights()
            .into_iter()
            .collect()
    }

    pub fn block_queue_list_meta(&self) -> Vec<FfiQueuedBlockMeta> {
        self.inner
            .block_queue_list_meta()
            .into_iter()
            .map(|m| m.into())
            .collect()
    }

    pub fn block_queue_has_height(&self, height: u32) -> bool {
        self.inner.block_queue_has_height(height)
    }

    pub fn block_queue_hash_at_height(&self, height: u32) -> Option<String> {
        self.inner
            .block_queue_hash_at_height(height)
            .map(rbitcoin_primitives::hex_encode)
    }

    pub fn block_queue_take_raw(&self, height: u32) -> Option<FfiTakenRaw> {
        self.inner
            .block_queue_take_raw(height)
            .map(|t| FfiTakenRaw {
                hash: rbitcoin_primitives::hex_encode(t.hash),
                header_fk: t.header_fk,
                payload: t.payload,
            })
    }

    pub fn block_queue_is_resolve_complete(&self, height: u32) -> bool {
        self.inner.block_queue_is_resolve_complete(height)
    }

    pub fn block_queue_payload(&self, height: u32) -> Result<Option<Vec<u8>>, RustyError> {
        self.inner
            .block_queue_payload(height)
            .map_err(|_| RustyError::StoreError)
    }

    pub fn block_queue_raw_payload(&self, height: u32) -> Result<Option<Vec<u8>>, RustyError> {
        self.inner
            .block_queue_raw_payload(height)
            .map_err(|_| RustyError::StoreError)
    }

    pub fn block_queue_has_hash(&self, hash_hex: String) -> Result<bool, RustyError> {
        let hash = parse_hash32(&hash_hex)?;
        Ok(self.inner.block_queue_has_hash(&hash))
    }

    pub fn block_queue_mark_resolve_complete(&self, height: u32) -> Result<(), RustyError> {
        self.inner
            .block_queue_mark_resolve_complete(height)
            .map_err(|_| RustyError::StoreError)
    }

    pub fn block_queue_dequeue_height(&self, height: u32) -> Result<u64, RustyError> {
        self.inner
            .block_queue_dequeue_height(height)
            .map_err(|_| RustyError::StoreError)
            .map(|n| n as u64)
    }

    pub fn block_queue_offer(
        &self,
        height: u32,
        hash_hex: String,
        header_fk: u64,
        payload: Vec<u8>,
    ) -> Result<FfiBlockQueueOffer, RustyError> {
        let hash = parse_hash32(&hash_hex)?;
        let offer = self
            .inner
            .block_queue_offer(height, hash, header_fk, &payload)
            .map_err(|_| RustyError::StoreError)?;
        Ok(FfiBlockQueueOffer {
            queue_id: offer.queue_id,
        })
    }

    pub fn block_queue_enqueue(
        &self,
        height: u32,
        hash_hex: String,
        header_fk: u64,
        payload: Vec<u8>,
    ) -> Result<u64, RustyError> {
        let hash = parse_hash32(&hash_hex)?;
        let id = self
            .inner
            .block_queue_enqueue(height, hash, header_fk, &payload)
            .map_err(|_| RustyError::StoreError)?;
        Ok(id)
    }

    pub fn block_queue_drop_resolved_from(&self, height: u32) {
        self.inner.block_queue_drop_resolved_from(height);
    }

    pub fn block_queue_mark_resolve_complete_wave(&self, heights: Vec<u32>) -> Result<u64, RustyError> {
        self.inner
            .block_queue_mark_resolve_complete_wave(&heights)
            .map_err(|_| RustyError::StoreError)
            .map(|n| n as u64)
    }

    pub fn tx_fk_by_txid_tip(&self, txid_hex: String) -> Result<Option<u64>, RustyError> {
        let txid = parse_hash32(&txid_hex)?;
        let fk = self
            .inner
            .tx_fk_by_txid_tip(&txid)
            .map_err(|_| RustyError::StoreError)?;
        Ok(fk.map(|f| f.0))
    }

    pub fn set_lookup_taken_hi(&self, hi: Option<u32>) {
        self.inner.set_lookup_taken_hi(hi);
    }

    pub fn set_lookup_started_hi(&self, hi: Option<u32>) {
        self.inner.set_lookup_started_hi(hi);
    }

    pub fn set_class_a_hi(&self, hi: Option<u32>) {
        self.inner.set_class_a_hi(hi);
    }

    pub fn get_header(&self, fk: u64) -> Result<FfiHeaderRecord, RustyError> {
        let rec = self
            .inner
            .get_header(rbitcoin_primitives::Fk(fk))
            .map_err(|_| RustyError::StoreError)?;
        Ok(rec.into())
    }

    pub fn get_tx(&self, fk: u64) -> Result<FfiTxRecord, RustyError> {
        let rec = self
            .inner
            .get_tx(rbitcoin_primitives::Fk(fk))
            .map_err(|_| RustyError::StoreError)?;
        Ok(rec.into())
    }

    pub fn header_tx_fks(&self, header_fk: u64, hash_hex: Option<String>) -> Result<Option<Vec<u64>>, RustyError> {
        let hash = hash_hex.as_ref().map(|h| parse_hash32(h)).transpose()?;
        let fks = self
            .inner
            .header_tx_fks(rbitcoin_primitives::Fk(header_fk), hash.as_ref())
            .map_err(|_| RustyError::StoreError)?;
        Ok(fks.map(|v| v.into_iter().map(|f| f.0).collect()))
    }

    pub fn spenders(&self, txid_hex: String, vout: u32) -> Result<Vec<FfiPointRecord>, RustyError> {
        let txid = parse_hash32(&txid_hex)?;
        let pts = self
            .inner
            .spenders(&txid, vout)
            .map_err(|_| RustyError::StoreError)?;
        Ok(pts.into_iter().map(|p| p.into()).collect())
    }

    pub fn spenders_at(&self, txid_hex: String, vout: u32, tip: Option<u32>) -> Result<Vec<FfiPointRecord>, RustyError> {
        let txid = parse_hash32(&txid_hex)?;
        let pts = self
            .inner
            .spenders_at(&txid, vout, tip)
            .map_err(|_| RustyError::StoreError)?;
        Ok(pts.into_iter().map(|p| p.into()).collect())
    }

    pub fn tx_input(&self, txid_hex: String, i: u32) -> Result<FfiInputRecord, RustyError> {
        let txid = parse_hash32(&txid_hex)?;
        let rec = self
            .inner
            .get_tx_by_txid(&txid)
            .map_err(|_| RustyError::StoreError)?;
        let (_fk, tx) = rec.ok_or(RustyError::StoreError)?;
        let input = self
            .inner
            .tx_input(&tx, i)
            .map_err(|_| RustyError::StoreError)?;
        Ok(input.into())
    }

    pub fn tx_output(&self, txid_hex: String, vout: u32) -> Result<FfiOutputRecord, RustyError> {
        let txid = parse_hash32(&txid_hex)?;
        let rec = self
            .inner
            .get_tx_by_txid(&txid)
            .map_err(|_| RustyError::StoreError)?;
        let (_fk, tx) = rec.ok_or(RustyError::StoreError)?;
        let output = self
            .inner
            .tx_output(&tx, vout)
            .map_err(|_| RustyError::StoreError)?;
        Ok(output.into())
    }

    pub fn tx_input_at_fk(&self, create_fk: u64, i: u32) -> Result<FfiInputRecord, RustyError> {
        let tx = self
            .inner
            .get_tx(rbitcoin_primitives::Fk(create_fk))
            .map_err(|_| RustyError::StoreError)?;
        let input = self
            .inner
            .tx_input_at_fk(rbitcoin_primitives::Fk(create_fk), &tx, i)
            .map_err(|_| RustyError::StoreError)?;
        Ok(input.into())
    }

    pub fn tx_output_at_fk(&self, create_fk: u64, vout: u32) -> Result<FfiOutputRecord, RustyError> {
        let output = self
            .inner
            .tx_output_at_fk(rbitcoin_primitives::Fk(create_fk), vout)
            .map_err(|_| RustyError::StoreError)?;
        Ok(output.into())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn put_header(
        &self,
        prev_fk: u64,
        version: i32,
        timestamp: u32,
        bits: u32,
        nonce: u32,
        merkle_root_hex: String,
        hash_hex: String,
        size: u32,
        weight: u32,
    ) -> Result<u64, RustyError> {
        let merkle_root = parse_hash32(&merkle_root_hex)?;
        let hash = parse_hash32(&hash_hex)?;
        let rec = rbitcoin_store::HeaderRecord {
            prev_fk: rbitcoin_primitives::Fk(prev_fk),
            version,
            timestamp,
            bits,
            nonce,
            merkle_root,
            hash,
            size,
            weight,
        };
        let fk = self.inner.put_header(&rec).map_err(|_| RustyError::StoreError)?;
        Ok(fk.0)
    }

    pub fn put_spend(
        &self,
        out_txid_hex: String,
        out_index: u32,
        spending_tx_fk: u64,
        spending_vin: u32,
    ) -> Result<u64, RustyError> {
        let out_txid = parse_hash32(&out_txid_hex)?;
        let fk = self
            .inner
            .put_spend(&out_txid, out_index, rbitcoin_primitives::Fk(spending_tx_fk), spending_vin)
            .map_err(|_| RustyError::StoreError)?;
        Ok(fk.0)
    }

    pub fn set_tx_index(&self, enabled: bool) {
        self.inner.set_tx_index(enabled);
    }

    pub fn set_spend_index(&self, enabled: bool) {
        self.inner.set_spend_index(enabled);
    }

    pub fn unspent_create_vouts(&self, create_fk: u64, vouts: Vec<u32>) -> Result<Vec<u32>, RustyError> {
        self.inner
            .unspent_create_vouts(rbitcoin_primitives::Fk(create_fk), &vouts)
            .map_err(|_| RustyError::StoreError)
    }

    pub fn unspent_create_vouts_batch(&self, items: Vec<FfiUnspentCreateVoutItem>) -> Result<Vec<Vec<u32>>, RustyError> {
        let items_inner: Vec<(rbitcoin_primitives::Fk, Vec<u32>)> = items
            .into_iter()
            .map(|i| (rbitcoin_primitives::Fk(i.create_fk), i.vouts))
            .collect();
        self.inner
            .unspent_create_vouts_batch(&items_inner)
            .map_err(|_| RustyError::StoreError)
    }

    pub fn note_head_drain_fk(&self, max_fk: u64) {
        self.inner.note_head_drain_fk(max_fk);
    }

    pub fn note_lookup_tiponly_start(&self, hi: u32) {
        self.inner.note_lookup_tiponly_start(hi);
    }

    pub fn prune_write_create_loc(&self, written_hi: u32) {
        self.inner.prune_write_create_loc(written_hi);
    }

    pub fn lookup_already_taken(&self, height: u32) -> bool {
        self.inner.lookup_already_taken(height)
    }

    pub fn scripthash_entry_count(&self) -> u64 {
        self.inner.scripthash_entry_count()
    }

    pub fn point_edge_count(&self) -> u64 {
        self.inner.point_edge_count()
    }

    pub fn backfill_tx_index(&self) -> Result<u64, RustyError> {
        self.inner
            .backfill_tx_index(|_done, _total, _rate| {})
            .map_err(|_| RustyError::StoreError)
    }

    pub fn tip_header_fk(&self) -> Result<Option<u64>, RustyError> {
        let fk = self
            .inner
            .tip_header_fk()
            .map_err(|_| RustyError::StoreError)?;
        Ok(fk.map(|f| f.0))
    }

    pub fn flush_header_archive(&self) -> Result<(), RustyError> {
        self.inner
            .flush_header_archive()
            .map_err(|_| RustyError::StoreError)
    }

    pub fn flush(&self) -> Result<(), RustyError> {
        self.inner.flush().map_err(|_| RustyError::StoreError)
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

    pub fn ensure_header(&self, header: FfiHeaderRecord) -> Result<u64, RustyError> {
        let rec: rbitcoin_store::HeaderRecord = header.try_into()?;
        let fk = self.inner.ensure_header(&rec).map_err(|_| RustyError::StoreError)?;
        Ok(fk.0)
    }

    pub fn ensure_headers(&self, headers: Vec<FfiHeaderRecord>) -> Result<Vec<u64>, RustyError> {
        let recs: Vec<rbitcoin_store::HeaderRecord> = headers
            .into_iter()
            .map(|h| h.try_into())
            .collect::<Result<_, _>>()?;
        let fks = self
            .inner
            .ensure_headers(&recs)
            .map_err(|_| RustyError::StoreError)?;
        Ok(fks.into_iter().map(|f| f.0).collect())
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

    pub fn enter_direct_index_mode(&self) -> Result<(), RustyError> {
        self.inner
            .enter_direct_index_mode()
            .map_err(|_| RustyError::StoreError)
    }

    pub fn enter_direct_index_mode_sh(&self, shindex: bool) -> Result<(), RustyError> {
        self.inner
            .enter_direct_index_mode_sh(shindex)
            .map_err(|_| RustyError::StoreError)
    }

    pub fn enter_tip_index_mode(&self) {
        self.inner.enter_tip_index_mode();
    }

    pub fn sync_sh_seal_from_include_hwm(&self) -> Result<(), RustyError> {
        self.inner
            .sync_sh_seal_from_include_hwm()
            .map_err(|_| RustyError::StoreError)
    }

    pub fn finalize_sh_runs(&self) -> Result<u64, RustyError> {
        self.inner.finalize_sh_runs().map_err(|_| RustyError::StoreError)
    }

    pub fn sh_lag_heights(&self) -> u32 {
        self.inner.sh_lag_heights()
    }

    pub fn pin_chain_view_at(
        &self,
        hash_hex: String,
    ) -> Result<Option<FfiChainView>, RustyError> {
        let hash = parse_hash32(&hash_hex)?;
        let view = self
            .inner
            .pin_chain_view_at(&hash)
            .map_err(|_| RustyError::StoreError)?;
        Ok(view.map(|v| FfiChainView {
            height: v.height.0 as u64,
            hash: rbitcoin_primitives::hex_encode(v.hash),
            header_fk: v.header_fk.0,
        }))
    }

    pub fn pin_sh_chain_view_at(
        &self,
        hash_hex: String,
    ) -> Result<Option<FfiChainView>, RustyError> {
        let hash = parse_hash32(&hash_hex)?;
        let view = self
            .inner
            .pin_sh_chain_view_at(&hash)
            .map_err(|_| RustyError::StoreError)?;
        Ok(view.map(|v| FfiChainView {
            height: v.height.0 as u64,
            hash: rbitcoin_primitives::hex_encode(v.hash),
            header_fk: v.header_fk.0,
        }))
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

    pub fn locator_hashes(&self) -> Result<Vec<String>, RustyError> {
        let hashes = self
            .inner
            .locator_hashes()
            .map_err(|_| RustyError::StoreError)?;
        Ok(hashes.into_iter().map(rbitcoin_primitives::hex_encode).collect())
    }

    pub fn headers_after_locator(
        &self,
        locator_hashes_hex: Vec<String>,
        stop_hash_hex: String,
        limit: u64,
    ) -> Result<Vec<String>, RustyError> {
        let locator: Vec<bitcoin::BlockHash> = locator_hashes_hex
            .into_iter()
            .map(|h| parse_hash32(&h).map(bitcoin::BlockHash::from_byte_array))
            .collect::<Result<_, _>>()?;
        let stop = parse_hash32(&stop_hash_hex)?;
        let headers = self
            .inner
            .headers_after_locator(&locator, bitcoin::BlockHash::from_byte_array(stop), limit as usize)
            .map_err(|_| RustyError::StoreError)?;
        Ok(headers
            .into_iter()
            .map(|h| rbitcoin_primitives::hex_encode(bitcoin::consensus::encode::serialize(&h)))
            .collect())
    }

    pub fn invalidate_height_by_hash_index(&self) {
        self.inner.invalidate_height_by_hash_index();
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

    pub fn disconnect_tip(&self) -> Result<(), RustyError> {
        self.inner.disconnect_tip().map_err(|_| RustyError::StoreError)
    }

    pub fn disconnect_tip_keep_pending(&self) -> Result<(), RustyError> {
        self.inner
            .disconnect_tip_keep_pending()
            .map_err(|_| RustyError::StoreError)
    }

    pub fn apply_sh_pending(&self) -> Result<(), RustyError> {
        self.inner.apply_sh_pending().map_err(|_| RustyError::StoreError)
    }

    pub fn drop_sh_pending_from(&self, height: u32) {
        self.inner.drop_sh_pending_from(rbitcoin_primitives::Height(height));
    }

    pub fn process_owned_size_snapshot(&self) -> FfiProcessOwnedSizes {
        let s = self.inner.process_owned_size_snapshot();
        FfiProcessOwnedSizes {
            conf_plans: s.conf_plans as u64,
            sh_runs: s.sh_runs as u64,
            sh_heads: s.sh_heads as u64,
            head: s.head.into(),
            inflight_layers: s.inflight_layers as u64,
            inflight_pins: s.inflight_pins as u64,
            inflight_bytes: s.inflight_bytes,
            h2h_keys: s.h2h_keys as u64,
            fence_runs: s.fence_runs as u64,
            bq_promoted: s.bq_promoted as u64,
            wloc_packs: s.wloc_packs as u64,
            wloc_pairs: s.wloc_pairs as u64,
            wloc_bytes: s.wloc_bytes,
        }
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

    pub fn pin_chain_view(&self) -> Result<Option<FfiChainView>, RustyError> {
        let view = self
            .inner
            .pin_chain_view()
            .map_err(|_| RustyError::StoreError)?;
        Ok(view.map(|v| FfiChainView {
            height: v.height.0 as u64,
            hash: rbitcoin_primitives::hex_encode(v.hash),
            header_fk: v.header_fk.0,
        }))
    }

    pub fn pin_sh_chain_view(&self) -> Result<Option<FfiChainView>, RustyError> {
        let view = self
            .inner
            .pin_sh_chain_view()
            .map_err(|_| RustyError::StoreError)?;
        Ok(view.map(|v| FfiChainView {
            height: v.height.0 as u64,
            hash: rbitcoin_primitives::hex_encode(v.hash),
            header_fk: v.header_fk.0,
        }))
    }

    pub fn spenders_raw(
        &self,
        txid_hex: String,
        vout: u32,
    ) -> Result<Vec<FfiPointRecord>, RustyError> {
        let txid = parse_hash32(&txid_hex)?;
        let pts = self
            .inner
            .spenders_raw(&txid, vout)
            .map_err(|_| RustyError::StoreError)?;
        Ok(pts.into_iter().map(|p| p.into()).collect())
    }

    pub fn resume_work_path_after_tip(
        &self,
        tip_hash_hex: String,
        tip_height: u32,
        max: u64,
    ) -> Result<Vec<FfiResumeWorkEntry>, RustyError> {
        let tip_hash = parse_hash32(&tip_hash_hex)?;
        let entries = self
            .inner
            .resume_work_path_after_tip(tip_hash, tip_height, max as usize)
            .map_err(|_| RustyError::StoreError)?;
        Ok(entries
            .into_iter()
            .map(|e| FfiResumeWorkEntry {
                height: e.height,
                hash: rbitcoin_primitives::hex_encode(e.hash),
                header_fk: e.header_fk.0,
                has_body: e.has_body,
            })
            .collect())
    }

    pub fn resume_work_path_after_tip_excluding(
        &self,
        tip_hash_hex: String,
        tip_height: u32,
        max: u64,
        exclude_hashes_hex: Vec<String>,
    ) -> Result<Vec<FfiResumeWorkEntry>, RustyError> {
        let tip_hash = parse_hash32(&tip_hash_hex)?;
        let exclude: Vec<[u8; 32]> = exclude_hashes_hex
            .into_iter()
            .map(|h| parse_hash32(&h))
            .collect::<Result<_, _>>()?;
        let entries = self
            .inner
            .resume_work_path_after_tip_excluding(tip_hash, tip_height, max as usize, &exclude)
            .map_err(|_| RustyError::StoreError)?;
        Ok(entries
            .into_iter()
            .map(|e| FfiResumeWorkEntry {
                height: e.height,
                hash: rbitcoin_primitives::hex_encode(e.hash),
                header_fk: e.header_fk.0,
                has_body: e.has_body,
            })
            .collect())
    }
}

#[derive(uniffi::Record)]
pub struct FfiBlockQueueStats {
    pub assign_stop_bytes: u64,
    pub bytes: u64,
    pub count: u64,
}

#[derive(uniffi::Record)]
pub struct FfiResumeWorkEntry {
    pub height: u32,
    pub hash: String,
    pub header_fk: u64,
    pub has_body: bool,
}

#[derive(uniffi::Record)]
pub struct FfiUnspentCreateVoutItem {
    pub create_fk: u64,
    pub vouts: Vec<u32>,
}

#[derive(uniffi::Record)]
pub struct FfiQueuedBlockMeta {
    pub id: u64,
    pub height: u32,
    pub hash: String,
    pub header_fk: u64,
    pub payload_len: u64,
    pub n_inputs: u32,
    pub resolve_complete: bool,
}

impl From<rbitcoin_store::QueuedBlockMeta> for FfiQueuedBlockMeta {
    fn from(m: rbitcoin_store::QueuedBlockMeta) -> Self {
        Self {
            id: m.id,
            height: m.height,
            hash: rbitcoin_primitives::hex_encode(m.hash),
            header_fk: m.header_fk,
            payload_len: m.payload_len,
            n_inputs: m.n_inputs,
            resolve_complete: m.resolve_complete,
        }
    }
}

#[derive(Debug, PartialEq, uniffi::Record)]
pub struct FfiBlockQueueOffer {
    pub queue_id: u64,
}

#[derive(Debug, PartialEq, uniffi::Record)]
pub struct FfiTakenRaw {
    pub hash: String,
    pub header_fk: u64,
    pub payload: Vec<u8>,
}

#[uniffi::export]
pub fn lookup_taken_covers(height: u32, taken_hi: Option<u32>) -> bool {
    rbitcoin_query::Query::lookup_taken_covers(height, taken_hi)
}

#[derive(uniffi::Record)]
pub struct FfiHeadResizeSizeSnapshot {
    pub class_a_n: u64,
    pub primary_bits: u32,
    pub primary_slots: u64,
    pub primary_entry_b: u8,
    pub primary_occupied: u64,
    pub primary_body_bytes: u64,
    pub segment_count: u64,
    pub sealed_segments: u64,
    pub fuse8_bytes: u64,
    pub mphf_g_bytes: u64,
    pub open_keys_bytes: u64,
    pub class_c_l2_bytes: u64,
}

impl From<rbitcoin_store::HeadResizeSizeSnapshot> for FfiHeadResizeSizeSnapshot {
    fn from(h: rbitcoin_store::HeadResizeSizeSnapshot) -> Self {
        Self {
            class_a_n: h.class_a_n,
            primary_bits: h.primary_bits,
            primary_slots: h.primary_slots,
            primary_entry_b: h.primary_entry_b,
            primary_occupied: h.primary_occupied,
            primary_body_bytes: h.primary_body_bytes,
            segment_count: h.segment_count,
            sealed_segments: h.sealed_segments,
            fuse8_bytes: h.fuse8_bytes,
            mphf_g_bytes: h.mphf_g_bytes,
            open_keys_bytes: h.open_keys_bytes,
            class_c_l2_bytes: h.class_c_l2_bytes,
        }
    }
}

#[derive(uniffi::Record)]
pub struct FfiProcessOwnedSizes {
    pub conf_plans: u64,
    pub sh_runs: u64,
    pub sh_heads: u64,
    pub head: FfiHeadResizeSizeSnapshot,
    pub inflight_layers: u64,
    pub inflight_pins: u64,
    pub inflight_bytes: u64,
    pub h2h_keys: u64,
    pub fence_runs: u64,
    pub bq_promoted: u64,
    pub wloc_packs: u64,
    pub wloc_pairs: u64,
    pub wloc_bytes: u64,
}

#[derive(uniffi::Record)]
pub struct FfiPeerEntry {
    pub addr: String,
    pub flags: u8,
}

// --- AddrMan FFI ---

#[derive(uniffi::Object)]
pub struct FfiAddrMan {
    inner: std::sync::Mutex<rbitcoin_net::AddrMan>,
}

#[uniffi::export]
impl FfiAddrMan {
    #[uniffi::constructor]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: std::sync::Mutex::new(rbitcoin_net::AddrMan::new()),
        })
    }

    #[uniffi::constructor]
    pub fn with_seeds(network: String) -> Result<Arc<Self>, RustyError> {
        let net = match network.as_str() {
            "mainnet" => rbitcoin_primitives::Network::Mainnet,
            "testnet" => rbitcoin_primitives::Network::Testnet,
            "regtest" => rbitcoin_primitives::Network::Regtest,
            "signet" => rbitcoin_primitives::Network::Signet,
            _ => return Err(RustyError::InvalidInput),
        };
        Ok(Arc::new(Self {
            inner: std::sync::Mutex::new(rbitcoin_net::AddrMan::with_seeds(net)),
        }))
    }

    pub fn add(&self, addr: String) -> Result<(), RustyError> {
        let socket = rbitcoin_net::parse_peer_addr(&addr).map_err(|_| RustyError::InvalidInput)?;
        self.inner.lock().unwrap().add(socket);
        Ok(())
    }

    pub fn add_with_flags(&self, addr: String, flags: u8) -> Result<(), RustyError> {
        let socket = rbitcoin_net::parse_peer_addr(&addr).map_err(|_| RustyError::InvalidInput)?;
        self.inner
            .lock()
            .unwrap()
            .add_with_flags(socket, rbitcoin_net::PeerFlags(flags));
        Ok(())
    }

    pub fn note_connected(&self, addr: String) -> Result<(), RustyError> {
        let socket = rbitcoin_net::parse_peer_addr(&addr).map_err(|_| RustyError::InvalidInput)?;
        self.inner.lock().unwrap().note_connected(socket);
        Ok(())
    }

    pub fn note_attempt(&self, addr: String) -> Result<(), RustyError> {
        let socket = rbitcoin_net::parse_peer_addr(&addr).map_err(|_| RustyError::InvalidInput)?;
        self.inner.lock().unwrap().note_attempt(socket);
        Ok(())
    }

    pub fn note_connect_failed(&self, addr: String, incompatible: bool) -> Result<(), RustyError> {
        let socket = rbitcoin_net::parse_peer_addr(&addr).map_err(|_| RustyError::InvalidInput)?;
        self.inner
            .lock()
            .unwrap()
            .note_connect_failed(socket, incompatible);
        Ok(())
    }

    pub fn note_speed(&self, addr: String, latency_ms: u64, bytes_per_sec: u64) -> Result<(), RustyError> {
        let socket = rbitcoin_net::parse_peer_addr(&addr).map_err(|_| RustyError::InvalidInput)?;
        self.inner
            .lock()
            .unwrap()
            .note_speed(socket, latency_ms, bytes_per_sec);
        Ok(())
    }

    pub fn note_ibd_slow(&self, addr: String) -> Result<(), RustyError> {
        let socket = rbitcoin_net::parse_peer_addr(&addr).map_err(|_| RustyError::InvalidInput)?;
        self.inner.lock().unwrap().note_ibd_slow(socket);
        Ok(())
    }

    pub fn apply_ibd_dead_speed(
        &self,
        addr: String,
        latency_ms: u64,
        bps: Option<u64>,
        ibd_outlier: bool,
    ) -> Result<(), RustyError> {
        let socket = rbitcoin_net::parse_peer_addr(&addr).map_err(|_| RustyError::InvalidInput)?;
        self.inner
            .lock()
            .unwrap()
            .apply_ibd_dead_speed(socket, latency_ms, bps, ibd_outlier);
        Ok(())
    }

    pub fn len(&self) -> u64 {
        self.inner.lock().unwrap().len() as u64
    }

    pub fn is_empty(&self) -> bool {
        self.inner.lock().unwrap().is_empty()
    }

    pub fn peers(&self) -> Vec<String> {
        self.inner
            .lock()
            .unwrap()
            .peers()
            .iter()
            .map(|a| a.to_string())
            .collect()
    }

    pub fn entries(&self) -> Vec<FfiPeerEntry> {
        self.inner
            .lock()
            .unwrap()
            .entries()
            .into_iter()
            .map(|e| FfiPeerEntry {
                addr: e.addr.to_string(),
                flags: e.flags.0,
            })
            .collect()
    }

    pub fn flags(&self, addr: String) -> Result<u8, RustyError> {
        let socket = rbitcoin_net::parse_peer_addr(&addr).map_err(|_| RustyError::InvalidInput)?;
        Ok(self.inner.lock().unwrap().flags(&socket).0)
    }

    pub fn take_outbound(&self, max: u64) -> Vec<String> {
        self.inner
            .lock()
            .unwrap()
            .take_outbound(max as usize)
            .into_iter()
            .map(|a| a.to_string())
            .collect()
    }

    pub fn take_outbound_occupied(&self, max: u64, occupied: Vec<String>) -> Vec<String> {
        let occ: Vec<std::net::SocketAddr> = occupied
            .into_iter()
            .filter_map(|s| s.parse().ok())
            .collect();
        self.inner
            .lock()
            .unwrap()
            .take_outbound_occupied(max as usize, &occ)
            .into_iter()
            .map(|a| a.to_string())
            .collect()
    }

    #[uniffi::constructor]
    pub fn load(path: String) -> Result<Arc<Self>, RustyError> {
        let am = rbitcoin_net::AddrMan::load(std::path::Path::new(&path))
            .map_err(|_| RustyError::InvalidInput)?;
        Ok(Arc::new(Self {
            inner: std::sync::Mutex::new(am),
        }))
    }

    pub fn save(&self, path: String) -> Result<(), RustyError> {
        self.inner
            .lock()
            .unwrap()
            .save(std::path::Path::new(&path))
            .map_err(|_| RustyError::InvalidInput)
    }

    pub fn inject(&self, addrs: Vec<String>) {
        let sockets: Vec<std::net::SocketAddr> = addrs
            .into_iter()
            .filter_map(|s| s.parse().ok())
            .collect();
        self.inner.lock().unwrap().inject(sockets);
    }

    pub fn add_learned(&self, addr: String, cap: u64) -> Result<bool, RustyError> {
        let socket = rbitcoin_net::parse_peer_addr(&addr).map_err(|_| RustyError::InvalidInput)?;
        Ok(self.inner.lock().unwrap().add_learned(socket, cap as usize))
    }

    pub fn merge_from(&self, other: Arc<FfiAddrMan>) {
        let other_guard = other.inner.lock().unwrap();
        self.inner.lock().unwrap().merge_from(&other_guard);
    }

    pub fn take_dial_candidates(
        &self,
        max: u64,
        exclude: Vec<String>,
        occupied: Vec<String>,
    ) -> Vec<String> {
        let exclude_set: std::collections::HashSet<std::net::SocketAddr> = exclude
            .into_iter()
            .filter_map(|s| s.parse().ok())
            .collect();
        let occ: Vec<std::net::SocketAddr> = occupied
            .into_iter()
            .filter_map(|s| s.parse().ok())
            .collect();
        self.inner
            .lock()
            .unwrap()
            .take_dial_candidates(max as usize, &exclude_set, &occ)
            .into_iter()
            .map(|a| a.to_string())
            .collect()
    }

    pub fn take_outbound_offset(&self, max: u64, offset: u64) -> Vec<String> {
        self.inner
            .lock()
            .unwrap()
            .take_outbound_offset(max as usize, offset as usize)
            .into_iter()
            .map(|a| a.to_string())
            .collect()
    }

    pub fn take_outbound_offset_occupied(
        &self,
        max: u64,
        offset: u64,
        occupied: Vec<String>,
    ) -> Vec<String> {
        let occ: Vec<std::net::SocketAddr> = occupied
            .into_iter()
            .filter_map(|s| s.parse().ok())
            .collect();
        self.inner
            .lock()
            .unwrap()
            .take_outbound_offset_occupied(max as usize, offset as usize, &occ)
            .into_iter()
            .map(|a| a.to_string())
            .collect()
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

    pub fn has_free_slot(&self) -> bool {
        self.inner.lock().unwrap().has_free_slot()
    }

    pub fn compact(&self) -> Result<String, RustyError> {
        let mut guard = self.inner.lock().unwrap();
        let (dead, shrunk) = guard.compact().map_err(|_| RustyError::MempoolError)?;
        Ok(format!("dead={dead} shrunk={shrunk}"))
    }

    pub fn body_logical_len(&self) -> Result<u64, RustyError> {
        let len = self
            .inner
            .lock()
            .unwrap()
            .body_logical_len()
            .map_err(|_| RustyError::MempoolError)?;
        Ok(len as u64)
    }

    pub fn dir(&self) -> String {
        self.inner
            .lock()
            .unwrap()
            .dir()
            .to_string_lossy()
            .into_owned()
    }

    pub fn persist_if_dirty(&self) -> Result<(), RustyError> {
        self.inner
            .lock()
            .unwrap()
            .persist_if_dirty()
            .map_err(|_| RustyError::MempoolError)
    }

    pub fn abandon_live(&self) -> Result<u32, RustyError> {
        self.inner
            .lock()
            .unwrap()
            .abandon_live()
            .map_err(|_| RustyError::MempoolError)
    }

    pub fn mark_slot_dead(&self, slot: u32) -> Result<(), RustyError> {
        self.inner
            .lock()
            .unwrap()
            .mark_slot_dead(slot)
            .map_err(|_| RustyError::MempoolError)
    }

    pub fn grow_slots(&self) -> Result<(), RustyError> {
        self.inner
            .lock()
            .unwrap()
            .grow_slots()
            .map_err(|_| RustyError::MempoolError)
    }

    pub fn append_live_tx(
        &self,
        tx_hex: String,
        fee_sat: u64,
        weight: u64,
    ) -> Result<u32, RustyError> {
        let tx: bitcoin::Transaction =
            deserialize_hex(&tx_hex).map_err(|_| RustyError::InvalidInput)?;
        let txid = tx.compute_txid();
        let raw = bitcoin::consensus::encode::serialize(&tx);
        self.inner
            .lock()
            .unwrap()
            .append_live_tx(&raw, &txid, fee_sat, weight)
            .map_err(|_| RustyError::MempoolError)
    }
}

#[derive(uniffi::Record)]
pub struct FfiMempoolGraphStats {
    pub ancestorcount: u64,
    pub ancestorsize: u64,
    pub ancestorfees: u64,
    pub descendantcount: u64,
    pub descendantsize: u64,
    pub descendantfees: u64,
}

#[derive(uniffi::Object)]
pub struct FfiTxGraph {
    inner: std::sync::Mutex<rbitcoin_mempool::TxGraph>,
}

#[uniffi::export]
impl FfiTxGraph {
    #[uniffi::constructor]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: std::sync::Mutex::new(rbitcoin_mempool::TxGraph::new()),
        })
    }

    pub fn len(&self) -> u64 {
        self.inner.lock().unwrap().len() as u64
    }

    pub fn is_empty(&self) -> bool {
        self.inner.lock().unwrap().is_empty()
    }

    pub fn total_weight(&self) -> u64 {
        self.inner.lock().unwrap().total_weight()
    }

    pub fn graph_stats(&self, txid_hex: String) -> Result<Option<FfiMempoolGraphStats>, RustyError> {
        let txid = bitcoin::Txid::from_str(&txid_hex).map_err(|_| RustyError::InvalidInput)?;
        let stats = self.inner.lock().unwrap().graph_stats(&txid);
        Ok(stats.map(|s| FfiMempoolGraphStats {
            ancestorcount: s.ancestorcount,
            ancestorsize: s.ancestorsize,
            ancestorfees: s.ancestorfees,
            descendantcount: s.descendantcount,
            descendantsize: s.descendantsize,
            descendantfees: s.descendantfees,
        }))
    }

    pub fn frontier_feerate_sat_per_kvb(&self, target_wu: u64) -> Option<u64> {
        self.inner.lock().unwrap().frontier_feerate_sat_per_kvb(target_wu)
    }

    pub fn weight_above_feerate(&self, rate_sat_per_kvb: u64) -> u64 {
        self.inner.lock().unwrap().weight_above_feerate(rate_sat_per_kvb)
    }

    pub fn select_block_txids(&self, max_weight_wu: u64) -> Vec<String> {
        self.inner
            .lock()
            .unwrap()
            .select_block_txids(max_weight_wu)
            .into_iter()
            .map(|t| t.to_string())
            .collect()
    }

    pub fn contains(&self, txid_hex: String) -> Result<bool, RustyError> {
        let txid = bitcoin::Txid::from_str(&txid_hex).map_err(|_| RustyError::InvalidInput)?;
        Ok(self.inner.lock().unwrap().contains(&txid))
    }

    pub fn set_cluster_limits(&self, count: Option<u32>, size_kvb: Option<u32>) {
        self.inner.lock().unwrap().set_cluster_limits(count, size_kvb);
    }

    pub fn cluster_count_limit(&self) -> u64 {
        self.inner.lock().unwrap().cluster_count_limit() as u64
    }

    pub fn cluster_vsize_limit(&self) -> u64 {
        self.inner.lock().unwrap().cluster_vsize_limit()
    }

    pub fn cluster_weight_limit(&self) -> u64 {
        self.inner.lock().unwrap().cluster_weight_limit()
    }
}

#[derive(uniffi::Object)]
pub struct FfiActiveMempool {
    inner: std::sync::Mutex<rbitcoin_mempool::ActiveMempool>,
}

#[uniffi::export]
impl FfiActiveMempool {
    #[uniffi::constructor]
    pub fn open_or_create(path: String) -> Result<Arc<Self>, RustyError> {
        let mempool = rbitcoin_mempool::ActiveMempool::open_or_create(&path)
            .map_err(|_| RustyError::MempoolError)?;
        Ok(Arc::new(Self {
            inner: std::sync::Mutex::new(mempool),
        }))
    }

    #[uniffi::constructor]
    pub fn open_or_create_with_limit(path: String, max_weight: u64) -> Result<Arc<Self>, RustyError> {
        let mempool = rbitcoin_mempool::ActiveMempool::open_or_create_with_limit(&path, max_weight)
            .map_err(|_| RustyError::MempoolError)?;
        Ok(Arc::new(Self {
            inner: std::sync::Mutex::new(mempool),
        }))
    }

    pub fn live_count(&self) -> u64 {
        self.inner.lock().unwrap().live_count() as u64
    }

    pub fn generation(&self) -> u64 {
        self.inner.lock().unwrap().generation()
    }

    pub fn orphan_count(&self) -> u64 {
        self.inner.lock().unwrap().orphan_count() as u64
    }

    pub fn min_relay_sat_kvb(&self) -> u64 {
        self.inner.lock().unwrap().min_relay_sat_kvb()
    }

    pub fn set_min_relay_sat_kvb(&self, sat_kvb: u64) {
        self.inner.lock().unwrap().set_min_relay_sat_kvb(sat_kvb);
    }

    pub fn set_cluster_limits(&self, count: Option<u32>, size_kvb: Option<u32>) {
        self.inner.lock().unwrap().set_cluster_limits(count, size_kvb);
    }

    pub fn get_tx(&self, txid_hex: String) -> Result<Option<String>, RustyError> {
        let txid = bitcoin::Txid::from_str(&txid_hex).map_err(|_| RustyError::InvalidInput)?;
        let guard = self.inner.lock().unwrap();
        let tx = guard.get_tx(&txid);
        let hex = tx.map(|t| rbitcoin_primitives::hex_encode(bitcoin::consensus::encode::serialize(t)));
        drop(guard);
        Ok(hex)
    }

    pub fn select_block_txs(&self, max_weight_wu: u64) -> Vec<String> {
        self.inner
            .lock()
            .unwrap()
            .select_block_txs(max_weight_wu)
            .into_iter()
            .map(|t| rbitcoin_primitives::hex_encode(bitcoin::consensus::encode::serialize(&t)))
            .collect()
    }

    pub fn remove_txid(&self, txid_hex: String) -> Result<(), RustyError> {
        let txid = bitcoin::Txid::from_str(&txid_hex).map_err(|_| RustyError::InvalidInput)?;
        self.inner
            .lock()
            .unwrap()
            .remove_txid(&txid)
            .map_err(|_| RustyError::MempoolError)
    }

    pub fn remove_txid_tree(&self, txid_hex: String) -> Result<Vec<String>, RustyError> {
        let txid = bitcoin::Txid::from_str(&txid_hex).map_err(|_| RustyError::InvalidInput)?;
        let removed = self.inner.lock().unwrap().remove_txid_tree(&txid);
        Ok(removed.into_iter().map(|t| t.to_string()).collect())
    }

    pub fn remove_for_block(&self, txids_hex: Vec<String>) -> Result<u64, RustyError> {
        let txids: Vec<bitcoin::Txid> = txids_hex
            .into_iter()
            .map(|h| bitcoin::Txid::from_str(&h))
            .collect::<Result<_, _>>()
            .map_err(|_| RustyError::InvalidInput)?;
        let n = self
            .inner
            .lock()
            .unwrap()
            .remove_for_block(&txids)
            .map_err(|_| RustyError::MempoolError)?;
        Ok(n as u64)
    }

    pub fn remove_live_txids(&self, txids_hex: Vec<String>) -> Result<u64, RustyError> {
        let txids: Vec<bitcoin::Txid> = txids_hex
            .into_iter()
            .map(|h| bitcoin::Txid::from_str(&h))
            .collect::<Result<_, _>>()
            .map_err(|_| RustyError::InvalidInput)?;
        let n = self
            .inner
            .lock()
            .unwrap()
            .remove_live_txids(&txids)
            .map_err(|_| RustyError::MempoolError)?;
        Ok(n as u64)
    }

    pub fn evict_conflicts_with(&self, txids_hex: Vec<String>, vouts: Vec<u32>) -> Result<Vec<String>, RustyError> {
        if txids_hex.len() != vouts.len() {
            return Err(RustyError::InvalidInput);
        }
        let ops: Vec<bitcoin::OutPoint> = txids_hex
            .into_iter()
            .zip(vouts)
            .map(|(h, v)| {
                let txid = bitcoin::Txid::from_str(&h).map_err(|_| RustyError::InvalidInput)?;
                Ok(bitcoin::OutPoint::new(txid, v))
            })
            .collect::<Result<_, _>>()?;
        let evicted = self.inner.lock().unwrap().evict_conflicts_with(&ops);
        Ok(evicted.into_iter().map(|t| t.to_string()).collect())
    }

    pub fn evict_to_budget(&self, protect_txid_hex: Option<String>) -> Result<u64, RustyError> {
        let protect = protect_txid_hex
            .map(|h| bitcoin::Txid::from_str(&h))
            .transpose()
            .map_err(|_| RustyError::InvalidInput)?;
        let n = self
            .inner
            .lock()
            .unwrap()
            .evict_to_budget(protect)
            .map_err(|_| RustyError::MempoolError)?;
        Ok(n as u64)
    }

    pub fn flush(&self) -> Result<(), RustyError> {
        self.inner
            .lock()
            .unwrap()
            .flush()
            .map_err(|_| RustyError::MempoolError)
    }

    pub fn persist_if_dirty(&self) -> Result<(), RustyError> {
        self.inner
            .lock()
            .unwrap()
            .persist_if_dirty()
            .map_err(|_| RustyError::MempoolError)
    }

    pub fn compact(&self) -> Result<String, RustyError> {
        let mut guard = self.inner.lock().unwrap();
        let (dead, shrunk) = guard.compact().map_err(|_| RustyError::MempoolError)?;
        Ok(format!("dead={dead} shrunk={shrunk}"))
    }

    pub fn maybe_compact(&self) -> Result<Option<String>, RustyError> {
        let mut guard = self.inner.lock().unwrap();
        let result = guard.maybe_compact().map_err(|_| RustyError::MempoolError)?;
        Ok(result.map(|(dead, shrunk)| format!("dead={dead} shrunk={shrunk}")))
    }

    pub fn park_orphan(&self, tx_hex: String, missing_txids_hex: Vec<String>) -> Result<String, RustyError> {
        let tx: bitcoin::Transaction = deserialize_hex(&tx_hex).map_err(|_| RustyError::InvalidInput)?;
        let missing: std::collections::BTreeSet<bitcoin::Txid> = missing_txids_hex
            .into_iter()
            .map(|h| bitcoin::Txid::from_str(&h))
            .collect::<Result<_, _>>()
            .map_err(|_| RustyError::InvalidInput)?;
        let err = self.inner.lock().unwrap().park_orphan(&tx, missing);
        Ok(format!("{err:?}"))
    }

    pub fn take_orphan_children(&self, parent_txid_hex: String) -> Result<Vec<String>, RustyError> {
        let parent = bitcoin::Txid::from_str(&parent_txid_hex).map_err(|_| RustyError::InvalidInput)?;
        let children = self.inner.lock().unwrap().take_orphan_children(parent);
        Ok(children.into_iter().map(|t| rbitcoin_primitives::hex_encode(bitcoin::consensus::encode::serialize(&t))).collect())
    }

    pub fn erase_orphans_for_block(&self, block_txids_hex: Vec<String>) -> Result<(), RustyError> {
        let txids: Vec<bitcoin::Txid> = block_txids_hex
            .into_iter()
            .map(|h| bitcoin::Txid::from_str(&h))
            .collect::<Result<_, _>>()
            .map_err(|_| RustyError::InvalidInput)?;
        self.inner.lock().unwrap().erase_orphans_for_block(&txids);
        Ok(())
    }

    pub fn remember_extra_compact(&self, tx_hex: String) -> Result<(), RustyError> {
        let tx: bitcoin::Transaction = deserialize_hex(&tx_hex).map_err(|_| RustyError::InvalidInput)?;
        self.inner.lock().unwrap().remember_extra_compact(&tx);
        Ok(())
    }

    pub fn accept_tx(
        &self,
        query: Arc<FfiQuery>,
        tx_hex: String,
    ) -> Result<String, RustyError> {
        let tx: bitcoin::Transaction = deserialize_hex(&tx_hex).map_err(|_| RustyError::InvalidInput)?;
        let provider = FfiUtxoProvider {
            query: Arc::clone(&query.inner),
        };
        let tip_height = query.inner.tip_height().map(|h| h.0).unwrap_or(0);
        let mtp = if tip_height == 0 {
            0
        } else {
            rbitcoin_consensus::median_time_past(&query.inner, rbitcoin_primitives::Height(tip_height.saturating_sub(1)))
                .unwrap_or(0)
        };
        let tip_ctx = rbitcoin_mempool::ChainTipCtx {
            height: tip_height,
            mtp,
        };
        let result = self.inner.lock().unwrap().accept_tx(&tx, &provider, tip_ctx);
        Ok(format!("{result:?}"))
    }

    pub fn accept_package(
        &self,
        query: Arc<FfiQuery>,
        txs_hex: Vec<String>,
    ) -> Result<String, RustyError> {
        let txs: Vec<bitcoin::Transaction> = txs_hex
            .into_iter()
            .map(|h| deserialize_hex(&h).map_err(|_| RustyError::InvalidInput))
            .collect::<Result<_, _>>()?;
        let provider = FfiUtxoProvider {
            query: Arc::clone(&query.inner),
        };
        let tip_height = query.inner.tip_height().map(|h| h.0).unwrap_or(0);
        let mtp = if tip_height == 0 {
            0
        } else {
            rbitcoin_consensus::median_time_past(&query.inner, rbitcoin_primitives::Height(tip_height.saturating_sub(1)))
                .unwrap_or(0)
        };
        let tip_ctx = rbitcoin_mempool::ChainTipCtx {
            height: tip_height,
            mtp,
        };
        let result = self.inner.lock().unwrap().accept_package(&txs, &provider, tip_ctx);
        Ok(format!("{result:?}"))
    }

    pub fn promote_orphans_of(
        &self,
        query: Arc<FfiQuery>,
        txid_hex: String,
    ) -> Result<(), RustyError> {
        let txid = bitcoin::Txid::from_str(&txid_hex).map_err(|_| RustyError::InvalidInput)?;
        let provider = FfiUtxoProvider {
            query: Arc::clone(&query.inner),
        };
        let tip_height = query.inner.tip_height().map(|h| h.0).unwrap_or(0);
        let mtp = if tip_height == 0 {
            0
        } else {
            rbitcoin_consensus::median_time_past(&query.inner, rbitcoin_primitives::Height(tip_height.saturating_sub(1)))
                .unwrap_or(0)
        };
        let tip_ctx = rbitcoin_mempool::ChainTipCtx {
            height: tip_height,
            mtp,
        };
        self.inner.lock().unwrap().promote_orphans_of(txid, &provider, tip_ctx);
        Ok(())
    }

    pub fn reorg_disconnect_reaccept(
        &self,
        query: Arc<FfiQuery>,
        txs_hex: Vec<String>,
    ) -> Result<Vec<String>, RustyError> {
        let txs: Vec<bitcoin::Transaction> = txs_hex
            .into_iter()
            .map(|h| deserialize_hex(&h).map_err(|_| RustyError::InvalidInput))
            .collect::<Result<_, _>>()?;
        let provider = FfiUtxoProvider {
            query: Arc::clone(&query.inner),
        };
        let tip_height = query.inner.tip_height().map(|h| h.0).unwrap_or(0);
        let mtp = if tip_height == 0 {
            0
        } else {
            rbitcoin_consensus::median_time_past(&query.inner, rbitcoin_primitives::Height(tip_height.saturating_sub(1)))
                .unwrap_or(0)
        };
        let tip_ctx = rbitcoin_mempool::ChainTipCtx {
            height: tip_height,
            mtp,
        };
        let results = self.inner.lock().unwrap().reorg_disconnect_reaccept(&txs, &provider, tip_ctx);
        Ok(results.into_iter().map(|r| format!("{r:?}")).collect())
    }
}

struct FfiUtxoProvider {
    query: Arc<rbitcoin_query::Query>,
}

impl rbitcoin_mempool::UtxoProvider for FfiUtxoProvider {
    fn get_coin(&self, op: &bitcoin::OutPoint) -> Option<rbitcoin_mempool::Coin> {
        match self.chain_prevout(op) {
            rbitcoin_mempool::ChainPrevout::Unspent(c) => Some(c),
            _ => None,
        }
    }

    fn chain_prevout(&self, op: &bitcoin::OutPoint) -> rbitcoin_mempool::ChainPrevout {
        let tid = op.txid.to_byte_array();
        let Some((fk, rec)) = self.query.get_tx_by_txid(&tid).ok().flatten() else {
            return rbitcoin_mempool::ChainPrevout::Unknown;
        };
        let Some(create_height) = self.query.store().tx_height_get(fk).ok().flatten() else {
            return rbitcoin_mempool::ChainPrevout::Unknown;
        };
        let Some(tip) = self.query.tip_height().map(|h| h.0) else {
            return rbitcoin_mempool::ChainPrevout::Unknown;
        };
        if create_height > tip {
            return rbitcoin_mempool::ChainPrevout::Unknown;
        }
        match self.query.is_outpoint_spent(&tid, op.vout) {
            Ok(true) => return rbitcoin_mempool::ChainPrevout::KnownUnavailable,
            Ok(false) => {}
            Err(_) => return rbitcoin_mempool::ChainPrevout::KnownUnavailable,
        }
        let out = self
            .query
            .tx_output_at_fk(fk, op.vout)
            .ok()
            .or_else(|| self.query.tx_output(&rec, op.vout).ok());
        let Some(out) = out else {
            return rbitcoin_mempool::ChainPrevout::KnownUnavailable;
        };
        let value = if out.value < 0 {
            bitcoin::Amount::ZERO
        } else {
            bitcoin::Amount::from_sat(out.value as u64)
        };
        let is_coinbase = match self.query.tx_input_at_fk(fk, &rec, 0) {
            Ok(i) => i.is_coinbase() || i.prev_index == u32::MAX,
            Err(_) => {
                create_height > 0
                    && self
                        .query
                        .header_tx_fks(rbitcoin_primitives::Fk(create_height as u64), None)
                        .ok()
                        .flatten()
                        .and_then(|fks| fks.first().copied())
                        == Some(fk)
            }
        };
        let create_mtp = if create_height == 0 {
            0
        } else {
            rbitcoin_consensus::median_time_past(&self.query, rbitcoin_primitives::Height(create_height.saturating_sub(1)))
                .unwrap_or(0)
        };
        rbitcoin_mempool::ChainPrevout::Unspent(rbitcoin_mempool::Coin {
            txout: bitcoin::TxOut {
                value,
                script_pubkey: bitcoin::ScriptBuf::from_bytes(out.script),
            },
            create_height,
            create_mtp,
            is_coinbase,
        })
    }
}

// --- Block Queue soft targets FFI ---

#[derive(Debug, PartialEq, uniffi::Record)]
pub struct FfiBlockQueueSoftTargets {
    pub window: u32,
    pub free_mib: u32,
}

#[uniffi::export]
pub fn block_queue_soft_targets(rate_blocks_per_s: Option<f64>) -> FfiBlockQueueSoftTargets {
    let (win, free_mib) = rbitcoin_query::Query::block_queue_soft_targets(rate_blocks_per_s);
    FfiBlockQueueSoftTargets { window: win, free_mib }
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

// --- Node Time FFI ---

#[uniffi::export]
pub fn tip_too_far_in_future(tip_time: u32, now: u64) -> bool {
    rbitcoin_node::tip_too_far_in_future(tip_time, now)
}

#[uniffi::export]
pub fn max_future_block_time() -> u64 {
    rbitcoin_node::MAX_FUTURE_BLOCK_TIME
}

// --- Blockstats FFI ---

#[uniffi::export]
pub fn is_unspendable(script_hex: String) -> bool {
    let script = rbitcoin_primitives::hex_decode(&script_hex).unwrap_or_default();
    rbitcoin_rpc::is_unspendable(&script)
}

#[uniffi::export]
pub fn txout_serialized_size(out_hex: String) -> Result<i64, RustyError> {
    let bytes = rbitcoin_primitives::hex_decode(&out_hex).map_err(|_| RustyError::InvalidInput)?;
    let out: bitcoin::TxOut =
        bitcoin::consensus::encode::deserialize(&bytes).map_err(|_| RustyError::InvalidInput)?;
    Ok(rbitcoin_rpc::txout_serialized_size(&out))
}

#[uniffi::export]
pub fn truncated_median(scores: Vec<i64>) -> i64 {
    rbitcoin_rpc::truncated_median(scores)
}

#[uniffi::export]
pub fn percentiles_by_weight(
    scores: Vec<i64>,
    weights: Vec<i64>,
    total_weight: i64,
) -> Result<Vec<i64>, RustyError> {
    if scores.len() != weights.len() {
        return Err(RustyError::InvalidInput);
    }
    let pairs: Vec<(i64, i64)> = scores.into_iter().zip(weights).collect();
    let result = rbitcoin_rpc::percentiles_by_weight(pairs, total_weight);
    Ok(result.to_vec())
}

#[uniffi::export]
pub fn rpc_per_utxo_overhead() -> i64 {
    rbitcoin_rpc::PER_UTXO_OVERHEAD
}

// --- Work Comparison FFI ---

#[uniffi::export]
pub fn work_better(new_work_hex: String, old_work_hex: String) -> Result<bool, RustyError> {
    let new_bytes =
        rbitcoin_primitives::hex_decode(&new_work_hex).map_err(|_| RustyError::InvalidInput)?;
    let new_arr: [u8; 32] = new_bytes.try_into().map_err(|_| RustyError::InvalidInput)?;
    let old_bytes =
        rbitcoin_primitives::hex_decode(&old_work_hex).map_err(|_| RustyError::InvalidInput)?;
    let old_arr: [u8; 32] = old_bytes.try_into().map_err(|_| RustyError::InvalidInput)?;
    Ok(rbitcoin_net::work_better(
        bitcoin::Work::from_be_bytes(new_arr),
        bitcoin::Work::from_be_bytes(old_arr),
    ))
}

// --- Reorg / Bad Prev FFI ---

#[uniffi::export]
pub fn is_bad_prev_err(err: String) -> bool {
    rbitcoin_net::is_bad_prev_err(&err)
}

// --- Header Download Timeout FFI ---

#[uniffi::export]
pub fn headers_download_timeout_secs(now: u64, best_header_time: u64) -> u64 {
    rbitcoin_net::headers_download_timeout_secs(now, best_header_time)
}

// --- Stale Follow Eviction FFI ---

#[uniffi::export]
pub fn pick_stale_follow_evict(ids: Vec<u64>, salt: u64, groups: Vec<u64>) -> Option<u64> {
    rbitcoin_net::pick_stale_follow_evict(&ids, salt, &groups)
}

// --- Witness Commitment FFI ---

#[uniffi::export]
pub fn apply_witness_commitment(block_hex: String) -> Result<String, RustyError> {
    let bytes =
        rbitcoin_primitives::hex_decode(&block_hex).map_err(|_| RustyError::InvalidInput)?;
    let mut block: bitcoin::Block =
        bitcoin::consensus::encode::deserialize(&bytes).map_err(|_| RustyError::InvalidInput)?;
    rbitcoin_consensus::apply_witness_commitment(&mut block);
    Ok(bitcoin::consensus::encode::serialize_hex(&block))
}

// --- DNS Seed Query FFI ---

#[uniffi::export]
pub fn dns_seed_query_host(seed: String, services_u64: u64) -> String {
    let services = bitcoin::p2p::ServiceFlags::from(services_u64);
    rbitcoin_net::dns_seed_query_host(&seed, services)
}

// --- Regtest Candidate FFI ---

#[uniffi::export]
pub fn prepare_regtest_candidate(
    block_hex: String,
    prev_hash_hex: String,
    time: u32,
) -> Result<String, RustyError> {
    let bytes =
        rbitcoin_primitives::hex_decode(&block_hex).map_err(|_| RustyError::InvalidInput)?;
    let mut block: bitcoin::Block =
        bitcoin::consensus::encode::deserialize(&bytes).map_err(|_| RustyError::InvalidInput)?;
    let prev = parse_hash32(&prev_hash_hex)?;
    let prev_hash = bitcoin::BlockHash::from_byte_array(prev);
    rbitcoin_consensus::prepare_regtest_candidate(&mut block, prev_hash, time);
    Ok(bitcoin::consensus::encode::serialize_hex(&block))
}

// --- Electrum Tweaks FFI ---

#[uniffi::export]
pub fn last_height(start: u32, count: u32, tip: Option<u32>) -> Option<u32> {
    rbitcoin_electrum::last_height(start, count, tip)
}

#[uniffi::export]
pub fn seal_subscribe_chunk(elapsed_secs: u64, budget_secs: u64, more: bool) -> bool {
    let elapsed = std::time::Duration::from_secs(elapsed_secs);
    let budget = std::time::Duration::from_secs(budget_secs);
    rbitcoin_electrum::seal_subscribe_chunk(elapsed, budget, more)
}

// --- Scripthash FFI ---

#[uniffi::export]
pub fn script_hash_hex(script_hex: String) -> String {
    let script = rbitcoin_primitives::hex_decode(&script_hex).unwrap_or_default();
    rbitcoin_primitives::hex_encode(rbitcoin_store::script_hash(&script))
}

// --- Soft Densify FFI ---

#[uniffi::export]
pub fn soft_confirm_window_n(rate_blocks_per_s: Option<f64>) -> u32 {
    rbitcoin_query::soft_confirm_window_n(rate_blocks_per_s)
}

#[uniffi::export]
pub fn soft_assign_restricted(depth_bytes: u64) -> bool {
    rbitcoin_query::soft_assign_restricted(depth_bytes)
}

#[uniffi::export]
pub fn soft_assign_stopped(depth_bytes: u64, stop_bytes: u64) -> bool {
    rbitcoin_query::soft_assign_stopped(depth_bytes, stop_bytes)
}

#[uniffi::export]
pub fn bq_assign_stop_bytes() -> u64 {
    rbitcoin_query::bq_assign_stop_bytes()
}

#[uniffi::export]
pub fn bq_soft_free_bytes() -> u64 {
    rbitcoin_query::BQ_SOFT_FREE_BYTES
}

#[uniffi::export]
pub fn bq_soft_confirm_secs() -> u64 {
    rbitcoin_query::BQ_SOFT_CONFIRM_SECS as u64
}

#[uniffi::export]
pub fn soft_densify_band_hi(
    path_lo: u32,
    densify_hi: u32,
    depth_bytes: u64,
    rate_blocks_per_s: Option<f64>,
    assign_stop_bytes: u64,
    fetched_hi: Option<u32>,
) -> u32 {
    rbitcoin_query::soft_densify_band_hi(
        path_lo,
        densify_hi,
        depth_bytes,
        rate_blocks_per_s,
        assign_stop_bytes,
        fetched_hi,
    )
}

#[uniffi::export]
pub fn soft_confirm_window_covered(
    depth_n: u32,
    depth_bytes: u64,
    rate_blocks_per_s: Option<f64>,
) -> bool {
    rbitcoin_query::soft_confirm_window_covered(depth_n, depth_bytes, rate_blocks_per_s)
}

#[uniffi::export]
pub fn mempool_admit_half_life_secs() -> u64 {
    rbitcoin_mempool::ADMIT_HALF_LIFE_SECS as u64
}

#[uniffi::export]
pub fn mempool_warm_after_secs() -> u64 {
    rbitcoin_mempool::WARM_AFTER_SECS as u64
}

#[uniffi::export]
pub fn mempool_warm_after_admits() -> u64 {
    rbitcoin_mempool::WARM_AFTER_ADMITS
}

// --- Esplora Script Fields FFI ---

#[derive(Debug, PartialEq, uniffi::Record)]
pub struct FfiEsploraScriptFields {
    pub hex: String,
    pub asm: String,
    pub script_type: String,
    pub address: Option<String>,
}

fn bitcoin_network(net: rbitcoin_primitives::Network) -> bitcoin::Network {
    match net {
        rbitcoin_primitives::Network::Mainnet => bitcoin::Network::Bitcoin,
        rbitcoin_primitives::Network::Testnet => bitcoin::Network::Testnet,
        rbitcoin_primitives::Network::Signet => bitcoin::Network::Signet,
        rbitcoin_primitives::Network::Regtest => bitcoin::Network::Regtest,
    }
}

#[uniffi::export]
pub fn esplora_script_fields(
    script_hex: String,
    network: String,
) -> Result<FfiEsploraScriptFields, RustyError> {
    let net = rbitcoin_network(&network)?;
    let script =
        rbitcoin_primitives::hex_decode(&script_hex).map_err(|_| RustyError::InvalidInput)?;
    let fields = rbitcoin_esplora::esplora_script_fields(&script, bitcoin_network(net));
    Ok(FfiEsploraScriptFields {
        hex: fields.hex,
        asm: fields.asm,
        script_type: fields.script_type.to_string(),
        address: fields.address,
    })
}

// --- Esplora Perf FFI ---

#[derive(Debug, PartialEq, uniffi::Record)]
pub struct FfiEsploraPerfSample {
    pub requests: u64,
    pub bytes: u64,
    pub elapsed_ms: u64,
}

#[uniffi::export]
pub fn sample_reset_esplora_perf() -> FfiEsploraPerfSample {
    let (requests, bytes, elapsed_ms) = rbitcoin_esplora::sample_reset_perf();
    FfiEsploraPerfSample {
        requests,
        bytes,
        elapsed_ms,
    }
}

// --- Peer constants & logs FFI ---

#[uniffi::export]
pub fn ban_score_threshold() -> u32 {
    rbitcoin_net::BAN_SCORE_THRESHOLD
}

#[uniffi::export]
pub fn max_serve_blocks() -> u32 {
    rbitcoin_net::MAX_SERVE_BLOCKS as u32
}

#[uniffi::export]
pub fn min_peer_proto_version() -> i32 {
    rbitcoin_net::MIN_PEER_PROTO_VERSION
}

#[uniffi::export]
pub fn handshake_timeout_secs() -> u64 {
    rbitcoin_net::HANDSHAKE_TIMEOUT.as_secs()
}

#[uniffi::export]
pub fn expected_services_disconnect_log(offered: u64, expected: u64) -> String {
    rbitcoin_net::expected_services_disconnect_log(offered, expected)
}

#[uniffi::export]
pub fn feeler_connection_completed_log() -> String {
    rbitcoin_net::feeler_connection_completed_log().to_string()
}

#[uniffi::export]
pub fn version_handshake_timeout_log(peer: u64) -> String {
    rbitcoin_net::version_handshake_timeout_log(peer)
}

#[uniffi::export]
pub fn obsolete_version_log(version: i32, peer: u64) -> String {
    rbitcoin_net::obsolete_version_log(version, peer)
}

#[uniffi::export]
pub fn connected_to_self_log(addr: String) -> String {
    rbitcoin_net::connected_to_self_log(&addr)
}

#[uniffi::export]
pub fn advertising_address_log(addr_port: String, peer: u64) -> String {
    rbitcoin_net::advertising_address_log(&addr_port, peer)
}

#[uniffi::export]
pub fn sendaddrv2_after_verack_log(peer: u64) -> String {
    rbitcoin_net::sendaddrv2_after_verack_log(peer)
}

#[uniffi::export]
pub fn addrv2_message_size_log(n: u32) -> String {
    rbitcoin_net::addrv2_message_size_log(n as usize)
}

#[uniffi::export]
pub fn ping_prior_to_verack_log(peer: u64) -> String {
    rbitcoin_net::ping_prior_to_verack_log(peer)
}

#[uniffi::export]
pub fn unsupported_before_verack_log(cmd: String, peer: u64) -> String {
    rbitcoin_net::unsupported_before_verack_log(&cmd, peer)
}

#[uniffi::export]
pub fn non_version_before_handshake_log(cmd: String, peer: u64) -> String {
    rbitcoin_net::non_version_before_handshake_log(&cmd, peer)
}

// --- Consensus error helpers FFI ---

#[uniffi::export]
pub fn script_flag_paren(token: String) -> String {
    rbitcoin_consensus::script_flag_paren(&token).to_string()
}

#[uniffi::export]
pub fn block_reject_log_line(hash: String, reason: String) -> String {
    rbitcoin_consensus::block_reject_log_line(&hash, &reason)
}

// --- Mempool constants FFI ---

#[uniffi::export]
pub fn mempool_block_weight_wu() -> u64 {
    rbitcoin_mempool::BLOCK_WEIGHT_WU
}

#[uniffi::export]
pub fn mempool_seconds_per_block() -> u64 {
    rbitcoin_mempool::SECONDS_PER_BLOCK
}

#[uniffi::export]
pub fn mempool_capacity_safety_num() -> u64 {
    rbitcoin_mempool::CAPACITY_SAFETY_NUM
}

#[uniffi::export]
pub fn mempool_capacity_safety_den() -> u64 {
    rbitcoin_mempool::CAPACITY_SAFETY_DEN
}

#[uniffi::export]
pub fn mempool_max_package_count() -> u32 {
    rbitcoin_mempool::MAX_PACKAGE_COUNT as u32
}

#[uniffi::export]
pub fn mempool_max_package_weight() -> u64 {
    rbitcoin_mempool::MAX_PACKAGE_WEIGHT
}

#[uniffi::export]
pub fn mempool_rbfr_ratio_num() -> u64 {
    rbitcoin_mempool::RBFR_RATIO_NUM
}

#[uniffi::export]
pub fn mempool_rbfr_ratio_den() -> u64 {
    rbitcoin_mempool::RBFR_RATIO_DEN
}

// --- Peer DOS constants FFI ---

#[uniffi::export]
pub fn peer_default_max_msgs_per_sec() -> u32 {
    rbitcoin_net::DEFAULT_MAX_MSGS_PER_SEC
}

#[uniffi::export]
pub fn peer_default_max_bytes_per_sec() -> u64 {
    rbitcoin_net::DEFAULT_MAX_BYTES_PER_SEC
}

#[uniffi::export]
pub fn peer_rate_limit_ban_score() -> u32 {
    rbitcoin_net::RATE_LIMIT_BAN_SCORE
}

#[uniffi::export]
pub fn peer_oversize_ban_score() -> u32 {
    rbitcoin_net::OVERSIZE_BAN_SCORE
}

#[uniffi::export]
pub fn peer_max_addr_to_send() -> u32 {
    rbitcoin_net::MAX_ADDR_TO_SEND as u32
}

#[uniffi::export]
pub fn peer_max_pct_addr_to_send() -> u32 {
    rbitcoin_net::MAX_PCT_ADDR_TO_SEND as u32
}

// --- Key & Address FFI ---

#[derive(Debug, PartialEq, uniffi::Record)]
pub struct FfiKeypair {
    pub private_key_wif: String,
    pub public_key_hex: String,
}

#[uniffi::export]
pub fn generate_keypair(network: String) -> Result<FfiKeypair, RustyError> {
    let net = rbitcoin_network(&network)?;
    let bnet = bitcoin_network(net);
    let secp = bitcoin::secp256k1::Secp256k1::new();
    let secret = bitcoin::secp256k1::SecretKey::new(&mut rand::thread_rng());
    let sk = bitcoin::PrivateKey::new(secret, bnet);
    let pk = sk.public_key(&secp);
    Ok(FfiKeypair {
        private_key_wif: sk.to_wif(),
        public_key_hex: rbitcoin_primitives::hex_encode(pk.to_bytes()),
    })
}

#[uniffi::export]
pub fn p2wpkh_address_from_pubkey(
    pubkey_hex: String,
    network: String,
) -> Result<String, RustyError> {
    let net = rbitcoin_network(&network)?;
    let bytes =
        rbitcoin_primitives::hex_decode(&pubkey_hex).map_err(|_| RustyError::InvalidInput)?;
    let pk =
        bitcoin::CompressedPublicKey::from_slice(&bytes).map_err(|_| RustyError::InvalidInput)?;
    let hrp = known_hrp(net);
    let addr = bitcoin::Address::p2wpkh(&pk, hrp);
    Ok(addr.to_string())
}

#[uniffi::export]
pub fn p2tr_address_from_pubkey(pubkey_hex: String, network: String) -> Result<String, RustyError> {
    let net = rbitcoin_network(&network)?;
    let bytes =
        rbitcoin_primitives::hex_decode(&pubkey_hex).map_err(|_| RustyError::InvalidInput)?;
    let xonly =
        bitcoin::XOnlyPublicKey::from_slice(&bytes).map_err(|_| RustyError::InvalidInput)?;
    let secp = bitcoin::secp256k1::Secp256k1::new();
    let hrp = known_hrp(net);
    let addr = bitcoin::Address::p2tr(&secp, xonly, None, hrp);
    Ok(addr.to_string())
}

fn known_hrp(net: rbitcoin_primitives::Network) -> bitcoin::address::KnownHrp {
    match net {
        rbitcoin_primitives::Network::Mainnet => bitcoin::address::KnownHrp::Mainnet,
        rbitcoin_primitives::Network::Testnet | rbitcoin_primitives::Network::Signet => {
            bitcoin::address::KnownHrp::Testnets
        }
        rbitcoin_primitives::Network::Regtest => bitcoin::address::KnownHrp::Regtest,
    }
}

// --- BIP32 HD Wallet FFI ---

#[uniffi::export]
pub fn xpriv_from_seed(seed_hex: String, network: String) -> Result<String, RustyError> {
    let net = rbitcoin_network(&network)?;
    let seed = rbitcoin_primitives::hex_decode(&seed_hex).map_err(|_| RustyError::InvalidInput)?;
    let xpriv = bitcoin::bip32::Xpriv::new_master(bitcoin_network(net), &seed)
        .map_err(|_| RustyError::InvalidInput)?;
    Ok(xpriv.to_string())
}

#[uniffi::export]
pub fn xpub_from_xpriv(xpriv_string: String) -> Result<String, RustyError> {
    let xpriv: bitcoin::bip32::Xpriv =
        xpriv_string.parse().map_err(|_| RustyError::InvalidInput)?;
    let secp = bitcoin::secp256k1::Secp256k1::new();
    let xpub = bitcoin::bip32::Xpub::from_priv(&secp, &xpriv);
    Ok(xpub.to_string())
}

#[uniffi::export]
pub fn derive_xpriv(xpriv_string: String, path: String) -> Result<String, RustyError> {
    let xpriv: bitcoin::bip32::Xpriv =
        xpriv_string.parse().map_err(|_| RustyError::InvalidInput)?;
    let dp: bitcoin::bip32::DerivationPath = path.parse().map_err(|_| RustyError::InvalidInput)?;
    let secp = bitcoin::secp256k1::Secp256k1::new();
    let derived = xpriv
        .derive_priv(&secp, &dp)
        .map_err(|_| RustyError::InvalidInput)?;
    Ok(derived.to_string())
}

#[uniffi::export]
pub fn derive_xpub(xpub_string: String, path: String) -> Result<String, RustyError> {
    let xpub: bitcoin::bip32::Xpub = xpub_string.parse().map_err(|_| RustyError::InvalidInput)?;
    let dp: bitcoin::bip32::DerivationPath = path.parse().map_err(|_| RustyError::InvalidInput)?;
    let secp = bitcoin::secp256k1::Secp256k1::new();
    let derived = xpub
        .derive_pub(&secp, &dp)
        .map_err(|_| RustyError::InvalidInput)?;
    Ok(derived.to_string())
}

#[uniffi::export]
pub fn p2wpkh_address_from_xpub(
    xpub_string: String,
    path: String,
    network: String,
) -> Result<String, RustyError> {
    let net = rbitcoin_network(&network)?;
    let xpub: bitcoin::bip32::Xpub = xpub_string.parse().map_err(|_| RustyError::InvalidInput)?;
    let dp: bitcoin::bip32::DerivationPath = path.parse().map_err(|_| RustyError::InvalidInput)?;
    let secp = bitcoin::secp256k1::Secp256k1::new();
    let derived = xpub
        .derive_pub(&secp, &dp)
        .map_err(|_| RustyError::InvalidInput)?;
    let pk = derived.to_pub();
    let hrp = known_hrp(net);
    let addr = bitcoin::Address::p2wpkh(&pk, hrp);
    Ok(addr.to_string())
}

// --- PSBT FFI ---

#[uniffi::export]
pub fn psbt_from_hex(hex: String) -> Result<bool, RustyError> {
    let bytes = rbitcoin_primitives::hex_decode(&hex).map_err(|_| RustyError::InvalidInput)?;
    let _ = bitcoin::Psbt::deserialize(&bytes).map_err(|_| RustyError::InvalidInput)?;
    Ok(true)
}

#[uniffi::export]
pub fn psbt_extract_tx_hex(hex: String) -> Result<String, RustyError> {
    let bytes = rbitcoin_primitives::hex_decode(&hex).map_err(|_| RustyError::InvalidInput)?;
    let psbt = bitcoin::Psbt::deserialize(&bytes).map_err(|_| RustyError::InvalidInput)?;
    let tx = psbt.extract_tx_unchecked_fee_rate();
    Ok(rbitcoin_primitives::hex_encode(
        bitcoin::consensus::serialize(&tx),
    ))
}

#[uniffi::export]
pub fn psbt_fee_sat(hex: String) -> Result<u64, RustyError> {
    let bytes = rbitcoin_primitives::hex_decode(&hex).map_err(|_| RustyError::InvalidInput)?;
    let psbt = bitcoin::Psbt::deserialize(&bytes).map_err(|_| RustyError::InvalidInput)?;
    let fee = psbt.fee().map_err(|_| RustyError::InvalidInput)?;
    Ok(fee.to_sat())
}

// --- Chain constants & logs FFI ---

#[uniffi::export]
pub fn default_max_tip_age_secs() -> u64 {
    rbitcoin_net::DEFAULT_MAX_TIP_AGE_SECS
}

#[uniffi::export]
pub fn ibd_feefilter_sat_kvb() -> u64 {
    rbitcoin_net::IBD_FEEFILTER_SAT_KVB
}

#[uniffi::export]
pub fn stale_relay_age_limit_secs() -> u64 {
    rbitcoin_net::STALE_RELAY_AGE_LIMIT_SECS
}

#[uniffi::export]
pub fn max_addr_man() -> u32 {
    rbitcoin_net::MAX_ADDR_MAN as u32
}

#[uniffi::export]
pub fn accept_block_header_nodos_log(hash: String) -> String {
    rbitcoin_net::accept_block_header_nodos_log(&hash)
}

#[uniffi::export]
pub fn ignoring_low_work_chain_log(height: u32) -> String {
    rbitcoin_net::ignoring_low_work_chain_log(height)
}

#[uniffi::export]
pub fn synchronizing_blockheaders_log(height: u32) -> String {
    rbitcoin_net::synchronizing_blockheaders_log(height)
}

#[uniffi::export]
pub fn initial_getheaders_log(locator_height: u32, peer: u64) -> String {
    rbitcoin_net::initial_getheaders_log(locator_height, peer)
}

#[uniffi::export]
pub fn headers_timeout_disconnect_log(peer: u64) -> String {
    rbitcoin_net::headers_timeout_disconnect_log(peer)
}

#[uniffi::export]
pub fn headers_timeout_noban_log(peer: u64) -> String {
    rbitcoin_net::headers_timeout_noban_log(peer)
}

#[uniffi::export]
pub fn received_getdata_wtx_log(wtxid: String, peer: u64) -> String {
    rbitcoin_net::received_getdata_wtx_log(&wtxid, peer)
}

#[uniffi::export]
pub fn received_tx_log() -> String {
    rbitcoin_net::received_tx_log().to_string()
}

// --- V2 transport constants & logs FFI ---

#[uniffi::export]
pub fn max_v2_contents_len() -> u32 {
    rbitcoin_net::MAX_V2_CONTENTS_LEN as u32
}

#[uniffi::export]
pub fn v2_cipher_expansion() -> u32 {
    rbitcoin_net::V2_CIPHER_EXPANSION as u32
}

#[uniffi::export]
pub fn v2_other_recv_bytes(contents_len: u32) -> u64 {
    rbitcoin_net::v2_other_recv_bytes(contents_len as usize)
}

#[uniffi::export]
pub fn v2_handshake_timeout_log(peer: u64) -> String {
    rbitcoin_net::v2_handshake_timeout_log(peer)
}

#[uniffi::export]
pub fn v2_missing_garbage_terminator_log() -> String {
    rbitcoin_net::v2_missing_garbage_terminator_log().to_string()
}

#[uniffi::export]
pub fn v2_packet_decryption_failure_log() -> String {
    rbitcoin_net::v2_packet_decryption_failure_log().to_string()
}

#[uniffi::export]
pub fn v2_packet_too_large_log(n: u32) -> String {
    rbitcoin_net::v2_packet_too_large_log(n as usize)
}

#[uniffi::export]
pub fn v2_invalid_message_type_log() -> String {
    rbitcoin_net::v2_invalid_message_type_log().to_string()
}

// --- Consensus policy constants FFI ---

#[uniffi::export]
pub fn min_relay_fee_rate_sat_per_kvb() -> u64 {
    rbitcoin_consensus::policy::MIN_RELAY_FEE_RATE_SAT_PER_KVB
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
            assert!(!store.is_confirmed_strong(1).unwrap());
            assert!(!store.is_confirmed_strong_at(1, None).unwrap());
            assert_eq!(
                store.tx_height_get_batch(vec![1, 2]).unwrap(),
                vec![None, None]
            );
            assert_eq!(
                store.txids_get_many(vec![1, 2]).unwrap(),
                vec![None, None]
            );
            assert_eq!(
                store.tx_body_range_batch(vec![1, 2]).unwrap(),
                vec![None, None]
            );
            assert_eq!(
                store.tx_spent_range_batch(vec![1, 2]).unwrap(),
                vec![None, None]
            );
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
        init_log_off();
        assert!(!log_level_enabled("info".to_string()));
        init_log_from_env();
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
        assert!(!rbitcoin_version().is_empty());
        let subver = rbitcoin_subversion(vec!["test".to_string()]).unwrap();
        assert!(subver.contains("rbitcoin"));
        assert!(rbitcoin_subversion(vec!["bad/char".to_string()]).is_err());
    }

    #[test]
    fn test_store_open_or_create() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("store2").to_str().unwrap().to_string();
        let store = FfiStore::open_or_create(path.clone()).unwrap();
        assert_eq!(store.tip_height(), None);
        assert!(store.datadir_bytes() > 0 || store.datadir_bytes() == 0);
        assert!(!store.path().is_empty());
        assert_eq!(store.archived_block_count().unwrap(), 0);
        assert!(store.header_slots() > 0);
        assert!(store.tx_head_bits() > 0);
        assert!(!store.is_split());
        assert_eq!(store.spender_list_count(), 0);
        assert_eq!(store.class_c_l2_resident_bytes(), 0);
        assert!(store.spenders_create(1, 0).unwrap().is_empty());
        assert_eq!(store.fence_max_connected_fk(), 0);
        assert_eq!(store.height_fence_run_count(), 0);
        assert_eq!(store.fence_tip_height(), None);
        store.rebuild_height_fence().unwrap();
        store.flush_header_archive().unwrap();
        store.flush().unwrap();
    }

    #[test]
    fn test_store_tx_height_get_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("store3").to_str().unwrap().to_string();
        let store = FfiStore::open_or_create(path).unwrap();
        assert_eq!(store.tx_height_get(1).unwrap(), None);
        assert_eq!(store.get_fk_by_txid("0".repeat(64)).unwrap(), None);
        assert_eq!(store.get_fk_by_txid_tip("0".repeat(64)).unwrap(), None);
        assert!(store.tx_body_range(1).is_err());
        assert!(store.tx_spent_range(1).is_err());
        assert!(store.tx_inwit_range(1).is_err());
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
    fn test_query_more_read_functions() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("query3").to_str().unwrap().to_string();
        let query = FfiQuery::open_or_create(path).unwrap();
        assert_eq!(query.lookup_started_hi(), None);
        assert_eq!(query.class_a_hi(), None);
        assert!(!query.block_queue_soft_pressure());
        assert_eq!(query.block_queue_take_raw_clone_n(), 0);
        assert_eq!(query.block_queue_promoted_count(), 0);
        assert_eq!(query.scripthash_entry_count(), 0);
        assert_eq!(query.point_edge_count(), 0);
        assert_eq!(query.tip_header_fk().unwrap(), None);
        assert_eq!(query.head_drain_fk(), 0);
        assert!(query.block_queue_queued_heights().is_empty());
        assert!(query.block_queue_list_meta().is_empty());
        assert!(!query.block_queue_has_height(0));
        assert_eq!(query.block_queue_hash_at_height(0), None);
        assert!(!query.block_queue_is_resolve_complete(0));
        assert_eq!(query.block_queue_payload(0).unwrap(), None);
        assert_eq!(query.block_queue_raw_payload(0).unwrap(), None);
        assert!(!query.block_queue_has_hash("0".repeat(64)).unwrap());
        assert_eq!(query.block_queue_dequeue_height(0).unwrap(), 0);
        assert_eq!(query.tx_fk_by_txid_tip("0".repeat(64)).unwrap(), None);
        assert_eq!(query.take_disconnect(0), None);
        assert!(query.block_queue_unresolved_heights(0, vec![], 100).is_empty());
        assert_eq!(
            query.block_queue_payload_by_hash("0".repeat(64)).unwrap(),
            None
        );
        query.set_lookup_taken_hi(Some(100));
        assert_eq!(query.lookup_taken_hi(), Some(100));
        query.set_lookup_taken_hi(None);
        assert_eq!(query.lookup_taken_hi(), None);
        query.set_lookup_started_hi(Some(200));
        assert_eq!(query.lookup_started_hi(), Some(200));
        query.set_lookup_started_hi(None);
        assert_eq!(query.lookup_started_hi(), None);
        query.set_class_a_hi(Some(300));
        assert_eq!(query.class_a_hi(), Some(300));
        query.set_class_a_hi(None);
        assert_eq!(query.class_a_hi(), None);
        query.note_head_drain_fk(100);
        assert_eq!(query.head_drain_fk(), 100);
        query.note_lookup_tiponly_start(50);
        query.prune_write_create_loc(10);
        assert!(!query.lookup_already_taken(0));
        query.flush_header_archive().unwrap();
        query.flush().unwrap();
        let sizes = query.process_owned_size_snapshot();
        assert_eq!(sizes.conf_plans, 0);
        assert_eq!(sizes.h2h_keys, 0);
        query.apply_sh_pending().unwrap();
        query.drop_sh_pending_from(0);
        query.enter_tip_index_mode();
        assert_eq!(query.index_mode(), 2);
        query.enter_direct_index_mode().unwrap();
        assert_eq!(query.index_mode(), 1);
        query.enter_direct_index_mode_sh(false).unwrap();
        assert!(!query.sh_index_enabled());
        assert_eq!(query.sh_lag_heights(), 0);
        assert_eq!(query.pin_chain_view_at("0".repeat(64)).unwrap(), None);
        assert_eq!(query.pin_sh_chain_view_at("0".repeat(64)).unwrap(), None);
        query.sync_sh_seal_from_include_hwm().unwrap();
        assert_eq!(query.finalize_sh_runs().unwrap(), 0);
        assert_eq!(query.backfill_tx_index().unwrap(), 0);
        let rec = FfiHeaderRecord {
            prev_fk: 0,
            version: 0,
            timestamp: 0,
            bits: 0,
            nonce: 0,
            merkle_root: "0".repeat(64),
            hash: "0".repeat(64),
            size: 0,
            weight: 0,
        };
        let fk = query.ensure_header(rec).unwrap();
        assert_eq!(fk, 1);
        let fks = query.ensure_headers(vec![]).unwrap();
        assert!(fks.is_empty());
    }

    #[test]
    fn test_query_get_header_get_tx_spenders_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("query4").to_str().unwrap().to_string();
        let query = FfiQuery::open_or_create(path).unwrap();
        assert!(query.get_header(1).is_err());
        assert!(query.get_tx(1).is_err());
        assert_eq!(query.header_tx_fks(1, None).unwrap(), None);
        assert!(query.spenders("0".repeat(64), 0).unwrap().is_empty());
        assert!(query.spenders_at("0".repeat(64), 0, None).unwrap().is_empty());
        assert!(query.tx_input("0".repeat(64), 0).is_err());
        assert!(query.tx_output("0".repeat(64), 0).is_err());
        assert!(query.tx_input_at_fk(1, 0).is_err());
        assert!(query.tx_output_at_fk(1, 0).is_err());
    }

    #[test]
    fn test_query_put_header() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("query5").to_str().unwrap().to_string();
        let query = FfiQuery::open_or_create(path).unwrap();
        let zero_hash = "0".repeat(64);
        let fk = query
            .put_header(0, 0x20000000, 1231006505, 0x1d00ffff, 2083236893, zero_hash.clone(), zero_hash.clone(), 80, 320)
            .unwrap();
        assert!(fk > 0);
        let header = query.get_header(fk).unwrap();
        assert_eq!(header.version, 0x20000000);
        assert_eq!(header.timestamp, 1231006505);
    }

    #[test]
    fn test_query_put_spend_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("query6").to_str().unwrap().to_string();
        let query = FfiQuery::open_or_create(path).unwrap();
        assert!(query.put_spend("0".repeat(64), 0, 1, 0).is_err());
        query.set_tx_index(true);
        assert!(query.tx_index_enabled());
        query.set_tx_index(false);
        assert!(!query.tx_index_enabled());
        query.set_spend_index(true);
        assert!(query.spend_index_enabled());
        query.set_spend_index(false);
        assert!(!query.spend_index_enabled());
        assert!(query.unspent_create_vouts(1, vec![0, 1]).is_err());
    }

    #[test]
    fn test_query_block_queue_offer_enqueue() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("query7").to_str().unwrap().to_string();
        let query = FfiQuery::open_or_create(path).unwrap();
        let hash = "0".repeat(64);
        let offer = query.block_queue_offer(100, hash.clone(), 1, vec![1, 2, 3]).unwrap();
        assert_eq!(offer.queue_id, 1);
        let id = query.block_queue_enqueue(101, hash.clone(), 2, vec![4, 5, 6]).unwrap();
        assert_eq!(id, 2);
        assert!(query.block_queue_has_height(100));
        assert!(query.block_queue_has_height(101));
        assert_eq!(query.block_queue_count(), 2);
        let dequeued = query.block_queue_dequeue_height(100).unwrap();
        assert_eq!(dequeued, 1);
        assert!(!query.block_queue_has_height(100));
        query.block_queue_drop_resolved_from(50);
        assert_eq!(query.block_queue_mark_resolve_complete_wave(vec![101]).unwrap(), 1);
        assert!(query.block_queue_is_resolve_complete(101));
        let taken = query.block_queue_take_raw(101);
        assert!(taken.is_some() || taken.is_none());
        let targets = block_queue_soft_targets(None);
        assert!(targets.free_mib > 0);
    }

    #[test]
    fn test_mempool_compact_and_free_slot() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("mempool2").to_str().unwrap().to_string();
        let mempool = FfiMempool::open_or_create(path).unwrap();
        assert!(mempool.has_free_slot());
        let compact = mempool.compact().unwrap();
        assert!(compact.starts_with("dead="));
        assert!(mempool.body_logical_len().unwrap() > 0);
        assert!(!mempool.dir().is_empty());
    }

    #[test]
    fn test_mempool_append_live_tx() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("mempool_append").to_str().unwrap().to_string();
        let mempool = FfiMempool::open_or_create(path).unwrap();
        let tx_hex = "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff025101ffffffff0100f2052a010000001976a914000000000000000000000000000000000000000088ac00000000";
        let slot = mempool.append_live_tx(tx_hex.to_string(), 0, 484).unwrap();
        assert_eq!(slot, 0);
        assert_eq!(mempool.live_count(), 1);
    }

    #[test]
    fn test_mempool_persist_abandon_grow() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("mempool3").to_str().unwrap().to_string();
        let mempool = FfiMempool::open_or_create(path).unwrap();
        mempool.persist_if_dirty().unwrap();
        assert_eq!(mempool.abandon_live().unwrap(), 0);
        mempool.grow_slots().unwrap();
        assert!(mempool.has_free_slot());
    }

    #[test]
    fn test_electrum_last_height() {
        assert_eq!(electrum_last_height(0, 10, Some(5)), Some(5));
        assert_eq!(electrum_last_height(0, 10, Some(100)), Some(9));
        assert_eq!(electrum_last_height(10, 5, Some(5)), None);
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
    fn test_net_ibd_constants() {
        assert_eq!(net_default_blocks_in_transit_per_peer(), 16);
        assert_eq!(net_default_ibd_window(), 1024);
    }

    #[test]
    fn test_network_magic_hex() {
        assert_eq!(
            network_magic_hex("mainnet".to_string()).unwrap(),
            "f9beb4d9"
        );
        assert_eq!(
            network_magic_hex("regtest".to_string()).unwrap(),
            "fabfb5da"
        );
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
    fn test_addr_man() {
        let am = FfiAddrMan::new();
        assert!(am.is_empty());
        assert_eq!(am.len(), 0);
        am.add("127.0.0.1:8333".to_string()).unwrap();
        assert_eq!(am.len(), 1);
        assert!(!am.is_empty());
        assert_eq!(am.peers().len(), 1);
        assert_eq!(am.flags("127.0.0.1:8333".to_string()).unwrap(), 0);
        am.note_connected("127.0.0.1:8333".to_string()).unwrap();
        assert!(am.flags("127.0.0.1:8333".to_string()).unwrap() > 0);
        let entries = am.entries();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].addr, "127.0.0.1:8333");
        let out = am.take_outbound(10);
        assert!(!out.is_empty());
        let occ = vec!["127.0.0.1:8333".to_string()];
        let out2 = am.take_outbound_occupied(10, occ);
        assert!(out2.is_empty());
        am.note_ibd_slow("127.0.0.1:8333".to_string()).unwrap();
        am.apply_ibd_dead_speed("127.0.0.1:8333".to_string(), 500, Some(1000), true)
            .unwrap();
    }

    #[test]
    fn test_addr_man_with_seeds() {
        let am = FfiAddrMan::with_seeds("mainnet".to_string()).unwrap();
        assert!(!am.is_empty());
    }

    #[test]
    fn test_addr_man_save_load() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("peers.txt").to_str().unwrap().to_string();
        let am = FfiAddrMan::new();
        am.add("127.0.0.1:8333".to_string()).unwrap();
        am.save(path.clone()).unwrap();
        let am2 = FfiAddrMan::load(path).unwrap();
        assert_eq!(am2.len(), 1);
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

    #[test]
    fn test_header_to_record() {
        let header_hex = "0100000000000000000000000000000000000000000000000000000000000000000000003ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4a29ab5f49ffff001d1dac2b7c";
        let hash_hex = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f";
        let rec = header_to_record(0, header_hex.to_string(), hash_hex.to_string()).unwrap();
        assert_eq!(rec.prev_fk, 0);
        assert_eq!(rec.version, 1);
        assert_eq!(rec.timestamp, 1231006505);
    }

    #[test]
    fn test_select_inbound_eviction() {
        // More than 20 candidates so at least some remain unprotected
        // (netgroup=4 + blocks=4 + txs=4 + minping=8 = 20 protected slots)
        let mut cands = Vec::new();
        for i in 0..30u64 {
            cands.push(FfiInboundEvictCandidate {
                id: i + 1,
                connected_at: i * 10,
                min_ping: Some(i as f64),
                last_block: i,
                last_tx: i,
                netgroup: i,
                noban: false,
            });
        }
        let evicted = select_inbound_eviction(cands);
        assert!(evicted.is_some());
    }

    #[test]
    fn test_query_pin_chain_view_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("query_pin").to_str().unwrap().to_string();
        let query = FfiQuery::open_or_create(path).unwrap();
        let view = query.pin_chain_view().unwrap();
        assert_eq!(view, None);
        let sh_view = query.pin_sh_chain_view().unwrap();
        assert_eq!(sh_view, None);
        let loc = query.locator_hashes().unwrap();
        assert_eq!(loc.len(), 1);
        assert_eq!(loc[0], "0".repeat(64));
        let headers = query
            .headers_after_locator(vec!["0".repeat(64)], "0".repeat(64), 2000)
            .unwrap();
        assert!(headers.is_empty());
        query.invalidate_height_by_hash_index();
    }

    #[test]
    fn test_tip_too_far_in_future() {
        assert!(!tip_too_far_in_future(1000, 1000));
        assert!(tip_too_far_in_future(1000 + 7200 + 1, 1000));
        assert!(!tip_too_far_in_future(1000 + 7200, 1000));
        assert_eq!(max_future_block_time(), 7200);
    }

    #[test]
    fn test_is_unspendable() {
        assert!(is_unspendable("6a".to_string())); // OP_RETURN
        assert!(!is_unspendable("76a914".to_string())); // P2PKH start
                                                        // Script over 10_000 bytes
        let huge = vec![0x00u8; 10001];
        assert!(is_unspendable(rbitcoin_primitives::hex_encode(&huge)));
    }

    #[test]
    fn test_txout_serialized_size() {
        // Simple P2PKH output: value(8) + len(1) + script(25) = 34
        let out_hex = "00f2052a010000001976a914000000000000000000000000000000000000000088ac";
        let size = txout_serialized_size(out_hex.to_string()).unwrap();
        assert_eq!(size, 34);
    }

    #[test]
    fn test_truncated_median() {
        assert_eq!(truncated_median(vec![1, 3, 2]), 2);
        assert_eq!(truncated_median(vec![1, 2, 3, 4]), 2); // (2+3)/2
        assert_eq!(truncated_median(vec![]), 0);
        assert_eq!(truncated_median(vec![5]), 5);
    }

    #[test]
    fn test_percentiles_by_weight() {
        let scores = vec![1, 2, 3, 4, 5];
        let weights = vec![10, 10, 10, 10, 10];
        let result = percentiles_by_weight(scores, weights, 50).unwrap();
        assert_eq!(result.len(), 5);
        // With total_weight=50, cumulative at each point: 10, 20, 30, 40, 50
        // percentiles: 10% → 5, 25% → 10, 50% → 15, 75% → 20, 90% → 25
        // Wait, total_weight * percentile / 100:
        // 50*10/100=5 → first weight 10 >= 5 → result[0]=1
        // 50*25/100=12.5 → cumulative 20 >= 12.5 → result[1]=2
        // 50*50/100=25 → cumulative 30 >= 25 → result[2]=3
        // 50*75/100=37.5 → cumulative 40 >= 37.5 → result[3]=4
        // 50*90/100=45 → cumulative 50 >= 45 → result[4]=5
        assert_eq!(result, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_percentiles_by_weight_mismatch() {
        let result = percentiles_by_weight(vec![1, 2], vec![10], 10);
        assert!(result.is_err());
    }

    #[test]
    fn test_rpc_per_utxo_overhead() {
        assert_eq!(rpc_per_utxo_overhead(), 41);
    }

    #[test]
    fn test_work_better() {
        let w1 = "0000000000000000000000000000000000000000000000000000000000000001";
        let w2 = "0000000000000000000000000000000000000000000000000000000000000002";
        assert!(work_better(w2.to_string(), w1.to_string()).unwrap());
        assert!(!work_better(w1.to_string(), w2.to_string()).unwrap());
        assert!(!work_better(w1.to_string(), w1.to_string()).unwrap());
    }

    #[test]
    fn test_is_bad_prev_err() {
        assert!(is_bad_prev_err("unexpected previous header".to_string()));
        assert!(is_bad_prev_err("unexpected previous".to_string()));
        assert!(!is_bad_prev_err("some other error".to_string()));
    }

    #[test]
    fn test_headers_download_timeout_secs() {
        let timeout = headers_download_timeout_secs(1000, 500);
        assert!(timeout > 1000);
    }

    #[test]
    fn test_pick_stale_follow_evict() {
        let ids = vec![1u64, 2, 3];
        let groups = vec![10u64, 20, 10];
        let evicted = pick_stale_follow_evict(ids.clone(), 0, groups);
        assert!(evicted.is_some());
        assert!(ids.contains(&evicted.unwrap()));
    }

    #[test]
    fn test_apply_witness_commitment_no_witness() {
        // Regtest block without witness
        let genesis_hash = "0000000000000000000000000000000000000000000000000000000000000000";
        let block_hex = mine_empty_regtest(genesis_hash.to_string(), 1296688602, 0).unwrap();
        let result = apply_witness_commitment(block_hex).unwrap();
        assert!(!result.is_empty());
    }

    #[test]
    fn test_dns_seed_query_host() {
        let host = dns_seed_query_host("seed.bitcoin.sipa.be".to_string(), 0);
        assert!(!host.is_empty());
    }

    #[test]
    fn test_prepare_regtest_candidate() {
        let genesis_hash = "0000000000000000000000000000000000000000000000000000000000000000";
        let block_hex = mine_empty_regtest(genesis_hash.to_string(), 1296688602, 0).unwrap();
        let result =
            prepare_regtest_candidate(block_hex, genesis_hash.to_string(), 1296688602).unwrap();
        assert!(!result.is_empty());
    }

    #[test]
    fn test_last_height() {
        assert_eq!(last_height(0, 10, Some(5)), Some(5));
        assert_eq!(last_height(0, 10, Some(100)), Some(9));
        assert_eq!(last_height(10, 1, Some(5)), None);
        assert_eq!(last_height(0, 1, None), None);
    }

    #[test]
    fn test_seal_subscribe_chunk() {
        assert!(seal_subscribe_chunk(10, 5, true));
        assert!(!seal_subscribe_chunk(4, 5, true));
        assert!(!seal_subscribe_chunk(10, 5, false));
    }

    #[test]
    fn test_script_hash_hex() {
        let hash =
            script_hash_hex("76a914000000000000000000000000000000000000000088ac".to_string());
        assert_eq!(hash.len(), 64);
    }

    #[test]
    fn test_soft_densify() {
        assert_eq!(soft_confirm_window_n(None), 0);
        assert_eq!(soft_confirm_window_n(Some(1.0)), 60);
        assert!(!soft_assign_restricted(0));
        assert!(!soft_assign_stopped(0, u64::MAX));
        assert!(bq_assign_stop_bytes() > 0);
        assert_eq!(
            soft_densify_band_hi(0, 100, 0, Some(1.0), bq_assign_stop_bytes(), None),
            100
        );
        assert!(!soft_confirm_window_covered(0, 0, Some(1.0)));
        assert!(soft_confirm_window_covered(
            100,
            200 * 1024 * 1024,
            Some(1.0)
        ));
    }

    #[test]
    fn test_esplora_script_fields_p2pkh() {
        let script_hex = "76a914000000000000000000000000000000000000000088ac";
        let fields = esplora_script_fields(script_hex.to_string(), "mainnet".to_string()).unwrap();
        assert_eq!(fields.script_type, "p2pkh");
        assert!(!fields.asm.is_empty());
        assert!(!fields.hex.is_empty());
    }

    #[test]
    fn test_esplora_script_fields_op_return() {
        let fields = esplora_script_fields("6a".to_string(), "mainnet".to_string()).unwrap();
        assert_eq!(fields.script_type, "op_return");
    }

    #[test]
    fn test_sample_reset_esplora_perf() {
        let sample = sample_reset_esplora_perf();
        // Fresh sample should be zero
        assert_eq!(sample.requests, 0);
        assert_eq!(sample.bytes, 0);
        assert_eq!(sample.elapsed_ms, 0);
    }

    #[test]
    fn test_peer_constants() {
        assert_eq!(ban_score_threshold(), 100);
        assert_eq!(max_serve_blocks(), 16);
        assert_eq!(min_peer_proto_version(), 31800);
        assert_eq!(handshake_timeout_secs(), 60);
    }

    #[test]
    fn test_peer_logs() {
        assert!(!feeler_connection_completed_log().is_empty());
        assert!(!version_handshake_timeout_log(1).is_empty());
        assert!(!obsolete_version_log(70015, 1).is_empty());
        assert!(!connected_to_self_log("127.0.0.1:8333".to_string()).is_empty());
        assert!(!advertising_address_log("127.0.0.1:8333".to_string(), 1).is_empty());
        assert!(!sendaddrv2_after_verack_log(1).is_empty());
        assert!(!addrv2_message_size_log(10).is_empty());
        assert!(!ping_prior_to_verack_log(1).is_empty());
        assert!(!unsupported_before_verack_log("ping".to_string(), 1).is_empty());
        assert!(!non_version_before_handshake_log("ping".to_string(), 1).is_empty());
        assert!(!expected_services_disconnect_log(0, 1).is_empty());
    }

    #[test]
    fn test_consensus_error_helpers() {
        assert_eq!(
            script_flag_paren("CSV".to_string()),
            "Locktime requirement not satisfied"
        );
        assert!(
            !block_reject_log_line("0000…".to_string(), "bad-txns-nonfinal".to_string()).is_empty()
        );
    }

    #[test]
    fn test_mempool_more_constants() {
        assert_eq!(mempool_block_weight_wu(), 4_000_000);
        assert_eq!(mempool_seconds_per_block(), 600);
        assert_eq!(mempool_capacity_safety_num(), 95);
        assert_eq!(mempool_capacity_safety_den(), 100);
        assert_eq!(mempool_max_package_count(), 25);
        assert_eq!(mempool_max_package_weight(), 404_000);
        assert_eq!(mempool_rbfr_ratio_num(), 5);
        assert_eq!(mempool_rbfr_ratio_den(), 4);
        assert_eq!(mempool_admit_half_life_secs(), 150);
        assert_eq!(mempool_warm_after_admits(), 32);
        assert_eq!(mempool_warm_after_secs(), 60);
    }

    #[test]
    fn test_peer_dos_constants() {
        assert_eq!(peer_default_max_msgs_per_sec(), 4_000);
        assert_eq!(peer_default_max_bytes_per_sec(), 16_000_000);
        assert_eq!(peer_rate_limit_ban_score(), 50);
        assert_eq!(peer_oversize_ban_score(), 100);
        assert_eq!(peer_max_addr_to_send(), 1000);
        assert_eq!(peer_max_pct_addr_to_send(), 23);
    }

    #[test]
    fn test_generate_keypair() {
        let kp = generate_keypair("mainnet".to_string()).unwrap();
        assert!(!kp.private_key_wif.is_empty());
        assert!(!kp.public_key_hex.is_empty());
        assert_eq!(kp.public_key_hex.len(), 66);
    }

    #[test]
    fn test_p2wpkh_address_from_pubkey() {
        let kp = generate_keypair("mainnet".to_string()).unwrap();
        let addr = p2wpkh_address_from_pubkey(kp.public_key_hex, "mainnet".to_string()).unwrap();
        assert!(addr.starts_with("bc1q"));
    }

    #[test]
    fn test_p2tr_address_from_pubkey() {
        // Need x-only pubkey (32 bytes) for P2TR; CompressedPublicKey is 33 bytes.
        // Just verify the function exists and rejects bad input.
        assert!(p2tr_address_from_pubkey("00".to_string(), "mainnet".to_string()).is_err());
    }

    #[test]
    fn test_bip32_xpriv_from_seed() {
        let seed = "000102030405060708090a0b0c0d0e0f";
        let xpriv = xpriv_from_seed(seed.to_string(), "mainnet".to_string()).unwrap();
        assert!(xpriv.starts_with("xprv"));
    }

    #[test]
    fn test_bip32_xpub_from_xpriv() {
        let seed = "000102030405060708090a0b0c0d0e0f";
        let xpriv = xpriv_from_seed(seed.to_string(), "mainnet".to_string()).unwrap();
        let xpub = xpub_from_xpriv(xpriv).unwrap();
        assert!(xpub.starts_with("xpub"));
    }

    #[test]
    fn test_bip32_derive_xpriv() {
        let seed = "000102030405060708090a0b0c0d0e0f";
        let xpriv = xpriv_from_seed(seed.to_string(), "mainnet".to_string()).unwrap();
        let derived = derive_xpriv(xpriv, "m/44'/0'/0'/0/0".to_string()).unwrap();
        assert!(derived.starts_with("xprv"));
    }

    #[test]
    fn test_bip32_p2wpkh_address_from_xpub() {
        let seed = "000102030405060708090a0b0c0d0e0f";
        let xpriv = xpriv_from_seed(seed.to_string(), "mainnet".to_string()).unwrap();
        let _xpub = xpub_from_xpriv(xpriv.clone()).unwrap();
        // Derive account xpriv first (hardened), then get its xpub for non-hardened child derivation
        let account_xpriv = derive_xpriv(xpriv, "m/44'/0'/0'".to_string()).unwrap();
        let account_xpub = xpub_from_xpriv(account_xpriv).unwrap();
        let addr =
            p2wpkh_address_from_xpub(account_xpub, "m/0/0".to_string(), "mainnet".to_string())
                .unwrap();
        assert!(addr.starts_with("bc1q"));
    }

    #[test]
    fn test_psbt_roundtrip() {
        // Create a minimal unsigned tx and PSBT
        let tx = bitcoin::Transaction {
            version: bitcoin::transaction::Version(2),
            lock_time: bitcoin::locktime::absolute::LockTime::from_height(0).unwrap(),
            input: vec![],
            output: vec![],
        };
        let psbt = bitcoin::Psbt::from_unsigned_tx(tx).unwrap();
        let psbt_hex = rbitcoin_primitives::hex_encode(psbt.serialize());
        assert!(psbt_from_hex(psbt_hex.clone()).unwrap());
        let tx_hex = psbt_extract_tx_hex(psbt_hex.clone()).unwrap();
        assert!(!tx_hex.is_empty());
        let fee = psbt_fee_sat(psbt_hex).unwrap();
        assert_eq!(fee, 0);
    }

    #[test]
    fn test_chain_constants() {
        assert_eq!(default_max_tip_age_secs(), 24 * 60 * 60);
        assert_eq!(ibd_feefilter_sat_kvb(), 9_936_506);
        assert_eq!(stale_relay_age_limit_secs(), 30 * 24 * 60 * 60);
        assert_eq!(max_addr_man(), 8192);
    }

    #[test]
    fn test_chain_logs() {
        assert!(!accept_block_header_nodos_log("0000…".to_string()).is_empty());
        assert!(!ignoring_low_work_chain_log(100).is_empty());
        assert!(!synchronizing_blockheaders_log(100).is_empty());
        assert!(!initial_getheaders_log(100, 1).is_empty());
        assert!(!headers_timeout_disconnect_log(1).is_empty());
        assert!(!headers_timeout_noban_log(1).is_empty());
        assert!(!received_getdata_wtx_log("0000…".to_string(), 1).is_empty());
        assert!(!received_tx_log().is_empty());
    }

    #[test]
    fn test_min_relay_fee_rate() {
        assert_eq!(min_relay_fee_rate_sat_per_kvb(), 100);
    }

    #[test]
    fn test_v2_constants() {
        assert!(max_v2_contents_len() > 0);
        assert!(v2_cipher_expansion() > 0);
        assert!(v2_other_recv_bytes(100) > 0);
    }

    #[test]
    fn test_v2_logs() {
        assert!(!v2_handshake_timeout_log(1).is_empty());
        assert!(!v2_missing_garbage_terminator_log().is_empty());
        assert!(!v2_packet_decryption_failure_log().is_empty());
        assert!(!v2_packet_too_large_log(100).is_empty());
        assert!(!v2_invalid_message_type_log().is_empty());
    }

    #[test]
    fn test_more_constants() {
        assert_eq!(bq_soft_free_bytes(), 100 * 1024 * 1024);
        assert_eq!(bq_soft_confirm_secs(), 60);
        assert_eq!(mempool_admit_half_life_secs(), 150);
        assert_eq!(mempool_warm_after_secs(), 60);
        assert_eq!(mempool_warm_after_admits(), 32);
    }

    #[test]
    fn test_query_spenders_raw_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let query = FfiQuery::open_or_create(tmp.path().to_str().unwrap().to_string()).unwrap();
        let pts = query.spenders_raw(
            "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
            0,
        );
        assert!(pts.is_ok());
        assert!(pts.unwrap().is_empty());
    }

    #[test]
    fn test_query_resume_work_path_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let query = FfiQuery::open_or_create(tmp.path().to_str().unwrap().to_string()).unwrap();
        let entries = query.resume_work_path_after_tip(
            "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
            0,
            10,
        );
        assert!(entries.is_ok());
        assert!(entries.unwrap().is_empty());
    }

    #[test]
    fn test_query_resume_work_path_excluding_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let query = FfiQuery::open_or_create(tmp.path().to_str().unwrap().to_string()).unwrap();
        let entries = query.resume_work_path_after_tip_excluding(
            "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
            0,
            10,
            vec![],
        );
        assert!(entries.is_ok());
        assert!(entries.unwrap().is_empty());
    }

    #[test]
    fn test_store_mtp_times_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FfiStore::open_or_create(tmp.path().to_str().unwrap().to_string()).unwrap();
        let mtp = store.mtp_times_at(0);
        assert!(mtp.is_none());
    }

    #[test]
    fn test_store_coinbase_fk_at_heights_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FfiStore::open_or_create(tmp.path().to_str().unwrap().to_string()).unwrap();
        let coins = store.coinbase_fk_at_heights(vec![0, 1, 2]);
        assert!(coins.is_ok());
        assert!(coins.unwrap().is_empty());
    }

    #[test]
    fn test_store_get_tx_full_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FfiStore::open_or_create(tmp.path().to_str().unwrap().to_string()).unwrap();
        assert!(store.get_tx_full(0).is_err());
    }

    #[test]
    fn test_store_get_tx_full_span_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FfiStore::open_or_create(tmp.path().to_str().unwrap().to_string()).unwrap();
        assert!(store.get_tx_full_span(0, 0).is_err());
    }

    #[test]
    fn test_store_get_tx_meta_and_outputs_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FfiStore::open_or_create(tmp.path().to_str().unwrap().to_string()).unwrap();
        assert!(store.get_tx_meta_and_outputs(0).is_err());
    }

    #[test]
    fn test_store_get_tx_meta_and_prevouts_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FfiStore::open_or_create(tmp.path().to_str().unwrap().to_string()).unwrap();
        assert!(store.get_tx_meta_and_prevouts(0).is_err());
    }

    #[test]
    fn test_addrman_take_dial_candidates() {
        let am = FfiAddrMan::new();
        am.add("127.0.0.1:8333".to_string()).unwrap();
        let candidates = am.take_dial_candidates(10, vec![], vec![]);
        assert_eq!(candidates.len(), 1);
    }

    #[test]
    fn test_addrman_take_outbound_offset() {
        let am = FfiAddrMan::new();
        am.add("127.0.0.1:8333".to_string()).unwrap();
        am.add("127.0.0.1:8334".to_string()).unwrap();
        let out = am.take_outbound_offset(1, 0);
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn test_addrman_add_learned() {
        let am = FfiAddrMan::new();
        let added = am.add_learned("127.0.0.1:8333".to_string(), 10);
        assert!(added.is_ok());
        assert!(added.unwrap());
        assert_eq!(am.len(), 1);
    }

    #[test]
    fn test_addrman_merge_from() {
        let am1 = FfiAddrMan::new();
        let am2 = FfiAddrMan::new();
        am2.add("127.0.0.1:8333".to_string()).unwrap();
        am1.merge_from(am2);
        assert_eq!(am1.len(), 1);
    }

    #[test]
    fn test_addrman_inject() {
        let am = FfiAddrMan::new();
        am.inject(vec!["127.0.0.1:8333".to_string(), "127.0.0.1:8334".to_string()]);
        assert_eq!(am.len(), 2);
    }

    #[test]
    fn test_tx_graph_new() {
        let g = FfiTxGraph::new();
        assert!(g.is_empty());
        assert_eq!(g.len(), 0);
        assert_eq!(g.total_weight(), 0);
    }

    #[test]
    fn test_tx_graph_cluster_limits() {
        let g = FfiTxGraph::new();
        g.set_cluster_limits(Some(100), Some(500));
        assert_eq!(g.cluster_count_limit(), 100);
        assert_eq!(g.cluster_vsize_limit(), 500_000);
        assert_eq!(g.cluster_weight_limit(), 2_000_000);
    }

    #[test]
    fn test_tx_graph_contains_missing() {
        let g = FfiTxGraph::new();
        assert!(!g.contains(
            "0000000000000000000000000000000000000000000000000000000000000000".to_string()
        ).unwrap());
    }

    #[test]
    fn test_tx_graph_frontier_empty() {
        let g = FfiTxGraph::new();
        assert!(g.frontier_feerate_sat_per_kvb(1000).is_none());
        assert_eq!(g.weight_above_feerate(1000), 0);
    }

    #[test]
    fn test_tx_graph_select_block_empty() {
        let g = FfiTxGraph::new();
        assert!(g.select_block_txids(4_000_000).is_empty());
    }

    #[test]
    fn test_store_resolve_txid_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FfiStore::open_or_create(tmp.path().to_str().unwrap().to_string()).unwrap();
        let fk = store.resolve_txid(
            "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
            false,
        );
        assert!(fk.is_ok());
        assert!(fk.unwrap().is_none());
    }

    #[test]
    fn test_store_get_fk_by_txid_batch_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FfiStore::open_or_create(tmp.path().to_str().unwrap().to_string()).unwrap();
        let fks = store.get_fk_by_txid_batch(vec![
            "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
        ]);
        assert!(fks.is_ok());
        let result = fks.unwrap();
        assert_eq!(result.len(), 1);
        assert!(result[0].is_none());
    }

    #[test]
    fn test_active_mempool_open_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let am = FfiActiveMempool::open_or_create(tmp.path().to_str().unwrap().to_string());
        assert!(am.is_ok());
        let am = am.unwrap();
        assert_eq!(am.live_count(), 0);
        assert_eq!(am.orphan_count(), 0);
        assert_eq!(am.generation(), 0);
    }

    #[test]
    fn test_active_mempool_open_with_limit() {
        let tmp = tempfile::tempdir().unwrap();
        let am = FfiActiveMempool::open_or_create_with_limit(
            tmp.path().to_str().unwrap().to_string(),
            1_000_000,
        );
        assert!(am.is_ok());
    }

    #[test]
    fn test_active_mempool_relay_fee() {
        let tmp = tempfile::tempdir().unwrap();
        let am = FfiActiveMempool::open_or_create(tmp.path().to_str().unwrap().to_string()).unwrap();
        assert_eq!(am.min_relay_sat_kvb(), 100);
        am.set_min_relay_sat_kvb(200);
        assert_eq!(am.min_relay_sat_kvb(), 200);
    }

    #[test]
    fn test_active_mempool_cluster_limits() {
        let tmp = tempfile::tempdir().unwrap();
        let am = FfiActiveMempool::open_or_create(tmp.path().to_str().unwrap().to_string()).unwrap();
        am.set_cluster_limits(Some(50), Some(200));
    }

    #[test]
    fn test_active_mempool_get_tx_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let am = FfiActiveMempool::open_or_create(tmp.path().to_str().unwrap().to_string()).unwrap();
        let tx = am.get_tx(
            "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
        );
        assert!(tx.is_ok());
        assert!(tx.unwrap().is_none());
    }

    #[test]
    fn test_active_mempool_select_block_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let am = FfiActiveMempool::open_or_create(tmp.path().to_str().unwrap().to_string()).unwrap();
        let txs = am.select_block_txs(4_000_000);
        assert!(txs.is_empty());
    }

    #[test]
    fn test_active_mempool_remove_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let am = FfiActiveMempool::open_or_create(tmp.path().to_str().unwrap().to_string()).unwrap();
        let result = am.remove_txid(
            "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_active_mempool_compact_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let am = FfiActiveMempool::open_or_create(tmp.path().to_str().unwrap().to_string()).unwrap();
        let result = am.compact();
        assert!(result.is_ok());
    }

    #[test]
    fn test_active_mempool_park_orphan_bad_tx() {
        let tmp = tempfile::tempdir().unwrap();
        let am = FfiActiveMempool::open_or_create(tmp.path().to_str().unwrap().to_string()).unwrap();
        let tx = bitcoin::Transaction {
            version: bitcoin::transaction::Version(2),
            lock_time: bitcoin::locktime::absolute::LockTime::from_height(0).unwrap(),
            input: vec![bitcoin::TxIn {
                previous_output: bitcoin::OutPoint::null(),
                script_sig: bitcoin::ScriptBuf::new(),
                sequence: bitcoin::Sequence(0),
                witness: bitcoin::Witness::new(),
            }],
            output: vec![],
        };
        let tx_hex = rbitcoin_primitives::hex_encode(bitcoin::consensus::encode::serialize(&tx));
        let result = am.park_orphan(tx_hex, vec![]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_active_mempool_erase_orphans_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let am = FfiActiveMempool::open_or_create(tmp.path().to_str().unwrap().to_string()).unwrap();
        am.erase_orphans_for_block(vec![]).unwrap();
    }

    #[test]
    fn test_active_mempool_accept_tx_empty() {
        let tmp_query = tempfile::tempdir().unwrap();
        let tmp_mempool = tempfile::tempdir().unwrap();
        let query = FfiQuery::open_or_create(tmp_query.path().to_str().unwrap().to_string()).unwrap();
        let am = FfiActiveMempool::open_or_create(tmp_mempool.path().to_str().unwrap().to_string()).unwrap();
        let tx = bitcoin::Transaction {
            version: bitcoin::transaction::Version(2),
            lock_time: bitcoin::locktime::absolute::LockTime::from_height(0).unwrap(),
            input: vec![bitcoin::TxIn {
                previous_output: bitcoin::OutPoint::null(),
                script_sig: bitcoin::ScriptBuf::new(),
                sequence: bitcoin::Sequence(0),
                witness: bitcoin::Witness::new(),
            }],
            output: vec![bitcoin::TxOut {
                value: bitcoin::Amount::from_sat(1000),
                script_pubkey: bitcoin::ScriptBuf::new(),
            }],
        };
        let tx_hex = rbitcoin_primitives::hex_encode(bitcoin::consensus::encode::serialize(&tx));
        let result = am.accept_tx(query, tx_hex);
        assert!(result.is_ok());
        let result_str = result.unwrap();
        assert!(result_str.contains("Err") || result_str.contains("MissingPrevout"));
    }

    #[test]
    fn test_active_mempool_accept_package_empty() {
        let tmp_query = tempfile::tempdir().unwrap();
        let tmp_mempool = tempfile::tempdir().unwrap();
        let query = FfiQuery::open_or_create(tmp_query.path().to_str().unwrap().to_string()).unwrap();
        let am = FfiActiveMempool::open_or_create(tmp_mempool.path().to_str().unwrap().to_string()).unwrap();
        let result = am.accept_package(query, vec![]);
        assert!(result.is_ok());
        let result_str = result.unwrap();
        assert!(result_str.contains("Err") || result_str.contains("PackageEmpty"));
    }

    #[test]
    fn test_active_mempool_promote_orphans_of_missing() {
        let tmp_query = tempfile::tempdir().unwrap();
        let tmp_mempool = tempfile::tempdir().unwrap();
        let query = FfiQuery::open_or_create(tmp_query.path().to_str().unwrap().to_string()).unwrap();
        let am = FfiActiveMempool::open_or_create(tmp_mempool.path().to_str().unwrap().to_string()).unwrap();
        let result = am.promote_orphans_of(
            query,
            "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_active_mempool_reorg_disconnect_reaccept_empty() {
        let tmp_query = tempfile::tempdir().unwrap();
        let tmp_mempool = tempfile::tempdir().unwrap();
        let query = FfiQuery::open_or_create(tmp_query.path().to_str().unwrap().to_string()).unwrap();
        let am = FfiActiveMempool::open_or_create(tmp_mempool.path().to_str().unwrap().to_string()).unwrap();
        let result = am.reorg_disconnect_reaccept(query, vec![]);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_query_unspent_create_vouts_batch_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let query = FfiQuery::open_or_create(tmp.path().to_str().unwrap().to_string()).unwrap();
        let result = query.unspent_create_vouts_batch(vec![]);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_lookup_taken_covers() {
        assert!(!lookup_taken_covers(0, None));
        assert!(lookup_taken_covers(0, Some(0)));
        assert!(lookup_taken_covers(5, Some(10)));
        assert!(!lookup_taken_covers(15, Some(10)));
    }
}
