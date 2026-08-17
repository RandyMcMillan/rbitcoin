//! Core `getblockstats` — reconstruct a block and sum fees / UTXO / weight.

use crate::methods::{
    parse_hash32_display, rpc_error, RpcContext, RpcParams, ERR_INVALID_ADDRESS_OR_KEY,
    ERR_INVALID_PARAMETER, ERR_MISC,
};
use bitcoin::consensus::Encodable;
use bitcoin::hashes::Hash;
use bitcoin::{Amount, Block, OutPoint, ScriptBuf, TxOut};
use rbitcoin_primitives::Height;
use serde_json::{json, Map, Value};

/// Core `PER_UTXO_OVERHEAD` = `sizeof(COutPoint)+sizeof(uint32_t)+sizeof(bool)`.
pub const PER_UTXO_OVERHEAD: i64 = 41;

/// Weight percentiles reported by `getblockstats` (10 / 25 / 50 / 75 / 90).
const PERCENTILES: [i64; 5] = [10, 25, 50, 75, 90];

const HELP: &str = "getblockstats hash_or_height ( stats )";

/// Core `CScript::IsUnspendable`: leading `OP_RETURN`, or over `MAX_SCRIPT_SIZE`.
pub fn is_unspendable(script: &[u8]) -> bool {
    script.first() == Some(&0x6a) || script.len() > 10_000
}

/// Consensus-serialized `CTxOut` size (value + compact script).
pub fn txout_serialized_size(out: &TxOut) -> i64 {
    let mut buf = Vec::new();
    out.consensus_encode(&mut buf)
        .expect("TxOut consensus encode is infallible");
    buf.len() as i64
}

/// Core `CalculateTruncatedMedian`: even length is the integer mean of the two middles.
pub fn truncated_median(mut scores: Vec<i64>) -> i64 {
    let n = scores.len();
    if n == 0 {
        return 0;
    }
    scores.sort_unstable();
    if n.is_multiple_of(2) {
        (scores[n / 2 - 1] + scores[n / 2]) / 2
    } else {
        scores[n / 2]
    }
}

/// Core `CalculatePercentilesByWeight`. `scores` is `(feerate, weight)`.
pub fn percentiles_by_weight(mut scores: Vec<(i64, i64)>, total_weight: i64) -> [i64; 5] {
    let mut result = [0i64; 5];
    if total_weight == 0 {
        return result;
    }
    scores.sort_unstable();
    let mut next = 0usize;
    let mut cumulative = 0i64;
    for (feerate, weight) in scores {
        cumulative += weight;
        while next < 5 && cumulative >= total_weight * PERCENTILES[next] / 100 {
            result[next] = feerate;
            next += 1;
        }
    }
    result
}

/// All `getblockstats` fields for one reconstructed block.
#[derive(Debug, Clone)]
pub struct BlockStats {
    pub avgfee: i64,
    pub avgfeerate: i64,
    pub avgtxsize: i64,
    pub blockhash: String,
    pub feerate_percentiles: [i64; 5],
    pub height: u32,
    pub ins: i64,
    pub maxfee: i64,
    pub maxfeerate: i64,
    pub maxtxsize: i64,
    pub medianfee: i64,
    pub mediantime: u32,
    pub mediantxsize: i64,
    pub minfee: i64,
    pub minfeerate: i64,
    pub mintxsize: i64,
    pub outs: i64,
    pub subsidy: i64,
    pub swtotal_size: i64,
    pub swtotal_weight: i64,
    pub swtxs: i64,
    pub time: u32,
    pub total_out: i64,
    pub total_size: i64,
    pub total_weight: i64,
    pub totalfee: i64,
    pub txs: i64,
    pub utxo_increase: i64,
    pub utxo_size_inc: i64,
    pub utxo_increase_actual: i64,
    pub utxo_size_inc_actual: i64,
}

impl BlockStats {
    pub fn to_json(&self) -> Value {
        Value::Object(self.to_map())
    }

    fn to_map(&self) -> Map<String, Value> {
        let mut m = Map::new();
        m.insert("avgfee".into(), json!(self.avgfee));
        m.insert("avgfeerate".into(), json!(self.avgfeerate));
        m.insert("avgtxsize".into(), json!(self.avgtxsize));
        m.insert("blockhash".into(), json!(self.blockhash.clone()));
        m.insert(
            "feerate_percentiles".into(),
            json!(self.feerate_percentiles.to_vec()),
        );
        m.insert("height".into(), json!(self.height));
        m.insert("ins".into(), json!(self.ins));
        m.insert("maxfee".into(), json!(self.maxfee));
        m.insert("maxfeerate".into(), json!(self.maxfeerate));
        m.insert("maxtxsize".into(), json!(self.maxtxsize));
        m.insert("medianfee".into(), json!(self.medianfee));
        m.insert("mediantime".into(), json!(self.mediantime));
        m.insert("mediantxsize".into(), json!(self.mediantxsize));
        m.insert("minfee".into(), json!(self.minfee));
        m.insert("minfeerate".into(), json!(self.minfeerate));
        m.insert("mintxsize".into(), json!(self.mintxsize));
        m.insert("outs".into(), json!(self.outs));
        m.insert("subsidy".into(), json!(self.subsidy));
        m.insert("swtotal_size".into(), json!(self.swtotal_size));
        m.insert("swtotal_weight".into(), json!(self.swtotal_weight));
        m.insert("swtxs".into(), json!(self.swtxs));
        m.insert("time".into(), json!(self.time));
        m.insert("total_out".into(), json!(self.total_out));
        m.insert("total_size".into(), json!(self.total_size));
        m.insert("total_weight".into(), json!(self.total_weight));
        m.insert("totalfee".into(), json!(self.totalfee));
        m.insert("txs".into(), json!(self.txs));
        m.insert("utxo_increase".into(), json!(self.utxo_increase));
        m.insert("utxo_size_inc".into(), json!(self.utxo_size_inc));
        m.insert(
            "utxo_increase_actual".into(),
            json!(self.utxo_increase_actual),
        );
        m.insert(
            "utxo_size_inc_actual".into(),
            json!(self.utxo_size_inc_actual),
        );
        m
    }

    fn select(&self, stats: &[String]) -> Result<Value, Value> {
        if stats.is_empty() {
            return Ok(self.to_json());
        }
        let all = self.to_map();
        let mut out = Map::new();
        for s in stats {
            match all.get(s) {
                Some(v) => {
                    out.insert(s.clone(), v.clone());
                }
                None => {
                    return Err(rpc_error(
                        ERR_INVALID_PARAMETER,
                        format!("Invalid selected statistic '{s}'"),
                    ));
                }
            }
        }
        Ok(Value::Object(out))
    }
}

/// Sum fees / UTXO / weight for a reconstructed block (same numbers RPC returns).
pub fn compute_block_stats(
    height: u32,
    block: &Block,
    mediantime: u32,
    subsidy: i64,
    prevout: impl Fn(&OutPoint) -> Option<TxOut>,
) -> Result<BlockStats, String> {
    let mut ins = 0i64;
    let mut outs = 0i64;
    let mut utxo_size_inc = 0i64;
    let mut utxo_size_inc_actual = 0i64;
    let mut utxo_increase_actual = 0i64;
    let mut total_out = 0i64;
    let mut total_size = 0i64;
    let mut total_weight = 0i64;
    let mut totalfee = 0i64;
    let mut swtotal_size = 0i64;
    let mut swtotal_weight = 0i64;
    let mut swtxs = 0i64;
    let mut fees = Vec::new();
    let mut sizes = Vec::new();
    let mut feerate_weights: Vec<(i64, i64)> = Vec::new();
    let mut minfee = i64::MAX;
    let mut maxfee = 0i64;
    let mut minfeerate = i64::MAX;
    let mut maxfeerate = 0i64;
    let mut mintxsize = i64::MAX;
    let mut maxtxsize = 0i64;
    let skip_actual = height == 0;

    for tx in &block.txdata {
        let is_cb = tx.is_coinbase();
        let tx_size = tx.total_size() as i64;
        let tx_weight = tx.weight().to_wu() as i64;
        let has_wit = tx.input.iter().any(|i| !i.witness.is_empty());

        if !is_cb {
            total_size += tx_size;
            total_weight += tx_weight;
            if has_wit {
                swtxs += 1;
                swtotal_size += tx_size;
                swtotal_weight += tx_weight;
            }
            let mut input_value = 0i64;
            for inp in &tx.input {
                ins += 1;
                let po = prevout(&inp.previous_output)
                    .ok_or_else(|| format!("missing prevout {}", inp.previous_output))?;
                input_value += po.value.to_sat() as i64;
                let ser = txout_serialized_size(&po) + PER_UTXO_OVERHEAD;
                utxo_size_inc -= ser;
                if !is_unspendable(po.script_pubkey.as_bytes()) {
                    utxo_size_inc_actual -= ser;
                    utxo_increase_actual -= 1;
                }
            }
            let mut output_value = 0i64;
            for o in &tx.output {
                output_value += o.value.to_sat() as i64;
            }
            total_out += output_value;
            let fee = input_value.saturating_sub(output_value);
            totalfee += fee;
            let feerate = if tx_weight > 0 {
                fee.saturating_mul(4) / tx_weight
            } else {
                0
            };
            fees.push(fee);
            sizes.push(tx_size);
            feerate_weights.push((feerate, tx_weight));
            minfee = minfee.min(fee);
            maxfee = maxfee.max(fee);
            minfeerate = minfeerate.min(feerate);
            maxfeerate = maxfeerate.max(feerate);
            mintxsize = mintxsize.min(tx_size);
            maxtxsize = maxtxsize.max(tx_size);
        }

        for o in &tx.output {
            outs += 1;
            let ser = txout_serialized_size(o) + PER_UTXO_OVERHEAD;
            utxo_size_inc += ser;
            if !skip_actual && !is_unspendable(o.script_pubkey.as_bytes()) {
                utxo_size_inc_actual += ser;
                utxo_increase_actual += 1;
            }
        }
    }

    let n_non_cb = fees.len() as i64;
    if n_non_cb == 0 {
        minfee = 0;
        minfeerate = 0;
        mintxsize = 0;
    }
    let avgfee = if n_non_cb > 0 { totalfee / n_non_cb } else { 0 };
    let avgfeerate = if total_weight > 0 {
        totalfee.saturating_mul(4) / total_weight
    } else {
        0
    };
    let avgtxsize = if n_non_cb > 0 {
        total_size / n_non_cb
    } else {
        0
    };

    Ok(BlockStats {
        avgfee,
        avgfeerate,
        avgtxsize,
        blockhash: block.block_hash().to_string(),
        feerate_percentiles: percentiles_by_weight(feerate_weights, total_weight),
        height,
        ins,
        maxfee,
        maxfeerate,
        maxtxsize,
        medianfee: truncated_median(fees),
        mediantime,
        mediantxsize: truncated_median(sizes),
        minfee,
        minfeerate,
        mintxsize,
        outs,
        subsidy,
        swtotal_size,
        swtotal_weight,
        swtxs,
        time: block.header.time,
        total_out,
        total_size,
        total_weight,
        totalfee,
        txs: block.txdata.len() as i64,
        utxo_increase: outs - ins,
        utxo_size_inc,
        utxo_increase_actual,
        utxo_size_inc_actual,
    })
}

fn help_err() -> Value {
    rpc_error(ERR_MISC, HELP)
}

fn parse_stats(params: &RpcParams) -> Result<Vec<String>, Value> {
    match params.get(1, "stats") {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(a)) => {
            let mut out = Vec::with_capacity(a.len());
            for v in a {
                let s = v.as_str().ok_or_else(|| {
                    rpc_error(ERR_INVALID_PARAMETER, "Invalid parameter, expected string")
                })?;
                out.push(s.to_string());
            }
            Ok(out)
        }
        Some(_) => Err(rpc_error(
            ERR_INVALID_PARAMETER,
            "Invalid parameter, expected array",
        )),
    }
}

enum HashOrHeight {
    Height(i64),
    Hash([u8; 32]),
}

fn parse_hash_or_height(v: &Value) -> Result<HashOrHeight, Value> {
    if let Some(n) = v.as_i64() {
        return Ok(HashOrHeight::Height(n));
    }
    if let Some(n) = v.as_u64() {
        return Ok(HashOrHeight::Height(n as i64));
    }
    if let Some(s) = v.as_str() {
        let h = parse_hash32_display(s)
            .map_err(|_| rpc_error(ERR_INVALID_ADDRESS_OR_KEY, "Block not found"))?;
        return Ok(HashOrHeight::Hash(h));
    }
    Err(rpc_error(
        ERR_INVALID_PARAMETER,
        "hash_or_height must be a hash string or height",
    ))
}

fn prevout_from_block_or_query(ctx: &RpcContext, block: &Block, op: &OutPoint) -> Option<TxOut> {
    for tx in &block.txdata {
        if tx.compute_txid() == op.txid {
            return tx.output.get(op.vout as usize).cloned();
        }
    }
    let (fk, rec) = ctx.query.get_tx_by_txid(&op.txid.to_byte_array()).ok()??;
    let out = ctx.query.tx_output_at_fk(fk, &rec, op.vout).ok()?;
    Some(TxOut {
        value: Amount::from_sat(out.value.max(0) as u64),
        script_pubkey: ScriptBuf::from_bytes(out.script),
    })
}

fn stats_for_connected(
    ctx: &RpcContext,
    height: Height,
    block: &Block,
) -> Result<BlockStats, Value> {
    let mediantime = rbitcoin_consensus::median_time_past(ctx.query.as_ref(), height)
        .unwrap_or(block.header.time);
    let params = match ctx.chain.as_ref() {
        Some(c) => c.params.clone(),
        None => rbitcoin_consensus::ChainParams::for_network(ctx.network),
    };
    let subsidy = rbitcoin_consensus::block_subsidy(height.0, &params);
    compute_block_stats(height.0, block, mediantime, subsidy, |op| {
        prevout_from_block_or_query(ctx, block, op)
    })
    .map_err(|e| rpc_error(ERR_MISC, e))
}

/// `getblockstats hash_or_height ( stats )`
pub fn getblockstats(ctx: &RpcContext, params: &RpcParams) -> Result<Value, Value> {
    params.reject_unknown(&["hash_or_height", "stats"])?;
    if params.pos_len() > 2 {
        return Err(help_err());
    }
    let Some(raw) = params.get(0, "hash_or_height") else {
        return Err(help_err());
    };
    let want = parse_stats(params)?;
    let tip = ctx.query.tip_height().map(|h| h.0).unwrap_or(0);

    let (height, block) = match parse_hash_or_height(raw)? {
        HashOrHeight::Height(h) => {
            if h < 0 {
                return Err(rpc_error(
                    ERR_INVALID_PARAMETER,
                    format!("Target block height {h} is negative"),
                ));
            }
            if h > i64::from(tip) {
                return Err(rpc_error(
                    ERR_INVALID_PARAMETER,
                    format!("Target block height {h} after current tip {tip}"),
                ));
            }
            let height = Height(h as u32);
            let block = ctx
                .query
                .reconstruct_block_at_height(height)
                .map_err(|_| rpc_error(ERR_MISC, "Block not found on disk"))?;
            (height, block)
        }
        HashOrHeight::Hash(hash) => {
            match ctx
                .query
                .height_of_hash(&hash)
                .map_err(|e| rpc_error(ERR_MISC, e.to_string()))?
            {
                Some(height) => {
                    let block = ctx
                        .query
                        .reconstruct_block_at_height(height)
                        .map_err(|_| rpc_error(ERR_MISC, "Block not found on disk"))?;
                    (height, block)
                }
                None => {
                    if ctx
                        .query
                        .get_header_by_hash(&hash)
                        .map_err(|e| rpc_error(ERR_MISC, e.to_string()))?
                        .is_some()
                    {
                        return Err(rpc_error(
                            ERR_MISC,
                            "Block not available (not fully downloaded)",
                        ));
                    }
                    return Err(rpc_error(ERR_INVALID_ADDRESS_OR_KEY, "Block not found"));
                }
            }
        }
    };

    let stats = stats_for_connected(ctx, height, &block)?;
    stats.select(&want)
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn truncated_median_even_averages_two_middles() {
        assert_eq!(truncated_median(vec![]), 0);
        assert_eq!(truncated_median(vec![3]), 3);
        assert_eq!(truncated_median(vec![1, 2, 3]), 2);
        assert_eq!(truncated_median(vec![1, 2, 3, 4]), 2);
        // rpc_getblockstats.py height 103: (2880 + 36600) / 2
        assert_eq!(truncated_median(vec![2880, 2880, 36600, 43200]), 19740);
    }

    #[test]
    fn percentiles_match_core_rpc_tests() {
        let mut scores = Vec::new();
        for _ in 0..100 {
            scores.push((1, 1));
        }
        for _ in 0..100 {
            scores.push((2, 1));
        }
        assert_eq!(percentiles_by_weight(scores, 200), [1, 1, 1, 2, 2]);

        let scores = vec![(1, 9), (2, 16), (4, 50), (5, 10), (9, 15)];
        assert_eq!(percentiles_by_weight(scores, 100), [2, 2, 4, 4, 9]);

        let scores = vec![(1, 9), (2, 11), (2, 5), (4, 50), (5, 10), (9, 15)];
        assert_eq!(percentiles_by_weight(scores, 100), [2, 2, 4, 4, 9]);

        let scores = vec![(1, 100), (2, 1), (3, 1), (3, 1), (999999, 1)];
        assert_eq!(percentiles_by_weight(scores, 104), [1, 1, 1, 1, 1]);
    }

    #[test]
    fn genesis_utxo_size_is_117() {
        let script = ScriptBuf::from_bytes({
            let mut s = vec![0x41];
            s.extend_from_slice(&[0u8; 65]);
            s.push(0xac);
            s
        });
        let out = TxOut {
            value: Amount::from_sat(50_0000_0000),
            script_pubkey: script,
        };
        assert_eq!(txout_serialized_size(&out) + PER_UTXO_OVERHEAD, 117);
        assert!(!is_unspendable(out.script_pubkey.as_bytes()));
        assert!(is_unspendable(&[0x6a, 0x01, 0x21]));
    }
}
