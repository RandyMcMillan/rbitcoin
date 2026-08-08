//! Esplora route handlers beyond tip/header/basic tx (Steps 8–11).

use crate::server::{block_hash_hex, not_found, parse_hash32, plain_ok, store_err, AppState};
use crate::tx_json::{build_tx_json, tx_status_json};
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use bitcoin::address::Address;
use bitcoin::consensus::deserialize;
use bitcoin::hashes::Hash;
use bitcoin::Network;
use rbitcoin_primitives::Height;
use rbitcoin_query::{HistoryFilter, Query};
use rbitcoin_store::script_hash;
use serde_json::{json, Value};
use std::str::FromStr;

// --- Block listing ---

pub async fn block_txids(State(st): State<AppState>, Path(hash_hex): Path<String>) -> Response {
    let Ok(hash) = parse_hash32(&hash_hex) else {
        return not_found();
    };
    let Some(h) = (match st.query.height_of_hash(&hash) {
        Ok(h) => h,
        Err(e) => return store_err(e),
    }) else {
        return not_found();
    };
    match st.query.block_tx_fks(h) {
        Ok(fks) => {
            let mut ids = Vec::with_capacity(fks.len());
            for fk in fks {
                match st.query.get_tx(fk) {
                    Ok(tx) => ids.push(block_hash_hex(&tx.txid)),
                    Err(e) => return store_err(e),
                }
            }
            Json(ids).into_response()
        }
        Err(e) => store_err(e),
    }
}

/// Optional start index via path: `/block/:hash/txs` or `/block/:hash/txs/:start`.
pub async fn block_txs_start(
    State(st): State<AppState>,
    Path((hash_hex, start)): Path<(String, u32)>,
) -> Response {
    block_txs_impl(st, &hash_hex, start)
}

pub async fn block_txs_0(State(st): State<AppState>, Path(hash_hex): Path<String>) -> Response {
    block_txs_impl(st, &hash_hex, 0)
}

fn block_txs_impl(st: AppState, hash_hex: &str, start: u32) -> Response {
    if start % 25 != 0 {
        return (
            StatusCode::BAD_REQUEST,
            "start_index must be a multiple of 25",
        )
            .into_response();
    }
    let Ok(hash) = parse_hash32(hash_hex) else {
        return not_found();
    };
    let Some(h) = (match st.query.height_of_hash(&hash) {
        Ok(h) => h,
        Err(e) => return store_err(e),
    }) else {
        return not_found();
    };
    match st.query.block_tx_fks(h) {
        Ok(fks) => {
            let page: Vec<_> = fks.into_iter().skip(start as usize).take(25).collect();
            let mut out = Vec::with_capacity(page.len());
            for fk in page {
                match build_tx_json(&st.query, fk, st.network) {
                    Ok(v) => out.push(v),
                    Err(e) => return store_err(e),
                }
            }
            Json(out).into_response()
        }
        Err(e) => store_err(e),
    }
}

// --- Merkle + outspends ---

pub async fn tx_merkle_proof(State(st): State<AppState>, Path(txid_hex): Path<String>) -> Response {
    let Ok(txid) = parse_hash32(&txid_hex) else {
        return not_found();
    };
    let Ok(Some((fk, _))) = st.query.get_tx_by_txid(&txid) else {
        return not_found();
    };
    let height = match st.query.store().tx_height.get(fk) {
        Ok(Some(h)) => h,
        Ok(None) => return not_found(),
        Err(e) => return store_err(e),
    };
    if !matches!(st.query.store().is_confirmed_strong(fk), Ok(true)) {
        return not_found();
    }
    match st.query.merkle_proof(Height(height), &txid) {
        Ok(proof) => {
            let merkle: Vec<String> = proof.merkle.iter().map(block_hash_hex).collect();
            Json(json!({
                "block_height": proof.block_height,
                "merkle": merkle,
                "pos": proof.pos,
            }))
            .into_response()
        }
        Err(e) => store_err(e),
    }
}

pub async fn tx_outspend(
    State(st): State<AppState>,
    Path((txid_hex, vout)): Path<(String, u32)>,
) -> Response {
    let Ok(txid) = parse_hash32(&txid_hex) else {
        return not_found();
    };
    if st.query.get_tx_by_txid(&txid).ok().flatten().is_none() {
        return not_found();
    }
    match outspend_json(&st.query, &txid, vout) {
        Ok(v) => Json(v).into_response(),
        Err(e) => store_err(e),
    }
}

pub async fn tx_outspends(State(st): State<AppState>, Path(txid_hex): Path<String>) -> Response {
    let Ok(txid) = parse_hash32(&txid_hex) else {
        return not_found();
    };
    let Some((_fk, rec)) = (match st.query.get_tx_by_txid(&txid) {
        Ok(v) => v,
        Err(e) => return store_err(e),
    }) else {
        return not_found();
    };
    let mut arr = Vec::with_capacity(rec.output_count as usize);
    for vout in 0..rec.output_count {
        match outspend_json(&st.query, &txid, vout) {
            Ok(v) => arr.push(v),
            Err(e) => return store_err(e),
        }
    }
    Json(arr).into_response()
}

fn outspend_json(
    query: &Query,
    txid: &[u8; 32],
    vout: u32,
) -> Result<Value, rbitcoin_query::QueryError> {
    let spenders = query.spenders(txid, vout)?;
    if spenders.is_empty() {
        return Ok(json!({ "spent": false }));
    }
    let p = &spenders[0];
    let spend_tx = query.get_tx(p.spending_tx_fk)?;
    let status = tx_status_json(query, p.spending_tx_fk)?;
    let mut vin = p.spending_input_index;
    if let Ok(wire) = query.reconstruct_tx(p.spending_tx_fk) {
        if let Some(i) = wire.input.iter().position(|inp| {
            inp.previous_output.txid.to_byte_array() == *txid && inp.previous_output.vout == vout
        }) {
            vin = i as u32;
        }
    }
    Ok(json!({
        "spent": true,
        "txid": block_hash_hex(&spend_tx.txid),
        "vin": vin,
        "status": status,
    }))
}

// --- Address / scripthash ---

pub async fn address_info(State(st): State<AppState>, Path(addr_s): Path<String>) -> Response {
    match resolve_address_sh(&addr_s, st.network) {
        Ok(sh) => match sh_stats_json(&st, &sh, Some(addr_s.as_str()), None) {
            Ok(v) => Json(v).into_response(),
            Err(e) => store_err(e),
        },
        Err(_) => not_found(),
    }
}

pub async fn scripthash_info(State(st): State<AppState>, Path(sh_hex): Path<String>) -> Response {
    let Ok(sh) = parse_hash32(&sh_hex) else {
        return not_found();
    };
    match sh_stats_json(&st, &sh, None, Some(sh_hex.as_str())) {
        Ok(v) => Json(v).into_response(),
        Err(e) => store_err(e),
    }
}

pub async fn address_utxo(State(st): State<AppState>, Path(addr_s): Path<String>) -> Response {
    match resolve_address_sh(&addr_s, st.network) {
        Ok(sh) => utxo_response(&st, &sh),
        Err(_) => not_found(),
    }
}

pub async fn scripthash_utxo(State(st): State<AppState>, Path(sh_hex): Path<String>) -> Response {
    let Ok(sh) = parse_hash32(&sh_hex) else {
        return not_found();
    };
    utxo_response(&st, &sh)
}

fn utxo_response(st: &AppState, sh: &[u8; 32]) -> Response {
    match st.query.scripthash_listunspent(sh) {
        Ok(list) => {
            let mut arr = Vec::new();
            for u in list {
                let status = match st.query.get_tx_by_txid(&u.tx_hash) {
                    Ok(Some((fk, _))) => {
                        tx_status_json(&st.query, fk).unwrap_or(json!({"confirmed": true}))
                    }
                    _ => json!({"confirmed": true, "block_height": u.height}),
                };
                arr.push(json!({
                    "txid": block_hash_hex(&u.tx_hash),
                    "vout": u.tx_pos,
                    "value": u.value,
                    "status": status,
                }));
            }
            Json(arr).into_response()
        }
        Err(e) => store_err(e),
    }
}

fn resolve_address_sh(addr_s: &str, network: Network) -> Result<[u8; 32], ()> {
    let addr = Address::from_str(addr_s).map_err(|_| ())?;
    let addr = addr.require_network(network).map_err(|_| ())?;
    Ok(script_hash(addr.script_pubkey().as_bytes()))
}

fn sh_stats_json(
    st: &AppState,
    sh: &[u8; 32],
    address: Option<&str>,
    scripthash_hex: Option<&str>,
) -> Result<Value, rbitcoin_query::QueryError> {
    let chain = st.query.scripthash_chain_stats(sh)?;
    let chain_stats = json!({
        "tx_count": chain.tx_count,
        "funded_txo_count": chain.funded_txo_count,
        "funded_txo_sum": chain.funded_txo_sum,
        "spent_txo_count": chain.spent_txo_count,
        "spent_txo_sum": chain.spent_txo_sum,
    });
    let mempool_stats = json!({
        "tx_count": 0,
        "funded_txo_count": 0,
        "funded_txo_sum": 0,
        "spent_txo_count": 0,
        "spent_txo_sum": 0,
    });
    // Mempool stats: optional refine when hub present (count mempool touches).
    let mempool_stats = if let Some(mp) = st.mempool.as_ref() {
        let items = mp.scripthash_mempool(sh);
        json!({
            "tx_count": items.len() as u32,
            "funded_txo_count": 0,
            "funded_txo_sum": 0,
            "spent_txo_count": 0,
            "spent_txo_sum": 0,
        })
    } else {
        mempool_stats
    };
    let mut obj = json!({
        "chain_stats": chain_stats,
        "mempool_stats": mempool_stats,
    });
    if let Some(a) = address {
        obj["address"] = Value::String(a.to_string());
    }
    if let Some(h) = scripthash_hex {
        obj["scripthash"] = Value::String(h.to_string());
    }
    Ok(obj)
}

// --- History pages ---

pub async fn scripthash_txs_chain(
    State(st): State<AppState>,
    Path(sh_hex): Path<String>,
) -> Response {
    chain_page(&st, &sh_hex, None)
}

pub async fn scripthash_txs_chain_cursor(
    State(st): State<AppState>,
    Path((sh_hex, last)): Path<(String, String)>,
) -> Response {
    let Ok(after) = parse_hash32(&last) else {
        return not_found();
    };
    chain_page(&st, &sh_hex, Some(after))
}

pub async fn address_txs_chain(State(st): State<AppState>, Path(addr_s): Path<String>) -> Response {
    match resolve_address_sh(&addr_s, st.network) {
        Ok(sh) => chain_page_sh(&st, &sh, None),
        Err(_) => not_found(),
    }
}

pub async fn address_txs_chain_cursor(
    State(st): State<AppState>,
    Path((addr_s, last)): Path<(String, String)>,
) -> Response {
    let Ok(after) = parse_hash32(&last) else {
        return not_found();
    };
    match resolve_address_sh(&addr_s, st.network) {
        Ok(sh) => chain_page_sh(&st, &sh, Some(after)),
        Err(_) => not_found(),
    }
}

/// Combined `/scripthash/:h/txs` = mempool (cap 50) + first chain page.
pub async fn scripthash_txs(State(st): State<AppState>, Path(sh_hex): Path<String>) -> Response {
    let Ok(sh) = parse_hash32(&sh_hex) else {
        return not_found();
    };
    combined_txs(&st, &sh)
}

pub async fn address_txs(State(st): State<AppState>, Path(addr_s): Path<String>) -> Response {
    match resolve_address_sh(&addr_s, st.network) {
        Ok(sh) => combined_txs(&st, &sh),
        Err(_) => not_found(),
    }
}

fn chain_page(st: &AppState, sh_hex: &str, after: Option<[u8; 32]>) -> Response {
    let Ok(sh) = parse_hash32(sh_hex) else {
        return not_found();
    };
    chain_page_sh(st, &sh, after)
}

fn chain_page_sh(st: &AppState, sh: &[u8; 32], after: Option<[u8; 32]>) -> Response {
    let filter = HistoryFilter::esplora_chain_page(after);
    match st.query.scripthash_history_filtered(sh, &filter) {
        Ok(items) => hist_to_tx_json(st, &items),
        Err(e) => store_err(e),
    }
}

fn combined_txs(st: &AppState, sh: &[u8; 32]) -> Response {
    let mut out = Vec::new();
    // Mempool first (up to 50), then first chain page.
    if let Some(mp) = st.mempool.as_ref() {
        for item in mp.scripthash_mempool(sh).into_iter().take(50) {
            if let Ok(Some((fk, _))) = st.query.get_tx_by_txid(&item.txid) {
                // Confirmed path shouldn't hit; mempool txs may not be in store.
                if let Ok(v) = build_tx_json(&st.query, fk, st.network) {
                    out.push(v);
                    continue;
                }
            }
            // Minimal mempool row if not in Class A store.
            out.push(json!({
                "txid": block_hash_hex(&item.txid),
                "status": { "confirmed": false },
                "fee": item.fee,
            }));
        }
    }
    let filter = HistoryFilter::esplora_chain_page(None);
    match st.query.scripthash_history_filtered(sh, &filter) {
        Ok(items) => {
            for item in items {
                if let Ok(Some((fk, _))) = st.query.get_tx_by_txid(&item.txid) {
                    if let Ok(v) = build_tx_json(&st.query, fk, st.network) {
                        out.push(v);
                    }
                }
            }
            Json(out).into_response()
        }
        Err(e) => store_err(e),
    }
}

fn hist_to_tx_json(st: &AppState, items: &[rbitcoin_query::ScriptHashHistoryItem]) -> Response {
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        match st.query.get_tx_by_txid(&item.txid) {
            Ok(Some((fk, _))) => match build_tx_json(&st.query, fk, st.network) {
                Ok(v) => out.push(v),
                Err(e) => return store_err(e),
            },
            Ok(None) => continue,
            Err(e) => return store_err(e),
        }
    }
    Json(out).into_response()
}

// --- Mempool / fees / broadcast ---

pub async fn mempool_info(State(st): State<AppState>) -> Response {
    let Some(mp) = st.mempool.as_ref() else {
        return Json(json!({
            "count": 0,
            "vsize": 0,
            "total_fee": 0,
            "fee_histogram": [],
        }))
        .into_response();
    };
    let live = mp.list_live();
    let count = live.len();
    let mut vsize = 0u64;
    let mut total_fee = 0u64;
    for (_txid, fee, weight, _tx) in &live {
        total_fee = total_fee.saturating_add(*fee);
        vsize = vsize.saturating_add(weight.saturating_add(3) / 4);
    }
    let hist: Vec<Value> = mp
        .fee_histogram()
        .into_iter()
        .map(|(rate_kvb, vs)| {
            // Convert sat/kvB → sat/vB for Esplora-style histogram.
            let rate_vb = (rate_kvb as f64) / 1000.0;
            json!([rate_vb, vs])
        })
        .collect();
    Json(json!({
        "count": count,
        "vsize": vsize,
        "total_fee": total_fee,
        "fee_histogram": hist,
    }))
    .into_response()
}

pub async fn fee_estimates(State(st): State<AppState>) -> Response {
    let mut obj = serde_json::Map::new();
    let targets = [1u32, 2, 3, 4, 5, 6, 10, 20, 144, 504, 1008];
    for t in targets {
        let btc_kb = st
            .mempool
            .as_ref()
            .map(|m| m.estimate_fee_btc_per_kb(t))
            .unwrap_or(-1.0);
        let sat_vb = if btc_kb < 0.0 {
            1.0 // floor when empty
        } else {
            // BTC/kB → sat/vB: * 1e8 / 1000
            btc_kb * 100_000.0
        };
        obj.insert(t.to_string(), json!(sat_vb));
    }
    Json(Value::Object(obj)).into_response()
}

pub async fn post_tx(State(st): State<AppState>, body: Bytes) -> Response {
    let Some(mp) = st.mempool.as_ref() else {
        return (StatusCode::SERVICE_UNAVAILABLE, "mempool not available").into_response();
    };
    if body.len() > st.max_body {
        return (StatusCode::PAYLOAD_TOO_LARGE, "body too large").into_response();
    }
    let hex = std::str::from_utf8(&body)
        .unwrap_or("")
        .trim()
        .trim_matches('"');
    let raw = match rbitcoin_primitives::hex_decode(hex) {
        Ok(r) => r,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, format!("invalid hex: {e}")).into_response();
        }
    };
    let tx: bitcoin::Transaction = match deserialize(&raw) {
        Ok(t) => t,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, format!("invalid tx: {e}")).into_response();
        }
    };
    match mp.accept_tx(&tx) {
        Ok(r) => {
            let tid = r.txid.to_byte_array();
            plain_ok(block_hash_hex(&tid))
        }
        Err(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}
