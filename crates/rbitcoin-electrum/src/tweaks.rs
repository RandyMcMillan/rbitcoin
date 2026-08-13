//! Cake-compatible `blockchain.tweaks.subscribe` (naive, uncached).

use rbitcoin_consensus::{tweaks_at_height, ChainParams, TxTweak};
use rbitcoin_primitives::{hex_encode, Height};
use rbitcoin_query::Query;
use serde_json::{json, Map, Value};
use std::time::Instant;

/// Parsed `blockchain.tweaks.subscribe` window.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TweakReq {
    pub start: u32,
    /// Requested height count (Cake scan sends tip − restore). Served through tip.
    pub count: u32,
}

pub fn parse_req(params: &Value) -> Result<TweakReq, String> {
    let start = param_u32(params, 0)?;
    let count = param_u32(params, 1).unwrap_or(1).max(1);
    let _historical = param_bool(params, 2);
    Ok(TweakReq { start, count })
}

/// Inclusive last height to serve (`start` if `count==1`), not past tip.
pub fn last_height(start: u32, count: u32, tip: Option<u32>) -> Option<u32> {
    let tip = tip?;
    if start > tip {
        return None;
    }
    Some(start.saturating_add(count.saturating_sub(1)).min(tip))
}

/// One height key → txs. Cake `fromJson` uses the **last** map key as `block`.
pub fn height_map(query: &Query, chain: &ChainParams, h: u32) -> Result<Value, String> {
    let tip = query.tip_height().map(|t| t.0);
    if tip.is_none_or(|t| h > t) {
        return Ok(json!({ h.to_string(): {} }));
    }
    let tweaks = tweaks_at_height(query, chain, Height(h)).map_err(|e| e.to_string())?;
    Ok(json!({ h.to_string(): height_object(&tweaks) }))
}

/// Cake isolate notification: `params[0]` is the height map.
pub fn height_notify(map: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "method": "blockchain.tweaks.subscribe",
        "params": [map],
    })
}

/// Cake `noData` / resubscribe signal (`fromJson` catch path reads `message`).
pub fn done_notify() -> Value {
    json!({
        "jsonrpc": "2.0",
        "method": "blockchain.tweaks.subscribe",
        "params": [{"message": "done"}],
    })
}

/// JSON-RPC **result** is the **first** height only (Cake `getTweaks` / subscribe
/// first stream event). Further heights are notifications from the server loop.
pub fn subscribe(query: &Query, params: &Value, chain: &ChainParams) -> Result<Value, String> {
    let req = parse_req(params)?;
    let t0 = Instant::now();
    let map = height_map(query, chain, req.start)?;
    rbitcoin_log::debug!(
        "electrum: tweaks h={} count={} result_keys={} wall_ms={}",
        req.start,
        req.count,
        map.as_object().map(|o| o.len()).unwrap_or(0),
        t0.elapsed().as_millis()
    );
    Ok(map)
}

pub fn height_object(tweaks: &std::collections::BTreeMap<[u8; 32], TxTweak>) -> Value {
    let mut txs = Map::new();
    for (txid, t) in tweaks {
        txs.insert(txid_display_hex(txid), encode_tx_tweak(t));
    }
    Value::Object(txs)
}

pub fn encode_tx_tweak(t: &TxTweak) -> Value {
    let mut outs = Map::new();
    for o in &t.output_pubkeys {
        outs.insert(o.vout.to_string(), json!([hex_encode(o.xonly), o.value]));
    }
    json!({
        "tweak": hex_encode(t.tweak),
        "output_pubkeys": Value::Object(outs),
    })
}

fn txid_display_hex(txid: &[u8; 32]) -> String {
    let mut r = *txid;
    r.reverse();
    hex_encode(r)
}

fn param_u32(params: &Value, idx: usize) -> Result<u32, String> {
    params
        .as_array()
        .and_then(|a| a.get(idx))
        .and_then(|v| {
            v.as_u64()
                .map(|n| n as u32)
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
        .ok_or_else(|| format!("param {idx} expected number"))
}

fn param_bool(params: &Value, idx: usize) -> Option<bool> {
    params.as_array().and_then(|a| a.get(idx)).and_then(|v| {
        v.as_bool()
            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cake_probe_fixture_is_empty_height_map() {
        let raw = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/tweaks_cake_probe.json"
        ));
        let v: Value = serde_json::from_str(raw).unwrap();
        assert!(v.get("0").unwrap().as_object().unwrap().is_empty());
    }

    #[test]
    fn cake_850000_sample_encoding() {
        let raw = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/tweaks_cake_850000_sample.json"
        ));
        let v: Value = serde_json::from_str(raw).unwrap();
        let tx = &v["850000"]["0185a62484ca086b1a620552c770f852fb2303ff26f85849beb66f767da4e078"];
        let tweak = tx["tweak"].as_str().unwrap();
        assert_eq!(tweak.len(), 66);
        assert!(tweak.starts_with("02") || tweak.starts_with("03"));
        let pk = tx["output_pubkeys"]["1"][0].as_str().unwrap();
        assert_eq!(pk.len(), 64);
        assert_eq!(tx["output_pubkeys"]["1"][1], 5410);
    }

    #[test]
    fn last_height_stops_at_tip() {
        assert_eq!(last_height(880_791, 81_427, Some(962_217)), Some(962_217));
        assert_eq!(last_height(10, 3, Some(11)), Some(11));
        assert_eq!(last_height(10, 1, Some(100)), Some(10));
        assert_eq!(last_height(50, 1, Some(10)), None);
        assert_eq!(last_height(0, 1, None), None);
    }

    #[test]
    fn done_notify_is_cake_message() {
        let v = done_notify();
        assert_eq!(v["method"], "blockchain.tweaks.subscribe");
        assert_eq!(v["params"][0]["message"], "done");
    }

    #[test]
    fn encode_tx_tweak_xonly_and_33_byte() {
        let t = TxTweak {
            tweak: {
                let mut a = [0u8; 33];
                a[0] = 0x02;
                a[1] = 0xaa;
                a
            },
            output_pubkeys: vec![rbitcoin_consensus::TaprootOut {
                vout: 1,
                xonly: [0x5f; 32],
                value: 5410,
            }],
        };
        let v = encode_tx_tweak(&t);
        assert_eq!(v["tweak"].as_str().unwrap().len(), 66);
        assert!(v["tweak"].as_str().unwrap().starts_with("02"));
        assert_eq!(v["output_pubkeys"]["1"][0].as_str().unwrap().len(), 64);
        assert_eq!(v["output_pubkeys"]["1"][1], 5410);
    }
}
