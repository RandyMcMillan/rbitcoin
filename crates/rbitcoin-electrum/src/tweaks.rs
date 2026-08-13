//! Cake-compatible `blockchain.tweaks.subscribe` (naive, uncached).

use rbitcoin_consensus::{tweaks_for_height, ChainParams, TxTweak};
use rbitcoin_primitives::{hex_encode, Height};
use rbitcoin_query::Query;
use serde_json::{json, Map, Value};
use std::time::Instant;

/// Hard cap on `count` (Cake may pass the remaining chain).
pub const MAX_TWEAK_COUNT: u32 = 8;

/// JSON-RPC result: height string → txid display hex → `{tweak, output_pubkeys}`.
pub fn subscribe(query: &Query, params: &Value, chain: &ChainParams) -> Result<Value, String> {
    let height = param_u32(params, 0)?;
    let count = param_u32(params, 1).unwrap_or(1).clamp(1, MAX_TWEAK_COUNT);
    let _historical = param_bool(params, 2); // accepted, ignored

    let t0 = Instant::now();
    let tip = query.tip_height().map(|h| h.0);
    let mut map = Map::new();
    if let Some(tip) = tip {
        for h in height..height.saturating_add(count) {
            if h > tip {
                break;
            }
            let tweaks = tweaks_for_height(query, chain, Height(h)).map_err(|e| e.to_string())?;
            map.insert(h.to_string(), height_object(&tweaks));
        }
    }
    if map.is_empty() {
        // Probe `[0,1,false]` on an empty store / pre-tip must still succeed.
        map.insert(height.to_string(), json!({}));
    }
    let wall_ms = t0.elapsed().as_millis();
    rbitcoin_log::debug!(
        "electrum: tweaks h={height} n={} wall_ms={wall_ms}",
        map.len()
    );
    Ok(Value::Object(map))
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
