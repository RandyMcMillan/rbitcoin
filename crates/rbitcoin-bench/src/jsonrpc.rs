use serde_json::{json, Value};

pub fn request(id: u64, method: &str, params: Value) -> String {
    json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}).to_string()
}

pub fn result_ok(line: &str) -> Result<Value, String> {
    let v: Value = serde_json::from_str(line).map_err(|e| e.to_string())?;
    if let Some(err) = v.get("error").filter(|e| !e.is_null()) {
        return Err(err.to_string());
    }
    Ok(v.get("result").cloned().unwrap_or(Value::Null))
}

pub fn history_len(result: &Value) -> u64 {
    result.as_array().map(|a| a.len() as u64).unwrap_or(0)
}

pub fn utxo_len(result: &Value) -> u64 {
    history_len(result)
}

fn item_height(item: &Value) -> Option<i64> {
    item.get("height").and_then(|v| v.as_i64()).or_else(|| {
        item.pointer("/status/block_height")
            .and_then(|v| v.as_i64())
    })
}

/// Confirmed height span (skips mempool height ≤ 0).
pub fn height_span(result: &Value) -> (Option<u32>, Option<u32>) {
    let Some(arr) = result.as_array() else {
        return (None, None);
    };
    let mut lo: Option<u32> = None;
    let mut hi: Option<u32> = None;
    for item in arr {
        let Some(h) = item_height(item) else {
            continue;
        };
        if h <= 0 {
            continue;
        }
        let h = h as u32;
        lo = Some(lo.map_or(h, |x| x.min(h)));
        hi = Some(hi.map_or(h, |x| x.max(h)));
    }
    (lo, hi)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_is_one_line_object() {
        let s = request(3, "server.ping", json!([]));
        assert!(s.contains("\"id\":3"));
        assert!(!s.contains('\n'));
    }

    #[test]
    fn result_ok_and_error() {
        assert_eq!(
            result_ok(r#"{"jsonrpc":"2.0","id":1,"result":null}"#).unwrap(),
            Value::Null
        );
        assert!(result_ok(r#"{"id":1,"error":{"code":-1,"message":"no"}}"#)
            .unwrap_err()
            .contains("no"));
        assert_eq!(history_len(&json!([1, 2, 3])), 3);
    }

    #[test]
    fn height_span_skips_mempool_and_reads_esplora() {
        let hist = json!([
            {"height": 0, "tx_hash": "m"},
            {"height": 100, "tx_hash": "a"},
            {"height": 800000, "tx_hash": "b"},
            {"height": -1, "tx_hash": "u"}
        ]);
        assert_eq!(height_span(&hist), (Some(100), Some(800_000)));
        let utxo = json!([
            {"txid": "x", "status": {"block_height": 700000, "confirmed": true}},
            {"txid": "y", "status": {"confirmed": false}}
        ]);
        assert_eq!(height_span(&utxo), (Some(700_000), Some(700_000)));
        assert_eq!(height_span(&json!([])), (None, None));
    }
}
