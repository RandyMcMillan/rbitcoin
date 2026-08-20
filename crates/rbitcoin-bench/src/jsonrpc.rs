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
}
