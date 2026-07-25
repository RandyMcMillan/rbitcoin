//! Bitcoin Core `tx_valid.json` / `tx_invalid.json` smoke runners.
//!
//! Full flag mapping is expanded over time. This binary ensures fixtures are
//! present, parse, and that we can walk every row without panicking.

use serde_json::Value;
use std::fs;
use std::path::PathBuf;

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures").join(name)
}

fn load_array(name: &str) -> Vec<Value> {
    let path = fixture(name);
    let s = fs::read_to_string(&path).unwrap_or_else(|e| panic!("missing {path:?}: {e}"));
    let v: Value = serde_json::from_str(&s).unwrap_or_else(|e| panic!("parse {name}: {e}"));
    v.as_array()
        .cloned()
        .unwrap_or_else(|| panic!("{name}: root not array"))
}

/// Count non-comment rows (arrays whose first element is itself an array of prevouts).
fn data_rows(rows: &[Value]) -> usize {
    rows.iter()
        .filter(|r| {
            r.as_array()
                .map(|a| a.first().map(|x| x.is_array()).unwrap_or(false))
                .unwrap_or(false)
        })
        .count()
}

#[test]
fn tx_valid_json_present_and_parseable() {
    let rows = load_array("tx_valid.json");
    let n = data_rows(&rows);
    assert!(n > 50, "expected many valid tx vectors, got {n}");
}

#[test]
fn tx_invalid_json_present_and_parseable() {
    let rows = load_array("tx_invalid.json");
    let n = data_rows(&rows);
    assert!(n > 50, "expected many invalid tx vectors, got {n}");
}
