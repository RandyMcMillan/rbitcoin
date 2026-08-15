//! Thin binary: Core `tx_valid` / `tx_invalid` live as unit tests in
//! `script::core_tx_vectors` so they can call `pub(crate)` verify paths.
//!
//! Run: `cargo test -p rbitcoin-consensus --lib core_tx_`

use std::path::{Path, PathBuf};

#[test]
fn core_tx_vectors_live_in_lib() {
    // Structural: unit tests are compiled into the lib test harness.
    // Full runners: core_tx_valid_all_rows / core_tx_invalid_all_rows.
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    assert!(manifest.join("src/script/core_tx_vectors.rs").is_file());
    assert!(manifest.join("src/script/core_fixture.rs").is_file());
}

#[test]
fn core_json_comes_from_submodule_not_fixtures_dir() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    for name in ["script_tests.json", "tx_valid.json", "tx_invalid.json"] {
        assert!(
            !manifest.join("tests/fixtures").join(name).exists(),
            "do not check in tests/fixtures/{name}"
        );
    }
    let mut cur: &Path = &manifest;
    let data = loop {
        let cand = cur.join("third_party/bitcoin/src/test/data");
        if cand.join("tx_valid.json").is_file() {
            break cand;
        }
        cur = cur.parent().expect(
            "missing third_party/bitcoin/src/test/data; \
             run ./scripts/core-functional/init-submodule.sh",
        );
    };
    assert!(data.join("tx_invalid.json").is_file());
    assert!(data.join("script_tests.json").is_file());
}
