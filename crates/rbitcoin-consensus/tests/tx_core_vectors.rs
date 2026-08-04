//! Thin binary: Core `tx_valid` / `tx_invalid` live as unit tests in
//! `script::core_tx_vectors` so they can call `pub(crate)` verify paths.
//!
//! Run: `cargo test -p rbitcoin-consensus --lib core_tx_`

#[test]
fn core_tx_vectors_live_in_lib() {
    // Structural: unit tests are compiled into the lib test harness.
    // Full runners: core_tx_valid_all_rows / core_tx_invalid_all_rows.
    assert!(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/script/core_tx_vectors.rs")
            .is_file()
    );
    assert!(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/tx_valid.json")
            .is_file()
    );
    assert!(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/tx_invalid.json")
            .is_file()
    );
}
