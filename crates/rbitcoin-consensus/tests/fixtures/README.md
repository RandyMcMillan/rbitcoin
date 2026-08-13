# Consensus test fixtures

| File | Origin | License |
|------|--------|---------|
| `script_tests.json` | Bitcoin Core `src/test/data/script_tests.json` | MIT |
| `tx_valid.json` | Bitcoin Core `src/test/data/tx_valid.json` | MIT |
| `tx_invalid.json` | Bitcoin Core `src/test/data/tx_invalid.json` | MIT |
| `*.bin` / `*.hex` / `*.txt` | Captured mainnet/signet blocks for regression | project |
| `bip352_send_and_receive_test_vectors.json` | BIP-352 official send/receive vectors | BSD-2-Clause (BIP) |

Core JSON files are vendored for offline CI. Update by re-fetching from
`https://github.com/bitcoin/bitcoin` `master` (or a pinned tag) when expanding coverage:

```bash
curl -sL https://raw.githubusercontent.com/bitcoin/bitcoin/master/src/test/data/script_tests.json \
  -o crates/rbitcoin-consensus/tests/fixtures/script_tests.json
curl -sL https://raw.githubusercontent.com/bitcoin/bitcoin/master/src/test/data/tx_valid.json \
  -o crates/rbitcoin-consensus/tests/fixtures/tx_valid.json
curl -sL https://raw.githubusercontent.com/bitcoin/bitcoin/master/src/test/data/tx_invalid.json \
  -o crates/rbitcoin-consensus/tests/fixtures/tx_invalid.json
```

## Harness

| Corpus | Test (lib) |
|--------|------------|
| `script_tests.json` | `cargo test -p rbitcoin-consensus --lib core_script_tests_all_rows -- --nocapture` |
| `tx_valid.json` | `cargo test -p rbitcoin-consensus --lib core_tx_valid_all_rows -- --nocapture` |
| `tx_invalid.json` | `cargo test -p rbitcoin-consensus --lib core_tx_invalid_all_rows -- --nocapture` |

Success requires **fail == 0** after an explicit allowlist (see `docs/consensus-tests.md`). Soft majority pass rates are not used.
