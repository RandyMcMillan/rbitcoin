# Consensus test fixtures

| File | Origin | License |
|------|--------|---------|
| `script_tests.json` | Bitcoin Core `src/test/data/script_tests.json` | MIT |
| `tx_valid.json` | Bitcoin Core `src/test/data/tx_valid.json` | MIT |
| `tx_invalid.json` | Bitcoin Core `src/test/data/tx_invalid.json` | MIT |
| `*.bin` / `*.hex` / `*.txt` | Captured mainnet/signet blocks for regression | project |
| `bip352_send_and_receive_test_vectors.json` | BIP-352 official send/receive vectors | BSD-2-Clause (BIP) |

Core JSON files are **in-tree copies** so `cargo test` works without a
submodule. Source of truth is Bitcoin Core **v31.1**
(`9be056a8a72b624dae9623b2f7bded92c2a21c91`) at
`third_party/bitcoin/src/test/data/` after:

```bash
./scripts/core-functional/init-submodule.sh
./scripts/core-functional/sync-core-fixtures.sh --check   # must be silent-ok
# after a pin bump:
./scripts/core-functional/sync-core-fixtures.sh --write
```

Do **not** curl from `master`. Extra local rows do not belong in these JSON
files — add a rust unit instead.

## Harness

| Corpus | Test (lib) |
|--------|------------|
| `script_tests.json` | `cargo test -p rbitcoin-consensus --lib core_script_tests_all_rows -- --nocapture` |
| `tx_valid.json` | `cargo test -p rbitcoin-consensus --lib core_tx_valid_all_rows -- --nocapture` |
| `tx_invalid.json` | `cargo test -p rbitcoin-consensus --lib core_tx_invalid_all_rows -- --nocapture` |

Success requires **fail == 0** after an explicit allowlist (see `docs/consensus-tests.md`). Soft majority pass rates are not used.
