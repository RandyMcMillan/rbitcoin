# Consensus test fixtures

| File | Origin | License |
|------|--------|---------|
| `*.bin` / `*.hex` / `*.txt` | Captured mainnet/signet blocks for regression | project |
| `bip352_send_and_receive_test_vectors.json` | BIP-352 official send/receive vectors | BSD-2-Clause (BIP) |

Core JSON corpora (`script_tests.json`, `tx_valid.json`, `tx_invalid.json`)
are **not** checked in here. Each `cargo test` run hard-links or copies them
from Bitcoin Core **v31.1**
(`9be056a8a72b624dae9623b2f7bded92c2a21c91`) at
`third_party/bitcoin/src/test/data/` into `$CARGO_TARGET_DIR/core-data/`.

```bash
./scripts/core-functional/init-submodule.sh   # also invoked by cargo test / coverage.sh if missing
./scripts/core-functional/sync-core-fixtures.sh --check   # submodule present; no copies here
```

Do **not** curl from `master` and do **not** add rows to those JSON files —
add a rust unit instead.

## Harness

| Corpus | Test (lib) |
|--------|------------|
| `script_tests.json` | `cargo test -p rbitcoin-consensus --lib core_script_tests_all_rows -- --nocapture` |
| `tx_valid.json` | `cargo test -p rbitcoin-consensus --lib core_tx_valid_all_rows -- --nocapture` |
| `tx_invalid.json` | `cargo test -p rbitcoin-consensus --lib core_tx_invalid_all_rows -- --nocapture` |

Success requires **fail == 0** after an explicit allowlist (see `docs/consensus-tests.md`). Soft majority pass rates are not used.
