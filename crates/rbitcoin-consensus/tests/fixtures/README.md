# Consensus test fixtures

| File | Origin | License |
|------|--------|---------|
| `script_tests.json` | Bitcoin Core `src/test/data/script_tests.json` | MIT |
| `tx_valid.json` | Bitcoin Core `src/test/data/tx_valid.json` | MIT |
| `tx_invalid.json` | Bitcoin Core `src/test/data/tx_invalid.json` | MIT |
| `*.bin` | Captured mainnet/signet blocks for regression | project |

Core JSON files are vendored for offline CI. Update by re-fetching from
`https://github.com/bitcoin/bitcoin` `master` (or a pinned tag) when expanding coverage.
