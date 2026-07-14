# Testing guide

## Preference order

1. **Multi-process / full node** — binary + datadir + RPC/CLI/P2P
2. **Subsystem integration** — store+query, wallet+chain notifications, net pipeline
3. **Fault injection** through public APIs / `integration-testing` features
4. **Unit tests** — last resort only

Shared helpers live in the `rbitcoin-test` crate.

## Running tests

```bash
nix-shell
cargo test --workspace
./scripts/coverage.sh
```

## Scenario catalog (initial)

| ID | Layer | Description |
|----|-------|-------------|
| `node_lifecycle_default` | Config / lifecycle | Default config, start, clean shutdown |
| `node_lifecycle_custom_datadir` | Config / lifecycle | Custom datadir path |
| `node_lifecycle_invalid_datadir` | Config / lifecycle | Unwritable/missing parent fails clearly |
| `node_config_network` | Config | Mainnet/testnet/signet/regtest selection |
| `store_open_create` | Store | Create store in empty dir, reopen |
| `store_put_get_header` | Store/query | Put header row, read back after reopen |
| `store_put_tx_outputs_point` | Store/query | Put tx/outs and point spend link |
| `store_allocate_publish_visibility` | Store | Readers only see published records |
| `cli_help_version` | CLI | `--help` / `--version` exit paths |

New features must add rows here (or in a linked scenario index) when they land.

## Core differential

Not yet wired. Future: pin bitcoind version, run shared RPC scripts on regtest against both nodes.

## Fault injectors

Optional `integration-testing` cargo feature on crates that need crash points (e.g. mid-finalize). Off by default in release builds used for production packaging; **on** in CI test builds when needed for coverage.
