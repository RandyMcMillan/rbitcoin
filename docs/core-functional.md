# Bitcoin Core functional tests (rbitcoin)

Pin: **Bitcoin Core v31.1** (`9be056a8a72b624dae9623b2f7bded92c2a21c91`).

`scripts/core-functional/` holds the inventory, checkers, and (later) the
bitcoind shim. Core’s Python tests and `src/test/data` live in the
**`third_party/bitcoin` submodule** (v31.1). We do **not** copy the 267
`*.py` files into this repo.

Default `cargo test` never runs those Python tests. Consensus JSON corpora
are staged from the submodule each run (the helper runs `init-submodule.sh`
if the pin is missing; see
`crates/rbitcoin-consensus/tests/fixtures/README.md`).

```bash
./scripts/core-functional/init-submodule.sh
./scripts/core-functional/sync-core-fixtures.sh --check
python3 scripts/core-functional/check_inventory.py \
  --tests-dir third_party/bitcoin/test/functional
```

## Check the inventory

```bash
python3 scripts/core-functional/check_inventory.py
./scripts/core-functional/check_inventory_test.sh
```

`--tests-dir PATH` compares against a Core checkout’s `test/functional`.
Without it, the checker uses `scripts/core-functional/v31.1-tests.txt`
(the v31.1 filename list).

## Inventory schema

`scripts/core-functional/inventory.toml`:

| Field | Rule |
|-------|------|
| `name` | `*.py` basename, unique |
| `status` | `run` or `skip` |
| `reason` | required on `skip`; **forbidden** on `run`; never `unknown` |
| `analog` | required when `reason` is `no-prune`, `core-internal`, or `no-utxo-set` (`none` if we will not re-home) |
| `log_map` | optional; later `debuglog_map.toml` keys |

A file on disk (or in `v31.1-tests.txt`) that is missing from the inventory,
or an inventory row with no file, **fails the checker**.

`run` means an **unmodified** Core script is green against rbitcoin. First
pass is almost all `skip`. Flip to `run` only in the PR that makes that
script pass.

## Skip reasons

| Code | Meaning |
|------|---------|
| `no-wallet` | wallet RPC / `wallet/` URL |
| `no-mining-product` | GBT / `prioritisetransaction` as Core mining |
| `no-prune` | prune / blk xor / `-blocksdir` |
| `no-utxo-set` | coins DB / assumeutxo / scantxoutset |
| `no-zmq` / `no-ipc` / `no-qt` | those interfaces |
| `no-core-rest` | Core REST (`interface_rest.py`); we have Esplora instead |
| `no-tool` | bitcoin-wallet / bitcoin-tx / bitcoin-util / bitcoin-chainstate |
| `v1-only` | requires v1 or v2→v1 downgrade |
| `core-log` | `assert_debug_log` string not yet in the debug.log map |
| `core-internal` | LevelDB / LoadExternalBlockFile / USDT / rw_settings |
| `core-net-policy` | banlist format, tor, anchors.dat, asmap |
| `policy-libre` | assertion *is* Core standardness |
| `rpc-missing` | method/harness not implemented yet (shrinks) |
| `core-cpp-unit` | Boost units — never in this runner |
| `prev-release` | previous-release binaries |
| `harness` | `test_runner.py`, `combine_logs.py`, framework self-tests |
| `unknown` | **illegal** |

## CI (later)

Nightly + `workflow_dispatch`, and PRs labeled **`core-functional`**.
Unlabeled PRs keep the cargo gates only.

## Analog column

LevelDB / `blocks/blk*.dat` tests cannot pass unmodified. `analog` names
the rbitcoin scenario (or `none`) so we do not drop the behavior. See the
design plan for the first-pass mapping (`--datadir-cold`, reconstruct,
`--milestone`, durable mempool).
