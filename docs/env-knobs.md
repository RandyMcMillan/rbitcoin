# `RBITCOIN_*` inventory and policy (Q-16)

Operator configuration is **CLI / conf first**. Process env is only for
bootstrap, a single IO field hatch, and an **unstable** debug set listed
below. Do not grow env surface without a damn-good reason.

## Survivors (production)

| Env | Why it stays |
|-----|----------------|
| **`RBITCOIN_LOG`** / **`RUST_LOG`** | Bootstrap logging before conf parse; CLI `--log-level` wins when set |
| **`RBITCOIN_IO`** | Field escape hatch: force `pread` when io_uring is broken (`mmap` demotes to pread). **Single** bulk switch for all paths |

`RBITCOIN_P2P_MAX_INBOUND` is an **input** when CLI/conf omit `--maxinbound`
(`NodeConfig::absorb_inbound_env`). The node does not `set_var` it.

## Unstable (honored, not advertised)

Rare operator/debug reads. Prefer changing defaults in code. Not required
for signet/mainnet sync. **Not** CLI.

| Env | Default | Role |
|-----|---------|------|
| `RBITCOIN_BLOCK_QUEUE_GB` | unlimited | Absolute in-RAM body-queue ceiling (GiB) |
| `RBITCOIN_BLOCK_QUEUE_BYTES` | unlimited | Same ceiling in bytes (wins over GB) |
| `RBITCOIN_BULK_IO_WORKERS` | backend default | pread worker count when `RBITCOIN_IO=pread` |
| `RBITCOIN_CLASS_C_INRAM_MAX_MB` | 256 | L2 cap for `confirmed` / `header_txs_*`; over → fd L0. `strong_tx` always L2 |
| `RBITCOIN_TX_HEAD_BITS` | scale default | `tx.head` bits (dangerous on a live datadir) |
| `RBITCOIN_TX_IDX_SOFT_SPAN` | 16 GiB | Per-stem idx soft rollover (do not set above 32 GiB hard span) |
| `RBITCOIN_HEAD_SLOTS_HEADER` | scale default | Header hash-head initial slots |
| `RBITCOIN_HEAD_SLOTS_SCRIPTHASH` | scale default | SH hash-head initial slots |
| `RBITCOIN_SH_UNIQUE_HINT` | off | SH unique-hint probe |
| `RBITCOIN_SH_FORCE_REBUILD` | off | Sticky SH rebuild (also in OPERATOR) |
| `RBITCOIN_SH_RECOLLECT_WORKERS` | default | SH recollect parallelism |
| `RBITCOIN_SH_MAX_DIRECT_MERGE` | default | SH direct-merge cap |
| `RBITCOIN_SH_TARGET_RUN_BYTES` | default | SH run target size |
| `RBITCOIN_SH_MERGE_FANIN` | default | SH merge fan-in |
| `RBITCOIN_SH_MEMTABLE_CAP` | default | SH memtable cap |
| `RBITCOIN_SH_MERGE_WORKERS` | default | SH merge workers |
| `RBITCOIN_P2P_MAX_INBOUND` | 125 | Only if `--maxinbound` / conf omitted |

## Hardcoded (no env)

| Former env | Production default |
|------------|--------------------|
| Confirm `scriptq` / `writeq` | 4 / 20 (`ready=` is not a cap) |
| `RBITCOIN_CONFIRM_BATCH_INPUTS` | 8000 soft inputs/pack |
| Per-path IO (`PIN_IO`, `HEAD_RESOLVE_IO`, `SPEND_META`, `SPEND_ANN`, `CLASS_C_IO`) | Follow **`RBITCOIN_IO` only** (strings deleted) |
| `RBITCOIN_FD_APPEND` | Never read (deleted) |
| `RBITCOIN_BLOCK_QUEUE_MB` | Never read (deleted; use `_BYTES` / `_GB`) |

## Test-only (not operator)

| Env | Use |
|-----|-----|
| `RBITCOIN_HEAD_SCALE` | Tiny heads under `cargo test` (honored if exported — do not set on operators) |
| `RBITCOIN_TEST_*` | Node/store test fixtures (`TEST_DROP_STORE`, `TEST_NO_SUCH_CAP`) |
| `RBITCOIN_DIAG_DATADIR` | Offline diagnostic tests |
| `RBITCOIN_CAND_FK_FIXTURE` | Store fixture |
| `RBITCOIN_CORE_DATA` | Directory of Core JSON corpora for consensus tests |

## Deleted / do not reintroduce

| Env | Note |
|-----|------|
| `RBITCOIN_RESIDENCY_BYTES` / create pin FIFO | Feature removed |
| Per-path bulk IO matrix | Collapsed to `RBITCOIN_IO` |
| Confirm queue env overrides | Hardcoded depths |
| `RBITCOIN_IO_URING` | Deleted; use `RBITCOIN_IO=pread` |
| `RBITCOIN_TX_HEAD_ACCESS` | Deleted; tables are always fd pread/pwrite |
| `RBITCOIN_HEAD_SLOTS_TX` | Deleted; `tx.head` is segmented address head |

## Related

- [`OPERATOR.md`](../OPERATOR.md) — CLI / conf
- [`docs/io-modality.md`](./io-modality.md) — bulk IO behavior
