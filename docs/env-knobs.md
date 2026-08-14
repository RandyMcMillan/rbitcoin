# `RBITCOIN_*` inventory and policy (Q-16)

Operator configuration is **CLI / conf first**. Process env is only for
bootstrap, a single IO field hatch, and an **unstable** debug set listed
below. Do not grow env surface without a damn-good reason.

## Survivors (production)

| Env | Why it stays |
|-----|----------------|
| **`RBITCOIN_LOG`** / **`RUST_LOG`** | Bootstrap logging before conf parse; CLI `--log-level` wins when set |
| **`RBITCOIN_IO`** | Field escape hatch: force `pread` when io_uring is broken (`mmap` demotes to pread). **Single** bulk switch for all paths |

CLI still sets process env for library readers where needed today:

| CLI / conf | Env bridge (transitional) |
|------------|---------------------------|
| `--maxinbound` / `--maxconnections` | `RBITCOIN_P2P_MAX_INBOUND` via `NodeConfig::apply_operator_env` |

Long term: pass config structs; drop `set_var` bridges.

## Unstable (honored, not advertised)

Rare operator/debug reads. Prefer changing defaults in code. Not required
for signet/mainnet sync. **Not** CLI.

| Env | Default | Role |
|-----|---------|------|
| `RBITCOIN_BLOCK_QUEUE_GB` | unlimited | Absolute in-RAM body-queue ceiling (GiB) |
| `RBITCOIN_BLOCK_QUEUE_BYTES` | unlimited | Same ceiling in bytes (wins over GB) |
| `RBITCOIN_BULK_IO_WORKERS` | backend default | pread worker count when `RBITCOIN_IO=pread` |
| `RBITCOIN_CLASS_C_INRAM_MAX_MB` | 256 | L2 Class C image cap; over → fd L0 |
| `RBITCOIN_TX_HEAD_BITS` | scale default | `tx.head` bits (dangerous on a live datadir) |
| `RBITCOIN_TX_IDX_SOFT_SPAN` | 16 GiB | Idx segment soft rollover (bytes) |
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

## Hardcoded (no env)

| Former env | Production default |
|------------|--------------------|
| Confirm `loadq` / `scriptq` / `writeq` | 8 / 4 / 20 |
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
