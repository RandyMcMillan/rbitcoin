# `RBITCOIN_*` inventory and policy (Q-04)

Operator configuration is **CLI / conf first**. Most historical env knobs are
**hardcoded**, **test-only**, or **removed**. Do not grow process-env surface
without a damn-good reason.

## Survivors (production)

| Env | Why it stays |
|-----|----------------|
| **`RBITCOIN_LOG`** / **`RUST_LOG`** | Bootstrap logging before conf parse; CLI `--log-level` wins when set |
| **`RBITCOIN_IO`** | Field escape hatch: force `pread` when io_uring is broken (`mmap` demotes to pread). **Single** bulk switch for all paths |

Deprecated alias: `RBITCOIN_IO_URING=0` ≈ `RBITCOIN_IO=pread` (one-time info log).

CLI still sets process env for library readers where needed today:

| CLI / conf | Env bridge (transitional) |
|------------|---------------------------|
| `--maxinbound` / `--maxconnections` | `RBITCOIN_P2P_MAX_INBOUND` via `NodeConfig::apply_operator_env` |
| `--archive-queue-mb` | `RBITCOIN_ARCHIVE_QUEUE_MB` when flag set |

Long term: pass config structs; drop `set_var` bridges.

## Hardcoded (no env)

| Former env | Production default |
|------------|--------------------|
| Confirm `loadq` / `scriptq` / `writeq` | 8 / 4 / 20 |
| `RBITCOIN_CONFIRM_BATCH_INPUTS` | 8000 soft inputs/pack |
| Per-path IO (`PIN_IO`, `HEAD_RESOLVE_IO`, `SPEND_META`, `SPEND_ANN`, `CLASS_C_IO`) | Follow **`RBITCOIN_IO` only** |
| `RBITCOIN_TX_HEAD_ACCESS=map` | Ignored (FdOnly); warn once |

Other store/SH knobs (`BLOCK_QUEUE_*`, `BULK_IO_WORKERS`, `CLASS_C_INRAM_*`,
`SH_*`, `TX_HEAD_BITS`, `TX_IDX_SOFT_SPAN`, `HEAD_SLOTS_*`, `FD_APPEND`) remain
readable for rare operator/debug use today but are **not** advertised in OPERATOR
as required knobs. Prefer changing defaults in code; treat as unstable.

## Test-only (not operator)

| Env | Use |
|-----|-----|
| `RBITCOIN_HEAD_SCALE` | Tiny heads under `cargo test` |
| `RBITCOIN_TEST_*` | Node/store test fixtures |
| `RBITCOIN_DIAG_DATADIR` | Offline diagnostic tests |
| `RBITCOIN_CAND_FK_FIXTURE` | Store fixture |

## Deleted / do not reintroduce

| Env | Note |
|-----|------|
| `RBITCOIN_RESIDENCY_BYTES` / create pin FIFO | Feature removed |
| Per-path bulk IO matrix | Collapsed to `RBITCOIN_IO` |
| Confirm queue env overrides | Hardcoded depths |

## Related

- [`OPERATOR.md`](../OPERATOR.md) — CLI / conf
- [`docs/io-modality.md`](./io-modality.md) — bulk IO behavior
