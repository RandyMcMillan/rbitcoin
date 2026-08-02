# Store IO modality matrix

**Source of truth** for what `RBITCOIN_IO=uring` does vs what is still
`mmap`’d, host bench protocol, and the phased plan to remove `memmap2`.

Related: [`OPERATOR.md`](../OPERATOR.md) (env knobs), [`concurrency.md`](./concurrency.md),
[`ibd-io-audit.md`](./ibd-io-audit.md), [`architecture.md`](./architecture.md).

---

## Two independent layers

| Layer | Controlled by | Values | Purpose |
|-------|---------------|--------|---------|
| **Bulk batch** | `RBITCOIN_IO` (+ path overrides) | `uring` \| `pread` (annotate: `pwrite`) | Multi-op waves on **file descriptors** (body denserels, head-resolve body prefix, spend meta/ann, Class C bulk) |
| **Table transport** | [`TableAccess`](../crates/rbitcoin-store/src/file.rs) on each `TableFile` | `FdOnly` \| `MapFull` | Whether payload is pread/pwrite vs full-file `MmapMut` map epochs |

**`RBITCOIN_IO=uring` does not unmap heads or idx.** It only selects the bulk
batch backend. Legacy token `RBITCOIN_IO=mmap` demotes to **pread** with a
one-time warning (not a live bulk mode).

---

## Current matrix (`RBITCOIN_IO=uring`)

### Bulk batch (env)

| Path | Env | Syscalls |
|------|-----|----------|
| Pin denserels / body pipeline | `RBITCOIN_PIN_IO` → global | uring/pread on **`tx.body` FD** |
| Head-resolve body prefix ≤32 B | `RBITCOIN_HEAD_RESOLVE_IO` | uring/pread on body FD |
| Spend-meta 9 B peeks | `RBITCOIN_SPEND_META` | uring/pread on body FD |
| Spend pure-write annotate | `RBITCOIN_SPEND_ANN` | uring/pwrite or pwrite on body FD |
| Class C create-height bulk | `RBITCOIN_CLASS_C_IO` | uring/pread |
| Class A body/idx **linear append** | always | **pwrite** |

Default: uring if the ring opens, else pread/pwrite. Ring depth **128**.

### Table transport (`TableAccess`)

| Object | Access today | Notes |
|--------|--------------|--------|
| **`tx.body`** | **FdOnly** | Tiny header-only map; payload pread/pwrite/uring; grow without multi‑GiB remap (`3a0c220`) |
| **`tx.idx` segments** | **FdOnly** | Append pwrite; **reads pread**; grow fallocate only (phase 1) |
| **`tx.head` segments** | **FdOnly** (default) | Page-coalesced insert (4 KiB probe page RMW). Opt-out: `RBITCOIN_TX_HEAD_ACCESS=map` |
| Header hash head | **FdOnly** | Sharded `HashHead`; 128-slot (~3 KiB) chunk cache insert + probe |
| Hash multi-list (`.mlt`) | **FdOnly** | Linear append (`ArrayLink` kind forced FdOnly at site) |
| **`scripthash.head` / body** | **FdOnly** | 4 KiB chunk cache (128×32 B) insert + probe; body pread/pwrite slabs |
| **Spenders** | **FdOnly** | Linear append multi-spender list |
| Class C arrays | **FdOnly** | header_txs, Confirmed, heights, … |
| Mempool (`{datadir}/mempool/*`) | **InRam + file sidecar** | Private durability; **not** Class A `store/tx.body` (phase 5b M2) |

### Hybrid paths (easy to misread)

| Path | Table part | Fd/uring bulk part |
|------|------------|---------------------|
| Head resolve stream | FdOnly **page-batched** head probe + FdOnly idx | uring/pread body prefix |
| Pin denserels | FdOnly idx ranges | uring/pread body bytes |

---

## Historical record: head insert uring ~5× slower

| Commit | Note |
|--------|------|
| `0ee28c0` / `77cb2ab` | io_uring bulk / page-grouped RMW for `tx.head` insert |
| **`259b766`** (2026-07-23) | **Reverted to mmap-only head insert.** Host A/B: **io_uring head inserts ~5× slower on head ms/blk** than mmap Release. Bulk uring kept for **reads** only |
| `788936e` | Page-coalesce insert still via **plain map** `write_at` |
| `bulk_io::page_rmw_pipelined` | Still in tree, **`#[allow(dead_code)]`** — not production head path |
| `3a0c220` | **Body** FdOnly success (different pattern: linear append + bulk batch read) |
| `f829090` / `11134cb` | Segmented idx/head landed **after** the 5× failure |

**Implication:** do not ship head demap without **operator host** A/B (musl
static). Prefer page-coalesced **pread→mutate→pwrite** over per-slot uring.
Segmented heads reduce grow/remap pain but do not free us from page locality.

---

## End goal (phased)

1. **FdOnly** for multi‑GiB random tables: `tx.idx` → `tx.head` / header head → SH head/body / spenders.
2. **InRam** (explicit process buffers) for small Class C / mempool — not leftover MapFull “because small.”
3. **Remove `memmap2`** from the workspace.
4. Update this doc after **each** phase with host A/B results.

Agent correctness tests under `/tmp` are required; **perf ship/fail is host-only**.

---

## Host benchmarks (operator; musl static)

### Rules

- Run on a **real host** with a **local filesystem** datadir (not agent 9p workspace).
- Use the **portable static musl** binary — same as release (`docs/reproducible-builds.md`).
- **Do not** use `nix-shell --run 'cargo build -p rbitcoin-node --release'` for IBD
  benches (Nix-store glibc dynamic link).

### Build musl `rbitcoin-node`

```bash
# Repo root; Nix flakes enabled
nix build .#rbitcoin-musl --out-link result
mkdir -p target/release
install -m 755 result/bin/rbitcoin-node result/bin/rbitcoin-cli target/release/
file target/release/rbitcoin-node   # must say "statically linked"
# or: ./scripts/repro-build.sh
```

### IBD / tip-rate window (primary live gate)

```bash
export RBITCOIN_IO=uring   # or pread for second arm
./target/release/rbitcoin-node --datadir /path/to/local/datadir …flags…

# Capture steady-state minutes:
grep 'ibd: perf' host.log
grep 'ibd: perf_dbg' host.log    # head=, plan_mega, class_a head insert, pin
grep 'ibd: sizes' host.log       # rss= anon= file=
```

**A/B:** same host, same network/milestone/height band when possible; baseline
SHA vs candidate SHA; compare tip rate, head ms/blk, RssFile. **Fail ship** on
5×-class head regression or agreed **>+20%** head ms/blk without tip-rate win.

### Store microbench (phase 2 head insert A/B)

Binary: **`rbitcoin-store-bench`** (`crates/rbitcoin-store`).

```bash
# Dev / agent (glibc nix-shell) — correctness + rough order of magnitude only:
cargo build -p rbitcoin-store --release --bin rbitcoin-store-bench
./target/release/rbitcoin-store-bench --n 200000 --bits 18 --access both --dir /var/tmp/head-ab

# Operator preferred: build via musl package (ships when -p rbitcoin-store is built):
nix build .#rbitcoin-musl --out-link result
# After install from result, or:
find result -name 'rbitcoin-store-bench' 2>/dev/null
# Run on local NVMe, not 9p:
./target/release/rbitcoin-store-bench --n 500000 --bits 20 --access both --dir /var/tmp/head-ab
```

| Flag | Meaning |
|------|---------|
| `--access map` | [`TableAccess::MapFull`](../crates/rbitcoin-store/src/file.rs) only |
| `--access fd` | [`TableAccess::FdOnly`](../crates/rbitcoin-store/src/file.rs) only |
| `--access both` | Sequential A/B (default) |

Live node rollback: set **`RBITCOIN_TX_HEAD_ACCESS=map`** before open/create
(default **`fd`** / unset). Prefer `--access both` on the store-bench for A/B.

---

## Phase results (append here)

### Phase 0 — Truth layer (landed with `TableAccess`)

- [`TableAccess::FdOnly` \| `MapFull`](../crates/rbitcoin-store/src/file.rs);
  originally only leading `TableKind::Tx` was FdOnly by default.
- Bulk comments/OPERATOR no longer claim bulk `mmap` as a live mode.
- No host A/B required (behavior unchanged).

### Phase 1 — `tx.idx` FdOnly (landed)

- Segment create/open via `TableFile::create_with_access` / `open_with_access`
  with **`TableAccess::FdOnly`** (kind remains `ArrayLink` for on-disk identity).
- Reads: `read_at` → pread; appends: pwrite; grow: fallocate only.
- Correctness: store unit tests (multi-segment soft-span, reopen, range batch).
- Host A/B: **optional** for tip-rate; not a hard gate (lower risk than heads).

### Phase 2 — Head FdOnly path + bench harness (landed)

- Trailing-header [`TableAccess::FdOnly`](../crates/rbitcoin-store/src/file.rs):
  tiny map window, pread/pwrite payload, pwrite trailer/HWM, fallocate grow.
- Env **`RBITCOIN_TX_HEAD_ACCESS`** (initially default **map**; phase 3 flips).
- Binary **`rbitcoin-store-bench`**.

### Phase 2b — Page coalesce + resolve state machine (landed)

| Path | Behavior |
|------|----------|
| **Head insert** | One **page read** + multi in-buffer hop; **one page write-back** if dirty (not per-slot pwrite). 4 KiB-aligned on trailing heads. |
| **Idx reads** | **OS-page-aligned** preads (4096 B). Contiguous runs expand to page spans. Sparse `record_range_batch` → **one uring/pread SQE per distinct OS page**. Same-page interior range → single page pread. |
| **Head resolve (uring)** | Per key: CPU **probe** → **STAGE_IDX** (idx OS-page pread) → **STAGE_BODY** (≤32 B body). Many keys mixed in flight — **not** “all idx then all body”. |

- **Agent-side sample** after page write-back (bits=16 n=50k /tmp; not a ship gate):

  | access | insert_ns/key | probe_ns/key |
  |--------|---------------|--------------|
  | MapFull | ~111 | ~489 |
  | FdOnly | ~100 | ~500 |

  Operator: numbers look good on host → phase 3 cutover.

### Phase 3 — `tx.head` default FdOnly + header head (landed)

- **`RBITCOIN_TX_HEAD_ACCESS`** default **FdOnly**; opt-out `map` / `mmap`.
- [`TableAccess::for_kind`](../crates/rbitcoin-store/src/file.rs): `HashHead` (leading + trailing) → FdOnly.
- Header `HashHead` + multi-list (`.mlt`) FdOnly; insert/probe via 128-slot chunk cache.
- Segmented `tx.head` create/open follows env (default FdOnly).

### Phase 4 — scripthash + spenders FdOnly (landed)

- `ScriptHash` body + `ScriptHashHead` shards: default FdOnly via `for_kind`.
- SH probe/get/clear use the same 4 KiB chunk cache as insert (not per-slot pread).
- `Spender` multi-list body: FdOnly via `for_kind`.
- Class C / mempool remain MapFull (phase 5 InRam).

### Phase 5a — Class C FdOnly + resolve page-batch (landed)

- [`TableAccess::for_kind`](../crates/rbitcoin-store/src/file.rs): Class C
  (`ArrayLink`, Confirmed, Header, TxHeight, StrongTx, …) default **FdOnly**.
- Head resolve: **`probe_candidates_batch` / `probe_fks_batch`** — one page
  pread per distinct probe page across the whole key wave (uring + pread paths).
- Logging: `access=` on address-head **open** + segment open; node start logs
  `version=` + `tx_head_access=`; `perf_dbg` adds `probe_us/key=` `idx_us/key=`
  `body_us/key=` and `ca_head_us/blk=` `ca_body_us/blk=`.

### Phase 5b — Mempool InRam + sidecar (landed)

- `rbitcoin-mempool` uses process buffers + normal file IO under
  `{datadir}/mempool/` (`meta` / `slots` / `tx.body`).
- **Not** Class A: confirmed archive remains `{datadir}/store/tx.body` with
  confirm as sole writer.
- Tip script skip for live mempool txs unchanged (`script_preverified_txids`).
- No `memmap2` in the mempool crate.

### Phase 6 — (pending)

Remove residual `memmap2` from store `TableFile` (tiny header maps / MapFull
rollback path); workspace greps empty.
