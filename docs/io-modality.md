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
| **`tx.head` segments** | **MapFull** | Page-coalesced insert_many via map RMW (`788936e`) |
| Header hash head | **MapFull** | Sharded `HashHead` |
| **`scripthash.head` / body** | **MapFull** | |
| Spenders, Class C arrays | **MapFull** | Smaller, still mapped |
| Mempool store | **MapFull** | Separate crate |

### Hybrid paths (easy to misread)

| Path | Map part | Fd/uring part |
|------|----------|----------------|
| Head resolve stream | table-map head probe + FdOnly idx | uring/pread body prefix |
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

### Store microbench (phase 2+ head insert)

Musl package currently ships node+cli only. Phase 2 adds a packaged bench or
documented ignored test; operator runs **both modes** on the same disk. Agent
only ensures harness compiles and unit tests pass.

---

## Phase results (append here)

### Phase 0 — Truth layer (landed with `TableAccess`)

- [`TableAccess::FdOnly` \| `MapFull`](../crates/rbitcoin-store/src/file.rs); only
  leading `TableKind::Tx` (`tx.body`) is FdOnly by default.
- Bulk comments/OPERATOR no longer claim bulk `mmap` as a live mode.
- No host A/B required (behavior unchanged).

### Phase 1 — `tx.idx` FdOnly (landed)

- Segment create/open via `TableFile::create_with_access` / `open_with_access`
  with **`TableAccess::FdOnly`** (kind remains `ArrayLink` for on-disk identity;
  HashHead multi-list still MapFull).
- Reads: `read_at` → pread; appends: pwrite; grow: fallocate only.
- Correctness: store unit tests (multi-segment soft-span, reopen, range batch).
- Host A/B: **optional** for tip-rate; not a hard gate (lower risk than heads).

### Phase 2+ — (pending)

_Host A/B tables for head demap go here after operator runs._
