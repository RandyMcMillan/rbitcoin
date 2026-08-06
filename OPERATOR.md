# Operator guide — full participant node

## Status

BIP324 v2-only P2P, cluster mempool (Libre admission + **consensus script checks on accept**),
Electrum confirmed + unconfirmed (TLS via reverse proxy). **Mainnet is experimental** — see
[`docs/experimental-mainnet.md`](./docs/experimental-mainnet.md). Soak reorgs and disk headroom
before any serious use. Default mainnet **`--milestone 840000` skips script/sig checks** at/below
that height; use `--milestone 0` for full scripts.

Architecture: **archive-before-confirm** — block bodies land in Class A as peers
deliver them; tip **confirm** (Class C) walks contiguous archived runs. Download
defaults to **1024** concurrent getdata (not a tip-distance cap), max **16** blocks
in transit per peer.

## Build

Portable **static musl** binary (runs on ordinary Linux without Nix):

```bash
nix build .#rbitcoin-musl
mkdir -p target/release
install -m 755 result/bin/rbitcoin-node result/bin/rbitcoin-cli target/release/
./target/release/rbitcoin-node --help
```

Do not use `cargo build --release` under `nix-shell` for the operator binary —
that produces a Nix-glibc dynamic link that fails outside the store. Dev/test
builds stay on `nix develop` / `cargo test`; release is always musl static.
See [`docs/reproducible-builds.md`](./docs/reproducible-builds.md).

## CLI (operator-first)

Routine knobs are **CLI / conf**, not required env vars. Clean smoke:

```bash
./target/release/rbitcoin-node --smoke --datadir /tmp/rb-smoke --network regtest
```

| Flag | Core-ish alias | Default |
|------|----------------|---------|
| `--datadir PATH` | same | `./datadir` |
| `--network NET` | `--chain` | `mainnet` |
| `--listen ADDR` | | bind later default port |
| `--connect ADDR` | (repeatable) | seeds |
| `--milestone HEIGHT` | `--assumevalid-height` | network default (mainnet 840000) |
| `--max-outbound N` | `--maxoutbound` | 16 live download peers |
| `--maxinbound N` | `--maxconnections` | 125 inbound sessions |
| `--mempool-size-mb N` | `--maxmempool` | ~300 MiB weight |
| `--archive-queue-mb N` | | 512 |
| `--class-a-cache-mb N` | | unset (store default) |
| `--conf FILE` | | none |
| `--log-level LEVEL` | | `info` |
| `--no-seeds` | `--noseeds` | seeds on |
| `--electrum-listen ADDR` | | disabled |
| `--inhibit-suspend` | | off |

Conf file: simple `key=value` lines (`#` comments). CLI overrides conf. Example:

```
network=signet
maxinbound=64
archive_queue_mb=256
mempool_size_mb=100
```

**Advanced** IO/perf tunables may still use `RBITCOIN_*` (see below); they are
**not required** for normal signet/mainnet sync or tip follow.

## Logging

Operational logs go to **stderr** with UTC timestamps:

```
2026-07-15T03:04:26.725Z INFO  rbitcoin-node starting network=mainnet …
```

| Control | Values |
|---------|--------|
| `--log-level LEVEL` | `error` `warn` `info` `debug` `trace` `off` |
| `RBITCOIN_LOG` / `RUST_LOG` | advanced fallback if CLI omits `--log-level` |

Default: **info**. CLI wins over env.

### Tip-follow (every block)

After IBD, each accepted tip extension logs one **info** line (Core-like):

```
UpdateTip: new best=<hash> height=<n> version=<v> tx=<n> date=<unix> progress=tip
```

Emitted from the tip-follow / wire accept path (`ChainHub::connect_at`). IBD bulk
confirm does **not** spam this line per block — use the periodic IBD status below.

### IBD status lines (every ~5s)

| Line | Level | Use |
|------|-------|-----|
| `ibd: progress` | INFO | Tip rate, `planq`/`prepq`/`writeq`, `txs=` (Class A / `tx.idx` count), horizon, tip ETA, **`bq soft=n/win RAM=`** (in-RAM body queue; soft densify: under ~100 MiB free ahead, over that only ~1 min confirm window at tip rate) |
| `ibd: perf` | INFO | Inflight + **`bq soft= RAM=`**; **load / script / write** walls; live confirm `h= n= in=` (blocks + inputs in current pack); pin/write detail |
| `ibd: sizes` | INFO | RSS + work path + **`bq soft=` / `RAM=`** + **conf_plans** + confirm pipe |
| `ibd: perf_dbg` | DEBUG | µs/blk load/write, pin/edge detail, **plan_mega head resolve** + **class_a commit**, contig park |

At **info**, progress + perf already expose load/write bottlenecks (schema 12). Enable **debug** for plan-mega / Class A commit subtimers and per-block µs. Ghost columns from deleted paths (wave-fill stubs, Direct SH head RMW) are omitted from both formatters.

**Create pins:** pipeline-local only (`batch_pin` / `BatchParents` / plan-local external parents). No process pin FIFO. Header plans via ConfirmParentCache.

**Archive `tx.head` split (perf_dbg):** `plan_mega … head_rd=` is parent
**read** resolve (`get_fk_by_txid_batch`, with `probe` / `idx` / `body` subtimers).
`class_a_commit … head=` is create **insert** (`head_insert_many`). Pipeline pins stay on the plan (`batch_pin`); no process denserels seed.

**Archive head resolve:** streaming — **FdOnly** page-coalesced head probe +
**FdOnly** `tx.idx` + body prefix via **io_uring or pread** (deepest-cand-first).
**Class A `tx.body` / `tx.idx` / `tx.head`, header head, SH head/body, and
spenders are FdOnly** ([`TableAccess::FdOnly`](docs/io-modality.md)). Full
modality matrix and demap plan: [`docs/io-modality.md`](docs/io-modality.md).

## Bulk store IO backends

**Bulk batch** only (`RBITCOIN_IO`). Table transport is always **fd pread/pwrite**
(phase 6 — no maps). Compact Class C is L2 write-behind; see `docs/io-modality.md`.

Hierarchy: **path env** (if set) → **global `RBITCOIN_IO`** → default (**uring** if
the ring is available, else **pread** / **pwrite** for annotate). If `uring` is
selected but setup fails, demote to **pread** / **pwrite**.

| Env | Values | Site |
|-----|--------|------|
| **`RBITCOIN_IO`** | `uring` \| `pread` | Global default (`mmap` token demotes to pread with a warning — not a live bulk mode) |
| `RBITCOIN_PIN_IO` | uring \| pread | Class A denserels / pin body pipeline |
| `RBITCOIN_HEAD_RESOLVE_IO` | uring \| pread | Head-resolve body prefix (≤32 B) |
| `RBITCOIN_SPEND_META` | uring \| pread | Structural 9 B spender-meta peeks |
| `RBITCOIN_SPEND_ANN` | uring \| **pwrite** | Pure-write annotate store |
| `RBITCOIN_CLASS_C_IO` | uring \| pread | Bulk create-height 4 B slots |
| `RBITCOIN_CLASS_C_INRAM_MAX_MB` | integer MiB (default **256**) | Cap for L2 Class C load; over → pure fd L0 |
| `RBITCOIN_TX_HEAD_ACCESS` | ignored if `map` | Always FdOnly after phase 6 (warn once if `map`) |

- **uring** — io_uring bulk pread/pwrite (ring depth **128**).
- **pread** / **pwrite** — libc positional IO; `RBITCOIN_BULK_IO_WORKERS` parallelizes pread only.
- Class A **`tx.body` / `tx.idx` linear appends always pwrite**.
- Perf log tokens: `ann={}ms/n={}` and `meta={}ms/n={}` (no `_mmap` / `_uring` suffix).
- Compat: `RBITCOIN_IO_URING=0` (deprecated) ≈ global `RBITCOIN_IO=pread`.

## Defaults and memory budgets

| Knob | Default | Override |
|------|---------|----------|
| IBD concurrent getdata | **1024** | code `IbdConfig::window` |
| Blocks in transit / peer | **16** | `IbdConfig::per_peer` |
| Live IBD peers | **16** | `--max-outbound` |
| Inbound P2P sessions | **125** | `--maxinbound` / `--maxconnections` |
| Milestone (skip scripts ≤ height) | mainnet **840000**, signet 2000000, … | `--milestone` / `--assumevalid-height` (`0` = full scripts) |
| Archive queue RAM | **512 MiB** | `--archive-queue-mb` (or advanced `RBITCOIN_ARCHIVE_QUEUE_MB`) |
| ConfirmParentCache header plans | always on | Tip-ahead header + tx_fks for multi-block MTP. No create pin FIFO / `RBITCOIN_RESIDENCY_BYTES` |
| Class A working-set cache | **256 MiB** | `--class-a-cache-mb` (or advanced `RBITCOIN_CLASS_A_CACHE_MB`) |
| Bulk store IO | **uring** (Linux) when available | See **Bulk store IO backends** above; ring depth **128**; `RBITCOIN_BULK_IO_WORKERS` for pread. Segmented `tx.head` FdOnly page RMW; Class C L2 write-behind (`docs/io-modality.md`) |
| Archive Class A append | **pwrite** (always) | `tx.body` / `tx.idx` mega-appends use `write_at_pwrite` only |
| `tx.head` (segmented) | fixed geometry | Default **25-bit** heads (128 MiB) with **4 B relative** fks; roll at 80% load / body soft span; **binary fuse8** on seal. `RBITCOIN_TX_HEAD_BITS` for tests only. Legacy mono-head datadirs require reindex |
| Confirm stages | **plan · prep · scripts · write** | Pipeline queues default **plan=1 · prep=1 · write=10** (`planq=n/1 prepq=n/1 writeq=n/10`). Env: `RBITCOIN_CONFIRM_PLAN_QUEUE`, `RBITCOIN_CONFIRM_PREP_QUEUE`, `RBITCOIN_CONFIRM_WRITE_QUEUE` (each **1..=64**). Legacy `RBITCOIN_CONFIRM_QUEUE` fills any stage whose specific env is unset. Plan packs tip-contiguous waves by **decoding BQ one block at a time** until soft **Σ inputs** (`RBITCOIN_CONFIRM_BATCH_INPUTS`, default **8000**, include overshoot block) or hard **144** blocks — at dense heights **usually a few blocks per plan** (live `n=`), not large multi-dozen block waves |
| Confirm batch inputs | **8000** soft | `RBITCOIN_CONFIRM_BATCH_INPUTS` (1..=1e6). Live line: `h= n= in=` (**n** = blocks in this pack, **in** = Σ inputs). Dense mainnet: **n** often 1–3 |
| Mempool weight budget | **~300e6 WU** | `--mempool-size-mb N` (maps N×1e6 WU) |
| Inhibit auto-suspend | **off** | `--inhibit-suspend` (uses `systemd-inhibit` if available) |

### Suspend inhibit

Long IBD runs can be interrupted if the host auto-suspends. Pass
`--inhibit-suspend` to request a systemd **block** inhibit for `sleep` and
`idle` while the process runs (via `systemd-inhibit`). Default is off. If
`systemd-inhibit` is missing or logind rejects the request, the node logs a
warning and continues without inhibit.

**Peers file:** `{datadir}/peers` stores discovered addresses and **PeerFlags**
(connected / fast / slow / incompatible / last-fail) between runs. Loaded at
start (before seeds), updated after IBD and on shutdown. Seeds are merged in
without clearing known flags.

**Index modes:** IBD defaults to **`IndexMode::Direct`**: archive batch-writes
packed Class A + durable **`tx.head`**; confirm batch-writes **spend annotations**
after Class C. Those two indexes are **complete before tip** — catch-up must
finish; tip entry does not backfill them. Scripthash is **not** progressively
materialized into heads: confirm only enqueues sorted runs (background flush +
merge). At tip the node **merges remaining runs and cold bulk-loads** durable SH
tables before Electrum (the only deferred index work). Tip SH materialize
**streams catalog runs with direct k-way merge** (up to ~4096 open files;
`RBITCOIN_SH_MAX_DIRECT_MERGE`) into a **live in-RAM open-address image per
prefix shard** (final-sized, ~0.5–1 GiB peak on mainnet 64-way; never size from
create counts — `RBITCOIN_SH_UNIQUE_HINT` optional). Fan-in reduce is **fallback
only** when the catalog exceeds max direct. IBD promotes L0 spills only at
≥75% of target run size (default target **512 MiB**) and compacts tiny catalog
runs so tip stays **O(10³) runs**, not O(10⁴). Materialize status logs ~**every
10s**. Path selection logs `path=FullCold|ColdResume|WarmOnly` plus
`catalog_complete` / `seal` / `tip_max_fk`. **Full cold reinit only if the SH
head is empty** (or force rebuild); a nearly complete index with residual runs
uses **warm batch apply** only. **SH runs pipeline:** confirm enqueues → large catalog spills; tip Class A
recollect is parallel (~64k-fk chunks, ~128 MiB per-thread spills, contiguous
SEAL prefix for resume). Direct enter only recollects a small SEAL gap (crash
window); full recollect is tip finalize. Tip materialize: WarmOnly / ColdResume
/ FullCold. Catalog compact is **IBD worker only** (crumbs &lt;~96 MiB).
**`RBITCOIN_SH_FORCE_REBUILD=1`:** sticky env must never redo multi-hour Class A
work. Empty head + **usable** catalog (real run mass, not a stale high-SEAL tail)
→ **reinit head only** + FullCold (keep runs/SEAL; gap recollect fills tip lag).
Empty head + **unusable** catalog → nuclear wipe + full recollect. **Durable
head** + FORCE → never wipe (bootstrap/clamp/Noop + warm residual only; materialize
mode is WarmOnly). Unset the env after a successful rebuild. Incomplete catalog
(high SEAL + tiny run mass, or consumed runs with no head) on an **empty** head
triggers full Class A recollect (SEAL=0). **Durable head** never uses run-mass
incompleteness (empty runs after successful materialize are normal): missing
`include_hwm` bootstraps from SEAL (never clamp SEAL→0); only when
`0 < include_hwm < SEAL` is SEAL clamped for gap recollect + warm residual.
Clearing residual run files **preserves `SEAL`** (watermark is not a run).
**SIGINT** mid cold keeps finished prefix shards (`scripthash.cold_progress`).
Mid-reduce keeps CHECKPOINT.
Deferred/residual runs batch warm-apply (10s status). On enter Direct, leftover
`ibd_utxo.map` / `point.runs` / `tx.runs` from old Catchup datadirs are removed
— prefer a **fresh datadir**. Legacy **16-way** `scripthash.head/` with
**`scripthash.runs` still present** auto-migrates on open (old head renamed
`scripthash.head.legacy-*`, empty 64-way + tip rebuild from runs). No runs left
⇒ reindex.

New stores: **header.head** = **single** open-address file (~24 MiB pre-size; not
256-way), **scripthash** **64** shards, **tx.head** = **segmented** fixed **25-bit**
heads (`tx.head.meta` + `tx.head.NNNNNN`, **4 B relative** create ids, 128 MiB per
segment). Probe is **page-local**: high mixed-txid bits select a 1024-slot page,
double-hash within the page (one 4 KiB IO @ 4 B). Capacity ends at
**`MIN(body soft span ~16 GiB, 80% of head slots)`**: seal builds **binary fuse8**
(~9 bits/key) then opens a new segment (no mono-file bits-widen, no shadow
resize thread). Open segment has **no** filter (always probed); sealed segments
are fuse-gated newest→oldest. Legacy monolithic `tx.head` / `.new` / `.resize` /
`.overflow` are **refused** — reindex. **tx_height** uses 4 B height slots.
Dense Class A fk + segmented **tx.idx** retained. Packed Class A only. Spends are
schema-v5 annotations on create outputs (no `point.head`).

**Memory rule:** Direct IBD writes durable segmented `tx.head.*` live and spend
annotations on confirm. Class A is packed full-tx bodies; inputs always store
**external** `prev_txid`. Parent resolve uses parent cache + `tx.head` (open +
fuse-gated sealed). SH create dedupe is an **O(1) height watermark**; durable SH
tables bulk-load at tip. Do not raise archive queues without watching RSS vs
page cache.

## Libre-relay-class policy (mempool + Electrum broadcast)

| Rule | Value |
|------|--------|
| Min relay | **0.1 sat/vB** (100 sat/kvB) |
| Dust | **not enforced** |
| Script templates | allow if consensus-valid (within weight/CPU) |
| RBF | **full RBF** (no BIP125 signaling required) |
| Annex | empty OK; non-empty only if first data byte after `0x50` is `0x00` |
| Cluster caps | 64 txs / 101 kWU |
| Eviction | worst linearization **chunk** when over weight budget |
| Compaction | DEAD slots reclaimed when wasteful (auto after confirm removes) |

Policy lives in `rbitcoin-consensus::policy` and is **never** applied on block connect.

## P2P transport

- **BIP324 v2 only** — plaintext v1 peers disconnect (`peer does not speak BIP324 v2`).
- Tx inv/getdata/tx relay is **off during IBD**; enabled in tip mode after catch-up.
- **BIP152 compact blocks v2:** `sendcmpct` high-bandwidth; mempool short-id fill +
  `getblocktxn` / `blocktxn`; full witness getdata fallback. We also **serve** `getblocktxn`.
- **BIP339 wtxidrelay:** sent when peer version ≥70016; mutual negotiation uses `MSG_WTX`.
- Session **ban score** (threshold 100) disconnects peers that spam bad compact payloads.
- Package accept: `ActiveMempool::accept_package`; experimental wire command `rbtpkg`
  (BIP331 not yet in rust-bitcoin 0.32 `NetworkMessage`).

## Electrum

```bash
./target/release/rbitcoin-node \
  --datadir ./datadir-mainnet \
  --network mainnet \
  --electrum-listen 127.0.0.1:50001 \
  --log-level info
```

TLS is **not** built into the node. Terminate TLS at nginx, Caddy, HAProxy, etc.,
and proxy plain TCP to `--electrum-listen` (e.g. `127.0.0.1:50001`).

| Feature | Behavior |
|---------|----------|
| Banner | states **libre-relay-class** |
| Transport | plain TCP only (external TLS termination) |
| `transaction.broadcast` | mempool accept → P2P inv announce |
| Unconfirmed history/balance/mempool | from cluster mempool |
| `transaction.get` | chain then mempool fallback |
| `relayfee` / `estimatefee` / histogram | from Libre min + live mempool |

## Signet lab

```bash
mkdir -p ./datadir-signet
./target/release/rbitcoin-node \
  --datadir ./datadir-signet \
  --network signet \
  --listen 0.0.0.0:38333 \
  --max-outbound 16 \
  --log-level info
```

### Resume / clean stop

Same `--datadir` resumes tip from the relational archive.

```bash
kill <pid>   # SIGTERM — flush store + mempool, exit 0
```

Prefer SIGTERM over `kill -9` (last uncommitted mempool batch may be lost on hard kill).

## Mainnet experimental

```bash
mkdir -p ./datadir-mainnet
./target/release/rbitcoin-node \
  --datadir ./datadir-mainnet \
  --network mainnet \
  --listen 0.0.0.0:8333 \
  --max-outbound 16 \
  --mempool-size-mb 300 \
  --log-level info
```

Full script validation (slow, used for consensus parity labs):

```bash
  --milestone 0
```

### Soak checklist

- [ ] Signet (or large range) to tip; restart resume
- [ ] Multi-day mainnet soak without corruption / OOM
- [ ] Post-milestone or `--milestone 0` script path exercised
- [ ] Disk headroom for full Class A archive
- [ ] Mempool file growth bounded under load (compaction + eviction)
- [ ] Electrum TCP wallet smoke (subscribe, broadcast, fees; TLS via proxy if needed)
- [ ] Peer diversity and reorg behavior under load

## 16 GiB RAM / sluggish disk (mainnet)

Full-validation IBD will be **disk-bound** and can freeze the UI if `datadir` shares
the desktop disk. Prefer a dedicated volume and modest memory knobs:

```bash
export RAYON_NUM_THREADS=4
# Prefer --milestone 840000 for catch-up, then reindex/full validate later if needed
nice -n 10 ionice -c 3 ./target/release/rbitcoin-node \
  --datadir /mnt/dedicated/datadir-mainnet \
  --archive-queue-mb 128 --class-a-cache-mb 128 \
  --network mainnet \
  --max-outbound 12 \
  --mempool-size-mb 200 \
  --log-level info
```

Correlate freezes: `grep 'hash-head rehash' your.log`.

## Consensus notes (historical mainnet)

Full validation has fixed several pre-soft-fork script edges:

| Height / class | Issue |
|----------------|--------|
| High-S ECDSA | normalize before verify (never consensus-fail) |
| Hashtype 0 | raw byte, not `from_consensus` → ALL |
| Lax DER pre-BIP66 | always `from_der_lax`; BIP66 is encoding check |
| High-bit S, `from_der`≠lax | never prefer strict-first |
| CODESEPARATOR in scriptSig | full EvalScript(scriptSig) for bare |
| Pre-BIP16 P2SH shape | bare HASH160/EQUAL; Core BIP16Exception @ 170060 |

In-memory **confirm reject blacklist** clears only on process restart after a binary fix.
