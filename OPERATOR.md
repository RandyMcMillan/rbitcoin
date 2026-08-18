# Operator guide — full participant node

## Status

BIP324 v2-only P2P, cluster mempool (Libre admission + **consensus script checks on accept**),
Electrum confirmed + unconfirmed (TLS via reverse proxy). **Mainnet is experimental** — see
[`docs/experimental-mainnet.md`](./docs/experimental-mainnet.md). Watch reorgs and disk
headroom before any serious use. Default mainnet **`--milestone 840000` skips script/sig checks** at/below
that height; use `--milestone 0` for full scripts.

Architecture: peer wire lands in an **in-RAM body queue**; confirm
(lookup → load → scripts → write) is the **sole Class A appender** and
advances Class C tip in the same era. Download defaults to **1024** concurrent
getdata (not a tip-distance cap), max **16** blocks in transit per peer.

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

**CI snapshots:** every green `master` (or `main`) `ci` run starts
`musl`, `windows`, and `macos`, which upload 90-day artifacts:

| Workflow | Artifact |
|----------|----------|
| **musl** | `rbitcoin-musl-x86_64-linux-<12-hex>` — fully static |
| **windows** | `rbitcoin-x86_64-windows-<12-hex>` — MSVC CRT-static `.exe` |
| **macos** | `rbitcoin-aarch64-darwin-<12-hex>` — system-dylib only, ad-hoc signed (not notarized). **aarch64 only** |

Open the commit → Checks → workflow → Artifacts. None of these are
required PR checks. Retry from Actions → workflow → Run workflow.
Label a PR **`static-binaries`** to build the same three on that head.
Local Linux `target/release/` install is still `nix build .#rbitcoin-musl`
on a clean master tree. Windows IoRing is not supported. Darwin/Windows
are not Nix packages — see [`docs/reproducible-builds.md`](docs/reproducible-builds.md).

**Darwin Gatekeeper:** the zip is ad-hoc signed (`codesign -s -`), not
notarized. If Finder or a browser sets quarantine and the binary is killed
on launch:

```bash
xattr -d com.apple.quarantine rbitcoin-node rbitcoin-cli
```

**Windows store files** are opened `FILE_FLAG_OVERLAPPED` (IOCP). Header
create/open/grow use positional `ReadFile`/`WriteFile` +
`SetFileInformationByHandle`, not std `Read`/`Write`/`Seek`. Mixed
`./datadir\store\...` in logs is display only — not a path bug.

## CLI (operator-first)

Routine knobs are **CLI / conf**, not required env vars. Clean smoke:

```bash
./target/release/rbitcoin-node --smoke --datadir /tmp/rb-smoke --network regtest
```

| Flag | Core-ish alias | Default |
|------|----------------|---------|
| `--datadir PATH` | same | `./datadir` |
| `--datadir-cold PATH` | conf `datadir-cold=` | unset — Class A `inwit.body` / `inwit.idx/` under `{PATH}/store`; everything else stays in `--datadir` |
| `--network NET` | `--chain` | `mainnet` |
| `--signetchallenge HEX` | `--signet-challenge` | default global Signet challenge |
| `--signetblocktime SECONDS` | `--signet-block-time` | 600; requires a custom challenge |
| `--listen ADDR` | | bind later default port |
| `--connect ADDR` | (repeatable) | seeds |
| `--milestone HEIGHT` | `--assumevalid-height` | network default (mainnet 840000) |
| `--max-outbound N` | `--maxoutbound` | 16 live download peers |
| `--maxinbound N` | `--maxconnections` | 125 inbound sessions |
| `--mempool-size-mb N` | `--maxmempool` | ~300 MiB weight |
| `--conf FILE` | | none |
| `--log-level LEVEL` | | `info` |
| `--api-log PATH` | conf `api_log=` | off — JSONL of Electrum / Esplora / RPC calls |
| `--no-seeds` | `--noseeds` | seeds on |
| `--shindex` | conf `shindex=1` | **off** — Class B scripthash (required for Electrum/Esplora) |
| `--sptweaks` | conf `sptweaks=1` | **off** — thin BIP-352 tweak index (`sp_tweaks.*`) |
| `--electrum-listen ADDR` | | disabled (**requires** `--shindex`) |
| `--esplora-listen ADDR` | | disabled (Esplora REST; **requires** `--shindex`) |
| `--rpc-listen ADDR` | conf `rpc_listen` | disabled — Core-class JSON-RPC subset |
| `--rpcuser` / `--rpcpassword` | conf `rpcuser`/`rpcpassword` | unset — else cookie `{datadir}/.cookie` |
| `--inhibit-suspend` | | off |

Conf file: simple `key=value` lines (`#` comments). CLI overrides conf. Example:

```
network=signet
maxinbound=64
mempool_size_mb=100
```

`--datadir` holds the node root (`store/`, `mempool/`, `peers`, `.cookie`).
Omit `--datadir-cold` and cold files live there too. Set it to put the large
rarely-read Class A **inwit** stem (`inwit.body` + `inwit.idx/`, ~486 GiB + idx
on mainnet) on another volume. Pin / spend-annotate / Electrum / Cake do not
read inwit; reconstruct / `getrawtransaction` / block serve do.

```
--datadir /mnt/nvme/rbtc --datadir-cold /mnt/hdd/rbtc-cold
# hot:  /mnt/nvme/rbtc/store/txout.body  (and the rest)
# cold: /mnt/hdd/rbtc-cold/store/inwit.body
#       /mnt/hdd/rbtc-cold/store/inwit.idx/
```

A hot-store sidecar `inwit.reloc` records the split. Opening without
`--datadir-cold` then refuses. Do not leave `inwit.*` in both places. Moving an
existing datadir is operator `mv` (or copy+remove cross-device):

```
mkdir -p /mnt/hdd/rbtc-cold/store
mv /mnt/nvme/rbtc/store/inwit.body /mnt/nvme/rbtc/store/inwit.idx /mnt/hdd/rbtc-cold/store/
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

### Tip-follow status lines (after catch-up + tip SH ready)

| Line | Level | Use |
|------|-------|-----|
| `tip: perf` | DEBUG | Every ~5s: follow peers, blocks this window, mempool accept/reject + wall µs, inv/getdata/announce, Esplora/Electrum req counts + avg/max µs |
| `tip: accept` | INFO | Per accepted tip block: wall/load/script/class_a/class_c/SH breakdown (not emitted on reject) |
| `UpdateTip` | INFO | New best hash/height after connect |
| `node: tip=…` | INFO | Tip height change summary (follow_live) |

Requires **tip mode** (`node: catch-up complete … tip tracking`). During IBD use `ibd: perf` instead. Enable `tip: perf` with `--log-level debug` (or conf / `RBITCOIN_LOG=debug`).

### IBD status lines (every ~5s)

| Line | Level | Use |
|------|-------|-----|
| `ibd: progress` | INFO | Tip rate, `ready=` (BQ resolve-complete, not a queue), `scriptq`/`writeq`, `txs=` (Class A / `tx.idx` count), horizon, tip ETA, **`bq soft=n/win RAM=`** (in-RAM body queue; soft densify: under ~100 MiB free ahead, over that only ~1 min confirm window at tip rate) |
| `ibd: perf` | INFO | Inflight + **`bq soft= RAM=`**; **`load=`** is pin+assemble only. **`load_thr pack/stamp/pin/asm/prune`** is the load OS thread (leftover TipOnly is `stamp=`). **`script=`** is verify ns (`jobs=` / `skip=`); recv/send are wait. **`lookup_thr keep=`** is live-union splice. **`pin_txid=`** vs leftover `tx.head` |
| `ibd: sizes` | INFO | RSS + work path + **`bq soft=` / `RAM=`** + **conf_plans** + confirm pipe |
| `ibd: perf_dbg` | DEBUG | µs/blk load/write, pin/edge detail, **plan_batch** (`us/pin_txid` vs `probe/idx/body us/key`) + **class_a commit** |

At **info**, progress + perf already expose load/write bottlenecks (schema 16). Enable **debug** for plan-batch / Class A commit subtimers and per-block µs. Ghost columns from deleted paths (wave-fill stubs, Direct SH head RMW) are omitted from both formatters. Pipeline roles: [`docs/concurrency.md`](docs/concurrency.md). Head files: [`docs/heads.md`](docs/heads.md).

`pin_txid%` is stamp `txid→create_fk` from the published `live_union` chain vs leftover `tx.head`. `pin_hit%` is load outs adopt/plan reuse — this-window range-fills are `pin_new` only.

**Tip hole / peer hygiene:** `hole=` on the progress line is the fetch gap from
tip+1 to the next claim-ready body. Tip-batch getdata races up to 4 peers
(preferring faster live rates) and re-races after ~6s without wire. WARN
`ibd: peer[…] stalled` is absolute zero block progress (~30s). WARN
`ibd: peer[…] relative-slow (bps= med= spread=…)` disconnects a clear
half-median outlier only after ~60s warm-up and only when the peer pack is not
tight (max/min bps &gt; 2×); good-but-slightly-slower peers are kept.

**Create pins:** pipeline-local only (`batch_pin` / `BatchParents`). No process pin FIFO. Header plans via ConfirmParentCache. Just-confirmed **identity** (`txid → fk+range`) lives in a height-bounded RecentCreates ring (not outs).

**Archive `tx.head` split (perf_dbg):** `plan_batch … head_rd=` is parent
**read** resolve (`get_fk_by_txid_batch`, with `probe` / `idx` / `body` subtimers).
`class_a_commit … head=` is create **insert** (`head_insert_many`). Pipeline pins stay on the plan (`batch_pin`); no process denserels seed.

**Archive head resolve:** streaming — **FdOnly** page-coalesced head probe +
**FdOnly** `txout.idx` + **`txid.body`** identity via **io_uring or pread**
(deepest-cand-first).
**Class A `txout` / `inwit` / `spent` + their `*.idx`, `tx.head`, header head,
SH head/body, and spenders are fd pread/pwrite**.
Full modality matrix: [`docs/io-modality.md`](docs/io-modality.md).

## Bulk store IO backends

**Bulk batch** uses a **single** switch: `RBITCOIN_IO=uring|pread` (default uring
when available). Table transport is always **fd pread/pwrite**. Compact Class C
is L2 write-behind; see [`docs/io-modality.md`](docs/io-modality.md). Per-path
env overrides are **removed**. If `uring` is selected but setup fails, demote to
**pread** / **pwrite**.

| Env | Values | Note |
|-----|--------|------|
| **`RBITCOIN_IO`** | `uring` \| `pool` \| `iocp` \| `pread` | Only bulk switch (`mmap` demotes to pread) |

Inventory / survivors: [`docs/env-knobs.md`](docs/env-knobs.md).

**RWF_DONTCACHE:** not used. `spent.body` is its own file; evicting those
pages after annotate does not protect `txout`. See
[`SCHEMA.md`](SCHEMA.md) (Schema 17 freeze).

- **uring** — Linux io_uring (ring depth **128**). On Windows this opens
  IOCP.
- **pool** — worker-pool completion session (Darwin default).
- **iocp** — Windows IOCP (Windows default).
- **pread** / **pwrite** — libc positional IO (session off).
- Class A **`txout` / `inwit` / `spent` + `*.idx` linear appends always pwrite**.

## Defaults and memory budgets

| Knob | Default | Override |
|------|---------|----------|
| IBD concurrent getdata | **1024** | code `IbdConfig::window` |
| Blocks in transit / peer | **16** | `IbdConfig::per_peer` |
| Live IBD peers | **16** | `--max-outbound` |
| Inbound P2P sessions | **125** | `--maxinbound` / `--maxconnections` |
| Milestone (skip scripts ≤ height) | mainnet **840000**, signet 2000000, … | `--milestone` / `--assumevalid-height` (`0` = full scripts) |
| ConfirmParentCache header plans | always on | Tip-ahead header + tx_fks for multi-block MTP (no create pin FIFO) |
| Bulk store IO | **uring** (Linux) when available | `RBITCOIN_IO` only; ring depth **128**. Segmented `tx.head` FdOnly; Class C L2 write-behind (`docs/io-modality.md`) |
| Archive Class A append | **pwrite** (always) | `txout` / `inwit` / `spent` + `*.idx` mega-appends use `write_at_pwrite` only |
| `tx.head` (segmented) | fixed geometry | Default **25-bit** heads (128 MiB) with **4 B relative** fks; roll at 80% load / body soft span; **binary fuse8** on seal. Legacy mono-head datadirs require reindex |
| Confirm stages | **lookup · load · scripts · write** | Real queues **scriptq=4 · writeq=20**. **`ready=`** is BQ resolve-complete inventory (no lookup→load channel). **Load** packs tip-contiguous runs by soft **Σ inputs 8000** or hard **144** blocks — dense mainnet usually a few blocks per batch (not ~32). IBD **lookup** TipOnly-resolves at most **64000** inputs or **1080** BQ-ready heights per wave (holds a short wave while `ready` is over half the 1-min BQ window, unless the first unresolved height is in the load-facing half of that window). |
| Confirm batch inputs | **8000** soft | Hardcoded. Live line: `h= n= in=` (**n** = blocks in pack, **in** = Σ inputs) |
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
split Class A (`txout` / `inwit` / `spent`) + durable **`tx.head`**; confirm
batch-writes **spend annotations** on **`spent.body`** after Class C. Those
indexes are **complete before tip** — catch-up must finish; tip entry does not
backfill them. Scripthash is **not** progressively materialized into heads:
confirm only enqueues sorted runs (background flush + merge). At tip the node
**merges remaining runs and cold bulk-loads** durable SH tables before Electrum
(the only deferred index work). Tip SH materialize **streams catalog runs with
direct k-way merge** (up to ~4096 open files; records unique on
`(scripthash, create_fk)`, `key_len=40`) into **sealed sorted+idx
main shards** (no main fuse; no 0.5–1 GiB in-RAM OA image per shard;
megakey pages write as they fill — ≤510 FKs buffered). Schema-16
`key_len=32` leftover `scripthash.runs` are refused (wipe that dir and
rematerialize). Class A with creates in the pre-pack 16-byte meta /
9-byte spent layout is refused (wipe datadir and redo IBD). New keys after seal go
to one **global ingest OA** (mainnet 2²² slots ≈ 128 MiB). Fan-in reduce is
**fallback only** when the catalog exceeds max direct. IBD promotes L0 spills
only at ≥75% of target run size (default target **512 MiB**) and compacts tiny
catalog runs so tip stays **O(10³) runs**, not O(10⁴). Materialize status logs
~**every 10s**. Path selection logs `path=FullCold|ColdResume|WarmOnly` plus
`catalog_complete` / `seal` / `tip_max_fk`. **Full cold reinit only if the SH
head is empty** (or force rebuild); a nearly complete index with residual runs
uses **warm batch apply** only. **SH runs pipeline:** confirm enqueues → large
catalog spills; tip Class A recollect is parallel. Direct enter only recollects
a small SEAL gap (crash window); full recollect is tip finalize. Tip
materialize: WarmOnly / ColdResume / FullCold. Catalog compact is **IBD worker
only** (crumbs &lt;~96 MiB). Force-rebuild sticky env (`RBITCOIN_SH_FORCE_REBUILD`)
must never redo multi-hour Class A work casually — see [`docs/env-knobs.md`](docs/env-knobs.md).
Empty head + **usable** catalog → **reinit head only** + FullCold. Empty head +
**unusable** catalog → nuclear wipe + full recollect. **Durable
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

**Schema 15 SH layout:** durable values are Empty / Inline / geometric **slab**
(3–256 fks) / megakey **pages** (≥257). Main shards are **sealed sorted+idx** (no fuse);
new keys land in **`scripthash.ovf/ingest`**. ≥8 sealed ovf files compact-merge.
**Open upgrade:** empty Class A + empty/missing SH may silently rewrite `meta`
13/14→15. A packed `tx.body` **with creates**, or a durable page-era (or
schema-13 slab) SH index, is **refused** — wipe `store/scripthash*` (and/or
Class A) and rematerialize / redo IBD; there is **no dual-read**.
After ingest load ≥ ~0.80, ingest seals to sorted ovf and rolls. Legacy
full-size `scripthash.ovf.head` is removed on open. Existing main keys update
`value16` in place.

New stores: **header.head** = **single** open-address file (~24 MiB pre-size; not
256-way), **scripthash** **64** shards, **tx.head** = **segmented** fixed **25-bit**
heads (`tx.head.meta` + `tx.head.NNNNNN`, **4 B relative** create ids, 128 MiB per
segment). Probe is **page-local**: high mixed-txid bits select a 1024-slot page,
double-hash within the page (one 4 KiB IO @ 4 B). Capacity ends at
**`MIN(body soft span ~16 GiB, 80% of head slots)`**: seal builds **binary fuse8**
(~9 bits/key) then opens a new segment (no mono-file bits-widen, no shadow
resize thread). Open segment has **no** filter (always probed); sealed segments
are fuse-gated newest→oldest. Legacy monolithic `tx.head` / `.new` / `.resize` /
`.overflow` are **refused** — reindex. Create height is a RAM fence (no
`tx_height` file; schema 16).
Dense Class A fk + segmented **`txout.idx` / `inwit.idx` / `spent.idx`**.
Class A is **split** (outs / ins+wit / sole-spender). Spends are schema-v5
annotations on **`spent.body`** (no `point.head`). Inputs store **`create_fk` +
vout** (soft `prev_txid` in RAM only).

**Memory rule:** Direct IBD writes durable segmented `tx.head.*` live and spend
annotations on confirm. Pin/SH/Cake read **`txout` only**; annotate dirties
**`spent`**. Parent resolve uses parent cache + `tx.head` (open + fuse-gated
sealed). SH create dedupe is an **O(1) height watermark**; durable SH tables
bulk-load at tip as sorted files (ingest OA is the only large SH heap). Densify
is gated by body-queue soft depth — do not raise that depth without watching
RSS vs page cache. Working-set sizes:
[`SCHEMA.md`](./SCHEMA.md) (mainnet census) and [`docs/ibd-memory.md`](docs/ibd-memory.md).

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
| Fee estimate | **10-minute inclusion** (cluster-chunk frontier + confirm-memory floor); see [`docs/mempool-fee-estimation.md`](docs/mempool-fee-estimation.md) |
| Compaction | DEAD slots reclaimed when wasteful (auto after confirm removes) |
| Slot table | **131 072** initial records (grows by doubling to 1 048 576); free-slot ensure **before** append |

Policy lives in `rbitcoin-consensus::policy` and is **never** applied on block connect.

**Empty-headers lag — two different causes:**

| Symptom | Cause | Fix |
|---------|--------|-----|
| `known≈982k` while peers ~961k, absurd resume walk | False `prev_fk` / duplicate header edges | Prefer a **fresh datadir**; header rows are hash-unique on write |
| `tip=H` but tip **hash** is a short orphan sibling; peers ahead | Stale confirmed tip; most-work **explore + reorg** | Restart; expect reorg once bodies densify |
| Stuck on tip+1: `prevout already spent` / many re-rejects of same block | Orphan Class C (second Class A+C copy at tip height) | Fixed on open: complement `repair_class_c_above_tip` + confirmed-strong **membership** |

**Every open:** the node (1) revalidates the last **six** confirmed heights
(header `prev_fk`/hash chain, Class A range bounds, merkle from `txid.body`,
those six runs all-strong) and may **shrink tip** or clear a bad body, then
(2) one Class C complement repair (unstrong leftover 1s in fence holes / a
short suffix — not a minute-long walk of every create). Look for
`rbitcoin: class_c repair cleared=…` and `rbitcoin: tip revalidate …` on
stderr. That is intentional Core-style `checkblocks=6` + crash/race healing —
not a full reindex. Widespread mid-chain header graph poison still means a
clean datadir.

**Mempool recovery:** `{datadir}/mempool/` is a private sidecar (not Class A). If it
is damaged or an old 4k-slot table was left wedged, stop the node and delete that
directory — the next start recreates it empty and redownloads unconfirmed txs.
Do **not** wipe `store/` for mempool slot/full errors.

## P2P transport

- **BIP324 v2 only** — plaintext v1 peers disconnect (`peer does not speak BIP324 v2`).
- Tx inv/getdata/tx relay is **off during IBD**; enabled in tip mode after catch-up.
- **BIP152 compact blocks v2:** `sendcmpct` high-bandwidth; mempool short-id fill +
  `getblocktxn` / `blocktxn`; full witness getdata fallback. We also **serve** `getblocktxn`.
- **BIP339 wtxidrelay:** sent when peer version ≥70016; mutual negotiation uses `MSG_WTX`.
- Session **ban score** (threshold 100) disconnects peers that spam bad compact payloads.
- Package accept: `ActiveMempool::accept_package` via RPC `submitpackage` or
  Esplora `POST /txs/package`. No P2P package command (BIP331 is not in
  rust-bitcoin 0.32).

## Scripthash index (`--shindex`)

Class B **scripthash** reverse index is **optional** (default **off**), analogous
in *operator spirit* to Core’s heavy reverse indexes — **not** the same as
Core `-txindex` (we always keep Class A + `tx.head` for by-txid lookup).

| Mode | Behavior |
|------|----------|
| **off (default)** | No SH run enqueue during IBD; no tip bulk materialize. Tip follow + mempool relay + JSON-RPC work without SH. |
| **on** (`--shindex` / `shindex=1`) | Direct IBD SH runs + tip bulk materialize; Electrum/Esplora may start when SH is tip-ready. |

**Electrum or Esplora without `--shindex` fails at process start** (clear config error).

Order-of-magnitude costs (mainnet-class SSD; not a warranty):

- **During IBD with shindex=1:** modest extra work (run stream); after IBD, bulk materialize is typically **tens of minutes to a few hours**.
- **Enable after tip already synced:** full recollect/materialize from Class A — **often multi-hour**; tip follow continues; Electrum waits until SH ready.
- **Disable later:** tables are **left on disk** (no automatic purge). Re-enable may rematerialize.

Tip-follow readiness is **independent** of SH materialize (`tip_follow_ready` ≠ `sh_tip_ready`).

## Silent payment tweaks (`--sptweaks`)

Optional **thin** BIP-352 index for Cake-compatible `blockchain.tweaks.subscribe`.
Default **off**. The Electrum method still exists when off (naive per-height
walk). Flag on = persist + serve-from-index.

**Not built during Direct IBD** (the write thread stays Class A + annotate).
After catch-up, **SH materialize first** (if `--shindex`), then a background
walker fills `origin..=live tip` from Class A. Tip write-through only when
`height == next_height`; if confirm is ahead, backfill owns the hole. Kill
is safe: `next_height` is the last complete put; restart in Tip (or after
the next Direct catch-up reaches tip) resumes the walker. Electrum during
the hole uses the naive path.

On disk (schema 17 dirs; leftover single files are unlinked on startup):

| File | Contents |
|------|----------|
| `store/sp_tweaks.idx/` | `meta` (`origin` + fmt 3) + `NNNNNN` tip-only `u32` start offs (no `header_fk`) |
| `store/sp_tweaks.body/` | Matching `NNNNNN` files: per tx `len=0` or `len=33` + compressed `A_tweak`. New pair when the next start would exceed 4 GiB. |

**Not stored:** txids, Taproot outs, values, parent scripts. Cake
`output_pubkeys` are joined from this block’s **`txout`** body (~12 ms
sequential on a 4k-tx 9p block; witness stays in `inwit`). Indexed serve does
**not** parent-peek (~40–80 blk/s vs ~1.5–3 naive on that VM).

Tip follow writes 65 B-class records from already-pinned parents when the
cursor is caught up. Reorg truncates with tip. Post-IBD backfill is
IO-bound (**hours** on SSD; 9p/spinning rust longer). Progress is the idx
itself (kill-safe).

Cake’s scan isolate may still hardcode `electrs.cakewallet.com` even after a
successful probe — see `COMPAT.md`.

## Electrum

Internet-facing Electrum is supported as a **wallet-client backend** (Electrum,
Sparrow, similar): bind plain TCP (public or loopback), terminate **TLS at a
reverse proxy**, and rely on the node’s **app DoS limits** always being on. A
loopback-only bind is convenient with a local proxy, but it is **not** the
security model by itself.

**Requires `--shindex`.** Without it the node refuses to start.

`server.version[0]` is `rbitcoin-electrs <ver>` so Cake `getNodeIsElectrs()`
will probe silent-payment tweaks. We are **not** electrs — see `COMPAT.md`.

**Not a graphical explorer.** We serve clients that already know their
scripthashes / txids; we do **not** aim to back block-explorer search UIs.

```bash
./target/release/rbitcoin-node \
  --datadir ./datadir-mainnet \
  --network mainnet \
  --shindex \
  --sptweaks \
  --electrum-listen 127.0.0.1:50001 \
  --log-level info
```

TLS is **not** built into the node. Terminate TLS at nginx, Caddy, HAProxy, etc.,
and proxy plain TCP to `--electrum-listen` (e.g. `127.0.0.1:50001` behind the
proxy, or a public bind if the proxy sits elsewhere and you accept that risk).

| Feature | Behavior |
|---------|----------|
| Banner | states **libre-relay-class** |
| Transport | plain TCP only (external TLS termination) |
| `transaction.broadcast` | mempool accept → P2P inv announce |
| Unconfirmed history/balance/mempool | from cluster mempool |
| `transaction.get` | chain then mempool fallback |
| `relayfee` / `estimatefee` / histogram | from Libre min + live mempool |
| Silent Payments tweaks | `blockchain.tweaks.subscribe` — with `--sptweaks` index: multi-height load (default ≤128 heights / ≤8192 eligible txs per wave) then per-height Cake notifies; Class A join is one `idx_body_pipeline` wave per batch (uring when enabled). Without index / hole: naive per height (Class A + parent outs). JSON-RPC result is the **first** height (`getTweaks` probe `[0,1,false]` → `{"0": {}}`). Further heights are notifications, then `{"message":"done"}`. `count` is honored through tip. `server.version[0]` contains `electrs`. On 9p-class IO expect slower than local disk. |

### API request log

`--api-log PATH` (or conf `api_log=PATH`) appends **one JSON line per Electrum, Esplora, and RPC call**:

```
{"ts":"…Z","surface":"electrum","peer":"192.168.88.20:51122","method":"blockchain.tweaks.subscribe","params":"[850000,8,false]","wall_ms":2410,"ok":true,"err":null}
```

`tail -f` that file. The same line is also emitted at **DEBUG** as `api: …` (so `--log-level debug` shows methods in `mainnet.log`). Params are truncated (~384 bytes) so broadcast hex does not fill the disk.

Use this to see whether Cake is hitting tweaks vs only scripthash history, and which calls take seconds.

### App DoS floor (always on)

Shared [`ServeLimits`](crates/rbitcoin-electrum) defaults (also the future Esplora
floor). Excess connections are **rejected immediately** (no hang); oversize lines
and idle clients fail closed.

| Limit | Default | Role |
|-------|---------|------|
| Max connections | 256 | Concurrent Electrum TCP clients |
| Max request line | 1 MiB | One JSON-RPC line including `\n` |
| Idle timeout | 120 s | No complete request → disconnect |
| Max scripthash subs / conn | 1000 | Notify fan-out cap |
| Max broadcast hex | ~8 MiB | `transaction.broadcast` hex length |

Edge rate-limits, auth, and TLS cipher policy stay on the proxy. See
[`SECURITY.md`](./SECURITY.md).

## Esplora REST

Blockstream-**compatible** **plain HTTP** API for **wallet clients and APIs**
(exact address/scripthash, tx/block by id, broadcast)—**not** a graphical
block-explorer backend. Same internet-facing model as Electrum: app DoS limits
always on; terminate TLS at a reverse proxy.

**Requires `--shindex`.** Without it the node refuses to start.

**Explicit non-goals:** explorer search/`address-prefix`, Liquid, mining
templates, mempool.space-style catalogue UI APIs.

```bash
./target/release/rbitcoin-node \
  --datadir ./datadir-mainnet \
  --network mainnet \
  --shindex \
  --esplora-listen 127.0.0.1:3000 \
  --log-level info
```

Conf: `shindex=1` and `esplora_listen=127.0.0.1:3000`. Default is **disabled**.

## Core-class JSON-RPC

Optional HTTP JSON-RPC subset (default **off**). Auth: cookie file under
`{datadir}/.cookie` or `--rpcuser`/`--rpcpassword`. Does **not** require
`--shindex` (chain/mempool/rawtx by id only). See [`docs/rpc.md`](./docs/rpc.md)
and [`COMPAT.md`](./COMPAT.md).

```bash
./target/release/rbitcoin-node \
  --datadir ./datadir-mainnet \
  --network mainnet \
  --rpc-listen 127.0.0.1:8332 \
  --log-level info
# same datadir cookie:
rbitcoin-cli --datadir ./datadir-mainnet getblockcount
```

| Feature | Behavior |
|---------|----------|
| Transport | plain HTTP (axum + tower body/concurrency/timeout from `ServeLimits`) |
| WebSocket | `/v1/ws` (+ `/ws`); **separate** WS connection cap (default 64) so upgrades do not starve REST |
| Tip / blocks | tip height/hash; `/blocks[/:start_height]` (10 summaries); `/block/:hash` JSON + **raw** + status |
| Tx | full JSON, hex, **raw**, status, Electrum merkle-proof, **BIP37 merkleblock-proof**, outspends |
| Address / scripthash | chain_stats, utxo, history pages (25 + `last_seen_txid`), `/txs/mempool`; complete after SH tip finalize |
| Mempool | `/mempool`, `/mempool/txids`, `/mempool/recent`, `/fee-estimates`; `POST /tx` and **`POST /txs/package`** when hub open |
| Without mempool | mempool routes empty/safe; POST broadcast → **503**; WS track still upgrades but mempool pushes need hub |
| Unknown / non-goal | **404** (explorer-only APIs e.g. address-prefix; Liquid; mining template) |

**Large responses:** `GET /block/:hash/raw` may be multi‑MB; concurrency/timeout from `ServeLimits` still apply.  
**Package broadcast:** body is a JSON array of tx hex (max 25); uses the same libre-relay mempool policy as single `POST /tx`.

DoS knobs share Electrum’s `ServeLimits` defaults (256 conns, 1 MiB body, 120 s timeout).
WebSocket extras (defaults): max 64 concurrent `/v1/ws` sockets, 64 KiB client frames,
64 tracked addresses and 64 tracked txids per connection. See [`COMPAT.md`](./COMPAT.md)
“Esplora WebSocket”.

### Reverse proxy (TLS + WebSocket upgrade)

Terminate TLS and forward REST **and** WebSocket to the same upstream. Example nginx:

```nginx
location /api/ {
    proxy_pass http://127.0.0.1:3000/;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Host $host;
    proxy_read_timeout 3600s;
}
```

Clients then use `wss://host/api/v1/ws` (proxy strips `/api`). Caddy: `reverse_proxy`
with default HTTP/1.1 upgrade support to the same listen.

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

### Custom Signet

A custom Signet derives its P2P message magic from the challenge. Default
Signet seeds are not used, so provide at least one peer with `--connect`.
Use a dedicated datadir for each challenge.

```bash
mkdir -p ./datadir-custom-signet
./target/release/rbitcoin-node \
  --datadir ./datadir-custom-signet \
  --network signet \
  --signetchallenge 51 \
  --signetblocktime 60 \
  --connect 192.0.2.1:38333 \
  --listen 0.0.0.0:38333 \
  --milestone 0 \
  --log-level info
```

The equivalent conf-file keys are `signetchallenge` and `signetblocktime`.
Replace the illustrative `OP_TRUE` challenge and documentation-only peer with
the parameters supplied by the custom Signet operator.

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

### Before trusting mainnet

- [ ] Signet (or large range) to tip; restart resume
- [ ] Mainnet tip follow without corruption / OOM
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
