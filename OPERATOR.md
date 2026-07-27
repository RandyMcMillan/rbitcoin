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

## Logging

Operational logs go to **stderr** with UTC timestamps:

```
2026-07-15T03:04:26.725Z INFO  rbitcoin-node starting network=mainnet …
```

| Control | Values |
|---------|--------|
| `--log-level LEVEL` | `error` `warn` `info` `debug` `trace` `off` |
| `RBITCOIN_LOG` / `RUST_LOG` | bare level or `rbitcoin=debug` style |

Default: **info**. CLI wins over env.

### IBD status lines (every ~5s)

| Line | Level | Use |
|------|-------|-----|
| `ibd: progress` | INFO | Tip/arch rate, `loadq`/`writeq`, `txs=` (Class A / `tx.idx` count), horizon, ETA |
| `ibd: perf` | INFO | Inflight/archive pressure, **script/load** cost, **pin_hit%**, coarse load phases |
| `ibd: perf_dbg` | DEBUG | µs/blk, pin/edge detail, **archive prep/write/sticky**, contig park |

At **info**, you only get progress + slim perf. Enable **debug** when diagnosing archive sticky, prep vs write, or pin body_io. Ghost columns from deleted paths (wave-fill stubs, Direct SH head RMW) are omitted from both formatters.

**Archive `tx.head` split (perf_dbg):** `arch_prep … head_rd=` is parent
**read** resolve (`get_fk_by_txid_batch`, with `probe` / `idx` / `body` subtimers).
`arch_write … head_wr=` is create **insert** (`head_insert_many`). Sticky only
caches this batch’s creates (not head-resolved parents).

**Archive head resolve:** streaming — **mmap** probe + **io_uring** idx→body
prefix verify (deepest-cand-first early exit). `RBITCOIN_IO_URING=0` (or stream
setup failure) falls back to a phase-barrier path that still uses the shared
idx→body pipeline for body prefixes.

## Defaults and memory budgets

| Knob | Default | Override |
|------|---------|----------|
| IBD concurrent getdata | **1024** | code `IbdConfig::window` |
| Blocks in transit / peer | **16** | `IbdConfig::per_peer` |
| Live IBD peers | **16** | `--max-outbound` |
| Milestone (skip scripts ≤ height) | mainnet **840000**, signet 2000000, … | `--milestone` (`0` = full scripts) |
| Archive queue RAM | **512 MiB** | `RBITCOIN_ARCHIVE_QUEUE_MB` |
| Class A working-set cache | **256 MiB** | `RBITCOIN_CLASS_A_CACHE_MB` |
| Bulk store IO | **io_uring** (Linux) | Head resolve body prefixes, confirm body loads, **spend annotate** RMW (9 B meta pread→pwrite; multi-list appends mmap on read CQE), **`tx.head` resize** shadow fill. Archive `tx.head` **insert_many** stays mmap. `RBITCOIN_IO_URING=0` → pread/mmap fallbacks; `RBITCOIN_BULK_IO_WORKERS` for pread parallelism |
| Archive Class A append | **pwrite** (default) | `tx.body` / `tx.idx` mega-appends use `write_at_pwrite` (page cache; avoids dirtying multi‑GiB mmaps for sequential write). `RBITCOIN_FD_APPEND=0` → mmap `write_at` (debug/compare) |
| `tx.head` fill (rebuild + online resize) | shared knobs | `RBITCOIN_TX_HEAD_READ_BATCH` (default **65536**, max 1e6) — Class A fks per idx+txid bulk read; `RBITCOIN_TX_HEAD_WRITE_CHUNK` (default **65536**, max 1e6) — `insert_many` size (one fence/group); `RBITCOIN_TX_HEAD_FILL_WAVE` (default **1048576**) — fks per bg resize poll wave / progress log. Legacy `RBITCOIN_TX_HEAD_RESIZE_*` names still accepted |
| Confirm stages | **load · scripts · write** | Pipeline queues cap **5** each (`loadq=n/5 writeq=m/5`; `name<0/cap` when the next worker is waiting on an empty queue) |
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
tables before Electrum (the only deferred index work). On enter Direct, leftover
`ibd_utxo.map` / `point.runs` / `tx.runs` from old Catchup datadirs are removed —
prefer a **fresh datadir**.

New stores (schema **v10**): **header.head** = **single** open-address file (~24 MiB
pre-size; not 256-way), **scripthash** 16 shards, **tx.head** = single address file
starting at mainnet **BITS=26** (~**256 MiB** sparse, `2^26` × **4 B** create_fk;
override with `RBITCOIN_TX_HEAD_BITS` in `8..=34` / tiny scale). Probe is **page-local**:
high txid bits select a 1024-slot page, double-hash within the page (one 4 KiB IO @ 4 B).
**Online sequential resize** when `txs.count()/slots ≥ 0.80` or first deep insert
(depth>128) / probe exhaust: shadow rebuild on a **dedicated OS thread**
(`rbitcoin-tx-head-resize`). BITS **33+** use **8 B** entries. Layout (**v5**) lives in the
`tx.head` **trailing footer** (page-local probe, slots at offset 0); no `tx.head.meta`
sidecar. Older footers/meta force recreate + rebuild from Class A.
**tx_height** uses 4 B height slots (not fk width). Dense Class A fk + **tx.idx**
retained. Packed Class A only. Spends are schema-v5 annotations on create outputs
(no `point.head`).

**Memory rule:** Direct IBD writes durable `tx.head` live and spend annotations
on confirm. Class A is packed full-tx bodies; inputs always store **external**
`prev_txid`. Parent resolve uses parent cache + `tx.head`. SH create dedupe is
an **O(1) height watermark**; durable SH tables bulk-load at tip. Do not raise
archive queues without watching RSS vs page cache.

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
the desktop disk. See **[docs/store-efficiency-plan.md](./docs/store-efficiency-plan.md)**
for the TB-scale store + Electrum redesign plan.

Practical profile until the fat Electrum index lands:

```bash
export RBITCOIN_ARCHIVE_QUEUE_MB=128
export RBITCOIN_CLASS_A_CACHE_MB=128
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
