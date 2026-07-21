# Operator guide — full participant node

## Status

**Plan P0–P7 complete** (BIP324 v2-only P2P, cluster mempool, Libre admission, Electrum
confirmed + unconfirmed; TLS via reverse proxy). Mainnet full validation (`--milestone 0`) is
still **experimental**: exercise soak, reorgs, and disk headroom before production use.

Architecture: **archive-before-confirm** — block bodies land in Class A as peers
deliver them; tip **confirm** (Class C) walks contiguous archived runs. Download
defaults to **1024** concurrent getdata (not a tip-distance cap), max **16** blocks
in transit per peer.

## Build

```bash
nix-shell
cargo build -p rbitcoin-node --release
./target/release/rbitcoin-node --help
```

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

## Defaults and memory budgets

| Knob | Default | Override |
|------|---------|----------|
| IBD concurrent getdata | **1024** | code `IbdConfig::window` |
| Blocks in transit / peer | **16** | `IbdConfig::per_peer` |
| Live IBD peers | **16** | `--max-outbound` |
| Milestone (skip scripts ≤ height) | mainnet **840000**, signet 2000000, … | `--milestone` (`0` = full scripts) |
| Archive queue RAM | **256 MiB** | `RBITCOIN_ARCHIVE_QUEUE_MB` |
| Class A working-set cache | **256 MiB** | `RBITCOIN_CLASS_A_CACHE_MB` |
| Light UTXO (mmap) | **~96 MiB start** (24 B slots; grows) | `RBITCOIN_IBD_UTXO_SLOTS` |
| Parent prewarm depth / batch / headroom | **256 / 64 / 64** | `RBITCOIN_PARENT_PREWARM_DEPTH` / `_BATCH` / `_HEADROOM` |
| Confirm wait for prewarm worker before last-mile | **1500 ms** | `RBITCOIN_PREWARM_WORKER_GRACE_MS` (`0` = last-mile immediately if not ready; use `0` in unit tests) |
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
after Class C (no light UTXO). Scripthash is **not** progressively materialized:
confirm only enqueues sorted runs (background flush + merge); lead hysteresis
does **not** pause peer fetch/archive. At tip the node **merges remaining runs
and cold bulk-loads** durable SH tables (migration-style) before Electrum. On enter Direct, leftover `ibd_utxo.map` is removed and point/tx
runs are materialized — prefer a **fresh datadir**. **`IndexMode::Catchup`**
(runs + light UTXO + progressive mat) remains available for tests.

**Catch-up runs → open-hash (hysteresis):** memtable spills **small sorted runs
with no mid-IBD merge**. When `arch − tip ≥ 65536`, **new peer getdata stops**
(the Class A writer keeps draining already-queued work). After **inflight
downloads hit 0**, an idle-IO worker materializes **one run at a time** into
durable heads (point → tx → SH), with **at most one shard rehash at a time** and
a short pause between shards. When lead falls below `32768`, fetches resume and
materialize stops until lead rebuilds. When archive has caught the peer tip,
materialize continues until runs are empty (once inflight is clear). Progress:
`runs t=/p=/sh= mat=fetch|drain|materialize/pause_fetch=…`. Env:

| Env | Default | Meaning |
|-----|---------|---------|
| `RBITCOIN_RUN_MATERIALIZE_START_LEAD` | `65536` | Stop peer fetch / arm materialize; `0` = off |
| `RBITCOIN_RUN_MATERIALIZE_STOP_LEAD` | `32768` | Resume peer fetch / pause materialize |
| `RBITCOIN_HEAD_SHARD_PACE_MS` | `25` | Sleep between shards during paced head insert |

New stores (schema **v8**): **header.head 256 shards**, **scripthash 16 shards**,
**tx.head** = single fixed address file (~**16 GiB** sparse mainnet, `2^31` × 8 B
keyless entries; override width with `RBITCOIN_TX_HEAD_BITS` / tiny scale in tests).
v7 sharded `tx.head/` directories are not readable — wipe / fresh IBD. Spends are
schema-v5 annotations on create outputs (no `point.head`).

**Memory rule:** During Catchup, durable open-hash heads are off. Class A is
packed full-tx bodies; inputs always store **external** `prev_txid`. Parent
resolve uses light UTXO (`outpoint → create_fk`); Tip uses spend annotations /
`tx.head`. SH create dedupe is an **O(1) height watermark**. Do not raise
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
