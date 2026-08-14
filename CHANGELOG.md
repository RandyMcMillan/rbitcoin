# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
for the **0.x** experimental line (breaking on-disk and API changes are expected
before 1.0).

## [Unreleased]

### Fixed

- **Head resolve 2-wave:** wave 1 is open + sealed ages ≤3 again. The spend-only
  DONTCACHE change had made `head_or_idx_segment_index` always false, so hot
  probed every segment and cold was empty. Unconnected hot hits still run
  wave 2 so `TipThenAny` / `TipOnly` can take a connected sibling in age ≥4.

- **Tests:** scripts-phase steal-worker pin records the coordinator thread on
  the handle (not a process-global name). Archive plan/commit wall stats sample
  under an exclusive lock so parallel `sample_and_reset` cannot steal the
  window.

### Changed

- **Lookup stamp:** consult live `PipelineParentStore` by prev_txid before
  `tx.head` (`pin_txid=` / `pin_txid%` / `pin_txid_ms` / `head_n` /
  `us/pin_txid` on `ibd: perf`). Remaining head `txout.idx` fills are
  page-grouped on the held resolve session. `pin_hit%` is adopt/plan
  reuse only (this-window range-fills stay `pin_new`).

- **Schema 16:** drop `tx_height.body` (~5 GiB). Create height is a resident
  fence from `confirmed[]` + `header_txs_*` (O(blocks), RAM bsearch). Reorg
  holes return unconnected. Schema 15 stores soft-open (unlink leftover file).
  Old binaries refuse 16 (they still write `tx_height`).

- **Script pool:** `try_for_each_parallel` steals on process-wide
  `rbtc-scripts-*` workers (no per-batch `thread::scope`). Confirm phases run
  on two `rbtc-script-coord-*` threads so a steal worker is not blocked inside
  the phase. Pool wait uses a condvar deque (not `recv` under mutex).

- **`--sptweaks` during IBD:** Direct confirm no longer write-throughs the
  thin BIP-352 index (it was 50–80% of fat-era write). After tip, SH
  materialize (if `--shindex`) then a sequential backfill to live tip;
  Tip write-through only when `height == next_height`. Restart resumes
  from `next_height`.

- **Schema 15 Class A split:** `txout.body` (outs) + `inwit.body` (ins+witness)
  + `spent.body` (9 B×n_out). Packed `tx.body` with creates is refused. Pin/SH
  read outs only; annotate RMW is `spent_off+9×vout`. Working-set census in
  [`SCHEMA.md`](./SCHEMA.md).
- **Schema 15 Class B SH:** geometric slabs + megakey pages; sealed
  sorted+idx main (**no** main fuse); global ingest OA; sealed ovf keeps
  fuse8. Tip lookup is overflow (ingest + ovf fuse) then main. Open
  rematerialized SHSR shards via an OA stub; sealed ovf files are not
  opened as OA. Unlink writes the home `locate_head` found. Cold bulk
  streams packed recs (no per-shard OA image). Page-era durable SH is
  refused. The OA global `scripthash.head.fuse8` builder is gone.
- **Electrum / RPC:** skip O(mempool) API walks; overlap Electrum dispatch;
  thin `--sptweaks` serve is idx→body uring, not a packed span.
- **Electrum `server.version`:** first element is `rbitcoin-electrs <ver>` so
  Cake Wallet’s `getNodeIsElectrs()` will probe `blockchain.tweaks.subscribe`.
- **CLI-first config:** `--maxinbound`/`--maxconnections`, `--archive-queue-mb`,
  `--conf`, Core-like aliases (`--assumevalid-height`, `--maxmempool`, `--chain`).
- **Tip-follow logging:** every accepted tip block logs Core-like `UpdateTip: …`.
- **Fee snapshot / mempool APIs:** published fee table and mining chunks so
  Electrum/Esplora estimates do not block accepts (R-01–R-04).
- **Quality gates:** `cargo deny` on PR (Q-20); coverage uses prebuilt
  `cargo-llvm-cov` (Q-22); `scripts/sbom.sh` emits CycloneDX from Cargo.lock.

### Fixed

- **Findings 012–021** (fuzzamoto differential): identity/BIP30 cluster,
  tapleaf, compact-block, reorg drain — all closed with named regressions.
- **Mainnet BIP30:** skip the two Core `IsBIP30Repeat` overwrites (91842 /
  91880 hashes). Those coinbases were overwritten while still unspent, not
  fully spent. IBD `bad-txns-BIP30` at logged `@91859` was the first height
  of a write batch that contained 91880.
- **Electrum tweaks subscribe:** stream remaining heights as notifications
  and finish with Cake’s `{"message":"done"}`. A one-shot 8-height result left
  the scan isolate idle after `[restore, remaining, false]`.
- **Electrum `get_balance`:** unconfirmed delta uses the mempool scripthash
  index instead of store-resolving every live chain input. Empty Cake keys were
  ~1.5 s each on a mainnet mempool.

### Added

- **IBD write meters:** `tweaks=` on `ibd: perf` / `perf_dbg` and `confirm write slow`
  (BIP-352 index wall after spend annotate). Makes the `--sptweaks` write-thread
  cost visible in the fat-era IBD hole.

- **`--sptweaks`:** optional thin BIP-352 index (`sp_tweaks.idx` / `.body`).
  Persist is `len:tweak` only (0 or 33-byte compressed `A_tweak`). Cake outs
  join `txout`. Confirm appends; reorg truncates; background backfill.
  Electrum still serves naive when the flag is off or a height is a hole.

## [0.1.0] — 2026-07-26

### Experimental first public packaging

Initial **0.x** packaging of an experimental Bitcoin full node in Rust:

- Multi-peer IBD and tip follow over **BIP324 v2-only** P2P
- Relational Class A/B/C archive (reconstruct historical blocks; tip wire ring + tip durability after catch-up; store later fully map-free — see `docs/io-modality.md`)
- **Pure-Rust** consensus/script path (secp256k1 via rust-bitcoin only; no libbitcoinconsensus dual-eval)
- Confirm pipeline (load / scripts / write), Direct index mode during IBD, native scripthash + in-process **Electrum** after tip
- Libre-class mempool admission with script checks on accept; BIP152 v2 compact blocks and BIP339 wtxid relay on tip sessions
- Operator docs for **signet lab first** and **experimental mainnet** (default milestone skips scripts ≤ 840000)

### Documentation

- Architecture overview for unique store / IO / consensus design (`docs/architecture.md`)
- Security policy (`SECURITY.md`), this changelog, dual MIT OR Apache-2.0 licenses

### Notes

- On-disk schema is **unstable until 1.0** (reindex on incompatible changes).
- Completing a full mainnet IBD on an operator host is **out of band** for this
  release packaging; experimental mainnet remains lab-only.
- Workspace package metadata does not claim a public `repository` URL until one
  is published.
