# Algorithms & data structures — open findings

Crate-by-crate pass 2026-08-25 against production sources, judged against
[`concurrency.md`](./concurrency.md), [`invariants.md`](./invariants.md),
[`ibd-memory.md`](./ibd-memory.md), [`SCHEMA.md`](../SCHEMA.md),
[`COMPAT.md`](../COMPAT.md). This file is the **open** list. Close an item
in the same PR as the fix by **deleting** its row here. Do not grow a Closed
graveyard; landed behavior lives in owner docs / [`CHANGELOG.md`](../CHANGELOG.md).
Do not copy these tables into [`quality.md`](./quality.md).

Deliberate design (lock-free roles, RAM-only body queue, purpose-built
io_uring machines, Frozen→ArcSwap pins, leftover maps as `txid → one fk`)
is not a finding. Items already in `quality.md`, `errata.md`, or external
findings 001–023 are excluded unless the code still diverges.

Severity: **High** = wrong result / consensus split / production DoS;
**Medium** = wrong under a plausible schedule, or measurably slow at mainnet
scale; **Low** = edge, fragile, or minor waste.

---

## 1. Algorithm / data-structure inventory

What the node runs. Findings follow.

### Store (`rbitcoin-store`)

| Piece | Algorithm | Notes |
|-------|-----------|--------|
| `TableFile` | fallocate + published HWM (Release) | body → idx → count/HWM; readers use HWM only |
| `VarTable` / `ArrayTable` | append / dense slots + L2 write-behind | Class A/C. Seqlock on `(count, body_end)` |
| `HashHead` | 24-byte OA, 16-byte prefix key, linear probe | load 7/8; insert full → sibling gen (`header.head.gN`) or Corrupt. Open-only rewrite of undersized single gen. No occupied rewrite while serving |
| `ScriptHashHead` | 32-byte OA ingest | seals at 0.80; no occupied rewrite while serving |
| `AddressHead` | 4 KiB pages of relative fks | torn 4-byte writes accepted; body-txid verifies |
| Sealed `tx.head` | BDZ MPHF + fuse8 filter | FdOnly `g`; fingerprints in RAM |
| `binary_fuse8` | 3-hash XOR fingerprint | `contains` indexes fingerprints unchecked |
| `HeightFence` | sorted run vec behind `Arc` | leftover snapshot is O(1); extend/pop COW |
| Spender overflow | linked list of `next` fks | no cycle bound |
| SH pages | 4 KiB delta pages, `next` pointer | zero page = terminator |
| `sorted_run` | LSM-style runs + manifest | `runs_io` mutex is a comment, not a type |
| `BlockQueue` | FIFO + `height → id` first-wins | RAM-only by design |
| `U64IdentityHasher` | identity hash of fk | sequential fks spread; 7-bit control tag clusters |
| io_uring machines | multi-stage SQE pipelines | idx-body, spend annotate, SH, sp_tweaks |

### Query / confirm pipeline (`rbitcoin-query`, `rbitcoin-consensus`)

| Piece | Algorithm | Notes |
|-------|-----------|--------|
| Stamp | in-flight → live_union → RecentCreates → TipOnly | miss is permanent |
| `LiveUnion` | ArcSwap chain of identity layers | get walks; splice keep by BQ / taken / horizon |
| `RecentCreates` | height-FIFO + ArcSwap head + pending overlay | write-published; load-then-store (sole mutator unenforced) |
| `SharedParentPin` | Frozen `OnceLock` → `ArcSwap` RCU | compose-only; no in-place mutation |
| `PinOuts` / `ParentLayout` | sorted sparse vecs, `binary_search` | `covers_need` = examined, not necessarily live |
| `TxidHasher` | last 8-byte write wins | sound only for bare `[u8;32]` |
| `OutPointHasher` | FNV-ish mix every write | `pending_spent` |
| `TxPrecompute` | one-pass txid/wtxid/midstates | lookup publishes; scripts reuse |
| Confirm queues | loadq=14, scriptq=4, writeq=14 | bounded sync channels |
| Script pool | lock-free steal waves | `in_wave` + `failed` atomics (Acquire/Release) |
| Merkle | Bitcoin duplicate-last | CVE-2012-2459 covered by whole-block txid set |
| Difficulty | Core retarget + 4× clamps | testnet 20-min min-diff |
| MTP | 11-header median | fence-tip is an 11-slot ring; historical heights still walk |

### Script engine

| Piece | Algorithm | Notes |
|-------|-----------|--------|
| `eval_script` | Core-shaped opcode walk | tapscript OP_SUCCESS pre-scan, 520 B stack |
| CHECKSIG | DER lax + optional BIP66; Schnorr BIP340 | BIP342 validation-weight budget |
| P2SH | `eval_script` scriptSig then IsPushOnly | last stack item is redeem |
| Sigops | `script_sigop_count` byte walk | truncated PUSHDATA2/4 stops (Core `GetOp`) |
| Signet | challenge script + witness commitment | last exact 38-byte BIP141 |

### Net / IBD (`rbitcoin-net`)

| Piece | Algorithm | Notes |
|-------|-----------|--------|
| Headers sync | one `sync_started` peer | 50 ms session tick → `on_session_heartbeat` → stall timeout |
| Inbound handshake | 60 s VERSION/VERACK bound | `inbound_connect_and_handshake`; drops the `max_inbound` permit |
| IBD assign | densify walk, `FAR_SCAN_BUDGET` 65 536 | `densify_scan_lo` skips BQ-ready prefix; request-limited soft budget |
| Compact blocks | short-id + prefilled interleave | prefilled index validation weaker than Core |
| AddrMan | unbounded `HashMap` | no tried/new bucket caps |
| Tx relay | graph + per-peer announced set | `HashMap<Wtxid, Txid>` on `TxGraph`; `relay_seq`/`accept_at` dropped in `unindex_txid` |
| Chain work | RAM prefix `work through h` | rebuilt from wire headers; no durable `nChainWork` |
| `BlockCache` | `Vec` of hashes | prefix eviction O(chain) |
| Peer rate limit | **fixed** 1 s window | documented as sliding |
| Eviction of pending/held | `HashMap::keys().next()` | random, not FIFO |
| v2 decode | command + payload | no sha256d checksum / v1 reframe |

### Mempool / RPC / wallet APIs

| Piece | Algorithm | Notes |
|-------|-----------|--------|
| Mempool graph | clusters + linearization + chunks | eviction = first `(rate, rep)` map entry |
| Orphanage | count + weight FIFO | no 20-minute expiry |
| Mempool store | slot file + body image | `persist_all` rewrites whole files; body after slots |
| Fee estimator | log buckets | |
| `testmempoolaccept` | `MempoolHub::test_accept` | prepare + scripts + RBF/cluster; no commit |
| RPC `active` | id-keyed map | `dispatch` removes by id |
| Electrum status | `sha256(concat rows)` | confirmed rows include **blockhash** (COMPAT A-B-A); mempool appended last (same as `get_history`) |
| Esplora `/blocks` | reconstruct 10 full blocks | for size/weight only |
| SH join cache | process-wide single slot | serializes clients |

### Node / primitives

| Piece | Algorithm | Notes |
|-------|-----------|--------|
| CLI / conf | hand-rolled parsers | `milestone=0` in conf indistinguishable from unset |
| Log | global level, no module filter | `format_args` not gated |
| `script_sigop_count` | opcode walk | truncated PUSHDATA2/4 stops (Core `GetOp`) |

---

## 2. Highest-priority findings

*(none remaining — Electrum status extra `blockhash` is COMPAT, not High.)*

---

## 3. Medium findings (correctness / durability)

### Store

- **S-M1.** `VarTable::published_meta` seqlock (`var_table.rs`): data loads
  are `Relaxed` with no fence before the second seq load. x86 TSO hides it;
  ARM can tear `(count, end)` → spurious `Corrupt("record range")`. Fix:
  `Acquire` fence after data, or load data `Acquire`.
- **S-M2.** `ArrayTable::flush_dirty` / `StrongTxTable::flush_dirty`: snapshot
  under read lock, drop, write, then clear `dirty`. A `set` in the window is
  lost until some later set re-dirties. Roles intend a single Class C
  writer, but flush can be a sidecar. Strong-bit miss recreates the
  "unstrong tip txs" case `store.rs` comments guard. Fix: `swap` dirty false
  **then** snapshot.
- **S-M3.** Fuse8 `decode_body` (`fuse8_filter.rs`) does not check
  `segment_count_length + 2*segment_length ≤ fp_len`. `BinaryFuse8::contains`
  indexes fingerprints unchecked → panic on corrupt-but-decodable sidecar
  instead of `NeedsRewrite`.
- **S-M4.** `for_each_spender_create` (`point_table.rs`): overflow `next`
  walk has no cycle bound. Bit-rot loops the query thread. Bound by
  `spenders.count()`.
- **S-M5.** Sidecar `std::fs::write` + `rename` without `sync_all` (segment
  meta, `.mphf`, SH `.idx`). Crash can publish empty metadata. Open-time
  `Corrupt` is the recovery — against the "no silent wipe, no surprise
  repair" policy.
- **S-M6.** `sorted_run` orphan GC requires every caller to hold `runs_io`
  (comment only). List without the lock during "file written, manifest not
  yet updated" deletes a live run.

### Consensus

*(none remaining)*

### Query

- **Q-M1.** `retain_headers_needing_body` (`archive.rs`): missing `first`
  fk → `unwrap_or(0)` keeps the wrong span. Should be
  `Corrupt("invariant: …")`.
- **Q-M2.** `RecentCreates` head is load-then-store, not RCU
  (`recent_creates.rs`). Safe only if the write thread is the sole mutator
  — unenforced. `drop_from` is on the reorg path.
- **Q-M3.** `merge_outs` clones script bytes on every RCU retry; empty
  `checked` always publishes a new Arc (breaks `ptr_eq` / sticky).

### Net / mempool / RPC / esplora

- **N-M1.** Compact-block prefilled indexes: weaker than Core's strictly
  increasing + in-bounds check; short-id cursor can misalign (`compact.rs`).
- **N-M2.** `HashMap::keys().next()` eviction for pending blocks, held
  bodies, fork-tips — random under cap. `hold_body` already has `held_seq`
  unused for eviction.
- **N-M3.** `cmpct_fills` and `requested_blocks` grow until success/arrival;
  no prune on abandon/timeout.
- **N-M5.** AddrMan unbounded; Core caps tried/new.
- **N-M6.** `announced_wtx` cap = clear entire set → INV burst.
- **N-M7.** `disconnect_to` clones all disconnected txs even with no mempool
  (IBD reorg RAM). `invalidate_block` / `note_confirmed_tip` reconstruct
  full blocks when `header_at_height` would suffice for hashes.
- **M-M1.** Known-parent out-of-range vout parked as orphan forever
  (`accept.rs`). Hard-reject.
- **M-M2.** Eviction `worst_chunk` on rate **tie** can pick an earlier chunk
  and strand descendants (no `removeRecursive`).
- **M-M3.** `evict_to_budget` `removed` is cumulative; later no-op iteration
  does not break → spin.
- **M-M4.** `persist_all` writes meta, then slots, then body — inverted vs
  body-before-claim. Crash → LIVE slot, garbage body.
- **M-M5.** `accept_package` has no package feerate (0-fee CPFP parent
  rejected). Name vs Core `submitpackage`.
- **R-M1.** `submitblock` gated to regtest; help / [`rpc.md`](./rpc.md) say
  all networks.
- **R-M2.** `gettxout include_mempool` does not hide mempool-spent confirmed
  outs (`spends_outpoint` exists).
- **R-M3.** `getnetworkhashps` assumes ~2 hashes/block (regtest).
- **R-M4.** Unbounded JSON-RPC batch under one work permit.
- **R-M5.** `getmininginfo` `blockmintxfee` is a sat/kvB feerate passed
  through `json_btc_amount` (amount helper). Use a feerate formatter.
- **R-M6.** `sendrawtransaction` / `submitpackage` accept `maxfeerate` /
  `maxburnamount` and ignore them. Enforce or reject the params.
- **E-M1.** `scripthash_mempool_stats` undercounts chained unconfirmed vs
  Esplora.
- **X-M1.** Esplora mempool tx JSON stubs omit vin/vout/size/weight when the
  tx is not in Class A (`handlers.rs`). BDK-class clients fail to parse.
  Wire tx is in `mp.get_tx`.
- **X-M2.** Esplora WS does store IO on the tokio thread (`ws.rs` `on_tip` /
  `on_mempool_announce`). REST uses `spawn_blocking`.
- **X-M3.** Process-wide single `sh_join` slot: concurrent clients serialize
  and evict each other.
- **O-M1.** Conf `milestone=0` overwritten by network default (`cli.rs`).
  CLI `--milestone 0` works; conf cannot disable skip.
- **O-M2.** `--minrelaytxfee` parse failure silently ignored; negatives → 0.
- **O-M3.** Tip-follow stale redial uses AddrMan cloned once after IBD.

---

## 4. Performance (big-O, allocation, IO)

Grouped by cost at mainnet scale. Many overlap §3.

| ID | Where | Cost | Standard replacement |
|----|--------|------|----------------------|
| P4 | BDZ `index_batch_fd` fill | O(pages × keys) | pre-group by page |
| P5 | `HashHead::bulk_fill_empty` | full table in RAM + dense write | dirty 4 KiB pages only |
| P6 | SH `insert_many` `entries.iter().find` | O(N²) ingest | `HashMap` once |
| P9 | `queue_due_tx_invs` | O(peers × mempool), 1 Inv/tx | batch `Inv`; iterate `relay_seq` delta |
| P11 | Esplora `/blocks` | 10 full reconstructs / page | persist size/weight at connect |
| P12 | Electrum status | 1 header read / history row / notify | cache confirmed-prefix hash |
| P13 | `find_free_slot` | O(cap) per admit, O(cap²) fill | free-list `VecDeque` |
| P14 | `persist_all` | rewrite entire mempool files | append body + patch slot |
| P15 | `evict_nonfinal` | O(n²) per reorg | worklist of affected txs |
| P18 | BQ `dequeue` height remap | O(queue) `index.iter().find` | `HashMap<u32, VecDeque<id>>` |
| P19 | `BlockCache` prefix drop | O(chain) per connect | `VecDeque` + base height |
| P20 | GBT `depends` | O(txs × inputs) linear scan | `HashMap<Txid, idx>` |
| P21 | Log macros | eval args + TLS even if disabled | `if enabled { … }` in macro |
| P22 | `api_log` global mutex + `write_all` | blocks async workers | dedicated writer thread |
| P23 | Esplora WS `scripts_touched_full` | store/mempool IO per announced input per conn | intersect `ann.scripthashes` |

Low (still real): `StrongTxTable::count_ones_bits` bit-by-bit vs
`u8::count_ones`; `PendingHead::queued_fk` reverse scan of 262 144;
per-slot pread in occupied walks; AddressHead probe copies of 32-byte keys;
`InFlightLayer.creates` SipHash vs `TxidHasher`; SH `Vec::contains` dedup;
`check_libre_annex` clones the witness; `find_and_delete_sig` always
allocates; one-shot confirm `block.clone()`; seeds `Vec::contains`;
GBT longpoll parks a blocking thread with no timeout.

---

## 5. Memory

| ID | Where | Shape | Cap today? |
|----|--------|--------|------------|
| Mem2 | AddrMan | peer `addr` feed | **no** |
| Mem3 | `announced_wtx` | 50k then **clear all** | yes, but bursty |
| Mem4 | Mempool dead body bytes | RBF stall, compact not on admit | disk + rewrite |
| Mem5 | `bulk_fill_empty` | transient hundreds of MiB | per grow |
| Mem6 | IBD `disconnect_to` | whole losing branch cloned | skip if no mempool |
| Mem7 | `--api-log` JSONL | unbounded | **no** |
| Mem8 | Orphanage / Electrum subs / WS tracks | count/weight / `max_*` | yes |
| Mem11 | `TX_FULL_GETS` thread-local | prod `Vec` push per get | test-shaped leak |

Body queue and RecentCreates pins are request-/horizon-limited **by design**
([`ibd-memory.md`](./ibd-memory.md)). [`errata.md`](./errata.md) leftover
maps (`txid → one fk`) stay as-is (Won't-fix unless a mainnet miss).

---

## 6. Simplifications (ad-hoc → standard)

Do not flatten io_uring machines. Do not add a process pin FIFO.

1. **Mempool slots:** free list (P13).
2. **Block queue / densify:** keyed multimaps instead of `iter().find`.
3. **Held/pending eviction:** FIFO via existing `held_seq` / `VecDeque`.
4. **Announced-tx sets:** rolling bloom (Core `CRollingBloomFilter`).
5. **BlockCache:** `VecDeque` + height offset.
6. **Esplora `sh_join`:** small LRU, not one global slot.
7. **Esplora `/blocks`:** stored summary, not reconstruct.
8. **`last_push_data`:** `Script::instructions()` (gets PUSHDATA4).
9. **`U64IdentityHasher`:** multiply by odd golden-ratio constant if
    hashbrown clustering shows.
10. **Seqlock:** fences, or stop rolling your own for a 16-byte pair.
11. **CLI parsers:** table-driven `take_parsed` (node + bench).
12. **Bench hex:** use `rbitcoin_primitives::hex_*`.
13. **Bit count:** `u8::count_ones`.
14. **SH `put_sorted_creates` `seen`:** `put_chain` already sorts+dedups.

---

## 7. Gotchas / fragile invariants (not bugs today)

- Stamp / leftover-union miss is **permanent** — load bugs must not invent a
  spentness fallback (`docs/invariants.md`).
- `PinOuts::covers_need` means *examined*, including proven-absent vouts.
- Assemble sticky can miss a peer-batch widen (perf only; immutable snaps).
- `TxidHasher` last-write-wins: never put it on `(txid, vout)`.
- `AddressHead` torn 4-byte slots are OK only because body-txid verifies.
- `eval_script` `Ok(false)` discarded in `verify_bare` — load-bearing
  (tapscript OP_SUCCESS only).
- Store `Corrupt` string-matched to `MissingPrevout` in consensus `error.rs`.
- Future-time uses wall clock, not Core adjusted time.
- `PeerRateLimiter` is a fixed window (2× burst at boundary).
- Compact first-wins BQ + hash-checked claim mutates the queue from
  read-shaped helpers (`assign.rs`).
- Log `RUST_LOG=info,hyper=trace` sets **global** Trace.
- `spawn_sptweaks_backfill` process-global `Once` captures first `query`.
- Signal handler installed after store open.
- Electrum/Esplora `Lagged` broadcast: missed notifies never resync
  (COMPAT says best-effort).
- Mempool `commit_after_script` trusts the tip captured at `prepare_admit`;
  a reorg between prepare and commit relies on the caller running
  `evict_nonfinal`.

---

## 8. Checked and found sound

- Publish order body → idx → count/HWM on Class A; readers use HWM.
- Confirm pipeline: bounded downstream channels; no cycle; write condvar has
  a timeout.
- CVE-2012-2459: whole-block duplicate-txid set.
- Difficulty 4× clamps and `first_block_time` off-by-one match Core
  (mainnet). Testnet 20-minute min-diff walk-back matches Core.
- MTP median index matches Core `GetMedianTimePast`.
- BIP30 exception blocks 91842/91880 present.
- Duplicate inputs via `OutPointSet`; weight/MAX_MONEY saturating then range.
- CLTV/CSV 5-byte scriptnum + type match; tapscript OP_SUCCESS, MINIMALIF,
  520 B initial stack, annex, sighash midstate reuse; P2WSH 520 B; BIP342
  validation-weight budget.
- P2SH scriptSig is `eval_script` + IsPushOnly; truncated PUSHDATA2/4 does
  not count leftover CHECKSIG; witness sigops gated on segwit.
- Same-block spends = pending only; spender height before create = unspent.
- `PinHalf` Frozen→ArcSwap: `OnceLock::set` loser re-applies `f` — no lost
  compose.
- Stamp priority matches `docs/invariants.md`.
- Soft densify is request-limited only.
- Merkle odd-layer duplicate-last.
- `TxPrecompute` vs rust-bitcoin oracles (incl. zero-input BIP144).
- Body-queue budget accounting matches `docs/ibd-memory.md`.
- Orphanage count/weight caps; Electrum/WS per-conn caps.
- `unindex_txid` drops scripthash index, unbroadcast, `relay_seq`, and
  `accept_at`.

---

## 9. Per-crate finding index

Counts are unique **open** items in this document.
Intentional COMPAT Electrum status extra field is **not** counted as High.

| Crate | High | Medium | Perf/Mem notable |
|-------|------|--------|------------------|
| store | 0 | seqlock, flush lost-update, fuse8 OOB, spender cycle, sidecar fsync, runs_io | BDZ fill, bulk_fill, SH N² |
| consensus + primitives | 0 | *(none remaining)* | historical MTP walks, rehash txids |
| query | 0 | retain fallback, RecentCreates CAS, merge_outs clone | BQ scan, SipHash in-flight |
| net | 0 | compact indexes, random eviction, AddrMan/cmpct unbounded, v2 copies | INV flush, BlockCache |
| mempool | 0 | orphan vout, eviction tie, persist order, package feerate | free slot, persist_all |
| rpc | 0 | submitblock gate, gettxout, hashps, unbounded batch, blockmintxfee, maxfeerate | GBT depends, longpoll |
| electrum | 0 | mempool_stats | status full-history, announce O(subs) |
| esplora | 0 | stub mempool JSON, WS on runtime, sh_join slot | `/blocks` reconstruct, WS announce IO |
| node/cli/log/bench | 0 | milestone=0 conf, minrelay silent, frozen AddrMan | log gating, api_log mutex |

---

## 10. Suggested work order

Not a plan (no red/green steps). Split-risk, then operator-visible, then
IBD CPU.

1. Mempool persist order, AddrMan cap.
2. Esplora `/blocks` summaries (P11) and mempool JSON stubs (X-M1).

Out of scope (Won't-fix / policy): flattening uring, process pin FIFO,
leftover `Vec<Fk>`, explorer APIs, `rbitcoin-bench` in required CI.
Dropped from this list as too small or already owner-doc: ping RTT in
whole seconds, bench Electrum response pairing, CLI `--` for negative
RPC args, chunked HTTP in `rbitcoin-cli`.
