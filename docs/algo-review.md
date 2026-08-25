# Algorithms & data structures review — whole repo

Date: 2026-08-25. Snapshot against `master` @ `eceb9c7e` (post-#234); landed
on tree independently of later merges. ~174k first-party Rust LOC across 14
crates. Analysis only at write time.

**Owner of these findings:** this file. Close an item in the same PR as the
fix (delete or move to § Closed). Do not copy the tables into
[`quality.md`](./quality.md).

Method: production sources read crate-by-crate, judged against
`docs/concurrency.md`, `docs/invariants.md`, `docs/ibd-memory.md`,
`SCHEMA.md`, `COMPAT.md`. Deliberate design (lock-free roles, RAM-only body
queue, purpose-built io_uring machines, Frozen→ArcSwap pins, leftover maps as
`txid → one fk`) is not flagged as a bug. Items already in `docs/quality.md`,
`docs/errata.md`, or fixed findings 001–023 are excluded unless the code still
diverges.

Severity: **High** = wrong result / consensus split / production DoS;
**Medium** = wrong under a plausible schedule, or measurably slow at mainnet
scale; **Low** = edge, fragile, or minor waste. Items marked ✔ were
re-verified against the source during synthesis.

---

## 1. Algorithm / data-structure inventory

What the node actually runs. This is the map; findings follow.

### Store (`rbitcoin-store`)

| Piece | Algorithm | Notes |
|-------|-----------|--------|
| `TableFile` | fallocate + published HWM (Release) | body → idx → count/HWM; readers use HWM only |
| `VarTable` / `ArrayTable` | append / dense slots + L2 write-behind | Class A/C. Seqlock on `(count, body_end)` |
| `HashHead` | 24-byte OA, 16-byte prefix key, linear probe | load 7/8; insert full → sibling gen (`header.head.gN`) or Corrupt. Open-only rewrite of undersized single gen |
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
| `RecentCreates` | height-FIFO + ArcSwap head + pending overlay | EWMA clamp `[32, 32×144]` |
| `SharedParentPin` | Frozen `OnceLock` → `ArcSwap` RCU | compose-only; no in-place mutation |
| `PinOuts` / `ParentLayout` | sorted sparse vecs, `binary_search` | `covers_need` = examined, not necessarily live |
| `TxidHasher` | last 8-byte write wins | sound only for bare `[u8;32]` |
| `OutPointHasher` | FNV-ish mix every write | `pending_spent` |
| `TxPrecompute` | one-pass txid/wtxid/midstates | lookup publishes; scripts reuse |
| Confirm queues | loadq=14, scriptq=4, writeq=14 | bounded sync channels |
| Script pool | lock-free steal waves | `in_wave` + `failed` atomics |
| Merkle | Bitcoin duplicate-last | CVE-2012-2459 covered by whole-block txid set |
| Difficulty | Core retarget + 4× clamps | testnet 20-min min-diff |
| MTP | 11-header median | store walk per header |

### Script engine

| Piece | Algorithm | Notes |
|-------|-----------|--------|
| `eval_script` | Core-shaped opcode walk | tapscript OP_SUCCESS pre-scan, 520 B stack |
| CHECKSIG | DER lax + optional BIP66; Schnorr BIP340 | BIP342 validation-weight budget |
| P2SH | `eval_script` scriptSig then IsPushOnly | last stack item is redeem |
| Sigops | `script_sigop_count` byte walk | truncated PUSHDATA2/4 stops (Core `GetOp`) |
| Signet | challenge script + witness commitment | first-match prefix, not last 38-byte BIP141 |

### Net / IBD (`rbitcoin-net`)

| Piece | Algorithm | Notes |
|-------|-----------|--------|
| Headers sync | one `sync_started` peer | 50 ms session tick polls `on_session_heartbeat` |
| Inbound handshake | 60 s VERSION/VERACK bound | `inbound_connect_and_handshake`; drops the `max_inbound` permit |
| IBD assign | densify walk, `FAR_SCAN_BUDGET` 65 536 | request-limited soft budget |
| Compact blocks | short-id + prefilled interleave | prefilled index validation weaker than Core |
| AddrMan | unbounded `HashMap` | no tried/new bucket caps |
| Tx relay | graph + per-peer announced set | wtxid map on `TxGraph`; `relay_seq`/`accept_at` not unindexed |
| Chain work | RAM prefix `work through h` | rebuilt from wire headers; no durable `nChainWork` |
| `BlockCache` | `Vec` of hashes | prefix eviction O(chain) |
| Peer rate limit | **fixed** 1 s window | documented as sliding |
| Eviction of pending/held | `HashMap::keys().next()` | random, not FIFO |

### Mempool / RPC / wallet APIs

| Piece | Algorithm | Notes |
|-------|-----------|--------|
| Mempool graph | clusters + linearization + chunks | eviction = min-rate chunk from `(rate, rep)` index |
| Orphanage | count + weight FIFO | no 20-minute expiry |
| Mempool store | slot file + body image | `persist_all` rewrites whole files; body after slots |
| Fee estimator | log buckets | |
| Electrum status | `sha256(concat rows)` | confirmed rows include **blockhash** (documented in COMPAT for A-B-A) |
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

These are the ones worth a plan. ✔ = re-read against current source.

### High — mempool / RPC / Electrum (product-visible)

#### M-H1. ✔ `testmempoolaccept` really accepts, then only evicts the trial tx

`crates/rbitcoin-rpc/src/methods.rs:1540-1550`. Comment admits
best-effort rollback. `accept_tx` runs the live path: RBF **actually evicts**
conflicts inside `commit_after_script`. Rollback is
`evict_live_txids(&[trial])` — replaced txs stay gone, and an announce may
already have fired.

**Fix:** dry-run in `MempoolAccept` (prepare + scripts + RBF checks, no
commit), or snapshot/restore the conflict set.

#### M-H2. ✔ RPC `dispatch` `active` stack pops the wrong entry

`crates/rbitcoin-rpc/src/methods.rs:298-309`. Shared `Vec` push/pop across
concurrent `spawn_blocking` threads. Thread A finishes and `pop()`s B's
entry. `getrpcinfo` lies; durations pair with the wrong start.

**Fix:** id-keyed map / slab; remove by id.

#### M-H3. Electrum status preimage vs protocol — **documented, not a silent bug**

`crates/rbitcoin-electrum/src/server.rs:1692-1744` concatenates
`txid:height:blockhash:` for confirmed rows. Electrum spec is
`txid:height:` only. **COMPAT.md** records this as intentional A-B-A
protection (same-height reorg). Conformant clients that recompute status
from `get_history` will never match and will refetch. That is a product
choice, not an accidental bug — but it **does** break vanilla Electrum
desktop sync unless clients honor the extra field or ignore mismatch.

Separately, `scripthash_status_full_slot` `sort_by_key(|i| i.height)` puts
mempool rows (0 / −1) **before** confirmed. That disagrees with this
server's own `get_history` (mempool appended last) and with the protocol.
That half **is** a bug even given COMPAT.

**Fix (ordering):** reuse `get_history` assembly for the preimage.
**Fix (preimage):** either drop blockhash from the hash (keep it in
`chain_tip` JSON only) or keep COMPAT and document that vanilla clients
will loop.

---

## 3. Medium findings (correctness / durability)

### Store

- **S-M1.** `VarTable::published_meta` seqlock (`var_table.rs` ~552): data
  loads are `Relaxed` with no fence before the second seq load. x86 TSO
  hides it; ARM can tear `(count, end)` → spurious `Corrupt("record range")`.
  Fix: `Acquire` fence after data, or load data `Acquire`.
- **S-M2.** `ArrayTable::flush_dirty` / `StrongTxTable::flush_dirty`: snapshot
  under read lock, drop, write, then clear `dirty`. A `set` in the window is
  lost until some later set re-dirties. Roles intend a single Class C
  writer, but flush can be a sidecar. Strong-bit miss recreates the
  "unstrong tip txs" case `store.rs` comments guard. Fix: `swap` dirty false
  **then** snapshot.
- **S-M3.** Fuse8 `decode_body` (`fuse8_filter.rs` ~166) does not check
  `segment_count_length + 2*segment_length ≤ fp_len`. `BinaryFuse8::contains`
  indexes fingerprints unchecked → panic on corrupt-but-decodable sidecar
  instead of `NeedsRewrite`.
- **S-M4.** `for_each_spender_create` (`point_table.rs` ~76): overflow `next`
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

- **Q-M1.** `retain_headers_needing_body` (`archive.rs` ~145): missing
  `first` fk → `unwrap_or(0)` keeps the wrong span. Should be
  `Corrupt("invariant: …")`.
- **Q-M2.** `RecentCreates` head is load-then-store, not RCU (`recent_creates.rs`
  ~89–175). Safe only if the write thread is the sole mutator — unenforced.
  `drop_from` is on the reorg path.
- **Q-M3.** `merge_outs` clones script bytes on every RCU retry; empty
  `checked` always publishes a new Arc (breaks `ptr_eq` / sticky).

### Net / mempool / RPC / esplora

- **N-M1.** Compact-block prefilled indexes: weaker than Core's strictly
  increasing + in-bounds check; short-id cursor can misalign (`compact.rs`
  ~92).
- **N-M2.** `HashMap::keys().next()` eviction for pending blocks, held
  bodies, fork-tips — random under cap. `hold_body` already has `held_seq`
  unused for eviction.
- **N-M3.** `cmpct_fills` and `requested_blocks` grow until success/arrival;
  no prune on abandon/timeout.
- **N-M4.** `relay_seq` / `accept_at` inserted on accept (`tx_relay.rs` ~863)
  and **not** removed in `unindex_txid` (~459). ~20 MiB/day unbounded.
- **N-M5.** AddrMan unbounded; Core caps tried/new.
- **N-M6.** `announced_wtx` cap = clear entire set → INV burst.
- **N-M7.** `disconnect_to` clones all disconnected txs even with no mempool
  (IBD reorg RAM).
- **M-M1.** Known-parent out-of-range vout parked as orphan forever
  (`accept.rs` ~524). Hard-reject.
- **M-M2.** Eviction `worst_chunk` on rate **tie** can pick an earlier chunk
  and strand descendants (no `removeRecursive`).
- **M-M3.** `evict_to_budget` `removed` is cumulative; later no-op iteration
  does not break → spin.
- **M-M4.** `persist_all` writes meta, then slots, then body — inverted vs
  body-before-claim. Crash → LIVE slot, garbage body.
- **M-M5.** `accept_package` has no package feerate (0-fee CPFP parent
  rejected). Name vs Core `submitpackage`.
- **R-M1.** `submitblock` gated to regtest; help says all networks.
- **R-M2.** `gettxout include_mempool` does not hide mempool-spent confirmed
  outs (`spends_outpoint` exists).
- **R-M3.** `getnetworkhashps` assumes ~2 hashes/block (regtest).
- **R-M4.** Unbounded JSON-RPC batch under one work permit.
- **E-M1.** `scripthash_mempool_stats` undercounts chained unconfirmed vs
  Esplora.
- **X-M1.** Esplora mempool tx JSON stubs omit vin/vout/size/weight when the
  tx is not in Class A (`handlers.rs` ~989). BDK-class clients fail to parse.
  Wire tx is in `mp.get_tx`.
- **X-M2.** Esplora WS does store IO on the tokio thread (`ws.rs` `on_tip` /
  `on_mempool_announce`). REST uses `spawn_blocking`.
- **X-M3.** Process-wide single `sh_join` slot: concurrent clients serialize
  and evict each other.
- **O-M1.** Conf `milestone=0` overwritten by network default (`cli.rs`
  ~895). CLI `--milestone 0` works; conf cannot disable skip.
- **O-M2.** `--minrelaytxfee` parse failure silently ignored; negatives → 0.
- **O-M3.** Tip-follow stale redial uses AddrMan cloned once after IBD.

---

## 4. Performance (big-O, allocation, IO)

Grouped by cost at mainnet scale. Many overlap §2–3.

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
| P17 | `RecentCreates::snapshot` | clone pending map (pins) / wave | COW `Arc<LiveMap>` |
| P18 | BQ `dequeue` height remap | O(queue) `index.iter().find` | `HashMap<u32, VecDeque<id>>` |
| P19 | `BlockCache` prefix drop | O(chain) per connect | `VecDeque` + base height |
| P20 | GBT `depends` | O(txs × inputs) linear scan | `HashMap<Txid, idx>` |
| P21 | Log macros | eval args + TLS even if disabled | `if enabled { … }` in macro |
| P22 | `api_log` global mutex + `write_all` | blocks async workers | dedicated writer thread |

Low (still real): `StrongTxTable::count_ones_bits` bit-by-bit vs
`u8::count_ones`; `PendingHead::queued_fk` reverse scan of 262 144;
per-slot pread in occupied walks; AddressHead probe copies of 32-byte keys;
`InFlightLayer.creates` SipHash vs `TxidHasher`; SH `Vec::contains` dedup;
`check_libre_annex` clones the witness; `find_and_delete_sig` always
allocates; one-shot confirm `block.clone()`; seeds `Vec::contains`.

---

## 5. Memory

| ID | Where | Shape | Cap today? |
|----|--------|--------|------------|
| Mem1 | `relay_seq` / `accept_at` | process lifetime | **no** |
| Mem2 | AddrMan | peer `addr` feed | **no** |
| Mem3 | `announced_wtx` | 50k then **clear all** | yes, but bursty |
| Mem4 | Mempool dead body bytes | RBF stall, compact not on admit | disk + rewrite |
| Mem5 | `bulk_fill_empty` | transient hundreds of MiB | per grow |
| Mem6 | IBD `disconnect_to` | whole losing branch cloned | skip if no mempool |
| Mem7 | `--api-log` JSONL | unbounded | **no** |
| Mem8 | Orphanage / Electrum subs / WS tracks | count/weight / `max_*` | yes |
| Mem9 | Body queue | request-limited soft budget | **by design** (`ibd-memory.md`) |
| Mem10 | RecentCreates pins | pipeline horizon | **by design** (PR4) |
| Mem11 | `TX_FULL_GETS` thread-local | prod `Vec` push per get | test-shaped leak |

`docs/errata.md` leftover maps (`txid → one fk`) stay as-is (Won't-fix unless
a mainnet miss).

---

## 6. Simplifications (ad-hoc → standard)

Do not flatten io_uring machines. Do not add a process pin FIFO.

2. **Chain work:** one cumulative index (fixes N-H3 + RPC P2).
4. **Wtxid:** secondary `HashMap` (fixes N-H2).
5. **Mempool eviction:** `BinaryHeap` of cluster worst-rate (fixes P3).
6. **Mempool slots:** free list (fixes P13).
7. **Block queue / densify:** keyed multimaps instead of `iter().find`.
8. **Held/pending eviction:** FIFO via existing `held_seq` / `VecDeque`.
9. **Announced-tx sets:** rolling bloom (Core `CRollingBloomFilter`).
10. **BlockCache:** `VecDeque` + height offset.
11. **RPC `active`:** slab keyed by id (fixes M-H2).
12. **Esplora `sh_join`:** small LRU, not one global slot.
13. **Esplora `/blocks`:** stored summary, not reconstruct.
14. **Electrum status:** one history-row builder for wire + preimage.
15. **`last_push_data`:** `Script::instructions()` (gets PUSHDATA4).
16. **`U64IdentityHasher`:** multiply by odd golden-ratio constant if
    hashbrown clustering shows.
17. **Seqlock:** fences, or stop rolling your own for a 16-byte pair.
18. **CLI parsers:** table-driven `take_parsed` (node + bench).
19. **Bench hex:** use `rbitcoin_primitives::hex_*`.
21. **Bit count:** `u8::count_ones`.
22. **SH `put_sorted_creates` `seen`:** `put_chain` already sorts+dedups.

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

---

## 8. Checked and found sound

- Publish order body → idx → count/HWM on Class A; readers use HWM.
- Confirm pipeline: bounded downstream channels; no cycle; write condvar has
  a timeout.
- CVE-2012-2459: whole-block duplicate-txid set (`block/mod.rs` ~127).
- Difficulty 4× clamps and `first_block_time` off-by-one match Core
  (mainnet).
- MTP median index matches Core `GetMedianTimePast`.
- BIP30 exception blocks 91842/91880 present.
- Duplicate inputs via `OutPointSet`; weight/MAX_MONEY saturating then range.
- CLTV/CSV 5-byte scriptnum + type match; tapscript OP_SUCCESS, MINIMALIF,
  520 B initial stack, annex, sighash midstate reuse; P2WSH 520 B.
- Same-block spends = pending only; spender height before create = unspent.
- `PinHalf` Frozen→ArcSwap: `OnceLock::set` loser re-applies `f` — no lost
  compose.
- Stamp priority matches `docs/invariants.md`.
- Soft densify is request-limited only.
- Merkle odd-layer duplicate-last.
- `TxPrecompute` vs rust-bitcoin oracles (incl. zero-input BIP144).
- Body-queue budget accounting matches `docs/ibd-memory.md`.
- Orphanage count/weight caps; Electrum/WS per-conn caps.
- Findings 001–023 remain fixed (not re-opened by this pass).
- `unindex_txid` **does** drop scripthash index + unbroadcast; it does
  **not** drop `relay_seq`/`accept_at` (Mem1).

---

## 9. Per-crate finding index

Counts are unique items in this document (not raw reviewer bullets).
Intentional COMPAT Electrum status extra field is **not** counted as High.

| Crate | High | Medium | Perf/Mem notable |
|-------|------|--------|------------------|
| store | 0 | seqlock, flush lost-update, fuse8 OOB, spender cycle, sidecar fsync, runs_io | BDZ fill, bulk_fill, SH N², fence clone |
| consensus + primitives | 0 | *(none remaining)* | MTP walks, rehash txids |
| query | 0 | retain fallback, RecentCreates CAS, merge_outs clone | snapshot clone, BQ scan, SipHash in-flight |
| net | 0 | compact indexes, random eviction, unbounded maps, v2 copies | densify, INV flush, BlockCache |
| mempool | 1 (testmempoolaccept, shared with RPC) | orphan vout, eviction tie, persist order, package feerate | free slot, persist_all |
| rpc | 2 (active pop, testmempoolaccept) | submitblock gate, gettxout, hashps, unbounded batch | GBT depends, longpoll |
| electrum | 1 (status **order** vs get_history) | mempool_stats | status full-history, announce O(subs) |
| esplora | 0 | stub mempool JSON, WS on runtime, sh_join slot | `/blocks` reconstruct |
| node/cli/log/bench | 0 | milestone=0 conf, minrelay silent, frozen AddrMan | log gating, api_log mutex |

---

## 10. Suggested work order

Not a plan (no red/green steps). Order is split-risk then operator-visible
then IBD CPU.

1. **M-H1 dry-run testmempoolaccept** + **M-H2 RPC active map**.
2. Electrum status **ordering**; decide COMPAT vs spec for the extra
   blockhash.
3. Mempool persist order, `relay_seq` unindex, AddrMan cap.
4. Esplora `/blocks` summaries (P11). IBD fence Arc, densify watermark,
   v2 decode, MTP ring, and HashHead online rehash (S-H1) are closed.

Out of scope for that list (Won't-fix / policy): flattening uring, process
pin FIFO, leftover `Vec<Fk>`, explorer APIs, `rbitcoin-bench` in required CI.

---

## 11. Closed in later PRs

When a finding is fixed, move its ID here with the PR number instead of
leaving a stale High row in §2–6.

- **C-M5.** Script-pool `failed` uses Acquire loads / Release store (with
  existing increment-then-check). #247.
- **C-M6.** Signet last exact 38-byte BIP141 commitment + Core
  `BLOCK_SCRIPT_VERIFY_FLAGS` (P2SH|WITNESS|DERSIG|NULLDUMMY, no CLEANSTACK).
  Pins: `witness_commitment_index_last_exact_38_byte`,
  `signet_challenge_op_true_twice_is_not_cleanstack`,
  `signet_challenge_p2wpkh_empty_witness_rejected`. #247.
- **C-M4.** `assemble_run` runs future-time + BIP34/66/65 nVersion on every
  block (`check_header_version_and_future_time`). Pins:
  `check_header_version_and_future_time_regtest`,
  `assemble_second_block_rejects_stale_nversion`. #247.
- **C-M7.** Witness sigops only after segwit (`prevout_spk_sigops` / `flag_segwit`).
  Pin: `witness_sigops_gated_on_segwit`. #247.
- **C-M3.** P2SH sigops abort on scriptSig opcode `> OP_16` (`p2sh_sigops_non_push_scriptsig_is_zero`). #247.
- **C-M2.** Regtest subsidy halves every 150 (`p1_block_subsidy_halvings`). #247.
- **C-M1.** Coinbase empty `vout` rejected (`s13_rejects_coinbase_empty_vout`). #247.
- **C-H4.** Testnet 20-minute min-difficulty (`allow_min_difficulty_blocks`)
  and walk-back to last non-powLimit bits. Pin: `testnet_min_difficulty_after_20_minute_gap`.
  #247.
- **C-H3.** P2SH scriptSig is `eval_script` + IsPushOnly (`OP_1NEGATE` accepted;
  >10 000-byte scriptSig rejected). Pins: `p2sh_legacy_op_1negate_scriptsig_accepted`,
  `p2sh_legacy_scriptsig_over_10k_rejected`. #247.
- **C-H1.** BIP342 tapscript validation-weight budget (`50 + witness size`,
  −50 per nonempty CHECKSIG*). Pin: `script_path_rejects_tapscript_validation_weight`.
  #247.
- **C-H2.** `script_sigop_count` stops on truncated PUSHDATA2/4 (Core `GetOp`).
  Pin: `truncated_pushdata2_does_not_count_leftover_checksig`. #247.
- **P1 / N-H2** — `TxGraph` `HashMap<Wtxid, Txid>`; hub inv lookup is O(1). [#244](https://github.com/reardencode/rbitcoin/pull/244).
- **P2 / N-H3** — `ChainHub` RAM prefix `work through h` (extend/truncate to tip; ~32 B × height). RPC `chainwork` uses `work_through_height`. [#244](https://github.com/reardencode/rbitcoin/pull/244). Durable `nChainWork` column not done.
- **P3** — `worst_chunk` is the first `(rate, rep)` map entry; insert/remove repair the cluster. [#244](https://github.com/reardencode/rbitcoin/pull/244).
- **§6.20** — `has_spender_rels` deleted; `has_abs_layout` is the spent_range predicate. [#246](https://github.com/reardencode/rbitcoin/pull/246).
- Lookup now stamps `spent.idx` ranges (stage table already required it); load copies onto pins; write ensure is holes-only. Not in the 2026-08-25 Open tables. [#246](https://github.com/reardencode/rbitcoin/pull/246).
- **P7** — leftover TipOnly fence snapshot is `Arc<Vec<FenceRun>>` (O(1) clone; extend/pop COW). [#245](https://github.com/reardencode/rbitcoin/pull/245).
- **P8** — v2 `parse_v2_contents` does not sha256d; `FramedMessage::decode` is command+payload (checksum unused). [#245](https://github.com/reardencode/rbitcoin/pull/245).
- **P10** — densify `densify_scan_lo` skips BQ-ready / inflight / archived prefix; pending still walked. [#245](https://github.com/reardencode/rbitcoin/pull/245).
- **P16** — fence-tip MTP is an 11-slot ring on extend; pop rebuilds; historical heights still 11-read. [#245](https://github.com/reardencode/rbitcoin/pull/245).
- **N-H1.** 50 ms session tick calls `PeerHub::on_session_heartbeat` →
  `check_headers_sync_timeouts`. Pins:
  `session_heartbeat_disconnects_stalling_headers_sync`,
  `session_heartbeat_keeps_sole_preferred_headers_sync_peer`.
  [#252](https://github.com/reardencode/rbitcoin/pull/252).
- **N-H4.** Inbound accept uses `inbound_connect_and_handshake` (60 s Core
  VERSION/VERACK bound); timeout is `NetError::Timeout` and drops the
  `max_inbound` permit. Pins: `inbound_handshake_timeout_after_silence`,
  `inbound_handshake_timeout_is_core_60s`.
- **S-H1** — HashHead / ScriptHashHead no longer rewrite occupied tables while serving. Header overflow rolls `header.head.gN`; ingest SH seals at 0.80. Undersized single-gen `header.head` rewrites on open only. `rehash_to` / `rehash_gate` / `ShardedHashHead` deleted. [#248](https://github.com/reardencode/rbitcoin/pull/248).
