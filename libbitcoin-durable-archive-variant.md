# Libbitcoin Durable Archive Variant — Design Spec

**Purpose:** Spec for a libbitcoin (node + database) variant that keeps libbitcoin’s concurrent IBD / structural-index model, while adding **Core-class durability for buried archive data** and a **recent wire-format block cache**. Intended for implementation by a later agent against `libbitcoin-database` / `libbitcoin-node` (v4-era master-style store).

**Non-goals:** SQL backend; UTXO-centric redesign; pruning-first minimal node; changing consensus rules; degrading IBD performance.

---

## 1. Product intent

### 1.1 What we keep (do not regress)

- Memory-mapped hash/array/blob tables (not SQLite/PostgreSQL/LevelDB as primary store).
- Fully concurrent download / store / index during IBD (allocate-then-publish, lock-free heads where present).
- Transaction-relational archive: headers, txs, inputs, outputs, points, strong_tx, height indexes, optional address/filters.
- Spend model: **do not mutate old output rows**. Spends are recorded by appending `point` multimap entries (key = outpoint) + confirmation via `strong_tx` / `confirmed`.
- Milestone / assume-valid-style concurrent sync.

### 1.2 What we add

| Feature | Role |
|--------|------|
| **Wire block ring** | Last *N* blocks (or equivalent budget) in canonical wire format for serve, reorg, and tip recovery |
| **Archive finalization epochs** | Explicit fsync + epoch metadata so data older than the wire window need not be rebuilt from the network after a crash |
| **Archive mode gate** | Wire + finalize **disabled during IBD**; enabled only after contiguous chain to tip (steady state) |

### 1.3 Durability promise (user-facing)

| Zone | Guarantee |
|------|-----------|
| **During IBD** (`archive_mode == false`) | Same as current libbitcoin: store is rebuildable; crash may require resync / continue from incomplete state. **No** Core-class guarantee. |
| **Steady state, height ≤ last epoch** | Archive contents durable on disk (fsynced, epoch-recorded). Recovery must **not** require full IBD for this range unless checksum/verification fails. |
| **Steady state, epoch &lt; height ≤ tip** | Soft zone: reconstruct from wire ring and/or peers; may lose unsynced tip state on hard crash. |

Target: **as nearly as possible Core’s guarantee for buried block information**, without reintroducing UTXO total-order as the center of the design.

---

## 2. Background: how the store actually mutates

Implementors must not assume “old outputs get a spender field written in.”

### 2.1 Write-once archive (Class A)

Tables (representative): `header`, `transaction`/`tx`, `input`, `output`, `ins`, `outs`, `txs` (+ filter bodies as applicable).

- Bodies are append-oriented.
- Output row = parent tx + value + script only (no spender field).

### 2.2 Forever-open multimaps (Class B)

Tables: `point` (spend index), optional `address`, `duplicate`, etc.

- New spends of **ancient** outpoints: **append** a new `point` row keyed by that outpoint; **CAS/push** the hash head for that bucket.
- Heads remain mutable forever for keys that receive new entries.
- Finalization **never** means “spend list for this outpoint is complete forever.”

### 2.3 Tip / confirmation state (Class C)

Tables: `strong_tx`, `confirmed`, `candidate`, validation caches (`prevout`, `validated_*`, …).

- `set_strong` / `set_unstrong` allocate/put strong_tx records (including negative/unstrong); reorg does not rewrite Class A bodies.
- Tip confirmation is recoverable from headers + txs + policy once archive is consistent, or from wire for the soft window.

### 2.4 Implication for epochs

Finalize seals **durable prefixes of Class A (and consistent Class B body/head state as of finalize time)**.  
Future spends only append Class A/B and update heads—they must not require rewriting sealed body **prefixes**.

---

## 3. Operating modes

### 3.1 `archive_mode` flag

```text
archive_mode == false  →  IBD / catch-up (performance path)
archive_mode == true   →  steady state (durability path)
```

**Enable `archive_mode` only when:**

1. Confirmed (or equivalent best-chain) headers/blocks are **contiguous** from genesis (or configured prune/start point) to local tip, and  
2. Tip is **current** per existing node policy (e.g. within configured time/work of network), and  
3. Node is not in a bulk gap-fill / reindex mode.

**Optional:** if tip falls more than *M* blocks behind (long outage), set `archive_mode = false` for the catch-up burst, then re-enable and bulk-finalize again.

### 3.2 IBD path (`archive_mode == false`)

- **No** wire ring writes.
- **No** finalize / epoch fsync advancement (beyond whatever existing snapshot/backup the operator already configures—do not add mandatory per-block durability).
- Zero intentional IBD performance regression vs upstream.

### 3.3 Transition: first enable

When flipping `false → true`:

1. Take store exclusive/transactor (same class of lock as existing `store::snapshot`).
2. **Bulk finalize** once (or in large height batches): fsync Class A/B bodies + heads; write epoch at `finalized_height = tip - N` (or `tip` if wire starts empty and will fill going forward).
3. Emit progress events (e.g. `event_t` extensions: `archive_finalize_begin/end`).
4. Enable wire writes for subsequent blocks.
5. Release lock; enter steady-state loop.

This bulk fsync may take noticeable time once; it is an explicit one-time (or rare) cost, not an IBD tax.

### 3.4 Steady state (`archive_mode == true`)

On each accepted/confirmed block (or batch of *k* blocks):

1. Append **wire** serialization to the ring (indexed by height/hash).
2. When `tip - finalized_height > N` (or byte budget exceeded): run **incremental finalize** for heights exiting the window (may batch).
3. Drop wire entries for heights ≤ `finalized_height`.

---

## 4. Wire block ring

### 4.1 Requirements

- Store **canonical wire-format** block bytes for the most recent window.
- Window size: configurable; default suggestion **~100 blocks**, or better: **time/work/byte budget** (e.g. 24–72h of blocks, or 1–2 GB cap). Parameterize as `wire_depth` / `wire_bytes`.
- Support: `get_wire_block(height | hash)`, delete ≤ height, crash-safe enough for tip recovery (fsync policy can be lazier than archive epoch, but document it).
- Used for: peer serve of recent blocks, reorg within window, recovery replay after crash without full IBD.

### 4.2 Placement

- Prefer **node-layer** flat files or a thin optional database table so database core stays free of P2P concerns.
- Must be skippable when `archive_mode == false`.

### 4.3 Non-requirement

- Do **not** retain full historical wire for the entire chain unless optionally configured later. Sealed relational archive is the buried source of truth for query/validation navigation; wire is a **hot tip cache**.

---

## 5. Finalization epochs

### 5.1 Epoch record (durable)

Persist a fsynced record (file under store prefix), approximately:

```text
archive_epoch {
  magic, version
  finalized_height
  finalized_header_fk (or hash)
  per-table body HWM (counts/bytes): header, tx, input, output, ins, outs, txs,
                                     point, address, ... as needed
  per-table head identity (size / checksum) for hashmap heads
  optional: content checksums of sealed body prefixes
  timestamp
}
```

Write order for finalize:

1. Ensure no in-flight archive tx is between deferred commit points (use existing **transactor** / same exclusion as `snapshot`).
2. `msync`/`fsync` Class A bodies up to HWM (or full files if simpler and acceptable).
3. `fsync` Class B bodies up to HWM.
4. `fsync` Class A and B **head** files (heads always need to match published body prefixes).
5. Optionally fsync Class C prefix if policy includes hard finality of confirmations (see §5.3).
6. Write epoch record; `fsync` epoch file and parent directory **last**.

### 5.2 Recovery rules

On unclean shutdown when `archive_mode` was true:

1. Load last complete epoch → trust archive ≤ `finalized_height` (and HWMs).
2. Set logical tip to `min(physical_tip, finalized_height + recoverable_wire_or_network)`.
3. Replay wire ring for `finalized_height+1 …`; if wire missing, fetch blocks from peers from that height only.
4. Rebuild/repair Class C (strong/confirmed) for the soft zone as needed.
5. **Do not** full-resync genesis→tip solely because tip crashed, if epoch verifies.

On checksum failure of sealed prefix: targeted re-download of affected range (or full resync as fallback); do not silently trust.

### 5.3 Finality policy (choose and document)

**Recommended default (Core-like practical guarantee):**

- Epoch seals **Class A** + **Class B prefix as of finalize time**.
- Class C for height ≤ `finalized_height` may be included in epoch **or** reconstructed from sealed A + confirmed chain once tip is replayed—pick one and test.
- Reorgs deeper than `wire_depth` after finalization are unsupported or require explicit “unfinalize” (should be rarer than N if N is large enough).

**Do not** claim sealed point heads mean “no future spends of these outs.”

### 5.4 HWM under concurrent IBD

During IBD, blocks may be stored **out of height order**, so “current table count” ≠ “all objects for height ≤ H.”

Because finalize runs only in steady state after contiguous tip:

- **First bulk finalize** may use **global current HWMs** (entire store as of catch-up) plus `finalized_height = tip - N`. That is sufficient and simpler.
- **Incremental finalize** after that: either  
  - (A) record per-block link spans at archive time once `archive_mode` is on, and take max span over heights being sealed, or  
  - (B) always finalize by “current global HWM + raise finalized_height,” which is correct when the only new data is tip extension (true in steady state).

Prefer **(B) for v1** after catch-up; add per-block spans only if needed for finer partial seal.

---

## 6. Explicitly out of scope for v1 (unless free)

- In-place spender fields on `output` rows.
- Sealing multimap heads as immutable/complete.
- Mandatory finalize during IBD.
- Changing spend navigation (`to_spenders` via `point`).
- Making UTXO set the primary chainstate.
- Full historical wire archive.

---

## 7. Implementation map (code touch points)

Repos are indicative of current libbitcoin-database layout; adjust to tree as-of implementation date.

### 7.1 `libbitcoin-database`

| Area | Work |
|------|------|
| `store.hpp` / settings | `archive_mode`, `wire_depth` / budgets, paths for epoch file |
| New `types/epoch.hpp` (or similar) | Epoch struct, serialize, verify |
| `store::finalize`, `last_epoch`, `verify_epoch` | Ordered fsync + epoch write |
| `store_snapshot.ipp` / `mmap::flush` | Reuse flush; split “tip snapshot” vs “archive finalize” if useful |
| `store::open` / restore | Roll tip to epoch + wire; no discard of sealed HWM |
| `error.hpp` / events | `epoch_*`, `archive_mode`, finalize failures |
| Tests | Crash between body write and head publish; crash mid `tx.set`/`commit`; enable after fake IBD; spend ancient out after finalize; corrupt sealed byte detected |

### 7.2 `libbitcoin-node` (or server)

| Area | Work |
|------|------|
| Sync/state machine | Set `archive_mode` when contiguous + current; clear on deep lag |
| Wire ring component | Append on block accept; query; prune on finalize |
| IBD completion hook | Trigger bulk finalize once |
| Steady-state loop | Wire + incremental finalize batching |
| Optional | Serve recent `getdata` from wire when present |

### 7.3 Do not break

- `chain_writer.ipp` point put / deferred tx commit ordering.
- `hashhead` release-fence + CAS publish order.
- Transactor exclusion around multi-table consistency and finalize.

---

## 8. Configuration knobs (suggested)

```text
archive_durability = true|false          # master switch for variant features
archive_mode_auto = true                 # auto flip on catch-up / lag
wire_depth_blocks = 100                  # or 0 to disable wire when durability on
wire_depth_bytes = 0                     # optional cap; 0 = blocks-only
finalize_batch_blocks = 1                # amortize fsync (e.g. 6–72)
finalize_checksums = true|false          # v1 can start false; recommend true later
reorg_limit = wire_depth_blocks          # policy documentation
```

Defaults should leave **IBD identical** to upstream when catching up from genesis (`archive_mode` starts false).

---

## 9. Acceptance criteria

1. **IBD benchmark:** With durability features compiled/enabled but `archive_mode` false for the run, wall time and write path within noise of upstream (no extra fsync/wire per block).
2. **Catch-up transition:** After contiguous tip, bulk finalize completes; epoch file present; process kill + restart recovers without genesis IBD if tip crash simulated after finalize.
3. **Steady state:** Kill process after several post-IBD blocks; restart restores ≤ epoch without network for sealed range; tip rebuilds from wire or short peer fetch.
4. **Ancient spend:** After finalize, new block spending pre-epoch output indexes correctly via `point`; no output-row mutation; no epoch corruption.
5. **Reorg within wire window:** Unconfirm/reorg works; wire aids reconstruction.
6. **Query parity:** Existing structural queries (tx, spenders, confirmed) behave as upstream for sealed and soft zones after recovery.

---

## 10. Design rationale (short)

- Libbitcoin’s strength is **dependency-factored concurrency** and a **relational mmap archive**, not a tiny UTXO cache. UTXO-as-ordering-backbone aged poorly as the set grew multi-GB and stayed totally ordered.
- General SQL DBs are the wrong abstraction (write concurrency, fixed access paths, embeddability).
- mmap alone does not give Core-class buried durability; **explicit epoch + fsync** does.
- Mutability of “old” data is **index heads + tip state**, not output bodies—epochs must respect that.
- **Deferring wire + finalize until after IBD** preserves the architecture’s main performance win while still delivering archival guarantees for a general-purpose full node in steady state.

---

## 11. Suggested implementation order

1. Epoch record + `finalize()` + open/recover (no wire yet); manual trigger after sync in tests.
2. `archive_mode` gate in node; bulk finalize on catch-up; IBD path untouched.
3. Wire ring + prune on finalize; recovery replay.
4. Incremental finalize batching + optional checksums.
5. Hardening: crash tests, corrupt-prefix detection, lag re-disable.

---

## 12. References for implementors (context only)

- Store: `include/bitcoin/database/store.hpp`, `impl/store/store_snapshot.ipp`
- Archive write: `impl/query/archive/chain_writer.ipp` (point put, deferred tx commit)
- Spends: `point` multimap schema; `impl/query/navigate/navigate_reverse.ipp` (`to_spenders`); `impl/query/confirmed.ipp`
- Strong/confirm: `impl/query/consensus/consensus_strong.ipp`
- Heads: `impl/primitives/hashhead.ipp` (CAS + release fence)
- Conceptual writeup: Delving Bitcoin “Libbitcoin for Core people”; project history (LevelDB → mmap hashtables)

---

## 13. One-sentence summary for the implementor

**After IBD only:** keep a short wire-format block ring for the tip, and periodically fsync + epoch-seal the relational mmap archive behind that ring so buried data is as durable as Core’s flushed history—without mutating old outputs, without finalize during IBD, and without abandoning concurrent structural indexing.
