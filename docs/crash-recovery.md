# Crash recovery and reorg semantics (store)

Hard kills (`kill -9`) and tip disconnects are normal. **Corrupt files are not repaired in-process** — reindex / redo IBD.

## Tip as commit point

Best-chain views ignore uncommitted Class C state:

| Write order (confirm **write** thread) | Role |
|-------------------------------------------|------|
| 0. Structural spentness / maturity / subsidy | No durable tip write yet |
| 1. `strong_tx` + `tx_height` (L2 RAM / L0 file) | May lead tip after kill **before** barrier |
| 2. Thin scripthash **creates** (batched) | May lead tip after kill |
| 3. `confirmed[]` tip advance (L2 RAM) | In-process commit |
| 3b. **`flush_class_c_tip`** (complete-or-fail L2 images + `tx_height` sync) | **Durability barrier** |
| 4. Body-queue dequeue for those heights | Only after confirm-write returns Ok |
| 5. Spend annotations (Direct) | After tip; spentness filters use strong+height |

`is_confirmed_strong(tx)` ⇔ strong ∧ `tx_height ≤ tip`. Queries that mean “on best chain” use this (or equivalent).

On open, `repair_class_c_above_tip` clears strong/height **above** tip (tip-relative, not a full rebuild).

### L2 write-behind + body queue (phase 6)

- Compact Class C (`confirmed`, `header_txs_*`, `strong_tx`) mutate **RAM only** during the commit batch.
- Disk is updated as one complete body image per dirty table on `flush_class_c_tip` (not mid-batch per-slot write-through).
- **Kill mid-commit (before barrier):** BQ still holds block payloads → re-drive rebuilds Class C; disk L2 image stays last good flush.
- **Kill after barrier + before dequeue:** tip durable; BQ entry may still exist (harmless re-confirm / dequeue).
- Prefer **loss of uncommitted tip** over a **torn multi-slot Class C image**.

## Class A (archive)

- Append-oriented; re-archive is **idempotent** when `header_txs` already present.
- Kill mid-archive without a complete body association ⇒ not treated as archived; re-getdata.

## Spends (v5: annotation on create outputs)

- Sole spender: `output.spender_field = spending_tx_fk`. Multi: `MULTI_SPENDER` + `spenders.body` list.
- Annotations may remain after disconnect / for non-strong spenders.
- Best-chain spentness: annotation + `is_confirmed_strong(spender)`.
- Kill-safe: stale/non-strong fields do not false-positive if filter is applied.
- No `point.head` (v4 open-hash multimap removed).
- Class A bodies are **packed-only**; non-packed bare meta rows are rejected as corrupt.

## Thin scripthash (Electrum outpoint pointers)

- Hybrid (current / since v4–v6): head holds ≤2 inline creates or one geometric body slab (`create_tx_fk` only, no `next`). Size-class freelist reuses freed slabs.
- **No spend columns** — spentness from points + Class C at query time.
- Creates written on confirm (before tip advance).
- **Kill-safe without chain walks:** first confirm after open **sequentially scans**
  `scripthash.body` once into a process set of `create_tx_fk`s already present;
  re-confirm skips those txs. Hot path only appends + maintains heads in RAM.
- Creates for unstrong / above-tip txs are **invisible** via `is_confirmed_strong`.
- Disconnect tip: **unlink** creates for that block’s outputs (tombstone + rewire);
  process set updated so re-confirm can re-index.
- No tip-mode full rebuild; corrupt index ⇒ reindex (wipe store / redo IBD).

## Flush

Clean shutdown: `flush_for_shutdown` fsyncs tip/Class C (incl. L2 dirty images) then async Class A.
Steady path: payload pwrite + HWM publish; `sync_data` unless `defer_durable_flush`.
Kill mid-payload before HWM publish: readers never see past previous published length.

## Operator

- Direct IBD keeps segmented **`tx.head/`** (archive) and **spend annotations** (confirm) live; tip entry does **not** re-scan Class A to repair them. Corrupt head/spends ⇒ reindex (optional manual `backfill_tx_index` rebuilds segmented head mappings from Class A).
- **Segmented `tx.head`:** directory `tx.head/` with `meta` + per-segment files (+ `.fuse8` when sealed). Flat `tx.head.meta` / `tx.head.NNNNNN` are **migrated into** `tx.head/` on open. Seal publishes fuse then marks sealed in meta. Kill mid-seal may require deleting incomplete segment files / meta and rebuilding from Class A (or reindex). Legacy mono `tx.head` file / `.new` / `.resize` are not opened — reindex.
- Scripthash (Direct): thin creates → memtable → target-sized sorted spills + **SEAL** (`max_create_fk`). Memtable is not durable; on resume, re-enqueue creates with `create_fk > SEAL`. Tip: bounded-fan-in merge then bulk-load durable SH. Corrupt durable SH ⇒ reindex.
