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

- Compact Class C (`confirmed`, `header_txs_*`, `strong_tx`) mutate **RAM only** during the commit batch (`tx_height` stays L0 write-through).
- **Connect** barrier order on disk (**tip last**): `strong_tx` → `tx_height` → `header_txs` → **`confirmed[]` last**.
  - Mid-barrier kill after pre-tip tables: tip stays old; strong/height above tip repaired by `repair_class_c_above_tip`.
  - Never flush `confirmed` before strong/height on connect — tip with permanent unstrong txs (repair only clears **above** tip).
- **Disconnect** barrier order (**tip first** — opposite of connect):
  1. SH unlink only (do not clear strong/height yet).
  2. `confirmed` truncate → `flush_confirmed_only` (durable tip shrink).
  3. Then `set_unstrong` / `tx_height.clear` → flush strong/height.
  - Mid-kill after tip shrink: leftover strong/height are **above** new tip → repairable.
  - Never clear `tx_height` (L0 write-through) or unstrong while tip is still high — permanent unstrong-at-tip.
- Append-only tip extension writes **suffix only**. In-prefix full rewrites residual; tip-last connect + tip-first disconnect + BQ re-drive mitigate.
- Prefer **loss of uncommitted tip progress** over **tip-ahead-of-strong** or **tip-high-with-unstrong**.

## Class A (archive)

- Append-oriented; re-archive is **idempotent** when `header_txs` already present.
- **Never leads tip:** Class A is published only on the confirm-write path with tip advance in the same era (no dual-track archive-ahead).
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
