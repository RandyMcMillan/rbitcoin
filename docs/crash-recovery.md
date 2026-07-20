# Crash recovery and reorg semantics (store)

Hard kills (`kill -9`) and tip disconnects are normal. **Corrupt files are not repaired in-process** — reindex / redo IBD.

## Tip as commit point

Best-chain views ignore uncommitted Class C state:

| Write order (confirm run) | Role |
|---------------------------|------|
| 1. `strong_tx` + `tx_height` | May lead tip after kill |
| 2. Thin scripthash **creates** (batched) | May lead tip after kill |
| 3. `confirmed[]` tip advance | **Commit** |

`is_confirmed_strong(tx)` ⇔ strong ∧ `tx_height ≤ tip`. Queries that mean “on best chain” use this (or equivalent).

On open, `repair_class_c_above_tip` clears strong/height **above** tip (tip-relative, not a full rebuild).

## Class A (archive)

- Append-oriented; re-archive is **idempotent** when `header_txs` already present.
- Kill mid-archive without a complete body association ⇒ not treated as archived; re-getdata.

## Points (spend multimap)

- Edges may exist for non-strong (archive-ahead) spenders.
- Best-chain spentness: edge + `is_confirmed_strong(spender)`.
- Kill-safe: extra edges do not false-positive if filter is applied.

## Thin scripthash (Electrum outpoint pointers)

- Hybrid (schema v4): head holds ≤2 inline creates or one geometric body slab (`create_tx_fk | vout` entries, no `next`). Size-class freelist reuses freed slabs.
- Legacy v3: body `create_tx_fk | vout | next` (migrated on open / `migrate_scripthash`; runs dir untouched).
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

Clean shutdown flushes mmap tables. Kill may lose the last unflushed pages (same as other tables).

## Operator

- Indexes off during milestone (`tx.head`, points) are **backfilled** at tip mode when needed.
- Scripthash is **always** maintained on confirm (thin creates); no disable flag and no recovery backfill — corrupt index ⇒ reindex.
