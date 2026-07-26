# Agent notes

## Store concurrency: lock-free by default

**Default: no locks on the store hot path.** Concurrency is **roles + publish
order + map epochs**, not `Mutex` around mmap.

| Rule | Detail |
|------|--------|
| Roles | At most **one Class A appender** and **one spend annotator** per process; **N readers** of published ranges always free |
| Publish | body → idx → count/HWM (Release); then head / `header_txs` as visibility requires |
| Capacity grow | fallocate + map a **new epoch** on the same file; swap pointer; old epoch lives until pins drop (readers never pause). Same *spirit* as online `tx.head` shadow fill + brief final swap |
| Layout grow (`tx.head`) | shadow fill unlocked; exclusive **only** at final catch-up + rename + head swap |
| Not OK | Long-held map mutexes, “pause all queries during confirm”, multi appenders, dual-write to head shadow on every insert |

If a change introduces a new long-held store lock on the IBD/read path, it is the
wrong design — fix the protocol. See `docs/concurrency.md`.

## Commit + release build after code changes

Whenever a turn **changes code** (or you finish a multi-step coding task in that turn):

1. **Commit** the working tree with a clear message (what + why). Prefer one commit per logical checkpoint — especially before starting a risky follow-on experiment, so we can roll back. Do **not** leave multi-hour IBD perf/refactor work uncommitted.
2. **Rebuild release** so the user’s binary matches the tree:

```bash
nix-shell --run 'cargo build -p rbitcoin-node --release'
```

Do the release build even if tests already ran in debug — the operator typically runs `target/release/rbitcoin-node`. Skip commit/build only when the turn was pure discussion / docs with no compile-affecting edits.

If you cannot commit (hooks, secrets, user said not to), still rebuild release and say explicitly that the tree was **not** committed.

## Tests required for code changes

- **Always ship test coverage with behavioral code changes.** Prefer unit tests next to the code (`#[cfg(test)]` in the same crate) or focused integration tests in `rbitcoin-test` / crate `tests/`. Pure docs/comments need no tests.
- **Bug fixes must include a regression test** that fails without the fix and passes with it. Do not land a “fix” that only re-describes production logs; encode the failing case (fixture block, synthetic store, prevout/script edge) so it cannot silently come back.
- Run the new/related tests before commit (e.g. `cargo test -p <crate> …` or the scenario that covers the change). If a full-store mainnet case cannot run in this VM (see datadir notes), still add a synthetic/unit regression that pins the logic.

## Datadir / store on this workspace (do not open in the agent VM)

The workspace is mounted into the agent VM as **9p** (`workspace` on `/home/agent/workspace`, `trans=virtio`). On this mount:

- **Writable shared `mmap` (`MAP_SHARED` + `PROT_WRITE`) fails with `EINVAL`** for store table files.
- Read-only mmap may work; `pread`/`pwrite` work.
- `rbitcoin-store` opens tables with `MmapMut` today, so **`Query::open` / `Store::open` / `rbitcoin-node` against paths under the workspace will fail** with `io error … Invalid argument (os error 22)` (e.g. on `scripthash.body`).

**Do not use the user’s live test datadirs in this VM** (e.g. `datadir-signet/`, `datadir-mainnet/`) to open the store, run the node against those paths, or diagnose tip stalls by loading Class A/C tables here. That includes ~27 GiB signet store under `datadir-signet/store/`.

### What works instead

- Read **logs** the user leaves in-tree (`signet-ibd.log`, etc.).
- Inspect store files with **non-mmap** tools (`pread`/Python struct parsing of HWMs, headers) when useful for offline forensics only — not as a substitute for a full node open.
- Reproduce with **synthetic fixtures** and `rbitcoin-test` scenarios under `/tmp` or other non-9p paths where mmap works.
- Ask the user to run the node / confirm diagnostics on their host (where the datadir is a normal local FS).

### Related symptoms already seen

- Tip stall / confirm diagnostics against `./datadir-signet` failed at open with mmap EINVAL.
- User-side IBD still advances; agent-side cannot drive or fully open that store.

## No dead code warnings silenced unless there is an absolutely bulletproof justification.

Do not leave dead code around. Delete it. Don't silence warnings unless there
is bulletproof justification

## Do test-driven development when practical

Always for bugs, make sure to create a test the replicates the bug, run it to
see it fail, then fix the bug and run the test to see it pass.

For features, ideally we'd write a scenario test that fails without the
required feature before beginning and then implement the feature and see the
test pass.

For performance, ideally we'd have a benchmark before we begin development
that shows a clear change after.

## IBD / process memory leak prevention

**Full rules:** `docs/ibd-memory.md`. Summary for agents:

1. **Distinguish** process-owned heap (Rust structures, charged archive bodies)
   from **kernel page cache** under store mmaps (`RssFile`). Do not “fix” RSS
   by gutting intentional caches (OutFifo, sticky, ContigPark, archive budget).
2. **Archive queue ownership:** `charge` on first body enqueue must pair with
   exactly one `release` via `ArchiveResult` applied in `apply_archive_result`
   (or immediate release if `arch_job_tx.send` fails). Dropping an `ArchiveJob`
   after charge without Ok/Err/Dropped is a **leak**.
3. **`archive_charged` is not hygiene-pruned** — only `clear_archive_charged` on
   pipeline result (prevents double-charge if ordered hygiene runs early).
4. **Abort paths** (WriterDead, stop, prep exit) must drain ContigPark + job
   channels and emit results (`release_remaining_jobs`).
5. **Tests** must tear down intentional caches with **production** APIs
   (`advance_tip`, OutFifo eviction via insert, budget release via results,
   drop owning `Query`/pipeline) — not a secret free-all that masks production
   leaks.
6. **Regression filters:**
   `force_advance_returns_parked_jobs_for_charge_release`,
   `multi_block_park_abort_releases_all_charges`,
   `archive_budget_charge_release_symmetric`,
   `presence_lifecycle`.
