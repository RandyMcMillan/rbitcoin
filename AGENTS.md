# Agent notes

## Store concurrency: lock-free by default

**Default: no locks on the store hot path.** Concurrency is **roles + publish
order + map epochs**, not `Mutex` around mmap.

| Rule | Detail |
|------|--------|
| Roles | At most **one Class A appender** and **one spend annotator** per process; **N readers** of published ranges always free |
| Publish | body → idx → count/HWM (Release); then head / `header_txs` as visibility requires |
| Capacity grow | fallocate + map a **new epoch** on the same file; swap pointer; old epoch lives until pins drop (readers never pause) |
| Layout grow (`tx.head`) | **segment roll**: seal open head (fuse8) + create new fixed 25-bit head — no mono-file bits-widen |
| Not OK | Long-held map mutexes, “pause all queries during confirm”, multi appenders, dual-write to head shadow on every insert |

If a change introduces a new long-held store lock on the IBD/read path, it is the
wrong design — fix the protocol. See `docs/concurrency.md`.

## Create caches: residency sole map, FIFO only

| Rule | Detail |
|------|--------|
| Hot pin map | **`CreateResidency`** only (plan pin + commit denserels seed + prewarm) |
| IBD confirm intake | **body queue wire only** → plan → prep (no hash-only / Class-A-only confirm) |
| Eviction | **Insert-order FIFO** — never read-touch / LRU reorder (one spend ⇏ next spend on same create) |
| denserels_hit% | **~35–50% is normal** mid/late mainnet IBD (old UTXO spends). Do not chase ≥65% or inflate cache caps for it |
| Removed | **OutFifo** and **archive sticky** are gone — do not reintroduce dual maps |
| IBD sizes | **`residency creates=/outs=`** is the pin occupancy meter |

See `crates/rbitcoin-query/src/create_residency.rs` module docs.

## Commit + static musl release after code changes

Whenever a turn **changes code** (or you finish a multi-step coding task in that turn):

1. **Commit** the working tree with a clear message (what + why). Prefer one commit per logical checkpoint — especially before starting a risky follow-on experiment, so we can roll back. Do **not** leave multi-hour IBD perf/refactor work uncommitted.
2. **Rebuild and install the portable static musl release** so
   `./target/release/rbitcoin-node` matches the tree. This is **mandatory every
   code-changing turn** — not optional after tests.

### Required recipe (only this)

```bash
nix build .#rbitcoin-musl --out-link result
mkdir -p target/release
install -m 755 result/bin/rbitcoin-node result/bin/rbitcoin-cli target/release/
file target/release/rbitcoin-node   # must say "statically linked" (musl)
```

### Forbidden for the operator binary

| Do **not** run | Why |
|----------------|-----|
| `nix-shell --run 'cargo build -p rbitcoin-node --release'` | Dynamic **Nix glibc** link; dies off-store with `No such file or directory` |
| `cargo build --release` (host toolchain) | Same class of non-portable binary |
| Leaving `target/release/` as the last **debug** or glibc build | User restarts IBD from that path |

`nix-shell` / `cargo test` for **tests** is fine. Only the **shipped** node/cli
under `target/release/` must come from `nix build .#rbitcoin-musl`.

Skip commit/build only when the turn was pure discussion with no
compile-affecting edits. If you cannot commit (hooks, secrets, user said not
to), still do the static musl install and say the tree was **not** committed.

## Tests required for code changes

- **Always ship test coverage with behavioral code changes.** Prefer unit tests next to the code (`#[cfg(test)]` in the same crate) or focused integration tests in `rbitcoin-test` / crate `tests/`. Pure docs/comments need no tests.
- **Bug fixes must include a regression test** that fails without the fix and passes with it. Do not land a “fix” that only re-describes production logs; encode the failing case (fixture block, synthetic store, prevout/script edge) so it cannot silently come back.
- Run the new/related tests before commit (e.g. `cargo test -p <crate> …` or the scenario that covers the change). If a full-store mainnet case cannot run in this VM (see datadir notes), still add a synthetic/unit regression that pins the logic.

## Simplification / lean-code rules (apply while editing)

| Rule | Detail |
|------|--------|
| **Shared helpers** | Prefer one production implementation (composition or shared fn) over copy-paste probe/hash/layout math across modules. Put the helper in the **lowest crate that owns the concept** (`open_address` for FNV/open-hash, etc.). |
| **Invariants > silent fallbacks** | On confirm/store hot path, if prep or body load promised a fact (range present, packed decode, denserels for need_vouts), missing fact → `StoreError::Corrupt("invariant: …")` (or consensus wrap). Do **not** soft-continue to a colder path that hides bugs. Env/protocol multi-path (io_uring off, multi-spender list, RPC reconstruct) stays non-invariant. |
| **No test-only production APIs** | Do not add `*_for_test` / budget overrides / backdoors on production types when tests can use real clamps (large payloads, env, or public constructors). Prefer demoting or deleting over growing `cfg(test)` surface that does not exist for dependent crates. |
| **No re-implemented oracles in tests** | A test must drive the **shipped** function. Local helpers that re-code the unit under test and then “assert” that helper are test theater — delete them. |
| **Collapse same-entry duplicates** | Prefer one unit test next to the shipped path over twin unit+integration suites covering the same lines. Keep the closer entry-point test; drop the other only when coverage remains. |
| **Compile/test lean** | Prefer fewer full-store opens, less fixture copy-paste, and no giant dual test modules for the same slim/filter helper. Measure before claiming wall-time wins. |

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
   by gutting intentional caches (CreateResidency, ContigPark, archive budget).
2. **Archive queue ownership:** `charge` on first body enqueue must pair with
   exactly one `release` via `ArchiveResult` applied in `apply_archive_result`
   (or immediate release if `arch_job_tx.send` fails because the pipeline is
   **closed**). Dropping an `ArchiveJob` after charge without Ok/Err/Dropped is
   a **leak**.
3. **`archive_charged` is not hygiene-pruned** — only `clear_archive_charged` on
   pipeline result (prevents double-charge if ordered hygiene runs early).
4. **Abort paths** (WriterDead, stop, prep exit) must drain ContigPark + job
   channels and emit results (`release_remaining_jobs`).
5. **Soft archive budget is request-limited only.** Always read peer data and
   decode/enqueue blocks we already requested, even if that overshoots the soft
   queue budget. Bound memory by stopping new block **requests**
   (`can_assign` / getdata when archive is full) — never by stalling TCP reads,
   awaiting a decode permit on the reader before the next frame, or Full-dropping
   decoded bodies on a bounded `arch_job` channel. (That made healthy peers look
   stalled and was not a real leak fix.)
6. **Tests** must tear down intentional caches with **production** APIs (table
   below) — not a secret free-all that masks production leaks.
7. **Regression filters:**
   `force_advance_returns_parked_jobs_for_charge_release`,
   `multi_block_park_abort_releases_all_charges`,
   `multi_block_ibd_like_growth_then_production_abort_plateau`,
   `drain_job_rx_as_err_releases_via_apply`,
   `can_assign_stops_at_budget_charge_may_overshoot`,
   `archive_budget_charge_release_symmetric`,
   `presence_lifecycle`.

### Production clear / evict APIs (tests must call these)

| Structure | Production API |
|-----------|----------------|
| Archive queue charge | `ArchiveQueueBudget::charge`; release only via **`apply_archive_result`** on `ArchiveResult` **or** closed-pipeline `arch_job_tx.send` fail |
| Soft queue size | Bound **only** via **`can_assign`** / densify stop — never receive-side Full-drop or reader decode-permit wait |
| Arch job channel | **Unbounded** `arch_job` (always enqueue decoded first copy) |
| Block decode | Fire-and-forget **`spawn_decode_then_with_err`** — never await permit on the peer reader before next TCP read |
| WriterDead batch | **`emit_writer_dead_outcomes`** then apply results |
| ContigPark abort | **`release_remaining_jobs`** (or `force_advance` → **`emit_archive_job_dropped`**) |
| Forwarder stop | **`emit_archive_job_err`** + **`drain_job_rx_as_err`** |
| `archive_charged` marker | **`clear_archive_charged`** only (never hygiene-prune) |
| Confirm plans/headers | **`ConfirmParentCache::advance_tip`** (write `post_commit`) |
| CreateResidency | FIFO on **`CreateResidency::put_*` / `insert_fk_txid_range`** (create/out caps); sole pin map |
| Ordered maps | **`IbdWorkState::hygiene`** |
| Body presence | **`BodyPresence::hygiene_retain`** (rejected + charged retained by design) |
