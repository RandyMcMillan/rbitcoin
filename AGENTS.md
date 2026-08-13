# Agent notes

## Plain technical language

This is an engineering project. Write **clear, concrete technical English** in
code, comments, docs, commits, and PR text. Do **not** inject moralizing,
political framing, or performative “sensitivity” language. Prefer words that
describe the mechanism or policy accurately.

- **OK:** precise domain terms, plain failures (“reject”, “invalid”, “permanent
  blacklist” for a hash we never re-request, Core-aligned vocabulary where we
  are matching Bitcoin Core).
- **Also OK when clearer:** `allowlist` / `denylist` for sets of permitted or
  blocked names — they read better than color metaphors. Use them for clarity,
  not as a ritual rename of everything.
- **Not OK:** rewriting technical prose to satisfy fashion, adding equity
  disclaimers, or soft-pedaling consensus/security language so it sounds
  “inclusive.” Correctness and operator honesty first.

If you are unsure whether a wording change is engineering clarity or cultural
noise, keep the existing technical term (especially if it matches Core or our
logs) and move on.

## Prefer composition over inheritance in data models

Composition (has-a) is typically more clear and less error prone than
inheritance (is-a), although rust traits can make that blurry. Avoid tall
inheritance trees.

## Prefer immutable data structures

Immutable data structures built once then composed with other immutable
structures typically perform better than data structures mutated over time.

For example, prefer an immutable map that is built from the streamed results
of some work over a mutable hashmap. Even if the data structure itself is
mutable, in rust not having to make it mutable makes life better.

If we needed to add additional data to the members of a hashmap, we could
create an outer map that contains the additional info and annotates the
members of the inner map on read, converting them to the data type with the
additional fields (which will also have the inner object).

In short, prefer composition over mutation as well as composition over
inheritance.

## Scripthash (Class B) insert rules

| Rule | Detail |
|------|--------|
| Creates only | Index is thin `create_tx_fk` per Electrum scripthash (outputs); spends join Class A + annotations |
| Sorted FKs | Durable create_tx_fks per key are **strictly increasing** (within pages and across pages) |
| Insert | Max FK from **last page only** (or inline); **skip `fk ≤ max`** (re-queue OK); append higher only — **no full chain walk** on insert |
| Batch order | Callers must apply SH create batches in **non-decreasing block/batch time order** so skip-lower never leaves holes |
| Cold megakey | Pack each 4 KiB page in RAM with `next` predicted; **one write per page** (no previous-page RMW) |

See `SCHEMA.md` Class B and `scripthash_pages.rs`.

## Store concurrency: lock-free by default

**Default: no locks on the store hot path.** Concurrency is **roles + publish
order + HWM**, not map mutexes (maps removed — phase 6).

| Rule | Detail |
|------|--------|
| Roles | At most **one Class A appender** and **one spend annotator** per process; **N readers** of published ranges always free |
| Publish | body → idx → count/HWM (Release); then head / `header_txs` as visibility requires |
| Capacity grow | fallocate/`set_len` only (no map epochs); readers use published HWM |
| Layout grow (`tx.head`) | **segment roll**: seal open head (fuse8) + create new fixed 25-bit head — no mono-file bits-widen |
| Class C tip | L2 write-behind; `flush_class_c_tip` **before** body-queue dequeue |
| Not OK | Long-held store locks on IBD/read path, “pause all queries during confirm”, multi appenders |

If a change introduces a new long-held store lock on the IBD/read path, it is the
wrong design — fix the protocol. See `docs/concurrency.md`.

## On-disk format changes: warn, schema, or migrate

Changing any durable store bytes (table layout, side files like fuse8,
envelope version, encode of sealed products) must not surprise an operator with
a silent wipe / full head rebuild. Pick at least one:

| Option | When |
|--------|------|
| **Soft migrate** | Payload-only change (e.g. fuse8 v1→v2): open legacy, log a clear `warn!`, always-probe or dual-read, rewrite on open or next seal — **do not** treat decode failure as “recreate whole table” |
| **`SCHEMA_VERSION` bump** | Class A / OA / body layout change, or anything that cannot soft-open prior files |
| **Explicit refuse** | Incompatible durable state: hard error with a one-line wipe/reindex message (which files), not a cryptic `Corrupt` that cascades into head recreate |

Also document the change in `SCHEMA.md` / `SCHEMA_HISTORY.md` in the same
commit as the format code. Side-format version fields (e.g. BF8R version) are
not a substitute for operator-visible logs when migration runs. See fuse8 v1→v2
notes in `SCHEMA_HISTORY.md` (side format under schema 14).

## io_uring: do not flatten custom machines

**Under no circumstances** replace a purpose-built / multi-stage **io_uring
machine** (fused resolve, spend-annotate RMW, pipeline stages, depth-round
machines, etc.) with “simple” batched `pread`/`pwrite` / one-shot
`pread_batch`/`pwrite_batch` submission **without explicit permission from the
user**.

| OK | Not OK without permission |
|----|---------------------------|
| Fix bugs inside the existing machine | Delete/retire a custom machine and call bulk batch helpers instead |
| Thread new flags (e.g. DONTCACHE) through the same SQE path | “Simplify” to serial pread + one big submit for a path that had a staged machine |
| Fall back to pread when uring is unavailable (existing policy) | Rewrite a machine away “because batch is enough” |

If a change seems to require collapsing a machine, **stop and ask** — do not
land the simplification as a drive-by cleanup.

## Create pins: pipeline-local only (no process FIFO)

| Rule | Detail |
|------|--------|
| Pin material | **Plan / batch only** — `batch_pin`, `BatchParents`, plan-local **sparse** `external_parent_outs` (`SparseExternalPin`). SharedParentPin = immutable body compose. No process create pin FIFO |
| IBD confirm intake | **body queue wire only** → lookup → load (no hash-only / Class-A-only confirm) |
| Ancient parents | Cold Class A **`txout` outs** into plan-local / BatchParents only |
| Header plans | **ConfirmParentCache** always on (MTP / tip-ahead headers) |
| Removed | **CreateResidency**, **OutFifo**, **archive sticky**, half-row / out-slim, **`RBITCOIN_CONFIRM_CACHE`**, **`RBITCOIN_RESIDENCY_BYTES`** |
| IBD sizes | **`conf_plans=`** + body-queue / pipeline meters (no `residency creates=`) |


## GitHub CI must stay green (every commit)

CI is [`.github/workflows/ci.yml`](.github/workflows/ci.yml) (push/PR to
`master`/`main`). Required checks are separate jobs so the push/PR UI shows
which gate failed: **`fmt`**, **`clippy`**, **`test`**, **`multinode`**, **`coverage`**.
**Do not push or leave a commit that would fail any of them.** A red CI on
`master` is incomplete work.

### Required before each code commit

From `nix-shell` (or the same **rustc 1.95** class CI pins). The shell sets
`CARGO_TARGET_DIR=target/dev` so host test/clippy objects stay out of the
coverage tree (`target/cov` via `./scripts/coverage.sh`). Musl ship binaries
come only from `nix build .#rbitcoin-musl` → install into `target/release/`
(operator path; not the cargo debug target) — and **only on `master` after
commit** (see below).

```bash
cargo fmt --all -- --check          # if dirty: cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

| CI job | Local command | Expectation |
|--------|---------------|-------------|
| **fmt** | `cargo fmt --all -- --check` | Clean (default rustfmt; no project `rustfmt.toml`) |
| **clippy** | `clippy … -D warnings` | Clean under `[workspace.lints.clippy]` allows in root `Cargo.toml` |
| **test** | `cargo test --workspace` (+ CI also builds node/cli bins) | All non-ignored tests pass |
| **coverage** | `./scripts/coverage.sh` | ≥90% first-party LCOV; job waits on fmt+clippy+test |

**Toolchain:** CI pins **rustc 1.95.0** (same class as `nix-shell` / crane via
`nixos-26.05` in `flake.lock`). Do not rely on host “latest stable” alone.
Expand clippy allows only for real noise after a toolchain bump — prefer
fixing the code.

### Multi-step plan execution (targeted mid-plan; full gates at end)

When executing an **approved multi-step plan** (see [`docs/how-we-plan.md`](docs/how-we-plan.md)):

| Phase | Expectation |
|-------|-------------|
| **Each intermediate step** | Targeted tests for crates/modules touched; logical commits with public hygiene. Do **not** require full workspace suite, full coverage, or musl install after every slice. |
| **Plan complete / before calling the plan done** | Full local gates: fmt, workspace clippy `-D warnings`, `cargo test --workspace`, `./scripts/coverage.sh` (≥90%). **Musl only if** the work is finished **on `master` after commit** (merge/rebase first if the plan lived on a feature branch). |
| **Push to master** | Still must keep CI green — do not push intermediate commits that fail required jobs (`fmt` / `clippy` / `test` / `multinode` / `coverage`) if you push them at all; prefer finishing the plan then push, or ensure each pushed commit at least passes what CI runs. |

Single-shot turns (one bugfix, no multi-step plan) follow the commit recipe
below; musl only when that commit lands on **`master`**.

### Coverage job

`./scripts/coverage.sh` enforces **≥90% first-party line coverage** (LCOV
`LH`/`LF`; see `COVERAGE.md`). It runs as a **required** CI job (slow). Prefer
running it when touching store/query/consensus hot paths. Prefer not to grow
uncovered production regions; the 90% bar applies to new and existing code.
During multi-step plans, run coverage at **plan end** (not every step) unless
the step’s contract is the coverage gate itself.

If a change cannot pass required gates, **do not commit it as done** — fix, split,
or get explicit user approval for a temporary exception (prefer none).

## Commit + static musl release after code changes

### Public commit hygiene (required)

This tree is **public**. Every commit must:

| Rule | Detail |
|------|--------|
| **One logical change** | One concern per commit (one bug fix, one feature slice, one docs rule, one refactor). Do not bundle unrelated edits. |
| **Small** | Prefer a sequence of small commits over one mega-commit. Checkpoint before risky follow-ons so rollback is easy. |
| **Clear message** | Subject + body state **what** changed and **why** in complete sentences. Assume readers have no chat context. |
| **Not** | “WIP”, “misc”, “fix stuff”, drive-by renames mixed with behavior, or multi-hour experiments left as one opaque blob. |

Green-then-refactor is fine as **two** commits when each stands alone (tests still pass at each).

Whenever a turn **changes code** (or you finish a multi-step coding task in that turn):

1. **Pass tests for what you touched** (targeted during multi-step plans; full
   workspace suite when finishing a plan or for single-shot turns). A commit that
   would fail GitHub Actions `test` is incomplete work if pushed.
2. **Commit** following the public hygiene table above. Prefer one commit per logical checkpoint — especially before starting a risky follow-on experiment, so we can roll back. Do **not** leave multi-hour IBD perf/refactor work uncommitted.
3. **Musl install (strict):** build and install the portable static musl release
   **only when both** hold:
   - current branch is **`master`** (or `main` if that is the default); and
   - the tree is **clean after a successful commit** of the change (or the
     commit that finished a multi-step plan on master).

   | Situation | Musl? |
   |-----------|--------|
   | Feature / plan branch (`rpc/…`, `feat/…`, …) | **No** — even at plan end on that branch |
   | On `master`, after commit of code that ships in the node/cli | **Yes** — one `nix build .#rbitcoin-musl` + install (recipe below) |
   | Uncommitted dirty tree | **No** — commit first; never install from uncommitted work |
   | Cannot commit (hooks, secrets, user said not to) | **No** musl; say the tree was not committed and ship binary was **not** refreshed |
   | Pure docs/discussion, no compile-affecting edits | Skip commit and musl |

   Multi-step plans: no musl on intermediate slices; at plan end, full suite +
   coverage on the plan branch as usual, then **merge/rebase to master, commit
   if needed, then one musl** so `./target/release/rbitcoin-node` matches master.

### Required recipe (only this — single `nix build`; **master + post-commit only**)

```bash
# Preconditions (do not skip):
#   git branch --show-current   # must be master (or main)
#   git status -sb              # clean working tree after the commit
nix build .#rbitcoin-musl --out-link result
mkdir -p target/release
install -m 755 result/bin/rbitcoin-node result/bin/rbitcoin-cli target/release/
file target/release/rbitcoin-node   # must say "statically linked" (musl)
```

Musl builds use **crane** (deps derivation + app derivation). After the first
full deps build, **crate-only edits** recompile workspace crates against a
cached `cargoArtifacts` layer — still one `nix build`, not a host `cargo
build --release`.

### Do **not** run for day-to-day agent turns

| Command | When |
|---------|------|
| `./scripts/repro-check.sh` | **Release / digest gate only** — realize + **two** forced `--rebuild`s. Slow by design. Never as the post-edit install step. |
| `./scripts/repro-check.sh both` | Even heavier (musl + glibc). Release only. |
| `nix build .#rbitcoin-musl` on a feature branch | Agent workflow: **never** — only on master after commit |
| `nix build .#rbitcoin-musl` with uncommitted edits | **Never** — commit first |

On master after commit, portable install = **one** `nix build .#rbitcoin-musl`
(recipe above). Byte-identity claims for a revision = `./scripts/repro-check.sh`
once at release.

### Forbidden for the operator binary

| Do **not** run | Why |
|----------------|-----|
| `nix-shell --run 'cargo build -p rbitcoin-node --release'` | Dynamic **Nix glibc** link; dies off-store with `No such file or directory` |
| `cargo build --release` (host toolchain) | Same class of non-portable binary |
| Leaving `target/release/` as the last **debug** or glibc build | User restarts IBD from that path |

`nix-shell` / `cargo test` for **tests** is fine. Only the **shipped** node/cli
under `target/release/` must come from `nix build .#rbitcoin-musl` (and only
refreshed from master post-commit per the table above).

## How we plan

Multi-step work is planned as **many small vertical slices**, each roughly one
**Red → Green → Refactor** cycle — not a few large “implement phase N” blocks.
Prefer more steps with explicit contracts and test budgets over horizontal
layering (all store, then all consensus, then wire). Full guide:

**[`docs/how-we-plan.md`](docs/how-we-plan.md)** — XP/INVEST-inspired stories,
step template, spikes, suite-speed as a planning constraint, anti-patterns.

When writing or executing a plan/PR plan: every step should name **Contract,
Red, Green, Refactor, Verify** before production code for that step.

## Test-driven development (required for behavioral changes)

**Default: no production code change without a test that fails first and pins
exactly the contract you are fixing or adding.** “When practical” is not an
escape hatch for bugfixes or hot-path behavior. Pure docs/comments/formatting
need no tests. If a full mainnet case cannot run in this VM (see datadir notes),
still encode a synthetic/scenario regression that drives the **shipped** path.

Thrashing (heal → unheal, soft-requeue → permanent, walk-seed → plan-lookup)
comes from coding to logs instead of a failing assertion. A precise failing
test forces a **surgical** green, then a clean **refactor** under green tests
instead of leaving one-off patches everywhere.

### Virtuous cycle: Red → Green → Refactor (agile TDD)

Do not stop at “tests pass.” The suite is the safety net that lets you
**integrate** the fix into the design without re-breaking the contract.

| Phase | Goal | Rules |
|-------|------|--------|
| **Red** | Encode the contract | Failing test only. No production edit yet. Prove the path if non-obvious. |
| **Green** | Make it pass | **Smallest surgical** production change. One-offs and local branches are OK *temporarily* to get green. Do not refactor and invent design in the same breath as the first fix. |
| **Refactor** | Remove the one-off | With **all** relevant tests still green, fold the fix into the real shape: shared helper, right stage (lookup vs load), one policy site, delete dead dual paths. Re-run tests after each refactor step. |

| Anti-pattern | Prefer |
|--------------|--------|
| Ship the first green hunk forever (copy-paste guards, “if mainnet retarget…” special cases next to every caller) | One production implementation at the **lowest owner** of the concept; callers stay dumb |
| Big redesign before any green test | Green first, then refactor under the suite |
| “Refactor” that weakens or deletes the red test | Keep the contract pin; only collapse **duplicate** tests of the same entry (lean-code rules) |
| New soft path / heal beside the real path so tests pass | Fix the protocol; invariants over silent fallbacks |

Commit after green when the checkpoint is useful (especially before a risky
refactor). Prefer the refactored form in the final commit of the change when
it stays small; otherwise green commit then refactor commit — both must stay green.

### Order of work (bugs and features)

| Step | Required |
|------|----------|
| 1. Reproduce | Name the failing contract in one sentence (error string, invariant, observable outcome). Prefer static proof of the code path from production entry → bug when the failure is non-obvious. |
| 2. Red | Add or extend a test that **fails without the change** and would pass only if that contract holds. Run it; capture the fail. |
| 3. Green | Implement the **smallest** production change that makes that test pass. Do not expand scope mid-fix. |
| 4. Refactor | Still green: integrate the fix into shared structure / the correct stage; delete one-offs and dual paths introduced only to get green. Re-run the new and related tests. |
| 5. Before commit | `cargo test -p <crate> …` (or scenario) for everything touched; do not land known red. |

For **performance**, prefer a before/after benchmark or metered scenario that
shows the win; do not land “perf” rewrites with only correctness tests. Same
cycle: red (or baseline bench) → green (measured win) → refactor without
losing the win.

### What the test must assert

| Do | Do not |
|----|--------|
| Assert the **exact** bug/feature contract (e.g. `expected_bits` with period-start **only** on header plan; pin identity mismatch → invariant, not soft spentness) | Vague “does not panic” / “returns Ok” without encoding the failure mode |
| Drive the **shipped** function or pipeline stage under test | Local helpers that re-implement production and then assert the helper |
| Fail with the **same class of error** (or wrong result) seen in prod when the bug is present | Comment-only or log-narrative “tests” with no executable pin |
| Keep fixtures minimal and synthetic (`/tmp`, tiny head scale) | Require full mainnet datadir open in the agent VM |

### Scenario vs unit — prefer scenarios, balance cost

**Prefer scenario / integration tests** (`rbitcoin-test`, multi-stage confirm
scenarios, store open→append→read) when they exercise the real entry point
cheaply enough. They catch IO-split, tip-ahead, and stage-boundary bugs unit
tests miss.

**Also keep focused unit tests** next to the code (`#[cfg(test)]` in the same
crate) when:

- the scenario would be **slow**, multi-GB, or hard to set up for one branch;
- the bug lives in a **pure helper** on the hot path (bits, range decode, append guards);
- you need a **fast red/green loop** while iterating the fix.

| Goal | How |
|------|-----|
| Quick CI / agent loop | Unit or slim scenario; avoid full-store opens and duplicate suites for the same lines |
| Good practical coverage | One scenario at the entry that mattered in prod **or** one unit on the exact shipped fn — not both for the same lines unless the scenario cannot reach the branch |
| Cheap later refactors | Assert **behavior/contracts**, not private layout trivia; collapse twin unit+integration for the same entry (see lean-code rules) |

Do **not** grow a second full-store suite “for completeness” when a tighter test
already pins the fix. Do **not** skip the failing test because “we’ll add
coverage later.”

## Simplification / lean-code rules (apply while editing)

| Rule | Detail |
|------|--------|
| **Shared helpers** | Prefer one production implementation (composition or shared fn) over copy-paste probe/hash/layout math across modules. Put the helper in the **lowest crate that owns the concept** (`open_address` for FNV/open-hash, etc.). |
| **Invariants > silent fallbacks** | On confirm/store hot path, if load or body load promised a fact (range present, `txout` decode, outs for need_vouts, **spent_range abs**, **parent create identity for a pin**), missing fact → `StoreError::Corrupt("invariant: …")` (or consensus wrap). Do **not** soft-continue to a colder path that hides bugs. Env/protocol multi-path (io_uring off, multi-spender list, RPC reconstruct) stays non-invariant. |
| **No spentness fallbacks for load bugs** | Tip-follow and IBD confirm **must not** recover from wrong/missing pin `create_fk` identity or missing `spent_range` abs by soft spentness paths (thin-as-hint, unpinned wire-corrected idx spentness, reject-only wire re-checks). Fix load/stamp so parent identity matches wire `prev_txid` **before** structural (plan RAM reverse map, else `txid.body`; spent-range ensure when a Class A plan exists). False `PrevoutSpent` from zero-identity pins is a **load bug**, not a spentness oracle problem. |
| **Same-block / corrupt spender meta** | Same-block spends (`create_fk` null) use **pending only** — never durable store-by-txid (Class A rehydrate already holds those creates). Sole `spent` slots whose confirmed-strong spender height **predates** the create height are **impossible** (annotate corruption) — ignore as unspent, do not surface as consensus `PrevoutSpent`. |
| **No test-only production APIs** | Do not add `*_for_test` / budget overrides / backdoors on production types when tests can use real clamps (large payloads, env, or public constructors). Prefer demoting or deleting over growing `cfg(test)` surface that does not exist for dependent crates. |
| **No re-implemented oracles in tests** | A test must drive the **shipped** function. Local helpers that re-code the unit under test and then “assert” that helper are test theater — delete them. |
| **Collapse same-entry duplicates** | Prefer one unit test next to the shipped path over twin unit+integration suites covering the same lines. Keep the closer entry-point test; drop the other only when coverage remains. |
| **Compile/test lean** | Prefer fewer full-store opens, less fixture copy-paste, and no giant dual test modules for the same slim/filter helper. Measure before claiming wall-time wins. |
| **No production-scale fixtures in default unit tests** | Do **not** pin production constants as test IO size when a smaller N still hits the branch: e.g. `FANIN_TARGET_STREAM_RUNS` (4096) run files, multi‑GiB / mainnet heads under `cargo test`, or remine 100-block maturity pads with `confirm_wire_run`. Use tiny stream targets / `RBITCOIN_HEAD_SCALE=tiny` / `pad_empty_from`. Pure math may still assert production geometry. See [`TESTING.md`](TESTING.md) suite-speed budgets; new default tests **&gt;2 s** wall need PR justification. |

## Datadir / store on this workspace (do not open in the agent VM)

The workspace is mounted into the agent VM as **9p** (`workspace` on `/home/agent/workspace`, `trans=virtio`). On this mount:

- Store/mempool tables are **map-free** (pread/pwrite only) — open should work without `MAP_SHARED`.
- Prefer `/tmp` fixtures for agent correctness tests (synthetic stores).

**Perf A/B** is **operator-host only**, with the musl static binary — never agent-VM timings. See [`docs/io-modality.md`](docs/io-modality.md).

### What works instead

- Read **logs** the user leaves in-tree (`signet-ibd.log`, etc.).
- Inspect store files with **pread**/Python struct parsing of HWMs, headers when useful for offline forensics.
- Reproduce with **synthetic fixtures** and `rbitcoin-test` scenarios under `/tmp`.
- Ask the user to run the node / confirm diagnostics / **host musl benches** on their host (normal local FS).

## No dead code warnings silenced unless there is an absolutely bulletproof justification.

Do not leave dead code around. Delete it. Don't silence warnings unless there
is bulletproof justification

Same goes for #[cfg(test)].

## IBD / process memory leak prevention

**Full rules:** `docs/ibd-memory.md`. Summary for agents:

1. **Distinguish** process-owned heap (Rust structures, confirm pipeline wire,
   in-RAM body queue) from **kernel page cache** under store mmaps (`RssFile`).
   Do not “fix” RSS by gutting intentional caches (body queue, ConfirmParentCache header plans).
2. **Unified path only:** peer → in-RAM **body queue** → confirm lookup/load/
   scripts/commit (sole Class A). **No** dual-track `ArchiveJob` / ContigPark.
   Unknown-height `BlockFramed` → `mark_missing` and re-getdata after height.
   Body queue is **RAM-only** (redownload on restart) to avoid double disk write
   of every block; soft densify assign uses two limits (no hysteresis): under
   ~100 MiB free densify ahead; over ~100 MiB only heights confirm will consume
   in the next ~1 min at tip rate.
3. **Soft budgets are request-limited only.** Always accept already-requested
   block bytes into the body queue (`block_queue_offer` ignores soft assign
   limits), even if that overshoots soft depth. Bound memory by limiting new
   densify **getdata assign** — never by stalling TCP reads or Full-dropping
   bodies already on the wire.
4. **Tests** must tear down intentional caches with **production** APIs (table
   below) — not a secret free-all that masks production leaks.
5. **Regression filters:** body-queue soft depth / presence lifecycle / confirm
   reject paths as listed in `docs/ibd-memory.md`.

### Production clear / evict APIs (tests must call these)

| Structure | Production API |
|-----------|----------------|
| Soft densify depth | Bound **only** via body-queue soft assign (100 MiB free / 1 min confirm window) — never receive-side Full-drop |
| Confirm plans/headers | **`ConfirmParentCache::advance_tip`** (write `post_commit`) |
| Pipeline pins | Drop with plan/batch; **no** process pin FIFO. Tests tear down via production plan drop / batch drop |
| Ordered maps | **`IbdWorkState::hygiene`** |
| Body presence | **`BodyPresence::hygiene_retain`** (rejected retained by design) |
