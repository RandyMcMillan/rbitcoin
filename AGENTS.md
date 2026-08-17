# Agent notes

Documentation map (one owner per fact): [`docs/README.md`](docs/README.md).
This file is the harness-injected **hard-rule** contract. Design lives in
the owner docs; do not grow a second design book here.

## Plain technical language

Write **clear, concrete technical English** in code, comments, docs, commits,
and PR text. Do **not** inject moralizing, political framing, or performative
“sensitivity” language.

- **OK:** precise domain terms, plain failures (“reject”, “invalid”,
  “permanent blacklist”), Core-aligned vocabulary where we match Bitcoin Core.
- **Also OK when clearer:** `allowlist` / `denylist` for permitted or blocked
  names — for clarity, not as a ritual rename.
- **Not OK:** fashion rewrites, equity disclaimers, or softening
  consensus/security language.

If unsure whether a wording change is engineering clarity or cultural noise,
keep the existing technical term (especially if it matches Core or our logs).

## Comments are a smell

Do not restate *what* the next statements do, *why* they exist, or *why* the
approach is weird. Prefer names, types, and structure. Remaining `//` only for
an invariant, protocol rule, or `SAFETY` the types cannot state, or a library
quirk that would otherwise look like a bug. Crate/public rustdoc (`//!` /
`///`) that documents a surface is not this rule.

Full text and review checklist: [`CONTRIBUTING.md`](CONTRIBUTING.md) principle 7.

## Composition and immutability

Prefer composition (has-a) over inheritance (is-a); avoid tall trees. Prefer
immutable structures built once, then composed. If a hashmap needs extra
fields, wrap it on read rather than mutating members in place.

## Store

### Lock-free by default

**No locks on the store hot path.** Concurrency is roles + publish order +
HWM. Roles: [`docs/concurrency.md`](docs/concurrency.md). Heads:
[`docs/heads.md`](docs/heads.md). Class B insert geometry:
[`SCHEMA.md`](SCHEMA.md) (Class B).

| Rule | Detail |
|------|--------|
| Roles | At most **one Class A appender** and **one spend annotator** per process; **N readers** of published ranges always free |
| Publish | body → idx → count/HWM (Release); then head / `header_txs` as visibility requires |
| Capacity grow | fallocate/`set_len` only; readers use published HWM |
| Layout grow (`tx.head`) | **segment roll**: seal open head (fuse8) + create new fixed 25-bit head |
| Class C tip | L2 write-behind; `flush_class_c_tip` **before** body-queue dequeue |
| Not OK | Long-held store locks on IBD/read path, “pause all queries during confirm”, multi appenders |

Do **not** reintroduce CreateResidency, OutFifo, ContigPark, archive sticky,
process pin FIFO, or map epochs.

### On-disk format: warn, schema, or migrate

Changing durable store bytes must not surprise an operator with a silent wipe.

| Option | When |
|--------|------|
| **Soft migrate** | Payload-only (e.g. fuse8 v1→v2): open legacy, `warn!`, rewrite on open or next seal — **not** “recreate whole table” |
| **`SCHEMA_VERSION` bump** | Class A / OA / body layout change, or anything that cannot soft-open prior files |
| **Explicit refuse** | Hard error with a one-line wipe/reindex message (which files) |

Document in `SCHEMA.md` / `SCHEMA_HISTORY.md` in the same commit as the format
code.

### io_uring: do not flatten custom machines

**Do not** replace a purpose-built / multi-stage **io_uring machine** with
batched `pread`/`pwrite` / one-shot `pread_batch`/`pwrite_batch` **without
explicit permission from the user**.

| OK | Not OK without permission |
|----|---------------------------|
| Fix bugs inside the existing machine | Delete/retire a custom machine and call bulk batch helpers |
| Thread new flags through the same SQE path | “Simplify” to serial pread + one big submit |
| Fall back to pread when uring is unavailable | Rewrite a machine away “because batch is enough” |

If a change seems to require collapsing a machine, **stop and ask**.

### Pins: pipeline-local only

Pin material is **plan / batch only** (`batch_pin`, `BatchParents`). No process
create pin FIFO. IBD confirm intake
is **body queue wire only** → lookup → load.

Leftover union, stage IO, S0–S4: **[`docs/invariants.md`](docs/invariants.md)**
(the only Allowed/Forbidden IO table). In-flight prune after pin + scripts
handoff; no leftover pending map; union miss is permanent.

### Confirm pipeline timers

Anything on lookup / load / scripts / write (or a sidecar the write thread
joins) gets a **named** `ibd: perf` timer **in the same commit**. Inventory
lives in `crates/rbitcoin-net/src/ibd/perf_log.rs` — keep that list honest;
do not copy it here.

## Ship via worktree + pull request

```text
worktree branch → many small commits → one PR → poll GitHub Actions → green PR
```

A plan is **not complete** until that PR’s **required** checks are green.

```bash
git fetch https://github.com/reardencode/rbitcoin.git master:refs/remotes/origin/master
git worktree add -b <area>/<short-name> /tmp/rbtc-<short> origin/master
export CARGO_TARGET_DIR=/tmp/rbtc-<short>/target/dev
```

| Rule | Detail |
|------|--------|
| Base | Current `origin/master` (or `main`) |
| Branch | Topic name — **never** commit the plan onto `master` |
| `CARGO_TARGET_DIR` | Inside the worktree (`…/target/dev`) |
| Identity | Worktree-only `git config --worktree user.name` / `user.email` for bot commits |
| Remotes | Worktrees **share** `origin`. Never `git remote set-url origin`. |

### After merge cleanup

Once the PR is **merged** (not while open):

```bash
git worktree remove /tmp/rbtc-<short>
git branch -d <area>/<short-name>
git push https://github.com/reardencode/rbitcoin.git --delete <area>/<short-name>
git fetch https://github.com/reardencode/rbitcoin.git --prune
```

Keep `master` / `main`, the primary checkout, and any **open-PR** worktree.
Do **not** delete a branch that still has an open PR. Do **not**
`git push --delete master`.

### Local tests (thin on purpose)

From `nix-shell` (CI pins **rustc 1.95.0**). Shell `CARGO_TARGET_DIR=target/dev`.

| When | Run |
|------|-----|
| **Each plan step / single-shot** | Targeted `cargo test -p <crate> …` (or slim scenario). `cargo fmt --all` if dirty. |
| **Not by default** | `cargo test --workspace`, `./scripts/coverage.sh`, workspace clippy, `nix build .#rbitcoin-musl` |
| **Exception** | User asked for a local full suite, or you cannot push and must prove gates offline |

Do **not** wait out a host IBD or a 90% coverage run in the agent VM. GitHub
Actions is the workspace/coverage/clippy gate.

### Push, PR, poll CI

Required jobs: **`fmt`**, **`deny`**, **`clippy`**, **`test`**, **`multinode`**,
**`coverage`**. `musl.yml` / `windows.yml` / `macos.yml` are **not** required.
Label **`core-functional`** when the PR touches the Core functional harness.
Label **`static-binaries`** to build musl / Windows / Darwin operator
snapshots on that PR (same jobs as green `master` `ci`).

`origin` stays **SSH**. This VM has **no** GitHub App SSH key. The App token
from `~/.config/rbitcoin-grok/gh-login.sh` (~1h) is HTTPS-only.

`gh pr create` / `gh pr checks` talk to the API. `git fetch` / `git push` as
the bot must use an **explicit HTTPS URL**. Do **not** `git remote set-url
origin`. Do **not** `git push origin` as the bot.

```bash
~/.config/rbitcoin-grok/gh-login.sh
git fetch https://github.com/reardencode/rbitcoin.git master:refs/remotes/origin/master
git push https://github.com/reardencode/rbitcoin.git HEAD:<area>/<short-name>
gh pr create --repo reardencode/rbitcoin --head <area>/<short-name> --title "…" --body "…"
gh pr checks --watch
```

No `-u` on push (that would retarget the branch remote away from `origin`).

| Rule | Detail |
|------|--------|
| **One PR per plan** | Push more commits to the same branch. |
| **Poll until green** | Do not walk away and call the plan done. |
| **Done** | Required checks green **and** the PR is up for review. Do not merge unless asked. |
| **Do not** | Force-push `master`, merge a red PR, rewrite `origin` to HTTPS, or skip polling because “tests passed locally.” |

Coverage (≥90% LCOV `LH`/`LF`) is a required CI job — see
[`TESTING.md`](TESTING.md). If CI `coverage` fails, add a pin and push.

Plans: [`docs/how-we-plan.md`](docs/how-we-plan.md). Each step names
**Contract, Red, Green, Refactor, Verify**. Many small vertical slices.

## Commit hygiene

This tree is **public**.

| Rule | Detail |
|------|--------|
| **One logical change** | One concern per commit. |
| **Small** | Sequence of small commits; checkpoint before risky follow-ons. |
| **Clear message** | Subject + body: **what** and **why**. No chat context assumed. |
| **Not** | “WIP”, “misc”, drive-by renames mixed with behavior. |

Green-then-refactor is fine as **two** commits when each stands alone.

1. Pass targeted tests for what you touched.
2. Commit. A plan is **many commits, one PR**.
3. Push the worktree branch and open or update the plan PR. Poll to green.
4. **Musl install only after merge onto `master`/`main`**, tree clean, and the
   node/cli binary changed:

```bash
nix build .#rbitcoin-musl --out-link result
mkdir -p target/release
install -m 755 result/bin/rbitcoin-node result/bin/rbitcoin-cli target/release/
file target/release/rbitcoin-node   # must say "statically linked" (musl)
```

Do **not** `nix build .#rbitcoin-musl` on a feature branch or with uncommitted
edits. Do **not** run `./scripts/repro-check.sh` as the day-to-day install
(release / digest gate only). Do **not** ship `nix-shell` / host
`cargo build --release` as the operator binary (Nix/host glibc; dies off-store).

## Test-driven development

**No production code change without a test that fails first** and pins the
exact contract. Pure docs/comments/formatting need no tests. Encode a
synthetic `/tmp` scenario that drives the **shipped** path — do **not** open
a mainnet datadir in the agent VM. Perf A/B is operator-host only.

| Phase | Goal | Rules |
|-------|------|--------|
| **Red** | Encode the contract | Failing test only. No production edit yet. |
| **Green** | Make it pass | **Smallest surgical** change. One-offs OK *temporarily*. |
| **Refactor** | Remove the one-off | Still green: fold into the real shape; delete dual paths. |

| Anti-pattern | Prefer |
|--------------|--------|
| Ship the first green hunk forever | One implementation at the **lowest owner** |
| Big redesign before any green test | Green first, then refactor |
| “Refactor” that deletes the red test | Keep the contract pin |
| New soft path so tests pass | Fix the protocol |

| Step | Required |
|------|----------|
| 1. Reproduce | Name the failing contract in one sentence. |
| 2. Red | Test that **fails without** the change. |
| 3. Green | Smallest production change that passes. |
| 4. Refactor | Integrate; delete one-offs. |
| 5. Before commit | Targeted `cargo test -p <crate> …`. Workspace suite waits for Actions. |

The test must assert the **exact** contract, drive the **shipped** function,
fail with the **same class of error**, and use tiny `/tmp` fixtures.

Prefer scenarios at the real entry when cheap; unit tests for pure helpers or
when a scenario would be slow / multi-GB. One entry per production path.

## Lean-code rules

| Rule | Detail |
|------|--------|
| **Shared helpers** | One production implementation at the **lowest crate** that owns the concept. |
| **Invariants > silent fallbacks** | Missing promised fact → `StoreError::Corrupt("invariant: …")`. Do **not** soft-continue. |
| **No spentness fallbacks for load bugs** | Wrong/missing pin `create_fk` is a **load bug**. Fix stamp/identity first. |
| **Same-block / corrupt spender meta** | Same-block spends use **pending only**. Spender height before create → ignore as unspent. |
| **No test-only production APIs** | No `*_for_test` backdoors when tests can use real clamps. |
| **No re-implemented oracles in tests** | Drive the shipped function. |
| **No repo-text tests** | Do not `include_str!` production `.rs` / markdown and `contains` identifiers. See CONTRIBUTING principle 8. |
| **Collapse same-entry duplicates** | One closer test; drop the twin when coverage remains. |
| **Compile/test lean** | Fewer full-store opens; measure before claiming wall-time wins. |
| **No production-scale fixtures** | Tiny N / `RBITCOIN_HEAD_SCALE=tiny` / `pad_empty_from`. See [`TESTING.md`](TESTING.md). New default tests **>2 s** wall need PR justification. |

Do not leave dead code. Do not silence dead-code / `#[cfg(test)]` warnings
without a bulletproof justification — delete the code.

## IBD memory

**Full rules:** [`docs/ibd-memory.md`](docs/ibd-memory.md).

1. Distinguish process heap from kernel page cache (`RssFile`). Do not “fix”
   RSS by gutting the body queue or ConfirmParentCache.
2. Unified path only: peer → in-RAM **body queue** → confirm. **No**
   ArchiveJob / ContigPark. Body queue is RAM-only (redownload on restart).
3. Soft budgets are **request-limited only**. Always accept already-requested
   block bytes. Bound memory by limiting new densify **getdata assign**.
4. Tests tear down caches with **production** APIs listed in that doc.
