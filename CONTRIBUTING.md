# Contributing

## Principles

1. Prefer design notes in [`docs/architecture.md`](./docs/architecture.md),
   [`SCHEMA.md`](./SCHEMA.md), and [`docs/crash-recovery.md`](./docs/crash-recovery.md)
   over inventing parallel specs.
2. Prefer **high-level functional/integration tests** over unit tests
   ([`COVERAGE.md`](./COVERAGE.md), [`TESTING.md`](./TESTING.md)).
3. Every PR must keep **≥90% line** coverage on first-party code (and ≥90%
   branch when measured on nightly) via `./scripts/coverage.sh` — same bar as CI.
4. Target is **production server-side** node software (wallet backends, etc.).
   Tip-mode mempool + tx relay are **in scope**; no pruning/GUI/end-user wallet/
   mining without an explicit plan change.
5. Store durability / tip commit: [`docs/crash-recovery.md`](./docs/crash-recovery.md)
   and [`docs/concurrency.md`](./docs/concurrency.md).
6. Security-sensitive reports go through [`SECURITY.md`](./SECURITY.md), not
   public issues.

## Workflow

Matches [`.github/workflows/ci.yml`](./.github/workflows/ci.yml). Required
checks are **separate jobs** on every push/PR (`fmt`, `deny`, `clippy`, `test`,
`multinode`, `coverage`) so a red run shows which gate failed without digging
into a monolithic job log.

**Agents** implement on a **git worktree** topic branch, commit per plan step,
and open **one PR** per plan. They do **not** run the full workspace suite or
coverage locally by default — they poll these Actions jobs to green. After
merge they remove the worktree and delete the local **and** remote topic
branch. See [`AGENTS.md`](./AGENTS.md) (worktree + PR, after-merge cleanup)
and [`docs/how-we-plan.md`](./docs/how-we-plan.md).

Humans who want the same gates offline:

```bash
nix develop   # or nix-shell — both pin via flake.lock (rust-toolchain.toml is 1.95.0)
cargo fmt --all -- --check
# rustc warnings are denied via workspace.lints + RUSTFLAGS=-Dwarnings
cargo build --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo deny check                 # advisories + licenses (deny.toml)
cargo build -p rbitcoin-node -p rbitcoin-cli
cargo test --workspace
./scripts/coverage.sh
```

### Release binaries (portable static, byte-identical)

Do **not** treat host or nix-shell `cargo build --release` digests as canonical
(and do not ship those binaries to operators — they are not portable). Use the
pinned **musl static** flake package and the double-build check:

```bash
nix build .#rbitcoin-musl
./scripts/repro-build.sh          # day-to-day one musl build (crane deps + app)
./scripts/repro-check.sh          # release only: two clean --rebuilds; compare SHA-256
```

See [`docs/reproducible-builds.md`](./docs/reproducible-builds.md).

Green **push** CI on `master`/`main` also runs [`.github/workflows/musl.yml`](./.github/workflows/musl.yml)
and uploads `rbitcoin-musl-x86_64-linux-<sha>` (90 days). That is a snapshot,
not the byte-identity gate (`repro-check.sh`). Download from the **musl**
check on the commit, not from the `ci` run.

## Commits

- Small, reviewable commits with complete sentences in the message body.
- Production code and its covering scenarios land together.
- Do **not** commit live `datadir-*/`, operator `*.log` dumps, secrets, or keys
  (see `.gitignore`).

## Code review checklist

- [ ] Behavior covered by a high-level scenario (or justified narrow test)
- [ ] No new silent dead branches
- [ ] Public API preferred over `#[cfg(test)]` white-box access
- [ ] Store changes respect Class A/B/C and allocate-then-publish
- [ ] Experimental / milestone honesty preserved in user-facing docs when relevant
