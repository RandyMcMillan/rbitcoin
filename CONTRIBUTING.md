# Contributing

## Principles

1. Prefer design notes in [`docs/architecture.md`](./docs/architecture.md),
   [`SCHEMA.md`](./SCHEMA.md), and [`IMPLEMENTATION-PLAN.md`](./IMPLEMENTATION-PLAN.md)
   over inventing parallel specs.
2. Prefer **high-level functional/integration tests** over unit tests
   ([`COVERAGE.md`](./COVERAGE.md), [`TESTING.md`](./TESTING.md)).
3. Every PR must keep **100% line and 100% branch** coverage on first-party code
   (`./scripts/coverage.sh` — same bar as CI).
4. Tip-mode mempool + tx relay are **in scope**; no pruning/GUI/wallet/mining
   without an explicit plan change.
5. Store durability follows
   [`libbitcoin-durable-archive-variant.md`](./libbitcoin-durable-archive-variant.md).
6. Security-sensitive reports go through [`SECURITY.md`](./SECURITY.md), not
   public issues.

## Workflow

Matches [`.github/workflows/ci.yml`](./.github/workflows/ci.yml):

```bash
nix develop   # or nix-shell — both pin via flake.lock
cargo fmt --all -- --check
# rustc warnings are denied via workspace.lints + RUSTFLAGS=-Dwarnings
cargo build --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
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
