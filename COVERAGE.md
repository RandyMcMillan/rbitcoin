# Coverage policy

## Mandate

| Metric | Required |
|--------|----------|
| Line coverage | **100%** of first-party Rust code in the workspace |
| Branch coverage | **100%** of first-party Rust code in the workspace |

CI fails if either metric is below 100% on the measured set.

## Tooling

```bash
nix-shell
./scripts/coverage.sh
```

Uses `cargo llvm-cov` with branch instrumentation. Install if missing:

```bash
cargo install cargo-llvm-cov --locked
```

On Nix, prefer `cargo-llvm-cov` from nixpkgs when available, or install into a user cargo bin on `PATH`.

## What is measured

All workspace members that contain production code:

- `rbitcoin-primitives`, `rbitcoin-store`, `rbitcoin-query`, `rbitcoin-wire-cache`
- `rbitcoin-consensus`, `rbitcoin-net`, `rbitcoin-mempool`, `rbitcoin-wallet`
- `rbitcoin-rpc`, `rbitcoin-cli`, `rbitcoin-node`

**Excluded by default:** nothing. Third-party dependencies are not attributed to us.

## Philosophy

1. Cover code with **high-level functional/integration scenarios** (see [`TESTING.md`](./TESTING.md)).
2. Prefer expanding the harness over adding private unit tests.
3. If a branch is unreachable, **delete it** or add a public fault injector / config path so a scenario can hit it.
4. True unit tests only when a branch cannot be reached through any higher API without absurd cost — document the reason in the test file.

## Closing a red branch

1. Open the HTML/LCOV report from `./scripts/coverage.sh`.
2. Identify the uncovered line/branch.
3. Add or extend a **scenario** in `rbitcoin-test` (or an integration test binary) that triggers it through public surfaces.
4. Re-run `./scripts/coverage.sh` until green.

## Exclusions

The exclusion list should stay **empty**. Any proposed exclusion requires design review and an entry here with rationale. None today.
