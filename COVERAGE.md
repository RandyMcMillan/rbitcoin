# Coverage policy

## Mandate

| Metric | Required |
|--------|----------|
| Line coverage | **≥ 90%** of first-party executable lines (LCOV `LH`/`LF` from `./scripts/coverage.sh`) |
| Branch coverage | **≥ 90%** when measured on nightly with `--branch`; on stable, region-partial lines in the text report may remain — still close large gaps via scenarios |

CI fails if measured line coverage is **below 90%**. New and existing first-party code share this bar.

**Note:** `cargo llvm-cov`'s text “Missed Lines” column can count *partial regions within a line* (for example match or-patterns) even when the line executed. The gate uses LCOV line hit/total (`LH`/`LF`). HTML remains a diagnostic report under `coverage/`.

## Tooling

```bash
nix-shell
./scripts/coverage.sh
```

Uses `cargo llvm-cov` with optional branch instrumentation. Local install if missing:

```bash
cargo install cargo-llvm-cov --locked
```

On Nix, prefer `cargo-llvm-cov` from nixpkgs when available, or install into a user cargo bin on `PATH`.

**CI:** the `coverage` job installs a **prebuilt** `cargo-llvm-cov@0.6.14` via
`taiki-e/install-action` — it does **not** `cargo install` from crates.io on every PR.

**Target dir:** the script sets `CARGO_TARGET_DIR` to **`target/cov`** (override
with `CARGO_TARGET_DIR_COV`). Day-to-day `cargo test` / clippy use **`target/dev`**
from the nix shell so instrumented and uninstrumented artifacts never thrash
each other. Musl release stays on `nix build .#rbitcoin-musl` (crane), not host
`target/`.

## What is measured

All workspace members that contain production code:

- `rbitcoin-primitives`, `rbitcoin-store`, `rbitcoin-query`, `rbitcoin-wire-cache`
- `rbitcoin-consensus`, `rbitcoin-net`
- `rbitcoin-rpc`, `rbitcoin-cli`, `rbitcoin-node`

**Excluded by default:** third-party crates, `src/main.rs` trampolines, and the host-only `store_bench` binary. Dependencies are not attributed to us.

## Philosophy

1. Cover code with **high-level functional/integration scenarios** (see [`TESTING.md`](./TESTING.md)).
2. Prefer expanding the harness over adding private unit tests.
3. If a branch is unreachable, **delete it** or add a public fault injector / config path so a scenario can hit it.
4. True unit tests only when a branch cannot be reached through any higher API without absurd cost — document the reason in the test file.

## Closing a red region

1. Open the HTML/LCOV report from `./scripts/coverage.sh`.
2. Identify high-miss production files (largest `LF − LH`).
3. Add or extend a **scenario** in `rbitcoin-test` or a unit test next to the shipped path that drives the real entry point.
4. Re-run `./scripts/coverage.sh` until line coverage is **≥ 90%**.
