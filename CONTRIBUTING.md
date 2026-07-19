# Contributing

## Principles

1. Follow [`IMPLEMENTATION-PLAN.md`](./IMPLEMENTATION-PLAN.md).
2. Prefer **high-level functional/integration tests** over unit tests ([`COVERAGE.md`](./COVERAGE.md), [`TESTING.md`](./TESTING.md)).
3. Every PR must keep **100% line and 100% branch** coverage on first-party code.
4. Do not reintroduce mempool/wallet/tx-relay scope without an explicit plan change; no pruning/GUI.
5. Store durability follows [`libbitcoin-durable-archive-variant.md`](./libbitcoin-durable-archive-variant.md).

## Workflow

```bash
nix-shell
cargo fmt --all
# rustc warnings are denied via workspace.lints + RUSTFLAGS=-Dwarnings (shell.nix)
cargo build --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
./scripts/coverage.sh
```

## Commits

- Small, reviewable commits with complete sentences in the message body.
- Production code and its covering scenarios land together.

## Code review checklist

- [ ] Behavior covered by a high-level scenario (or justified narrow test)
- [ ] No new silent dead branches
- [ ] Public API preferred over `#[cfg(test)]` white-box access
- [ ] Store changes respect Class A/B/C and allocate-then-publish
