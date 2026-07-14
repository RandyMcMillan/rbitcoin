# Contributing

## Principles

1. Follow [`IMPLEMENTATION-PLAN.md`](./IMPLEMENTATION-PLAN.md).
2. Prefer **high-level functional/integration tests** over unit tests ([`COVERAGE.md`](./COVERAGE.md), [`TESTING.md`](./TESTING.md)).
3. Every PR must keep **100% line and 100% branch** coverage on first-party code.
4. Do not add legacy (non-descriptor) wallet support or pruning.
5. Store durability follows [`libbitcoin-durable-archive-variant.md`](./libbitcoin-durable-archive-variant.md).

## Workflow

```bash
nix-shell
cargo fmt --all
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
- [ ] Wallet changes are descriptor-only
- [ ] Store changes respect Class A/B/C and allocate-then-publish
