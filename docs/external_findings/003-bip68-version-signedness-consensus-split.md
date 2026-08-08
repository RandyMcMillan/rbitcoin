# Consensus split: BIP68 not enforced for transactions with version bit 31 set

**Component:** `rbitcoin-consensus` (`block.rs::sequence_locks_satisfied`)
**Commit:** `1ec7e42` (2026-08-07)
**Severity:** **high — consensus split.** rbitcoin accepts a block Bitcoin Core rejects as
invalid.
**Found by:** fuzzamoto differential campaign (Bitcoin Core primary vs rbitcoin reference,
`oracle_consensus`)
**Status:** fixed — BIP68 version gate uses unsigned compare (`bip68_active_for_tx`)

## Summary

rbitcoin decides whether to enforce BIP68 relative locktimes by comparing the transaction
version as a **signed** integer. Bitcoin Core compares it as **unsigned**. A transaction
whose `nVersion` has bit 31 set — e.g. `0xFFFFFFFF` — is therefore version `-1` to rbitcoin
(BIP68 skipped) and version `4294967295` to Core (BIP68 enforced).

A block containing such a transaction with an unsatisfied relative locktime is **accepted by
rbitcoin and rejected by Core**. That is a chain split: a miner producing this block
partitions rbitcoin nodes onto a chain Core nodes consider invalid.

## Observed

Block `6c32ccddaed4d880491cd2bcae7bba0ac7103aa6ce84664ac4dec590bcc2974a` at height 201:

```
Core:
  [validation] BlockChecked: block hash=6c32ccdd… state=bad-txns-nonfinal,
               contains a non-BIP68-final transaction 0a8b9d569e1ace45e7ae8c312c48f7eced948e08e29a71962cc20d0ef2d93371
  InvalidChainFound: invalid block=6c32ccdd… height=201

rbitcoin:
  UpdateTip: new best=6c32ccdd… height=201 version=5 tx=2 progress=tip
```

Core marks the block invalid and keeps its tip at height 200. rbitcoin makes it the tip.

## Root cause

`crates/rbitcoin-consensus/src/block.rs:1668`:

```rust
pub fn sequence_locks_satisfied(
    tx: &Transaction,
    …
) -> bool {
    if tx.version.0 < 2 {
        return true;      // <-- BIP68 not enforced
    }
```

`tx.version.0` is `i32`. rbitcoin declares `bitcoin = "0.32.101"` and resolves to
**0.32.102**, which defines `pub struct Version(pub i32)`
(`bitcoin-0.32.102/src/blockdata/transaction.rs:1236`; the crate docs describe "the inner
`i32`"). For `nVersion = 0xFFFFFFFF` this is `-1`, so `-1 < 2` holds and the function returns
`true`, meaning "sequence locks satisfied" — the check is skipped entirely.

Bitcoin Core stores the version **unsigned** (`src/primitives/transaction.h:293`,
`const uint32_t version;`) and compares it directly
(`src/consensus/tx_verify.cpp:57`):

```cpp
bool fEnforceBIP68 = tx.version >= 2 && flags & LOCKTIME_VERIFY_SEQUENCE;
```

`4294967295 >= 2` is true, so Core enforces BIP68 and rejects the transaction.

Note this is a field that Core deliberately changed to unsigned; any implementation reading
it as signed will disagree on every transaction with the high bit set.

## The triggering testcase

`testcases/timeout-5701acef084adc5b` — 119 IR instructions. The two relevant values:

| IR op | value | meaning |
| :-- | :-- | :-- |
| `LoadTxVersion(4294967295)` | `0xFFFFFFFF` | `-1` signed, `4294967295` unsigned |
| `LoadSequence(30577)` | `0x7771` | bit 31 clear (lock enabled), bit 22 clear (height-based), `rel = 30577` blocks |

A relative height lock of 30577 blocks cannot be satisfied at height 201, so Core rejects.
rbitcoin never evaluates it.

## Scope

The same signedness assumption should be audited wherever the version field gates consensus
behaviour. `sequence_locks_satisfied` is the instance proven here; BIP68 enforcement is
reached from two call sites (`block.rs:1117` in-walk and `block.rs:1536` structural) and
both funnel through this one guard, so both are affected.

Two adjacent fail-open defaults in the same function are worth reviewing at the same time,
though they are **not** implicated in this finding:

```rust
let coin_h = prev_heights.get(i).copied().unwrap_or(0);
let coin_mtp = prev_coin_mtps.get(i).copied().unwrap_or(0) as i64;
```

An unresolved coin height silently becomes `0`, which makes a relative-height lock trivially
satisfiable. Failing closed would be safer.

## Suggested fix

Compare the version as unsigned, mirroring Core:

```rust
if (tx.version.0 as u32) < 2 {
    return true;
}
```

An upgrade may also resolve it: later rust-bitcoin releases are reported to have changed
`transaction::Version` to `u32` (matching Core's own `int32_t` → `uint32_t` change), which
would make the existing `< 2` comparison correct on its own. **Unverified here** — the
sandbox has no crates.io access, and only 0.32.7 and 0.32.102 are available locally, both
`i32`. Worth confirming against the current release.

Even if an upgrade fixes it, the explicit cast is the better fix: it states the consensus
rule locally instead of making correctness depend on a dependency's choice of
representation, which can change again in either direction.

## Notes

Diverges on every attempt (3/3). The harness surfaces it as a hang rather than a crash,
because the consensus oracle polls for 60s waiting for the tips to converge.

Triggering testcase: `testcases/timeout-5701acef084adc5b`, with the annotated execution log
alongside it.
