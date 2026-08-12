# BIP68 treats an unresolved coin age as no constraint

**Component:** `rbitcoin-consensus` (`sequence_locks_satisfied`)
**Audit pin:** fuzzamoto report 002 / rbitcoin `8f3990f`
**Severity:** high — consensus accept-invalid (`bad-txns-nonfinal`)
**Status:** fixed — `bip68_unresolved_coin_age_fails_closed`
**Found by:** fuzzamoto

## Summary

Missing `prev_heights` / `prev_coin_mtps` default to `0`. Time-based relative
locks then compute a minimum from epoch MTP, which is always below a real block
MTP, so the lock looks satisfied. Core rejects the same tx as non-final.

Height 0 is the genesis coinbase (unspendable), so `0` means **unresolved**, not
a real spendable age.

## Fix

Unresolved age → fail **closed** (lock not satisfied). Same-block creates use
the spending block height and prev-block MTP (Core). Never `unwrap_or(0)` as
“ancient enough.”
