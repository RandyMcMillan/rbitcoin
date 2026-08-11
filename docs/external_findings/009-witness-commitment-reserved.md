# Witness commitment accepts empty or multi-item coinbase witness

**Component:** `rbitcoin-consensus` (`block.rs` `check_witness_commitment_with_wtxids`)
**Audit pin:** `2289578` (redteam zip 2026-08-10)
**Severity:** medium — consensus accept-invalid
**Status:** fixed — consensus reject on shipped path (2026-08-10)
**Found by:** redteam-ecosystem harness (static analysis / code-review)

## Summary

BIP141 / Core: when a witness commitment output is present, the coinbase input’s
witness stack must be **exactly one 32-byte** item (the reserved value / nonce).
Otherwise the block is invalid (`bad-witness-nonce-size`).

rbitcoin:

1. First accepts if the commitment matches `SHA256D(witness_root || 0x00*32)`
   **regardless** of the coinbase witness (empty stack OK when reserved is zeros).
2. Otherwise takes `witness.last()` if 32 bytes whenever `len >= 1`, allowing
   multi-item stacks.

## Root cause

```text
// pseudo: zero reserved first; else last 32-byte stack item
reserved = [0u8; 32];
if hash(root || reserved) == committed { Ok }
else if witness.len() >= 1 && last.len() == 32 { try last }
```

## Impact

Miner-only accept-invalid: a block with witness txs, a valid zero-reserved
commitment, and an empty (or multi-item) coinbase witness is accepted by rbitcoin
and rejected by Core → temporary minority tip until honest work leads.

## Suggested fix

When commitment is present: require `witness.len() == 1` and item length 32; use
that item as reserved; reject otherwise. Update fixtures that omitted a zero
reserved item.

## Notes

Zip report: `2026-08-10-rbitcoin-witness-commitment-check-accepts-empty-or-multi-item-…`.
Missing commitment when any non-coinbase has witness data is already enforced.
