# Mempool accept lacks chain-context structural validation

**Component:** `rbitcoin-mempool` (`accept.rs`) + consensus helpers not threaded in
**Audit pin:** `2289578` (redteam zip 2026-08-10)
**Severity:** medium — 0-conf fraud / relay pollution (not consensus)
**Status:** fixed — shipped accept path (2026-08) — structural tip checks + `is_final_tx` / BIP68 (plan 2026-08)

**Regression:** `rbitcoin-mempool` `accept::tests::reject_non_final_locktime_height`, `reject_immature_coinbase` (structural chain context).
**Found by:** redteam-ecosystem harness (static analysis / code-review)

## Summary

`accept_tx_inner` checks in-mempool graph conflicts, Libre policy, and script
verification. It does not run structural consensus checks that need chain context.
The `UtxoProvider` abstraction supplies only `TxOut` content (no create height,
coinbase flag, tip height, or MTP).

### Variants

| | Gap | Consensus counterpart |
|---|-----|------------------------|
| (a) | No duplicate-input rejection; dups inflate `input_value` / fee | per-block spent set / bad-txns-inputs-duplicate |
| (b) | No coinbase maturity | coinbase immature at connect |
| (c) | No absolute finality (BIP113) or BIP68 relative locks | `is_final_tx` / `sequence_locks_satisfied` |

All three can be accepted into the mempool and served as unconfirmed. (a) never
confirms; (b)/(c) are invalid at the current tip until maturity/finality.

## Impact

Phantom unconfirmed payments and fee math; mempool pollution. **No consensus damage**
at block connect (structural checks still reject).

## Suggested fix (future)

Structural stage before policy/script: dedup outpoints; extend provider to Coin-like
records; pass tip height + MTP; reuse exported `is_final_tx` and
`sequence_locks_satisfied`.

## Notes

Zip report: `2026-08-10-rbitcoin-mempool-accept-performs-no-chain-context-structural-…`.
Repo relay tests note mempool does not enforce maturity — corroborates (b).
Remediation intentionally deferred per project priority (consensus first).
