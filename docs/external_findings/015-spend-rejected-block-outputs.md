# A block may spend outputs that only existed in a rejected block

**Component:** `structural_validate_spends` + Class A archive-before-validate
**Audit pin:** fuzzamoto report 004 / rbitcoin `8f3990f`
**Severity:** high — consensus accept-invalid
**Status:** fixed — TipOnly stamp + structural fail-closed (`unwrap_or(0)` removed)
**Found by:** fuzzamoto

## Summary

Tx bodies are archived before the block is accepted. Rejected-block txs remain
in Class A. Structural spentness maps missing `tx_height` to **0**, so those
coins look ancient and valid. Core never had them on chain (`inputs-missingorspent`).

Fail-closed on “this fk has no height” alone is unsafe if lookup stamped the
**wrong** (unconnected) sibling of a real connected txid — see 017.

## Fix

Confirm stamp **`TipOnly`**: no connected instance → no create_fk. Structural:
never `unwrap_or(0)`; missing height → missing prevout. Ship with 017 + 019.
