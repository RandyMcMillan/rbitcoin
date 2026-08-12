# Duplicate txid resolves to an unconnected Class A instance

**Component:** `tx.head` probe / `get_fk_by_txid` / `resolve_fk_and_range_batch`
**Audit pin:** fuzzamoto report 006 / rbitcoin `8f3990f`
**Severity:** medium — wrong consensus-critical identity
**Status:** fixed — `resolve_txid_prefers_connected_over_newer_unconnected`
**Found by:** fuzzamoto

## Summary

`tx.head` returns the newest body_txid match and stops. A later rejected (or
just-archived) row can hide an older **connected** instance. Confirm stamps the
wrong fk; height is missing; 015-alone would reject a **valid** spend.

Hot→cold waves today skip cold once **any** winner exists. An unconnected hot
hit must **not** finish the key.

## Fix

Same probe machine: collect matches per wave, pick `tx_height` Some (tip).
Unconnected after wave 1 → still run wave 2. **`TipOnly`** for confirm/annotate/
mempool-confirmed (else None). **`TipThenAny`** for RPC/reconstruct.
