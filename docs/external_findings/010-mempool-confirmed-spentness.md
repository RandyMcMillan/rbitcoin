# Mempool accept never checks confirmed-chain spentness

**Component:** `rbitcoin-mempool` / `QueryUtxoProvider` (`rbitcoin-net` tx_relay)
**Audit pin:** `2289578` (redteam zip 2026-08-10)
**Severity:** medium — 0-conf fraud enablement / mempool pollution (not consensus)
**Status:** in progress — mempool Coin view + confirmed spentness (plan 2026-08)
**Found by:** redteam-ecosystem harness (static analysis / code-review)

## Summary

Mempool acceptance resolves each input’s prevout via a UTXO provider that returns
output value/script from the creating tx but does **not** check whether that
output already has a confirmed-strong spender on the chain. In-mempool conflicts
are detected; confirmed double-spends are not.

A tx spending an already-spent confirmed outpoint can enter the pool, be relayed,
and appear as unconfirmed to Electrum/Esplora clients. It can never confirm (block
connect rejects structural spentness).

## Impact

0-conf payment fraud against services trusting this node’s mempool view; durable
mempool/relay pollution. **No consensus damage** to confirmed chain or honest miners.

## Suggested fix (future)

Reject prevouts with a confirmed-strong spender (mirror Core coins-view spentness)
in `get_txout` or `accept_tx_inner`.

## Notes

Zip report: `2026-08-10-rbitcoin-mempool-accept-never-checks-confirmed-chain-spentness-…`.
Remediation intentionally deferred per project priority (consensus first).
