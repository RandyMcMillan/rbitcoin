# Core-class JSON-RPC (rbitcoin)

`params` may be a JSON **array** (positional) or **object** (Core named
keys such as `blockhash`, `verbosity`, `txid`, `hexstring`). Missing
required keys are `-32602`; unknown named keys are `-8`.

rbitcoin serves a **documented subset** of Bitcoin Core JSON-RPC over plain HTTP.
This is **not** full Core parity: no wallet, no mining GBT, no `scantxoutset`,
no `createrawtransaction`. Prefer **Electrum / Esplora** (with `--shindex`) for
address/script history.

## Operator knobs

| Knob | Default | Meaning |
|------|---------|---------|
| `--rpc-listen ADDR` / conf `rpc_listen` | **off** | Bind HTTP JSON-RPC |
| `--rpcuser` / `--rpcpassword` | unset | HTTP Basic credentials |
| Cookie | **on** when listen set and no user/pass | `{datadir}/.cookie` as `user:password` |
| `--shindex` | **off** | Class B scripthash (Electrum/Esplora only; RPC by height/hash/txid does not need it) |

TLS is external (reverse proxy). Non-loopback binds still use cookie or user/pass
(always authenticated).

### curl example (cookie)

```bash
# After node start with --rpc-listen 127.0.0.1:8332
USERPASS=$(cat datadir/.cookie)
curl --user "$USERPASS" --data-binary \
  '{"jsonrpc":"1.0","id":"1","method":"getblockcount","params":[]}' \
  -H 'content-type: application/json' http://127.0.0.1:8332/
```

### curl example (user/pass)

```bash
rbitcoin-node --rpc-listen 127.0.0.1:8332 --rpcuser u --rpcpassword p ...
curl --user u:p --data-binary \
  '{"jsonrpc":"1.0","id":"1","method":"getblockchaininfo","params":[]}' \
  -H 'content-type: application/json' http://127.0.0.1:8332/
```

### rbitcoin-cli

Same datadir cookie, or `--rpcuser` / `--rpcpassword`. Default
`127.0.0.1:8332`. Prints the JSON-RPC `result` (strings unquoted).

```bash
rbitcoin-cli --datadir datadir getblockcount
rbitcoin-cli --rpcuser u --rpcpassword p --rpcport 8332 getblockchaininfo
```

## shindex matrix

| Capability | `shindex=0` (default) | `shindex=1` |
|------------|----------------------|-------------|
| IBD, tip follow, P2P, mempool relay | Yes | Yes |
| Node JSON-RPC (by height/hash/txid) | Yes | Yes |
| Electrum / Esplora listen | **Refuse start** | Yes after SH tip-ready |
| SH run enqueue / tip bulk | **Skip** | On |

Tip-follow readiness is **independent** of scripthash materialize. Electrum/Esplora
still wait for durable SH when shindex is on.

## Supported methods (Tier 1)

| Method | Notes |
|--------|-------|
| `help` / `getrpcinfo` / `uptime` / `stop` | Control |
| `echo` | Testing RPC. Returns arguments as a positional array. Mixed AuthServiceProxy `{args: [...], argN: ...}` is supported. |
| `syncwithvalidationinterfacequeue` | No-op `null` (no wallet/index callback queue) |
| `getblockchaininfo` / `getblockcount` / `getbestblockhash` / `getblockhash` | Chain tip. **`getblockchaininfo` placeholders (not computed, not a plan to fill):** `chainwork` is `""`, `size_on_disk` is `0`, `verificationprogress` is `0.5` during IBD else `1.0`. Use `blocks` / `initialblockdownload` / `difficulty`. |
| `getblockheader` / `getblock` (verbosity 0/1/2) | Archive reconstruct |
| `getdifficulty` | From tip bits |
| `getnetworkinfo` / `getconnectioncount` / `getpeerinfo` | BIP324 v2-only; `getpeerinfo` is the live session table. `version` is rbitcoin semver as a Core integer (`0.1.0` → `100`), not a Core release. `localservices` matches advertised `NETWORK\|WITNESS\|P2P_V2` |
| `addnode` / `disconnectnode` / `addconnection` | All networks. `addnode onetry` / `add` dial; `disconnectnode` by `nodeid` or address |
| `getmempoolinfo` / `getrawmempool` / `getmempoolentry` | MempoolHub. `maxmempool` is the operator weight budget (`--mempool-size-mb`) |
| `getrawtransaction` | Class A + mempool |
| `sendrawtransaction` / `testmempoolaccept` | Accept path (relay must be enabled) |
| `decoderawtransaction` / `decodescript` / `validateaddress` | Pure decode |
| `estimatesmartfee` | **10-minute inclusion frontier** — not Core historical multi-horizon. See [`mempool-fee-estimation.md`](./mempool-fee-estimation.md). |
| `generatetoaddress` / `generatetodescriptor` / `generateblock` / `generate` | **Regtest only.** Mine through `ChainHub::accept_block` (same confirm as P2P). First generated block includes current mempool (topo-sorted), then `remove_for_block`. `generatetodescriptor` accepts `raw(HEX)` (MiniWallet) or an address. Not a mining product (no GBT). |
| `submitblock` | All networks. Same `ChainHub::accept_received_block` as a P2P `block` message: tip-extend, or park + most-work `accept_branch`. |
| `scantxoutset` | All networks. `raw(HEX)` over Class A unspent outputs. MiniWallet on-ramp. Not Core coins-DB / HD-range scan. |
| `gettxout` | All networks. Class A + mempool. |
| `getindexinfo` | All networks. Reports `txindex` synced at tip — we reconstruct by txid from Class A (no separate index flag). |
| `getchaintips` | All networks. Active tip only (parked-fork list still follows `accept_received_block` / invalidate). |
| `waitforblock` / `waitforblockheight` / `waitfornewblock` | All networks. Poll tip (milliseconds timeout). |
| `setmocktime` | **Regtest only.** `0` = wall clock. Generate timestamps and future-header checks use `NodeClock` (not a process `time()` hook). |
| `invalidateblock` / `reconsiderblock` / `preciousblock` | All networks. Disconnect/re-accept via `ChainHub`; precious prefers equal-work siblings. |

## Permanent gaps (will not match Core)

| Method / area | Why |
|---------------|-----|
| Wallet RPC | No keystore |
| Mining / GBT | Non-goal. Regtest `generate*` is harness-only. `submitblock` is the same receive path as P2P. |
| `createrawtransaction` / `combinerawtransaction` | Wallet-adjacent footgun; use external tools |
| Full `scantxoutset` / `gettxoutsetinfo` | No UTXO-set coins DB; denserels ≠ chainstate. `raw()` Class A walk is the MiniWallet subset only. |
| Address history via Core method names | Use Electrum/Esplora with `--shindex` |
| Exact Core JSON field-for-field | Best-effort |
| Multi-user `rpcauth` / method whitelist | Future |

## Auth (current / future)

| Now | Future (not v1) |
|-----|-----------------|
| Cookie under datadir | `rpcauth=` multi-user |
| `--rpcuser` / `--rpcpassword` | `rpcwhitelist` |
| Always Basic auth when listen set | `rpcallowip` / unix socket |

## Related

- [`COMPAT.md`](../COMPAT.md) — product surface
- [`OPERATOR.md`](../OPERATOR.md) — flags and shindex tradeoffs
- [`mempool-fee-estimation.md`](./mempool-fee-estimation.md) — fee product
