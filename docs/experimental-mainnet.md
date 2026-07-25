# Experimental mainnet runbook

**Status:** reckless / lab use only. Not a production Core or Fulcrum replacement.

This node can perform multi-peer IBD, tip follow, Electrum (post-tip), and Libre-class mempool participation. Treat consensus and ops as **under active hardening**.

## Build

```bash
nix-shell
cargo build -p rbitcoin-node --release
```

## Mainnet catch-up (typical)

```bash
# Prefer a dedicated disk; large Class A archive.
export RBITCOIN_ARCHIVE_QUEUE_MB=256
export RBITCOIN_CLASS_A_CACHE_MB=256

./target/release/rbitcoin-node \
  --datadir /path/to/datadir-mainnet \
  --network mainnet \
  --listen 0.0.0.0:8333 \
  --max-outbound 16 \
  --mempool-size-mb 300 \
  --inhibit-suspend \
  --log-level info
```

### Milestone (script validation)

| Flag | Meaning |
|------|---------|
| *(default mainnet)* | `--milestone` defaults to **840000**: **script/sig checks skipped** at/below that height. Prevouts, double-spend, maturity, fees still run. |
| `--milestone 0` | Full script validation for all heights (slower; use for consensus labs). |

Default is an **assumevalid-style speed tradeoff**, not “we validated all historical scripts.”

## Interrupt and resume

1. Prefer **SIGTERM** / Ctrl+C (clean flush of tip tables + mempool).  
2. Same `--datadir` on restart.  
3. Class A archive is largely durable mid-IBD; tip is Class C. Hard `kill -9` can lose the last unflushed pages.  
4. Incomplete IBD **does not** enter tip mode; restart continues catch-up.

See also [`crash-recovery.md`](./crash-recovery.md).

## When Electrum is safe

Electrum is for **after** catch-up completes:

1. Node enters tip mode (SH bulk materialize + indexes).  
2. Start with `--electrum-listen 127.0.0.1:50001` (TLS via reverse proxy if needed).  
3. During Direct IBD, durable scripthash history may be empty/incomplete — do not point wallets at a mid-IBD node.

## Tip follow

After tip: few outbound follow peers, getheaders / inv / block accept.

**Compact blocks (BIP152 v2):** we advertise `sendcmpct` high-bandwidth version 2. Incoming `cmpctblock` is reconstructed from the mempool short-id map; missing txs use `getblocktxn` / `blocktxn`. Full `getdata` MSG_WITNESS_BLOCK remains the fallback when mempool is cold or fill fails. We serve `getblocktxn` and `MSG_CMPCT_BLOCK` getdata from store/cache, and announce tips as `cmpctblock` when the peer enabled high-bandwidth mode.

**WTx (BIP339):** handshake sends `wtxidrelay` (protocol ≥70016). When the peer also sends it, we announce and request `MSG_WTX` inventory.

**Packages:** `accept_package` is implemented; experimental wire command `rbtpkg` (len-prefixed). Full BIP331 `NetworkMessage` needs a rust-bitcoin upgrade.

**Misbehavior:** per-session ban score (threshold 100) for unsolicited/bad compact payloads and oversized pending-cmpct pressure; disconnects the peer.

**BIP324 v2 only** — many seed peers speak v1 only and will not connect.

## Ops risks

| Risk | Notes |
|------|--------|
| Disk / RAM | Multi‑100 GiB Class A; `tx.head` sparse grow + online resize |
| `tx.head` resize lag | Deep load can block inserts while shadow fills — watch logs for probe-exhaust |
| Peer scarcity | v2-only + experimental software |
| Mempool | Libre policy (0.1 sat/vB, full RBF, no dust ban); **scripts verified on accept** |

## Signet lab first

```bash
./target/release/rbitcoin-node \
  --datadir ./datadir-signet \
  --network signet \
  --listen 127.0.0.1:38333 \
  --electrum-listen 127.0.0.1:50001 \
  --log-level info
```

Full operator detail: [`OPERATOR.md`](../OPERATOR.md). Product scope / intentional differences: [`COMPAT.md`](../COMPAT.md).
