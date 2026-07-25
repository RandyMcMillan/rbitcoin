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

After tip: few outbound follow peers, getheaders / inv / block accept. Compact-block reconstruction and package wire are improving; full getdata fallback remains.

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
