# Experimental mainnet runbook

**Status:** reckless / lab use only. **Not** a production Bitcoin Core or
Fulcrum replacement. Design overview: [`architecture.md`](./architecture.md).

This node can perform multi-peer IBD, tip follow, Electrum (post-tip), and
Libre-class mempool participation. Treat consensus and ops as **under active
hardening**. Completing any particular full mainnet IBD is an **operator-side**
job and is **not** a packaging or “ready for experimental use” gate for this
repository — resume catch-up on the same datadir until tip, then soak tip follow
before trusting Electrum.

## Prerequisites

1. **Signet lab first** (below) until restart/resume and basic Electrum look sane.
2. Dedicated disk with multi‑100 GiB free (Class A grows large; `tx.head` is sparse
   but page cache / resize can pressure RAM+swap).
3. Understand **milestone** defaults (script skip) before claiming “validated mainnet.”

## Build

```bash
nix-shell
cargo build -p rbitcoin-node --release
```

Binary: `./target/release/rbitcoin-node`. More knobs: [`OPERATOR.md`](../OPERATOR.md).

## Signet lab first

```bash
./target/release/rbitcoin-node \
  --datadir ./datadir-signet \
  --network signet \
  --listen 127.0.0.1:38333 \
  --electrum-listen 127.0.0.1:50001 \
  --log-level info
```

Time-box a soak with `--max-run-secs` if desired. Prefer local listen + reverse
proxy TLS for Electrum; do not expose plain Electrum to the internet.

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

Default mainnet **does not** pass `--electrum-listen` — enable Electrum only
after tip (see below).

### Milestone (script validation)

| Flag | Meaning |
|------|---------|
| *(default mainnet)* | `--milestone` defaults to **840000**: **script/sig checks skipped** at/below that height. Prevouts, double-spend, maturity, fees still run. |
| `--milestone 0` | Full script validation for all heights (slower; use for consensus labs). |

Default is an **assumevalid-style speed tradeoff**, not “we validated all
historical scripts.” State this honestly when reporting experimental mainnet
results.

## Interrupt and resume

1. Prefer **SIGTERM** / Ctrl+C (clean flush of tip tables + mempool).
2. Same `--datadir` on restart — catch-up continues; no special “resume” flag.
3. Class A archive is largely durable mid-IBD; tip is Class C. Hard `kill -9`
   can lose the last unflushed pages.
4. Incomplete IBD **does not** enter tip mode; restart continues catch-up.
5. Ongoing first full mainnet sync may take days; stop/start is expected.

See also [`crash-recovery.md`](./crash-recovery.md).

## When Electrum is safe

Electrum is for **after** catch-up completes:

1. Node enters tip mode (SH bulk materialize + indexes).
2. Start with `--electrum-listen 127.0.0.1:50001` (TLS via reverse proxy if needed).
3. During Direct IBD, durable scripthash history may be empty/incomplete — **do
   not** point wallets at a mid-IBD node.

## Tip follow

After tip: few outbound follow peers, getheaders / inv / block accept.

**Compact blocks (BIP152 v2):** we advertise `sendcmpct` high-bandwidth version 2.
Incoming `cmpctblock` is reconstructed from the mempool short-id map; missing txs
use `getblocktxn` / `blocktxn`. Full `getdata` MSG_WITNESS_BLOCK remains the
fallback when mempool is cold or fill fails. We serve `getblocktxn` and
`MSG_CMPCT_BLOCK` getdata from store/cache, and announce tips as `cmpctblock`
when the peer enabled high-bandwidth mode.

**WTx (BIP339):** handshake sends `wtxidrelay` (protocol ≥70016). When the peer
also sends it, we announce and request `MSG_WTX` inventory.

**Packages:** `accept_package` is implemented; experimental wire command `rbtpkg`
(len-prefixed). Full BIP331 `NetworkMessage` needs a rust-bitcoin upgrade.

**Misbehavior:** per-session ban score (threshold 100) for unsolicited/bad compact
payloads and oversized pending-cmpct pressure; disconnects the peer.

**BIP324 v2 only** — many seed peers speak v1 only and will not connect. Expect
fewer usable peers than a dual-stack Core node.

## Ops risks

| Risk | Notes |
|------|--------|
| Disk / RAM | Multi‑100 GiB Class A; `tx.head` sparse grow + online resize (primary + shadow can pressure page cache / swap) |
| `tx.head` resize lag | Deep load can block inserts while shadow fills — watch logs for probe-exhaust / archiver sleeping |
| Peer scarcity | v2-only + experimental user-agent |
| Mempool | Libre policy (0.1 sat/vB, full RBF, no dust ban); **scripts verified on accept** |
| Confirm load | Dense heights are pin-bound; expect `conf blks=32` with `loadq` low relative to cap (`loadq=*/5`) |
| Not Core/Fulcrum | No production SLA; schema unstable until 1.0; reindex on incompatible layout changes |

## Related docs

- Architecture (store / IO / consensus uniqueness): [`architecture.md`](./architecture.md)
- Operator knobs and IBD log lines: [`OPERATOR.md`](../OPERATOR.md)
- Product scope / Electrum methods: [`COMPAT.md`](../COMPAT.md)
- Security reporting: [`SECURITY.md`](../SECURITY.md)
