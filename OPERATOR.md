# Operator guide — IBD readiness

## Status

Phases 0–7 **core** are implemented. **Signet tip catch-up has been demonstrated.**
Mainnet IBD is experimental: start only with enough free disk for your store layout
(relational Class A is larger than Core’s block files).

Architecture: **archive-before-confirm** — block bodies are written to Class A as
peers deliver them; tip **confirm** (Class C) walks contiguous archived runs. Download
horizon defaults to **1024** blocks ahead of tip (Core-like window), max **16** blocks
in transit per peer.

## Build

```bash
nix-shell
cargo build -p rbitcoin-node --release
./target/release/rbitcoin-node --help
```

## Logging

Operational logs go to **stderr** with UTC timestamps and levels (no external log crate):

```
2026-07-15T03:04:26.725Z INFO  rbitcoin-node starting network=mainnet …
2026-07-15T03:04:27.100Z WARN  ibd: peer[3] dead: connect timeout (8s)
```

| Control | Values |
|---------|--------|
| `--log-level LEVEL` | `error` `warn` `info` `debug` `trace` `off` |
| `RBITCOIN_LOG` (or `RUST_LOG`) | bare level or `rbitcoin=debug` style |

Default: **info**. CLI flag wins over env.

## Defaults that matter for mainnet path

| Knob | Default | Override |
|------|---------|----------|
| IBD window (tip-ahead getdata) | **1024** | code `IbdConfig::window` |
| Blocks in transit / peer | **16** | code `IbdConfig::per_peer` |
| Milestone (skip scripts ≤ height) | **mainnet 840000**, signet 2000000, testnet 2500000, regtest 0 | `--milestone HEIGHT` (`0` = full validation) |
| Mainnet checkpoints | Core historical set | (compiled in) |
| Scripthash index | on, but **off under milestone** | `--no-scripthash-index` |
| Spend index | off under milestone | (code flag) |
| Log level | info | `--log-level` / `RBITCOIN_LOG` |

## Signet IBD lab

```bash
mkdir -p ./datadir-signet
./target/release/rbitcoin-node \
  --datadir ./datadir-signet \
  --network signet \
  --listen 0.0.0.0:38333 \
  --max-outbound 16 \
  --log-level info
```

- Omitting `--milestone` uses **2000000** on signet (assumevalid-style).
- Logs: `ibd: progress…`, `ibd: status tip=… arch_q=…`, `confirmed-set seed complete`.

### Resume

Re-run the same command with the same `--datadir`. Tip is in the relational archive.

### Clean stop

```bash
kill <pid>   # SIGTERM — flush store, exit 0
```

Prefer this over `kill -9`.

## Mainnet experimental

After signet tip is proven on your machine:

```bash
mkdir -p ./datadir-mainnet
./target/release/rbitcoin-node \
  --datadir ./datadir-mainnet \
  --network mainnet \
  --listen 0.0.0.0:8333 \
  --max-outbound 16 \
  --log-level info
```

- Default milestone **840000** skips script/prevout through that height.
- Or pin a peer: `--connect <ip>:8333 --no-seeds`.
- Watch **disk**: store growth can exhaust small volumes quickly; stop with SIGTERM and resume later.
- Do **not** claim production readiness until long runs, reorgs, and post-milestone validation are exercised.

Full validation lab (slow):

```bash
  --milestone 0
```

## Checklist before calling mainnet IBD “ready”

- [ ] Signet (or large signet range) to tip with clean restart resume
- [ ] Multi-day mainnet soak without corruption / OOM
- [ ] Post-milestone script/prevout path verified (or reindex plan)
- [ ] Disk headroom for full Class A archive
- [ ] Peer diversity and reorg behavior under load
