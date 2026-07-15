# Operator guide — IBD readiness

## Status

Phases 0–7 **core** are implemented. **Public signet IBD is the gate before mainnet.**

Architecture (2026-07): **archive-before-confirm** — block bodies are written to Class A as
peers deliver them; tip **confirm** (Class C) walks contiguous archived runs. Download
horizon defaults to **144** blocks ahead of tip (RAM bound).

## Build

```bash
nix-shell
cargo build -p rbitcoin-node --release
./target/release/rbitcoin-node --help
```

## Defaults that matter for mainnet path

| Knob | Default | Override |
|------|---------|----------|
| IBD window (tip-ahead getdata) | **144** | code `IbdConfig::window` |
| Milestone (skip scripts ≤ height) | **mainnet 840000**, signet 300000, testnet 2500000, regtest 0 | `--milestone HEIGHT` (`0` = full validation) |
| Mainnet checkpoints | Core historical set through **295000** | (compiled in) |
| Scripthash index | on, but **off under milestone** | `--no-scripthash-index` |
| Spend index | off under milestone | (code flag) |

## Signet IBD lab (recommended first public chain)

```bash
mkdir -p ./datadir-signet
./target/release/rbitcoin-node \
  --datadir ./datadir-signet \
  --network signet \
  --listen 0.0.0.0:38333 \
  --max-outbound 22
```

- Omitting `--milestone` uses **300000** on signet (assumevalid-style).
- Logs: `ibd: progress…`, `ibd: status tip=… archived_ahead=…`, `confirmed-set seed complete`.

### Resume

Re-run the same command with the same `--datadir`. Tip is in the relational archive.

### Clean stop

```bash
kill <pid>   # SIGTERM — flush store, exit 0
```

Prefer this over `kill -9`.

## Mainnet experimental (not a full-IBD claim)

Only after **signet tip (or near-tip)** is proven:

```bash
mkdir -p ./datadir-mainnet-exp
./target/release/rbitcoin-node \
  --datadir ./datadir-mainnet-exp \
  --network mainnet \
  --listen 0.0.0.0:8333 \
  --max-outbound 16 \
  --max-run-secs 7200
```

- Default milestone **840000** skips script/prevout through that height.
- Or pin a peer: `--connect <ip>:8333 --no-seeds`.
- Stop after a few thousand / tens of thousands of blocks; inspect logs and disk.
- Do **not** claim production readiness.

Full validation lab (slow):

```bash
  --milestone 0
```

## Checklist before calling mainnet IBD “ready”

| # | Item | Signet lab | Mainnet full |
|---|------|------------|--------------|
| 1 | Seeds resolve / peers connect | required | required |
| 2 | Tip advances; restart resumes | required | required |
| 3 | Reach signet tip (or N of tip) | **gate** | — |
| 4 | Concurrent multi-peer + archive/confirm | required | required |
| 5 | Milestone/assumevalid tuned | default OK | default / raise |
| 6 | No consensus rejects vs Core (post-milestone) | required | required |
| 7 | Disk/progress ops acceptable | required | required |
| 8 | Serve IBD after cold restart (reconstruct) | recommended | required |

## Lab notes (2026-07-14+)

| Check | Result |
|-------|--------|
| DNS/fixed seeds | Yes (~34 signet addrs) |
| Cancel-safe P2P framing + Core 4MB cap | Fixed |
| BIP34 OP_1..OP_16 | Fixed |
| Hash-head rehash | Fixed (chunked zeros) |
| Parallel multi-peer IBD | Working |
| Archive-before-confirm (no RAM body pool) | **Landed** |
| Tip-ahead window | **144** (was 1024) |
| Signet depth | **290k+** lab progress; continue to tip |
| Full signet tip | **In progress** |

**P2P limits (Core-aligned):** payload 4MB, inv 50k, headers 2000, locator 101.

## Signals / clean shutdown

`SIGTERM` / `SIGINT` interrupt IBD, flush the store, abort peers, exit 0.

## Known limitations

- Class A can retain **never-confirmed** archived bodies (append-only; no GC yet).
- Confirmed hash set seeds tip immediately; full set fills in a background thread.
- Protocol 70001; compact blocks → full getdata.
- Electrum: no TLS; mempool empty; broadcast incomplete.
- Dense post-295k checkpoints not used (Core model: assumevalid/milestone instead).
- Full BIP9 state machine not productized (see plan §3.1).

## Next operator steps toward mainnet

1. Let **signet** reach tip (or within ~100 of tip); restart once; confirm resume.
2. Short **mainnet experimental** run (`--max-run-secs 7200`) with default milestone.
3. Compare rejects / tip rate against a Core node if available.
4. Only then plan multi-day mainnet IBD + disk budget.
