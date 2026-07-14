# Operator guide — IBD readiness

## Status

Phases 0–7 **core** are implemented. **Public signet IBD is the gate before mainnet.**  
A full mainnet IBD is **experimental only** until signet reaches tip reliably.

## Build

```bash
nix-shell
cargo build -p rbitcoin-node --release
./target/release/rbitcoin-node --help
```

## Signet IBD lab (recommended first public chain)

### 1. Start with seeds (DNS + fixed)

```bash
mkdir -p ./datadir-signet
./target/release/rbitcoin-node \
  --datadir ./datadir-signet \
  --network signet \
  --listen 0.0.0.0:38333 \
  --milestone 200000 \
  --max-outbound 8
```

- `--milestone HEIGHT` skips script/prevout checks for blocks **≤ HEIGHT** (coarse assumevalid). Use a height near tip for faster catch-up; use `0` (default) for full validation.
- Logs: `ibd: progress…`, `ibd: tip=…`, seed resolve counts.

### 2. Or pin a known peer

```bash
./target/release/rbitcoin-node \
  --datadir ./datadir-signet \
  --network signet \
  --listen 127.0.0.1:38333 \
  --connect <peer-ip>:38333 \
  --no-seeds \
  --milestone 200000
```

### 3. Time-boxed lab (CI / smoke against public net)

```bash
./target/release/rbitcoin-node \
  --datadir ./datadir-signet-lab \
  --network signet \
  --listen 127.0.0.1:38333 \
  --milestone 200000 \
  --max-run-secs 120
```

Expect: seeds resolve, some blocks download (or clear connection errors if network blocked).

### 4. Resume

Re-run the same command with the same `--datadir`. Tip is stored in the relational archive; catch-up continues from local tip.

### 5. Electrum (optional, after some chain)

```bash
  --electrum-listen 127.0.0.1:50001
```

Confirmed history only; no TLS yet; broadcast does not push to peers.

## Mainnet experimental (not full-IBD claim)

Only after signet tip (or near-tip) is proven:

```bash
./target/release/rbitcoin-node \
  --datadir ./datadir-mainnet-exp \
  --network mainnet \
  --listen 0.0.0.0:8333 \
  --connect <trusted-peer>:8333 \
  --no-seeds \
  --milestone 800000 \
  --max-run-secs 3600
```

Stop after a few thousand blocks; inspect logs and disk. Do **not** claim production readiness.

## Checklist before calling mainnet IBD “ready”

| # | Item | Signet lab | Mainnet full |
|---|------|------------|--------------|
| 1 | Seeds resolve / peers connect | required | required |
| 2 | Tip advances; restart resumes | required | required |
| 3 | Reach signet tip (or N of tip) | **gate** | — |
| 4 | Concurrent multi-peer download | nice | recommended |
| 5 | Milestone/assumevalid tuned | recommended | required for time |
| 6 | No consensus rejects vs Core | required | required |
| 7 | Disk/progress ops acceptable | required | required |

## Lab notes (2026-07-14)

Time-boxed signet run in this environment:

| Check | Result |
|-------|--------|
| DNS/fixed seeds resolve | **Yes** (~34 addrs on signet) |
| TCP connect to public peers | **Yes** |
| Message parse against real peers | **Fixed** (lenient codec on extra-byte payloads) |
| Ordered block connect (out-of-order getdata) | **Fixed** (pending buffer) |
| P2P framing / multi-GB "message too large" | **Fixed** — cancel-safe `MessageStream` + Core payload cap |
| BIP34 coinbase height (OP_1..OP_16) | **Fixed** — match Core `CScript << int` |
| Hash-head full at ~2k blocks | **Fixed** — load-factor rehash (50%) |
| Parallel IBD (multi-peer window) | **Working** — lab: ~40k blocks / 35s, 4 peers, no desync |
| Blocks applied to store | **Yes** |
| Full signet tip | **In progress** — rate good; continue longer runs |

**P2P limits (Bitcoin Core–aligned):**

| Constant | Value | Core name |
|----------|------:|-----------|
| Max message payload | 4_000_000 | `MAX_PROTOCOL_MESSAGE_LENGTH` |
| Max inv/getdata items | 50_000 | `MAX_INV_SZ` |
| Max headers per message | 2_000 | `MAX_HEADERS_RESULTS` |
| Max locator hashes | 101 | `MAX_LOCATOR_SZ` |

(rust-bitcoin's `MAX_MSG_SIZE` is 5MB; we intentionally use Core's 4MB.)

**Next lab:** re-run with longer `--max-run-secs` (e.g. 3600), watch `ibd: progress` / tip rate, fix next stall (headers window, peer timeout, store write path).

## Signals / clean shutdown

`rbitcoin-node` installs handlers for **SIGTERM** and **SIGINT** (Ctrl+C):

```bash
kill <pid>            # SIGTERM — clean flush + exit
kill -TERM <pid>
kill -INT <pid>       # same as Ctrl+C
```

On signal the process interrupts IBD/follow, flushes the store, aborts peer tasks, and exits 0. Prefer this over `kill -9` so the archive is consistent for resume.

## Known limitations

- **Parallel IBD** (`parallel_ibd`): shared window default **1024** in-flight, **16/peer**, stall reassign ~5s; used by default in `run_p2p`.
- Sequential `sync_from_peers` is fallback only if parallel fails.
- Protocol version 70001; compact blocks → full getdata.
- Scripthash index grows with every output (disk) — costs IBD CPU/IO.
- Electrum: no TLS; mempool empty; broadcast incomplete.
- Dense checkpoints / full BIP9 state machine not productized (see plan §3.1).

## Tests

```bash
cargo test --workspace
./scripts/coverage.sh
```
