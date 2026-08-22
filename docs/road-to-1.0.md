# Road to 1.0

What an operator or library user should be able to **count on** at **1.0**.
Day-to-day ranked work stays in [`quality.md`](./quality.md).

**Today (0.5.x):** first published line. On-disk format can still refuse a
named wipe. Electrum/Esplora need `--shindex` (default off). BIP324 v2-only.
Install is a GitHub Release (Linux musl; Windows/Darwin snapshots).

1.0 is the first **frozen-format, support-windowed** line. Not a Bitcoin
Core clone, not a soak badge, not a desktop wallet.

---

## Promises

| You should get | 1.0 |
|----------------|-----|
| **Datadir** | 1.0.x opens a 1.0.0 store. Format changes after that are a new major (or an explicit migrate). No silent wipe. |
| **Chain** | Invalid blocks stay invalid. Core JSON corpora still have no allowlist; past findings stay fixed. |
| **Wallets** | Electrum / Esplora work for the clients we claim, on a node that finished IBD + SH index. |
| **Index time** | `--shindex` after IBD is a known, resume-safe wait — not an unbounded hour-loss. Faster than 0.5 is the point. |
| **RAM** | IBD, SH build, and tip-follow have **explained** process RSS (heap, not “the disk is in page cache”). No surprise multi-GiB leaks. |
| **Fees** | The 10-minute inclusion estimate is checked against **whether txs actually get in**, not against Core’s historical estimator. |
| **P2P** | A single network neighborhood should not own your tip; junk peers / compact-block spam should not knock the node over. DoS = Core is **not** the bar; “won’t fold to the obvious attacks” is. |
| **Support** | [`SECURITY.md`](../SECURITY.md) names **1.0.x** with a real window. |
| **Install** | Still the musl GitHub Release. Useful **libraries** may also be on crates.io. |

`--shindex` defaults **on** only if index build is fast and RAM-sane enough
that Electrum-from-a-fresh-node is the normal path. Otherwise it stays
opt-in; wallets still require it.

0.6 / 0.7 can ship any of this without freezing the store.

---

## Not 1.0

Things people sometimes expect from “a Bitcoin node” that we are **not**
taking on for 1.0:

- Wallet keys, GUI, prune, ZMQ, IPC, plaintext v1 P2P, explorer search APIs
- Every Bitcoin Core functional test (no wallet / prune / v1 scripts)
- Matching Core `estimatesmartfee` numbers
- Apple notarization
- A gated “we soaked mainnet for N days” badge

---

## Work that makes those promises true

### Claimed behavior actually runs

COMPAT **done** RPCs, P2P, and mempool should be covered by the Core
functional harness **or** an explicit “we differ on purpose” note (fee
product, error codes, our mempool files). Today: **44 / 267** `run`; the
interesting leftovers are dialect, not missing methods. Owner:
[`core-functional.md`](./core-functional.md), Open **Q-41**.

### Fuzz until junk input is boring

Peers and blocks should not crash the node. 0.5 has one nightly
`block_wire` job. 1.0 wants continuous coverage of **headers, blocks,
scripts, BIP324, compact blocks** — crashes become a numbered finding plus
a regression. Open **Q-30**; frozen corpora **Q-31**.

### Libraries other people can import

The operator binary is not crates.io. Two things *are* worth publishing:

| Crate | Why a stranger would care |
|-------|---------------------------|
| **Consensus engine** | Same job as `libbitcoinkernel`: structure, connect, scripts, headers, policy — **without** our store. Today `rbitcoin-consensus` is wired into IBD/query; that has to come apart first. |
| **`rbitcoin-bench`** | Electrum/Esplora **client** load tool (Casa / Sparrow / concurrent wallets). Already optional; works against Fulcrum/electrs too. |

Store, P2P, RPC, Electrum server, the node — stay in this repo. Revisit
**Q-25** only for the published crates.

### Faster scripthash, less RAM

Operators feel **wall-clock after IBD** (`--shindex`) and **RSS** while
syncing, while building the index, and at tip with wallets connected.
Measure on a real SSD; keep resume (don’t throw away sealed shards). Do
not “fix” RSS by deleting the body queue or pretending kernel page cache
is a leak ([`ibd-memory.md`](./ibd-memory.md)).

### Harder to eclipse or DoS

Today: 125 inbound, rate windows, compact-block score, v2-only discovery.
Missing vs a careful Core operator: addr diversity / netgroups, something
like **anchors** so a restart doesn’t redraw the whole peer graph from
DNS, inbound eviction that isn’t “newest wins.” Tor/asmap can wait.

Electrum/Esplora connection caps stay always-on.

### Fee estimates that match inclusion

Product is **“in about 10 minutes under this mempool”**
([`mempool-fee-estimation.md`](./mempool-fee-estimation.md)). 1.0 needs a
harness: recorded pool + blocks → estimate vs what actually confirmed
(signet / frozen pack). Cold start and fee spikes should have stated
error bars. No need to mimic Core’s multi-horizon API.

### Freeze the store last

When the rest is true, tag 1.0 so **1.0.x does not wipe 1.0.0 datadirs**.
Older-than-1.0 or corrupt files can still refuse with a one-line message.

---

## After 1.0 (unless it falls out earlier)

- BIP331 package relay, if rust-bitcoin still has no types (**Q-48**)
- Default `--shindex` on, if index build is not there yet
- Tor / asmap
- Publishing the store as a crate
