# Road to 1.0

What **1.0** means for rbitcoin, which product gates must be green before
that tag, and what stays Won't-fix. Living Open/Won't-fix/Completed work
stays in [`quality.md`](./quality.md). This file owns **1.0 product
milestones** only.

**Today:** published **0.5.x** — first named 0.x line. Schema still bumpable
(named refuse/wipe). Default `--milestone 840000` skips historical scripts.
`--shindex` default off. BIP324 v2-only. GitHub Release = Linux musl +
Windows/Darwin snapshots. In-tree fuzz is one `block_wire` nightly job.

1.0 is the first **schema-frozen, support-windowed** line. Not “clone Core.”
Not a soak badge ([**Q-35**](./quality.md)).

---

## What 1.0 is

An operator can install a tagged musl binary, IBD + tip-follow **mainnet**
with honest docs, serve Electrum/Esplora as a **wallet-client backend**, and
trust that:

| Gate | 1.0 promise |
|------|-------------|
| **Schema** | Frozen for the 1.0.x series. New layout = 1.1 / 2.0 with a documented migrate or an explicit refuse. No silent wipe. |
| **Consensus** | Core JSON corpora still no allowlist; findings stay **fixed** + regression. Independent review of the script/connect path has happened (or is in SECURITY as a dated exception). |
| **Scripts on history** | Default `--milestone` is either **0** with measured IBD cost, or still assumevalid with the skip **unmissable** and a published “we did `--milestone 0` once” note. |
| **SH index** | `--shindex` may default on **only** if tip materialize is boring (wall + RSS). Otherwise stays opt-in; Electrum/Esplora still require it. |
| **Support** | [`SECURITY.md`](../SECURITY.md) names **1.0.x** as the supported line with a window (not “until the next 0.x”). |
| **Distribution** | Operator artifact remains **GitHub Release musl**. Selected **libraries** may be on crates.io (milestone L). The node crate is not the operator install path. |

**0.6.x** (or 0.7…) may ship any subset of these gates without freezing
schema. Do not wait for a single “1.0 PR.”

---

## What 1.0 is not

Still Won't-fix / product-never unless a **new** product decision lands in
[`quality.md`](./quality.md) Won't-fix:

- Full Core RPC, wallet keys, prune, ZMQ, IPC, GUI, v1 P2P, explorer APIs
- Darwin notarization / Developer ID
- 100% Core functional **267** `run` (wallet / prune / v1 / zmq stay skip)
- Flattening purpose-built io_uring machines
- Process pin FIFO / coordinators / headerless SH interiors
- A gated mainnet soak program (**Q-35**)
- `rbitcoin-bench` in default-members / musl / required CI

---

## How this relates to `quality.md`

| File | Owns |
|------|------|
| **This file** | 1.0 **gates** and milestone sequence |
| [`quality.md`](./quality.md) | Ranked **Open** backlog, Won't-fix, Completed. Rank 1 is still the next slice. |

1.0 pulls these Open rows in (do not copy the “done looks like” table here):

| ID | Why it is a 1.0 gate |
|----|----------------------|
| **Q-30** | Core-parity fuzz (milestone F). Today's `block_wire` nightly is the 0.5 minimum, not 1.0. |
| **Q-41** | Remotely reasonable Core functional `run` (milestone C). 44/267 is not the 1.0 bar. |
| **Q-31** | Hermetic tip fixtures; unblocks F corpora without live APIs. |
| **Q-48** | BIP331 wire when rust-bitcoin ships (**RB-007**). 1.0 may ship **without** P2P packages if still blocked upstream. |
| **Q-25 / Q-33** | Revisit **only** for selected libraries (milestone L). Node install stays musl Release, not crates.io. |
| **R-10** | Not a 1.0 gate. Peel god-files when a higher milestone needs a seam. |

New 1.0-shaped work that is not yet an Open Q-id gets a **Q-51+** row in
`quality.md` when it becomes the next ranked slice. Do not grow a second
Open list here.

---

## Milestones

Parallel workstreams. Each can land as many small PRs. **1.0 is when all
gates in the table at the top are true**, not when one milestone is “done.”

### C — Core functional, remotely reasonable

**Gate:** every surface COMPAT already calls **done** has either an
unmodified `run` or an explicit skip that is **not** silent `rpc-missing`.
P2P / mempool / buried-activation scripts we **claim** `run`. Product-never
stays skip (`no-wallet`, `no-prune`, `no-zmq`/`no-ipc`, `v1-only`, explorer).

**Today:** 44 `run` / 267. COMPAT-done leftovers are `rpc-dialect` (type-check
/ field zoo). `rpc_getblockfrompeer` is still `rpc-missing`. Inventory:
[`core-functional.md`](./core-functional.md).

**1.0 looks like:**

- `rpc-dialect` rows flipped **or** documented as permanent dialect (error
  code / fee product / mempool.dat bytes) with an analog that pins *our*
  contract
- Remaining `rpc-missing` is only methods we still do not claim
- `core-log` shrinks as we map debug.log; leftover `core-log` is not a 1.0
  freeze if the behavior is covered elsewhere
- Nightly `core-functional.yml` green; labeled PRs still the PR path

Not 267. “Remotely reasonable” = claimed surface + P2P/mempool we advertise.

### F — Core-parity fuzz

**Gate:** continuous jobs that can fail the tree on adversarial **wire +
script + connect**, with crashes tracked to `docs/external_findings/` + a
named regression. Differential vs Core / `libbitcoinkernel` where the
oracle is cheap.

**Today:** isolated `fuzz/` `block_wire` + nightly `fuzz.yml` (not a required
PR check). Findings 001–021 were an external fuzzamoto campaign.

**1.0 looks like:**

| Target | Why |
|--------|-----|
| Block / header wire + structure | Already started (`check_block_wire`) |
| Script / sighash / taproot | Same class as 001–021 |
| BIP324 frame | v2-only node; handshake/session bugs are production |
| Connect / prevout / BIP68 | Differential vs Core JSON corpora **and** mutated blocks |
| Compact-block / inv | Tip-follow DoS surface |

- Corpora: frozen signet/regtest + **Q-31** packs. No live API dump in CI.
- Nightly (or weekly long) job **green**. Compile of harnesses may become a
  required PR check; the long run stays scheduled.
- A crash without a finding file + regression is a process bug.

### L — Libraries others can use

**Gate:** crates that are **actually a library** (stable-ish API, no node
datadir, docs, tests) may be published on crates.io under the same license.
The **node** is still the musl GitHub Release.

**Honesty first:** `rbitcoin-consensus` today is **not** libbitcoinkernel.
It depends on `rbitcoin-query` / `rbitcoin-store` and owns IBD confirm
(`confirm_run`, script pool, `Query`). A useful kernel is **block / tx /
script / header / policy** without a store.

| Crate | 1.0 publish? | Condition |
|-------|--------------|-----------|
| **`rbitcoin-consensus`** (kernel slice) | **Yes, if split** | Extract a store-free engine (structure, connect, scripts, headers, params, policy). Confirm/IBD stays in the node (query/net). That is the libbitcoinkernel analogue. Do not publish the mixed crate as-is. |
| **`rbitcoin-bench`** | **Yes** | Already `publish = false`, not default-members. Standalone Electrum/Esplora **client** (works against Fulcrum/electrs too). crates.io binary and/or a small repo. Still not musl / required CI. |
| **`rbitcoin-primitives`** | Maybe | Tiny types. Only if the kernel split needs a versioned support crate. |
| **`rbitcoin-mempool`** | Unlikely as 1.0 | Cluster + sidecar files under `datadir/mempool/`. Useful internally; API is node-shaped. |
| **`rbitcoin-store` / `query` / `net` / `rpc` / `electrum` / `esplora` / `node` / `cli`** | **No** | Node. Schema, IO sessions, P2P. Not a library. |
| Silent-payments tweak math | Maybe later | Today lives under consensus; only if a wallet author asks for a thin crate. |

Publishing **reopens Q-25 / Q-33 for those crates only** (crates.io + rustdoc).
Do not put `rbitcoin-node` on crates.io as the operator path.

### P — SH materialize + process RSS

**Gate:** operators can `--shindex` without an hour-loss surprise, and RSS
during IBD / SH build / tip-follow is **explained and bounded** (process
heap vs kernel page cache — [`ibd-memory.md`](./ibd-memory.md)).

**Today:** k-way tip materialize, keep-runs resume, last-page extent fix,
ingest OA ~768 MiB. Body queue is RAM-only by design. BDZ/`g` and fuse8
are the large identity RSS.

**1.0 looks like (measure, then change):**

| Phase | What to beat | Not a fix |
|-------|--------------|-----------|
| **SH after IBD** | Wall for mainnet rematerialize on a reference SSD; resume stays keep-runs / sealed shards | Schema bump for density; headerless interiors (Won't-fix) |
| **IBD RSS** | Process heap (not `RssFile`) at dense heights; BQ + loadq + parent cache + open `tx.head` | Gutting the body queue or ConfirmParentCache to “make RSS pretty” |
| **SH build RSS** | Ingest OA + pack scratch + one-worker peaks | Per-shard 0.5–1 GiB OA (already gone) |
| **Tip-follow RSS** | Sealed MPHF FdOnly vs accidental anon heap; Electrum join caches | Growing leftover maps to `Vec<Fk>` without a miss |

No new Open Q-id until a measured regression or a named budget. Fat
`other=` on confirm is confirm-perf, not a 1.0 freeze.

### N — Eclipse / DoS hardening

**Gate:** SECURITY can drop “DoS ≠ Core” **or** still say it, but then list
the **remaining** gaps with owners. Eclipse and resource DoS have explicit
mitigations comparable to Core **where we speak the same P2P**.

**Today (honest):** inbound cap 125, per-session rate windows, misbehavior
score (compact), v2-only discovery (`x809` + `P2P_V2` addr).
`core-net-policy` skips: banlist format, tor, `anchors.dat`, asmap.
No Core feeler / extra outbound / eclipse cookbook is claimed.

**1.0 looks like:**

- Addr management: enough outbound diversity that a single netgroup cannot
  own the tip (document the actual table: seeds, learned addr, eviction)
- Sticky / protected peers or an **anchors**-class file so restart does not
  redraw the whole graph from DNS
- Inbound eviction that is not “newest wins”
- Inventory / compact / headers DoS: bounded queues, score, disconnect —
  pin with tests, not log vibes
- Electrum/Esplora `ServeLimits` stay always-on (already)
- Tor / asmap remain **optional 1.0+** unless someone owns them; v1 P2P stays never

This is **not** “run `p2p_leak*` 267.” It is a written threat model + tests
for the mitigations we ship. New work gets Q-51+.

### E — Fee estimation validation

**Gate:** the **10-minute inclusion** product
([`mempool-fee-estimation.md`](./mempool-fee-estimation.md)) is checked
against **inclusion**, not against Core’s historical estimator.

**Today:** shipped v2 flow projection; APIs are Electrum / Esplora /
`estimatesmartfee`. `rpc_estimatefee.py` stays `rpc-dialect` because the
product is not Core multi-horizon. Flow meters are **process-local** (no
persist). Cold start uses the v1 frontier.

**1.0 looks like:**

- A repeatable harness: recorded mempool + block connects → published
  estimate vs actual next-block inclusion (regtest / signet / frozen pack)
- Documented error bars (cold start, after fee spike, after empty mempool)
- No requirement to match Core `estimatesmartfee` numbers
- Persist-across-restart is **optional**; if still process-local, OPERATOR
  says so (already implied)

### S — Schema freeze and operator voice

**Gate:** 1.0.x opens 1.0.0 datadirs. Refuse/wipe is only for **older than
1.0** or corruption.

**Today:** schema 19; 17 populated `tx.head`/`scripthash*` refuse (OPERATOR
copy-paste). 18→19 is a `meta` rewrite.

**1.0 looks like:**

- `SCHEMA.md` / `SCHEMA_HISTORY.md` freeze note in the 1.0 tag commit
- Soft-migrate or dual-read for anything we still expect in the field
- Default `--milestone` decision (see top table) written in README +
  OPERATOR + SECURITY, not only experimental-mainnet
- Wallet **client version** matrix in OPERATOR if we have host-smoked
  Electrum / Sparrow / Cake numbers — do not invent versions

---

## Suggested sequence (not a waterfall)

```text
C  Core functional claimed surface     (Q-41)
F  Fuzz targets + corpora              (Q-30, Q-31)
P  SH wall + RSS budgets               (measure first)
N  Eclipse / DoS threat model + pins   (new Q-ids)
E  Fee inclusion harness
L  Kernel split → optional crates.io   (revisit Q-25 for those crates)
S  Schema freeze + SECURITY 1.0 window
   → tag v1.0.0
```

C and F can (and should) run from today. L is blocked on a kernel split,
not on a crates.io account. S is last.

---

## Explicitly after 1.0 (unless pulled forward)

- BIP331 P2P if rust-bitcoin still lags at tag time (**Q-48**)
- Default `--shindex` on, if P did not land a boring materialize
- Tor / asmap / notarized Darwin
- Independent long-running mainnet soak as a **program**
- Publishing `rbitcoin-store` as a crate

---

## Working rules (unchanged)

Small PRs, TDD, worktree, no silent store wipes, no flattening uring
machines, no process pin FIFO. Plans: [`how-we-plan.md`](./how-we-plan.md).
Operator 0.5 voice: [`experimental-mainnet.md`](./experimental-mainnet.md).
