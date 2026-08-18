# Quality roadmap (living)

What is strong, what still blocks “industry-leading,” and what is already
closed. Replaces the 2026-08-06 point-in-time audit.

**Last reaudit:** 2026-08-18 (confirm perf program #117–#126, repo cruft
#127, baseline re-measure, new **Q-50**). Previous: 2026-08-17 (docs map
#81, Core functional **35** `run`, no-repo-sniff #85).

**Three lists only**

| Section | Purpose |
|---------|---------|
| **Open** | Single prioritized backlog — **rank 1 = next** |
| **Won't fix** | Explicitly retired. Do not reopen without a new product decision |
| **Completed** | Finished quality work — do not reopen without new evidence |

North star, baseline, and working rules are context, not a fourth backlog.
Update a row when work lands. Prefer that over a new dated audit PDF.

This is not a security audit. Numbers are order-of-magnitude.

---

## North star: industry-leading full node in Rust

rbitcoin’s thesis is already differentiated: **relational archive** (no UTXO
set), **pure-Rust scripts**, **in-process Electrum/Esplora for wallet backends**,
**Linux map-free IO + optional io_uring**, **reproducible static musl**.
Leadership is not “clone Core’s checklist”; it is **owning that thesis** at a
level peers cannot ignore.

### Pillars (priority)

| # | Pillar | Industry-leading looks like |
|---|--------|-----------------------------|
| 1 | **Correctness under adversarial load** | Consensus-aligned with Core where we claim parity; differential fuzz continuous; findings tracked to **fixed** + regression; no silent confirm/store fallbacks |
| 2 | **Operator trust** | Docs match shipped IO/store; milestone skip impossible to miss; CLI/conf primary; SECURITY contact; honest 0.x; dummy RPC numbers gone or labeled |
| 3 | **Build & release integrity** | Same toolchain in CI and Nix; byte-repro musl; SBOM/audit gates; no floating `stable` |
| 4 | **Contributor velocity** | God-files gone or split by stage; warm suite a few minutes; TDD practiced; first-hour tutorial |
| 5 | **Product surface honesty** | COMPAT accurate; Electrum/Esplora complete for *target* wallets, not explorer bloat |
| 6 | **Observability & ops** | Default INFO shippable; residual env in `docs/env-knobs.md`; signet-first then mainnet with monitoring |
| 7 | **Platform truth** | Linux-first operator binaries; store IO session exists for Darwin (`pool`) and Windows (IOCP). Packaging those OSes is still a later ask |

### Competitive bar

| Peer class | Beat them on | Do not waste effort matching |
|------------|--------------|------------------------------|
| **Bitcoin Core** | Archive model, Electrum-in-process, musl portable binary, pure-Rust script path | Every RPC, GUI, wallet, multi-OS desktop |
| **libbitcoin** | Modern Rust tooling, coverage gates, flake/repro, Electrum/Esplora | C++ cultural norms |
| **Fulcrum / Electrs** | Full validating node + index in one process | Being a pure indexer |
| **Other Rust nodes** | Store design, operator honesty, Linux IO, findings hygiene | Marketing or premature 1.0 |

### Non-goals that still look like “quality”

Not Open, not Completed — see **Won't fix** for retired Q-ids.

- Multi-OS operator binaries before Linux IBD/tip is boringly solid
- 100% line-coverage theater (gate is **≥90%** LCOV + property-focused tests)
- Rewriting secp256k1 / rust-bitcoin / tokio “to reduce deps”
- Flattening purpose-built io_uring machines to batched `pread` (see AGENTS.md)
- Core-compatible full RPC surface; graphical block-explorer APIs
- Growing leftover RAM maps to `Vec<Fk>` unless a mainnet miss is shown
  ([`errata.md`](./errata.md))

---

## Open (priority order)

**One list.** Rank is overall operator + contributor leverage, not a category.
Tags are scan hints only.

**P0 trust/correctness (Q-01–Q-05) stays empty.** Do not reopen without new
evidence (failed Core corpus, new dual path, red required CI, MSRV drift).

| Rank | ID | Item | Tag | Done looks like |
|-----:|----|------|-----|-----------------|
| 1 | **Q-30** | Continuous differential fuzz | reliability | A nightly/weekly job that feeds BIP324 + header/block (and script) wire. Crashes → `docs/external_findings/` + named regression. **Today: no fuzz crate, no corpus, no job.** |
| 2 | **Q-41** | Grow Core functional `run` set | test | Inventory `run` covers the wallet-client / P2P / mempool / buried-activation scripts we **claim**. **Today: 35 / 268.** Next: leftover P2P (`p2p_invalid_messages`) and `rpc-missing` / `core-log` that still match claimed node/P2P/miner surface. Product-never skips stay skip (`no-wallet` 68, `no-prune` 7, `no-zmq`/`no-ipc`, `v1-only`). Unlabeled PRs stay cargo-only; nightly green |
| 3 | **Q-50** | Perf meter residual coverage | ops | On a saturated confirm thread, the named `ibd: perf` sub-meters account for (nearly) all busy wall. 2026-08-18 mainnet run: lookup 5.15s busy vs 1.76s metered, write 5.05s vs 2.39s, `stamp_sub batch=` 854ms wall vs ~170ms subs — both perf audits this week spent most of their effort attributing dark time. Done: an explicit residual per stage wall (wall − named subs ≈ 0), same-commit rule as the AGENTS timer inventory |
| 4 | **Q-36** | Perf log diet | ops | Default INFO short enough to ship a node without a pager. DEBUG / `tip: perf` keep meters. Getheaders storm (#43) and SH megakey 10 s heartbeat are closed. Q-50 adds meters on DEBUG/`tip: perf`, never more INFO |
| 5 | **Q-49** | v2-only peer discovery | ops | Tip-follow is not starved by v1-only DNS seeds. Documented v2 seed set and/or addr relay that finds BIP324 peers without dual-stack |
| 6 | **Q-48** | BIP331 rust-bitcoin package types | interop | Native BIP331 `NetworkMessage` when rust-bitcoin exposes it (**RB-007**). Packages today are RPC `submitpackage` / Esplora `POST /txs/package` only — no private P2P command. Blocked upstream — ranked below unblocked ops work |
| 7 | **Q-31** | Hermetic tip fixtures | ops | Frozen signet/mainnet tip packs for offline consensus/Electrum regression (no live API). Unblocks Q-30 corpora |
| 8 | **Q-34** | First-hour tutorial | docs | Regtest mine → Electrum query → one Esplora GET. One page; no second map |
| 9 | **R-10** | Residual god-files | code | Peel **only** when a higher row needs a seam. 2026-08-18 giants (raw `wc -l`, inline test mods included): `rpc/methods` **5.5k**, `peer` **4.3k**, `query/lib` **3.7k**, `scripthash` **3.5k**, `electrum/server` **3.3k**, `chain` **3.1k**, `store` **3.0k**, `ibd/perf_log` **2.6k** (new entrant; grows with every perf PR — Q-50 is its seam). No drive-by splits |

### Still valid? (this reaudit)

2026-08-18 pass. Verified against the tree at #127: zero in-tree fuzz,
inventory 35 `run` / 232 `skip` / 268 total, schema 17, findings 001–021
all fixed, **0** `TODO`/`FIXME`, **2** `#[allow(`, `unsafe` confined to
store IO sessions + `script_pool`, clean tree (no tracked logs or build
artifacts).

| ID | Verdict |
|----|---------|
| **Q-30** | Keep rank 1. Still the highest correctness hole; zero in-tree fuzz. |
| **Q-41** | Keep rank 2. **35** `run` holds; 232 skips (68 `no-wallet`, 53 `rpc-missing`, 32 `core-log`). `rpc-missing` + `core-log` remain the only growth matching claimed surface. |
| **Q-50** | **New, rank 3.** Confirm perf program (#117–#126) raised IBD 3.9 → 6.4 blk/s, but both remaining saturated threads are >50% unmetered — every reaudit re-derives the same dark time. Cheapest leverage on the active program. |
| **Q-36** | Keep. IBD/tip INFO is still a firehose; perf lines are ~2.5 KB each. |
| **Q-49** | Keep, raised above Q-48 (operator-affecting now vs blocked upstream). |
| **Q-48** | Keep, lowered. Waits on rust-bitcoin (**RB-007**). |
| **Q-31** | Keep. Useful for Q-30; not blocking operators. |
| **Q-34** | Keep. Onboarding still medium. |
| **R-10** | Keep last, list refreshed. `peer` **4.3k** and `ibd/perf_log` **2.6k** joined the giants with the perf program; peel only when Q-50/Q-41 need a seam. |

Prior-reaudit closures (Q-37, Q-47) and Won't-fix calls
(Q-24/25/32/33/35/38) stand — evidence unchanged.

### ID aliases (R-program ↔ catalog)

R-ids were the 2026-08-12 ranked slice. Canonical Open/Completed/Won't-fix
id is in **bold**. Do not start **R-11+** — new work is the next unused
**Q-id (Q-51+)**.

| R-id | Canonical | Where |
|------|-----------|-------|
| R-01–R-06 | **R-01–R-06** | Completed |
| R-07 | **Q-30** | Open rank 1 |
| R-08 | **Q-20** | Completed |
| R-09 | **Q-16** | Completed |
| R-10 | **R-10** | Open rank 9 |

New work after this reaudit starts at **Q-51**.

---

## Won't fix

Retired on purpose. Not a backlog. Not a failure.

| ID | Item | Why |
|----|------|-----|
| **Q-24** | CODEOWNERS / issue templates | No public collaboration process to own. Revisit only if external reviewers are invited |
| **Q-25** | crates.io package metadata | Distribution is `nix build .#rbitcoin-musl`. `repository` is already set |
| **Q-32** | Structured logging option | INFO/DEBUG text is the operator contract. JSON/kv is a second dialect |
| **Q-33** | Published rustdoc site | `cargo doc` locally. No docs.rs until crates.io (Q-25) |
| **Q-38** | Tier-C multinode in default CI | Wall/flake. `#[ignore]` + `scripts/integration.sh` is the product |
| **Q-35** | Mainnet soak program | Not a program. Run signet first, then mainnet with monitoring. No gated checklist or badge |
| **—** | Darwin notarization / Developer ID | Ad-hoc `codesign -s -` on the macos snapshot. Notarization is still not a product |
| **—** | Leftover maps as `txid → Vec<Fk>` | [`errata.md`](./errata.md): only if a mainnet miss is shown |
| **—** | Explorer APIs, full Core RPC, prune, ZMQ, IPC, v1 P2P, GUI, wallet keys | Product never. Inventory skips already say so |

---

## Completed

**One short list** of the latest quality program. Older closures (Q-01–Q-14,
findings 001–021, CI split, map-free README, …) live in
[`CHANGELOG.md`](../CHANGELOG.md). Do not reopen without new evidence.

| ID | Item | Resolution |
|----|------|------------|
| **Q-47** | Honest `getblockchaininfo` disk / progress | Store file walk + `blocks/headers` (not dummy 0 / 0.5) |
| **Q-37** | Warm default suite ≤3 min | Required CI `test` **~85 s** (2026-08-17, ubuntu-24.04). Stretch &lt;2 min met on CI-class. Recorded in TESTING.md |
| **—** | Docs map + one owner per fact | `docs/README.md`; folded store-format / startup-states / future-features / COVERAGE (`#81`) |
| **—** | Tests assert behavior, not repo text | No `include_str!` of production `.rs` / CONTRIBUTING (`#85`) |
| **—** | Core functional `run` set | **35** unmodified v31.1 scripts (was 9). Remaining growth is **Q-41** |
| **Q-15 / Q-42–Q-46** | CLI, inbound config, RPC honesty, Libre-only, IO aliases | 2026-08-16 cruft program |
| **R-01–R-06** | Mempool snapshot, `script_pool`, remine pads, TxGraph cache, llvm-cov pin, tip-follow store integrity | 2026-08-12. Wall leftover was **Q-37** (now closed) |
| **Q-16 / Q-20 / Q-23** | Residual env, `cargo deny` CI, optional musl artifact | `env-knobs.md`; required `deny`; `musl.yml` after green master `ci` |
| **—** | Darwin / Windows operator snapshots | `windows.yml` / `macos.yml` after green master `ci` or PR label `static-binaries`. Native store smoke + Darwin ad-hoc codesign |

---

## Working the list

| Do | Do not |
|----|--------|
| Close work by **moving the Open row into Completed** in the same edit as the landing change | Leave `Status: fixed` in Open, or start a second table |
| New item: next unused **Q-id (Q-51+)** inserted at an explicit rank | Fill historical gaps (Q-06–Q-09, Q-17–Q-19, Q-26–Q-29) or start **R-11+** |
| Retire a row to **Won't fix** when the product will not do it | Leave dead Open rows “for completeness” |
| God-file peels only when a higher Open row needs a seam (**R-10**) | Split `query/lib` / `interpreter.rs` as a standalone “modularity” project |
| Suite: no new remine-100 / default test **&gt;2 s** without justification ([TESTING.md](../TESTING.md)) | Time the full workspace as a planning spike |
| Differentials / crashes → `docs/external_findings/` + named regression | Soft dual paths on confirm identity / denserels / Class A load |

---

## Baseline snapshot

**Measured 2026-08-18** (`crates/**/*.rs` raw `wc -l`, inline test mods
included; tree at #127):

| Metric | Value |
|--------|-------|
| First-party Rust LOC | **~150k** (was ~146k on 2026-08-17; #127 deleted ~6k of benches/forensics) |
| Workspace crates | **13** (`rbitcoin-cli` … `rbitcoin-test`; no wallet crate) |
| Largest production files (lines) | `rpc/methods` **5475**, `peer` **4279**, `query/lib` **3655**, `scripthash` **3516**, `electrum/server` **3271**, `chain` **3113**, `store` **2951**, `sorted_run` **2710**, `interpreter` **2628**, `ibd/perf_log` **2590** |
| Largest test files (lines) | `tx_table/tests` **2903**, `confirm_reject_tests` **2335**, `scenarios` **2267**, `write_idempotent_tests` **2250** |
| `#[test]` / `#[tokio::test]` | **~1.51k** |
| `TODO` / `FIXME` / `#[allow(` | **0** / **0** / **2** |
| Coverage gate | **≥90%** LCOV `LH`/`LF` (required CI) |
| Required CI | `fmt`, `deny`, `clippy`, `test`, `multinode`, `coverage` (+ CodeQL) |
| Extra CI | `musl` / `windows` / `macos` after green master `ci` or PR label `static-binaries`; `core-functional.yml` nightly / labeled PR (not required) |
| rustc | **1.95** (`Cargo.toml` + `rust-toolchain.toml` + `dtolnay/rust-toolchain@1.95.0` + nixos-26.05 / shell) |
| Nix | **nixos-26.05** + crane **0.23.x** |
| Host cargo silos | `target/dev` (test) / `target/cov` (coverage) |
| Release | `nix build .#rbitcoin-musl` → static install |
| Core corpora | **No allowlist** |
| Findings 001–021 | All **fixed** (no new numbered reports since 021) |
| Core functional | **35** unmodified v31.1 scripts `run`; 232 skip (68 `no-wallet`, 53 `rpc-missing`, 32 `core-log`, …) |
| Residual `RBITCOIN_*` in crates | Honored set listed in `env-knobs.md` (**Q-16** closed) |
| On-disk | **Schema 17 durable** |
| IBD confirm rate (mainnet ~890k, fat era) | **6.4 blk/s** (was 3.95 pre-#126); lookup + write threads saturated, >50% unmetered (**Q-50**) |
| Fuzz | **None** |

### Grade board (subjective; 2026-08-18)

| Dimension | Grade | Note |
|-----------|-------|------|
| Architecture clarity | Strong | Roles + HWM + single Class A appender; schema 17 freeze in SCHEMA.md |
| Dependency hygiene | Strong | No `libbitcoinconsensus`; fuse8/script_pool in-tree |
| Operator honesty | Strong | CLI primary; chaininfo disk/progress are real (Q-47); README size matches SCHEMA census |
| Code modularity | Medium | `rpc/methods` **5.5k**, `peer` **4.3k**, `ibd/perf_log` **2.6k** after Core-functional + perf growth. Residual giants only via **R-10** |
| Cross-platform | Medium (honest) | Completion session ports Darwin/Windows store IO. CI snapshots: musl + CRT-static Windows + system-dylib Darwin |
| Docs consistency | Strong | One map (`docs/README.md`); AGENTS slim; comments-as-smell + no repo-text tests |
| Contributor onboarding | Medium | how-we-plan + TDD + inventory; tutorial still **Q-34** |
| CI fidelity | Strong | Split gates; `test` ~85 s; Core functional nightly extra |
| Dead / stub surface | Strong | Node RPC is a real subset; no dummy chaininfo numbers |
| Test reliability/speed | Strong | **Q-37** closed on CI-class; 2 s default-test rule remains |
| Tip-follow mempool APIs | Strong | **R-01–R-04**; persist sidecars exist (Core persist script still skip → Q-41) |
| Adversarial / findings | Medium–Strong | **001–021** closed; **no fuzz** (**Q-30**); Core functional is the active surface program (**Q-41**) |
| Perf observability | Medium | Meters exist per AGENTS, but saturated confirm threads are >50% dark (**Q-50**); perf INFO lines ~2.5 KB (**Q-36**) |

---

## What to protect

- Distinct product thesis (archive + pure Rust scripts + in-process wallet APIs).
- Small dependency graph; no `libbitcoinconsensus`.
- Operator honesty (experimental, milestone, Linux-first, honest MSRV).
- Written concurrency model (roles, HWM, one Class A appender).
- Portable static musl + crane + repro notes.
- Warnings-as-errors; Red → Green → Refactor (`docs/how-we-plan.md`).
- SCHEMA / SCHEMA_HISTORY / crash-recovery / COMPAT at 0.x.
- External findings hygiene + Core corpora without allowlist.
- Confirm dual-path kill + tier-A multinode in default/CI.
- Soft-migrate durable side formats; no silent wipes.
- Tests assert shipped behavior, not repo text.
- Schema 17 leftover regenerate for optional `sp_tweaks` files (not a Class A wipe).

---

## Consumers

| Audience | Read |
|----------|------|
| Next quality slice | **Open**, rank 1 (**Q-30** fuzz). Active programs: **Q-41** (Core functional `run` set), confirm perf (**Q-50** meters first) |
| Release engineering | **Q-20**, **Q-21**, **Q-23** (completed) |
| Security / adversarial | Protect Q-01–Q-02; next **Q-30** |
| Docs / README | Map is done; then **Q-34** |
| “Are we leading yet?” | North star + grade board |

---

*Living document. Prefer updating this file over dated audit copies.
Reaudit after a multi-commit quality program or when grade claims would rot.*
