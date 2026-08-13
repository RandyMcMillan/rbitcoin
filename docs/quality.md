# Quality roadmap (living)

What is strong, what still blocks “industry-leading,” and what is already
closed. Replaces the 2026-08-06 point-in-time audit.

**Last reaudit:** 2026-08-12 (second pass: fold shipped tip-hole / page-group
work into Completed; add **Q-39–Q-40**).

**Two lists only**

| Section | Purpose |
|---------|---------|
| **Open** | Single prioritized backlog — **rank 1 = next** |
| **Completed** | Single list of finished quality work — do not reopen without new evidence |

North star, baseline, and working rules are context, not a third backlog.
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
| 2 | **Operator trust** | Docs match shipped IO/store; milestone skip impossible to miss; CLI/conf primary; SECURITY contact; honest 0.x |
| 3 | **Build & release integrity** | Same toolchain in CI and Nix; byte-repro musl; SBOM/audit gates; no floating `stable` |
| 4 | **Contributor velocity** | God-files gone or split by stage; warm suite a few minutes; TDD practiced; first-hour tutorial |
| 5 | **Product surface honesty** | Stub crates demoted or real; COMPAT accurate; Electrum/Esplora complete for *target* wallets, not explorer bloat |
| 6 | **Observability & ops** | Optional structured logs; residual env in `docs/env-knobs.md`; hermetic tip fixtures; soak playbooks |
| 7 | **Platform truth** | Linux-first everywhere; macOS/Windows non-goals until an IO story exists |

### Competitive bar

| Peer class | Beat them on | Do not waste effort matching |
|------------|--------------|------------------------------|
| **Bitcoin Core** | Archive model, Electrum-in-process, musl portable binary, pure-Rust script path | Every RPC, GUI, wallet, multi-OS desktop |
| **libbitcoin** | Modern Rust tooling, coverage gates, flake/repro, Electrum/Esplora | C++ cultural norms |
| **Fulcrum / Electrs** | Full validating node + index in one process | Being a pure indexer |
| **Other Rust nodes** | Store design, operator honesty, Linux IO, findings hygiene | Marketing or premature 1.0 |

### Non-goals that still look like “quality”

Not Open, not Completed — do not add them to either list.

- Multi-OS operator binaries before Linux IBD/tip is boringly solid
- 100% line-coverage theater (gate is **≥90%** LCOV + property-focused tests)
- Rewriting secp256k1 / rust-bitcoin / tokio “to reduce deps”
- Flattening purpose-built io_uring machines to batched `pread` (see AGENTS.md)
- Core-compatible full RPC surface; graphical block-explorer APIs

---

## Open (priority order)

**One list.** Rank is overall operator + contributor leverage, not a category.
Tags are scan hints only.

The 2026-08-12 **R-01–R-10** program lives here: **R-01–R-06 are in
Completed**; leftover R-ids keep their relative order (R-07/Q-30 before
R-08/Q-20 before R-09/Q-16; **R-10 last**). Older Q-ids that were never
R-ids sit among them by the same leverage rule. Do not recreate a second
R-table or P0/P1/P2 split.

| Rank | ID | Item | Tag | Done looks like |
|-----:|----|------|-----|-----------------|
| 1 | **Q-30** | Continuous differential fuzz | reliability | Nightly/weekly script + BIP324 + header/block wire (fuzzamoto-class or in-tree); crashes → `docs/external_findings/` + regression |
| 2 | **Q-16** | Residual `RBITCOIN_*` env (~36 names still in crates) | operator | Production reads are CLI/conf or the documented unstable set in `env-knobs.md`; dead string leftovers deleted |
| 3 | **Q-37** | Warm default suite **≤3 min** (stretch **&lt;2 min**) on a CI-class host | test-speed | Re-measure `cargo test --workspace` after R-03; record wall in TESTING.md. If still over budget, cut more; if inside, close. No new default remine-100 / &gt;2 s tests without justification |
| 4 | **Q-39** | Operator/docs inventory vs shipped model | docs | `OPERATOR.md` matches body-queue → confirm (no “archive-before-confirm”); README crate map lists Esplora + log and stops calling RPC a stub; `COVERAGE.md` crate list matches `--workspace`; `rust-bitcoin-limitations.md` findings **001–021**; `CHANGELOG` Unreleased covers landed work; `CONTRIBUTING.md` names the required **`multinode`** job |
| 5 | **Q-14** | Head-module glossary | docs | One diagram + “when to use which” for address_head / hashhead / sharded / segmented / scripthash_head (and confirm lookup/load/scripts/write) |
| 6 | **Q-21** | SBOM for musl release | supply-chain | CycloneDX/SPDX on release assets |
| 7 | **Q-23** | Optional musl CI | ci | Weekly or release-branch `nix build .#rbitcoin-musl` (proves crane; not every PR) |
| 8 | **Q-40** | Host `rust-toolchain` pin | ci | Root `rust-toolchain.toml` (or `rust-toolchain`) at **1.95** so rust-analyzer / host `cargo` match CI + Nix; still ignore Dependabot on `dtolnay/rust-toolchain` |
| 9 | **Q-31** | Hermetic tip fixtures | ops | Frozen signet/mainnet tip packs for offline regression (no live API) |
| 10 | **Q-15** | RPC + CLI destiny | product | `rbitcoin-cli` talks to the documented subset (or both are demoted from the product narrative); `getpeerinfo` is real or honestly absent; dummy `getblockchaininfo` fields (`chainwork`, `size_on_disk`, `verificationprogress`) filled or documented as never |
| 11 | **Q-36** | Perf log diet | ops | Default INFO short enough to ship; DEBUG/`tip: perf` keeps meters |
| 12 | **Q-32** | Structured logging option | ops | JSON or key=value **without** breaking operator greps |
| 13 | **Q-35** | Mainnet soak narrative | ops | Operator soak checklist; optional public tip-height badge |
| 14 | **Q-34** | First-hour tutorial | docs | Regtest mine → Electrum query → one Esplora GET |
| 15 | **Q-33** | Published rustdoc | docs | `cargo doc` site for first-party crates |
| 16 | **Q-24** | CODEOWNERS / issue templates | process | When public collaboration actually needs them |
| 17 | **Q-25** | Publish-ready package metadata | process | homepage / crates.io only if we intend to publish |
| 18 | **Q-38** | Tier-C multinode in default CI | ci | Keep `#[ignore]` + `scripts/integration.sh` unless wall/flake budget is proven |
| 19 | **R-10** | Residual god-files | code | Peel **only** when a higher row needs a seam (`query/lib`, `scripthash.rs`, `interpreter.rs`, `electrum/server.rs`). No new 2k-line modules |

**P0 trust/correctness (Q-01–Q-05) stays empty.** Do not reopen without new
evidence (failed Core corpus, new dual path, red required CI, MSRV drift).

### ID aliases (R-program ↔ catalog)

R-ids were the 2026-08-12 ranked slice. Canonical Open/Completed id is in
**bold**. Do not start **R-11+** — new work is the next unused **Q-id (Q-41+)**.

| R-id | Canonical | Where |
|------|-----------|-------|
| R-01 | **R-01** | Completed |
| R-02 | **R-02** | Completed |
| R-03 | **R-03** | Completed (Q-37 is the leftover wall measure) |
| R-04 | **R-04** | Completed |
| R-05 | **Q-22** | Completed |
| R-06 | **R-06** | Completed |
| R-07 | **Q-30** | Open rank 1 |
| R-08 | **Q-20** | Completed |
| R-09 | **Q-16** | Open rank 2 |
| R-10 | **R-10** | Open rank 19 |

---

## Completed

**One list.** Newest quality program first, then earlier Q-ids, then 2026-08-06
audit closures. Do not reopen without new evidence.

| ID | Item | Resolution |
|----|------|------------|
| **Q-20** | `cargo deny` / advisory CI | `deny.toml` + required `deny` job (`taiki-e/install-action` `cargo-deny@0.18.5`) |
| **R-06** | Tip-follow store integrity (`confirmed[]` null tail; tip+1 `NotFound`; idx clone) | Open trims trailing null `confirmed[]`; tip+1 after heal is not `NotFound`; `TxIdx::open` refuses non-monotone starts (`IDX_OPEN_DOUBLE_APPEND`) |
| — | IBD tip-hole cover + relative-slow disconnect | Tip-hole getdata ranks live `speed_sample` bps; stale inflight **6s** re-race (avoid last holders); `far_slots_per_peer=2` while `hole>0`; relative-slow outlier disconnect (`OUTLIER_RATIO=2` + cluster-spread + hysteresis) |
| — | Page-grouped `txid.body` identity on lookup | Head-resolve ID stage bulk-preads shared 4 KiB sidefile pages, then RAM BIP30 walk (same winners as serial peek) |
| **R-01** | Mempool read-path decoupling (histogram / frontier / `list_live`) | Published chunks on the fee snapshot; `list_live_meta` for body-free RPC/Esplora |
| **R-02** | Persistent shared `script_pool` | Process-wide `rbtc-scripts` workers; join does not spawn per admit |
| **R-03** | Default remine pads `1..=103` | Electrum + MempoolHub tests use `pad_empty_from`. Full-suite wall → **Q-37** |
| **R-04** | `TxGraph` mining-chunk cache | Rebuild only after insert/remove/`rebuild_from` |
| **Q-22** / **R-05** | Coverage job compiled `cargo-llvm-cov` from crates.io | `taiki-e/install-action` prebuilt `cargo-llvm-cov@0.6.14`, Action **commit-pinned** (CodeQL) |
| — | Esplora `/fee-estimates` 11× graph linearize under hub lock | Published fee table (`4714473`); histogram/list follow-on was **R-01** |
| — | Findings **012–021** (fuzzamoto differential) | All **fixed** + named Regression (identity/BIP30 cluster, tapleaf, compact-block, reorg drain) |
| **Q-01** | Core script/TX allowlist debt | **No allowlist**; all data rows must pass |
| **Q-02** | Confirm dual-path soft recovery | No `ColdPinMode`; no load identity soft-fill; `invariants.md` kill list |
| **Q-03** | Multi-node IBD only `#[ignore]` | Tier A default + required CI `multinode`; heavies stay ignored (**Q-38**) |
| **Q-04** | Env knob museum (primary) | Path-IO + confirm-queue envs removed; CLI first; leftovers → **Q-16** |
| **Q-05** | MSRV `1.74` untested | `rust-version = "1.95"` matching CI/Nix |
| **Q-10** / **Q-11** | God-files / long confirm fns | Test peels + `confirm_run` stages + `tx_table/packed` + `query/soft_densify`. Residual giants → **R-10** |
| **Q-12** | Store `allow(dead_code)` hotspots | Live APIs unsilenced; test-only under `#[cfg(test)]` |
| **Q-13** | Catch-up retry used `IbdConfig::for_test` | `catch_up_retry_config` uses production `Default` + `target_peers: 1` |
| — | Findings **001–011** + process | `docs/external_findings/`; all **fixed** + named Regression |
| — | Most-work reorg / tip-hole livelocks | Multi-hop reorg, tip-hole, zombie pending, resume O(N²)/stack |
| — | SH/fuse wipe on payload-only format | fuse8 soft-migrate; AGENTS format rules |
| — | Deps: rayon / xorf / bincode on hot graph | In-tree fuse8 + `script_pool` |
| — | README “map epochs” / mmap mental model | Map-free tables, HWM publish, no map epochs |
| — | Linux-only not front-and-center | README platform row + supported IO target |
| — | 100% coverage theater | Gate is **≥90%** first-party LCOV `LH`/`LF` |
| — | SECURITY contact missing | `security@reardencode.com` |
| — | RPC described as Core-compatible while stub | Description fixed: stub / not Core surface |
| — | Dual handbook chaos | AGENTS + CONTRIBUTING + how-we-plan (some overlap remains; acceptable) |
| — | CI floating `@stable` vs Nix pin | rustc **1.95.0**; nixos-26.05 |
| — | Coverage + test thrash one `target/` | `target/dev` vs `target/cov` |
| — | Monolithic `test` job hid fmt/clippy | Separate jobs + required **`multinode`** |
| — | No CodeQL | `.github/workflows/codeql.yml` (Rust `build-mode: none`) |
| — | Dependabot noise (rustc tag / hashes) | Ignore `dtolnay/rust-toolchain` + `bitcoin_hashes` |
| — | No Esplora | REST + wallet-scoped WS |
| — | Signet-only custom nets | Custom signet / mutinynet |

---

## Working the list

| Do | Do not |
|----|--------|
| Close work by **moving the Open row into Completed** in the same edit as the landing change | Leave `Status: fixed` in Open, or start a second table |
| New item: next unused **Q-id (Q-41+)** inserted at an explicit rank | Fill historical gaps (Q-06–Q-09, Q-17–Q-19, Q-26–Q-29) or start **R-11+** |
| Keep product research in `docs/future-features/` | Add those to Open or Completed |
| God-file peels only when a higher Open row needs a seam (**R-10**) | Split `query/lib` / `interpreter.rs` as a standalone “modularity” project |
| Suite: no new remine-100 / default test **&gt;2 s** without justification ([TESTING.md](../TESTING.md)) | Time the full workspace as a planning spike |
| Differentials / crashes → `docs/external_findings/` + named regression | Soft dual paths on confirm identity / denserels / Class A load |

---

## Baseline snapshot

**Measured 2026-08-12** (`crates/**/*.rs`, no build artifacts):

| Metric | Value |
|--------|-------|
| First-party Rust LOC | **~126k** |
| Workspace crates | **14** |
| Largest production files (lines) | `query/lib` **3303**, `scripthash` **3110**, `interpreter` **2592**, `sorted_run` **2577**, `ibd/perf_log` **2403**, `electrum/server` **2377**, `sh_builder` **2331**, `peer` **2319**, `block/mod` **2231**, `scripthash_head` **2051** |
| Largest test files (lines) | `scenarios` **2712**, `confirm_reject_tests` **2313**, `tx_table/tests` **2221** |
| `#[test]` / `#[tokio::test]` | **~1.15k** |
| Coverage gate | **≥90%** LCOV `LH`/`LF` (required CI) |
| Required CI | `fmt`, `deny`, `clippy`, `test`, `multinode`, `coverage` (+ CodeQL workflow) |
| rustc | **1.95** (`Cargo.toml` + `dtolnay/rust-toolchain@1.95.0` + nixos-26.05 / shell); no root `rust-toolchain.toml` yet (**Q-40**) |
| Nix | **nixos-26.05** + crane **0.23.x** |
| Host cargo silos | `target/dev` (test) / `target/cov` (coverage) |
| Release | `nix build .#rbitcoin-musl` → static install |
| Core corpora | **No allowlist** |
| Findings 001–021 | All **fixed** |
| Residual `RBITCOIN_*` in crates | **~36** (**Q-16**) |

### Grade board (subjective; 2026-08-12)

| Dimension | Grade | Note |
|-----------|-------|------|
| Architecture clarity | Strong | Roles + HWM + single Class A appender documented |
| Dependency hygiene | Strong | No `libbitcoinconsensus`; fuse8/script_pool in-tree |
| Operator honesty | Strong | CLI primary; experimental + milestone + Linux-first |
| Code modularity | Medium | Confirm peeled; residual giants only via **R-10** |
| Cross-platform | Weak (honest) | Linux-shaped IO |
| Docs consistency | Medium–Strong | SCHEMA/findings strong; **Q-39** — `OPERATOR.md` still says archive-before-confirm |
| Contributor onboarding | Medium–Strong | how-we-plan + TDD; tutorial still **Q-34**; host rustc unpinned (**Q-40**) |
| CI fidelity | Strong | Split gates, CodeQL, pinned Actions |
| Dead / stub surface | Medium–Strong | RPC exists; `rbitcoin-cli` + `getpeerinfo` still stub (**Q-15**) |
| Test reliability/speed | Medium–Strong | **R-03** removed worst remine pads; **Q-37** wall not re-measured |
| Tip-follow mempool APIs | Strong | **R-01–R-04**; refresh still one linearize under a short read |
| Adversarial / findings | Strong | No allowlist; **001–021** closed; next is **Q-30** |

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

---

## Consumers

| Audience | Read |
|----------|------|
| Next quality slice | **Open**, rank 1 |
| Release engineering | **Q-20**, **Q-21**, **Q-23** |
| Security / adversarial | Protect Q-01–Q-02; next **Q-30** |
| Docs / README | **Q-39**, then **Q-14**, **Q-34** |
| “Are we leading yet?” | North star + grade board |

---

*Living document. Prefer updating this file over dated audit copies.
Reaudit after a multi-commit quality program or when grade claims would rot.*
