# Quality roadmap (living)

This is the **open-source quality backlog** for rbitcoin: what is strong, what
still blocks “industry-leading,” and what is already closed. It replaces the
point-in-time audit of 2026-08-06 (`docs/quality-audit-2026-08-06.md`).

**Last reaudit:** 2026-08-11 (post P0 Q-01–Q-05 + P1 Q-10–Q-13 program). Metrics
re-measured from the tree; do not trust older line counts or “remaining” rows
without checking this date.

**How to use this doc**

| Section | Purpose |
|---------|---------|
| **North star** | What “industry-leading” means for *this* product (not a generic OSS checklist) |
| **Remaining work** | Prioritized open items — **top = next** |
| **Baseline snapshot** | Living metrics (re-measure when claiming progress) |
| **Resolved** | Closed audit items and how they closed |

Update this file when a P0/P1 item lands or a grade shifts. Prefer **one row
change + a short note in Resolved** over a new dated audit PDF.

**Method limits:** numbers are order-of-magnitude; re-measure file sizes and
coverage when planning a major cut. This is not a security audit.

---

## North star: industry-leading full node in Rust

rbitcoin’s thesis is already differentiated: **relational archive** (no UTXO
set), **pure-Rust scripts**, **in-process Electrum/Esplora for wallet backends**,
**Linux map-free IO + optional io_uring**, **reproducible static musl**. Industry
leadership is not “clone Core’s checklist”; it is **owning that thesis at a
level peers cannot ignore**.

### Pillars (in priority order)

| # | Pillar | Industry-leading looks like |
|---|--------|-----------------------------|
| 1 | **Correctness under adversarial load** | Consensus-aligned with Core where we claim parity; differential fuzz (fuzzamoto-class) continuous; external findings tracked to **fixed** with regression tests; no silent soft fallbacks on confirm/store invariants |
| 2 | **Operator trust** | Docs match shipped IO/store model; milestone/script-skip impossible to miss; CLI/conf primary (not env museum); SECURITY contact + disclosure; honest 0.x wording |
| 3 | **Build & release integrity** | Same toolchain in CI and Nix; byte-repro musl path; SBOM/audit gates; no floating `stable` surprises |
| 4 | **Contributor velocity** | God-files gone or split by stage; suite &lt; few minutes warm; TDD culture documented and practiced; first-hour tutorial |
| 5 | **Product surface honesty** | Stub crates demoted or real; COMPAT matrix accurate; wallet-client APIs (Electrum/Esplora) complete for *target* clients, not explorer bloat |
| 6 | **Observability & ops** | Optional structured logs; env survivors documented in `docs/env-knobs.md`; hermetic tip fixtures; optional soak badge; crash-recovery playbooks |
| 7 | **Platform truth** | Linux-first stated everywhere; macOS/Windows non-goals until IO story exists |

### Competitive bar (who we compare to)

| Peer class | Beat them on | Do not waste effort matching |
|------------|--------------|------------------------------|
| **Bitcoin Core** | Archive model, Electrum-in-process, musl portable binary, pure-Rust script path transparency | Every RPC, GUI, wallet, multi-OS desktop story |
| **libbitcoin** | Modern Rust tooling, coverage gates, flake/repro, Electrum/Esplora wallet APIs | C++ cultural norms |
| **Fulcrum / Electrs** | Full validating node + index in one process; no “index another Core” | Being a pure indexer |
| **Other Rust nodes** | Store design + operator honesty + Linux IO depth + findings hygiene | Marketing hype or premature 1.0 |

### Non-goals that still look like “quality”

- Multi-OS product ports before Linux IBD/tip is boringly solid  
- 100% line coverage theater (we use **≥90%** LCOV with property-focused tests)  
- Rewriting secp256k1 / rust-bitcoin / tokio “to reduce deps”

---

## Remaining work (prioritized — top first)

### P0 — Trust, correctness, honesty

**Empty as of 2026-08-11.** Closed Q-01–Q-05 (see **Resolved**). Do not re-open
without new evidence (failed Core corpus, new dual path, broken multinode job,
MSRV drift, etc.).

### P1 — Maintainability (code that scales with AI + human review)

| ID | Item | Why | Done looks like |
|----|------|-----|-----------------|
| **Q-14** | **Head-module glossary** | address_head / hashhead / sharded / segmented / scripthash_head | One architecture diagram + “when to use which” table in docs |
| **Q-15** | **RPC crate destiny** | Stub package text honest; still a workspace member with no surface | Either minimal useful node RPC slice *or* remove from default workspace “product” narrative |
| **Q-16** | **Residual process env** | ~36 `RBITCOIN_*` names still appear in crates (SH/BQ/slots, path-IO **string** leftovers, test-only). Path overrides and confirm queue envs are **dead**; many other reads remain | Either hardcode / CLI-struct remaining production reads, or keep only documented unstable set; no silent “advanced env bible” |

### P2 — Open-source packaging & supply chain

| ID | Item | Done looks like |
|----|------|-----------------|
| **Q-20** | **`cargo deny` / advisory CI** | `cargo deny check` or `cargo audit` on PR; documented exceptions |
| **Q-21** | **SBOM for musl release** | CycloneDX/SPDX attached to release assets |
| **Q-22** | **Cache `cargo-llvm-cov` in CI** | No cold `cargo install` every coverage job |
| **Q-23** | **Optional `nix build .#rbitcoin-musl` CI job** | Weekly or on release branch; proves crane path |
| **Q-24** | **CODEOWNERS / issue templates** | When public collaboration grows |
| **Q-25** | **Publish-ready package metadata** | `repository` / homepage when crates.io is intentional |

### P3 — Excellence / research-grade

| ID | Item | Done looks like |
|----|------|-----------------|
| **Q-30** | **Continuous differential fuzz** | fuzzamoto or in-tree libFuzzer targets for script + BIP324 + header wire; schedule + badge |
| **Q-31** | **Hermetic tip fixtures** | Frozen signet/mainnet tip packs for offline regression (no live API) |
| **Q-32** | **Structured logging option** | JSON or key=value mode without breaking human greps |
| **Q-33** | **Published rustdoc** | `cargo doc` site for first-party crates |
| **Q-34** | **First-hour tutorial** | Regtest mine → Electrum query → one Esplora GET |
| **Q-35** | **Mainnet soak narrative** | Documented operator soak checklist; optional public tip height badge |
| **Q-36** | **Perf log diet** | Shorter default INFO; DEBUG keeps full meters |
| **Q-37** | **Warm suite &lt;2 min** | Living budget in TESTING.md actually met on CI-class host |
| **Q-38** | **Tier-C multinode in CI (optional)** | Heavy mesh / 48-block / multi-hop remain `#[ignore]` + `scripts/integration.sh`; only promote if wall budget fits without flaking |

### P4 — Explicit non-goals (until a pillar above is green)

| Item | Why deferred |
|------|----------------|
| macOS / Windows operator binary | io_uring + map-free fd design is Linux-shaped |
| Core-compatible full RPC surface | Not the product thesis |
| Graphical block explorer APIs | Explicit non-goal in README |
| 100% line coverage | Replaced by **≥90%** LCOV + property tests |

---

## Baseline snapshot (living metrics)

**Measured 2026-08-11** (crate `.rs` under `crates/`, excluding build artifacts):

| Metric | Value |
|--------|-------|
| First-party Rust LOC (`crates/**/*.rs`) | **~121k** |
| Workspace crates | **14** |
| Largest source files (lines) | `query/lib` **~3240**, `scripthash` **3110**, `scenarios` **~2700**, `interpreter` **~2590**, `block/mod` **~2150** (tests peeled), `ibd/confirm/mod` **~1950** (tests peeled), `tx_table/mod` **~1650** + `packed` **~820**, `confirm_run/*` stage modules all **≲1030** (lookup max), `events/mod` **~970** |
| `#[test]` / `#[tokio::test]` count | **~1.1k** |
| Coverage gate | **≥90%** first-party LCOV (`LH`/`LF`) — required CI (last local run ~90.1% class) |
| CI jobs (required-style) | **`fmt`**, **`clippy`**, **`test`**, **`multinode`**, **`coverage`** (+ CodeQL workflow) |
| CI / MSRV rustc | **1.95** (`Cargo.toml` + `dtolnay/rust-toolchain@1.95.0` + nixos-26.05 / shell) |
| Nix pin | **nixos-26.05** + crane **0.23.x** |
| Host cargo targets | `target/dev` (test) / `target/cov` (coverage) |
| Release | `nix build .#rbitcoin-musl` → static install |
| Core corpora policy | **No allowlist** — all script/TX data rows must pass |
| External findings 001–011 | All **fixed** with named **Regression** links |
| Distinct `RBITCOIN_*` names still in crates | **~36** (many test/unstable/SH; path-IO overrides no longer honored) |

### Grade board (subjective; 2026-08-11 reaudit)

| Dimension | Grade | Trend |
|-----------|-------|--------|
| Architecture clarity | Strong | → |
| Dependency hygiene | Strong | → |
| Operator honesty | Strong | ↑ (CLI primary; `env-knobs.md`; MSRV honest) |
| Code modularity / size | **Medium** | ↑ (Q-10/Q-11: confirm_run stages, peels, tx_table packed; residual: query façade / scripthash / interpreter) |
| Cross-platform | Weak (honest) | → |
| Docs consistency | Strong | ↑ (findings, invariants, env policy) |
| Contributor onboarding | Medium–Strong | → |
| CI fidelity | **Strong** | ↑↑ (split gates + multinode + CodeQL + Dependabot ignores) |
| Dead / stub surface | Medium–Strong | ↑ (Q-12: store dead_code allows cleared; RPC stub remains) |
| Test reliability/speed | Medium–Strong | ↑ (tier A multinode default; overflow fix; heavies still ignored) |
| Adversarial / findings hygiene | **Strong** | ↑↑ (no allowlist; regressions named; dual-path kill) |

---

## Deep dive: how to get to “leading” on each pillar

### 1. Correctness

- **Never reopen soft dual paths** on confirm identity / denserels / Class A load
  (`docs/invariants.md`, AGENTS lean rules).  
- **Core JSON corpora all-rows-pass** is a permanent CI invariant (no allowlist).  
- Expand **fuzzamoto-style** campaigns after each store/consensus slice; log under
  `docs/external_findings/` with Status + Regression.  
- Prefer **scenario pins** that drive shipped entry points over test-only oracles.

### 2. Operator trust

- Confirm stage glossary still needed (lookup/load/scripts/write + queues).  
- **CLI / conf primary**; survivors and residual env in `docs/env-knobs.md` — not a
  growing advanced-env novel.  
- Milestone skip remains loud on CLI and progress logs.  
- Experimental mainnet doc stays linked from README.

### 3. Build integrity

- CI pin == flake pin == shell pin (**1.95** class).  
- Musl only via crane; never ship host `cargo build --release`.  
- Coverage/test **target dirs split**.  
- Add deny/audit (Q-20) when supply-chain is a release gate.

### 4. Contributor velocity

- **how-we-plan.md** Red→Green→Refactor is the process standard.  
- God-file splits are **vertical** (stage contracts). Next candidates: `confirm_run`,
  `tx_table`, `block`, `ibd/events`, `query/lib`, `scripthash`.  
- Suite speed budgets in TESTING.md remain acceptance criteria for new default tests &gt;2 s.

### 5. Product surface

- Electrum/Esplora for **wallet clients** (documented); not explorer search.  
- RPC stub: thin useful subset or demote from product narrative (Q-15).  

### 6. Observability

- Default INFO short enough to ship; DEBUG/perf_dbg for deep IBD forensics.  
- Optional JSON later — do not break operator greps.

### 7. Platform

- Linux first forever until a funded IO port. State it in every top-level doc.

---

## Resolved (closed since 2026-08-06 audit and related work)

Items below were open in the original audit or immediately adjacent. **Do not re-open without new evidence.**

### Docs & honesty

| Was | Resolution |
|-----|------------|
| README “map epochs” / mmap mental model | **Fixed** — map-free tables, HWM publish, no map epochs |
| Linux-only not front-and-center | **Fixed** — README platform row + supported IO target |
| 100% coverage theater | **Fixed** — gate is **≥90%** LCOV |
| SECURITY contact missing | **Fixed** — `security@reardencode.com` |
| RPC described as Core-compatible while stub | **Fixed** (description) — stub / not Core surface yet |
| Dual handbook chaos | **Improved** — AGENTS + CONTRIBUTING + how-we-plan (overlap remains) |

### Build & CI

| Was | Resolution |
|-----|------------|
| CI floating `@stable` vs Nix pin | **Fixed** — rustc **1.95.0**; nixos-26.05 |
| Coverage + test thrash one `target/` | **Fixed** — `target/dev` vs `target/cov` |
| Monolithic `test` job hid fmt/clippy | **Fixed** — separate jobs + **`multinode`** |
| No CodeQL | **Fixed** — `.github/workflows/codeql.yml` (Rust `build-mode: none`) |
| Dependabot noise (rustc tag / hashes) | **Fixed** — ignore `dtolnay/rust-toolchain` + `bitcoin_hashes` |
| MSRV `1.74` untested | **Fixed (Q-05)** — `rust-version = "1.95"` |
| Deps: rayon / xorf / bincode on hot graph | **Fixed** — in-tree fuse8 + script_pool |

### Correctness & adversarial

| Was | Resolution |
|-----|------------|
| No external findings process | **Fixed** — `docs/external_findings/` 001–011 fixed + Regression |
| Core script/TX allowlist debt | **Fixed (Q-01)** — **no allowlist**; all data rows must pass |
| Confirm dual-path soft recovery | **Fixed (Q-02)** — no `ColdPinMode`; no load identity soft-fill; invariants.md kill list |
| Multi-node IBD only `#[ignore]` | **Fixed (Q-03)** — tier A default + required CI `multinode`; heavies remain ignored (Q-38) |
| Env knob museum | **Fixed (Q-04) primary** — path IO + confirm queue envs removed; CLI first; residual names → **Q-16** |
| Most-work reorg / tip-hole livelocks | **Largely fixed** — multi-hop reorg, tip-hole, zombie pending, resume O(N²)/stack |
| SH/fuse wipe risk on format | **Fixed** — soft-migrate fuse8; AGENTS format rules |
| Node catch-up retry used `IbdConfig::for_test` | **Fixed (Q-13)** — `catch_up_retry_config` uses production `Default` + `target_peers: 1`; `for_test` remains for harnesses only |

### Maintainability (partial P1)

| Was | Resolution |
|-----|------------|
| Store `allow(dead_code)` hotspots | **Fixed (Q-12)** — live APIs unsilenced; test-only surfaces under `#[cfg(test)]` |
| God-files / long confirm fns | **Fixed (Q-10/Q-11)** — test peels (`events`, `confirm_run`, `tx_table`, `block`, `ibd/confirm`); `confirm_run` stage modules (`lookup`/`pin`/`scripts`/`write`/`phases`); `tx_table/packed`; `query/soft_densify`. Residual large files: `scripthash`, `query` Query façade, `interpreter` (optional follow-on) |

### Product surface growth (post-audit)

| Was | Resolution |
|-----|------------|
| No Esplora | **Added** — REST + wallet-scoped WS |
| Signet-only custom nets | **Improved** — custom signet / mutinynet |

### Still intentionally open (see Remaining)

Residual env reads (Q-16), cargo deny/SBOM/musl CI (P2), continuous fuzz /
tutorial / soak (P3), tier-C multinode optional (Q-38). Optional further splits:
scripthash / query façade / interpreter tables.

---

## What is already high quality (protect)

- Distinct product thesis (archive + pure Rust scripts + in-process wallet APIs).  
- Small, intentional dependency graph; no `libbitcoinconsensus`.  
- Operator honesty (experimental, milestone, Linux-first, honest MSRV).  
- Written concurrency model (roles, HWM, single Class A appender).  
- Portable static musl + crane layering + repro notes.  
- Warnings-as-errors; TDD / how-we-plan culture.  
- SCHEMA / SCHEMA_HISTORY / crash-recovery / COMPAT depth rare at 0.x.  
- External findings hygiene + Core corpora **without allowlist**.  
- Confirm dual-path kill + multinode in default/CI.  
- Active cleanup culture (rename pipelines, soft migrate, no silent format wipes).

---

## Suggested consumers

| Audience | Read |
|----------|------|
| Maintainers picking the next refactor | Remaining **P1** (**Q-14** glossary, **Q-16** residual env) |
| Release engineering | **P2 Q-20–Q-23** |
| Security / adversarial | Protect Q-01–Q-02; next **Q-30** fuzz |
| Docs / README | **Q-14**, **Q-34** |
| “Are we industry-leading yet?” | North star pillars + grade board — modularity still the main gap |

---

*Living document. Prefer updating this file over creating dated audit copies.
Reaudit after any multi-PR quality program or when grade claims would otherwise
rot.*
