# Quality roadmap (living)

This is the **open-source quality backlog** for rbitcoin: what is strong, what
still blocks “industry-leading,” and what is already closed. It replaces the
point-in-time audit of 2026-08-06 (`docs/quality-audit-2026-08-06.md`).

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
| 2 | **Operator trust** | Docs always match shipped IO/store model; milestone/script-skip is impossible to miss; logs greppable and complete; SECURITY contact + disclosure practice; honest 0.x wording |
| 3 | **Build & release integrity** | Same toolchain in CI and Nix; byte-repro musl path; SBOM/audit gates; no floating `stable` surprises |
| 4 | **Contributor velocity** | God-files gone or split by stage; suite &lt; few minutes warm; TDD culture documented and practiced; first-hour tutorial |
| 5 | **Product surface honesty** | Stub crates demoted or real; COMPAT matrix accurate; wallet-client APIs (Electrum/Esplora) complete for *target* clients, not explorer bloat |
| 6 | **Observability & ops** | Optional structured logs; documented env knobs; hermetic tip fixtures; optional soak badge; crash-recovery playbooks |
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

| ID | Item | Why it still matters | Done looks like |
|----|------|----------------------|-----------------|
| **Q-01** | **Keep external findings closed** | Fuzzamoto 001–011 tracked; new campaigns will reappear | `docs/external_findings/*` status accurate; every **fixed** has a regression; CI cannot drop Core TX/script allowlists silently |
| **Q-02** | **Confirm/store dual-path hygiene** | Soft fallbacks hide load bugs; historical thrash | Invariants on confirm hot path; no spentness soft-path for identity bugs; SH/fuse multi-path only where env/protocol requires |
| **Q-03** | **Default CI does not run multi-node IBD** | Hardest product surface is `#[ignore]` | Small hermetic multi-node path green in default CI *or* weekly required job with hang fixed |
| **Q-04** | **Env knobs inventory** | ~40 `RBITCOIN_*` in code vs fewer in OPERATOR | Every public knob in OPERATOR advanced section; private knobs `cfg`/`doc(hidden)` or `RBITCOIN_UNSTABLE_*` |
| **Q-05** | **MSRV honesty** | `rust-version = "1.74"` untested; real bar is **1.95** class | Either MSRV CI job on 1.74 or raise MSRV to the Nix/CI pin and document |

### P1 — Maintainability (code that scales with AI + human review)

| ID | Item | Why | Done looks like |
|----|------|-----|-----------------|
| **Q-10** | **Split god-files** | `confirm_run` ~5.4k, `tx_table` ~4.7k, `block` ~4.2k, `query/lib` ~3.3k, IBD `confirm` ~2.7k | Stage/IO modules &lt;~1.5k lines; tests next to stage or in `tests/` without dual oracles |
| **Q-11** | **Extract longest functions** | IBD main loop, pin batch, `eval_script`, peer frame handlers, perf formatters | Named pure helpers; unit tests on policy tables without full IBD |
| **Q-12** | **Kill or quarantine `allow(dead_code)`** | bulk_io, address_head, file, uring_session still allow | Delete unused; or `#[cfg(test)]` only; AGENTS policy holds |
| **Q-13** | **`IbdConfig::for_test` in node** | Blurs production clamps | Node uses explicit production constructor; `for_test` only under `cfg(test)` / rbitcoin-test |
| **Q-14** | **Head-module glossary** | address_head / hashhead / sharded / segmented / scripthash_head | One architecture diagram + “when to use which” table in docs |
| **Q-15** | **RPC crate destiny** | Still a stub; description already honest | Either minimal useful node RPC slice *or* remove from default workspace “product” narrative |

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

### P4 — Explicit non-goals (until a pillar above is green)

| Item | Why deferred |
|------|----------------|
| macOS / Windows operator binary | io_uring + map-free fd design is Linux-shaped |
| Core-compatible full RPC surface | Not the product thesis |
| Graphical block explorer APIs | Explicit non-goal in README |
| 100% line coverage | Replaced by **≥90%** LCOV + property tests |

---

## Baseline snapshot (living metrics)

Re-measure when claiming a maintainability win. Approximate as of post-2026-08 work:

| Metric | Approx. |
|--------|---------|
| First-party Rust LOC | ~121k |
| Workspace crates | 14 (`rbitcoin-esplora` added vs early audit) |
| Largest files | `confirm_run` ~5.4k, `tx_table` ~4.7k, `block` ~4.2k, `query/lib` ~3.3k |
| `#[test]` count | ~1.0k+ |
| Coverage gate | **≥90%** first-party LCOV (`LH`/`LF`) — required CI |
| CI rustc | **1.95.0** pinned (matches nixos-26.05 / shell) |
| Nix pin | **nixos-26.05** + crane 0.23.x |
| Host cargo targets | `target/dev` (test) / `target/cov` (coverage) |
| Release | `nix build .#rbitcoin-musl` → static install |

### Grade board (subjective; update when P0/P1 moves)

| Dimension | Grade | Trend vs 2026-08-06 audit |
|-----------|-------|---------------------------|
| Architecture clarity | Strong | → |
| Dependency hygiene | Strong | ↑ (rayon/xorf/bincode out; in-tree fuse8) |
| Operator honesty | Strong | ↑ (README map-free; Linux badge; milestone story) |
| Code modularity / size | Weak | → (files grew with features) |
| Cross-platform | Weak (honest) | → (now explicit non-goal) |
| Docs consistency | Strong | ↑↑ |
| Contributor onboarding | Medium–Strong | ↑ (how-we-plan, AGENTS, external findings) |
| CI fidelity | Strong | ↑↑ (pin 1.95; coverage job; target dirs) |
| Dead / stub surface | Medium | ↑ slightly (RPC description honest) |
| Test reliability/speed | Medium–Strong | ↑ (suite budgets; target split; deep resume fix) |
| Adversarial / findings hygiene | Strong | ↑↑ (external_findings + Core allowlist work) |

---

## Deep dive: how to get to “leading” on each pillar

### 1. Correctness

- **Never reopen soft dual paths** on confirm identity / denserels / Class A load (AGENTS lean rules).  
- Treat **Core JSON corpora + allowlist empty** as a permanent CI invariant.  
- Expand **fuzzamoto-style** campaigns after each store/consensus slice; log findings under `docs/external_findings/` with Status.  
- Prefer **scenario pins** that drive shipped entry points over test-only oracles.

### 2. Operator trust

- One **glossary** for confirm stages (lookup/load/scripts/write) and queues.  
- OPERATOR is the **env bible**; code without a row is unstable.  
- Milestone skip remains loud on CLI and progress logs.  
- Experimental mainnet doc stays linked from README.

### 3. Build integrity

- CI pin == flake pin == shell pin (today: **1.95** class).  
- Musl only via crane; never ship host `cargo build --release`.  
- Coverage/test **target dirs split** so gates stay fast.  
- Add deny/audit when supply-chain is a release gate.

### 4. Contributor velocity

- **how-we-plan.md** Red→Green→Refactor is the process standard.  
- God-file splits are **vertical** (stage contracts), not horizontal “all store then all net.”  
- Suite speed budgets in TESTING.md are real acceptance criteria for PRs that add default tests &gt;2 s.

### 5. Product surface

- Electrum/Esplora for **wallet clients** (documented); not explorer search.  
- RPC stub: either a thin useful subset or stop listing it as a peer of full crates.  
- Esplora REST growth should stay client-driven, not catalogue-driven.

### 6. Observability

- Default INFO short enough to ship; DEBUG/perf_dbg for deep IBD forensics.  
- Optional JSON later — do not break operator greps.

### 7. Platform

- Linux first forever until a funded IO port. State it in every top-level doc (done in README; keep consistent).

---

## Resolved (closed since 2026-08-06 audit and related work)

Items below were open in the original audit or immediately adjacent. **Do not re-open without new evidence.**

### Docs & honesty

| Was | Resolution |
|-----|------------|
| README “map epochs” / mmap mental model | **Fixed** — README and architecture stress **map-free** tables, HWM publish, no map epochs |
| Linux-only not front-and-center | **Fixed** — README platform row + “Linux is the supported IO target” |
| 100% coverage theater | **Fixed** — gate is **≥90%** LCOV; CONTRIBUTING/COVERAGE aligned |
| SECURITY contact missing | **Fixed** — `security@reardencode.com` in SECURITY.md |
| RPC described as Core-compatible while stub | **Fixed** (description) — package text says stub / not Core surface yet |
| Dual handbook chaos | **Improved** — AGENTS + CONTRIBUTING + how-we-plan; still some overlap (acceptable) |

### Build & CI

| Was | Resolution |
|-----|------------|
| CI floating `@stable` vs Nix 1.82 | **Fixed** — CI pins **rustc 1.95.0**; Nix on **nixos-26.05** / same class |
| Coverage + test thrash one `target/` | **Fixed** — `target/dev` vs `target/cov` (shell, coverage.sh, CI caches) |
| Monolithic `test` job hid fmt/clippy | **Fixed** — CI jobs `fmt`, `clippy`, `test`, `coverage` (coverage needs the three gates) |
| No CodeQL / code scanning workflow | **Fixed** — `.github/workflows/codeql.yml` (Rust `build-mode: none` + Actions, `security-extended`, weekly schedule; manual mode is unsupported for Rust) |
| Deps: rayon / xorf / bincode on hot graph | **Fixed** — in-tree fuse8 + script_pool; store Cargo notes |

### Correctness & adversarial

| Was | Resolution |
|-----|------------|
| No external findings process | **Fixed** — `docs/external_findings/` + status on 001–011 (and follow-ups) |
| Core script/TX allowlist debt | **Largely fixed** — empty Core script allowlist work; TX residual tracked |
| Most-work reorg / tip hole livelocks | **Largely fixed** — multi-hop reorg, tip-hole race, zombie pending, resume seed stack/O(N²) fixes |
| SH/fuse wipe risk on format | **Fixed** — soft-migrate fuse8; AGENTS on-disk format rules |

### Product surface growth (post-audit)

| Was | Resolution |
|-----|------------|
| No Esplora | **Added** — REST + wallet-scoped WS; scoped as wallet-client backend |
| Signet-only custom nets | **Improved** — custom signet / mutinynet support |

### Still intentionally open (see Remaining)

God-files, ignored multi-node IBD in default CI, `allow(dead_code)` bulk_io, `IbdConfig::for_test` in node, cargo deny/SBOM, continuous fuzz in CI, first-hour tutorial, MSRV 1.74 vs 1.95 honesty.

---

## What is already high quality (protect)

- Distinct product thesis (archive + pure Rust scripts + in-process wallet APIs).  
- Small, intentional dependency graph; no `libbitcoinconsensus`.  
- Operator honesty (experimental, milestone, Linux-first).  
- Written concurrency model (roles, HWM, single Class A appender).  
- Portable static musl + crane layering + repro notes.  
- Warnings-as-errors; TDD / how-we-plan culture.  
- SCHEMA / SCHEMA_HISTORY / crash-recovery / COMPAT depth rare at 0.x.  
- External findings hygiene and Core corpus discipline.  
- Active cleanup culture (rename pipelines, soft migrate, no silent format wipes).

---

## Suggested consumers

| Audience | Read |
|----------|------|
| Maintainers picking the next refactor | Remaining **P0–P1** |
| Release engineering | P0 **Q-05**, P2 **Q-20–Q-23** |
| Security / adversarial | P0 **Q-01–Q-03**, P3 **Q-30** |
| Docs / README | North star + Remaining **Q-04**, **Q-34** |
| “Are we industry-leading yet?” | North star pillars + grade board |

---

*Living document. Prefer updating this file over creating dated audit copies.*
