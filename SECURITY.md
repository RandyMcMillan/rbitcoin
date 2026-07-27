# Security policy

## Supported versions

| Version | Support |
|---------|---------|
| **0.x** (this tree) | Experimental. Security fixes land on a best-effort basis while the project is under active development. There is **no** long-term support promise and **no** production SLA. |

Treat any mainnet use as a **lab / reckless** deployment. Prefer signet and
regtest for validation work. See [`docs/experimental-mainnet.md`](./docs/experimental-mainnet.md).

## Reporting a vulnerability

Please report security issues **privately** — do not open a public GitHub issue
for unfixed remote or consensus-critical bugs.

1. If a public git remote / contact is published for this repository, use that
   project’s preferred private channel (security email or private advisory).
2. Until a public contact is listed here, contact the maintainers through the
   same private channel you already use for this codebase (direct maintainer
   contact). Include: affected version/commit, network (mainnet/signet/regtest),
   impact assessment, and a minimal reproduction when possible.
3. Allow reasonable time for a fix or public mitigation note before disclosure.

We will acknowledge receipt when we can and coordinate disclosure timing for
issues that affect consensus, P2P DoS surface, or Electrum/data integrity.

## Scope notes (experimental node)

- **Consensus and script:** pure-Rust verification; bugs can mean accepting
  invalid chain data or rejecting valid data. Report both.
- **P2P:** BIP324 v2-only; DoS parity with Bitcoin Core is **not** claimed.
- **Electrum:** binds plain TCP; TLS is an operator reverse-proxy concern.
- **No wallet / keys in this repository:** do not send seed phrases or private
  keys in reports.

## Non-security bugs

Use ordinary issue trackers or contribution channels for crashes, IBD stalls,
and documentation errors that are not security-sensitive — see
[`CONTRIBUTING.md`](./CONTRIBUTING.md).
