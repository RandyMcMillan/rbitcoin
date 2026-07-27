# Reproducible builds

Contributors with **Nix** (not necessarily NixOS) can produce **byte-for-byte
identical** release binaries of `rbitcoin-node` and `rbitcoin-cli` for a given
git revision and target triple.

Host-installed `rustc` / floating `import <nixpkgs> {}` are **not** the
reproducible path. Use the pinned flake (or `default.nix` + `flake.lock`).

## What is pinned

| Input | Mechanism |
|-------|-----------|
| nixpkgs (glibc, rustc, cargo, linker, …) | `flake.lock` → `nixpkgs` rev + `narHash` |
| Rust crate graph | `Cargo.lock` (crates.io checksums) |
| Source tree | Flake `self` filtered via `lib.cleanSourceWith` in `nix/rbitcoin.nix` |

Release builds set remapped path prefixes and strip symbols so digests do not
depend on the builder’s checkout path or username.

## Requirements

- [Nix](https://nixos.org/download/) 2.18+ with flakes enabled  
  (`experimental-features = nix-command flakes` in `nix.conf`)
- Network access the first time a pin is fetched into the store
- Linux (packages are Linux-only; primary CI/agent is `x86_64-linux`)

## Build (primary / native)

From the repository root:

```bash
# Preferred
nix build .#rbitcoin
# Binaries:
#   ./result/bin/rbitcoin-node
#   ./result/bin/rbitcoin-cli

# Helper (same attr)
./scripts/repro-build.sh native
```

Non-flake equivalent (still uses **`flake.lock`**, not `<nixpkgs>`):

```bash
nix-build -A rbitcoin
```

Dev shell with the **same** pin (for tests/clippy — not required for release digests):

```bash
nix develop
# or: nix-shell   # shell.nix reads flake.lock
```

## Second platform (musl static)

Distinct Rust target triple (`x86_64-unknown-linux-musl` on x86_64 hosts,
`aarch64-unknown-linux-musl` on aarch64 hosts) via `pkgsStatic`:

```bash
nix build .#rbitcoin-musl
./scripts/repro-build.sh musl
# or: nix-build -A rbitcoin-musl
```

Binaries are fully static (musl). **Bit-identity with the glibc build is not
expected**; only same-target digests must match across two clean musl builds.

### Optional: aarch64-linux cross (x86_64 host)

```bash
nix build .#rbitcoin-aarch64
./scripts/repro-build.sh aarch64
```

Pulls a full cross toolchain (large). Prefer musl for routine multi-target
checks when disk or cache is limited.

## Verify byte-identity (same platform)

Two independent clean rebuilds must yield the same SHA-256 for each binary:

```bash
./scripts/repro-check.sh          # native (glibc) only
./scripts/repro-check.sh both     # native + musl
```

What the script does:

1. `nix build .#rbitcoin --rebuild` → hash `rbitcoin-node` / `rbitcoin-cli`
2. Repeat rebuild → hash again
3. `diff` the digest lists (exit non-zero if they diverge)

You can also compare by hand:

```bash
nix build .#rbitcoin --out-link /tmp/rbitcoin-a --rebuild
nix build .#rbitcoin --out-link /tmp/rbitcoin-b --rebuild
sha256sum /tmp/rbitcoin-a/bin/* /tmp/rbitcoin-b/bin/*
```

Identical store paths after two pure builds are also fine; always hash the
**file contents** of the bins when reporting digests.

## Layout

| Path | Role |
|------|------|
| `flake.nix` / `flake.lock` | Pinned inputs + package outputs |
| `nix/rbitcoin.nix` | `buildRustPackage` for node + CLI |
| `default.nix` | Non-flake attrset using the same pin |
| `shell.nix` | Dev shell from the same pin |
| `scripts/repro-build.sh` | One-shot build + print digests |
| `scripts/repro-check.sh` | Double clean build gate |

## Non-goals

- Matching digests across different target triples or OS/libc combinations
- Reproducibility with an arbitrary host Rust toolchain outside Nix
- Guix-style bootstrap of the compiler itself
