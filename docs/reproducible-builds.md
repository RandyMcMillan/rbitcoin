# Reproducible builds

Contributors with **Nix** (not necessarily NixOS) can produce **byte-for-byte
identical**, **fully static (musl)** release binaries of `rbitcoin-node` and
`rbitcoin-cli` for a given git revision and target triple. These run on ordinary
Linux hosts without a Nix store or matching glibc.

Host-installed `rustc`, floating `import <nixpkgs> {}`, and
`cargo build --release` inside `nix-shell` / `nix develop` are **not** the
release path. The latter links against the Nix-store glibc interpreter and fails
with `No such file or directory` when run outside that environment. Use the
pinned flake (or `default.nix` + `flake.lock`) musl package.

## What is pinned

| Input | Mechanism |
|-------|-----------|
| nixpkgs (rustc, cargo, musl, linker, …) | `flake.lock` → `nixpkgs` rev + `narHash` |
| Rust crate graph | `Cargo.lock` (crates.io checksums) |
| Source tree | Flake `self` filtered via `lib.cleanSourceWith` in `nix/rbitcoin.nix` |

Release builds set remapped path prefixes and strip symbols so digests do not
depend on the builder’s checkout path or username.

## Requirements

- [Nix](https://nixos.org/download/) 2.18+ with flakes enabled  
  (`experimental-features = nix-command flakes` in `nix.conf`)
- Network access the first time a pin is fetched into the store
- Linux (packages are Linux-only; primary CI/agent is `x86_64-linux`)

## Build (primary / portable static)

From the repository root:

```bash
# Preferred — fully static musl (also the flake default package)
nix build .#rbitcoin-musl
# Binaries:
#   ./result/bin/rbitcoin-node
#   ./result/bin/rbitcoin-cli

# Install where operators typically look:
mkdir -p target/release
install -m 755 result/bin/rbitcoin-node result/bin/rbitcoin-cli target/release/

# Helper (same attr; default target is musl)
./scripts/repro-build.sh
# or: ./scripts/repro-build.sh musl
```

Non-flake equivalent (still uses **`flake.lock`**, not `<nixpkgs>`):

```bash
nix-build -A rbitcoin-musl
```

Target triple is host-CPU musl (`x86_64-unknown-linux-musl` on x86_64 hosts,
`aarch64-unknown-linux-musl` on aarch64 hosts) via `pkgsStatic`.

Dev shell with the **same** pin (for tests/clippy — **not** for operator release
binaries):

```bash
nix develop
# or: nix-shell   # shell.nix reads flake.lock
cargo test --workspace
```

## Optional: glibc dynamic (Nix-store linked)

Only needed if you intentionally want a dynamic glibc link for store-native
Nix environments. **Not** portable off the Nix store; not the operator default.

```bash
nix build .#rbitcoin
./scripts/repro-build.sh glibc
# or: nix-build -A rbitcoin
```

**Bit-identity with the musl build is not expected**; only same-target digests
must match across two clean builds of the same package.

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
./scripts/repro-check.sh          # musl static (primary)
./scripts/repro-check.sh both     # musl + optional glibc
./scripts/repro-check.sh glibc    # glibc only
```

What the script does:

1. Realize the package once (`nix build`) so a prior store path exists
2. Run **`nix build --rebuild` twice** (re-executes the builder; logs must
   contain `checking outputs of '….drv'…` — plain cache hits fail the check)
3. SHA-256 `rbitcoin-node` / `rbitcoin-cli` after each rebuild and `diff`
4. Never falls back to a plain `nix build` for the second pass (that would false-pass)

You can also compare by hand:

```bash
nix build .#rbitcoin-musl --out-link /tmp/rbitcoin-a --rebuild
nix build .#rbitcoin-musl --out-link /tmp/rbitcoin-b --rebuild
sha256sum /tmp/rbitcoin-a/bin/* /tmp/rbitcoin-b/bin/*
```

Identical store paths after two pure builds are also fine; always hash the
**file contents** of the bins when reporting digests.

## Layout

| Path | Role |
|------|------|
| `flake.nix` / `flake.lock` | Pinned inputs + package outputs (`default` = musl) |
| `nix/rbitcoin.nix` | `buildRustPackage` for node + CLI |
| `default.nix` | Non-flake attrset using the same pin |
| `shell.nix` | Dev shell from the same pin |
| `scripts/repro-build.sh` | One-shot build + print digests (default: musl) |
| `scripts/repro-check.sh` | Double clean build gate (default: musl) |

## Non-goals

- Matching digests across different target triples or OS/libc combinations
- Reproducibility with an arbitrary host Rust toolchain outside Nix
- Guix-style bootstrap of the compiler itself
- Shipping nix-shell `cargo build --release` binaries as the operator product
