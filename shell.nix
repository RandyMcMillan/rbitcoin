# Convenience shell. Prefer the **pinned** flake dev shell for matching toolchains:
#
#   nix develop
#
# This file uses the same pin as default.nix / flake.lock when available so
# contributors without flake UX still avoid floating <nixpkgs> for day-to-day work.
let
  lock = builtins.fromJSON (builtins.readFile ./flake.lock);
  nixpkgsEntry = lock.nodes.nixpkgs.locked;
  pkgs = import (
    fetchTarball {
      url = "https://github.com/NixOS/nixpkgs/archive/${nixpkgsEntry.rev}.tar.gz";
      sha256 = nixpkgsEntry.narHash;
    }
  ) { };
in
pkgs.mkShell {
  packages = with pkgs; [
    rustc
    cargo
    rustfmt
    clippy
    # Match rustc's LLVM major (nixos-26.05 → rustc 1.95 → LLVM 21).
    llvmPackages.bintools
    llvmPackages.llvm
    cargo-llvm-cov
    pkg-config
    # used in coverage.sh
    python3
  ];

  RUST_BACKTRACE = "1";
  # Deny rustc warnings for first-party crates (also via workspace.lints).
  RUSTFLAGS = "-Dwarnings";
  shellHook = ''
    export LLVM_COV="${pkgs.llvmPackages.llvm}/bin/llvm-cov"
    export LLVM_PROFDATA="${pkgs.llvmPackages.llvm}/bin/llvm-profdata"
    echo "rbitcoin shell.nix: rustc=$(rustc --version) (pinned via flake.lock)"
  '';
}
