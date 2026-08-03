{
  description = "rbitcoin — pinned Nix builds for byte-identical release binaries";

  # Pin advanced via flake.lock (nix flake lock). Do not use import <nixpkgs> {}.
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.11";
  # Layered cargo builds: deps derivation + app derivation (faster rebuilds).
  # Pin a crane that works with nixos-24.11 rustc (1.82). Latest crane wants
  # nixpkgs ≥26.05 and pulls edition2024 crates into crane-utils.
  inputs.crane.url = "github:ipetkov/crane/v0.20.1";

  outputs =
    { self, nixpkgs, crane }:
    let
      # Systems we expose packages for (native builds when host matches).
      systems = [
        "x86_64-linux"
        "aarch64-linux"
      ];
      forAllSystems = nixpkgs.lib.genAttrs systems;

      mkRbitcoin =
        pkgs:
        pkgs.callPackage ./nix/rbitcoin.nix {
          craneLib = crane.mkLib pkgs;
        };
    in
    {
      packages = forAllSystems (
        system:
        let
          pkgs = import nixpkgs {
            inherit system;
            # Disable impure overlays; pure evaluation for reproducibility.
            config = { };
            overlays = [ ];
          };
          # Optional dynamic glibc package (Nix-store linked; not portable off-store).
          rbitcoin-glibc = mkRbitcoin pkgs;
          # Primary / default: fully static musl — portable operator binary.
          rbitcoin-musl = mkRbitcoin pkgs.pkgsStatic;
        in
        {
          default = rbitcoin-musl;
          rbitcoin = rbitcoin-musl;
          rbitcoin-node = rbitcoin-musl;
          rbitcoin-cli = rbitcoin-musl;
          rbitcoin-musl = rbitcoin-musl;
          # Kept for store-native Nix environments / optional dual-platform repro.
          rbitcoin-glibc = rbitcoin-glibc;
        }
        // nixpkgs.lib.optionalAttrs (system == "x86_64-linux") {
          # Optional third platform: aarch64-linux cross from x86_64 (heavy toolchain).
          rbitcoin-aarch64 =
            let
              pkgsAarch64 = import nixpkgs {
                system = "x86_64-linux";
                crossSystem = {
                  config = "aarch64-unknown-linux-gnu";
                };
                config = { };
                overlays = [ ];
              };
            in
            mkRbitcoin pkgsAarch64;
        }
      );

      # Dev shell uses the **same pinned** nixpkgs (not floating <nixpkgs>).
      devShells = forAllSystems (
        system:
        let
          pkgs = import nixpkgs {
            inherit system;
            config = { };
            overlays = [ ];
          };
        in
        {
          default = pkgs.mkShell {
            packages = with pkgs; [
              rustc
              cargo
              rustfmt
              clippy
              llvmPackages.bintools
              llvmPackages.llvm
              cargo-llvm-cov
              pkg-config
            ];
            RUST_BACKTRACE = "1";
            # Dev shell still denies warnings; release package uses its own RUSTFLAGS.
            RUSTFLAGS = "-Dwarnings";
            shellHook = ''
              export LLVM_COV="${pkgs.llvmPackages.llvm}/bin/llvm-cov"
              export LLVM_PROFDATA="${pkgs.llvmPackages.llvm}/bin/llvm-profdata"
              echo "rbitcoin devShell: rustc=$(rustc --version) (pinned nixpkgs via flake)"
            '';
          };
        }
      );

      # `nix flake check` can validate the package builds on the current system.
      checks = forAllSystems (
        system:
        {
          rbitcoin = self.packages.${system}.rbitcoin-musl;
        }
      );
    };
}
