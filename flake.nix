{
  description = "rbitcoin — pinned Nix builds for byte-identical release binaries";

  # Pin advanced via flake.lock (nix flake lock). Do not use import <nixpkgs> {}.
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.11";

  outputs =
    { self, nixpkgs }:
    let
      # Systems we expose packages for (native builds when host matches).
      systems = [
        "x86_64-linux"
        "aarch64-linux"
      ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
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
          rbitcoin = pkgs.callPackage ./nix/rbitcoin.nix { };
        in
        {
          default = rbitcoin;
          rbitcoin = rbitcoin;
          rbitcoin-node = rbitcoin;
          rbitcoin-cli = rbitcoin;
        }
        // {
          # Secondary platform (distinct Rust target triple): fully static musl.
          # Same host CPU, different libc/target than the glibc primary package.
          rbitcoin-musl = pkgs.pkgsStatic.callPackage ./nix/rbitcoin.nix { };
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
            pkgsAarch64.callPackage ./nix/rbitcoin.nix { };
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
          rbitcoin = self.packages.${system}.rbitcoin;
        }
      );
    };
}
