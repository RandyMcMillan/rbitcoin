{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
  packages = with pkgs; [
    rustc
    cargo
    rustfmt
    clippy
    llvmPackages.bintools
    llvmPackages.llvm
    cargo-llvm-cov
    pkg-config
    openssl
  ];

  RUST_BACKTRACE = "1";
  shellHook = ''
    export LLVM_COV="${pkgs.llvmPackages.llvm}/bin/llvm-cov"
    export LLVM_PROFDATA="${pkgs.llvmPackages.llvm}/bin/llvm-profdata"
  '';
}
