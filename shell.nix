{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
  packages = with pkgs; [
    rustc
    cargo
    rustfmt
    clippy
    llvmPackages.bintools
    llvmPackages.llvm
    pkg-config
    openssl
  ];

  RUST_BACKTRACE = "1";
}
