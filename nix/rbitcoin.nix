# Reproducible release package for rbitcoin-node + rbitcoin-cli.
# Called from flake.nix with a pinned nixpkgs; do not import <nixpkgs> here.
{
  lib,
  rustPlatform,
  stdenv,
  pkg-config,
}:

rustPlatform.buildRustPackage rec {
  pname = "rbitcoin";
  version = "0.1.0";

  # Flake passes a cleaned source of the monorepo root.
  src = lib.cleanSourceWith {
    src = ../.;
    filter =
      path: type:
      let
        base = baseNameOf path;
      in
      # Drop local datadirs, logs, coverage, and IDE noise from the build input.
      !(lib.hasPrefix "datadir" base)
      && !(lib.hasSuffix ".log" base)
      && base != "target"
      && base != ".git"
      && base != "coverage"
      && base != ".coverage"
      && base != "result"
      && base != "result-1"
      && base != "result-2";
  };

  cargoLock = {
    lockFile = ../Cargo.lock;
  };

  # Release product only — full workspace tests stay on the CI/dev path.
  doCheck = false;

  cargoBuildFlags = [
    "-p"
    "rbitcoin-node"
    "-p"
    "rbitcoin-cli"
  ];

  # Deterministic path remapping for any debuginfo; align with fixed SOURCE_DATE_EPOCH.
  # Nix already sets SOURCE_DATE_EPOCH for fixed sources; keep RUSTFLAGS free of host paths.
  RUSTFLAGS = lib.concatStringsSep " " [
    "--remap-path-prefix"
    "${src}=/source"
    "-C"
    "debuginfo=0"
    "-C"
    "strip=symbols"
  ];

  nativeBuildInputs = [ pkg-config ];

  meta = with lib; {
    description = "Experimental Bitcoin full node (rbitcoin-node) and CLI";
    license = with licenses; [
      mit
      asl20
    ];
    mainProgram = "rbitcoin-node";
    platforms = platforms.linux;
  };
}
