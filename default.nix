# Non-flake entry that still uses the **pinned** nixpkgs from flake.lock.
# Requires network once to fetch the pinned tarball if not already in the store.
#
#   nix-build -A rbitcoin-musl   # portable static (preferred / default attr name)
#   nix-build -A rbitcoin        # same as rbitcoin-musl
#   nix-build -A rbitcoin-glibc  # optional dynamic glibc (not portable)
#   nix-build -A rbitcoin-aarch64   # x86_64 host only (cross)
#
let
  lock = builtins.fromJSON (builtins.readFile ./flake.lock);
  nixpkgsEntry = lock.nodes.nixpkgs.locked;
  nixpkgs = import (
    fetchTarball {
      url = "https://github.com/NixOS/nixpkgs/archive/${nixpkgsEntry.rev}.tar.gz";
      sha256 = nixpkgsEntry.narHash;
    }
  ) { };
  rbitcoin-musl = nixpkgs.pkgsStatic.callPackage ./nix/rbitcoin.nix { };
  rbitcoin-glibc = nixpkgs.callPackage ./nix/rbitcoin.nix { };
in
{
  # Primary / portable static.
  rbitcoin = rbitcoin-musl;
  rbitcoin-node = rbitcoin-musl;
  rbitcoin-cli = rbitcoin-musl;
  rbitcoin-musl = rbitcoin-musl;
  # Optional dynamic glibc (Nix-store linked).
  rbitcoin-glibc = rbitcoin-glibc;
  rbitcoin-aarch64 =
    let
      pkgsCross = import (
        fetchTarball {
          url = "https://github.com/NixOS/nixpkgs/archive/${nixpkgsEntry.rev}.tar.gz";
          sha256 = nixpkgsEntry.narHash;
        }
      ) {
        crossSystem = {
          config = "aarch64-unknown-linux-gnu";
        };
      };
    in
    pkgsCross.callPackage ./nix/rbitcoin.nix { };
}
