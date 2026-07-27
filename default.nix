# Non-flake entry that still uses the **pinned** nixpkgs from flake.lock.
# Requires network once to fetch the pinned tarball if not already in the store.
#
#   nix-build -A rbitcoin
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
in
{
  rbitcoin = nixpkgs.callPackage ./nix/rbitcoin.nix { };
  rbitcoin-node = nixpkgs.callPackage ./nix/rbitcoin.nix { };
  rbitcoin-cli = nixpkgs.callPackage ./nix/rbitcoin.nix { };
  rbitcoin-musl = nixpkgs.pkgsStatic.callPackage ./nix/rbitcoin.nix { };
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
