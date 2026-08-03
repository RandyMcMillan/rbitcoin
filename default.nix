# Non-flake entry that still uses the **pinned** nixpkgs (+ crane) from flake.lock.
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
  craneEntry = lock.nodes.crane.locked;
  nixpkgsSrc = fetchTarball {
    url = "https://github.com/NixOS/nixpkgs/archive/${nixpkgsEntry.rev}.tar.gz";
    sha256 = nixpkgsEntry.narHash;
  };
  craneSrc = fetchTarball {
    url = "https://github.com/ipetkov/crane/archive/${craneEntry.rev}.tar.gz";
    sha256 = craneEntry.narHash;
  };
  nixpkgs = import nixpkgsSrc { };

  # Crane default.nix: `{ pkgs }: pkgs.callPackage ./lib { }` → lib with buildPackage etc.
  craneMkLib = pkgs: import craneSrc { inherit pkgs; };

  mk =
    pkgs:
    pkgs.callPackage ./nix/rbitcoin.nix {
      craneLib = craneMkLib pkgs;
    };

  rbitcoin-musl = mk nixpkgs.pkgsStatic;
  rbitcoin-glibc = mk nixpkgs;
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
      pkgsCross = import nixpkgsSrc {
        crossSystem = {
          config = "aarch64-unknown-linux-gnu";
        };
      };
    in
    mk pkgsCross;
}
