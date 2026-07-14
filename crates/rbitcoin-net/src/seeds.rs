//! Fixed seeds and DNS seed names for peer discovery (Phase 4 foundation).

use rbitcoin_primitives::Network;
use std::net::{SocketAddr, ToSocketAddrs};

/// DNS seed hostnames (resolve at runtime).
pub fn dns_seeds(network: Network) -> &'static [&'static str] {
    match network {
        Network::Mainnet => &[
            "seed.bitcoin.sipa.be",
            "dnsseed.bluematt.me",
            "dnsseed.bitcoin.dashjr-list-of-p2p-nodes.us",
            "seed.bitcoinstats.com",
            "seed.bitcoin.jonasschnelli.ch",
            "seed.btc.petertodd.net",
            "seed.bitcoin.sprovoost.nl",
            "dnsseed.emzy.de",
            "seed.bitcoin.wiz.biz",
        ],
        Network::Testnet => &[
            "testnet-seed.bitcoin.jonasschnelli.ch",
            "seed.tbtc.petertodd.net",
            "testnet-seed.bluematt.me",
            "testnet-seed.bitcoin.schildbach.de",
        ],
        Network::Signet => &["seed.signet.bitcoin.sprovoost.nl"],
        Network::Regtest => &[],
    }
}

/// Hard-coded fallback seed addresses (host:port). Sparse; DNS is preferred.
pub fn fixed_seed_hosts(network: Network) -> &'static [&'static str] {
    match network {
        Network::Mainnet => &[
            "seed.bitcoin.sipa.be:8333",
            "dnsseed.emzy.de:8333",
        ],
        Network::Testnet => &["testnet-seed.bitcoin.jonasschnelli.ch:18333"],
        Network::Signet => &["seed.signet.bitcoin.sprovoost.nl:38333"],
        Network::Regtest => &[],
    }
}

/// Default P2P port for a network.
pub fn default_port(network: Network) -> u16 {
    match network {
        Network::Mainnet => 8333,
        Network::Testnet => 18333,
        Network::Signet => 38333,
        Network::Regtest => 18444,
    }
}

/// Resolve fixed seed host strings to socket addresses (best-effort).
pub fn resolve_fixed_seeds(network: Network) -> Vec<SocketAddr> {
    let mut out = Vec::new();
    for host in fixed_seed_hosts(network) {
        if let Ok(iter) = host.to_socket_addrs() {
            out.extend(iter);
        }
    }
    out
}

/// Simple address manager: remembered peers + optional seed inject.
#[derive(Debug, Default, Clone)]
pub struct AddrMan {
    peers: Vec<SocketAddr>,
}

impl AddrMan {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_seeds(network: Network) -> Self {
        let mut a = Self::new();
        a.inject(resolve_fixed_seeds(network));
        a
    }

    pub fn inject(&mut self, addrs: impl IntoIterator<Item = SocketAddr>) {
        for a in addrs {
            if !self.peers.contains(&a) {
                self.peers.push(a);
            }
        }
    }

    pub fn add(&mut self, addr: SocketAddr) {
        self.inject([addr]);
    }

    pub fn peers(&self) -> &[SocketAddr] {
        &self.peers
    }

    pub fn take_outbound(&self, max: usize) -> Vec<SocketAddr> {
        self.peers.iter().copied().take(max).collect()
    }

    pub fn len(&self) -> usize {
        self.peers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }
}
