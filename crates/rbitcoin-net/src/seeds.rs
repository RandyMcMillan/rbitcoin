//! Fixed seeds and DNS seed names for peer discovery.

use rbitcoin_primitives::Network;
use std::net::{SocketAddr, ToSocketAddrs};

/// DNS seed hostnames (resolve at runtime with default port).
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
        Network::Mainnet => &["seed.bitcoin.sipa.be:8333", "dnsseed.emzy.de:8333"],
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

/// Resolve DNS seed hostnames to socket addresses using the network default port.
pub fn resolve_dns_seeds(network: Network) -> Vec<SocketAddr> {
    let port = default_port(network);
    let mut out = Vec::new();
    for host in dns_seeds(network) {
        let with_port = format!("{host}:{port}");
        if let Ok(iter) = with_port.to_socket_addrs() {
            out.extend(iter);
        }
    }
    out
}

/// Resolve DNS + fixed seeds (DNS first, then fixed fallbacks). Deduplicated.
pub fn resolve_all_seeds(network: Network) -> Vec<SocketAddr> {
    let mut out = resolve_dns_seeds(network);
    for a in resolve_fixed_seeds(network) {
        if !out.contains(&a) {
            out.push(a);
        }
    }
    out
}

/// Simple address manager: remembered peers + seed inject.
#[derive(Debug, Default, Clone)]
pub struct AddrMan {
    peers: Vec<SocketAddr>,
}

impl AddrMan {
    pub fn new() -> Self {
        Self::default()
    }

    /// Populate from DNS seeds and fixed seed hosts.
    pub fn with_seeds(network: Network) -> Self {
        let mut a = Self::new();
        a.inject(resolve_all_seeds(network));
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

    /// Round-robin-ish: take up to `max` peers starting at `offset`.
    pub fn take_outbound_offset(&self, max: usize, offset: usize) -> Vec<SocketAddr> {
        if self.peers.is_empty() || max == 0 {
            return Vec::new();
        }
        let n = self.peers.len();
        let mut out = Vec::with_capacity(max.min(n));
        for i in 0..max.min(n) {
            out.push(self.peers[(offset + i) % n]);
        }
        out
    }

    pub fn take_outbound(&self, max: usize) -> Vec<SocketAddr> {
        self.take_outbound_offset(max, 0)
    }

    pub fn len(&self) -> usize {
        self.peers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }
}
