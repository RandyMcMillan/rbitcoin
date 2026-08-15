//! Live P2P session table for RPC (`getpeerinfo` / `addnode` / `disconnectnode`).

use crate::error::NetError;
use bitcoin::p2p::message_network::VersionMessage;
use bitcoin::p2p::ServiceFlags;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::mpsc;

/// How we classified the session (Core `connection_type`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PeerConnType {
    Inbound,
    OutboundFullRelay,
    BlockRelay,
    AddrFetch,
    Feeler,
}

impl PeerConnType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Inbound => "inbound",
            Self::OutboundFullRelay => "outbound-full-relay",
            Self::BlockRelay => "block-relay-only",
            Self::AddrFetch => "addr-fetch",
            Self::Feeler => "feeler",
        }
    }

    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "inbound" => Ok(Self::Inbound),
            "outbound-full-relay" => Ok(Self::OutboundFullRelay),
            "block-relay-only" => Ok(Self::BlockRelay),
            "addr-fetch" => Ok(Self::AddrFetch),
            "feeler" => Ok(Self::Feeler),
            other => Err(format!("unknown connection type {other}")),
        }
    }
}

/// Request that the node dial `addr` as `typ`.
#[derive(Clone, Debug)]
pub struct DialRequest {
    pub addr: SocketAddr,
    pub typ: PeerConnType,
}

/// One live session (RPC snapshot + disconnect flag + byte counters).
pub struct LivePeer {
    pub id: u64,
    pub addr: SocketAddr,
    pub addrbind: SocketAddr,
    pub subver: String,
    pub inbound: bool,
    pub services: u64,
    pub startingheight: i32,
    pub conn_type: PeerConnType,
    pub stop: AtomicBool,
    recv: Mutex<HashMap<String, u64>>,
    sent: Mutex<HashMap<String, u64>>,
}

impl LivePeer {
    pub fn note_recv(&self, cmd: &str, nbytes: u64) {
        let n = acct_bytes(cmd, nbytes);
        *self
            .recv
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(cmd.into())
            .or_insert(0) += n;
    }

    pub fn note_sent(&self, cmd: &str, nbytes: u64) {
        let n = acct_bytes(cmd, nbytes);
        *self
            .sent
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(cmd.into())
            .or_insert(0) += n;
    }

    pub fn request_disconnect(&self) {
        self.stop.store(true, Ordering::SeqCst);
    }

    fn snapshot(&self) -> PeerInfo {
        PeerInfo {
            id: self.id,
            addr: self.addr,
            addrbind: self.addrbind,
            subver: self.subver.clone(),
            inbound: self.inbound,
            services: self.services,
            startingheight: self.startingheight,
            bytesrecv_per_msg: self.recv.lock().unwrap_or_else(|e| e.into_inner()).clone(),
            bytessent_per_msg: self.sent.lock().unwrap_or_else(|e| e.into_inner()).clone(),
            conn_type: self.conn_type,
        }
    }
}

/// Count at least Core's 24-byte header; `pong` must be ≥29 for `connect_nodes`.
fn acct_bytes(cmd: &str, payload: u64) -> u64 {
    let n = payload.saturating_add(24);
    if cmd == "pong" {
        n.max(29)
    } else {
        n
    }
}

/// RPC-facing snapshot.
#[derive(Clone, Debug)]
pub struct PeerInfo {
    pub id: u64,
    pub addr: SocketAddr,
    pub addrbind: SocketAddr,
    pub subver: String,
    pub inbound: bool,
    pub services: u64,
    pub startingheight: i32,
    pub bytesrecv_per_msg: HashMap<String, u64>,
    pub bytessent_per_msg: HashMap<String, u64>,
    pub conn_type: PeerConnType,
}

/// Thread-safe session table + addnode remembered addrs.
pub struct PeerHub {
    next_id: AtomicU64,
    live: RwLock<HashMap<u64, Arc<LivePeer>>>,
    added: Mutex<HashSet<SocketAddr>>,
    dial_tx: Mutex<Option<mpsc::UnboundedSender<DialRequest>>>,
}

impl PeerHub {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            next_id: AtomicU64::new(0),
            live: RwLock::new(HashMap::new()),
            added: Mutex::new(HashSet::new()),
            dial_tx: Mutex::new(None),
        })
    }

    pub fn set_dialer(&self, tx: mpsc::UnboundedSender<DialRequest>) {
        *self.dial_tx.lock().unwrap_or_else(|e| e.into_inner()) = Some(tx);
    }

    pub fn register(
        &self,
        addr: SocketAddr,
        addrbind: SocketAddr,
        ver: &VersionMessage,
        inbound: bool,
        conn_type: PeerConnType,
    ) -> Arc<LivePeer> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let services = service_flags_u64(ver.services);
        let peer = Arc::new(LivePeer {
            id,
            addr,
            addrbind,
            subver: ver.user_agent.clone(),
            inbound,
            services,
            startingheight: ver.start_height,
            conn_type,
            stop: AtomicBool::new(false),
            recv: Mutex::new(HashMap::new()),
            sent: Mutex::new(HashMap::new()),
        });
        // Handshake already exchanged version + verack (+ maybe ping).
        peer.note_recv("version", 100);
        peer.note_recv("verack", 0);
        self.live
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id, Arc::clone(&peer));
        peer
    }

    pub fn unregister(&self, id: u64) {
        self.live
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&id);
    }

    pub fn snapshot(&self) -> Vec<PeerInfo> {
        let g = self.live.read().unwrap_or_else(|e| e.into_inner());
        let mut v: Vec<_> = g.values().map(|p| p.snapshot()).collect();
        v.sort_by_key(|p| p.id);
        v
    }

    pub fn get(&self, id: u64) -> Option<Arc<LivePeer>> {
        self.live
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&id)
            .cloned()
    }

    pub fn addnode(&self, addr: SocketAddr, cmd: &str) -> Result<(), String> {
        match cmd {
            "onetry" => self.dial(addr, PeerConnType::OutboundFullRelay),
            "add" => {
                self.added
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .insert(addr);
                let _ = self.dial(addr, PeerConnType::OutboundFullRelay);
                Ok(())
            }
            "remove" => {
                self.added
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .remove(&addr);
                self.disconnect_addr(addr);
                Ok(())
            }
            other => Err(format!("unknown addnode command {other}")),
        }
    }

    pub fn addconnection(&self, addr: SocketAddr, typ: PeerConnType) -> Result<(), String> {
        if matches!(typ, PeerConnType::Inbound) {
            return Err("addconnection cannot create inbound".into());
        }
        self.dial(addr, typ)
    }

    pub fn disconnect_id(&self, id: u64) -> bool {
        if let Some(p) = self.get(id) {
            p.request_disconnect();
            true
        } else {
            false
        }
    }

    pub fn disconnect_addr(&self, addr: SocketAddr) -> bool {
        let g = self.live.read().unwrap_or_else(|e| e.into_inner());
        let mut n = 0usize;
        for p in g.values() {
            if p.addr == addr {
                p.request_disconnect();
                n += 1;
            }
        }
        n > 0
    }

    fn dial(&self, addr: SocketAddr, typ: PeerConnType) -> Result<(), String> {
        let g = self.dial_tx.lock().unwrap_or_else(|e| e.into_inner());
        let tx = g.as_ref().ok_or("no dialer attached")?;
        tx.send(DialRequest { addr, typ })
            .map_err(|_| "dialer closed".to_string())
    }
}

fn service_flags_u64(f: ServiceFlags) -> u64 {
    // rust-bitcoin 0.32: ServiceFlags is a bitflags newtype.
    f.to_u64()
}

/// Parse Core `ip:port` / `[v6]:port`.
pub fn parse_peer_addr(s: &str) -> Result<SocketAddr, NetError> {
    s.parse()
        .map_err(|_| NetError::Encode(format!("bad peer address {s}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::p2p::address::Address;
    use std::net::{IpAddr, Ipv4Addr};

    fn ver(ua: &str) -> VersionMessage {
        VersionMessage {
            version: 70016,
            services: ServiceFlags::NETWORK | ServiceFlags::WITNESS | ServiceFlags::P2P_V2,
            timestamp: 0,
            receiver: Address::new(
                &SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1),
                ServiceFlags::NONE,
            ),
            sender: Address::new(
                &SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 2),
                ServiceFlags::NONE,
            ),
            nonce: 1,
            user_agent: ua.into(),
            start_height: 0,
            relay: true,
        }
    }

    #[test]
    fn peerhub_register_snapshot_disconnect() {
        let hub = PeerHub::new();
        let a = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 18444);
        let b = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 18445);
        let p = hub.register(
            a,
            b,
            &ver("/rbitcoin:0.1.0(testnode0)/"),
            false,
            PeerConnType::OutboundFullRelay,
        );
        p.note_recv("pong", 8);
        let snap = hub.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].id, 0);
        assert_eq!(snap[0].subver, "/rbitcoin:0.1.0(testnode0)/");
        assert!(!snap[0].inbound);
        assert!(snap[0].bytesrecv_per_msg.get("pong").copied().unwrap() >= 29);
        assert!(hub.disconnect_id(0));
        assert!(p.stop.load(Ordering::SeqCst));
        hub.unregister(0);
        assert!(hub.snapshot().is_empty());
    }

    #[test]
    fn addnode_unknown_command() {
        let hub = PeerHub::new();
        let a = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1);
        assert!(hub.addnode(a, "nope").is_err());
    }
}
