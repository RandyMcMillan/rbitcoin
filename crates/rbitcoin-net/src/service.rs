//! Listen / dial / sync / tip-follow orchestration.

use crate::cache::BlockCache;
use crate::chain::{AcceptOutcome, ChainHub};
use crate::error::NetError;
use crate::ibd::IbdConfig;
use crate::peer::{connect_and_handshake, peer_session_with, FollowSessionMeta};
use bitcoin::p2p::Magic;
use bitcoin::Block;
use bitcoin::BlockHash;
use rbitcoin_consensus::{ChainParams, Milestone};
use rbitcoin_primitives::Network as RNetwork;
use rbitcoin_query::Query;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

#[derive(Clone, Debug)]
pub struct NetConfig {
    pub magic: Magic,
    pub listen: Option<SocketAddr>,
    pub user_agent: String,
}

impl NetConfig {
    pub fn for_regtest(listen: Option<SocketAddr>) -> Self {
        Self {
            magic: Magic::REGTEST,
            listen,
            user_agent: "/rbitcoin:0.1.0/".into(),
        }
    }
}

/// Running P2P node handle (listen + optional outbound sync / tip follow).
pub struct P2PNode {
    /// Shared RAM cache (also on hub).
    pub cache: Arc<BlockCache>,
    /// Shared query store (also on hub).
    pub query: Arc<Query>,
    pub hub: Arc<ChainHub>,
    pub local_addr: SocketAddr,
    magic: Magic,
    shutdown: Arc<AtomicBool>,
    /// Live outbound tip-follow sessions (inc/dec inside session task).
    follow_live: Arc<AtomicUsize>,
    tasks: Vec<JoinHandle<()>>,
}

pub struct P2PHandle {
    pub cache: Arc<BlockCache>,
    pub query: Arc<Query>,
    pub local_addr: SocketAddr,
}

impl P2PNode {
    /// Bind listener. Serves getheaders/getdata and participates in tip announce/follow.
    pub async fn start(
        listen: SocketAddr,
        query: Query,
        params: ChainParams,
        milestone: Milestone,
    ) -> Result<Self, NetError> {
        let magic = Magic::from(params.network);
        let hub = Arc::new(ChainHub::new(query, params, milestone));
        hub.ensure_genesis()?;
        let cache = hub.cache.clone();
        let query = hub.query.clone();
        let listener = TcpListener::bind(listen).await?;
        let local_addr = listener.local_addr()?;
        let shutdown = Arc::new(AtomicBool::new(false));

        let hub_c = hub.clone();
        let shutdown_c = shutdown.clone();
        let magic_c = magic;
        let accept_task = tokio::spawn(async move {
            loop {
                if shutdown_c.load(Ordering::SeqCst) {
                    break;
                }
                let accept =
                    tokio::time::timeout(Duration::from_millis(200), listener.accept()).await;
                match accept {
                    Ok(Ok((stream, peer_addr))) => {
                        let our = local_addr;
                        let hub = hub_c.clone();
                        let height = hub.tip_height().map(|h| h as i32).unwrap_or(0);
                        let tip_rx = hub.subscribe_tips();
                        tokio::spawn(async move {
                            let (_ver, reader, writer) = match connect_and_handshake(
                                stream,
                                magic_c,
                                our,
                                peer_addr,
                                height,
                                true,
                            )
                            .await
                            {
                                Ok(x) => x,
                                Err(e) => {
                                    // V1-only peers fail BIP324; log once-style message.
                                    rbitcoin_log::debug!(
                                        "p2p: inbound handshake {peer_addr} failed: {e}"
                                    );
                                    return;
                                }
                            };
                            // Inbound: serve + tip announce + active getheaders pull.
                            let meta = FollowSessionMeta {
                                peer: Some(peer_addr),
                                live: None,
                            };
                            let _ =
                                peer_session_with(reader, writer, magic_c, hub, tip_rx, meta).await;
                        });
                    }
                    Ok(Err(_)) => break,
                    Err(_) => continue,
                }
            }
        });

        Ok(Self {
            cache,
            query,
            hub,
            local_addr,
            magic,
            shutdown,
            follow_live: Arc::new(AtomicUsize::new(0)),
            tasks: vec![accept_task],
        })
    }

    /// Number of live outbound tip-follow sessions.
    pub fn follow_live_count(&self) -> usize {
        self.follow_live.load(Ordering::SeqCst)
    }

    pub fn handle(&self) -> P2PHandle {
        P2PHandle {
            cache: self.cache.clone(),
            query: self.query.clone(),
            local_addr: self.local_addr,
        }
    }

    pub fn tip_height(&self) -> Option<u32> {
        self.hub.tip_height()
    }

    /// Push a validated block into cache + store.
    pub fn ingest_block(&self, height: u32, block: Block) -> Result<(), NetError> {
        let _ = height;
        match self.hub.accept_block(block)? {
            AcceptOutcome::Accepted { .. } | AcceptOutcome::AlreadyHave => Ok(()),
            AcceptOutcome::IgnoredWeaker => Err(NetError::Protocol("weaker tip ignored")),
        }
    }

    /// IBD / catch-up: multi-peer download window across `peers` (libbitcoin-class).
    ///
    /// This is the only history-sync path. Tip-follow is [`Self::follow_from`].
    pub async fn sync(
        &self,
        peers: &[SocketAddr],
        cfg: IbdConfig,
    ) -> Result<u32, NetError> {
        self.sync_cancellable(peers, cfg, None).await
    }

    /// IBD with optional cooperative cancel flag (SIGINT / SIGTERM path).
    pub async fn sync_cancellable(
        &self,
        peers: &[SocketAddr],
        cfg: IbdConfig,
        cancel: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    ) -> Result<u32, NetError> {
        crate::ibd::ibd_cancellable(
            self.hub.clone(),
            self.magic,
            self.local_addr,
            peers,
            cfg,
            cancel,
        )
        .await
    }

    /// IBD with default window (1024 concurrent getdata, 16/peer).
    pub async fn sync_default(&self, peers: &[SocketAddr]) -> Result<u32, NetError> {
        self.sync(peers, IbdConfig::default()).await
    }

    /// Persistent outbound peer: tip-follow + announce for the session lifetime.
    ///
    /// After handshake the session sends `getheaders` from our tip locator so
    /// any gap (e.g. blocks mined during SH materialize) is filled actively.
    /// Call [`Self::sync`] first when far behind (multi-thousand height IBD).
    pub async fn follow_from(&mut self, peer: SocketAddr) -> Result<(), NetError> {
        let stream = TcpStream::connect(peer).await?;
        let height = self.tip_height().map(|h| h as i32).unwrap_or(0);
        let (_ver, reader, writer) = connect_and_handshake(
            stream,
            self.magic,
            self.local_addr,
            peer,
            height,
            false,
        )
        .await?;
        let hub = self.hub.clone();
        let tip_rx = hub.subscribe_tips();
        let magic = self.magic;
        let live = self.follow_live.clone();
        // Count as live now (handshake done); session task decrements on exit.
        live.fetch_add(1, Ordering::SeqCst);
        let meta = FollowSessionMeta {
            peer: Some(peer),
            live: Some(live),
        };
        let task = tokio::spawn(async move {
            let _ = peer_session_with(reader, writer, magic, hub, tip_rx, meta).await;
        });
        self.tasks.push(task);
        Ok(())
    }

    pub async fn wait_height(&self, height: u32, timeout: Duration) -> Result<(), NetError> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.tip_height().unwrap_or(0) >= height {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(NetError::Timeout);
            }
            tokio::select! {
                _ = self.hub.notify.notified() => {}
                _ = tokio::time::sleep(Duration::from_millis(5)) => {}
            }
        }
    }

    pub async fn wait_tip_hash(
        &self,
        hash: BlockHash,
        timeout: Duration,
    ) -> Result<(), NetError> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.hub.tip_hash() == Some(hash) {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(NetError::Timeout);
            }
            tokio::select! {
                _ = self.hub.notify.notified() => {}
                _ = tokio::time::sleep(Duration::from_millis(5)) => {}
            }
        }
    }

    pub async fn shutdown(mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        for t in self.tasks.drain(..) {
            t.abort();
        }
        // Do **not** full-flush here: `run.rs` already calls `flush_for_shutdown`
        // (or callers that need durability flush explicitly). Double multi‑GiB
        // msync/fdatasync was a multi-minute host freeze on exit.
    }
}

/// Map our Network enum to bitcoin Magic.
pub fn magic_for(network: RNetwork) -> Magic {
    Magic::from(match network {
        RNetwork::Mainnet => bitcoin::Network::Bitcoin,
        RNetwork::Testnet => bitcoin::Network::Testnet,
        RNetwork::Signet => bitcoin::Network::Signet,
        RNetwork::Regtest => bitcoin::Network::Regtest,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn magic_for_all_networks_and_regtest_config() {
        assert_eq!(magic_for(RNetwork::Mainnet), Magic::from(bitcoin::Network::Bitcoin));
        assert_eq!(magic_for(RNetwork::Testnet), Magic::from(bitcoin::Network::Testnet));
        assert_eq!(magic_for(RNetwork::Signet), Magic::from(bitcoin::Network::Signet));
        assert_eq!(magic_for(RNetwork::Regtest), Magic::REGTEST);
        let cfg = NetConfig::for_regtest(None);
        assert_eq!(cfg.magic, Magic::REGTEST);
        assert!(cfg.listen.is_none());
        assert_eq!(cfg.user_agent, "/rbitcoin:0.1.0/");
    }
}
