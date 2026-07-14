//! Listen / dial / sync / tip-follow orchestration.

use crate::cache::BlockCache;
use crate::chain::{AcceptOutcome, ChainHub};
use crate::error::NetError;
use crate::peer::{handshake, peer_session, sync_from_peer};
use bitcoin::hashes::Hash;
use bitcoin::p2p::Magic;
use bitcoin::Block;
use bitcoin::BlockHash;
use rbitcoin_consensus::{ChainParams, Milestone};
use rbitcoin_primitives::Network as RNetwork;
use rbitcoin_query::Query;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
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
                    Ok(Ok((mut stream, peer_addr))) => {
                        let our = local_addr;
                        let hub = hub_c.clone();
                        let height = hub.tip_height().map(|h| h as i32).unwrap_or(0);
                        let tip_rx = hub.subscribe_tips();
                        tokio::spawn(async move {
                            if handshake(&mut stream, magic_c, our, peer_addr, height, true)
                                .await
                                .is_err()
                            {
                                return;
                            }
                            // Inbound: do not catch_up (avoids getheaders deadlock with dialer).
                            let _ = peer_session(stream, magic_c, hub, tip_rx, false).await;
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
            tasks: vec![accept_task],
        })
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

    /// Try multiple peers in order until one yields blocks (or all fail).
    pub async fn sync_from_peers(&self, peers: &[SocketAddr]) -> Result<u32, NetError> {
        if peers.is_empty() {
            return Err(NetError::Protocol("no peers to sync from"));
        }
        let mut last_err = NetError::Protocol("all peers failed");
        let mut total = 0u32;
        for peer in peers {
            match self.sync_from(*peer).await {
                Ok(n) => {
                    total = total.saturating_add(n);
                    if n > 0 {
                        return Ok(total);
                    }
                }
                Err(e) => last_err = e,
            }
        }
        if total > 0 {
            Ok(total)
        } else {
            Err(last_err)
        }
    }

    /// Connect outbound, catch up via getheaders, then return (connection closed).
    pub async fn sync_from(&self, peer: SocketAddr) -> Result<u32, NetError> {
        let mut stream = TcpStream::connect(peer).await?;
        let height = self.tip_height().map(|h| h as i32).unwrap_or(0);
        handshake(
            &mut stream,
            self.magic,
            self.local_addr,
            peer,
            height,
            false,
        )
        .await?;

        let hub = self.hub.clone();
        let locator = hub
            .query
            .locator_hashes()
            .map_err(|e| NetError::Consensus(e.to_string()))?;
        let locator = if locator.len() == 1
            && locator[0].to_byte_array() == [0u8; 32]
            && !hub.cache.is_empty()
        {
            hub.cache.locator()
        } else {
            locator
        };

        let n = sync_from_peer(
            &mut stream,
            self.magic,
            locator,
            |hash| hub.has_block(hash),
            |_, block| hub.accept_block(block).map(|_| ()),
        )
        .await?;
        Ok(n)
    }

    /// Persistent outbound peer: catch up, then tip-follow + announce for the session lifetime.
    pub async fn follow_from(&mut self, peer: SocketAddr) -> Result<(), NetError> {
        let mut stream = TcpStream::connect(peer).await?;
        let height = self.tip_height().map(|h| h as i32).unwrap_or(0);
        handshake(
            &mut stream,
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
        let task = tokio::spawn(async move {
            let _ = peer_session(stream, magic, hub, tip_rx, true).await;
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
                _ = tokio::time::sleep(Duration::from_millis(50)) => {}
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
                _ = tokio::time::sleep(Duration::from_millis(50)) => {}
            }
        }
    }

    pub async fn shutdown(mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        for t in self.tasks.drain(..) {
            t.abort();
        }
        let _ = self.hub.query.flush();
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
