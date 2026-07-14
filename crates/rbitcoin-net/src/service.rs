//! Listen / dial / sync orchestration.

use crate::cache::BlockCache;
use crate::error::NetError;
use crate::peer::{handshake, serve_peer_loop, sync_from_peer};
use bitcoin::hashes::Hash;
use bitcoin::p2p::Magic;
use bitcoin::Block;
use bitcoin::BlockHash;
use rbitcoin_consensus::{accept_and_connect_block, ChainParams, Milestone};
use rbitcoin_primitives::{Height, Network as RNetwork};
use rbitcoin_query::Query;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
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

/// Running P2P node handle (listen + optional outbound sync).
pub struct P2PNode {
    pub cache: Arc<BlockCache>,
    pub query: Arc<Query>,
    pub params: ChainParams,
    pub milestone: Milestone,
    pub local_addr: SocketAddr,
    magic: Magic,
    shutdown: Arc<AtomicBool>,
    notify: Arc<Notify>,
    tasks: Vec<JoinHandle<()>>,
}

pub struct P2PHandle {
    pub cache: Arc<BlockCache>,
    pub query: Arc<Query>,
    pub local_addr: SocketAddr,
}

impl P2PNode {
    /// Bind listener. Serves getheaders/getdata from the store (reconstruct) plus RAM cache.
    pub async fn start(
        listen: SocketAddr,
        query: Query,
        params: ChainParams,
        milestone: Milestone,
    ) -> Result<Self, NetError> {
        let magic = Magic::from(params.network);
        let cache = Arc::new(BlockCache::new());
        let query = Arc::new(query);
        let listener = TcpListener::bind(listen).await?;
        let local_addr = listener.local_addr()?;
        let shutdown = Arc::new(AtomicBool::new(false));
        let notify = Arc::new(Notify::new());

        let cache_c = cache.clone();
        let query_c = query.clone();
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
                        let cache = cache_c.clone();
                        let query = query_c.clone();
                        let height = query
                            .tip_height()
                            .map(|h| h.0 as i32)
                            .or_else(|| cache.tip_height().map(|h| h as i32))
                            .unwrap_or(0);
                        tokio::spawn(async move {
                            if handshake(&mut stream, magic_c, our, peer_addr, height, true)
                                .await
                                .is_err()
                            {
                                return;
                            }
                            let _ = serve_peer_loop(stream, magic_c, cache, query).await;
                        });
                    }
                    Ok(Err(_)) => break,
                    Err(_) => continue, // timeout — recheck shutdown
                }
            }
        });

        Ok(Self {
            cache,
            query,
            params,
            milestone,
            local_addr,
            magic,
            shutdown,
            notify,
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

    /// Best known tip height (store preferred).
    pub fn tip_height(&self) -> Option<u32> {
        self.query
            .tip_height()
            .map(|h| h.0)
            .or_else(|| self.cache.tip_height())
    }

    /// Push a validated block into cache + store (must extend best chain).
    pub fn ingest_block(&self, height: u32, block: Block) -> Result<(), NetError> {
        accept_and_connect_block(
            &self.query,
            &self.params,
            Height(height),
            &block,
            self.milestone,
        )
        .map_err(|e| NetError::Consensus(e.to_string()))?;
        self.cache
            .push_best(block)
            .map_err(NetError::Protocol)?;
        self.notify.notify_waiters();
        Ok(())
    }

    /// Connect outbound and sync headers/blocks until peer has no more.
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

        let cache = self.cache.clone();
        let query = self.query.clone();
        let params = self.params.clone();
        let milestone = self.milestone;
        let notify = self.notify.clone();

        let locator = query
            .locator_hashes()
            .map_err(|e| NetError::Consensus(e.to_string()))?;
        // If store empty, use cache locator
        let locator = if locator.len() == 1
            && locator[0].to_byte_array() == [0u8; 32]
            && !cache.is_empty()
        {
            cache.locator()
        } else {
            locator
        };

        let query_has = query.clone();
        let cache_has = cache.clone();
        let n = sync_from_peer(
            &mut stream,
            self.magic,
            locator,
            move |hash| {
                if cache_has.get_block(hash).is_some() {
                    return true;
                }
                query_has
                    .height_of_hash(&hash.to_byte_array())
                    .ok()
                    .flatten()
                    .is_some()
            },
            move |_ignored_height, block| {
                let height = match query.tip_height() {
                    None => 0,
                    Some(t) => t.0.saturating_add(1),
                };
                if let Some(tip_hash) = query
                    .tip_height()
                    .and_then(|h| query.header_at_height(h).ok().flatten())
                    .map(|(_, rec)| BlockHash::from_byte_array(rec.hash))
                {
                    if block.header.prev_blockhash != tip_hash {
                        return Err(NetError::Protocol("block does not connect to tip"));
                    }
                } else if height != 0 {
                    return Err(NetError::Protocol("missing tip for non-genesis"));
                }
                accept_and_connect_block(&query, &params, Height(height), &block, milestone)
                    .map_err(|e| NetError::Consensus(e.to_string()))?;
                let _ = cache.push_best(block);
                notify.notify_waiters();
                Ok(())
            },
        )
        .await?;
        Ok(n)
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
                _ = self.notify.notified() => {}
                _ = tokio::time::sleep(Duration::from_millis(50)) => {}
            }
        }
    }

    pub async fn shutdown(mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        for t in self.tasks.drain(..) {
            t.abort();
        }
        let _ = self.query.flush();
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
