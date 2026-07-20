//! IBD download peer: split read/write over BIP324 v2.
//!
//! Socket tasks stay **I/O-only**:
//! - reader: decrypt frame + cheap ping handling; heavy decode off-thread
//! - writer: encode offloaded for heavy payloads; then encrypt + write

use crate::codec::{MAX_HEADERS_RESULTS, MAX_INV_SIZE};
use crate::error::NetError;
use crate::msg_decode::spawn_decode_then_with_err;
use crate::peer::connect_and_handshake;
use crate::v2::{read_v2_frame_with_progress, write_v2_msg_offload};
use bitcoin::block::Header;
use bitcoin::hashes::Hash;
use bitcoin::p2p::message::NetworkMessage;
use bitcoin::p2p::message_blockdata::{GetHeadersMessage, Inventory};
use bitcoin::p2p::{Magic, ServiceFlags};
use bitcoin::{Block, BlockHash};
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

pub(crate) enum PeerCmd {
    GetHeaders { locator: Vec<BlockHash> },
    GetData { hashes: Vec<BlockHash> },
    Shutdown,
}

pub(crate) enum PeerEvent {
    Headers { peer: usize, headers: Vec<Header> },
    /// Full `block` frame on the wire (hash from header only). Free getdata slots
    /// immediately — full deserialize still in flight on the blocking pool.
    BlockFramed {
        peer: usize,
        hash: BlockHash,
        /// Payload size (for peer speed classification).
        wire_bytes: usize,
    },
    Block { peer: usize, block: Block },
    /// Framed as `block` but deserialize failed / unexpected command — re-request.
    BlockDecodeFailed { peer: usize, hash: BlockHash },
    /// Peer answered `notfound` for these block hashes (does not have them).
    NotFound { peer: usize, hashes: Vec<BlockHash> },
    /// Addresses learned from `addr` / `addrv2` (for IBD redial pool growth).
    Addrs { peer: usize, addrs: Vec<SocketAddr> },
    /// Peer failed or closed.
    Dead { peer: usize, reason: String },
}

/// Dual fan-out so body delivery is never stuck behind header floods on one FIFO.
///
/// - **body**: `BlockFramed` / `Block` / decode fail / `NotFound` / `Dead` — drain first
/// - **ctrl**: `Headers` — budgeted so multi-peer header spam cannot livelock apply
#[derive(Clone)]
pub(crate) struct PeerEventSinks {
    pub body: mpsc::UnboundedSender<PeerEvent>,
    pub ctrl: mpsc::UnboundedSender<PeerEvent>,
}

impl PeerEventSinks {
    pub(crate) fn send_body(&self, ev: PeerEvent) {
        let _ = self.body.send(ev);
    }
    pub(crate) fn send_ctrl(&self, ev: PeerEvent) {
        let _ = self.ctrl.send(ev);
    }
}

pub(crate) struct PeerSlot {
    pub id: usize,
    pub addr: SocketAddr,
    pub cmd_tx: mpsc::UnboundedSender<PeerCmd>,
    /// Hashes currently requested from this peer.
    pub in_flight: HashSet<BlockHash>,
    /// Last block-download progress as [`ibd_mono_ms`].
    pub block_progress_ms: Arc<AtomicU64>,
    /// Peer's `version.start_height` (best-effort network tip signal).
    pub peer_height: u32,
    /// Mono ms when the slot became live (post-handshake).
    pub connected_ms: u64,
    /// First block-payload mono ms (0 = none yet).
    pub first_data_ms: AtomicU64,
    /// Cumulative block payload bytes (speed sample).
    pub bytes_rx: AtomicU64,
    pub alive: bool,
    pub task: JoinHandle<()>,
}

impl PeerSlot {
    /// Record received block payload bytes for FAST/SLOW classification.
    pub fn note_rx_bytes(&self, n: u64) {
        if n == 0 {
            return;
        }
        let now = ibd_mono_ms();
        let _ = self.first_data_ms.compare_exchange(
            0,
            now,
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
        self.bytes_rx.fetch_add(n, Ordering::Relaxed);
    }

    /// `(latency_ms, bytes_per_sec)` once we have ≥64 KiB of block data.
    pub fn speed_sample(&self) -> Option<(u64, u64)> {
        let first = self.first_data_ms.load(Ordering::Relaxed);
        if first == 0 {
            return None;
        }
        let bytes = self.bytes_rx.load(Ordering::Relaxed);
        if bytes < 64 * 1024 {
            return None;
        }
        let latency_ms = first.saturating_sub(self.connected_ms);
        let elapsed_ms = ibd_mono_ms().saturating_sub(first).max(1);
        let bps = bytes.saturating_mul(1000) / elapsed_ms;
        Some((latency_ms, bps))
    }
}

impl Drop for PeerSlot {
    fn drop(&mut self) {
        let _ = self.cmd_tx.send(PeerCmd::Shutdown);
        self.task.abort();
    }
}

/// Monotonic milliseconds for IBD stall clocks (process-relative).
pub(crate) fn ibd_mono_ms() -> u64 {
    static T0: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    T0.get_or_init(Instant::now).elapsed().as_millis() as u64
}

pub(crate) fn touch_block_progress(ms: &AtomicU64) {
    ms.store(ibd_mono_ms(), Ordering::Relaxed);
}

pub(crate) fn note_block_progress(slots: &mut [PeerSlot], peer: usize) {
    if let Some(s) = slots.iter_mut().find(|s| s.id == peer) {
        touch_block_progress(&s.block_progress_ms);
    }
}

pub(crate) fn note_block_rx(slots: &mut [PeerSlot], peer: usize, wire_bytes: usize) {
    if let Some(s) = slots.iter_mut().find(|s| s.id == peer) {
        touch_block_progress(&s.block_progress_ms);
        s.note_rx_bytes(wire_bytes as u64);
    }
}

pub(crate) async fn spawn_peer(
    id: usize,
    addr: SocketAddr,
    magic: Magic,
    local: SocketAddr,
    tip_h: Option<u32>,
    sinks: PeerEventSinks,
) -> Result<PeerSlot, NetError> {
    let stream = TcpStream::connect(addr).await?;
    let (ver, reader, writer) = connect_and_handshake(
        stream,
        magic,
        local,
        addr,
        tip_h.map(|h| h as i32).unwrap_or(0),
        false,
    )
    .await?;
    // Peer's advertised chain height — used as IBD progress horizon when our
    // local header path has not yet reached the network tip.
    let peer_height = u32::try_from(ver.start_height).unwrap_or(0);

    let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<PeerCmd>();
    // Reader → writer for pongs (must not write on the read task — that would
    // stall the receive half and look like a peer stall).
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<NetworkMessage>();
    let block_progress_ms = Arc::new(AtomicU64::new(ibd_mono_ms()));
    let progress_io = Arc::clone(&block_progress_ms);

    // Parent owns concurrent read + write tasks. Aborting the parent (PeerSlot
    // Drop / stall disconnect) must abort both children — plain JoinHandle drop
    // only detaches.
    let task = tokio::spawn(async move {
        /// Aborts both halves if the parent task is cancelled mid-flight.
        struct PeerIoTasks {
            reader: tokio::task::JoinHandle<()>,
            writer: tokio::task::JoinHandle<()>,
        }
        impl Drop for PeerIoTasks {
            fn drop(&mut self) {
                self.reader.abort();
                self.writer.abort();
            }
        }

        // Reader first: Core pipelines sendheaders/sendcmpct/… right after verack.
        // July 18 cold-start worked with **no** post-handshake getaddr/sendaddrv2
        // before getheaders; those writes raced Core's pipeline and peers closed
        // (ordered=0 / inflight=0 / never archive).
        let mut reader = reader;
        let sinks_r = sinks.clone();
        let reader_task = tokio::spawn(async move {
            let mut prog_mark = 0usize;
            loop {
                let frame = read_v2_frame_with_progress(&mut reader, magic, |buffered| {
                    const STEP: usize = 64 * 1024;
                    if buffered >= prog_mark + STEP || buffered <= STEP {
                        prog_mark = buffered;
                        touch_block_progress(&progress_io);
                    }
                })
                .await;
                prog_mark = 0;
                match frame {
                    Ok(frame) => {
                        if frame.is_ping() {
                            if let Some(n) = frame.ping_nonce() {
                                let _ = out_tx.send(NetworkMessage::Pong(n));
                            }
                            continue;
                        }

                        if frame.is_block() || frame.is_notfound() {
                            touch_block_progress(&progress_io);
                        }

                        let framed_block_hash = if frame.is_block() {
                            frame.block_hash_from_header()
                        } else {
                            None
                        };
                        if let Some(hash) = framed_block_hash {
                            sinks_r.send_body(PeerEvent::BlockFramed {
                                peer: id,
                                hash,
                                wire_bytes: frame.payload.len(),
                            });
                        }

                        let progress = Arc::clone(&progress_io);
                        let sinks_d = sinks_r.clone();
                        let sinks_err = sinks_r.clone();
                        let framed_err_hash = framed_block_hash;
                        spawn_decode_then_with_err(
                            frame,
                            move |msg| {
                                match msg.into_payload() {
                                    NetworkMessage::Headers(h) => {
                                        let headers = if h.len() > MAX_HEADERS_RESULTS {
                                            h[..MAX_HEADERS_RESULTS].to_vec()
                                        } else {
                                            h
                                        };
                                        sinks_d.send_ctrl(PeerEvent::Headers {
                                            peer: id,
                                            headers,
                                        });
                                    }
                                    NetworkMessage::Block(b) => {
                                        touch_block_progress(&progress);
                                        sinks_d.send_body(PeerEvent::Block {
                                            peer: id,
                                            block: b,
                                        });
                                    }
                                    NetworkMessage::NotFound(inv) => {
                                        touch_block_progress(&progress);
                                        let hashes: Vec<BlockHash> = inv
                                            .iter()
                                            .filter_map(|i| match i {
                                                Inventory::Block(h)
                                                | Inventory::WitnessBlock(h) => Some(*h),
                                                _ => None,
                                            })
                                            .collect();
                                        if !hashes.is_empty() {
                                            sinks_d.send_body(PeerEvent::NotFound {
                                                peer: id,
                                                hashes,
                                            });
                                        }
                                    }
                                    NetworkMessage::Addr(list) => {
                                        let addrs = socket_addrs_from_addr(&list);
                                        if !addrs.is_empty() {
                                            sinks_d.send_ctrl(PeerEvent::Addrs {
                                                peer: id,
                                                addrs,
                                            });
                                        }
                                    }
                                    NetworkMessage::AddrV2(list) => {
                                        let addrs = socket_addrs_from_addrv2(&list);
                                        if !addrs.is_empty() {
                                            sinks_d.send_ctrl(PeerEvent::Addrs {
                                                peer: id,
                                                addrs,
                                            });
                                        }
                                    }
                                    NetworkMessage::SendAddrV2 => {}
                                    other => {
                                        if let Some(hash) = framed_err_hash {
                                            let _ = other;
                                            sinks_d.send_body(PeerEvent::BlockDecodeFailed {
                                                peer: id,
                                                hash,
                                            });
                                        }
                                    }
                                }
                            },
                            move || {
                                if let Some(hash) = framed_err_hash {
                                    sinks_err.send_body(PeerEvent::BlockDecodeFailed {
                                        peer: id,
                                        hash,
                                    });
                                }
                            },
                        );
                    }
                    Err(NetError::Io(e))
                        if e.kind() == std::io::ErrorKind::UnexpectedEof
                            || e.kind() == std::io::ErrorKind::ConnectionReset =>
                    {
                        sinks_r.send_body(PeerEvent::Dead {
                            peer: id,
                            reason: format!("eof: {e}"),
                        });
                        break;
                    }
                    Err(e) => {
                        sinks_r.send_body(PeerEvent::Dead {
                            peer: id,
                            reason: e.to_string(),
                        });
                        break;
                    }
                }
            }
        });

        // Let the reader poll once before we accept write work (getheaders).
        tokio::task::yield_now().await;

        let mut writer = writer;
        let sinks_w = sinks;
        let writer_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    cmd = cmd_rx.recv() => {
                        match cmd {
                            Some(PeerCmd::GetHeaders { locator }) => {
                                let locator = if locator.len() > crate::codec::MAX_LOCATOR_SZ {
                                    locator[..crate::codec::MAX_LOCATOR_SZ].to_vec()
                                } else {
                                    locator
                                };
                                let gh = GetHeadersMessage::new(
                                    locator,
                                    BlockHash::from_byte_array([0u8; 32]),
                                );
                                if write_v2_msg_offload(
                                    &mut writer,
                                    NetworkMessage::GetHeaders(gh),
                                )
                                .await
                                .is_err()
                                {
                                    sinks_w.send_body(PeerEvent::Dead {
                                        peer: id,
                                        reason: "write getheaders failed".into(),
                                    });
                                    break;
                                }
                            }
                            Some(PeerCmd::GetData { hashes }) => {
                                for chunk in hashes.chunks(MAX_INV_SIZE) {
                                    let inv: Vec<_> = chunk
                                        .iter()
                                        .copied()
                                        .map(Inventory::WitnessBlock)
                                        .collect();
                                    if inv.is_empty() {
                                        continue;
                                    }
                                    if write_v2_msg_offload(
                                        &mut writer,
                                        NetworkMessage::GetData(inv),
                                    )
                                    .await
                                    .is_err()
                                    {
                                        sinks_w.send_body(PeerEvent::Dead {
                                            peer: id,
                                            reason: "write getdata failed".into(),
                                        });
                                        return;
                                    }
                                }
                            }
                            Some(PeerCmd::Shutdown) | None => break,
                        }
                    }
                    msg = out_rx.recv() => {
                        match msg {
                            Some(payload) => {
                                if write_v2_msg_offload(&mut writer, payload).await.is_err() {
                                    sinks_w.send_body(PeerEvent::Dead {
                                        peer: id,
                                        reason: "write outbound failed".into(),
                                    });
                                    break;
                                }
                            }
                            None => break,
                        }
                    }
                }
            }
        });

        let mut guard = PeerIoTasks {
            reader: reader_task,
            writer: writer_task,
        };
        tokio::select! {
            _ = &mut guard.reader => {}
            _ = &mut guard.writer => {}
        }
    });

    Ok(PeerSlot {
        id,
        addr,
        cmd_tx,
        in_flight: HashSet::new(),
        block_progress_ms,
        peer_height,
        connected_ms: ibd_mono_ms(),
        first_data_ms: AtomicU64::new(0),
        bytes_rx: AtomicU64::new(0),
        alive: true,
        task,
    })
}

/// IPv4/IPv6 sockets with full/limited network service from classic `addr`.
fn socket_addrs_from_addr(list: &[(u32, bitcoin::p2p::address::Address)]) -> Vec<SocketAddr> {
    let mut out = Vec::with_capacity(list.len().min(32));
    for (_ts, a) in list {
        if !services_useful_for_ibd(a.services) {
            continue;
        }
        if let Ok(sa) = a.socket_addr() {
            if usable_dial_addr(&sa) {
                out.push(sa);
            }
        }
    }
    out
}

/// IPv4/IPv6 sockets with full/limited network service from `addrv2`.
fn socket_addrs_from_addrv2(list: &[bitcoin::p2p::address::AddrV2Message]) -> Vec<SocketAddr> {
    let mut out = Vec::with_capacity(list.len().min(32));
    for a in list {
        if !services_useful_for_ibd(a.services) {
            continue;
        }
        if let Ok(sa) = a.socket_addr() {
            if usable_dial_addr(&sa) {
                out.push(sa);
            }
        }
    }
    out
}

fn services_useful_for_ibd(flags: ServiceFlags) -> bool {
    flags.has(ServiceFlags::NETWORK) || flags.has(ServiceFlags::NETWORK_LIMITED)
}

fn usable_dial_addr(sa: &SocketAddr) -> bool {
    if sa.port() == 0 {
        return false;
    }
    match sa {
        SocketAddr::V4(v4) => {
            let ip = *v4.ip();
            !ip.is_unspecified() && !ip.is_broadcast() && !ip.is_multicast()
        }
        SocketAddr::V6(v6) => {
            let ip = *v6.ip();
            !ip.is_unspecified() && !ip.is_multicast()
        }
    }
}
