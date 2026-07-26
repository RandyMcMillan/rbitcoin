//! Off-thread P2P payload decode so socket tasks stay I/O-bound.
//!
//! Async peer readers call [`crate::v2::read_v2_frame`] then hand the
//! [`FramedMessage`] to [`decode_framed_offload`]. That runs
//! [`FramedMessage::decode`] on Tokio's **blocking** pool (OS threads), freeing
//! multi-thread runtime workers to poll other sockets.
//!
//! A process-wide semaphore bounds concurrent heavy decodes so we do not
//! explode the blocking pool or hold unbounded multi-MB frames in RAM.

use crate::codec::FramedMessage;
use crate::error::NetError;
use bitcoin::p2p::message::RawNetworkMessage;
use std::sync::{Arc, OnceLock};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Concurrent **block** payload decodes (Class A feed). Sized for IBD window.
fn block_decode_permits() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get().saturating_mul(8).max(64))
        .unwrap_or(64)
}

/// Concurrent **headers/notfound** decodes. Kept smaller so header storms cannot
/// starve block deserialize (signet: drain flooded while arch_q=0 / writer idle).
fn ctrl_decode_permits() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get().saturating_mul(2).max(8))
        .unwrap_or(8)
}

fn block_decode_semaphore() -> &'static Arc<Semaphore> {
    static SEM: OnceLock<Arc<Semaphore>> = OnceLock::new();
    SEM.get_or_init(|| Arc::new(Semaphore::new(block_decode_permits())))
}

fn ctrl_decode_semaphore() -> &'static Arc<Semaphore> {
    static SEM: OnceLock<Arc<Semaphore>> = OnceLock::new();
    SEM.get_or_init(|| Arc::new(Semaphore::new(ctrl_decode_permits())))
}

/// Decode a framed message on the blocking pool.
///
/// Acquires a decode permit first so IBD with many peers cannot queue unlimited
/// multi-MB frames. Blocks use a separate, larger pool than headers so multi-peer
/// getheaders spam cannot stall Class A. The calling async task **awaits** the
/// permit and the join, but does **not** run CPU work on a multi-thread worker.
pub async fn decode_framed_offload(frame: FramedMessage) -> Result<RawNetworkMessage, NetError> {
    if frame.is_block() {
        let permit = block_decode_semaphore()
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| NetError::Protocol("decode semaphore closed"))?;
        decode_with_permit(frame, permit).await
    } else if frame.decode_is_cpu_heavy() {
        let permit = ctrl_decode_semaphore()
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| NetError::Protocol("decode semaphore closed"))?;
        decode_with_permit(frame, permit).await
    } else {
        // Tiny messages (verack, ping already handled, sendheaders, …): cheap
        // enough that a spawn_blocking round-trip is pure overhead. Still keep
        // this path free of multi-MB work.
        Ok(frame.decode())
    }
}

pub(crate) async fn decode_with_permit(
    frame: FramedMessage,
    permit: OwnedSemaphorePermit,
) -> Result<RawNetworkMessage, NetError> {
    let msg = tokio::task::spawn_blocking(move || {
        let msg = frame.decode();
        drop(permit);
        msg
    })
    .await
    .map_err(|_| NetError::Protocol("decode task join failed"))?;
    Ok(msg)
}

/// Acquire a **block** decode permit (process-owned multi-MB frame budget).
///
/// IBD readers must hold this **before** parking a framed block for off-thread
/// decode and before reading the next frame. Otherwise fire-and-forget tasks
/// wait on the semaphore while each retains a full wire payload (unbounded
/// process RAM when peers outrun the decode pool — especially while archive
/// is stalled on `tx.head` resize).
pub async fn acquire_block_decode_permit() -> Result<OwnedSemaphorePermit, NetError> {
    block_decode_semaphore()
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| NetError::Protocol("decode semaphore closed"))
}

/// Fire-and-forget decode with a **pre-acquired** permit (frame already budgeted).
pub fn spawn_decode_with_permit<F, E>(
    frame: FramedMessage,
    permit: OwnedSemaphorePermit,
    on_done: F,
    on_err: E,
) where
    F: FnOnce(RawNetworkMessage) + Send + 'static,
    E: FnOnce() + Send + 'static,
{
    tokio::spawn(async move {
        match decode_with_permit(frame, permit).await {
            Ok(msg) => on_done(msg),
            Err(_) => on_err(),
        }
    });
}

/// Fire-and-forget heavy decode: free the reader immediately after framing.
///
/// Prefer [`spawn_decode_with_permit`] + [`acquire_block_decode_permit`] for
/// **block** frames so waiting multi-MB payloads cannot outrun the permit pool.
/// This entry still acquires a permit **inside** the spawned task (safe for
/// non-block ctrl messages; do not use for unbounded block fan-in).
pub fn spawn_decode_then_with_err<F, E>(frame: FramedMessage, on_done: F, on_err: E)
where
    F: FnOnce(RawNetworkMessage) + Send + 'static,
    E: FnOnce() + Send + 'static,
{
    tokio::spawn(async move {
        match decode_framed_offload(frame).await {
            Ok(msg) => on_done(msg),
            Err(_) => on_err(),
        }
    });
}
