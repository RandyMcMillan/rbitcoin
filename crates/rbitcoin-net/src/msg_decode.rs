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

async fn decode_with_permit(
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

/// Fire-and-forget heavy decode: free the reader immediately after framing.
///
/// Used by IBD peer readers so one slow `block` deserialize never blocks the
/// next TCP read on that peer. `on_done` runs on the async runtime after decode
/// (keep it light — only channel sends). `on_err` runs if the blocking join /
/// semaphore fails (optional re-request path for framed block hashes).
///
/// **Invariant:** readers must not await a decode permit (or any other soft
/// archive-queue gate) before the next TCP read. Soft queue budget is enforced
/// only by stopping new block *requests* (`can_assign` / getdata), never by
/// refusing to read or decode data a peer already sends for a prior request.
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

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::hashes::Hash as _;
    use bitcoin::p2p::message::NetworkMessage;
    use bitcoin::p2p::Magic;
    use bitcoin::Network;
    use std::sync::{Arc, Mutex};

    fn verack_frame() -> FramedMessage {
        let magic = Magic::from(Network::Regtest);
        let payload = Vec::<u8>::new();
        let dig = bitcoin::hashes::sha256d::Hash::hash(&payload);
        let ba = dig.to_byte_array();
        FramedMessage {
            magic,
            command: *b"verack\0\0\0\0\0\0",
            checksum: [ba[0], ba[1], ba[2], ba[3]],
            payload,
        }
    }

    #[test]
    fn decode_offload_light_and_spawn() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let msg = rt.block_on(decode_framed_offload(verack_frame())).unwrap();
        assert!(matches!(msg.payload(), NetworkMessage::Verack));

        let done = Arc::new(Mutex::new(false));
        let d2 = done.clone();
        rt.block_on(async {
            spawn_decode_then_with_err(
                verack_frame(),
                move |_| {
                    *d2.lock().unwrap() = true;
                },
                || panic!("should not err"),
            );
            // Yield so the spawned task can finish.
            tokio::task::yield_now().await;
            for _ in 0..50 {
                if *done.lock().unwrap() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        });
        assert!(*done.lock().unwrap());
    }
}
