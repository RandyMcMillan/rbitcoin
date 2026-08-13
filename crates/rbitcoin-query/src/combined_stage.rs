//! Combined archive-prep + confirm-load stage (single parent-body path).
//!
//! Production confirm load calls [`load_creates_once`] for Class A create decode
//! and pin_new denserels. Always idx→body with `range=None` (no process pin FIFO).
//! Pipeline pins live on the plan (`batch_pin`, `BatchParents`, plan-local
//! `external_parent_outs`); ancient parents use cold Class A.

use rbitcoin_primitives::Fk;
use rbitcoin_store::{
    decode_packed_tx_outs_with_spender_rels_secret, decode_packed_tx_with_spender_rels_secret,
    IdxBodyJob, IdxBodyMode, Store, StoreError, StoreSecret,
};
use std::cell::Cell;

// Per-thread body-ok pread counter (thread-local so parallel tests do not race).
thread_local! {
    static BODY_OK_READS: Cell<u64> = const { Cell::new(0) };
}

/// Reset body-read counter for **this thread** (tests).
pub fn reset_body_ok_reads() {
    BODY_OK_READS.with(|c| c.set(0));
}

/// Snapshot body-read counter for **this thread**.
pub fn body_ok_reads() -> u64 {
    BODY_OK_READS.with(|c| c.get())
}

#[inline]
fn note_body_ok_read() {
    BODY_OK_READS.with(|c| c.set(c.get().saturating_add(1)));
}

/// One create loaded for the combined path.
#[derive(Debug, Clone)]
pub struct CombinedCreate {
    pub fk: Fk,
    pub body_range: (u64, u64),
    pub raw: Vec<u8>,
    /// When `mode == Full`, one full decode lives here so callers do not re-decode.
    pub decoded_full: Option<(
        rbitcoin_store::TxRecord,
        Vec<rbitcoin_store::InputRecord>,
        Vec<rbitcoin_store::OutputRecord>,
        Vec<u32>,
    )>,
    /// When `mode == OutsDenserels`, decoded meta/outs/denserels (avoid re-decode on pin).
    pub decoded_outs: Option<(
        rbitcoin_store::TxRecord,
        Vec<rbitcoin_store::OutputRecord>,
        Vec<u32>,
    )>,
}

/// Load creates by fk via idx→body, decode once.
///
/// Each successful body fetch increments [`body_ok_reads`]. Ranges are always
/// resolved from `tx.idx` (`range=None` on jobs). Callers fill schema-13 zero
/// body `TxRecord.txid` from plan RAM maps when needed — this path never seeds
/// a process pin map and does not fill txid from `txid.body` for that purpose.
///
/// **Shipped entry used by** [`crate::Query::load_confirm_parents`] and wire pin.
pub fn load_creates_once(
    store: &Store,
    fks: &[Fk],
    mode: IdxBodyMode,
) -> Result<Vec<CombinedCreate>, rbitcoin_store::StoreError> {
    if fks.is_empty() {
        return Ok(Vec::new());
    }
    let mut jobs: Vec<IdxBodyJob> = fks
        .iter()
        .map(|fk| IdxBodyJob::new(fk.get().unwrap_or(0), None))
        .collect();
    store.idx_body_pipeline(&mut jobs, mode)?;
    let mut inwit_jobs: Vec<IdxBodyJob> = if mode == IdxBodyMode::Full {
        fks.iter()
            .map(|fk| IdxBodyJob::new(fk.get().unwrap_or(0), None))
            .collect()
    } else {
        Vec::new()
    };
    if mode == IdxBodyMode::Full {
        store.idx_inwit_pipeline(&mut inwit_jobs, IdxBodyMode::Full)?;
    }
    let secret: &StoreSecret = store.txs.store_secret();
    let mut out = Vec::with_capacity(jobs.len());
    for (i, (fk, job)) in fks.iter().zip(jobs.into_iter()).enumerate() {
        if !job.ok {
            continue;
        }
        let Some(range) = job.range else {
            continue;
        };
        note_body_ok_read();
        let mut decoded_full = None;
        let mut decoded_outs = None;
        match mode {
            IdxBodyMode::Full => {
                if let Ok((tx, _empty_ins, outs, rels)) =
                    decode_packed_tx_with_spender_rels_secret(&job.body, Some(secret))
                {
                    let Some(ij) = inwit_jobs.get(i) else {
                        return Err(StoreError::Corrupt(
                            "invariant: Full create missing inwit job",
                        ));
                    };
                    if !ij.ok {
                        return Err(StoreError::Corrupt(
                            "invariant: Full create inwit body missing after load",
                        ));
                    }
                    let ins =
                        rbitcoin_store::decode_inwit_secret(&ij.body, tx.input_count, Some(secret))
                            .map_err(|_| {
                                StoreError::Corrupt("invariant: packed create inwit decode failed")
                            })?;
                    decoded_full = Some((tx, ins, outs, rels));
                } else {
                    return Err(StoreError::Corrupt(
                        "invariant: packed create Full decode failed after body load",
                    ));
                }
            }
            IdxBodyMode::Outs | IdxBodyMode::Prefix33 => {
                if let Ok((tx, outs, rels)) =
                    decode_packed_tx_outs_with_spender_rels_secret(&job.body, Some(secret))
                {
                    // Leave txid zero; caller fills from plan
                    // `external_parent_txids` / batch maps only.
                    decoded_outs = Some((tx, outs, rels));
                } else {
                    return Err(StoreError::Corrupt(
                        "invariant: packed create denserels decode failed after body load",
                    ));
                }
            }
        }
        out.push(CombinedCreate {
            fk: *fk,
            body_range: range,
            raw: job.body,
            decoded_full,
            decoded_outs,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Query;
    use rbitcoin_store::{InputRecord, OutputRecord, TxRecord};
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

    fn temp_query() -> (std::path::PathBuf, Query) {
        static N: AtomicU64 = AtomicU64::new(0);
        let id = N.fetch_add(1, AtomicOrdering::Relaxed);
        let dir = std::env::temp_dir().join(format!("rbitcoin-combined-q-{id}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let q = Query::open_or_create(dir.join("store")).unwrap();
        (dir, q)
    }

    fn put_tx(q: &Query, seed: u8) -> Fk {
        let mut txid = [0u8; 32];
        txid[0] = seed;
        let tx = TxRecord {
            txid,
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 1,
            output_start_fk: Fk::NULL,
            output_count: 2,
        };
        let inputs = vec![InputRecord::coinbase(u32::MAX, vec![0x01, seed], vec![])];
        let outs = vec![
            OutputRecord::unspent(10, vec![0x76, 0xa9, seed]),
            OutputRecord::unspent(20, vec![0x51]),
        ];
        q.store()
            .txs
            .put_full_batch_indexed(&[(tx, inputs, outs)], true)
            .unwrap()[0]
    }

    /// Drive shipped `load_creates_once` as used by `load_confirm_parents`.
    #[test]
    fn load_confirm_parents_uses_combined_body_path() {
        let (dir, q) = temp_query();
        let fks: Vec<Fk> = (0..4u8).map(|i| put_tx(&q, i + 20)).collect();
        reset_body_ok_reads();
        let creates = load_creates_once(q.store(), &fks, IdxBodyMode::Full).unwrap();
        assert_eq!(creates.len(), fks.len());
        assert!(body_ok_reads() >= 1, "combined path must body-fetch");
        // Schema 13: identity lives in txid.body / plan RAM, not body prefix.
        let c = &creates[0];
        let t = c
            .decoded_full
            .as_ref()
            .map(|(tx, _, _, _)| tx.txid)
            .unwrap_or([0u8; 32]);
        // Full decode leaves zero unless filled; sidefile holds identity.
        let tid = if t == [0u8; 32] {
            q.store().txs.body_txid(c.fk).unwrap()
        } else {
            t
        };
        assert_ne!(tid, [0u8; 32], "sidefile must supply identity");
        let _ = std::fs::remove_dir_all(dir);
    }

    /// OutsDenserels parent path returns denserels decode without process pins.
    #[test]
    fn outs_denserels_loads_parent_decode() {
        let (dir, q) = temp_query();
        let fk = put_tx(&q, 7);
        let creates = load_creates_once(q.store(), &[fk], IdxBodyMode::Outs).unwrap();
        assert_eq!(creates.len(), 1);
        assert!(
            creates[0].decoded_outs.is_some(),
            "decode must succeed for pin"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn block_queue_via_query_enqueue_reopen_empty() {
        let (dir, q) = temp_query();
        let payload = b"ibd-block-payload-bytes".to_vec();
        let id = q
            .block_queue_enqueue(42, [0xCDu8; 32], 7, &payload)
            .unwrap();
        assert_eq!(q.block_queue_stats().2, 1);
        let all = q.block_queue_load_all().unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].id, id);
        assert_eq!(all[0].height, 42);
        assert_eq!(all[0].payload, payload);
        // Confirm-write hook: dequeue by height.
        assert_eq!(q.block_queue_dequeue_height(42).unwrap(), 1);
        assert_eq!(q.block_queue_stats().2, 0);
        // Restart: RAM queue is empty (by design — redownload, no double disk write).
        drop(q);
        let q2 = Query::open_or_create(dir.join("store")).unwrap();
        assert_eq!(q2.block_queue_load_all().unwrap().len(), 0);
        assert_eq!(q2.block_queue_stats().2, 0);
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Offer always lands in the process-local RAM queue.
    #[test]
    fn block_queue_offer_always_ram() {
        let (dir, q) = temp_query();
        let p1 = vec![1u8; 64 * 1024];
        let p2 = vec![2u8; 64 * 1024];
        let o1 = q.block_queue_offer(1, [1u8; 32], 1, &p1).unwrap();
        assert!(o1.queue_id > 0);
        assert_eq!(q.block_queue_stats().2, 1);
        let o2 = q.block_queue_offer(2, [2u8; 32], 2, &p2).unwrap();
        assert!(o2.queue_id > 0);
        assert_eq!(q.block_queue_stats().2, 2);
        let n = q.block_queue_dequeue_height(1).unwrap();
        assert_eq!(n, 1);
        assert_eq!(q.block_queue_stats().2, 1);
        let all = q.block_queue_load_all().unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].height, 2);
        assert_eq!(all[0].payload, p2);
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Confirm load intake: payload by height from RAM (no dequeue).
    #[test]
    fn block_queue_payload_peek_ram() {
        let (dir, q) = temp_query();
        let wire = b"ram-payload".to_vec();
        q.block_queue_enqueue(10, [0xAAu8; 32], 1, &wire).unwrap();
        assert_eq!(
            q.block_queue_payload(10).unwrap().as_deref(),
            Some(wire.as_slice())
        );
        assert!(q.block_queue_has_height(10));
        assert_eq!(q.block_queue_stats().2, 1, "peek does not dequeue");
        assert!(q.block_queue_payload(999).unwrap().is_none());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn block_queue_soft_free_bytes_and_confirm_window() {
        use crate::{
            soft_assign_restricted, soft_confirm_window_covered, soft_confirm_window_n,
            soft_densify_band_hi, BQ_SOFT_CONFIRM_SECS, BQ_SOFT_FREE_BYTES,
        };
        // 5 blk/s × 60s → window 300.
        assert_eq!(soft_confirm_window_n(Some(5.0)), 300);
        assert_eq!(soft_confirm_window_n(None), 0);
        assert_eq!(
            soft_confirm_window_n(Some(2.0)),
            (2.0 * BQ_SOFT_CONFIRM_SECS).ceil() as u32
        );

        let free = BQ_SOFT_FREE_BYTES;
        let over = free + 1;
        // Under free: full densify_hi regardless of rate.
        assert_eq!(soft_densify_band_hi(100, 1000, free, Some(0.1)), 1000);
        assert!(!soft_assign_restricted(free));
        // densify_hi < path_lo edge (empty band).
        assert_eq!(soft_densify_band_hi(50, 40, free, Some(1.0)), 40);
        assert_eq!(soft_densify_band_hi(50, 40, over, Some(1.0)), 40);
        // Over free: confirm window only.
        assert_eq!(
            soft_densify_band_hi(100, 1000, over, Some(0.1)),
            105,
            "0.1 blk/s × 60s = 6 heights → path_lo..path_lo+5"
        );
        assert_eq!(
            soft_densify_band_hi(100, 1000, over, None),
            100,
            "rate cold → tip-adjacent only"
        );
        // Window clamp to densify_hi when rate is high.
        assert_eq!(
            soft_densify_band_hi(100, 110, over, Some(5.0)),
            110,
            "window 300 clamped to densify_hi=110"
        );
        assert!(soft_assign_restricted(over));
        assert!(!soft_confirm_window_covered(50, over, Some(5.0))); // 50 < 300
        assert!(soft_confirm_window_covered(300, over, Some(5.0)));
        assert!(soft_confirm_window_covered(1, over, None)); // cold + over free
        assert!(!soft_confirm_window_covered(1, free, None)); // under free never covered

        let (dir, q) = temp_query();
        assert!(!q.block_queue_update_soft_pressure(Some(5.0)));
        // Many tiny payloads — under free floor, unrestricted.
        for i in 0..451u32 {
            q.block_queue_enqueue(
                i,
                {
                    let mut h = [0u8; 32];
                    h[..4].copy_from_slice(&i.to_le_bytes());
                    h
                },
                1,
                b"x",
            )
            .unwrap();
        }
        assert!(
            !q.block_queue_update_soft_pressure(Some(5.0)),
            "early-chain style: many tiny blocks under free-byte floor"
        );
        // Two ~80 MiB payloads → over free floor → restricted.
        for i in 0..451u32 {
            let _ = q.block_queue_dequeue_height(i);
        }
        let chunk = vec![0u8; 80 * 1024 * 1024];
        q.block_queue_enqueue(1, [1u8; 32], 1, &chunk).unwrap();
        q.block_queue_enqueue(2, [2u8; 32], 2, &chunk).unwrap();
        assert!(q.block_queue_stats().1 > BQ_SOFT_FREE_BYTES);
        assert!(
            q.block_queue_update_soft_pressure(None),
            "bytes over free floor → restricted"
        );
        // Drop one chunk → under free floor → unrestricted.
        q.block_queue_dequeue_height(1).unwrap();
        assert!(q.block_queue_stats().1 < BQ_SOFT_FREE_BYTES);
        assert!(
            !q.block_queue_update_soft_pressure(None),
            "bytes under free floor → unrestricted"
        );

        // Soft restriction must never block peer offer / enqueue (request-limited only).
        let chunk2 = vec![0u8; 80 * 1024 * 1024];
        q.block_queue_enqueue(3, [3u8; 32], 3, &chunk2).unwrap();
        q.block_queue_enqueue(4, [4u8; 32], 4, &chunk2).unwrap();
        assert!(
            q.block_queue_update_soft_pressure(None),
            "re-enter restricted for offer regression"
        );
        assert!(q.block_queue_soft_pressure());
        let offered = q
            .block_queue_offer(5, [5u8; 32], 5, b"already-requested-body")
            .expect("offer must succeed while soft densify is restricted");
        assert!(offered.queue_id > 0);
        assert!(q.block_queue_has_height(5));
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Multi-block AC1: archive h0 creates + h1 spends h0; load_confirm_parents
    /// on h0; load of h1 pins parent from batch_bodies same-batch or cold Class A
    /// once (full_tx_reads for external parent).
    #[test]
    fn multi_block_load_confirm_parents_single_parent_body() {
        use crate::TxApply;
        use rbitcoin_store::{HeaderRecord, InputRecord, OutputRecord, TxRecord};

        let (dir, q) = temp_query();
        // h0 coinbase
        let mut h0hash = [0u8; 32];
        h0hash[0] = 0xa0;
        let h0 = HeaderRecord {
            prev_fk: Fk::NULL,
            version: 1,
            timestamp: 1,
            bits: 0x207fffff,
            nonce: 0,
            merkle_root: h0hash,
            hash: h0hash,
        };
        let mut parent_txid = [0u8; 32];
        parent_txid[0] = 0xcb;
        let ta0 = TxApply {
            tx: TxRecord {
                txid: parent_txid,
                version: 1,
                locktime: 0,
                input_start_fk: Fk::NULL,
                input_count: 1,
                output_start_fk: Fk::NULL,
                output_count: 1,
            },
            inputs: vec![InputRecord::coinbase(u32::MAX, vec![0], vec![])],
            outputs: vec![OutputRecord::unspent(50_0000_0000, vec![0x51])],
        };
        let hfk0 = q.archive_block(&h0, &[ta0]).unwrap();

        // h1: coinbase + spend of parent vout 0 (hash commits to h0 via write gate).
        let version = 1;
        let timestamp = 2;
        let bits = 0x207fffff;
        let nonce = 1;
        let mut merkle = [0u8; 32];
        merkle[0] = 0xa1;
        let h1hash =
            rbitcoin_store::block_header_hash(version, &h0.hash, &merkle, timestamp, bits, nonce);
        let h1 = HeaderRecord {
            prev_fk: hfk0,
            version,
            timestamp,
            bits,
            nonce,
            merkle_root: merkle,
            hash: h1hash,
        };
        let mut cb_txid = [0u8; 32];
        cb_txid[0] = 0xcc;
        let cb1 = TxApply {
            tx: TxRecord {
                txid: cb_txid,
                version: 1,
                locktime: 0,
                input_start_fk: Fk::NULL,
                input_count: 1,
                output_start_fk: Fk::NULL,
                output_count: 1,
            },
            inputs: vec![InputRecord::coinbase(u32::MAX, vec![1], vec![])],
            outputs: vec![OutputRecord::unspent(50_0000_0000, vec![0x51])],
        };
        let mut child_txid = [0u8; 32];
        child_txid[0] = 0x5e;
        let child = TxApply {
            tx: TxRecord {
                txid: child_txid,
                version: 1,
                locktime: 0,
                input_start_fk: Fk::NULL,
                input_count: 1,
                output_start_fk: Fk::NULL,
                output_count: 1,
            },
            inputs: vec![InputRecord {
                prev_txid: parent_txid,
                create_fk: Fk::NULL, // archive stamps
                prev_index: 0,
                sequence: u32::MAX,
                script_sig: vec![],
                witness: vec![],
            }],
            outputs: vec![OutputRecord::unspent(49_0000_0000, vec![0x51])],
        };
        q.archive_block(&h1, &[cb1, child]).unwrap();

        reset_body_ok_reads();
        let (st0, parents0, _thin0, bodies0) =
            q.load_confirm_parents(&[(0, h0hash)]).expect("load h0");
        assert!(st0.blocks >= 1, "h0 load blocks={}", st0.blocks);
        assert!(bodies0.len() >= 1, "h0 bodies");
        let body_reads_after_h0 = body_ok_reads();
        assert!(
            body_reads_after_h0 >= 1,
            "h0 must body-read creates, got {body_reads_after_h0}"
        );
        let parent_fk = q
            .store()
            .txs
            .get_fk_by_txid(&parent_txid)
            .unwrap()
            .expect("parent head");
        let _ = parents0;
        let _ = parent_fk;

        // Load h1: child spends parent → pin via same-batch (if multi-height) or cold.
        // Separate load of h1 only: parent is external → one denserels cold load.
        let (st1, parents1, _thin1, bodies1) =
            q.load_confirm_parents(&[(1, h1hash)]).expect("load h1");
        assert!(st1.blocks >= 1);
        assert!(bodies1.len() >= 1);
        // Parent pin from cold denserels (or batch if multi-block load).
        assert!(
            st1.full_tx_reads >= 1 || parents1.has_parent_out(parent_fk, 0),
            "parent must pin: full_tx_reads={} has_parent_out={}",
            st1.full_tx_reads,
            parents1.has_parent_out(parent_fk, 0)
        );
        assert!(
            parents1.has_parent_out(parent_fk, 0),
            "parent out must be in BatchParents"
        );

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn obfuscation_on_disk_via_store_put() {
        let (dir, q) = temp_query();
        let script = vec![0x76, 0xa9, 0x14, 0x11, 0x22, 0x33];
        let mut txid = [0u8; 32];
        txid[0] = 0xee;
        let tx = TxRecord {
            txid,
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 1,
            output_start_fk: Fk::NULL,
            output_count: 1,
        };
        let inputs = vec![InputRecord::coinbase(
            u32::MAX,
            script.clone(),
            vec![vec![9]],
        )];
        let outs = vec![OutputRecord::unspent(1, script.clone())];
        let mut plain = Vec::new();
        rbitcoin_store::encode_packed_tx_with_secret(&tx, &inputs, &outs, &mut plain, None);
        let mut obf = Vec::new();
        rbitcoin_store::encode_packed_tx_with_secret(
            &tx,
            &inputs,
            &outs,
            &mut obf,
            Some(q.store().txs.store_secret()),
        );
        assert_ne!(plain, obf);
        let fk = q
            .store()
            .txs
            .put_full_batch_indexed(&[(tx, inputs, outs)], true)
            .unwrap()[0];
        reset_body_ok_reads();
        let creates = load_creates_once(q.store(), &[fk], IdxBodyMode::Full).unwrap();
        assert_eq!(creates.len(), 1);
        assert!(
            !creates[0]
                .raw
                .windows(script.len())
                .any(|w| w == script.as_slice()),
            "plaintext script must not appear on disk"
        );
        let (_dtx, _ins, douts, _) = decode_packed_tx_with_spender_rels_secret(
            &creates[0].raw,
            Some(q.store().txs.store_secret()),
        )
        .unwrap();
        assert_eq!(douts[0].script, script);
        let _ = std::fs::remove_dir_all(dir);
    }
}
