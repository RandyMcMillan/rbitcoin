//! Combined archive-prep + confirm-load stage (single parent-body path).
//!
//! Production confirm load calls [`load_creates_once`] for Class A create decode
//! and pin_new denserels. Creates land in [`CreateResidency`] so archive prep
//! (fk/range) and confirm pin (outs) share one CreateResidency map.

use crate::create_residency::CreateResidency;
use rbitcoin_primitives::Fk;
use rbitcoin_store::{
    decode_packed_tx_outs_with_spender_rels_secret, decode_packed_tx_with_spender_rels_secret,
    IdxBodyJob, IdxBodyMode, Store, StoreError, StoreSecret,
};
use std::sync::atomic::{AtomicU64, Ordering};

/// Test/prod counter of body pread jobs that completed `ok` through the pipeline.
static BODY_OK_READS: AtomicU64 = AtomicU64::new(0);

/// Reset body-read counter (tests).
pub fn reset_body_ok_reads() {
    BODY_OK_READS.store(0, Ordering::Relaxed);
}

/// Snapshot body-read counter.
pub fn body_ok_reads() -> u64 {
    BODY_OK_READS.load(Ordering::Relaxed)
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

/// Load outs+denserels for plan head-miss parents **without** seeding CreateResidency.
///
/// Long-tail external parents thrash residency FIFO; prep consumes the returned
/// map as pipeline-local pin source instead. Ranges come from idx (or residency
/// if a prior range-only seed exists).
pub fn load_creates_outs_pipeline_local(
    store: &Store,
    residency: &CreateResidency,
    fks: &[Fk],
) -> Result<
    std::collections::HashMap<
        u64,
        (
            rbitcoin_store::TxRecord,
            Vec<rbitcoin_store::OutputRecord>,
            Vec<u32>,
        ),
    >,
    StoreError,
> {
    use std::collections::HashMap;
    if fks.is_empty() {
        return Ok(HashMap::new());
    }
    let ranges = residency.body_ranges_by_fk(fks);
    let mut jobs: Vec<IdxBodyJob> = fks
        .iter()
        .zip(ranges.into_iter())
        .map(|(fk, range)| IdxBodyJob::new(fk.get().unwrap_or(0), range))
        .collect();
    store.idx_body_pipeline(&mut jobs, IdxBodyMode::OutsDenserels)?;
    let secret: &StoreSecret = store.txs.store_secret();
    let mut out = HashMap::with_capacity(jobs.len());
    for (fk, job) in fks.iter().zip(jobs.into_iter()) {
        if !job.ok {
            continue;
        }
        BODY_OK_READS.fetch_add(1, Ordering::Relaxed);
        let (tx, outs, rels) =
            decode_packed_tx_outs_with_spender_rels_secret(&job.body, Some(secret))?;
        if let Some(id) = fk.get() {
            out.insert(id, (tx, outs, rels));
        }
    }
    Ok(out)
}

/// Load creates by fk (and optional known ranges from residency), decode once,
/// seed residency. Each successful body fetch increments [`body_ok_reads`].
///
/// **Shipped entry used by** [`crate::Query::load_confirm_parents`].
pub fn load_creates_once(
    store: &Store,
    residency: &CreateResidency,
    fks: &[Fk],
    mode: IdxBodyMode,
) -> Result<Vec<CombinedCreate>, rbitcoin_store::StoreError> {
    if fks.is_empty() {
        return Ok(Vec::new());
    }
    let ranges = residency.body_ranges_by_fk(fks);
    let mut jobs: Vec<IdxBodyJob> = fks
        .iter()
        .zip(ranges.into_iter())
        .map(|(fk, range)| IdxBodyJob::new(fk.get().unwrap_or(0), range))
        .collect();
    store.idx_body_pipeline(&mut jobs, mode)?;
    let secret: &StoreSecret = store.txs.store_secret();
    let mut out = Vec::with_capacity(jobs.len());
    for (fk, job) in fks.iter().zip(jobs.into_iter()) {
        if !job.ok {
            continue;
        }
        let Some(range) = job.range else {
            continue;
        };
        BODY_OK_READS.fetch_add(1, Ordering::Relaxed);
        let mut decoded_full = None;
        let mut decoded_outs = None;
        match mode {
            IdxBodyMode::Full => {
                if let Ok((tx, ins, outs, rels)) =
                    decode_packed_tx_with_spender_rels_secret(&job.body, Some(secret))
                {
                    residency.put_outs(
                        *fk,
                        tx.clone(),
                        outs.clone(),
                        rels.clone(),
                        Some(range),
                    );
                    decoded_full = Some((tx, ins, outs, rels));
                } else {
                    // Body loaded (job.ok) but packed decode failed — prep miss, not
                    // a soft range-only row (that hid corrupt Class A from confirm).
                    return Err(StoreError::Corrupt(
                        "invariant: packed create Full decode failed after body load",
                    ));
                }
            }
            IdxBodyMode::OutsDenserels | IdxBodyMode::Prefix33 => {
                if let Ok((tx, outs, rels)) =
                    decode_packed_tx_outs_with_spender_rels_secret(&job.body, Some(secret))
                {
                    // Clone into residency; keep decoded for pin without re-decode.
                    residency.put_outs(
                        *fk,
                        tx.clone(),
                        outs.clone(),
                        rels.clone(),
                        Some(range),
                    );
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

    /// Serialise env budget knobs across parallel tests.
    static BQ_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

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
        let creates = load_creates_once(
            q.store(),
            q.create_residency(),
            &fks,
            IdxBodyMode::Full,
        )
        .unwrap();
        assert_eq!(creates.len(), fks.len());
        // body_ok_reads is process-global (parallel tests race); require progress only.
        assert!(body_ok_reads() >= 1, "combined path must body-fetch");
        // Residency holds outs for pin without re-IO.
        for fk in &fks {
            assert!(q.create_residency().get_outs(*fk).is_some());
        }
        // Archive commit-style residency insert (fk/range for prep).
        let (txid, fk, off, len) = {
            let c = &creates[0];
            let t = rbitcoin_store::decode_packed_tx_with_spender_rels_secret(
                &c.raw,
                Some(q.store().txs.store_secret()),
            )
            .unwrap()
            .0
            .txid;
            (t, c.fk, c.body_range.0, c.body_range.1)
        };
        q.create_residency()
            .insert_fk_txid_range(fk, txid, Some((off, len)));
        assert_eq!(q.create_residency().lookup_fk_by_txid(&txid), Some(fk));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn block_queue_via_query_enqueue_reopen_dequeue() {
        let (dir, q) = temp_query();
        let payload = b"ibd-block-payload-bytes".to_vec();
        let id = q
            .block_queue_enqueue(42, [0xCDu8; 32], 7, &payload)
            .unwrap();
        assert_eq!(q.block_queue_stats().2, 1);
        // Simulate restart: reopen Query on same store.
        drop(q);
        let q2 = Query::open_or_create(dir.join("store")).unwrap();
        let all = q2.block_queue_load_all().unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].id, id);
        assert_eq!(all[0].height, 42);
        assert_eq!(all[0].payload, payload);
        // Confirm-write hook: dequeue by height.
        assert_eq!(q2.block_queue_dequeue_height(42).unwrap().0, 1);
        assert_eq!(q2.block_queue_stats().2, 0);
        drop(q2);
        let q3 = Query::open_or_create(dir.join("store")).unwrap();
        assert_eq!(q3.block_queue_load_all().unwrap().len(), 0);
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Full durable budget → offer buffers in RAM (no error); dequeue flushes.
    ///
    /// Production budget via `RBITCOIN_BLOCK_QUEUE_BYTES` (min 64 MiB clamp) —
    /// no production `*_for_test` budget backdoor.
    #[test]
    fn block_queue_offer_buffers_when_full_then_flush_on_dequeue() {
        let _env = BQ_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let prev = std::env::var_os("RBITCOIN_BLOCK_QUEUE_BYTES");
        std::env::set_var("RBITCOIN_BLOCK_QUEUE_BYTES", format!("{}", 64 * 1024 * 1024));
        let (dir, q) = temp_query();
        // Two ~40 MiB payloads with 64 MiB budget: first disk, second RAM.
        let p1 = vec![1u8; 40 * 1024 * 1024];
        let p2 = vec![2u8; 40 * 1024 * 1024];
        let o1 = q.block_queue_offer(1, [1u8; 32], 1, &p1).unwrap();
        assert!(o1.disk_id.is_some(), "first fits on disk");
        assert!(o1.flushed_to_disk.is_empty());
        assert_eq!(q.block_queue_stats().2, 1);
        let o2 = q.block_queue_offer(2, [2u8; 32], 2, &p2).unwrap();
        assert!(o2.disk_id.is_none(), "second held in RAM, not error");
        assert_eq!(q.block_queue_pending_len(), 1);
        assert_eq!(q.block_queue_stats().2, 1, "disk count unchanged");
        assert!(!q.block_queue_can_request(), "effective fill ≥ budget");
        let (n, flushed) = q.block_queue_dequeue_height(1).unwrap();
        assert_eq!(n, 1);
        assert_eq!(flushed, vec![[2u8; 32]], "RAM h2 spilled to disk");
        assert_eq!(q.block_queue_pending_len(), 0, "flushed after dequeue");
        assert_eq!(q.block_queue_stats().2, 1, "h2 now on disk");
        let all = q.block_queue_load_all().unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].height, 2);
        assert_eq!(all[0].payload, p2);
        match prev {
            Some(v) => std::env::set_var("RBITCOIN_BLOCK_QUEUE_BYTES", v),
            None => std::env::remove_var("RBITCOIN_BLOCK_QUEUE_BYTES"),
        }
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Confirm prep intake: payload by height from disk or RAM pending (no dequeue).
    #[test]
    fn block_queue_payload_peek_disk_and_pending() {
        let _env = BQ_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let prev = std::env::var_os("RBITCOIN_BLOCK_QUEUE_BYTES");
        std::env::set_var("RBITCOIN_BLOCK_QUEUE_BYTES", format!("{}", 64 * 1024 * 1024));
        let (dir, q) = temp_query();
        let disk = b"disk-payload".to_vec();
        q.block_queue_enqueue(10, [0xAAu8; 32], 1, &disk).unwrap();
        assert_eq!(
            q.block_queue_payload(10).unwrap().as_deref(),
            Some(disk.as_slice())
        );
        assert!(q.block_queue_has_height(10));
        assert_eq!(q.block_queue_stats().2, 1, "peek does not dequeue");

        let fill = vec![9u8; 60 * 1024 * 1024];
        q.block_queue_offer(20, [0xCCu8; 32], 3, &fill).unwrap();
        let ram = vec![7u8; 8 * 1024 * 1024];
        let o = q.block_queue_offer(11, [0xBBu8; 32], 2, &ram).unwrap();
        assert!(o.disk_id.is_none(), "held in RAM pending after budget full");
        assert_eq!(
            q.block_queue_payload(11).unwrap().as_deref(),
            Some(ram.as_slice())
        );
        assert!(q.block_queue_has_height(11));
        assert!(q.block_queue_payload(999).unwrap().is_none());
        match prev {
            Some(v) => std::env::set_var("RBITCOIN_BLOCK_QUEUE_BYTES", v),
            None => std::env::remove_var("RBITCOIN_BLOCK_QUEUE_BYTES"),
        }
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn block_queue_hysteresis_scale() {
        // Enter at 0.90, stay until ≤0.70.
        let (s0, p0) = Query::block_queue_far_scale_from(0.5, false);
        assert!((s0 - 0.5).abs() < 1e-9);
        assert!(!p0);
        let (s1, p1) = Query::block_queue_far_scale_from(0.91, false);
        assert_eq!(s1, 0.0);
        assert!(p1);
        let (s2, p2) = Query::block_queue_far_scale_from(0.80, true);
        assert_eq!(s2, 0.0, "still latched above exit");
        assert!(p2);
        let (s3, p3) = Query::block_queue_far_scale_from(0.70, true);
        assert!(!p3);
        assert!((s3 - 0.30).abs() < 1e-9);
    }

    /// Multi-block AC1: archive h0 creates + h1 spends h0; load_confirm_parents
    /// on h0 seeds residency; load of h1 pins parent **without** a second denserels
    /// body fetch (`full_tx_reads` stays 0 for the parent pin).
    #[test]
    fn multi_block_load_confirm_parents_single_parent_body() {
        use rbitcoin_store::{HeaderRecord, InputRecord, OutputRecord, TxRecord};
        use crate::TxApply;

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

        // h1: coinbase + spend of parent vout 0
        let mut h1hash = [0u8; 32];
        h1hash[0] = 0xa1;
        let h1 = HeaderRecord {
            prev_fk: hfk0,
            version: 1,
            timestamp: 2,
            bits: 0x207fffff,
            nonce: 1,
            merkle_root: h1hash,
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

        // Parent create is on residency after archive commit (range dual-write).
        // Load h0 through **shipped** load_confirm_parents → seeds outs into residency.
        reset_body_ok_reads();
        let (st0, parents0, _thin0, bodies0) = q
            .load_confirm_parents(&[(0, h0hash)])
            .expect("load h0");
        assert!(st0.blocks >= 1, "h0 load blocks={}", st0.blocks);
        assert!(bodies0.len() >= 1, "h0 bodies");
        let body_reads_after_h0 = body_ok_reads();
        assert!(
            body_reads_after_h0 >= 1,
            "h0 must body-read creates, got {body_reads_after_h0}"
        );
        // CreateResidency holds parent outs for pin.
        let parent_fk = q
            .store()
            .txs
            .get_fk_by_txid(&parent_txid)
            .unwrap()
            .expect("parent head");
        assert!(
            q.create_residency().get_outs(parent_fk).is_some(),
            "load_creates_once must seed residency outs for parent create"
        );
        let _ = parents0;

        // Load h1: child spends parent → pin path must NOT denserels-IO parent again.
        let (st1, parents1, _thin1, bodies1) = q
            .load_confirm_parents(&[(1, h1hash)])
            .expect("load h1");
        assert!(st1.blocks >= 1);
        assert!(bodies1.len() >= 1);
        // Parent pin from residency: pin_new denserels path uses full_tx_reads.
        assert_eq!(
            st1.full_tx_reads, 0,
            "parent pin must not re-fetch denserels body (full_tx_reads={})",
            st1.full_tx_reads
        );
        // pin_cache_body covers parent and/or batch_parents has the out.
        assert!(
            st1.pin_cache_body >= 1 || parents1.has_parent_out(parent_fk, 0),
            "parent pin_cache_body={} has_parent_out={}",
            st1.pin_cache_body,
            parents1.has_parent_out(parent_fk, 0)
        );
        // Body reads for h1 are only for h1 creates (coinbase+child), not parent denserels.
        let body_reads_h1 = body_ok_reads().saturating_sub(body_reads_after_h0);
        assert!(
            body_reads_h1 <= 2,
            "h1 should only load its own creates (≤2), got extra body reads={body_reads_h1}"
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
        let inputs = vec![InputRecord::coinbase(u32::MAX, script.clone(), vec![vec![9]])];
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
        let creates = load_creates_once(
            q.store(),
            q.create_residency(),
            &[fk],
            IdxBodyMode::Full,
        )
        .unwrap();
        assert_eq!(creates.len(), 1);
        assert!(
            !creates[0].raw.windows(script.len()).any(|w| w == script.as_slice()),
            "plaintext script must not appear on disk"
        );
        let (_dtx, _ins, douts, _) =
            decode_packed_tx_with_spender_rels_secret(
                &creates[0].raw,
                Some(q.store().txs.store_secret()),
            )
            .unwrap();
        assert_eq!(douts[0].script, script);
        let _ = std::fs::remove_dir_all(dir);
    }
}
