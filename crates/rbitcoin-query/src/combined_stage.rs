//! Combined archive-prep + confirm-load stage (single parent-body path).
//!
//! Production confirm load calls [`load_creates_once`] for Class A create decode
//! and pin_new denserels. Creates land in [`CreateResidency`] so archive prep
//! (fk/range) and confirm pin (outs) share one map — not dual sticky+OutFifo thrash.

use crate::create_residency::CreateResidency;
use rbitcoin_primitives::Fk;
use rbitcoin_store::{
    decode_packed_tx_outs_with_spender_rels_secret, decode_packed_tx_with_spender_rels_secret,
    IdxBodyJob, IdxBodyMode, Store, StoreSecret,
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

/// One create decoded for the combined path.
#[derive(Debug, Clone)]
pub struct CombinedCreate {
    pub fk: Fk,
    pub body_range: (u64, u64),
    pub raw: Vec<u8>,
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
        match mode {
            IdxBodyMode::Full => {
                if let Ok((tx, _ins, outs, rels)) =
                    decode_packed_tx_with_spender_rels_secret(&job.body, Some(secret))
                {
                    residency.put_outs(*fk, tx, outs, rels, Some(range));
                } else {
                    let mut txid = [0u8; 32];
                    if job.body.len() >= 32 {
                        txid.copy_from_slice(&job.body[..32]);
                    }
                    residency.insert_fk_txid_range(*fk, txid, Some(range));
                }
            }
            IdxBodyMode::OutsDenserels | IdxBodyMode::Prefix33 => {
                if let Ok((tx, outs, rels)) =
                    decode_packed_tx_outs_with_spender_rels_secret(&job.body, Some(secret))
                {
                    residency.put_outs(*fk, tx, outs, rels, Some(range));
                } else if job.body.len() >= 32 {
                    let mut txid = [0u8; 32];
                    txid.copy_from_slice(&job.body[..32]);
                    residency.insert_fk_txid_range(*fk, txid, Some(range));
                }
            }
        }
        out.push(CombinedCreate {
            fk: *fk,
            body_range: range,
            raw: job.body,
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
        let creates = load_creates_once(
            q.store(),
            q.create_residency(),
            &fks,
            IdxBodyMode::Full,
        )
        .unwrap();
        assert_eq!(creates.len(), fks.len());
        let n = body_ok_reads();
        assert_eq!(n, fks.len() as u64);
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
        assert_eq!(q2.block_queue_dequeue_height(42).unwrap(), 1);
        assert_eq!(q2.block_queue_stats().2, 0);
        drop(q2);
        let q3 = Query::open_or_create(dir.join("store")).unwrap();
        assert_eq!(q3.block_queue_load_all().unwrap().len(), 0);
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
