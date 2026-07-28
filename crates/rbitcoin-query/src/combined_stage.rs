//! Combined archive-prep + confirm-load stage (single parent-body path).
//!
//! One read stage owns: Class A create decode, parent pin denserels, and
//! residency updates. Archive stamp and confirm pin both consume the same
//! [`CreateResidency`] / decoded bodies — no second idx→body wave for the same
//! create in one batch.

use crate::create_residency::CreateResidency;
use rbitcoin_primitives::Fk;
use rbitcoin_store::{
    decode_packed_tx_outs_with_spender_rels_secret, decode_packed_tx_with_spender_rels_secret,
    IdxBodyJob, IdxBodyMode, Store, StoreSecret,
};
use std::collections::HashMap;
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

/// Load creates by fk (and optional known ranges), decode once, seed residency.
///
/// Returns decoded creates. Each successful body fetch increments
/// [`body_ok_reads`] exactly once.
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
        // Seed residency with range (and outs when denserels mode).
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

/// Second consumer of the same creates (confirm pin) hits residency — **zero**
/// additional body IO when ranges/outs are present.
pub fn pin_from_residency(
    residency: &CreateResidency,
    need: &[(Fk, Vec<u32>)],
) -> HashMap<u64, Vec<u32>> {
    let mut hits = HashMap::new();
    for (fk, vouts) in need {
        let Some(id) = fk.get() else {
            continue;
        };
        if residency.get_outs(*fk).is_some() {
            hits.insert(id, vouts.clone());
        }
    }
    hits
}

#[cfg(test)]
mod tests {
    use super::*;
    use rbitcoin_store::{
        encode_packed_tx_with_secret, InputRecord, OutputRecord, Store, TxRecord,
    };
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

    fn temp_store() -> (std::path::PathBuf, Store) {
        static N: AtomicU64 = AtomicU64::new(0);
        let id = N.fetch_add(1, AtomicOrdering::Relaxed);
        let dir = std::env::temp_dir().join(format!("rbitcoin-combined-{id}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let s = Store::create(&dir).unwrap();
        (dir, s)
    }

    fn put_tx(s: &Store, seed: u8) -> Fk {
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
        // Use table put (secret XOR on disk).
        s.txs
            .put_full_batch_indexed(&[(tx, inputs, outs)], true)
            .unwrap()[0]
    }

    #[test]
    fn single_body_fetch_serves_archive_and_confirm() {
        let (dir, s) = temp_store();
        let fks: Vec<Fk> = (1..9u8).map(|i| put_tx(&s, i)).collect();
        let res = CreateResidency::new(1000, 10_000);
        reset_body_ok_reads();
        let creates = load_creates_once(&s, &res, &fks, IdxBodyMode::Full).unwrap();
        assert_eq!(creates.len(), fks.len());
        let reads_after_load = body_ok_reads();
        assert_eq!(reads_after_load, fks.len() as u64);

        // Confirm pin consumers hit residency — no additional body IO.
        let need: Vec<(Fk, Vec<u32>)> = fks.iter().map(|f| (*f, vec![0u32, 1])).collect();
        let hits = pin_from_residency(&res, &need);
        assert_eq!(hits.len(), fks.len());
        assert_eq!(
            body_ok_reads(),
            reads_after_load,
            "pin must not re-fetch parent bodies"
        );

        // Second load of same fks: ranges in residency skip idx; body still read
        // once more if we call load again — but residency-only pin stays zero IO.
        let hits2 = pin_from_residency(&res, &need);
        assert_eq!(hits2.len(), fks.len());
        assert_eq!(body_ok_reads(), reads_after_load);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn obfuscation_on_disk_differs_from_plaintext() {
        let (dir, s) = temp_store();
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
        let inputs = vec![InputRecord::coinbase(u32::MAX, script.clone(), vec![vec![9, 9]])];
        let outs = vec![OutputRecord::unspent(1, script.clone())];
        let mut plain = Vec::new();
        encode_packed_tx_with_secret(&tx, &inputs, &outs, &mut plain, None);
        let mut obf = Vec::new();
        encode_packed_tx_with_secret(&tx, &inputs, &outs, &mut obf, Some(s.txs.store_secret()));
        assert_ne!(plain, obf, "obfuscated body must differ from plaintext");
        // Round-trip via store put/get.
        let fk = s
            .txs
            .put_full_batch_indexed(&[(tx.clone(), inputs, outs)], true)
            .unwrap()[0];
        let res = CreateResidency::new(10, 100);
        let creates = load_creates_once(&s, &res, &[fk], IdxBodyMode::Full).unwrap();
        assert_eq!(creates.len(), 1);
        let (dtx, _ins, douts, _) =
            decode_packed_tx_with_spender_rels_secret(&creates[0].raw, Some(s.txs.store_secret()))
                .unwrap();
        assert_eq!(dtx.txid, txid);
        assert_eq!(douts[0].script, script);
        // Raw on-disk (without de-xor) must not equal plaintext script.
        assert!(
            !creates[0].raw.windows(script.len()).any(|w| w == script),
            "plaintext script must not appear on disk payload"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
