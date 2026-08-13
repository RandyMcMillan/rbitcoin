//! Thin BIP-352 tweak index: Query join of `sp_tweaks.*` + Class A.

use super::*;
use rbitcoin_store::{IdxBodyJob, IdxBodyMode, SpTweaksTable};

/// Eligible tx after thin-index join (P2TR outs only).
#[derive(Clone, Debug)]
pub struct ThinTweakRow {
    pub txid: [u8; 32],
    pub tweak: [u8; 33],
    pub p2tr: Vec<(u32, [u8; 32], u64)>,
}

impl Query {
    pub fn sptweaks_enabled(&self) -> bool {
        self.sptweaks_enabled.load(AtomicOrdering::Acquire)
    }

    /// Enable persist + serve-from-index + backfill. Creates empty files if needed.
    ///
    /// Does **not** gate Electrum: naive walk remains when off / hole.
    pub fn set_sptweaks_enabled(&self, on: bool, origin: Height) -> Result<(), QueryError> {
        self.sptweaks_origin
            .store(origin.0, AtomicOrdering::Release);
        if on {
            self.ensure_sp_tweaks(origin)?;
        }
        self.sptweaks_enabled.store(on, AtomicOrdering::Release);
        Ok(())
    }

    fn ensure_sp_tweaks(&self, origin: Height) -> Result<(), QueryError> {
        let mut g = self.sp_tweaks.lock().unwrap_or_else(|e| e.into_inner());
        if g.is_none() {
            *g = Some(SpTweaksTable::open_or_create(self.store.path(), origin)?);
            self.sptweaks_origin
                .store(origin.0, AtomicOrdering::Release);
        }
        Ok(())
    }

    pub fn sptweaks_origin(&self) -> Height {
        Height(self.sptweaks_origin.load(AtomicOrdering::Acquire))
    }

    pub fn sptweaks_next_height(&self) -> Option<Height> {
        let g = self.sp_tweaks.lock().unwrap_or_else(|e| e.into_inner());
        g.as_ref().map(|t| t.next_height())
    }

    /// Write one height of aligned per-tx tweaks (`None` = ineligible).
    ///
    /// No-op when the flag is off, the table is missing, or `height` is not next.
    pub fn put_sp_tweaks_block(
        &self,
        height: Height,
        header_fk: Fk,
        records: &[Option<[u8; 33]>],
    ) -> Result<(), QueryError> {
        if !self.sptweaks_enabled() {
            return Ok(());
        }
        let g = self.sp_tweaks.lock().unwrap_or_else(|e| e.into_inner());
        let Some(t) = g.as_ref() else {
            return Ok(());
        };
        if height != t.next_height() {
            return Ok(());
        }
        t.put_block(height, header_fk, records)
    }

    pub fn truncate_sp_tweaks_through_tip(&self, tip: Option<Height>) -> Result<(), QueryError> {
        let g = self.sp_tweaks.lock().unwrap_or_else(|e| e.into_inner());
        let Some(t) = g.as_ref() else {
            return Ok(());
        };
        t.truncate_through_tip(tip)
    }

    /// Indexed height, or `None` if hole / no table / missing header.
    ///
    /// Returns **only eligible** txs (`len=33`). Join is by `header_txs` order
    /// (no txid search). Eligible create fks go through the confirm
    /// [`IdxBodyMode::Full`] idx→body machine (page-coalesced idx, one body SQE
    /// per eligible row). Txids are page-grouped from `txid.body`. Ineligible
    /// packed neighbors are not read. No parent peeks.
    pub fn load_thin_tweaks(
        &self,
        height: Height,
    ) -> Result<Option<Vec<ThinTweakRow>>, QueryError> {
        let header_fk = match self.store.confirmed.get(height)? {
            Some(fk) => fk,
            None => return Ok(None),
        };
        let Some((first_fk, n_tx)) = self.store.header_txs.get_range(header_fk)? else {
            return Ok(None);
        };
        let elig = {
            let g = self.sp_tweaks.lock().unwrap_or_else(|e| e.into_inner());
            let Some(t) = g.as_ref() else {
                return Ok(None);
            };
            match t.get_eligible(height, header_fk, n_tx)? {
                Some(r) => r,
                None => return Ok(None),
            }
        };
        if elig.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let first_id = first_fk.get().ok_or(StoreError::InvalidFk)?;
        let elig_fks: Vec<Fk> = elig
            .iter()
            .map(|&(i, _)| Fk(first_id.saturating_add(u64::from(i))))
            .collect();
        let txids = self.store.txs.txid_sidefile().get_many(&elig_fks)?;
        let mut jobs: Vec<IdxBodyJob> = elig_fks
            .iter()
            .map(|fk| IdxBodyJob::new(fk.get().unwrap_or(0), None))
            .collect();
        self.store.idx_body_pipeline(&mut jobs, IdxBodyMode::Full)?;
        let mut body_bytes = 0u64;
        let mut rows = Vec::with_capacity(elig.len());
        for (i, job) in jobs.iter().enumerate() {
            if !job.ok {
                return Err(StoreError::Corrupt(
                    "invariant: thin tweak eligible body missing",
                ));
            }
            body_bytes = body_bytes.saturating_add(job.body.len() as u64);
            let Some(txid) = txids.get(i).copied().flatten() else {
                return Err(StoreError::Corrupt(
                    "invariant: thin tweak eligible txid missing",
                ));
            };
            let p2tr = self.store.txs.packed_p2tr_from_raw(&job.body)?;
            rows.push(ThinTweakRow {
                txid,
                tweak: elig[i].1,
                p2tr,
            });
        }
        self.note_thin_tweak_body_bytes(body_bytes);
        Ok(Some(rows))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rbitcoin_store::{HeaderRecord, InputRecord, OutputRecord, TxRecord};

    fn tmp_q() -> (std::path::PathBuf, Query) {
        let path = std::env::temp_dir().join(format!(
            "rbitcoin-q-sptweaks-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&path).unwrap();
        if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
            std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        }
        let q = Query::open_or_create(&path).unwrap();
        (path, q)
    }

    #[test]
    fn put_and_load_noop_when_disabled() {
        let (dir, q) = tmp_q();
        assert!(!q.sptweaks_enabled());
        assert!(q.sptweaks_next_height().is_none());
        q.put_sp_tweaks_block(Height(0), Fk(1), &[None]).unwrap();
        q.truncate_sp_tweaks_through_tip(None).unwrap();
        assert!(q.load_thin_tweaks(Height(0)).unwrap().is_none());
        q.set_sptweaks_enabled(true, Height(0)).unwrap();
        assert!(q.sptweaks_enabled());
        assert_eq!(q.sptweaks_origin(), Height(0));
        assert_eq!(q.sptweaks_next_height(), Some(Height(0)));
        // No confirmed header → hole.
        assert!(q.load_thin_tweaks(Height(0)).unwrap().is_none());
        // Not next height is a no-op.
        q.put_sp_tweaks_block(Height(3), Fk(1), &[None]).unwrap();
        assert_eq!(q.sptweaks_next_height(), Some(Height(0)));
        q.set_sptweaks_enabled(true, Height(0)).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn header(h: u32, prev_fk: Fk, prev_hash: Option<[u8; 32]>) -> HeaderRecord {
        let mut merkle = [0u8; 32];
        merkle[0..4].copy_from_slice(&h.to_le_bytes());
        merkle[5] = 0xec;
        let hash = match prev_hash {
            None => merkle,
            Some(ph) => rbitcoin_store::block_header_hash(1, &ph, &merkle, h + 1, 0x207f_ffff, h),
        };
        HeaderRecord {
            prev_fk,
            version: 1,
            timestamp: h + 1,
            bits: 0x207f_ffff,
            nonce: h,
            merkle_root: merkle,
            hash,
        }
    }

    fn p2wpkh_p2tr() -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        use bitcoin::hashes::hash160;
        use bitcoin::hashes::Hash;
        use bitcoin::secp256k1::{Secp256k1, SecretKey};
        let secp = Secp256k1::new();
        let sk = SecretKey::from_slice(&[2u8; 32]).unwrap();
        let pk = bitcoin::secp256k1::PublicKey::from_secret_key(&secp, &sk);
        let ser = pk.serialize();
        let h160 = hash160::Hash::hash(&ser);
        let mut p2wpkh = vec![0x00, 0x14];
        p2wpkh.extend_from_slice(h160.as_ref());
        let (xonly, _) = pk.x_only_public_key();
        let mut p2tr = vec![0x51, 0x20];
        p2tr.extend_from_slice(&xonly.serialize());
        (p2wpkh, p2tr, ser.to_vec())
    }

    /// Fat ineligible packed row between two eligible txs must not be pulled
    /// into the thin-serve body read (span would include it).
    #[test]
    fn load_thin_skips_fat_ineligible_between_eligible() {
        let (dir, q) = tmp_q();
        q.set_sptweaks_enabled(true, Height(0)).unwrap();
        let (p2wpkh, p2tr, ser) = p2wpkh_p2tr();
        let mut genesis_txid = [0u8; 32];
        genesis_txid[31] = 0xcb;
        let h0 = header(0, Fk::NULL, None);
        let fk0 = q
            .connect_block(
                Height(0),
                &h0,
                &[TxApply {
                    tx: TxRecord {
                        txid: genesis_txid,
                        version: 1,
                        locktime: 0,
                        input_start_fk: Fk::NULL,
                        input_count: 1,
                        output_start_fk: Fk::NULL,
                        output_count: 3,
                    },
                    inputs: vec![InputRecord::coinbase(u32::MAX, vec![0x00], vec![])],
                    outputs: vec![
                        OutputRecord::unspent(20_0000_0000, p2wpkh.clone()),
                        OutputRecord::unspent(20_0000_0000, p2wpkh.clone()),
                        OutputRecord::unspent(10_0000_0000, vec![0x51]),
                    ],
                }],
            )
            .unwrap();
        q.put_sp_tweaks_block(Height(0), fk0, &[None]).unwrap();
        let create_fk = q.block_tx_fks(Height(0)).unwrap()[0];
        let mut tid_a = [0u8; 32];
        tid_a[0] = 0xaa;
        let mut tid_fat = [0u8; 32];
        tid_fat[0] = 0xfe;
        let mut tid_b = [0u8; 32];
        tid_b[0] = 0xbb;
        let spend =
            |prev: u32, txid: [u8; 32], outs: Vec<OutputRecord>, wit: Vec<Vec<u8>>| TxApply {
                tx: TxRecord {
                    txid,
                    version: 2,
                    locktime: 0,
                    input_start_fk: Fk::NULL,
                    input_count: 1,
                    output_start_fk: Fk::NULL,
                    output_count: outs.len() as u32,
                },
                inputs: vec![InputRecord {
                    prev_txid: genesis_txid,
                    create_fk,
                    prev_index: prev,
                    sequence: u32::MAX,
                    script_sig: vec![],
                    witness: wit,
                }],
                outputs: outs,
            };
        let h1 = header(1, fk0, Some(h0.hash));
        let header_fk = q
            .connect_block(
                Height(1),
                &h1,
                &[
                    spend(
                        0,
                        tid_a,
                        vec![OutputRecord::unspent(19_0000_0000, p2tr.clone())],
                        vec![vec![0u8; 64], ser.clone()],
                    ),
                    spend(
                        2,
                        tid_fat,
                        vec![OutputRecord::unspent(9_0000_0000, vec![0x51])],
                        vec![vec![0u8; 16_384]],
                    ),
                    spend(
                        1,
                        tid_b,
                        vec![OutputRecord::unspent(19_0000_0000, p2tr)],
                        vec![vec![0u8; 64], ser],
                    ),
                ],
            )
            .unwrap();
        let mut tw_a = [0x02; 33];
        tw_a[0] = 0x02;
        let mut tw_b = [0x03; 33];
        tw_b[0] = 0x03;
        q.put_sp_tweaks_block(Height(1), header_fk, &[Some(tw_a), None, Some(tw_b)])
            .unwrap();

        let fks = q.block_tx_fks(Height(1)).unwrap();
        let elig_a = q.store().tx_inwit_range(fks[0]).unwrap();
        let fat = q.store().tx_inwit_range(fks[1]).unwrap();
        let elig_b = q.store().tx_inwit_range(fks[2]).unwrap();
        assert!(
            fat.1 > 8_000,
            "fat ineligible inwit row too small: {}",
            fat.1
        );
        let elig_sum = elig_a.1.saturating_add(elig_b.1);
        assert!(
            fat.1 > elig_sum.saturating_mul(2),
            "need span/elig >> 2.5 (fat={} elig={})",
            fat.1,
            elig_sum
        );

        let _ = q.sample_reset_thin_tweak_body_bytes();
        let rows = q.load_thin_tweaks(Height(1)).unwrap().expect("indexed");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].txid, tid_a);
        assert_eq!(rows[1].txid, tid_b);
        assert_eq!(rows[0].tweak, tw_a);
        assert_eq!(rows[1].tweak, tw_b);
        assert_eq!(rows[0].p2tr.len(), 1);
        assert_eq!(rows[1].p2tr.len(), 1);
        let read = q.sample_reset_thin_tweak_body_bytes();
        assert!(
            read <= elig_sum.saturating_add(64),
            "thin serve must not read fat ineligible body (read={read} elig={elig_sum} fat={})",
            fat.1
        );
        assert!(
            read < fat.1,
            "read {read} must be smaller than the skipped fat row {}",
            fat.1
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
