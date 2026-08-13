//! Thin BIP-352 tweak index: Query join of `sp_tweaks.*` + Class A.

use super::*;
use rbitcoin_store::SpTweaksTable;

/// One tx in a height, aligned with `header_txs`. Packed outs only when a tweak
/// is stored (serve path; no parent scripts).
#[derive(Clone, Debug)]
pub struct ThinTweakRow {
    pub txid: [u8; 32],
    pub tweak: Option<[u8; 33]>,
    pub outputs: Option<Vec<OutputRecord>>,
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
    /// Loads `txid.body` + packed outs only for `len=33` txs. No parent peeks.
    pub fn load_thin_tweaks(
        &self,
        height: Height,
    ) -> Result<Option<Vec<ThinTweakRow>>, QueryError> {
        let header_fk = match self.store.confirmed.get(height)? {
            Some(fk) => fk,
            None => return Ok(None),
        };
        let Some((_, n_tx)) = self.store.header_txs.get_range(header_fk)? else {
            return Ok(None);
        };
        let fks = match self.block_tx_fks(height) {
            Ok(f) => f,
            Err(StoreError::NotFound) => return Ok(None),
            Err(e) => return Err(e),
        };
        let recs = {
            let g = self.sp_tweaks.lock().unwrap_or_else(|e| e.into_inner());
            let Some(t) = g.as_ref() else {
                return Ok(None);
            };
            match t.get_block(height, header_fk, n_tx)? {
                Some(r) => r,
                None => return Ok(None),
            }
        };
        if recs.len() != fks.len() {
            return Err(StoreError::Corrupt(
                "invariant: thin tweak n_tx != header_txs",
            ));
        }
        let mut rows = Vec::with_capacity(fks.len());
        for (fk, tweak) in fks.into_iter().zip(recs.into_iter()) {
            let txid = self.store.txs.body_txid(fk)?;
            let outputs = if tweak.is_some() {
                match self.store.get_tx_meta_and_outputs(fk) {
                    Ok((_, outs)) => Some(outs),
                    Err(StoreError::NotFound) => {
                        return Err(StoreError::Corrupt(
                            "invariant: thin tweak missing packed body",
                        ));
                    }
                    Err(e) => return Err(e),
                }
            } else {
                None
            };
            rows.push(ThinTweakRow {
                txid,
                tweak,
                outputs,
            });
        }
        Ok(Some(rows))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
