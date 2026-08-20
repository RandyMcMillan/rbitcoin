//! Electrum scripthash history / balance / UTXO.
//!
//! Index rows are create_tx_fk only ([`ScriptHashRecord`]). Expand to
//! [`ScriptHashOutpoint`] by loading Class A outputs and matching SHA256(spk).
//! Spentness/heights from spends + Class C.

use super::*;
use rbitcoin_store::{script_hash, IdxBodyMode};

/// Class A expand / spend-join wave. Bounds decoded `txout` pages in RAM.
const SH_JOIN_WAVE: usize = 4096;

fn sh_join_waves<T>(items: &[T], wave: usize) -> impl Iterator<Item = &[T]> {
    items.chunks(wave.max(1))
}

/// Expanded Electrum create outpoint (Class A + height joins).
///
/// Store index only holds [`ScriptHashRecord`] (scripthash + create_tx_fk).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScriptHashOutpoint {
    pub scripthash: [u8; 32],
    pub create_tx_fk: rbitcoin_primitives::Fk,
    pub vout: u32,
    pub txid: [u8; 32],
    pub value: i64,
    pub create_height: u32,
}

/// Electrum `blockchain.scripthash.get_history` row (confirmed only in v1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScriptHashHistoryItem {
    pub height: i64,
    pub txid: [u8; 32],
}

/// Sort order for [`apply_history_filter`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum HistoryOrder {
    /// Electrum: ascending height.
    #[default]
    HeightAsc,
    /// Esplora chain pages: newest first (height desc, then txid desc for stability).
    NewestFirst,
}

/// Immutable filter for history lists (Electrum windows + Esplora paging).
///
/// Height window: confirmed rows with `height ∈ [from_height, to_height)` when
/// `to_height` is `Some`. `to_height: None` means no upper bound. Callers that
/// need BCH `to_height=-1` (include mempool) handle mempool separately and pass
/// `to_height: None` for the confirmed slice.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryFilter {
    /// Inclusive lower bound on `item.height` (default 0).
    pub from_height: u32,
    /// Exclusive upper bound on `item.height`, or open.
    pub to_height: Option<i64>,
    /// Max items after window / cursor (Esplora uses 25).
    pub limit: Option<usize>,
    /// Esplora `last_seen_txid`: after sort, drop through this txid (inclusive), keep following.
    pub after_txid: Option<[u8; 32]>,
    pub order: HistoryOrder,
}

impl Default for HistoryFilter {
    fn default() -> Self {
        Self {
            from_height: 0,
            to_height: None,
            limit: None,
            after_txid: None,
            order: HistoryOrder::HeightAsc,
        }
    }
}

impl HistoryFilter {
    pub fn open() -> Self {
        Self::default()
    }

    /// Electrum Cash-style window (`to_height` exclusive; `None` = open).
    pub fn height_window(from_height: u32, to_height: Option<i64>) -> Self {
        Self {
            from_height,
            to_height,
            limit: None,
            after_txid: None,
            order: HistoryOrder::HeightAsc,
        }
    }

    /// Esplora confirmed chain page (newest first, 25, optional cursor).
    pub fn esplora_chain_page(after_txid: Option<[u8; 32]>) -> Self {
        Self {
            from_height: 0,
            to_height: None,
            limit: Some(25),
            after_txid,
            order: HistoryOrder::NewestFirst,
        }
    }
}

/// Apply [`HistoryFilter`] to an already-built history list (no store I/O).
///
/// Does not re-sort input beyond the filter's [`HistoryOrder`]. Window is applied
/// first, then order, then `after_txid`, then `limit`.
pub fn apply_history_filter(
    items: &[ScriptHashHistoryItem],
    filter: &HistoryFilter,
) -> Vec<ScriptHashHistoryItem> {
    let from = i64::from(filter.from_height);
    let mut out: Vec<ScriptHashHistoryItem> = items
        .iter()
        .filter(|i| {
            if i.height < from {
                return false;
            }
            if let Some(to) = filter.to_height {
                if i.height >= to {
                    return false;
                }
            }
            true
        })
        .cloned()
        .collect();

    match filter.order {
        HistoryOrder::HeightAsc => {
            out.sort_by(|a, b| a.height.cmp(&b.height).then_with(|| a.txid.cmp(&b.txid)));
        }
        HistoryOrder::NewestFirst => {
            out.sort_by(|a, b| b.height.cmp(&a.height).then_with(|| b.txid.cmp(&a.txid)));
        }
    }

    if let Some(after) = filter.after_txid {
        if let Some(pos) = out.iter().position(|i| i.txid == after) {
            out = out.split_off(pos.saturating_add(1));
        }
        // If after_txid not found, Esplora-like behavior: return from start (no skip).
    }

    if let Some(lim) = filter.limit {
        if out.len() > lim {
            out.truncate(lim);
        }
    }
    out
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScriptHashBalance {
    pub confirmed: i64,
    pub unconfirmed: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScriptHashUtxo {
    pub tx_hash: [u8; 32],
    pub tx_pos: u32,
    pub height: u32,
    pub value: i64,
}

/// One confirmed unspent from [`Query::scan_unspent_scripts`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanUtxo {
    pub txid: [u8; 32],
    pub vout: u32,
    pub height: u32,
    pub value: u64,
    pub script: Vec<u8>,
    pub coinbase: bool,
}

/// Esplora-style confirmed chain stats for a scripthash.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct ScriptHashChainStats {
    pub tx_count: u32,
    pub funded_txo_count: u32,
    pub funded_txo_sum: i64,
    pub spent_txo_count: u32,
    pub spent_txo_sum: i64,
}

impl Query {
    /// Expand confirmed-strong create fks in one idx→body wave (`load_creates_once`).
    fn expand_create_fks_wave(
        &self,
        scripthash: &[u8; 32],
        fks: &[Fk],
    ) -> Result<Vec<ScriptHashOutpoint>, QueryError> {
        if fks.is_empty() {
            return Ok(Vec::new());
        }
        let loaded = super::load_creates_once(&self.store, fks, IdxBodyMode::Outs)?;
        if loaded.len() != fks.len() {
            return Err(StoreError::Corrupt(
                "invariant: SH create body missing after load",
            ));
        }
        let txids = self.store.txids_get_many(fks)?;
        let heights = self.store.tx_height_get_batch(fks)?;
        if txids.len() != fks.len() || heights.len() != fks.len() {
            return Err(StoreError::Corrupt(
                "invariant: SH create identity/height batch length",
            ));
        }
        let mut out = Vec::new();
        for (i, create) in loaded.iter().enumerate() {
            if create.fk != fks[i] {
                return Err(StoreError::Corrupt(
                    "invariant: SH create load order mismatch",
                ));
            }
            let Some((_tx, outs, _rels)) = &create.decoded_outs else {
                return Err(StoreError::Corrupt(
                    "invariant: SH create missing decoded outs",
                ));
            };
            let Some(txid) = txids[i] else {
                return Err(StoreError::Corrupt(
                    "invariant: SH create missing txid.body",
                ));
            };
            let height = heights[i].unwrap_or(0);
            for (vout, o) in outs.iter().enumerate() {
                if script_hash(&o.script) != *scripthash {
                    continue;
                }
                out.push(ScriptHashOutpoint {
                    scripthash: *scripthash,
                    create_tx_fk: create.fk,
                    vout: vout as u32,
                    txid,
                    value: o.value,
                    create_height: height,
                });
            }
        }
        Ok(out)
    }

    /// All confirmed-strong create outpoints for a scripthash (expanded).
    fn scripthash_create_outpoints(
        &self,
        scripthash: &[u8; 32],
    ) -> Result<Vec<ScriptHashOutpoint>, QueryError> {
        let entries = self.store.scripthash.entries(scripthash)?;
        let mut fks = Vec::new();
        for (_fk, thin) in entries {
            if self.store.is_confirmed_strong(thin.create_tx_fk)? {
                fks.push(thin.create_tx_fk);
            }
        }
        let mut out = Vec::new();
        for wave in sh_join_waves(&fks, SH_JOIN_WAVE) {
            out.extend(self.expand_create_fks_wave(scripthash, wave)?);
        }
        Ok(out)
    }

    /// Confirmed Electrum-style history for a scripthash: (height, txid) pairs.
    ///
    /// Equivalent to [`Self::scripthash_history_filtered`] with [`HistoryFilter::open`].
    pub fn scripthash_history(
        &self,
        scripthash: &[u8; 32],
    ) -> Result<Vec<ScriptHashHistoryItem>, QueryError> {
        self.scripthash_history_filtered(scripthash, &HistoryFilter::open())
    }

    /// Confirmed history for a scripthash, filtered by height window / limit / cursor.
    ///
    /// Assembles the full confirmed history (creates + confirmed spenders), then
    /// applies [`apply_history_filter`]. When `filter.to_height` is set, create
    /// outpoints with `create_height >= to_height` are skipped during expand
    /// (spends of those creates are also ≥ create height, so they cannot fall
    /// inside the window).
    pub fn scripthash_history_filtered(
        &self,
        scripthash: &[u8; 32],
        filter: &HistoryFilter,
    ) -> Result<Vec<ScriptHashHistoryItem>, QueryError> {
        let creates = self.scripthash_create_outpoints(scripthash)?;
        let mut by_txid: BTreeMap<[u8; 32], i64> = BTreeMap::new();
        let to_excl = filter.to_height;
        for rec in creates {
            if let Some(to) = to_excl {
                if i64::from(rec.create_height) >= to {
                    continue;
                }
            }
            by_txid
                .entry(rec.txid)
                .and_modify(|h| *h = (*h).min(i64::from(rec.create_height)))
                .or_insert(i64::from(rec.create_height));

            if self.spend_index_enabled() {
                let spenders = self.store.spenders(&rec.txid, rec.vout)?;
                for p in spenders {
                    if !self.store.is_confirmed_strong(p.spending_tx_fk)? {
                        continue;
                    }
                    let spend_tx = self.store.get_tx(p.spending_tx_fk)?;
                    let spend_h = self.store.tx_height_get(p.spending_tx_fk)?.unwrap_or(0);
                    if let Some(to) = to_excl {
                        if i64::from(spend_h) >= to {
                            continue;
                        }
                    }
                    by_txid
                        .entry(spend_tx.txid)
                        .and_modify(|h| *h = (*h).min(i64::from(spend_h)))
                        .or_insert(i64::from(spend_h));
                }
            }
        }
        let items: Vec<ScriptHashHistoryItem> = by_txid
            .into_iter()
            .map(|(txid, height)| ScriptHashHistoryItem { height, txid })
            .collect();
        Ok(apply_history_filter(&items, filter))
    }

    /// Confirmed balance for a scripthash.
    pub fn scripthash_balance(
        &self,
        scripthash: &[u8; 32],
    ) -> Result<ScriptHashBalance, QueryError> {
        let mut confirmed = 0i64;
        for rec in self.scripthash_create_outpoints(scripthash)? {
            let spent = if self.spend_index_enabled() {
                self.store
                    .has_confirmed_strong_spender(&rec.txid, rec.vout)?
            } else {
                self.is_outpoint_spent(&rec.txid, rec.vout)?
            };
            if !spent {
                confirmed = confirmed.saturating_add(rec.value);
            }
        }
        Ok(ScriptHashBalance {
            confirmed,
            unconfirmed: 0,
        })
    }

    /// Confirmed UTXOs for a scripthash.
    pub fn scripthash_listunspent(
        &self,
        scripthash: &[u8; 32],
    ) -> Result<Vec<ScriptHashUtxo>, QueryError> {
        let mut out = Vec::new();
        for rec in self.scripthash_create_outpoints(scripthash)? {
            let spent = if self.spend_index_enabled() {
                self.store
                    .has_confirmed_strong_spender(&rec.txid, rec.vout)?
            } else {
                self.is_outpoint_spent(&rec.txid, rec.vout)?
            };
            if !spent {
                out.push(ScriptHashUtxo {
                    tx_hash: rec.txid,
                    tx_pos: rec.vout,
                    height: rec.create_height,
                    value: rec.value,
                });
            }
        }
        out.sort_by(|a, b| a.height.cmp(&b.height).then(a.tx_pos.cmp(&b.tx_pos)));
        Ok(out)
    }

    /// Confirmed unspents whose `scriptPubKey` is in `scripts`.
    ///
    /// With `--shindex`, this is [`Self::scripthash_listunspent`]. Without, it
    /// walks Class A `txout` + spentness per confirmed height — never
    /// [`Self::reconstruct_block_at_height`].
    pub fn scan_unspent_scripts(&self, scripts: &[Vec<u8>]) -> Result<Vec<ScanUtxo>, QueryError> {
        if self.sh_index_enabled() {
            self.scan_unspent_via_shindex(scripts)
        } else {
            self.scan_unspent_via_txout(scripts)
        }
    }

    fn scan_unspent_via_shindex(&self, scripts: &[Vec<u8>]) -> Result<Vec<ScanUtxo>, QueryError> {
        let mut out = Vec::new();
        for spk in scripts {
            let sh = script_hash(spk);
            for u in self.scripthash_listunspent(&sh)? {
                let coinbase = match self.get_tx_by_txid(&u.tx_hash)? {
                    Some((fk, _)) => {
                        let (_, ins, _) = self.store.get_tx_full(fk)?;
                        ins.first().is_some_and(|i| i.is_coinbase())
                    }
                    None => false,
                };
                if u.value < 0 {
                    continue;
                }
                out.push(ScanUtxo {
                    txid: u.tx_hash,
                    vout: u.tx_pos,
                    height: u.height,
                    value: u.value as u64,
                    script: spk.clone(),
                    coinbase,
                });
            }
        }
        Ok(out)
    }

    fn scan_unspent_via_txout(&self, scripts: &[Vec<u8>]) -> Result<Vec<ScanUtxo>, QueryError> {
        let Some(tip) = self.tip_height() else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        for h in 0..=tip.0 {
            let fks = self.block_tx_fks(Height(h))?;
            for (ti, fk) in fks.into_iter().enumerate() {
                let (tx, _ins, outs) = self.store.get_tx_full(fk)?;
                let coinbase = ti == 0;
                for (vout, o) in outs.iter().enumerate() {
                    if !scripts.iter().any(|s| s.as_slice() == o.script.as_slice()) {
                        continue;
                    }
                    if self.is_outpoint_spent(&tx.txid, vout as u32)? {
                        continue;
                    }
                    if o.value < 0 {
                        continue;
                    }
                    out.push(ScanUtxo {
                        txid: tx.txid,
                        vout: vout as u32,
                        height: h,
                        value: o.value as u64,
                        script: o.script.clone(),
                        coinbase,
                    });
                }
            }
        }
        Ok(out)
    }

    /// Confirmed chain_stats for Esplora address/scripthash routes.
    ///
    /// Single expand of creates (avoids a second full history walk).
    pub fn scripthash_chain_stats(
        &self,
        scripthash: &[u8; 32],
    ) -> Result<ScriptHashChainStats, QueryError> {
        use std::collections::HashSet;
        let creates = self.scripthash_create_outpoints(scripthash)?;
        let mut funded_n = 0u32;
        let mut funded_sum = 0i64;
        let mut spent_n = 0u32;
        let mut spent_sum = 0i64;
        let mut txids: HashSet<[u8; 32]> = HashSet::new();
        for rec in &creates {
            funded_n = funded_n.saturating_add(1);
            funded_sum = funded_sum.saturating_add(rec.value);
            txids.insert(rec.txid);
            let spent = if self.spend_index_enabled() {
                self.store
                    .has_confirmed_strong_spender(&rec.txid, rec.vout)?
            } else {
                self.is_outpoint_spent(&rec.txid, rec.vout)?
            };
            if spent {
                spent_n = spent_n.saturating_add(1);
                spent_sum = spent_sum.saturating_add(rec.value);
                if self.spend_index_enabled() {
                    for p in self.store.spenders(&rec.txid, rec.vout)? {
                        if self.store.is_confirmed_strong(p.spending_tx_fk)? {
                            let spend_tx = self.store.get_tx(p.spending_tx_fk)?;
                            txids.insert(spend_tx.txid);
                        }
                    }
                }
            }
        }
        Ok(ScriptHashChainStats {
            tx_count: txids.len() as u32,
            funded_txo_count: funded_n,
            funded_txo_sum: funded_sum,
            spent_txo_count: spent_n,
            spent_txo_sum: spent_sum,
        })
    }
}

#[cfg(test)]
mod history_filter_tests {
    use super::*;

    #[test]
    fn sh_join_waves_splits_on_wave() {
        let v = [1u8, 2, 3, 4, 5];
        let got: Vec<&[u8]> = sh_join_waves(&v, 2).collect();
        assert_eq!(got, vec![&[1, 2][..], &[3, 4][..], &[5][..]]);
        assert!(sh_join_waves(&v, 0).next().is_some());
    }

    fn item(height: i64, txid0: u8) -> ScriptHashHistoryItem {
        let mut txid = [0u8; 32];
        txid[0] = txid0;
        ScriptHashHistoryItem { height, txid }
    }

    #[test]
    fn open_filter_keeps_all_height_asc() {
        let items = vec![item(10, 1), item(5, 2), item(20, 3)];
        let got = apply_history_filter(&items, &HistoryFilter::open());
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].height, 5);
        assert_eq!(got[1].height, 10);
        assert_eq!(got[2].height, 20);
    }

    #[test]
    fn height_window_inclusive_from_exclusive_to() {
        let items = vec![item(1, 1), item(5, 2), item(10, 3), item(15, 4)];
        let f = HistoryFilter::height_window(5, Some(15));
        let got = apply_history_filter(&items, &f);
        assert_eq!(
            got.iter().map(|i| i.height).collect::<Vec<_>>(),
            vec![5, 10]
        );
    }

    #[test]
    fn height_window_open_upper() {
        let items = vec![item(1, 1), item(100, 2)];
        let f = HistoryFilter::height_window(50, None);
        let got = apply_history_filter(&items, &f);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].height, 100);
    }

    #[test]
    fn newest_first_order() {
        let items = vec![item(1, 1), item(3, 2), item(2, 3)];
        let mut f = HistoryFilter::open();
        f.order = HistoryOrder::NewestFirst;
        let got = apply_history_filter(&items, &f);
        assert_eq!(
            got.iter().map(|i| i.height).collect::<Vec<_>>(),
            vec![3, 2, 1]
        );
    }

    #[test]
    fn after_txid_skips_through_cursor_then_limit() {
        // Newest first: h=30,20,10 with distinct txids
        let items = vec![item(10, 1), item(20, 2), item(30, 3)];
        let mut after = [0u8; 32];
        after[0] = 3; // tip (height 30)
        let f = HistoryFilter {
            from_height: 0,
            to_height: None,
            limit: Some(1),
            after_txid: Some(after),
            order: HistoryOrder::NewestFirst,
        };
        let got = apply_history_filter(&items, &f);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].height, 20);
        assert_eq!(got[0].txid[0], 2);
    }

    #[test]
    fn after_txid_unknown_does_not_skip() {
        let items = vec![item(10, 1), item(20, 2)];
        let f = HistoryFilter {
            from_height: 0,
            to_height: None,
            limit: Some(1),
            after_txid: Some([0xff; 32]),
            order: HistoryOrder::NewestFirst,
        };
        let got = apply_history_filter(&items, &f);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].height, 20);
    }

    #[test]
    fn esplora_chain_page_defaults() {
        let f = HistoryFilter::esplora_chain_page(None);
        assert_eq!(f.limit, Some(25));
        assert_eq!(f.order, HistoryOrder::NewestFirst);
        assert!(f.after_txid.is_none());
    }

    #[test]
    fn limit_truncates_after_window() {
        let items: Vec<_> = (1..=10).map(|h| item(h, h as u8)).collect();
        let f = HistoryFilter {
            from_height: 0,
            to_height: None,
            limit: Some(3),
            after_txid: None,
            order: HistoryOrder::HeightAsc,
        };
        let got = apply_history_filter(&items, &f);
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].height, 1);
        assert_eq!(got[2].height, 3);
    }
}
