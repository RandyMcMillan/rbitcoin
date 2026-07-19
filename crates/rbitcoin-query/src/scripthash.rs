//! Electrum scripthash history / balance / UTXO.
//!
//! Index rows are thin create pointers. Spendedness and heights are joined from
//! Class A, points (`has_confirmed_strong_spender`), and Class C (`tx_height`).

use super::*;

impl Query {
    /// Join Class A + tx_height onto a thin create row.
    fn enrich_scripthash_create(
        &self,
        mut rec: ScriptHashRecord,
    ) -> Result<ScriptHashRecord, QueryError> {
        let create = self.store.get_tx(rec.create_tx_fk)?;
        rec.txid = create.txid;
        if rec.vout < create.output_count {
            if let Ok(out) = self.tx_output(&create, rec.vout) {
                rec.value = out.value;
            }
        }
        if let Some(h) = self.store.tx_height.get(rec.create_tx_fk)? {
            rec.create_height = h;
        }
        Ok(rec)
    }

    /// Confirmed Electrum-style history for a scripthash: (height, txid) pairs.
    pub fn scripthash_history(
        &self,
        scripthash: &[u8; 32],
    ) -> Result<Vec<ScriptHashHistoryItem>, QueryError> {
        let entries = self.store.scripthash.entries(scripthash)?;
        let mut by_txid: BTreeMap<[u8; 32], i64> = BTreeMap::new();
        for (_fk, thin) in entries {
            if !self.store.is_confirmed_strong(thin.create_tx_fk)? {
                continue;
            }
            let rec = self.enrich_scripthash_create(thin)?;
            by_txid
                .entry(rec.txid)
                .and_modify(|h| *h = (*h).min(i64::from(rec.create_height)))
                .or_insert(i64::from(rec.create_height));

            // Spend tx via durable points (confirmed-strong only).
            if self.spend_index_enabled() {
                let spenders = self.store.spenders(&rec.txid, rec.vout)?;
                for p in spenders {
                    if !self.store.is_confirmed_strong(p.spending_tx_fk)? {
                        continue;
                    }
                    let spend_tx = self.store.get_tx(p.spending_tx_fk)?;
                    let spend_h = self
                        .store
                        .tx_height
                        .get(p.spending_tx_fk)?
                        .unwrap_or(0);
                    by_txid
                        .entry(spend_tx.txid)
                        .and_modify(|h| *h = (*h).min(i64::from(spend_h)))
                        .or_insert(i64::from(spend_h));
                }
            }
        }
        let mut items: Vec<ScriptHashHistoryItem> = by_txid
            .into_iter()
            .map(|(txid, height)| ScriptHashHistoryItem { height, txid })
            .collect();
        items.sort_by_key(|i| i.height);
        Ok(items)
    }

    /// Confirmed balance for a scripthash.
    pub fn scripthash_balance(&self, scripthash: &[u8; 32]) -> Result<ScriptHashBalance, QueryError> {
        let mut confirmed = 0i64;
        for (_fk, thin) in self.store.scripthash.entries(scripthash)? {
            if !self.store.is_confirmed_strong(thin.create_tx_fk)? {
                continue;
            }
            let rec = self.enrich_scripthash_create(thin)?;
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
        for (_fk, thin) in self.store.scripthash.entries(scripthash)? {
            if !self.store.is_confirmed_strong(thin.create_tx_fk)? {
                continue;
            }
            let rec = self.enrich_scripthash_create(thin)?;
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
}
