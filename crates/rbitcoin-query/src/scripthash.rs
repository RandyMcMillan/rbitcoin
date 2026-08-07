//! Electrum scripthash history / balance / UTXO.
//!
//! Index rows are create_tx_fk only ([`ScriptHashRecord`]). Expand to
//! [`ScriptHashOutpoint`] by loading Class A outputs and matching SHA256(spk).
//! Spentness/heights from spends + Class C.

use super::*;
use rbitcoin_store::script_hash;

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

impl Query {
    /// Expand one create_tx_fk into outpoints for each output matching `scripthash`.
    fn expand_create_to_outpoints(
        &self,
        scripthash: &[u8; 32],
        create_tx_fk: rbitcoin_primitives::Fk,
    ) -> Result<Vec<ScriptHashOutpoint>, QueryError> {
        let create = self.store.get_tx(create_tx_fk)?;
        let outs = if create.output_count == 0 {
            Vec::new()
        } else {
            self.store.get_tx_meta_and_outputs(create_tx_fk)?.1
        };
        let height = self.store.tx_height.get(create_tx_fk)?.unwrap_or(0);
        let mut out = Vec::new();
        for (vout, o) in outs.into_iter().enumerate() {
            if script_hash(&o.script) != *scripthash {
                continue;
            }
            out.push(ScriptHashOutpoint {
                scripthash: *scripthash,
                create_tx_fk,
                vout: vout as u32,
                txid: create.txid,
                value: o.value,
                create_height: height,
            });
        }
        Ok(out)
    }

    /// All confirmed-strong create outpoints for a scripthash (expanded).
    fn scripthash_create_outpoints(
        &self,
        scripthash: &[u8; 32],
    ) -> Result<Vec<ScriptHashOutpoint>, QueryError> {
        let entries = self.store.scripthash.entries(scripthash)?;
        let mut out = Vec::new();
        for (_fk, thin) in entries {
            if !self.store.is_confirmed_strong(thin.create_tx_fk)? {
                continue;
            }
            out.extend(self.expand_create_to_outpoints(scripthash, thin.create_tx_fk)?);
        }
        Ok(out)
    }

    /// Confirmed Electrum-style history for a scripthash: (height, txid) pairs.
    pub fn scripthash_history(
        &self,
        scripthash: &[u8; 32],
    ) -> Result<Vec<ScriptHashHistoryItem>, QueryError> {
        let creates = self.scripthash_create_outpoints(scripthash)?;
        let mut by_txid: BTreeMap<[u8; 32], i64> = BTreeMap::new();
        for rec in creates {
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
                    let spend_h = self.store.tx_height.get(p.spending_tx_fk)?.unwrap_or(0);
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
}
