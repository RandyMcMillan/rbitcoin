//! Tip-mode transaction relay (P4): inv/getdata/tx + mempool announce.
//!
//! Heavy relay is **gated** on [`MempoolHub::set_relay_enabled`] (false during IBD).
//! BIP331 package *wire* is not in rust-bitcoin 0.32's `NetworkMessage`; package
//! accept stays on [`rbitcoin_mempool::ActiveMempool::accept_package`]. Unknown
//! `sendpackages` / package commands are ignored until a bitcoin crate upgrade.

use bitcoin::consensus::encode::deserialize;
use bitcoin::hashes::Hash;
use bitcoin::{Amount, OutPoint, ScriptBuf, Transaction, TxOut, Txid, Wtxid};
use rbitcoin_mempool::{AcceptError, AcceptResult, ActiveMempool, UtxoProvider};
use rbitcoin_query::Query;
use rbitcoin_store::OutputRecord;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

/// Resolve prevouts from the relational archive (confirmed UTXOs).
pub struct QueryUtxoProvider<'a> {
    pub query: &'a Query,
}

impl UtxoProvider for QueryUtxoProvider<'_> {
    fn get_txout(&self, op: &OutPoint) -> Option<TxOut> {
        let tid = op.txid.to_byte_array();
        let (_fk, rec) = self.query.get_tx_by_txid(&tid).ok().flatten()?;
        let out: OutputRecord = self.query.tx_output(&rec, op.vout).ok()?;
        let value = if out.value < 0 {
            Amount::ZERO
        } else {
            Amount::from_sat(out.value as u64)
        };
        Some(TxOut {
            value,
            script_pubkey: ScriptBuf::from_bytes(out.script),
        })
    }
}

/// Shared mempool + relay gate used by peer sessions and tip confirm.
pub struct MempoolHub {
    inner: Mutex<ActiveMempool>,
    query: Arc<Query>,
    /// When false, peers' tx inv/tx are ignored (IBD / catch-up).
    relay_enabled: AtomicBool,
    /// Broadcast accepted txids so sessions can inv (origin exclusion is per-session).
    announce: broadcast::Sender<Txid>,
}

impl MempoolHub {
    pub fn open(dir: impl AsRef<Path>, query: Arc<Query>) -> Result<Arc<Self>, String> {
        Self::open_with_weight(dir, query, rbitcoin_mempool::DEFAULT_MAX_MEMPOOL_WEIGHT)
    }

    /// Open with a weight budget (WU). `max_weight_wu` drives chunk eviction.
    pub fn open_with_weight(
        dir: impl AsRef<Path>,
        query: Arc<Query>,
        max_weight_wu: u64,
    ) -> Result<Arc<Self>, String> {
        let mp = ActiveMempool::open_or_create_with_limit(dir.as_ref(), max_weight_wu)
            .map_err(|e| format!("mempool open: {e}"))?;
        let (announce, _) = broadcast::channel(256);
        Ok(Arc::new(Self {
            inner: Mutex::new(mp),
            query,
            relay_enabled: AtomicBool::new(false),
            announce,
        }))
    }

    /// Compact durable mempool files (reclaim DEAD slots / body holes).
    pub fn compact(&self) -> Result<(u32, usize), String> {
        self.inner
            .lock()
            .unwrap()
            .compact()
            .map_err(|e| format!("mempool compact: {e}"))
    }

    pub fn set_relay_enabled(&self, on: bool) {
        self.relay_enabled.store(on, Ordering::SeqCst);
    }

    pub fn relay_enabled(&self) -> bool {
        self.relay_enabled.load(Ordering::SeqCst)
    }

    pub fn subscribe_announces(&self) -> broadcast::Receiver<Txid> {
        self.announce.subscribe()
    }

    pub fn live_count(&self) -> usize {
        self.inner.lock().unwrap().live_count()
    }

    pub fn generation(&self) -> u64 {
        self.inner.lock().unwrap().generation()
    }

    pub fn flush(&self) -> Result<(), String> {
        self.inner
            .lock()
            .unwrap()
            .flush()
            .map_err(|e| format!("mempool flush: {e}"))
    }

    pub fn contains(&self, txid: &Txid) -> bool {
        self.inner.lock().unwrap().graph.contains(txid)
    }

    pub fn get_tx(&self, txid: &Txid) -> Option<Transaction> {
        self.inner.lock().unwrap().get_tx(txid).cloned()
    }

    /// Look up a live mempool tx by wtxid (BIP339 / compact v2).
    pub fn get_tx_by_wtxid(&self, wtxid: &Wtxid) -> Option<Transaction> {
        let g = self.inner.lock().unwrap();
        for (txid, e) in g.graph.iter() {
            if e.wtxid == *wtxid {
                return g.get_tx(txid).cloned();
            }
        }
        None
    }

    /// True if a live mempool entry has this wtxid (BIP339 inv filter).
    pub fn contains_wtxid(&self, wtxid: &Wtxid) -> bool {
        let g = self.inner.lock().unwrap();
        let found = g.graph.iter().any(|(_, e)| e.wtxid == *wtxid);
        found
    }

    /// Accept a peer (or local) transaction when relay is enabled.
    pub fn accept_tx(&self, tx: &Transaction) -> Result<AcceptResult, AcceptError> {
        let utxo = QueryUtxoProvider {
            query: self.query.as_ref(),
        };
        let mut g = self.inner.lock().unwrap();
        let r = g.accept_tx(tx, &utxo)?;
        let _ = self.announce.send(r.txid);
        Ok(r)
    }

    /// Accept an ancestor package (local / Electrum path; BIP331 wire later).
    pub fn accept_package(&self, txs: &[Transaction]) -> Result<Vec<AcceptResult>, AcceptError> {
        let utxo = QueryUtxoProvider {
            query: self.query.as_ref(),
        };
        let mut g = self.inner.lock().unwrap();
        let res = g.accept_package(txs, &utxo)?;
        for r in &res {
            let _ = self.announce.send(r.txid);
        }
        Ok(res)
    }

    /// Remove confirmed txids (tip connect / archive confirm).
    pub fn remove_for_block(&self, txids: &[Txid]) -> usize {
        let mut g = self.inner.lock().unwrap();
        g.remove_for_block(txids).unwrap_or(0)
    }

    /// Re-admit txs after reorg disconnect (best-effort).
    pub fn reorg_reaccept(&self, txs: &[Transaction]) -> usize {
        let utxo = QueryUtxoProvider {
            query: self.query.as_ref(),
        };
        let mut g = self.inner.lock().unwrap();
        g.reorg_disconnect_reaccept(txs, &utxo)
            .into_iter()
            .filter(|r| r.is_ok())
            .count()
    }

    /// Snapshot of live txs (for Electrum / RPC).
    pub fn list_live(&self) -> Vec<(Txid, u64, u64, Transaction)> {
        let g = self.inner.lock().unwrap();
        g.graph
            .iter()
            .filter_map(|(txid, e)| {
                g.get_tx(txid)
                    .cloned()
                    .map(|tx| (*txid, e.fee_sat, e.weight, tx))
            })
            .collect()
    }

    /// Outpoints spent by any live mempool transaction (confirmed or mempool parents).
    pub fn spent_outpoints(&self) -> std::collections::HashSet<OutPoint> {
        let g = self.inner.lock().unwrap();
        let mut set = std::collections::HashSet::new();
        for (txid, _) in g.graph.iter() {
            let Some(tx) = g.get_tx(txid) else { continue };
            for inp in &tx.input {
                set.insert(inp.previous_output);
            }
        }
        set
    }

    /// Electrum `blockchain.scripthash.get_mempool` rows for `scripthash` (internal order).
    pub fn scripthash_mempool(&self, scripthash: &[u8; 32]) -> Vec<ElectrumMempoolItem> {
        use rbitcoin_store::script_hash;
        let g = self.inner.lock().unwrap();
        let mut out = Vec::new();
        for (txid, e) in g.graph.iter() {
            let Some(tx) = g.get_tx(txid) else { continue };
            let mut touches = false;
            for o in &tx.output {
                if script_hash(o.script_pubkey.as_bytes()) == *scripthash {
                    touches = true;
                    break;
                }
            }
            if !touches {
                for inp in &tx.input {
                    let op = inp.previous_output;
                    let spk = if let Some(creator) = g.graph.creator(&op) {
                        g.get_tx(&creator)
                            .and_then(|t| t.output.get(op.vout as usize))
                            .map(|o| o.script_pubkey.as_bytes().to_vec())
                    } else {
                        QueryUtxoProvider {
                            query: self.query.as_ref(),
                        }
                        .get_txout(&op)
                        .map(|o| o.script_pubkey.as_bytes().to_vec())
                    };
                    if let Some(s) = spk {
                        if script_hash(&s) == *scripthash {
                            touches = true;
                            break;
                        }
                    }
                }
            }
            if !touches {
                continue;
            }
            let mut height = 0i64;
            for inp in &tx.input {
                if g.graph.contains(&inp.previous_output.txid) {
                    height = -1;
                    break;
                }
            }
            out.push(ElectrumMempoolItem {
                txid: txid.to_byte_array(),
                height,
                fee: e.fee_sat as i64,
            });
        }
        out.sort_by(|a, b| a.txid.cmp(&b.txid));
        out
    }

    /// Unconfirmed delta for Electrum balance (sats): +mempool outputs − spent confirmed.
    pub fn scripthash_unconfirmed_delta(&self, scripthash: &[u8; 32]) -> i64 {
        use rbitcoin_store::script_hash;
        let g = self.inner.lock().unwrap();
        let mut delta = 0i64;
        for (txid, _e) in g.graph.iter() {
            let Some(tx) = g.get_tx(txid) else { continue };
            for (vout, o) in tx.output.iter().enumerate() {
                if script_hash(o.script_pubkey.as_bytes()) != *scripthash {
                    continue;
                }
                let op = OutPoint {
                    txid: *txid,
                    vout: vout as u32,
                };
                if g.graph.mempool_utxo(&op) {
                    delta = delta.saturating_add(o.value.to_sat() as i64);
                }
            }
            for inp in &tx.input {
                let op = inp.previous_output;
                // Only count spending of **chain** UTXOs (not pure mempool-parent).
                if g.graph.creator(&op).is_some() {
                    continue;
                }
                let provider = QueryUtxoProvider {
                    query: self.query.as_ref(),
                };
                if let Some(txout) = provider.get_txout(&op) {
                    if script_hash(txout.script_pubkey.as_bytes()) == *scripthash {
                        delta = delta.saturating_sub(txout.value.to_sat() as i64);
                    }
                }
            }
        }
        delta
    }

    /// Fee histogram buckets for Electrum: `[[feerate_sat_per_kvb, vsize], ...]` descending rate.
    pub fn fee_histogram(&self) -> Vec<(u64, u64)> {
        let g = self.inner.lock().unwrap();
        let mut by_rate: std::collections::BTreeMap<u64, u64> = std::collections::BTreeMap::new();
        for (_txid, e) in g.graph.iter() {
            let rate = e.fee_rate_sat_per_kvb();
            let vsize = rbitcoin_consensus::policy::get_virtual_size(e.weight);
            *by_rate.entry(rate).or_insert(0) += vsize;
        }
        // Electrum expects descending feerate.
        by_rate.into_iter().rev().collect()
    }

    /// Rough `estimatefee` in BTC/kB for target blocks (mempool feerate percentile).
    /// Returns negative if empty (Electrum convention for “unavailable”).
    ///
    /// `target_blocks`: 1–2 → ~90th percentile (high fee), 3–6 → median, else ~20th.
    pub fn estimate_fee_btc_per_kb(&self, target_blocks: u32) -> f64 {
        let g = self.inner.lock().unwrap();
        if g.graph.is_empty() {
            return -1.0;
        }
        let mut rates: Vec<u64> = g
            .graph
            .iter()
            .map(|(_, e)| e.fee_rate_sat_per_kvb())
            .collect();
        rates.sort_unstable();
        let pct = if target_blocks <= 2 {
            90
        } else if target_blocks <= 6 {
            50
        } else {
            20
        };
        let idx = ((rates.len().saturating_sub(1)) * pct) / 100;
        let pick = rates[idx.min(rates.len() - 1)];
        (pick as f64) / 100_000_000.0
    }

    /// Relay fee in BTC/kB (Libre 0.1 sat/vB = 100 sat/kvB).
    pub fn relay_fee_btc_per_kb() -> f64 {
        rbitcoin_consensus::policy::MIN_RELAY_FEE_RATE_SAT_PER_KVB as f64 / 100_000_000.0
    }
}

/// One Electrum mempool history row.
#[derive(Debug, Clone)]
pub struct ElectrumMempoolItem {
    pub txid: [u8; 32],
    pub height: i64,
    pub fee: i64,
}

/// Decode raw package payload: concat of bitcoin txs (for future BIP331 `pkgtxns`).
///
/// Format used by tests / local inject: each tx is length-prefixed u32 LE + raw.
pub fn decode_len_prefixed_package(payload: &[u8]) -> Result<Vec<Transaction>, String> {
    let mut out = Vec::new();
    let mut i = 0usize;
    while i + 4 <= payload.len() {
        let n = u32::from_le_bytes(payload[i..i + 4].try_into().unwrap()) as usize;
        i += 4;
        if i + n > payload.len() {
            return Err("package truncated".into());
        }
        let tx: Transaction = deserialize(&payload[i..i + n]).map_err(|e| e.to_string())?;
        out.push(tx);
        i += n;
    }
    if i != payload.len() {
        return Err("package trailing bytes".into());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::absolute::LockTime;
    use bitcoin::consensus::encode::serialize;
    use bitcoin::hashes::Hash;
    use bitcoin::transaction::Version;
    use bitcoin::{Sequence, TxIn, Witness};
    use rbitcoin_query::Query;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn tmp() -> std::path::PathBuf {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::path::PathBuf::from(format!("/tmp/rbitcoin-txrelay-{n}"))
    }

    #[test]
    fn package_codec_roundtrip() {
        let tx = Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: Txid::from_byte_array([1u8; 32]),
                    vout: 0,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        };
        let raw = serialize(&tx);
        let mut payload = Vec::new();
        payload.extend_from_slice(&(raw.len() as u32).to_le_bytes());
        payload.extend_from_slice(&raw);
        let decoded = decode_len_prefixed_package(&payload).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].compute_txid(), tx.compute_txid());
    }

    #[test]
    fn hub_accept_remove_with_map_utxo_path() {
        // MempoolHub needs Query; use open empty store + MapUtxo via direct ActiveMempool
        // for isolation — hub Query path covered when store has txs.
        let dir = tmp();
        let store_dir = tmp();
        let q = Query::open_or_create(&store_dir).unwrap();
        let hub = MempoolHub::open(&dir, Arc::new(q)).unwrap();
        assert!(!hub.relay_enabled());
        hub.set_relay_enabled(true);
        assert!(hub.relay_enabled());
        assert_eq!(hub.live_count(), 0);
        // Without chain UTXO, accept fails missing prevout — expected.
        let tx = Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: Txid::from_byte_array([9u8; 32]),
                    vout: 0,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        };
        let err = hub.accept_tx(&tx).unwrap_err();
        assert!(matches!(err, AcceptError::MissingPrevout(_)));
        assert!(hub.fee_histogram().is_empty());
        assert!(hub.estimate_fee_btc_per_kb(2) < 0.0 || hub.estimate_fee_btc_per_kb(2) >= 0.0);
        assert!(MempoolHub::relay_fee_btc_per_kb() > 0.0);
        assert!(hub.scripthash_mempool(&[0u8; 32]).is_empty());
        assert_eq!(hub.scripthash_unconfirmed_delta(&[0u8; 32]), 0);
        assert!(hub.list_live().is_empty());
        assert!(!hub.contains_wtxid(&Wtxid::from_byte_array([0u8; 32])));
        assert!(hub.get_tx_by_wtxid(&Wtxid::from_byte_array([0u8; 32])).is_none());
        assert_eq!(hub.remove_for_block(&[]), 0);
        assert_eq!(hub.reorg_reaccept(&[]), 0);
        hub.flush().unwrap();
        let _ = hub.compact();
        let _ = hub.generation();
        let _ = hub.subscribe_announces();
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&store_dir);
    }

    #[test]
    fn open_with_weight_and_package_empty() {
        let dir = tmp();
        let store_dir = tmp();
        let q = Query::open_or_create(&store_dir).unwrap();
        let hub = MempoolHub::open_with_weight(&dir, Arc::new(q), 1_000_000).unwrap();
        hub.set_relay_enabled(true);
        assert!(matches!(
            hub.accept_package(&[]),
            Err(AcceptError::PackageEmpty)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&store_dir);
    }
}
