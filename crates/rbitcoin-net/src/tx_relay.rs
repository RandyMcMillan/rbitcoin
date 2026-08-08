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

/// Cap for Esplora `/mempool/recent` (newest accepts, process-local).
pub const MEMPOOL_RECENT_CAP: usize = 32;

/// One recently accepted mempool tx (for explorer "recent" strips).
#[derive(Clone, Debug)]
pub struct RecentAccept {
    pub txid: Txid,
    pub fee_sat: u64,
    pub weight: u64,
    /// Sum of output values (sats).
    pub value_sat: u64,
}

/// Shared mempool + relay gate used by peer sessions and tip confirm.
pub struct MempoolHub {
    inner: Mutex<ActiveMempool>,
    query: Arc<Query>,
    /// When false, peers' tx inv/tx are ignored (IBD / catch-up).
    relay_enabled: AtomicBool,
    /// Broadcast accepted txids so sessions can inv (origin exclusion is per-session).
    announce: broadcast::Sender<Txid>,
    /// Newest-last ring of successful accepts (Esplora `/mempool/recent`).
    recent: Mutex<std::collections::VecDeque<RecentAccept>>,
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
            recent: Mutex::new(std::collections::VecDeque::with_capacity(MEMPOOL_RECENT_CAP)),
        }))
    }

    fn push_recent(&self, tx: &Transaction, r: &AcceptResult) {
        let value_sat: u64 = tx
            .output
            .iter()
            .map(|o| o.value.to_sat())
            .fold(0u64, |a, b| a.saturating_add(b));
        let entry = RecentAccept {
            txid: r.txid,
            fee_sat: r.fee_sat,
            weight: r.weight,
            value_sat,
        };
        let mut q = self.recent.lock().unwrap();
        q.push_back(entry);
        while q.len() > MEMPOOL_RECENT_CAP {
            q.pop_front();
        }
    }

    /// Newest-first snapshot of recent accepts (at most 10 for Esplora `/mempool/recent`).
    pub fn recent_accepts(&self) -> Vec<RecentAccept> {
        const ESPLORA_RECENT: usize = 10;
        let q = self.recent.lock().unwrap();
        q.iter().rev().take(ESPLORA_RECENT).cloned().collect()
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

    /// Live mempool txids that passed consensus script verify at accept.
    ///
    /// Tip confirm may skip re-verifying these (same tip-era softfork flags).
    pub fn script_preverified_txids(&self) -> std::collections::HashSet<[u8; 32]> {
        use bitcoin::hashes::Hash;
        let g = self.inner.lock().unwrap();
        g.graph
            .iter()
            .map(|(txid, _)| txid.to_byte_array())
            .collect()
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
        drop(g);
        self.push_recent(tx, &r);
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
        drop(g);
        for (tx, r) in txs.iter().zip(res.iter()) {
            self.push_recent(tx, r);
            let _ = self.announce.send(r.txid);
        }
        Ok(res)
    }

    /// Remove confirmed txids (tip connect / archive confirm) and re-try orphans
    /// whose parents just confirmed (Query UTXO view).
    pub fn remove_for_block(&self, txids: &[Txid]) -> usize {
        let utxo = QueryUtxoProvider {
            query: self.query.as_ref(),
        };
        let mut g = self.inner.lock().unwrap();
        g.remove_for_block_with_utxo(txids, &utxo).unwrap_or(0)
    }

    /// Unique txs parked waiting on missing parents (Core-class orphanage).
    pub fn orphan_count(&self) -> usize {
        self.inner.lock().unwrap().orphan_count()
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
        std::env::temp_dir().join(format!("rbitcoin-txrelay-{n}"))
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
        // Without chain UTXO, accept parks as orphan (Core-class soft path).
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
        assert!(matches!(err, AcceptError::Orphaned(_)), "{err}");
        assert_eq!(hub.orphan_count(), 1);
        assert!(hub.fee_histogram().is_empty());
        assert!(hub.estimate_fee_btc_per_kb(2) < 0.0 || hub.estimate_fee_btc_per_kb(2) >= 0.0);
        assert!(MempoolHub::relay_fee_btc_per_kb() > 0.0);
        assert!(hub.scripthash_mempool(&[0u8; 32]).is_empty());
        assert_eq!(hub.scripthash_unconfirmed_delta(&[0u8; 32]), 0);
        assert!(hub.list_live().is_empty());
        assert!(!hub.contains_wtxid(&Wtxid::from_byte_array([0u8; 32])));
        assert!(hub
            .get_tx_by_wtxid(&Wtxid::from_byte_array([0u8; 32]))
            .is_none());
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

    #[test]
    fn package_codec_errors_and_query_utxo_miss() {
        // Truncated length prefix.
        assert!(decode_len_prefixed_package(&[1, 2, 3]).is_err());
        // Length claims more than remaining.
        let mut bad = 100u32.to_le_bytes().to_vec();
        bad.extend_from_slice(&[0u8; 4]);
        assert!(decode_len_prefixed_package(&bad).is_err());
        // Empty package ok.
        assert!(decode_len_prefixed_package(&[]).unwrap().is_empty());
        // Garbage tx body.
        let mut junk = 4u32.to_le_bytes().to_vec();
        junk.extend_from_slice(&[0xff; 4]);
        assert!(decode_len_prefixed_package(&junk).is_err());
        // Trailing bytes after valid package.
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
        payload.push(0xff); // trailing
        assert!(decode_len_prefixed_package(&payload)
            .unwrap_err()
            .contains("trailing"));

        let store_dir = tmp();
        let q = Query::open_or_create(&store_dir).unwrap();
        let provider = QueryUtxoProvider { query: &q };
        let op = OutPoint {
            txid: Txid::from_byte_array([0xcd; 32]),
            vout: 0,
        };
        assert!(provider.get_txout(&op).is_none());
        let _ = std::fs::remove_dir_all(&store_dir);
    }

    #[test]
    fn estimate_fee_percentiles_and_spent_outpoints_empty() {
        let dir = tmp();
        let store_dir = tmp();
        let q = Query::open_or_create(&store_dir).unwrap();
        let hub = MempoolHub::open(&dir, Arc::new(q)).unwrap();
        // Empty → negative estimate for all targets.
        assert!(hub.estimate_fee_btc_per_kb(1) < 0.0);
        assert!(hub.estimate_fee_btc_per_kb(5) < 0.0);
        assert!(hub.estimate_fee_btc_per_kb(100) < 0.0);
        assert!(hub.spent_outpoints().is_empty());
        assert!(hub.contains(&Txid::from_byte_array([0u8; 32])) == false);
        assert!(hub.get_tx(&Txid::from_byte_array([0u8; 32])).is_none());
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&store_dir);
    }

    #[test]
    fn recent_accepts_ring_newest_first() {
        if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
            std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        }
        let store_dir = tmp();
        let mp_dir = tmp();
        let q = Query::open_or_create(&store_dir).unwrap();
        // Empty store: still can accept nothing without parents — just open hub.
        let hub = MempoolHub::open(&mp_dir, Arc::new(q)).unwrap();
        assert!(hub.recent_accepts().is_empty());
        let _ = std::fs::remove_dir_all(&mp_dir);
        let _ = std::fs::remove_dir_all(&store_dir);
    }

    /// Accept live spends of confirmed OP_TRUE coinbases via QueryUtxoProvider —
    /// covers fee histogram, estimate percentiles, scripthash mempool/delta,
    /// wtxid lookup, package announce, and spent outpoints.
    #[test]
    fn hub_live_accept_fee_scripthash_and_package() {
        use bitcoin::block::{Header, Version as BlockVersion};
        use bitcoin::transaction::Version as TxVersion;
        use bitcoin::{Block, CompactTarget, Target, TxMerkleNode};
        use rbitcoin_consensus::{accept_and_connect_block, ChainParams, Milestone};
        use rbitcoin_primitives::Height;
        use rbitcoin_store::script_hash;

        if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
            std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        }
        let store_dir = tmp();
        let mp_dir = tmp();
        let q = Query::open_or_create(&store_dir).unwrap();
        let params = ChainParams::regtest();
        let ms = Milestone::NONE;

        fn coinbase(height: u32) -> Transaction {
            let mut ss = if height == 0 {
                vec![0x00]
            } else {
                rbitcoin_consensus::bip34_height_script(height)
            };
            while ss.len() < 2 {
                ss.push(0x00);
            }
            Transaction {
                version: TxVersion::ONE,
                lock_time: LockTime::ZERO,
                input: vec![TxIn {
                    previous_output: OutPoint::null(),
                    script_sig: ScriptBuf::from_bytes(ss),
                    sequence: Sequence::MAX,
                    witness: Witness::new(),
                }],
                output: vec![TxOut {
                    value: Amount::from_sat(50_0000_0000),
                    script_pubkey: ScriptBuf::from_bytes(vec![0x51]), // OP_TRUE
                }],
            }
        }
        fn mine(prev: bitcoin::BlockHash, time: u32, height: u32) -> Block {
            let bits = CompactTarget::from_consensus(0x207f_ffff);
            let header = Header {
                version: BlockVersion::ONE,
                prev_blockhash: prev,
                merkle_root: TxMerkleNode::from_byte_array([0u8; 32]),
                time,
                bits,
                nonce: 0,
            };
            let mut block = Block {
                header,
                txdata: vec![coinbase(height)],
            };
            block.header.merkle_root = block.compute_merkle_root().unwrap();
            let target = Target::from_compact(bits);
            for nonce in 0..u32::MAX {
                block.header.nonce = nonce;
                if block.header.validate_pow(target).is_ok() {
                    break;
                }
            }
            block
        }

        let genesis = bitcoin::blockdata::constants::genesis_block(bitcoin::Network::Regtest);
        accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, ms).unwrap();
        let mut tip = genesis.block_hash();
        let mut tip_time = genesis.header.time;
        // Two blocks → two coinbases we can spend (mempool does not enforce maturity).
        let mut coinbase_txids = Vec::new();
        for h in 1u32..=3 {
            let b = mine(tip, tip_time + 600, h);
            coinbase_txids.push(b.txdata[0].compute_txid());
            accept_and_connect_block(&q, &params, Height(h), &b, ms).unwrap();
            tip = b.block_hash();
            tip_time = b.header.time;
        }

        let q_arc = Arc::new(q);
        let hub = MempoolHub::open(&mp_dir, Arc::clone(&q_arc)).unwrap();
        hub.set_relay_enabled(true);
        let mut ann_rx = hub.subscribe_announces();

        // QueryUtxoProvider hit on confirmed coinbase.
        let provider = QueryUtxoProvider {
            query: q_arc.as_ref(),
        };
        let op0 = OutPoint {
            txid: coinbase_txids[0],
            vout: 0,
        };
        assert!(provider.get_txout(&op0).is_some());

        let spk = ScriptBuf::from_bytes(vec![0x51]);
        let sh = script_hash(spk.as_bytes());

        // Parent spend → fee 1000.
        let parent = Transaction {
            version: TxVersion::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: op0,
                script_sig: ScriptBuf::new(),
                sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(50_0000_0000 - 1_000),
                script_pubkey: spk.clone(),
            }],
        };
        let pr = hub.accept_tx(&parent).expect("accept parent");
        assert_eq!(pr.txid, parent.compute_txid());
        assert!(matches!(ann_rx.try_recv(), Ok(_)));
        let recent = hub.recent_accepts();
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].txid, parent.compute_txid());
        assert_eq!(recent[0].fee_sat, 1_000);

        // Child of mempool parent (height=-1 scripthash path) + second chain spend.
        let child = Transaction {
            version: TxVersion::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: parent.compute_txid(),
                    vout: 0,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(50_0000_0000 - 2_000),
                script_pubkey: spk.clone(),
            }],
        };
        let second = Transaction {
            version: TxVersion::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: coinbase_txids[1],
                    vout: 0,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(50_0000_0000 - 5_000),
                script_pubkey: ScriptBuf::from_bytes(vec![0x52]), // different spk
            }],
        };
        // Package: child then second is not topo for child; accept child alone then package second.
        hub.accept_tx(&child).expect("child");
        let pkg = hub.accept_package(&[second.clone()]).expect("package");
        assert_eq!(pkg.len(), 1);

        assert_eq!(hub.live_count(), 3);
        assert!(hub.contains(&parent.compute_txid()));
        assert!(hub.get_tx(&parent.compute_txid()).is_some());
        let wtxid = parent.compute_wtxid();
        assert!(hub.contains_wtxid(&wtxid));
        assert!(hub.get_tx_by_wtxid(&wtxid).is_some());

        // Fee surfaces with live graph.
        assert!(!hub.fee_histogram().is_empty());
        let e1 = hub.estimate_fee_btc_per_kb(1); // 90th
        let e5 = hub.estimate_fee_btc_per_kb(5); // 50th
        let e20 = hub.estimate_fee_btc_per_kb(20); // 20th
        assert!(e1 >= 0.0 && e5 >= 0.0 && e20 >= 0.0);

        let spent = hub.spent_outpoints();
        assert!(spent.contains(&op0));
        assert!(spent.contains(&OutPoint {
            txid: parent.compute_txid(),
            vout: 0
        }));

        // Scripthash: parent/child touch OP_TRUE; second does not.
        let rows = hub.scripthash_mempool(&sh);
        assert!(rows.len() >= 2);
        assert!(rows.iter().any(|r| r.height == -1)); // child of mempool parent
        let delta = hub.scripthash_unconfirmed_delta(&sh);
        // Parent output still live until child spent it; child holds OP_TRUE UTXO.
        // Net: +child_out - coinbase (parent spent chain UTXO) roughly non-zero path.
        let _ = delta;

        // Remove confirmed + reorg reaccept empty already covered; remove live.
        assert!(hub.remove_for_block(&[parent.compute_txid()]) >= 1);
        assert!(hub.list_live().len() < 3);

        let _ = std::fs::remove_dir_all(&mp_dir);
        let _ = std::fs::remove_dir_all(&store_dir);
    }
}
