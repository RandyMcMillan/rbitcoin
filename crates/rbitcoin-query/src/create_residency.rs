//! Unified create residency — **sole hot create map** for wire IBD pin / plan.
//!
//! # Map shape
//!
//! `create_fk → (txid, body range, optional outs + denserels)`.
//!
//! # Eviction: raw FIFO only (no read-LRU)
//!
//! Oldest inserts drop first when create-count or out-count budget is exceeded.
//! Lookups **never** reorder the FIFO (spend of one out does not predict another
//! out of the same create). Do not reintroduce touch-on-hit.
//!
//! # Denserels hit rate is largely structural
//!
//! Mid/late mainnet IBD often sees **~35–50%** denserels pin hits. Many spends
//! reference **old UTXOs** that left any process cache long ago — that miss rate
//! is expected, not a residency bug. Do **not** grow multi‑GiB caps hoping for
//! 65–70% hit rate. Optimize: (1) keep **recent** commit-seed / offline denserels
//! working, (2) make **cold** denserels loads cheap (batch once, no double ensure).
//!
//! Legacy **archive sticky** (fk/range mirror) and **OutFifo** are not the wire
//! pin path; see plan / IBD sizes logging (residency primary).

use rbitcoin_primitives::Fk;
use rbitcoin_store::{OutputRecord, TxRecord};
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

/// Default max creates in residency (~same planning band as prior sticky).
pub const DEFAULT_CREATE_CAP: usize = 8_000_000;
/// Default max outputs held (same as prior OutFifo default).
pub const DEFAULT_OUT_CAP: u64 = 1 << 24;

#[derive(Debug, Clone)]
pub struct ResidentCreate {
    pub txid: [u8; 32],
    pub body_range: Option<(u64, u64)>,
    pub tx: Option<TxRecord>,
    pub outs: Option<Vec<OutputRecord>>,
    pub denserels: Option<Vec<u32>>,
}

struct Inner {
    by_fk: HashMap<u64, ResidentCreate>,
    by_txid: HashMap<[u8; 32], u64>,
    order: VecDeque<u64>,
    create_cap: usize,
    out_cap: u64,
    total_outs: u64,
}

/// Process-local unified residency (sole writer for inserts; shared Mutex).
pub struct CreateResidency {
    inner: Mutex<Inner>,
}

impl CreateResidency {
    pub fn new(create_cap: usize, out_cap: u64) -> Self {
        let create_cap = create_cap.max(1).min(20_000_000);
        let out_cap = out_cap.max(1);
        let init = create_cap.min(1 << 20);
        Self {
            inner: Mutex::new(Inner {
                by_fk: HashMap::with_capacity(init),
                by_txid: HashMap::with_capacity(init),
                order: VecDeque::with_capacity(init),
                create_cap,
                out_cap,
                total_outs: 0,
            }),
        }
    }

    pub fn from_env() -> Self {
        let create_cap = std::env::var("RBITCOIN_CREATE_RESIDENCY_CAP")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_CREATE_CAP);
        let out_cap = std::env::var("RBITCOIN_CONFIRM_OUT_FIFO")
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|&n| n > 0)
            .unwrap_or(DEFAULT_OUT_CAP);
        Self::new(create_cap, out_cap)
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().by_fk.len()
    }

    pub fn total_outs(&self) -> u64 {
        self.inner.lock().unwrap().total_outs
    }

    /// `(creates, create_cap, total_outs, out_cap)` under one lock (IBD sizes).
    pub fn size_stats(&self) -> (usize, usize, u64, u64) {
        let g = self.inner.lock().unwrap();
        (g.by_fk.len(), g.create_cap, g.total_outs, g.out_cap)
    }

    /// Insert / update fk→txid (+ optional range). Raw FIFO: new creates only at back.
    pub fn insert_fk_txid_range(
        &self,
        fk: Fk,
        txid: [u8; 32],
        body_range: Option<(u64, u64)>,
    ) {
        let Some(id) = fk.get() else {
            return;
        };
        let mut g = self.inner.lock().unwrap();
        if let Some(e) = g.by_fk.get_mut(&id) {
            e.txid = txid;
            if body_range.is_some() {
                e.body_range = body_range;
            }
            g.by_txid.insert(txid, id);
            return;
        }
        let create_cap = g.create_cap;
        g.evict_until_creates(create_cap.saturating_sub(1));
        g.by_fk.insert(
            id,
            ResidentCreate {
                txid,
                body_range,
                tx: None,
                outs: None,
                denserels: None,
            },
        );
        g.by_txid.insert(txid, id);
        g.order.push_back(id);
    }

    /// Attach outs (pin denserels) — in-place; may evict by out budget.
    pub fn put_outs(
        &self,
        fk: Fk,
        tx: TxRecord,
        outs: Vec<OutputRecord>,
        denserels: Vec<u32>,
        body_range: Option<(u64, u64)>,
    ) {
        let Some(id) = fk.get() else {
            return;
        };
        let n = outs.len() as u64;
        let mut g = self.inner.lock().unwrap();
        if let Some(e) = g.by_fk.get_mut(&id) {
            let old_n = e.outs.as_ref().map(|o| o.len() as u64).unwrap_or(0);
            e.tx = Some(tx);
            e.outs = Some(outs);
            e.denserels = Some(denserels);
            if body_range.is_some() {
                e.body_range = body_range;
            }
            let new_total = g.total_outs.saturating_sub(old_n).saturating_add(n);
            g.total_outs = new_total;
            let cap = g.out_cap;
            g.evict_until_outs(cap);
            return;
        }
        let create_cap = g.create_cap;
        let out_cap = g.out_cap;
        g.evict_until_creates(create_cap.saturating_sub(1));
        g.evict_until_outs(out_cap.saturating_sub(n));
        let txid = tx.txid;
        g.by_fk.insert(
            id,
            ResidentCreate {
                txid,
                body_range,
                tx: Some(tx),
                outs: Some(outs),
                denserels: Some(denserels),
            },
        );
        g.by_txid.insert(txid, id);
        g.order.push_back(id);
        g.total_outs = g.total_outs.saturating_add(n);
    }

    pub fn body_ranges_by_fk(&self, fks: &[Fk]) -> Vec<Option<(u64, u64)>> {
        let g = self.inner.lock().unwrap();
        fks.iter()
            .map(|fk| {
                let id = fk.get()?;
                g.by_fk.get(&id).and_then(|e| e.body_range)
            })
            .collect()
    }

    pub fn lookup_fk_by_txid(&self, txid: &[u8; 32]) -> Option<Fk> {
        let g = self.inner.lock().unwrap();
        g.by_txid.get(txid).map(|&id| Fk(id))
    }

    pub fn get_outs(&self, fk: Fk) -> Option<(TxRecord, Vec<OutputRecord>, Vec<u32>, Option<(u64, u64)>)> {
        let id = fk.get()?;
        let g = self.inner.lock().unwrap();
        let e = g.by_fk.get(&id)?;
        let tx = e.tx.clone()?;
        let outs = e.outs.clone()?;
        let rels = e.denserels.clone().unwrap_or_default();
        Some((tx, outs, rels, e.body_range))
    }

    /// Sparse pin hit: clone only `need_vouts` scripts + denserel slots (not full outs).
    ///
    /// Returns `None` when row missing, denserels incomplete, or a need vout is OOB.
    /// Holds the map lock only while copying the sparse projection.
    pub fn get_parent_needed(
        &self,
        fk: Fk,
        need_vouts: &[u32],
    ) -> Option<(
        TxRecord,
        Vec<(u32, OutputRecord)>,
        Vec<(u32, u32)>,
        Option<(u64, u64)>,
    )> {
        use crate::batch_parents::{layout_covers_need, sparse_spender_rels, SPENDER_REL_UNKNOWN};

        let id = fk.get()?;
        let g = self.inner.lock().unwrap();
        let e = g.by_fk.get(&id)?;
        let tx = e.tx.as_ref()?;
        let outs = e.outs.as_ref()?;
        let denserels = e.denserels.as_ref()?;
        if denserels.is_empty() && !need_vouts.is_empty() {
            return None;
        }
        let mut need: Vec<u32> = if need_vouts.is_empty() {
            (0..outs.len() as u32).collect()
        } else {
            need_vouts.to_vec()
        };
        need.sort_unstable();
        need.dedup();
        let sparse = sparse_spender_rels(denserels, &need);
        if !layout_covers_need(e.body_range, &sparse, &need) {
            return None;
        }
        let mut live = Vec::with_capacity(need.len());
        for &v in &need {
            let o = outs.get(v as usize)?;
            // denserels slot already validated by layout_covers_need
            let _ = denserels.get(v as usize).filter(|&&r| r != SPENDER_REL_UNKNOWN)?;
            live.push((v, o.clone()));
        }
        Some((tx.clone(), live, sparse, e.body_range))
    }
}

impl Inner {
    fn evict_until_creates(&mut self, max_creates: usize) {
        while self.by_fk.len() > max_creates {
            if !self.evict_one() {
                break;
            }
        }
    }

    fn evict_until_outs(&mut self, max_outs: u64) {
        while self.total_outs > max_outs {
            if !self.evict_one() {
                break;
            }
        }
    }

    fn evict_one(&mut self) -> bool {
        while let Some(id) = self.order.pop_front() {
            if let Some(e) = self.by_fk.remove(&id) {
                self.by_txid.remove(&e.txid);
                let n = e.outs.as_ref().map(|o| o.len() as u64).unwrap_or(0);
                self.total_outs = self.total_outs.saturating_sub(n);
                return true;
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tx(id: u8) -> TxRecord {
        let mut txid = [0u8; 32];
        txid[0] = id;
        TxRecord {
            txid,
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 1,
            output_start_fk: Fk::NULL,
            output_count: 1,
        }
    }

    #[test]
    fn fifo_evicts_oldest_create() {
        let r = CreateResidency::new(2, 1000);
        r.insert_fk_txid_range(Fk(1), [1u8; 32], Some((10, 20)));
        r.insert_fk_txid_range(Fk(2), [2u8; 32], Some((30, 40)));
        r.insert_fk_txid_range(Fk(3), [3u8; 32], Some((50, 60)));
        assert_eq!(r.len(), 2);
        assert!(r.lookup_fk_by_txid(&[1u8; 32]).is_none());
        assert_eq!(r.lookup_fk_by_txid(&[3u8; 32]), Some(Fk(3)));
    }

    #[test]
    fn body_range_and_outs_roundtrip() {
        let r = CreateResidency::new(100, 1000);
        let t = tx(9);
        r.put_outs(
            Fk(9),
            t.clone(),
            vec![OutputRecord::unspent(1, vec![0x51])],
            vec![8],
            Some((100, 50)),
        );
        assert_eq!(r.body_ranges_by_fk(&[Fk(9)]), vec![Some((100, 50))]);
        let (tx2, outs, rels, range) = r.get_outs(Fk(9)).unwrap();
        assert_eq!(tx2.txid, t.txid);
        assert_eq!(outs.len(), 1);
        assert_eq!(rels, vec![8]);
        assert_eq!(range, Some((100, 50)));
    }
}
