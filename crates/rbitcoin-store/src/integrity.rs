//! Tip-window revalidation on store open (Bitcoin Core `checkblocks`-style).
//!
//! Default window is [`VERIFY_TIP_BLOCKS`] (6) for both structural checks and
//! Class A merkle content. Runs after `repair_class_c_above_tip` so tip is the
//! source of truth before P2P extends it.

use crate::error::StoreError;
use crate::header_table::block_header_hash;
use crate::store::Store;
use bitcoin_hashes::{sha256, Hash, HashEngine};
use rbitcoin_primitives::Height;

/// Core `-checkblocks` default: re-read the last N confirmed blocks at open.
pub const VERIFY_TIP_BLOCKS: u32 = 6;

/// Result of [`Store::revalidate_tip_window`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TipRevalidateReport {
    /// Confirmed tip height before any shrink (`None` if empty chain).
    pub tip_before: Option<u32>,
    /// Tip after shrink (`None` if emptied).
    pub tip_after: Option<u32>,
    /// First height that failed (if any).
    pub first_bad_height: Option<u32>,
    /// Human-readable reason for first_bad (for logs/tests).
    pub first_bad_reason: Option<&'static str>,
    /// Header_fk body associations cleared (out-of-bounds or merkle fail).
    pub bodies_cleared: u64,
    /// True when confirmed tip was truncated.
    pub tip_shrunk: bool,
}

impl TipRevalidateReport {
    pub fn is_clean(&self) -> bool {
        self.first_bad_height.is_none() && !self.tip_shrunk && self.bodies_cleared == 0
    }
}

/// Bitcoin block merkle root from leaf txids (internal byte order).
pub fn merkle_root_from_txids(leaves: &[[u8; 32]]) -> [u8; 32] {
    if leaves.is_empty() {
        return [0u8; 32];
    }
    let mut level: Vec<[u8; 32]> = leaves.to_vec();
    while level.len() > 1 {
        if level.len() % 2 == 1 {
            if let Some(last) = level.last().copied() {
                level.push(last);
            }
        }
        let mut next = Vec::with_capacity(level.len() / 2);
        for pair in level.chunks_exact(2) {
            next.push(hash256_concat(&pair[0], &pair[1]));
        }
        level = next;
    }
    level[0]
}

fn hash256_concat(a: &[u8; 32], b: &[u8; 32]) -> [u8; 32] {
    let mut eng = sha256::HashEngine::default();
    eng.input(a);
    eng.input(b);
    let mid = sha256::Hash::from_engine(eng);
    let mut eng2 = sha256::HashEngine::default();
    eng2.input(mid.as_byte_array());
    sha256::Hash::from_engine(eng2).to_byte_array()
}

impl Store {
    /// Revalidate the last [`VERIFY_TIP_BLOCKS`] confirmed heights (structure + merkle).
    ///
    /// On failure: clear bad Class A associations and/or shrink tip to the last
    /// good height, flush confirmed, then `repair_class_c_above_tip`.
    pub fn revalidate_tip_window(&self) -> Result<TipRevalidateReport, StoreError> {
        self.revalidate_tip_window_n(VERIFY_TIP_BLOCKS)
    }

    /// Same as [`Self::revalidate_tip_window`] with an explicit window (tests).
    pub fn revalidate_tip_window_n(&self, n: u32) -> Result<TipRevalidateReport, StoreError> {
        let mut report = TipRevalidateReport::default();
        let Some(tip) = self.confirmed.tip_height() else {
            return Ok(report);
        };
        report.tip_before = Some(tip.0);
        if n == 0 {
            report.tip_after = Some(tip.0);
            return Ok(report);
        }

        let lo = tip.0.saturating_sub(n.saturating_sub(1));
        let tx_count = self.txs.count();
        let mut last_good: Option<u32> = if lo == 0 { None } else { Some(lo - 1) };
        // Heights below the window are assumed good for shrink baseline when lo > 0.

        for h in lo..=tip.0 {
            match self.check_confirmed_height(Height(h), tx_count, &mut report) {
                Ok(()) => last_good = Some(h),
                Err(reason) => {
                    report.first_bad_height = Some(h);
                    report.first_bad_reason = Some(reason);
                    break;
                }
            }
        }

        if report.first_bad_height.is_some() {
            self.shrink_tip_to(last_good, &mut report)?;
        } else {
            report.tip_after = Some(tip.0);
        }

        if report.bodies_cleared > 0 {
            self.header_txs.flush()?;
        }
        Ok(report)
    }

    /// Structural + content check for one confirmed height.
    ///
    /// Returns `Err(reason)` on first hard failure that requires tip shrink.
    /// Body clear alone (without tip claim failure) is counted in report and Ok.
    fn check_confirmed_height(
        &self,
        height: Height,
        tx_count: u64,
        report: &mut TipRevalidateReport,
    ) -> Result<(), &'static str> {
        let Some(fk) = self
            .confirmed
            .get(height)
            .map_err(|_| "confirmed read")?
        else {
            return Err("confirmed null header_fk");
        };
        let rec = match self.headers.get(fk) {
            Ok(r) => r,
            Err(_) => return Err("header load"),
        };

        // S1: prev link + hash identity against conf parent.
        if height.0 == 0 {
            if !rec.prev_fk.is_null() {
                return Err("genesis prev_fk non-null");
            }
            let zeros = [0u8; 32];
            let expect = block_header_hash(
                rec.version,
                &zeros,
                &rec.merkle_root,
                rec.timestamp,
                rec.bits,
                rec.nonce,
            );
            if expect != rec.hash {
                return Err("genesis header hash mismatch");
            }
        } else {
            let parent_h = Height(height.0 - 1);
            let Some(parent_fk) = self
                .confirmed
                .get(parent_h)
                .map_err(|_| "parent confirmed read")?
            else {
                return Err("parent confirmed null");
            };
            if rec.prev_fk != parent_fk {
                return Err("prev_fk != confirmed parent");
            }
            let parent = match self.headers.get(parent_fk) {
                Ok(p) => p,
                Err(_) => return Err("parent header load"),
            };
            let expect = block_header_hash(
                rec.version,
                &parent.hash,
                &rec.merkle_root,
                rec.timestamp,
                rec.bits,
                rec.nonce,
            );
            if expect != rec.hash {
                return Err("header hash mismatch vs parent");
            }
        }

        // S2: header_txs bounds; S3: merkle from txid.body when body present.
        match self.header_txs.get_range(fk) {
            Ok(None) => Ok(()), // no body — ok for structural tip (unarchived tip rare)
            Ok(Some((first, count))) => {
                if count == 0 || first.is_null() {
                    let _ = self.header_txs.clear_body(fk);
                    report.bodies_cleared = report.bodies_cleared.saturating_add(1);
                    return Err("header_txs empty association");
                }
                let last = first.0.saturating_add(u64::from(count)).saturating_sub(1);
                if first.0 == 0 || last > tx_count {
                    let _ = self.header_txs.clear_body(fk);
                    report.bodies_cleared = report.bodies_cleared.saturating_add(1);
                    return Err("header_txs range OOB");
                }
                // S3: merkle from dense txid sidefile (no full body decode).
                let leaves = match self.txs.body_txid_range(first.0, last) {
                    Ok(v) if v.len() == count as usize => v,
                    Ok(_) => {
                        let _ = self.header_txs.clear_body(fk);
                        report.bodies_cleared = report.bodies_cleared.saturating_add(1);
                        return Err("txid.body short for header_txs range");
                    }
                    Err(_) => {
                        let _ = self.header_txs.clear_body(fk);
                        report.bodies_cleared = report.bodies_cleared.saturating_add(1);
                        return Err("txid.body read");
                    }
                };
                let root = merkle_root_from_txids(&leaves);
                if root != rec.merkle_root {
                    let _ = self.header_txs.clear_body(fk);
                    report.bodies_cleared = report.bodies_cleared.saturating_add(1);
                    return Err("merkle root mismatch");
                }
                Ok(())
            }
            Err(_) => Err("header_txs read"),
        }
    }

    /// Truncate confirmed tip to `last_good` (inclusive), or empty if `None`.
    fn shrink_tip_to(
        &self,
        last_good: Option<u32>,
        report: &mut TipRevalidateReport,
    ) -> Result<(), StoreError> {
        let Some(tip) = self.confirmed.tip_height() else {
            report.tip_after = None;
            return Ok(());
        };
        let target_tip = last_good;
        match target_tip {
            Some(lg) if lg >= tip.0 => {
                report.tip_after = Some(tip.0);
                return Ok(());
            }
            _ => {}
        }

        // Disconnect from tip down until target (or empty).
        let mut cur = tip.0;
        loop {
            let keep = match target_tip {
                Some(lg) => cur > lg,
                None => true,
            };
            if !keep {
                break;
            }
            self.confirmed.disconnect_tip(Height(cur))?;
            if cur == 0 {
                break;
            }
            cur -= 1;
            if target_tip == Some(cur) {
                break;
            }
            if self.confirmed.tip_height().is_none() {
                break;
            }
        }

        self.flush_confirmed_only()?;
        let _ = self.repair_class_c_above_tip()?;
        report.tip_shrunk = true;
        report.tip_after = self.confirmed.tip_height().map(|h| h.0);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::header_table::HeaderRecord;
    use crate::tx_table::{InputRecord, OutputRecord, TxRecord};
    use rbitcoin_primitives::Fk;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn tmp() -> PathBuf {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let p = std::env::temp_dir().join(format!("rbitcoin-integrity-{n}"));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn hdr(prev: Fk, parent_hash: [u8; 32], salt: u8) -> HeaderRecord {
        let version = 1i32;
        let timestamp = u32::from(salt) + 1;
        let bits = 0x207fffff;
        let nonce = u32::from(salt);
        let mut merkle = [0u8; 32];
        merkle[0] = salt;
        let hash = if prev.is_null() {
            block_header_hash(version, &[0u8; 32], &merkle, timestamp, bits, nonce)
        } else {
            block_header_hash(version, &parent_hash, &merkle, timestamp, bits, nonce)
        };
        HeaderRecord {
            prev_fk: prev,
            version,
            timestamp,
            bits,
            nonce,
            merkle_root: merkle,
            hash,
        }
    }

    fn put_coinbase(s: &Store, salt: u8) -> Fk {
        let rec = TxRecord {
            txid: [salt; 32],
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 1,
            output_start_fk: Fk::NULL,
            output_count: 1,
        };
        let ins = vec![InputRecord::coinbase(u32::MAX, vec![salt], vec![])];
        let outs = vec![OutputRecord::unspent(50, vec![0x51])];
        s.put_tx_full_batch_indexed(&[(rec, ins, outs)], true)
            .unwrap()[0]
    }

    #[test]
    fn merkle_root_single_and_pair() {
        let a = [1u8; 32];
        assert_eq!(merkle_root_from_txids(&[a]), a);
        let b = [2u8; 32];
        let root = merkle_root_from_txids(&[a, b]);
        assert_eq!(root, hash256_concat(&a, &b));
    }

    #[test]
    fn clean_header_chain_revalidate_no_shrink() {
        let dir = tmp();
        let s = Store::create(&dir).unwrap();
        let mut parent_hash = [0u8; 32];
        let mut prev = Fk::NULL;
        for h in 0u32..4 {
            let rec = hdr(prev, parent_hash, h as u8);
            parent_hash = rec.hash;
            let fk = s.put_header(&rec).unwrap();
            prev = fk;
            s.confirmed.set(Height(h), fk).unwrap();
        }
        s.flush_class_c_tip().unwrap();
        s.headers.flush().unwrap();

        let r = s.revalidate_tip_window_n(6).unwrap();
        assert!(r.is_clean(), "clean chain should not shrink: {r:?}");
        assert_eq!(s.confirmed.tip_height(), Some(Height(3)));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn poison_prev_edge_shrinks_tip() {
        let dir = tmp();
        let s = Store::create(&dir).unwrap();
        let g = hdr(Fk::NULL, [0u8; 32], 0);
        let g_fk = s.put_header(&g).unwrap();
        s.confirmed.set(Height(0), g_fk).unwrap();
        let a = hdr(g_fk, g.hash, 1);
        let a_fk = s.put_header(&a).unwrap();
        s.confirmed.set(Height(1), a_fk).unwrap();
        let b = hdr(a_fk, a.hash, 2);
        let b_fk = s.put_header(&b).unwrap();
        s.confirmed.set(Height(2), b_fk).unwrap();
        // Steal tip height 2 to point at G (false conf edge).
        s.confirmed.set(Height(2), g_fk).unwrap();
        s.flush_class_c_tip().unwrap();

        let r = s.revalidate_tip_window_n(6).unwrap();
        assert!(r.tip_shrunk, "must shrink: {r:?}");
        assert_eq!(r.first_bad_reason, Some("prev_fk != confirmed parent"));
        assert_eq!(s.confirmed.tip_height(), Some(Height(1)));
        assert_eq!(r.tip_after, Some(1));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn merkle_mismatch_clears_body_and_shrinks() {
        let dir = tmp();
        let s = Store::create(&dir).unwrap();
        let g = hdr(Fk::NULL, [0u8; 32], 0);
        let g_fk = s.put_header(&g).unwrap();
        s.confirmed.set(Height(0), g_fk).unwrap();
        let tx_fk = put_coinbase(&s, 9);
        let txid = s.txs.body_txid(tx_fk).unwrap();
        // Header merkle is salt-based, not txid → S3 fail.
        s.header_txs.put_range(g_fk, tx_fk, 1).unwrap();
        // Ensure genesis hash still consistent with its merkle field (no rewrite).
        let _ = txid;
        s.flush_class_c_tip().unwrap();
        s.header_txs.flush().unwrap();
        s.txs.flush().unwrap();

        let r = s.revalidate_tip_window_n(6).unwrap();
        assert_eq!(r.first_bad_reason, Some("merkle root mismatch"));
        assert!(r.bodies_cleared >= 1);
        assert!(r.tip_shrunk);
        assert!(s.confirmed.tip_height().is_none());
        assert!(!s.header_txs.has_body(g_fk).unwrap());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
