//! Offline repair of Class A header graph invariants.
//!
//! Mainnet failure mode: millions of duplicate header rows and false `prev_fk`
//! edges (parent pointer does not match the prev committed in the block hash).
//! IBD `resume_work_path_after_tip` walks **every** body row by `prev_fk`, so a
//! single false edge after tip invents tens of thousands of synthetic heights.
//!
//! This tool (node **stopped**):
//! 1. Re-links the confirmed chain: `prev_fk(h) = confirmed[h-1]`.
//! 2. Nulls every non-null `prev_fk` whose hash does not verify against the
//!    parent row (breaks false edges, including tip+1 → old-block dups).
//! 3. Optionally reports dry-run metrics without writing.
//!
//! Unconfirmed archive beyond tip is left in place but unlinked when invalid;
//! normal IBD re-`getheaders` under the write gate re-extends cleanly.

use crate::error::StoreError;
use crate::header_table::{block_header_hash, HeaderRecord};
use crate::store::Store;
use rbitcoin_primitives::{Fk, Height};
use std::collections::HashSet;
use std::path::Path;

#[derive(Debug, Clone, Default)]
pub struct RebuildReport {
    pub header_rows: u64,
    pub tip_height: Option<u32>,
    pub confirmed_relinked: u64,
    pub false_prev_nulled: u64,
    /// Confirmed heights that wrongly pointed at tip+1 (or tip) and were restored.
    pub confirmed_tip_plus_one_scrubbed: u64,
    pub resume_walk_before: u64,
    pub resume_walk_after: u64,
    pub null_prev_rows: u64,
    pub wrote: bool,
}

/// Walk children by `prev_fk` from tip (highest fk child), same idea as
/// `resume_work_path_after_tip` without body preference.
pub fn resume_walk_len(store: &Store, tip_fk: Fk, max: u64) -> Result<u64, StoreError> {
    let n = store.header_count();
    // prev_fk → max child fk
    let mut best_child: std::collections::HashMap<u64, u64> = std::collections::HashMap::new();
    for id in 1..=n {
        let rec = store.get_header(Fk(id))?;
        let p = rec.prev_fk.get().unwrap_or(0);
        best_child
            .entry(p)
            .and_modify(|c| *c = (*c).max(id))
            .or_insert(id);
    }
    let mut cur = tip_fk.get().unwrap_or(0);
    let mut steps = 0u64;
    let mut seen = HashSet::new();
    while steps < max {
        let Some(&next) = best_child.get(&cur) else {
            break;
        };
        if !seen.insert(next) {
            break;
        }
        steps += 1;
        cur = next;
    }
    Ok(steps)
}

fn prev_matches_hash(store: &Store, rec: &HeaderRecord) -> Result<bool, StoreError> {
    if rec.prev_fk.is_null() {
        return Ok(true);
    }
    let parent = match store.get_header(rec.prev_fk) {
        Ok(p) => p,
        Err(StoreError::NotFound) | Err(StoreError::InvalidFk) => return Ok(false),
        Err(e) => return Err(e),
    };
    let expect = block_header_hash(
        rec.version,
        &parent.hash,
        &rec.merkle_root,
        rec.timestamp,
        rec.bits,
        rec.nonce,
    );
    Ok(expect == rec.hash)
}

/// Analyze and optionally repair header `prev_fk` integrity in `store_dir`
/// (the `store/` directory inside a datadir).
pub fn rebuild_headers(store_dir: &Path, write: bool) -> Result<RebuildReport, StoreError> {
    let store = Store::open(store_dir)?;
    let n = store.header_count();
    let tip_h = store.tip_height();
    let tip_fk = match tip_h {
        Some(h) => store
            .confirmed
            .get(h)?
            .ok_or(StoreError::Corrupt("tip fk missing"))?,
        None => Fk::NULL,
    };

    let mut report = RebuildReport {
        header_rows: n,
        tip_height: tip_h.map(|h| h.0),
        wrote: write,
        ..Default::default()
    };

    if !tip_fk.is_null() {
        report.resume_walk_before = resume_walk_len(&store, tip_fk, 500_000)?;
    }

    // Count null prev.
    for id in 1..=n {
        let rec = store.get_header(Fk(id))?;
        if rec.prev_fk.is_null() {
            report.null_prev_rows += 1;
        }
    }

    // 0) Scrub tip / tip+1 stolen into confirmed[] **before** any header rewrites
    // that key off conf[h] (wrong conf[h] would rewrite the real tip+1 row).
    if let (Some(tip), tip_fk) = (tip_h, tip_fk) {
        if !tip_fk.is_null() {
            report.confirmed_tip_plus_one_scrubbed =
                scrub_stolen_tip_headers(&store, tip, tip_fk, write)?;
        }
    }

    // 1) Relink confirmed chain when the proposed parent verifies.
    // Skip (do not abort) heights whose hash does not commit to confirmed[h-1]
    // — deeper corruption is left for a full rebuild; false-edge nulling below
    // still unsticks resume.
    if let Some(tip) = tip_h {
        let mut prev_fk = Fk::NULL;
        for h in 0..=tip.0 {
            let fk = store
                .confirmed
                .get(Height(h))?
                .ok_or(StoreError::Corrupt("confirmed hole"))?;
            let mut rec = store.get_header(fk)?;
            if h == 0 {
                if !rec.prev_fk.is_null() {
                    rec.prev_fk = Fk::NULL;
                    if write {
                        store.rewrite_header(fk, &rec)?;
                    }
                    report.confirmed_relinked += 1;
                }
            } else if rec.prev_fk != prev_fk {
                let mut candidate = rec.clone();
                candidate.prev_fk = prev_fk;
                if prev_matches_hash(&store, &candidate)? {
                    if write {
                        store.rewrite_header(fk, &candidate)?;
                    }
                    report.confirmed_relinked += 1;
                    rec = candidate;
                }
                // else: leave existing prev_fk; may be nulled in pass 2 if invalid
            }
            let _ = rec;
            prev_fk = fk;
        }
    }

    // 2) Null false prev edges on all rows (including unconfirmed dups).
    for id in 1..=n {
        let fk = Fk(id);
        let mut rec = store.get_header(fk)?;
        if rec.prev_fk.is_null() {
            continue;
        }
        if prev_matches_hash(&store, &rec)? {
            continue;
        }
        report.false_prev_nulled += 1;
        if write {
            rec.prev_fk = Fk::NULL;
            store.rewrite_header(fk, &rec)?;
        }
    }

    if write {
        store.flush_header_archive()?;
        // confirmed[] is Class C L2 — must flush or scrub is lost on restart.
        store.flush_confirmed_only()?;
    }

    if !tip_fk.is_null() {
        // Re-open not required: rewrites are visible via same handle.
        report.resume_walk_after = resume_walk_len(&store, tip_fk, 500_000)?;
    }

    Ok(report)
}

/// True tip+1 = verified child of `tip_fk` (wire prev commits to tip hash).
fn tip_plus_one_fks(store: &Store, tip_fk: Fk) -> Result<Vec<Fk>, StoreError> {
    let tip_rec = store.get_header(tip_fk)?;
    let n = store.header_count();
    let mut out = Vec::new();
    for id in 1..=n {
        let fk = Fk(id);
        let rec = store.get_header(fk)?;
        if rec.prev_fk != tip_fk {
            continue;
        }
        let expect = block_header_hash(
            rec.version,
            &tip_rec.hash,
            &rec.merkle_root,
            rec.timestamp,
            rec.bits,
            rec.nonce,
        );
        if expect == rec.hash {
            out.push(fk);
        }
    }
    Ok(out)
}

/// Restore confirmed heights that steal the tip or tip+1 header row.
///
/// Prefer original sequential fk `h+1` when that row exists and is **not** the
/// tip/tip+1 thief; otherwise leave unchanged (caller can reindex).
fn scrub_stolen_tip_headers(
    store: &Store,
    tip: Height,
    tip_fk: Fk,
    write: bool,
) -> Result<u64, StoreError> {
    let tip_plus = tip_plus_one_fks(store, tip_fk)?;
    if tip_plus.is_empty() {
        return Ok(0);
    }
    let tip_rec = store.get_header(tip_fk)?;
    let mut stolen_hashes: HashSet<[u8; 32]> = HashSet::new();
    stolen_hashes.insert(tip_rec.hash);
    for &fk in &tip_plus {
        stolen_hashes.insert(store.get_header(fk)?.hash);
    }
    let tip_plus_set: HashSet<u64> = tip_plus.iter().filter_map(|f| f.get()).collect();

    let mut fixed = 0u64;
    for h in 0..tip.0 {
        let Some(fk) = store.confirmed.get(Height(h))? else {
            continue;
        };
        let steal = tip_plus_set.contains(&fk.get().unwrap_or(0)) || {
            let rec = store.get_header(fk)?;
            stolen_hashes.contains(&rec.hash) && fk != tip_fk
        };
        // tip hash may only appear at tip height; tip+1 hash must not appear at all below tip.
        if !steal {
            // tip_fk itself at height h < tip is also wrong
            if fk == tip_fk {
                // fall through to restore
            } else {
                continue;
            }
        }
        // Prefer original 1-based sequential fk for early mainnet layout.
        let candidate = Fk(u64::from(h) + 1);
        if candidate.get() == Some(0) || candidate == tip_fk || tip_plus_set.contains(&candidate.0)
        {
            continue;
        }
        if candidate.0 > store.header_count() {
            continue;
        }
        let crec = store.get_header(candidate)?;
        if stolen_hashes.contains(&crec.hash) {
            continue;
        }
        if write {
            store.confirmed.set(Height(h), candidate)?;
        }
        fixed = fixed.saturating_add(1);
    }
    Ok(fixed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::header_table::block_header_hash;
    use crate::store::Store;
    use rbitcoin_primitives::{Fk, Height};

    fn tmp() -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!(
            "rbitcoin-rebuild-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn genesis() -> HeaderRecord {
        HeaderRecord {
            prev_fk: Fk::NULL,
            version: 1,
            timestamp: 1_234_567,
            bits: 0x207fffff,
            nonce: 0,
            merkle_root: [1u8; 32],
            hash: [0xaa; 32],
        }
    }

    fn child(parent: &HeaderRecord, parent_fk: Fk, salt: u32) -> HeaderRecord {
        let version = 1;
        let timestamp = 1_700_000_000 + salt;
        let bits = 0x207fffff;
        let nonce = salt;
        let mut merkle = [0u8; 32];
        merkle[0] = salt as u8;
        let hash = block_header_hash(version, &parent.hash, &merkle, timestamp, bits, nonce);
        HeaderRecord {
            prev_fk: parent_fk,
            version,
            timestamp,
            bits,
            nonce,
            merkle_root: merkle,
            hash,
        }
    }

    /// One pad: dry-run null report, write repair, confirmed-chain relink, empty tip.
    #[test]
    fn rebuild_headers_dry_run_write_relink_and_empty_tip_journey() {
        let dir = tmp();
        let store = Store::create(&dir).unwrap();

        let g = genesis();
        let g_fk = store.put_header(&g).unwrap();
        let a = child(&g, g_fk, 1);
        let a_fk = store.put_header(&a).unwrap();
        let b = child(&a, a_fk, 2);
        let b_fk = store.put_header(&b).unwrap();

        // Confirm through A (tip = A). Wrong prev on A so relink pass runs.
        store.confirmed.set(Height(0), g_fk).unwrap();
        store.confirmed.set(Height(1), a_fk).unwrap();
        {
            let mut a_bad = store.get_header(a_fk).unwrap();
            a_bad.prev_fk = b_fk; // not G — relink restores g_fk
            store.rewrite_header(a_fk, &a_bad).unwrap();
        }

        // Plant poison with put_raw: hash commits to A, but prev_fk lies as B.
        // Unique hash (not a G duplicate) so ensure stays unambiguous.
        let mut poison = child(&a, a_fk, 99);
        poison.prev_fk = b_fk;
        let p_fk = store.put_header_raw(&poison).unwrap();
        assert_eq!(store.get_header(p_fk).unwrap().prev_fk, b_fk);

        // put() delegates to ensure (idempotent same hash) — covers HeaderTable::put.
        assert_eq!(store.put_header(&g).unwrap(), g_fk);

        // rebuild_headers re-opens from disk — durable tip + headers required.
        store.flush().unwrap();

        let before = resume_walk_len(&store, a_fk, 100).unwrap();
        assert!(
            before >= 1,
            "tip A should reach B then poison or at least B; walk={before}"
        );

        // Dry-run: reports false edges, does not rewrite.
        let dry = rebuild_headers(&dir, false).unwrap();
        assert!(dry.false_prev_nulled >= 1, "dry={dry:?}");
        assert!(!dry.wrote);
        assert_eq!(
            store.get_header(p_fk).unwrap().prev_fk,
            b_fk,
            "dry-run must leave poison prev intact"
        );
        assert!(dry.null_prev_rows >= 1);
        assert_eq!(dry.tip_height, Some(1));
        assert!(dry.resume_walk_before >= 1);

        let report = rebuild_headers(&dir, true).unwrap();
        assert!(report.wrote);
        assert!(report.false_prev_nulled >= 1);
        assert!(
            report.confirmed_relinked >= 1,
            "A must be relinked to G: {report:?}"
        );
        assert_eq!(store.get_header(p_fk).unwrap().prev_fk, Fk::NULL);
        assert_eq!(
            store.get_header(a_fk).unwrap().prev_fk,
            g_fk,
            "confirmed A must point at G after relink"
        );

        let after = resume_walk_len(&store, a_fk, 100).unwrap();
        assert!(
            after <= before,
            "repair must not lengthen resume walk: before={before} after={after}"
        );
        let mut kids_b = 0u32;
        for id in 1..=store.header_count() {
            if store.get_header(Fk(id)).unwrap().prev_fk == b_fk {
                kids_b += 1;
            }
        }
        assert_eq!(kids_b, 0);

        // Empty tip path (no confirmed): still scans rows.
        let dir2 = tmp();
        let s2 = Store::create(&dir2).unwrap();
        let g2 = genesis();
        let _ = s2.put_header(&g2).unwrap();
        let empty = rebuild_headers(&dir2, true).unwrap();
        assert_eq!(empty.tip_height, None);
        assert_eq!(empty.resume_walk_before, 0);
        assert_eq!(empty.resume_walk_after, 0);
        assert!(empty.null_prev_rows >= 1);
        assert_eq!(empty.header_rows, 1);

        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&dir2);
    }

    /// Mainnet tip stall: confirmed[mid] points at real tip+1 → has_block(tip+1).
    #[test]
    fn rebuild_scrubs_tip_plus_one_stolen_into_confirmed() {
        let dir = tmp();
        let store = Store::create(&dir).unwrap();

        let g = genesis();
        let g_fk = store.put_header(&g).unwrap();
        let a = child(&g, g_fk, 1);
        let a_fk = store.put_header(&a).unwrap();
        let b = child(&a, a_fk, 2); // tip+1 when tip = A
        let b_fk = store.put_header(&b).unwrap();

        store.confirmed.set(Height(0), g_fk).unwrap();
        store.confirmed.set(Height(1), a_fk).unwrap(); // tip
                                                       // Corrupt: height 0 points at tip+1 (b) — same shape as mainnet conf[874022].
        store.confirmed.set(Height(0), b_fk).unwrap();
        store.flush().unwrap();

        let report = rebuild_headers(&dir, true).unwrap();
        assert!(
            report.confirmed_tip_plus_one_scrubbed >= 1,
            "must scrub stolen tip+1 from confirmed, got {report:?}"
        );
        // rebuild_headers opens its own Store — re-open to see disk.
        drop(store);
        let store = Store::open(&dir).unwrap();
        let restored = store.confirmed.get(Height(0)).unwrap().unwrap();
        assert_ne!(restored, b_fk, "tip+1 must not remain at height 0");
        assert_eq!(store.confirmed.get(Height(1)).unwrap().unwrap(), a_fk);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
