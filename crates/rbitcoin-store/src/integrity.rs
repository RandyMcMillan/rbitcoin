//! Tip-window revalidation on store open (Bitcoin Core `checkblocks`-style).
//!
//! Default window is [`VERIFY_TIP_BLOCKS`] (6) for both structural checks and
//! Class A merkle content. Open clears leftover strong bits and restores
//! fence bits, revalidates this window, then clears and restores again.
//!
//! Soft sidecar [`TIP_SEAL_NAME`]: written after a successful connect/disconnect
//! Class C barrier so a kill that advanced `confirmed` without a complete seal
//! can be clamped before revalidation. The barrier does not seal a tip whose
//! tx range ends past the durable create count. Open still drops a tip that
//! a later Class A truncate left past that count, including when the header
//! clear landed and the tip shrink did not.

use crate::error::StoreError;
use crate::header_table::block_header_hash;
use crate::store::Store;
use rbitcoin_primitives::{
    bytes, le_u16, le_u32, le_u64, merkle_root_from_txids, schema_file_openable, Fk, Height,
    SCHEMA_VERSION, STORE_MAGIC,
};
use std::fs::OpenOptions;
use std::io::Read;
use std::path::Path;

/// Core `-checkblocks` default: re-read the last N confirmed blocks at open.
pub const VERIFY_TIP_BLOCKS: u32 = 6;

/// Progress cadence while the open window walks confirmed heights.
#[doc(hidden)]
pub const TIP_REVALIDATE_STATUS_MS: u128 = 10_000;

/// Start, 10s progress, and done lines for one tip-window walk.
#[doc(hidden)]
pub struct TipRevalidateLog {
    pub started: std::time::Instant,
    pub logged_at: std::time::Instant,
    pub count: u64,
    pub walked: u64,
}

impl TipRevalidateLog {
    #[doc(hidden)]
    pub fn start(lo: u32, tip: u32) -> Self {
        let count = u64::from(tip - lo) + 1;
        rbitcoin_log::info!("store: tip revalidate {lo}..={tip} ({count} heights)");
        let started = std::time::Instant::now();
        Self {
            started,
            logged_at: started,
            count,
            walked: 0,
        }
    }

    #[doc(hidden)]
    pub fn step(&mut self, height: u32) {
        self.walked += 1;
        if self.logged_at.elapsed().as_millis() >= TIP_REVALIDATE_STATUS_MS {
            rbitcoin_log::info!(
                "store: tip revalidate {}/{} height={height}",
                self.walked,
                self.count
            );
            self.logged_at = std::time::Instant::now();
        }
    }

    #[doc(hidden)]
    pub fn done(&self, report: &TipRevalidateReport) {
        let height = match report.tip_after {
            Some(h) => h.to_string(),
            None => "none".to_string(),
        };
        let ms = self.started.elapsed().as_millis();
        if let Some(bad) = report.first_bad_height {
            rbitcoin_log::info!(
                "store: tip revalidate done height={height} ms={ms} bad={bad} reason={}",
                report.first_bad_reason.unwrap_or("")
            );
        } else {
            rbitcoin_log::info!("store: tip revalidate done height={height} ms={ms}");
        }
    }
}

/// Soft tip seal file under `store/` (not a SCHEMA_VERSION bump).
pub const TIP_SEAL_NAME: &str = "tip_seal";

/// On-disk tip seal after a complete Class C barrier.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TipSeal {
    pub tip_height: u32,
    pub tip_hash: [u8; 32],
    /// `confirmed` length (= tip_height + 1 when non-empty).
    pub confirmed_len: u64,
    pub generation: u64,
}

impl TipSeal {
    #[doc(hidden)]
    pub const BYTES: usize = 60;

    pub fn path(dir: &Path) -> std::path::PathBuf {
        dir.join(TIP_SEAL_NAME)
    }

    pub fn load(dir: &Path) -> Result<Option<Self>, StoreError> {
        let p = Self::path(dir);
        if !p.exists() {
            return Ok(None);
        }
        let mut f = OpenOptions::new()
            .read(true)
            .open(&p)
            .map_err(|e| StoreError::io(&p, e))?;
        let mut buf = [0u8; Self::BYTES];
        f.read_exact(&mut buf).map_err(|e| StoreError::io(&p, e))?;
        if buf[0..4] != STORE_MAGIC {
            return Err(StoreError::BadMagic);
        }
        let ver = le_u16!(buf, 4);
        if !schema_file_openable(ver) {
            return Err(StoreError::BadSchema(ver));
        }
        Ok(Some(Self {
            tip_height: le_u32!(buf, 8),
            confirmed_len: le_u64!(buf, 12),
            generation: le_u64!(buf, 20),
            tip_hash: bytes!(buf, 28, 32),
        }))
    }

    pub fn store(&self, dir: &Path) -> Result<(), StoreError> {
        let p = Self::path(dir);
        let mut buf = [0u8; Self::BYTES];
        buf[0..4].copy_from_slice(&STORE_MAGIC);
        buf[4..6].copy_from_slice(&SCHEMA_VERSION.to_le_bytes());
        // 6..8 reserved
        buf[8..12].copy_from_slice(&self.tip_height.to_le_bytes());
        buf[12..20].copy_from_slice(&self.confirmed_len.to_le_bytes());
        buf[20..28].copy_from_slice(&self.generation.to_le_bytes());
        buf[28..60].copy_from_slice(&self.tip_hash);
        crate::file::write_synced_tmp_rename(&p, &buf)
    }
}

/// Result of [`Store::revalidate_tip_window_n`].
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

/// Why one tip-window height did not pass.
#[derive(Debug)]
pub enum TipFault {
    /// The stored bytes disagree with the chain. The tip shrinks below this
    /// height.
    Bad(&'static str),
    /// A read failed and says nothing about the stored bytes. Open stops with
    /// this error and changes nothing.
    Read(StoreError),
}

impl TipFault {
    /// Classify a failed read. A corrupt or missing record, or a stored
    /// length past the body read cap, is `Bad(reason)`. An OS error, a
    /// completion-session fault, or any other error is `Read`.
    pub fn read(reason: &'static str) -> impl FnOnce(StoreError) -> TipFault {
        move |e| match e {
            StoreError::Corrupt(m) if !StoreError::is_uring_session_fault_msg(m) => {
                TipFault::Bad(reason)
            }
            StoreError::NotFound | StoreError::Rejected(_) => TipFault::Bad(reason),
            other => TipFault::Read(other),
        }
    }
}

/// Cut or refuse a `confirmed[]` with a null slot under the tip open keeps.
///
/// A torn page comes back from the write-ahead image, so the only nulls a
/// crash of this binary leaves are on top: above the sealed tip, or a
/// trailing run under it that the trim removes. A binary without the image
/// rewrote the 4 KiB block that holds the sealed tip on every barrier, so a
/// null in that block is its crash: the tip is cut below the first one, as
/// the window used to. A null anywhere else is not crash damage. Open would
/// build a fence without that height and clear its strong bits, so it
/// refuses before any repair runs.
///
/// The cut is in RAM. `true` means the caller flushes `confirmed` once the
/// spend delta has adopted a pre-30 `spent.body` and `meta` is current. The
/// flush arms a mark, which needs a current `meta`, and a current `meta`
/// beside a populated body with no `spent.delta/` refuses. A stop before
/// that flush cuts again at the next open.
pub(crate) fn cut_or_refuse_confirmed_hole(
    dir: &Path,
    confirmed: &crate::chain::ConfirmedTable,
) -> Result<bool, StoreError> {
    use crate::array_table::{ELEM, FLUSH_SCRATCH};
    use crate::file::FILE_HEADER_LEN;
    let Some(tip) = confirmed.tip_height() else {
        return Ok(false);
    };
    let cap = TipSeal::load(dir)?.map_or(tip.0, |s| s.tip_height.min(tip.0));
    let mut top = cap;
    while confirmed.get(Height(top))?.is_none() {
        let Some(below) = top.checked_sub(1) else {
            return Ok(false);
        };
        top = below;
    }
    let Some(hole) = (0..top).find_map(|h| match confirmed.get(Height(h)) {
        Ok(Some(_)) => None,
        Ok(None) => Some(Ok(h)),
        Err(e) => Some(Err(e)),
    }) else {
        return Ok(false);
    };
    let hole = hole?;
    // The block is the one that holds the sealed tip, not the top non-null
    // slot: a torn block can null a run that ends in the block before it.
    let cap_byte = FILE_HEADER_LEN as u64 + u64::from(cap) * ELEM;
    let block_byte = cap_byte & !(FLUSH_SCRATCH as u64 - 1);
    let block_first = block_byte.saturating_sub(FILE_HEADER_LEN as u64) / ELEM;
    if u64::from(hole) < block_first {
        eprintln!("rbitcoin: confirmed[{hole}] is null below tip {cap}; the store needs a reindex");
        return Err(StoreError::Corrupt("confirmed[] null slot below the tip"));
    }
    eprintln!(
        "rbitcoin: confirmed[{hole}] is null in the block that holds tip {cap}; \
         cutting the tip to {}",
        i64::from(hole) - 1
    );
    let write = crate::array_table::ClassCWrite::new();
    confirmed.arr.truncate(&write, u64::from(hole))?;
    Ok(true)
}

impl Store {
    /// Persist tip seal after a complete Class C tip barrier (connect or disconnect).
    ///
    /// Soft: if tip header is not loadable (synthetic Class C barrier tests, or
    /// mid-repair), skip the seal rather than failing the barrier — open
    /// revalidate still walks the tip window.
    pub fn publish_tip_seal(&self) -> Result<(), StoreError> {
        let Some(tip) = self.confirmed.tip_height() else {
            let p = TipSeal::path(self.path());
            let _ = std::fs::remove_file(&p);
            return Ok(());
        };
        let Some(fk) = self.confirmed.get(tip)? else {
            return Ok(());
        };
        let Ok(rec) = self.headers.get(fk) else {
            return Ok(());
        };
        let prev_gen = TipSeal::load(self.path())?
            .map(|s| s.generation)
            .unwrap_or(0);
        let seal = TipSeal {
            tip_height: tip.0,
            tip_hash: rec.hash,
            confirmed_len: u64::from(tip.0) + 1,
            generation: prev_gen.saturating_add(1),
        };
        seal.store(self.path())
    }

    /// Drop a confirmed tip that advanced past the last barrier seal, or a
    /// sealed-height tip whose header is not the sealed hash.
    fn clamp_to_tip_seal(
        &self,
        write: &crate::array_table::ClassCWrite,
        report: &mut TipRevalidateReport,
    ) -> Result<(), StoreError> {
        let Some(seal) = TipSeal::load(self.path())? else {
            return Ok(());
        };
        let Some(mut tip) = self.confirmed.tip_height() else {
            return Ok(());
        };
        if tip.0 > seal.tip_height {
            // Then compare the hash at the sealed height like any other tip.
            self.shrink_tip_to(write, Some(seal.tip_height), report)?;
            let Some(shrunk) = self.confirmed.tip_height() else {
                return Ok(());
            };
            tip = shrunk;
        }
        if tip.0 < seal.tip_height {
            return Ok(());
        }
        let Some(fk) = self.confirmed.get(tip)? else {
            return Ok(());
        };
        match self.headers.get(fk) {
            Ok(rec) if rec.hash != seal.tip_hash => {
                self.shrink_tip_to(write, tip.0.checked_sub(1), report)
            }
            _ => Ok(()),
        }
    }

    #[doc(hidden)]
    pub fn effective_checkblocks(&self, n: u32) -> Result<u32, StoreError> {
        // A missing marker is not "durable through nothing". Heights below the
        // last six can still be torn, and `n == 0` is the only whole-chain walk.
        let Some(durable) =
            crate::spend_durable::SpendDurable::load(self.path())?.map(|m| m.durable_through())
        else {
            return Ok(0);
        };
        Ok(crate::spend_durable::widen_checkblocks(
            n,
            self.confirmed.tip_height().map(|h| h.0),
            Some(durable),
        ))
    }

    /// Revalidate confirmed heights (structure + merkle).
    ///
    /// `n == 0` walks from genesis (Core `-checkblocks=0`). A missing
    /// `spend_durable` marker is that same walk: nothing has been device-flushed.
    /// A present marker widens any other window to at least [`VERIFY_TIP_BLOCKS`]
    /// and far enough to cover heights above that marker.
    pub fn revalidate_tip_window_n(&self, n: u32) -> Result<TipRevalidateReport, StoreError> {
        let n = self.effective_checkblocks(n)?;
        let mut report = TipRevalidateReport::default();
        if let Some(h) = self.confirmed.tip_height() {
            report.tip_before = Some(h.0);
        }
        let write = crate::array_table::ClassCWrite::new();

        // Clamp first: a cut back to the sealed tip can leave a null run on
        // top, and the trim below must see it.
        self.clamp_to_tip_seal(&write, &mut report)?;

        // HWM can include a trailing run of null slots (crash mid-grow). Trim
        // the whole suffix in one shot so the 6-height window sees a real tip.
        let trimmed = self.confirmed.trim_trailing_nulls(&write)?;
        if trimmed > 0 {
            self.flush_confirmed_only()?;
            self.rebuild_height_fence()?;
            self.persist_class_c_repair()?;
            report.tip_shrunk = true;
            eprintln!("rbitcoin: trimmed {trimmed} trailing null confirmed[] slots");
        }

        let Some(tip) = self.confirmed.tip_height() else {
            report.tip_before = report.tip_before.or(None);
            report.tip_after = None;
            return Ok(report);
        };
        if report.tip_before.is_none() {
            report.tip_before = Some(tip.0);
        }
        // Core `-checkblocks=0` (and negatives mapped to 0 by the node) = all.
        let lo = if n == 0 {
            0
        } else {
            tip.0.saturating_sub(n.saturating_sub(1))
        };
        let tx_count = self.txs.count();
        let mut last_good: Option<u32> = if lo == 0 { None } else { Some(lo - 1) };
        // Heights below the window are assumed good for shrink baseline when lo > 0.
        let mut progress = TipRevalidateLog::start(lo, tip.0);

        for h in lo..=tip.0 {
            match self.check_confirmed_height(&write, Height(h), tx_count, &mut report) {
                Ok(()) => last_good = Some(h),
                Err(TipFault::Bad(reason)) => {
                    report.first_bad_height = Some(h);
                    report.first_bad_reason = Some(reason);
                    break;
                }
                Err(TipFault::Read(e)) => return Err(e),
            }
            progress.step(h);
        }

        if report.first_bad_height.is_some() {
            self.shrink_tip_to(&write, last_good, &mut report)?;
        } else {
            report.tip_after = Some(tip.0);
        }

        if report.bodies_cleared > 0 {
            self.header_txs.flush()?;
        }
        let _ = self.publish_tip_seal();
        progress.done(&report);
        Ok(report)
    }

    /// Structural + content check for one confirmed height.
    ///
    /// [`TipFault::Bad`] is the first stored mismatch; the tip shrinks below
    /// it. [`TipFault::Read`] is a read that failed and changed nothing.
    /// Body clear alone (without tip claim failure) is counted in report and Ok.
    #[doc(hidden)]
    pub fn check_confirmed_height(
        &self,
        write: &crate::array_table::ClassCWrite,
        height: Height,
        tx_count: u64,
        report: &mut TipRevalidateReport,
    ) -> Result<(), TipFault> {
        let Some(fk) = self
            .confirmed
            .get(height)
            .map_err(TipFault::read("confirmed read"))?
        else {
            return Err(TipFault::Bad("confirmed null header_fk"));
        };
        let rec = self
            .headers
            .get(fk)
            .map_err(TipFault::read("header load"))?;

        if height.0 == 0 {
            if !rec.prev_fk.is_null() {
                return Err(TipFault::Bad("genesis prev_fk non-null"));
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
                return Err(TipFault::Bad("genesis header hash mismatch"));
            }
        } else {
            let parent_h = Height(height.0 - 1);
            let Some(parent_fk) = self
                .confirmed
                .get(parent_h)
                .map_err(TipFault::read("parent confirmed read"))?
            else {
                return Err(TipFault::Bad("parent confirmed null"));
            };
            if rec.prev_fk != parent_fk {
                return Err(TipFault::Bad("prev_fk != confirmed parent"));
            }
            let parent = self
                .headers
                .get(parent_fk)
                .map_err(TipFault::read("parent header load"))?;
            let expect = block_header_hash(
                rec.version,
                &parent.hash,
                &rec.merkle_root,
                rec.timestamp,
                rec.bits,
                rec.nonce,
            );
            if expect != rec.hash {
                return Err(TipFault::Bad("header hash mismatch vs parent"));
            }
        }

        match self
            .header_txs
            .get_range(fk)
            .map_err(TipFault::read("header_txs read"))?
        {
            None => Ok(()),
            Some((first, count)) => self.check_header_tx_range(
                write,
                fk,
                first,
                count,
                tx_count,
                &rec.merkle_root,
                report,
            ),
        }
    }

    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    pub fn check_header_tx_range(
        &self,
        write: &crate::array_table::ClassCWrite,
        fk: Fk,
        first: Fk,
        count: u32,
        tx_count: u64,
        merkle_root: &[u8; 32],
        report: &mut TipRevalidateReport,
    ) -> Result<(), TipFault> {
        self.block_txid_fault(first, count, tx_count, merkle_root)
            .map_err(|fault| self.drop_body_on_bad(write, fk, report, fault))?;
        self.check_block_input_edges(write, fk, first.0, count, report)
    }

    /// Range bounds and merkle root of one block's txids.
    fn block_txid_fault(
        &self,
        first: Fk,
        count: u32,
        tx_count: u64,
        merkle_root: &[u8; 32],
    ) -> Result<(), TipFault> {
        if count == 0 || first.is_null() {
            return Err(TipFault::Bad("header_txs empty association"));
        }
        let last = first.0.saturating_add(u64::from(count)).saturating_sub(1);
        if first.0 == 0 || last > tx_count {
            return Err(TipFault::Bad("header_txs range OOB"));
        }
        let leaves = self
            .txs
            .body_txid_range(first.0, last)
            .map_err(TipFault::read("txid.body read"))?;
        if leaves.len() != count as usize {
            return Err(TipFault::Bad("txid.body short for header_txs range"));
        }
        if merkle_root_from_txids(&leaves) != *merkle_root {
            return Err(TipFault::Bad("merkle root mismatch"));
        }
        Ok(())
    }

    /// A stored mismatch drops the block's body association. A failed read
    /// leaves it.
    fn drop_body_on_bad(
        &self,
        write: &crate::array_table::ClassCWrite,
        header_fk: Fk,
        report: &mut TipRevalidateReport,
        fault: TipFault,
    ) -> TipFault {
        if let TipFault::Bad(_) = fault {
            let _ = self.header_txs.clear_body(write, header_fk);
            report.bodies_cleared = report.bodies_cleared.saturating_add(1);
        }
        fault
    }

    /// Parent edges for one confirmed block. The first tx is the coinbase.
    ///
    /// A missing edge, a coinbase-shaped edge on a later tx, or a parent that
    /// is not an in-range output drops the block association and fails the
    /// window so the tip shrinks below that block. Seqsigwit count is skipped
    /// while that stem is pruned.
    #[doc(hidden)]
    pub fn check_block_input_edges(
        &self,
        write: &crate::array_table::ClassCWrite,
        header_fk: Fk,
        first: u64,
        count: u32,
        report: &mut TipRevalidateReport,
    ) -> Result<(), TipFault> {
        self.block_input_edge_reason(first, count)
            .map_err(|fault| self.drop_body_on_bad(write, header_fk, report, fault))
    }

    #[doc(hidden)]
    pub fn block_input_edge_reason(&self, first: u64, count: u32) -> Result<(), TipFault> {
        if count == 0 {
            return Err(TipFault::Bad("input edges short"));
        }
        let last = first.saturating_add(u64::from(count)).saturating_sub(1);
        let spans = self
            .txs
            .input
            .edges_span(first, last)
            .map_err(TipFault::read("input edges read"))?;
        if spans.len() != count as usize {
            return Err(TipFault::Bad("input edges short"));
        }
        let prune = self.txs.prune_seqsigwit_mode();
        let mut parents: Vec<(Fk, u32)> = Vec::new();
        for (i, slot) in spans.iter().enumerate() {
            let Some(edges) = slot else {
                return Err(TipFault::Bad("input unstamped"));
            };
            if !prune {
                let n = self.seqsigwit_input_count(Fk(first + i as u64))?;
                if n != edges.len() {
                    return Err(TipFault::Bad("input edge count"));
                }
            }
            if i == 0 {
                let coinbase = edges.len() == 1 && edges[0].parent.is_null() && edges[0].vout == 0;
                if !coinbase {
                    return Err(TipFault::Bad("coinbase input edge"));
                }
                continue;
            }
            for edge in edges {
                if edge.parent.is_null() {
                    return Err(TipFault::Bad("non-coinbase input edge"));
                }
                parents.push((edge.parent, edge.vout));
            }
        }
        if parents.is_empty() {
            return Ok(());
        }
        let fks: Vec<Fk> = parents.iter().map(|(fk, _)| *fk).collect();
        let locs = self
            .txs
            .create_loc
            .range_batch(&fks)
            .map_err(TipFault::read("input parent read"))?;
        if locs.len() != parents.len() {
            return Err(TipFault::Bad("input parent missing"));
        }
        for ((_, vout), loc) in parents.iter().zip(locs.iter()) {
            let Some(pair) = loc else {
                return Err(TipFault::Bad("input parent missing"));
            };
            if *vout >= pair.n_out {
                return Err(TipFault::Bad("input vout OOB"));
            }
        }
        Ok(())
    }

    #[doc(hidden)]
    pub fn seqsigwit_input_count(&self, fk: Fk) -> Result<usize, TipFault> {
        let (off, len) = self
            .txs
            .seqsigwit_range(fk)
            .map_err(TipFault::read("seqsigwit range"))?;
        if len == 0 {
            return Ok(0);
        }
        let raw = self
            .txs
            .seqsigwit
            .with_bytes_at(off, len, |b| Ok(b.to_vec()))
            .map_err(TipFault::read("seqsigwit read"))?;
        crate::tx_table::decode_seqsigwit_secret_to_end(&raw, None)
            .map(|ins| ins.len())
            .map_err(|_| TipFault::Bad("seqsigwit decode"))
    }

    /// Truncate confirmed tip to `last_good` (inclusive), or empty if `None`.
    #[doc(hidden)]
    pub fn shrink_tip_to(
        &self,
        write: &crate::array_table::ClassCWrite,
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

        let mut cur = tip.0;
        loop {
            let keep = match target_tip {
                Some(lg) => cur > lg,
                None => true,
            };
            if !keep {
                break;
            }
            self.confirmed.disconnect_tip(write, Height(cur))?;
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
        self.rebuild_height_fence()?;
        self.persist_class_c_repair()?;
        // Disconnect clamps in `Query`. Open revalidation shrinks here and
        // then replay treats a marker above the new tip as already annotated.
        self.clamp_spend_durable()?;
        report.tip_shrunk = true;
        report.tip_after = self.confirmed.tip_height().map(|h| h.0);
        Ok(())
    }

    /// After open fits Class A, drop confirmed heights it no longer covers.
    ///
    /// The fit never keeps fewer creates than a durable Class C row names:
    /// [`crate::tx_table::refuse_below_committed`] stops open first. What
    /// is left is trailing nulls, a header-only tip, and a Class C fixture.
    pub(crate) fn drop_confirmed_past_creates(&self) -> Result<(), StoreError> {
        let covered = self.txs.count();
        let Some(tip) = self.confirmed.tip_height() else {
            return Ok(());
        };
        let write = crate::array_table::ClassCWrite::new();
        let mut report = TipRevalidateReport::default();
        if let Some(bad) = self.class_a_confirmed_cut(covered)? {
            if self.header_tx_end(Height(bad))?.is_none() && self.strong_past_covered(covered)? {
                self.shrink_tip_to(&write, bad.checked_sub(1), &mut report)?;
            }
            return Ok(());
        }
        if self.header_tx_end(tip)?.is_some() || !self.strong_past_covered(covered)? {
            return Ok(());
        }
        if self.highest_height_with_tx_range(tip.0)?.is_some() {
            return Ok(());
        }
        self.shrink_tip_to(&write, None, &mut report)
    }

    fn strong_past_covered(&self, covered: u64) -> Result<bool, StoreError> {
        let Some(fk) = Fk::new(covered.saturating_add(1)) else {
            return Ok(false);
        };
        self.strong_tx.is_strong(fk)
    }

    /// First confirmed height whose tx range ends past `covered`, if any.
    fn class_a_confirmed_cut(&self, covered: u64) -> Result<Option<u32>, StoreError> {
        let Some(tip) = self.confirmed.tip_height() else {
            return Ok(None);
        };
        if let Some(end) = self.header_tx_end(tip)? {
            if end <= covered {
                return Ok(None);
            }
            return Ok(Some(self.first_height_past_creates(tip.0, covered)?));
        }
        if self.header_tx_end(Height(0))?.is_none() {
            return Ok(None);
        }
        let Some(last) = self.highest_height_with_tx_range(tip.0)? else {
            return Ok(None);
        };
        let Some(end) = self.header_tx_end(Height(last))? else {
            return Ok(None);
        };
        if end > covered {
            return Ok(Some(self.first_height_past_creates(last, covered)?));
        }
        if last < tip.0 {
            return Ok(Some(last + 1));
        }
        Ok(None)
    }

    fn header_tx_end(&self, height: Height) -> Result<Option<u64>, StoreError> {
        let Some(fk) = self.confirmed.get(height)? else {
            return Ok(None);
        };
        let Some((first, n)) = self.header_txs.get_range(fk)? else {
            return Ok(None);
        };
        if n == 0 || first.is_null() {
            return Ok(None);
        }
        Ok(Some(first.0.saturating_add(u64::from(n)).saturating_sub(1)))
    }

    /// `hi` itself ends past `covered`. Heights above `hi` are not searched.
    fn first_height_past_creates(&self, hi: u32, covered: u64) -> Result<u32, StoreError> {
        let mut lo = 0u32;
        let mut hi = hi;
        let mut found = hi;
        loop {
            let mid = lo + (hi - lo) / 2;
            let past = self
                .header_tx_end(Height(mid))?
                .is_some_and(|end| end > covered);
            if past {
                found = mid;
                if mid == lo {
                    break;
                }
                hi = mid - 1;
            } else if mid == hi {
                break;
            } else {
                lo = mid + 1;
            }
        }
        Ok(found)
    }

    fn highest_height_with_tx_range(&self, tip: u32) -> Result<Option<u32>, StoreError> {
        if self.header_tx_end(Height(tip))?.is_some() {
            return Ok(Some(tip));
        }
        let mut lo = 0u32;
        let mut hi = tip;
        let mut found = None;
        loop {
            let mid = lo + (hi - lo) / 2;
            if self.header_tx_end(Height(mid))?.is_some() {
                found = Some(mid);
                if mid == tip {
                    break;
                }
                lo = mid + 1;
            } else if mid == 0 {
                break;
            } else {
                hi = mid - 1;
            }
            if lo > hi {
                break;
            }
        }
        Ok(found)
    }

    #[doc(hidden)]
    pub fn persist_class_c_repair(&self) -> Result<(), StoreError> {
        let n = self.repair_class_c_above_tip()?;
        if n > 0 {
            self.strong_tx.flush()?;
        }
        Ok(())
    }
}
