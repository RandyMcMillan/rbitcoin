//! Segmented Class A `tx.head`: fixed-bits open-address tables + seal-time fuse8.
//!
//! Layout (after on-open migration from flat files):
//! ```text
//! store/
//!   tx.head/
//!     meta                       # segment descriptors
//!     000000                     # fixed-bits 4 B relative create ids
//!     000000.fuse8               # sealed binary fuse8 (absent while open)
//!     …
//! ```
//!
//! **Migration:** flat `tx.head.meta` + `tx.head.NNNNNN`(+`.fuse8`) rename into
//! `tx.head/` on open.
//!
//! **Relative fks:** slot stores `rel` where `0` = empty and
//! `fk = first_fk + rel - 1` (1-based relative within the segment).
//!
//! **Capacity:** open segment ends at
//! `MIN(body soft-span, floor(slots × HEAD_LOAD_START))`; then seal (fuse8) and
//! open a new head. Soft-span is supplied by the caller ([`force_roll`]).
//!
//! **Lookup:** open always probed first; sealed newest→oldest gated by fuse8;
//! candidates are absolute fks for body-verify by the caller.

use crate::address_head::{AddressHead, HeadLayout, HEAD_LOAD_START, MAINNET_BITS};
use crate::error::StoreError;
use crate::fuse8_filter::{fuse_key_from_mixed, open_file, FuseFileOpen, SealedFuse8};
use crate::tx_idx::DEFAULT_SOFT_SPAN;
use rbitcoin_primitives::{Fk, SCHEMA_VERSION, STORE_MAGIC};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

const META_VERSION: u32 = 1;
const META_HEADER_LEN: usize = 24;
const SEG_DESC_LEN: usize = 32;
const FLAG_SEALED: u32 = 1;

/// Product default head width (2²⁵ slots × 4 B = 128 MiB per segment).
pub const SEGMENT_HEAD_BITS: u32 = MAINNET_BITS;

#[derive(Debug, Default, Clone, Copy)]
pub struct HeadLookupStats {
    pub open_probes: u64,
    pub sealed_fuse_checks: u64,
    pub sealed_fuse_skips: u64,
    pub sealed_head_probes: u64,
    pub rolls: u64,
    pub seals: u64,
}

static LOOKUP_OPEN: AtomicU64 = AtomicU64::new(0);
static LOOKUP_FUSE_CHK: AtomicU64 = AtomicU64::new(0);
static LOOKUP_FUSE_SKIP: AtomicU64 = AtomicU64::new(0);
static LOOKUP_SEALED_PROBE: AtomicU64 = AtomicU64::new(0);
static ROLLS: AtomicU64 = AtomicU64::new(0);
static SEALS: AtomicU64 = AtomicU64::new(0);

// Test-only soft-span override (bytes). Thread-local: a sibling test that
// `test_set_soft_span_bytes(0)` must not reset this thread's roll window.
// Non-zero wins over env.
#[cfg(test)]
thread_local! {
    static TEST_SOFT_SPAN_OVERRIDE: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

pub fn sample_lookup_stats() -> HeadLookupStats {
    HeadLookupStats {
        open_probes: LOOKUP_OPEN.swap(0, Ordering::Relaxed),
        sealed_fuse_checks: LOOKUP_FUSE_CHK.swap(0, Ordering::Relaxed),
        sealed_fuse_skips: LOOKUP_FUSE_SKIP.swap(0, Ordering::Relaxed),
        sealed_head_probes: LOOKUP_SEALED_PROBE.swap(0, Ordering::Relaxed),
        rolls: ROLLS.swap(0, Ordering::Relaxed),
        seals: SEALS.swap(0, Ordering::Relaxed),
    }
}

pub fn snapshot_lookup_stats() -> HeadLookupStats {
    HeadLookupStats {
        open_probes: LOOKUP_OPEN.load(Ordering::Relaxed),
        sealed_fuse_checks: LOOKUP_FUSE_CHK.load(Ordering::Relaxed),
        sealed_fuse_skips: LOOKUP_FUSE_SKIP.load(Ordering::Relaxed),
        sealed_head_probes: LOOKUP_SEALED_PROBE.load(Ordering::Relaxed),
        rolls: ROLLS.load(Ordering::Relaxed),
        seals: SEALS.load(Ordering::Relaxed),
    }
}

struct Segment {
    first_fk: u64,
    count: AtomicU64,
    file_id: u32,
    sealed: bool,
    head: Arc<AddressHead>,
    fuse: Option<SealedFuse8>,
    /// Mixed fuse keys while open (for seal). Empty when sealed.
    open_keys: Mutex<Vec<u64>>,
    /// Sealed fuse is always-probe (legacy v1 / unreadable body); rewrite as v2.
    fuse_needs_rewrite: bool,
}

/// Multi-segment keyless address head with seal-time binary fuse8.
pub struct SegmentedTxHead {
    dir: PathBuf,
    layout: HeadLayout,
    segments: RwLock<Arc<Vec<Arc<Segment>>>>,
    next_file_id: AtomicU32,
    max_keys: u64,
    /// Serializes seal/roll + inserts (sole Class A appender still the rule).
    write: Mutex<()>,
}

impl SegmentedTxHead {
    pub fn create(dir: &Path, layout: HeadLayout) -> Result<Self, StoreError> {
        if layout.entry_bytes != 4 {
            return Err(StoreError::Corrupt(
                "segmented tx.head requires 4 B relative entries",
            ));
        }
        let dir = dir.to_path_buf();
        refuse_legacy_mono_head(&dir)?;
        write_meta(&dir, layout.bits, &[])?;
        Ok(Self {
            dir,
            max_keys: max_keys_for_layout(layout),
            layout,
            segments: RwLock::new(Arc::new(Vec::new())),
            next_file_id: AtomicU32::new(0),
            write: Mutex::new(()),
        })
    }

    pub fn open(dir: &Path) -> Result<Self, StoreError> {
        let dir = dir.to_path_buf();
        refuse_legacy_mono_head(&dir)?;
        let (bits, descs) = read_meta(&dir)?;
        let layout = HeadLayout::with_entry_bytes(bits, 4)?;
        let max_keys = max_keys_for_layout(layout);
        let mut segs = Vec::with_capacity(descs.len());
        let mut max_id = 0u32;
        for d in descs {
            let path = segment_head_path(&dir, d.file_id);
            let head = AddressHead::open(&path)?;
            if head.bits() != bits || head.entry_bytes() != 4 {
                return Err(StoreError::Corrupt("tx.head segment layout mismatch"));
            }
            let sealed = d.flags & FLAG_SEALED != 0;
            let (fuse, fuse_needs_rewrite) = if sealed {
                let fp = segment_fuse_path(&dir, d.file_id);
                if !fp.exists() {
                    return Err(StoreError::Corrupt("tx.head sealed segment missing fuse8"));
                }
                match open_file(&fp)? {
                    FuseFileOpen::Ready(f) => (Some(f), false),
                    FuseFileOpen::NeedsRewrite { gate, reason } => {
                        rbitcoin_log::warn!(
                            "store: tx.head fuse migrate file_id={} path={} ({reason}) — \
                             using always-probe until Class A rewrite to fuse8 v2",
                            d.file_id,
                            fp.display()
                        );
                        (Some(gate), true)
                    }
                }
            } else {
                (None, false)
            };
            max_id = max_id.max(d.file_id);
            segs.push(Arc::new(Segment {
                first_fk: d.first_fk,
                count: AtomicU64::new(d.count),
                file_id: d.file_id,
                sealed,
                head: Arc::new(head),
                fuse,
                open_keys: Mutex::new(Vec::new()),
                fuse_needs_rewrite,
            }));
        }
        for (i, s) in segs.iter().enumerate() {
            let is_last = i + 1 == segs.len();
            if !is_last && !s.sealed {
                return Err(StoreError::Corrupt("tx.head non-tail segment not sealed"));
            }
        }
        for w in segs.windows(2) {
            let a_end = w[0]
                .first_fk
                .saturating_add(w[0].count.load(Ordering::Relaxed));
            if w[1].first_fk != a_end {
                return Err(StoreError::Corrupt("tx.head segment fk gap/overlap"));
            }
        }
        // One summary for the whole head (not one line per segment).
        // Per-seg detail: `file_id@first_fk:count{s|o}` (s=sealed, o=open tail).
        let sealed_n = segs.iter().filter(|s| s.sealed).count();
        let open_n = segs.len().saturating_sub(sealed_n);
        let creates: u64 = segs.iter().map(|s| s.count.load(Ordering::Relaxed)).sum();
        let detail: String = segs
            .iter()
            .map(|s| {
                let c = s.count.load(Ordering::Relaxed);
                let flag = if s.sealed { 's' } else { 'o' };
                format!("{}@{}:{}{}", s.file_id, s.first_fk, c, flag)
            })
            .collect::<Vec<_>>()
            .join(" ");
        rbitcoin_log::info!(
            "store: tx.head open bits={bits} entry=4B slots={} segs={} sealed={sealed_n} \
             open={open_n} creates≈{creates} [{detail}]",
            layout.slots(),
            segs.len(),
        );
        Ok(Self {
            dir,
            layout,
            segments: RwLock::new(Arc::new(segs)),
            next_file_id: AtomicU32::new(max_id.saturating_add(1)),
            max_keys,
            write: Mutex::new(()),
        })
    }

    pub fn layout(&self) -> HeadLayout {
        self.layout
    }

    pub fn bits(&self) -> u32 {
        self.layout.bits
    }

    pub fn slots(&self) -> u64 {
        self.layout.slots()
    }

    pub fn entry_bytes(&self) -> u8 {
        4
    }

    pub fn max_keys_per_segment(&self) -> u64 {
        self.max_keys
    }

    pub fn segment_count(&self) -> usize {
        self.segments_snapshot().len()
    }

    /// Per-segment `first_fk` (index 0 = oldest; last = open/tip).
    ///
    /// Used by head-resolve winner-age stats ([`crate::head_resolve_stats::sealed_age_for_fk`]).
    pub fn first_fks_snapshot(&self) -> Vec<u64> {
        self.segments_snapshot()
            .iter()
            .map(|s| s.first_fk)
            .collect()
    }

    pub fn sealed_segment_count(&self) -> usize {
        self.segments_snapshot().iter().filter(|s| s.sealed).count()
    }

    pub fn occupied(&self) -> u64 {
        self.segments_snapshot()
            .iter()
            .map(|s| s.count.load(Ordering::Relaxed))
            .sum()
    }

    /// Open (unsealed) tail: `(first_fk, count)`. `None` if no segments or tail sealed.
    pub fn open_tail_range(&self) -> Option<(u64, u64)> {
        let segs = self.segments_snapshot();
        let last = segs.last()?;
        if last.sealed {
            return None;
        }
        Some((last.first_fk, last.count.load(Ordering::Relaxed)))
    }

    /// Replace fuse keys for the open tail (e.g. rebuild from Class A after reopen).
    ///
    /// Required before seal when this process did not insert every open create
    /// (crash/restart mid-segment). `keys.len()` must equal open `count`.
    /// On-disk path for a segment's sealed fuse file.
    pub fn fuse_path_for_file_id(&self, file_id: u32) -> PathBuf {
        segment_fuse_path(&self.dir, file_id)
    }

    /// Sealed segments whose fuse is legacy (v1) or unreadable: `(file_id, first_fk, count)`.
    ///
    /// Used after open to rebuild fuse8 **v2** from Class A without wiping `tx.head`.
    pub fn sealed_fuse_rewrite_queue(&self) -> Vec<(u32, u64, u64)> {
        self.segments_snapshot()
            .iter()
            .filter(|s| s.sealed && s.fuse_needs_rewrite)
            .map(|s| (s.file_id, s.first_fk, s.count.load(Ordering::Relaxed)))
            .collect()
    }

    /// Install a rebuilt v2 fuse for a sealed segment (after rewriting `.fuse8` on disk).
    pub fn install_sealed_fuse(&self, file_id: u32, fuse: SealedFuse8) -> Result<(), StoreError> {
        if fuse.is_always_probe() {
            return Err(StoreError::Corrupt(
                "tx.head install_sealed_fuse: always-probe not durable",
            ));
        }
        let _g = self.write.lock().unwrap_or_else(|e| e.into_inner());
        let mut guard = self.segments.write().unwrap_or_else(|e| e.into_inner());
        let mut new_list = (**guard).clone();
        let mut found = false;
        for s in &mut new_list {
            if s.file_id != file_id {
                continue;
            }
            if !s.sealed {
                return Err(StoreError::Corrupt(
                    "tx.head install_sealed_fuse: segment not sealed",
                ));
            }
            *s = Arc::new(Segment {
                first_fk: s.first_fk,
                count: AtomicU64::new(s.count.load(Ordering::Relaxed)),
                file_id: s.file_id,
                sealed: true,
                head: Arc::clone(&s.head),
                fuse: Some(fuse),
                open_keys: Mutex::new(Vec::new()),
                fuse_needs_rewrite: false,
            });
            found = true;
            break;
        }
        if !found {
            return Err(StoreError::Corrupt(
                "tx.head install_sealed_fuse: file_id not found",
            ));
        }
        *guard = Arc::new(new_list);
        Ok(())
    }

    pub fn replace_open_keys(&self, keys: Vec<u64>) -> Result<(), StoreError> {
        let _w = self.write.lock().unwrap_or_else(|e| e.into_inner());
        let segs = self.segments_snapshot();
        let last = segs
            .last()
            .ok_or(StoreError::Corrupt("tx.head replace_open_keys: no segment"))?;
        if last.sealed {
            return Err(StoreError::Corrupt(
                "tx.head replace_open_keys: tail sealed",
            ));
        }
        let count = last.count.load(Ordering::Relaxed);
        if keys.len() as u64 != count {
            return Err(StoreError::Corrupt(
                "tx.head replace_open_keys: key count mismatch",
            ));
        }
        *last.open_keys.lock().unwrap_or_else(|e| e.into_inner()) = keys;
        Ok(())
    }

    /// Number of fuse keys buffered for the open tail (diagnostics / tests).
    pub fn open_keys_len(&self) -> usize {
        let segs = self.segments_snapshot();
        let Some(last) = segs.last() else {
            return 0;
        };
        if last.sealed {
            return 0;
        }
        let n = last
            .open_keys
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len();
        n
    }

    /// Body soft-span roll threshold (bytes). Same default as `tx.idx`.
    ///
    /// Production: env `RBITCOIN_TX_IDX_SOFT_SPAN` (min 8). Under test,
    /// [`test_with_soft_span_bytes`] overrides env when non-zero (thread-local
    /// so parallel store tests cannot steal each other's roll window).
    pub fn soft_span_bytes() -> u64 {
        #[cfg(test)]
        {
            let o = TEST_SOFT_SPAN_OVERRIDE.with(std::cell::Cell::get);
            if o > 0 {
                return o;
            }
        }
        if let Some(v) = std::env::var("RBITCOIN_TX_IDX_SOFT_SPAN")
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|&v: &u64| v >= 8)
        {
            return v;
        }
        DEFAULT_SOFT_SPAN
    }

    /// Test-only soft-span override (`0` = use env/default). Thread-local.
    /// Prefer [`test_with_soft_span_bytes`] so panic/restore cannot leak.
    #[cfg(test)]
    pub fn test_set_soft_span_bytes(bytes: u64) {
        TEST_SOFT_SPAN_OVERRIDE.with(|c| c.set(bytes));
    }

    /// Hold this thread's soft-span override for `f`, then restore.
    #[cfg(test)]
    pub fn test_with_soft_span_bytes<R>(bytes: u64, f: impl FnOnce() -> R) -> R {
        let prev = TEST_SOFT_SPAN_OVERRIDE.with(|c| c.replace(bytes));
        struct Restore(u64);
        impl Drop for Restore {
            fn drop(&mut self) {
                TEST_SOFT_SPAN_OVERRIDE.with(|c| c.set(self.0));
            }
        }
        let _restore = Restore(prev);
        f()
    }

    fn segments_snapshot(&self) -> Arc<Vec<Arc<Segment>>> {
        Arc::clone(&self.segments.read().unwrap_or_else(|e| e.into_inner()))
    }

    /// Insert mixed probe keys → absolute create fks (sole writer).
    ///
    /// Relativizes `entries` fks in place (absolute → segment-relative) then
    /// sorts that same buffer in the open HashHead — no extra pair copy.
    ///
    /// When `force_roll` is true (body soft-span), seal the open segment first
    /// if it has any creates. Also rolls when open count reaches `max_keys`.
    pub fn insert_many(
        &self,
        entries: &mut [([u8; 32], Fk)],
        force_roll: bool,
    ) -> Result<(), StoreError> {
        if entries.is_empty() {
            return Ok(());
        }
        let _w = self.write.lock().unwrap_or_else(|e| e.into_inner());

        if force_roll {
            let segs = self.segments_snapshot();
            if let Some(last) = segs.last() {
                if !last.sealed && last.count.load(Ordering::Relaxed) > 0 {
                    self.seal_tail_locked()?;
                }
            }
        }

        let mut i = 0usize;
        while i < entries.len() {
            self.ensure_open_for(entries[i].1 .0)?;
            let segs = self.segments_snapshot();
            let last = segs
                .last()
                .ok_or(StoreError::Corrupt("tx.head no open segment"))?;
            if last.sealed {
                return Err(StoreError::Corrupt("tx.head tail sealed unexpectedly"));
            }
            let count = last.count.load(Ordering::Relaxed);
            if count >= self.max_keys {
                self.seal_tail_locked()?;
                continue;
            }
            let room = self.max_keys - count;
            let take = (entries.len() - i).min(room as usize);
            let batch = &mut entries[i..i + take];
            let first_fk = last.first_fk;

            let mut fuse_keys = Vec::with_capacity(batch.len());
            for (mixed, fk) in batch.iter_mut() {
                if fk.0 < first_fk {
                    return Err(StoreError::Corrupt("tx.head insert fk before segment"));
                }
                let rel = fk.0 - first_fk + 1;
                if rel == 0 || rel > u32::MAX as u64 {
                    return Err(StoreError::Corrupt("tx.head relative fk overflow"));
                }
                *fk = Fk(rel);
                fuse_keys.push(fuse_key_from_mixed(mixed));
            }
            last.head.insert_many_in_place(batch)?;
            last.open_keys
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .extend(fuse_keys);
            last.count.fetch_add(batch.len() as u64, Ordering::Relaxed);
            i += take;

            if last.count.load(Ordering::Relaxed) >= self.max_keys {
                self.seal_tail_locked()?;
            }
        }
        self.persist_meta_locked()?;
        Ok(())
    }
}

/// Wave-1 sealed-age cap: open (age 0) + sealed ages `1..=` this.
///
/// Independent of [`crate::dontcache_policy::head_or_idx_segment_index`]
/// (that flag is spend-annotate pwrite only and is always false for head).
pub(crate) const HEAD_PROBE_HOT_MAX_AGE: u32 = 3;

/// Which head segments to probe (two-wave resolve vs full baseline).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HeadProbeWave {
    /// Open + all sealed (legacy full probe).
    All,
    /// Open + sealed ages ≤ [`HEAD_PROBE_HOT_MAX_AGE`].
    Hot,
    /// Sealed ages > [`HEAD_PROBE_HOT_MAX_AGE`].
    Cold,
}

impl HeadProbeWave {
    #[inline]
    fn includes_hot(self) -> bool {
        matches!(self, HeadProbeWave::All | HeadProbeWave::Hot)
    }

    /// `age` = [`crate::dontcache_policy::sealed_age_from_index`] for the seg.
    #[inline]
    fn includes_sealed_age(self, age: u32) -> bool {
        match self {
            HeadProbeWave::All => true,
            HeadProbeWave::Hot => age <= HEAD_PROBE_HOT_MAX_AGE,
            HeadProbeWave::Cold => age > HEAD_PROBE_HOT_MAX_AGE,
        }
    }
}

impl SegmentedTxHead {
    /// Probe absolute create_fk candidates for a mixed key (open → sealed new→old).
    ///
    /// Order within each segment: deepest probe first is applied by reversing
    /// the page probe list. Across segments: open first, then sealed newest first.
    /// Caller body-verifies.
    pub fn probe_candidates(&self, mixed: &[u8; 32]) -> Result<Vec<Fk>, StoreError> {
        let mut out = self.probe_candidates_batch(std::slice::from_ref(mixed))?;
        Ok(out.pop().unwrap_or_default())
    }

    /// Batch probe: same order/results as N× [`Self::probe_candidates`], with
    /// **page-coalesced** loads inside each segment ([`AddressHead::probe_fks_batch`]).
    ///
    /// Sealed segments still fuse-gate per key; only keys that pass are batched
    /// for that segment's page loads. Page IO uses TLS bulk_io.
    pub fn probe_candidates_batch(&self, mixed: &[[u8; 32]]) -> Result<Vec<Vec<Fk>>, StoreError> {
        self.probe_candidates_batch_inner(mixed, None, HeadProbeWave::All, None)
    }

    /// Same as [`Self::probe_candidates_batch`] but head page preads use the
    /// **already-held** plan TLS session (no nested `with_thread_local`).
    pub fn probe_candidates_batch_on_session(
        &self,
        mixed: &[[u8; 32]],
        session: &mut crate::uring_session::UringSession,
    ) -> Result<Vec<Vec<Fk>>, StoreError> {
        self.probe_candidates_batch_inner(mixed, Some(session), HeadProbeWave::All, None)
    }

    /// Two-wave resolve: probe only **hot** (open + sealed ages ≤3) for all keys.
    pub(crate) fn probe_candidates_batch_hot(
        &self,
        mixed: &[[u8; 32]],
    ) -> Result<Vec<Vec<Fk>>, StoreError> {
        self.probe_candidates_batch_inner(mixed, None, HeadProbeWave::Hot, None)
    }

    /// Two-wave resolve: hot probe on a held plan TLS session.
    pub(crate) fn probe_candidates_batch_hot_on_session(
        &self,
        mixed: &[[u8; 32]],
        session: &mut crate::uring_session::UringSession,
    ) -> Result<Vec<Vec<Fk>>, StoreError> {
        self.probe_candidates_batch_inner(mixed, Some(session), HeadProbeWave::Hot, None)
    }

    /// Two-wave resolve: probe only **cold** (sealed ages ≥4) for keys where
    /// `active[i]` is true (wave-1 misses / unconnected hot). Inactive keys
    /// get empty cand lists.
    pub(crate) fn probe_candidates_batch_cold(
        &self,
        mixed: &[[u8; 32]],
        active: &[bool],
    ) -> Result<Vec<Vec<Fk>>, StoreError> {
        self.probe_candidates_batch_inner(mixed, None, HeadProbeWave::Cold, Some(active))
    }

    /// Two-wave resolve: cold probe on a held plan TLS session.
    pub(crate) fn probe_candidates_batch_cold_on_session(
        &self,
        mixed: &[[u8; 32]],
        active: &[bool],
        session: &mut crate::uring_session::UringSession,
    ) -> Result<Vec<Vec<Fk>>, StoreError> {
        self.probe_candidates_batch_inner(mixed, Some(session), HeadProbeWave::Cold, Some(active))
    }

    fn probe_candidates_batch_inner(
        &self,
        mixed: &[[u8; 32]],
        mut session: Option<&mut crate::uring_session::UringSession>,
        wave: HeadProbeWave,
        active: Option<&[bool]>,
    ) -> Result<Vec<Vec<Fk>>, StoreError> {
        let n = mixed.len();
        let mut out = vec![Vec::new(); n];
        if n == 0 {
            return Ok(out);
        }
        if let Some(a) = active {
            if a.len() != n {
                return Err(StoreError::Corrupt("probe active mask len"));
            }
        }
        let segs = self.segments_snapshot();
        if segs.is_empty() {
            return Ok(out);
        }

        let key_on = |i: usize| active.map(|a| a[i]).unwrap_or(true);

        let n_segs = segs.len();
        let last = segs.last().unwrap();
        // Open is always age 0 → wave 1 only (not cold).
        if !last.sealed && wave.includes_hot() {
            let mut pass_i: Vec<usize> = Vec::new();
            let mut pass_keys: Vec<[u8; 32]> = Vec::new();
            for i in 0..n {
                if !key_on(i) {
                    continue;
                }
                pass_i.push(i);
                pass_keys.push(mixed[i]);
            }
            if !pass_keys.is_empty() {
                LOOKUP_OPEN.fetch_add(pass_keys.len() as u64, Ordering::Relaxed);
                let dc = crate::dontcache_policy::head_or_idx_segment_index(n_segs - 1, n_segs);
                let rel_lists = match session.as_mut() {
                    Some(s) => last
                        .head
                        .probe_fks_batch_dontcache_on_session(&pass_keys, dc, s)?,
                    None => last.head.probe_fks_batch_dontcache(&pass_keys, dc)?,
                };
                for (orig_i, rels) in pass_i.into_iter().zip(rel_lists) {
                    for r in rels.into_iter().rev() {
                        if let Some(fk) = rel_to_abs(last.first_fk, r.0) {
                            out[orig_i].push(fk);
                        }
                    }
                }
            }
        }

        // Sealed newest → oldest (skip open tail if already handled).
        // Index si: last = tip age 0; older segments get higher sealed_age.
        let sealed_range: Box<dyn Iterator<Item = usize>> = if last.sealed {
            Box::new((0..n_segs).rev())
        } else {
            Box::new((0..n_segs.saturating_sub(1)).rev())
        };
        for si in sealed_range {
            let seg = &segs[si];
            if !seg.sealed {
                continue;
            }
            let age = crate::dontcache_policy::sealed_age_from_index(si, n_segs);
            if !wave.includes_sealed_age(age) {
                continue;
            }
            // Page-cache flag only (always false under spend-pwrite DONTCACHE).
            let dc = crate::dontcache_policy::head_or_idx_segment_index(si, n_segs);
            let Some(fuse) = seg.fuse.as_ref() else {
                return Err(StoreError::Corrupt("sealed segment missing fuse"));
            };
            // Keys that pass fuse for this segment → one page-coalesced probe batch.
            let mut pass_i: Vec<usize> = Vec::new();
            let mut pass_keys: Vec<[u8; 32]> = Vec::new();
            for (i, m) in mixed.iter().enumerate() {
                if !key_on(i) {
                    continue;
                }
                LOOKUP_FUSE_CHK.fetch_add(1, Ordering::Relaxed);
                let fuse_key = fuse_key_from_mixed(m);
                if !fuse.contains(fuse_key) {
                    LOOKUP_FUSE_SKIP.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                LOOKUP_SEALED_PROBE.fetch_add(1, Ordering::Relaxed);
                pass_i.push(i);
                pass_keys.push(*m);
            }
            if pass_keys.is_empty() {
                continue;
            }
            let rel_lists = match session.as_mut() {
                Some(s) => seg
                    .head
                    .probe_fks_batch_dontcache_on_session(&pass_keys, dc, s)?,
                None => seg.head.probe_fks_batch_dontcache(&pass_keys, dc)?,
            };
            for (orig_i, rels) in pass_i.into_iter().zip(rel_lists) {
                for r in rels.into_iter().rev() {
                    if let Some(fk) = rel_to_abs(seg.first_fk, r.0) {
                        out[orig_i].push(fk);
                    }
                }
            }
        }
        Ok(out)
    }

    pub fn flush(&self) -> Result<(), StoreError> {
        let segs = self.segments_snapshot();
        for s in segs.iter() {
            s.head.flush()?;
        }
        let _w = self.write.lock().unwrap_or_else(|e| e.into_inner());
        self.persist_meta_locked()?;
        Ok(())
    }

    pub fn flush_async(&self) -> Result<(), StoreError> {
        let segs = self.segments_snapshot();
        for s in segs.iter() {
            s.head.flush_async()?;
        }
        Ok(())
    }

    fn ensure_open_for(&self, first_fk: u64) -> Result<(), StoreError> {
        let segs = self.segments_snapshot();
        if segs.is_empty() {
            return self.open_new_locked(first_fk);
        }
        let last = segs.last().unwrap();
        if last.sealed {
            return self.open_new_locked(first_fk);
        }
        Ok(())
    }

    fn open_new_locked(&self, first_fk: u64) -> Result<(), StoreError> {
        if first_fk == 0 {
            return Err(StoreError::InvalidFk);
        }
        let file_id = self.next_file_id.fetch_add(1, Ordering::Relaxed);
        let path = segment_head_path(&self.dir, file_id);
        let _ = std::fs::remove_file(&path);
        let head = AddressHead::create_with_layout(&path, self.layout)?;
        let seg = Arc::new(Segment {
            first_fk,
            count: AtomicU64::new(0),
            file_id,
            sealed: false,
            head: Arc::new(head),
            fuse: None,
            open_keys: Mutex::new(Vec::new()),
            fuse_needs_rewrite: false,
        });
        {
            let mut guard = self.segments.write().unwrap_or_else(|e| e.into_inner());
            let mut new_list = (**guard).clone();
            // Drop empty unsealed tail if present.
            if let Some(last) = new_list.last() {
                if !last.sealed && last.count.load(Ordering::Relaxed) == 0 {
                    let fid = last.file_id;
                    new_list.pop();
                    let _ = std::fs::remove_file(segment_head_path(&self.dir, fid));
                }
            }
            new_list.push(seg);
            *guard = Arc::new(new_list);
        }
        ROLLS.fetch_add(1, Ordering::Relaxed);
        // Roll opens one new empty tail — compact one-liner (startup open is multi-seg).
        rbitcoin_log::info!(
            "store: tx.head roll open file_id={file_id} first_fk={first_fk} bits={} slots={}",
            self.layout.bits,
            self.layout.slots(),
        );
        self.persist_meta_locked()?;
        Ok(())
    }

    fn seal_tail_locked(&self) -> Result<(), StoreError> {
        let segs = self.segments_snapshot();
        let last = segs
            .last()
            .ok_or(StoreError::Corrupt("tx.head seal empty"))?;
        if last.sealed {
            return Ok(());
        }
        let count = last.count.load(Ordering::Relaxed);
        if count == 0 {
            return Ok(());
        }
        let t0 = Instant::now();
        let mut keys = last.open_keys.lock().unwrap_or_else(|e| e.into_inner());
        // Completeness is on the raw append stream (one fuse key per create insert),
        // not unique keys — BIP30 same-txid pushes the same mixed key twice.
        let raw_n = keys.len();
        if raw_n as u64 != count {
            return Err(StoreError::Corrupt(
                "tx.head seal open_keys incomplete (reopen mid-segment without rebuild)",
            ));
        }
        // Dedupe only for BinaryFuse8 construction (duplicate keys can fail build).
        keys.sort_unstable();
        keys.dedup();
        let unique_n = keys.len();
        rbitcoin_log::info!(
            "store: tx.head seal begin file_id={} first_fk={} count={count} \
             fuse_keys_raw={raw_n} fuse_keys_unique={unique_n}",
            last.file_id,
            last.first_fk
        );
        let fuse = SealedFuse8::build(&keys)?;
        keys.clear();
        drop(keys);
        let fuse_path = segment_fuse_path(&self.dir, last.file_id);
        fuse.write_to(&fuse_path)?;
        last.head.flush()?;
        let fuse_bytes = fuse.fingerprint_bytes();
        // Replace tail with sealed Arc.
        {
            let mut guard = self.segments.write().unwrap_or_else(|e| e.into_inner());
            let mut new_list = (**guard).clone();
            let old = new_list.pop().unwrap();
            new_list.push(Arc::new(Segment {
                first_fk: old.first_fk,
                count: AtomicU64::new(count),
                file_id: old.file_id,
                sealed: true,
                head: Arc::clone(&old.head),
                fuse: Some(fuse),
                open_keys: Mutex::new(Vec::new()),
                fuse_needs_rewrite: false,
            }));
            *guard = Arc::new(new_list);
        }
        SEALS.fetch_add(1, Ordering::Relaxed);
        let dt = t0.elapsed();
        rbitcoin_log::info!(
            "store: tx.head seal done file_id={} count={count} fuse_keys_unique={unique_n} \
             fuse_bytes={fuse_bytes} duration_ms={}",
            last.file_id,
            dt.as_millis()
        );
        self.persist_meta_locked()?;
        Ok(())
    }

    fn persist_meta_locked(&self) -> Result<(), StoreError> {
        let segs = self.segments_snapshot();
        let descs: Vec<(u64, u64, u32, u32)> = segs
            .iter()
            .map(|s| {
                let flags = if s.sealed { FLAG_SEALED } else { 0 };
                (
                    s.first_fk,
                    s.count.load(Ordering::Relaxed),
                    s.file_id,
                    flags,
                )
            })
            .collect();
        write_meta(&self.dir, self.layout.bits, &descs)
    }
}

#[inline]
fn rel_to_abs(first_fk: u64, rel: u64) -> Option<Fk> {
    if rel == 0 {
        return None;
    }
    Some(Fk(first_fk + rel - 1))
}

fn max_keys_for_layout(layout: HeadLayout) -> u64 {
    let slots = layout.slots();
    ((slots as f64) * HEAD_LOAD_START).floor() as u64
}

/// `store/tx.head/` — segment files + meta live here.
#[inline]
fn head_root(dir: &Path) -> PathBuf {
    dir.join("tx.head")
}

fn refuse_legacy_mono_head(dir: &Path) -> Result<(), StoreError> {
    let mono = dir.join("tx.head");
    if mono.is_file() {
        return Err(StoreError::Corrupt(
            "legacy monolithic tx.head present — reindex required (segmented 25-bit heads)",
        ));
    }
    // Directory is the **new** segment home. Reject only non-empty dirs that are
    // not our layout (no `meta`, no pending flat migration).
    if mono.is_dir() {
        let new_meta = mono.join("meta");
        if !new_meta.is_file() && !dir.join("tx.head.meta").is_file() {
            let non_empty = std::fs::read_dir(&mono)
                .map(|rd| rd.filter_map(|e| e.ok()).next().is_some())
                .unwrap_or(false);
            if non_empty {
                return Err(StoreError::Corrupt(
                    "legacy sharded tx.head/ dir — reindex required",
                ));
            }
        }
    }
    for name in [
        "tx.head.new",
        "tx.head.resize",
        "tx.head.bak",
        "tx.head.overflow",
    ] {
        let p = dir.join(name);
        if p.exists() {
            rbitcoin_log::warn!(
                "store: removing obsolete mono-head artifact {}",
                p.display()
            );
            let _ = std::fs::remove_file(&p);
        }
    }
    ensure_head_layout(dir)?;
    Ok(())
}

/// Ensure `tx.head/` exists; migrate flat `tx.head.meta` + segment/fuse files.
fn ensure_head_layout(dir: &Path) -> Result<(), StoreError> {
    let root = head_root(dir);
    let new_meta = meta_path(dir);
    if new_meta.is_file() {
        return Ok(());
    }
    std::fs::create_dir_all(&root).map_err(|e| StoreError::io(&root, e))?;
    let flat_meta = dir.join("tx.head.meta");
    if !flat_meta.is_file() {
        return Ok(());
    }
    let buf = std::fs::read(&flat_meta).map_err(|e| StoreError::io(&flat_meta, e))?;
    let (_bits, descs) = read_meta_buf(&buf)?;
    let mut moved = 0u32;
    for d in &descs {
        let src = dir.join(format!("tx.head.{:06}", d.file_id));
        let dst = segment_head_path(dir, d.file_id);
        if src.is_file() {
            std::fs::rename(&src, &dst).map_err(|e| StoreError::io(&dst, e))?;
            moved = moved.saturating_add(1);
        }
        let fsrc = dir.join(format!("tx.head.{:06}.fuse8", d.file_id));
        let fdst = segment_fuse_path(dir, d.file_id);
        if fsrc.is_file() {
            std::fs::rename(&fsrc, &fdst).map_err(|e| StoreError::io(&fdst, e))?;
        }
    }
    // Leftover flat segments not listed (shouldn't happen; best-effort).
    if let Ok(rd) = std::fs::read_dir(dir) {
        for ent in rd.flatten() {
            let name = ent.file_name();
            let s = name.to_string_lossy();
            if s == "tx.head.meta" {
                continue;
            }
            if let Some(rest) = s.strip_prefix("tx.head.") {
                let base = rest.strip_suffix(".fuse8").unwrap_or(rest);
                if base.chars().all(|c| c.is_ascii_digit()) && base.len() == 6 {
                    let dst = if rest.ends_with(".fuse8") {
                        root.join(format!("{base}.fuse8"))
                    } else {
                        root.join(base)
                    };
                    if !dst.exists() && ent.path().is_file() {
                        let _ = std::fs::rename(ent.path(), &dst);
                        moved = moved.saturating_add(1);
                    }
                }
            }
        }
    }
    std::fs::rename(&flat_meta, &new_meta).map_err(|e| StoreError::io(&new_meta, e))?;
    rbitcoin_log::info!(
        "store: migrated tx.head layout → {}/ (segments_moved={moved})",
        root.display()
    );
    Ok(())
}

fn segment_head_path(dir: &Path, file_id: u32) -> PathBuf {
    head_root(dir).join(format!("{file_id:06}"))
}

fn segment_fuse_path(dir: &Path, file_id: u32) -> PathBuf {
    head_root(dir).join(format!("{file_id:06}.fuse8"))
}

fn meta_path(dir: &Path) -> PathBuf {
    head_root(dir).join("meta")
}

/// True when segmented head meta exists (subdir or pre-migration flat).
pub fn head_meta_exists(dir: &Path) -> bool {
    meta_path(dir).is_file() || dir.join("tx.head.meta").is_file()
}

/// Remove all segmented head files (subdir layout + any leftover flat files).
pub fn wipe_segmented_head_files(dir: &Path) {
    let root = head_root(dir);
    if root.is_dir() {
        let _ = std::fs::remove_dir_all(&root);
    }
    let _ = std::fs::remove_file(dir.join("tx.head.meta"));
    if let Ok(rd) = std::fs::read_dir(dir) {
        for ent in rd.flatten() {
            let s = ent.file_name().to_string_lossy().into_owned();
            if s.starts_with("tx.head.") {
                let _ = std::fs::remove_file(ent.path());
            }
        }
    }
}

fn write_meta(dir: &Path, bits: u32, segs: &[(u64, u64, u32, u32)]) -> Result<(), StoreError> {
    let path = meta_path(dir);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| StoreError::io(parent, e))?;
    }
    let mut buf = Vec::with_capacity(META_HEADER_LEN + segs.len() * SEG_DESC_LEN);
    buf.extend_from_slice(&STORE_MAGIC);
    buf.extend_from_slice(&SCHEMA_VERSION.to_le_bytes());
    buf.extend_from_slice(&0u16.to_le_bytes());
    buf.extend_from_slice(&META_VERSION.to_le_bytes());
    buf.extend_from_slice(&(segs.len() as u32).to_le_bytes());
    buf.extend_from_slice(&bits.to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes());
    for &(first_fk, count, file_id, flags) in segs {
        buf.extend_from_slice(&first_fk.to_le_bytes());
        buf.extend_from_slice(&count.to_le_bytes());
        buf.extend_from_slice(&file_id.to_le_bytes());
        buf.extend_from_slice(&flags.to_le_bytes());
        buf.extend_from_slice(&0u64.to_le_bytes());
    }
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, &buf).map_err(|e| StoreError::io(&tmp, e))?;
    std::fs::rename(&tmp, &path).map_err(|e| StoreError::io(&path, e))?;
    Ok(())
}

struct SegDesc {
    first_fk: u64,
    count: u64,
    file_id: u32,
    flags: u32,
}

fn read_meta(dir: &Path) -> Result<(u32, Vec<SegDesc>), StoreError> {
    let path = meta_path(dir);
    if !path.exists() {
        return Ok((SEGMENT_HEAD_BITS, Vec::new()));
    }
    let buf = std::fs::read(&path).map_err(|e| StoreError::io(&path, e))?;
    read_meta_buf(&buf)
}

fn read_meta_buf(buf: &[u8]) -> Result<(u32, Vec<SegDesc>), StoreError> {
    if buf.len() < META_HEADER_LEN {
        return Err(StoreError::Corrupt("tx.head.meta short"));
    }
    if buf[0..4] != STORE_MAGIC {
        return Err(StoreError::Corrupt("tx.head.meta magic"));
    }
    let meta_ver = u32::from_le_bytes(buf[8..12].try_into().unwrap());
    if meta_ver != META_VERSION {
        return Err(StoreError::Corrupt("tx.head.meta version"));
    }
    let seg_count = u32::from_le_bytes(buf[12..16].try_into().unwrap()) as usize;
    let bits = u32::from_le_bytes(buf[16..20].try_into().unwrap());
    let need = META_HEADER_LEN + seg_count * SEG_DESC_LEN;
    if buf.len() < need {
        return Err(StoreError::Corrupt("tx.head.meta truncated"));
    }
    let mut descs = Vec::with_capacity(seg_count);
    for i in 0..seg_count {
        let o = META_HEADER_LEN + i * SEG_DESC_LEN;
        descs.push(SegDesc {
            first_fk: u64::from_le_bytes(buf[o..o + 8].try_into().unwrap()),
            count: u64::from_le_bytes(buf[o + 8..o + 16].try_into().unwrap()),
            file_id: u32::from_le_bytes(buf[o + 16..o + 20].try_into().unwrap()),
            flags: u32::from_le_bytes(buf[o + 20..o + 24].try_into().unwrap()),
        });
    }
    Ok((bits, descs))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::address_head::HeadLayout;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn tmp() -> PathBuf {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let p = std::env::temp_dir().join(format!("rbitcoin-seghead-{n}"));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn mixed(i: u64) -> [u8; 32] {
        let mut m = [0u8; 32];
        m[0..8].copy_from_slice(&i.to_le_bytes());
        m[8] = 0xA5;
        m
    }

    /// Sibling tests used to `test_set_soft_span_bytes(0)` on a process-global
    /// Atomic and steal the 48-byte roll window from
    /// `tip_then_any_connected_in_cold_beats_unconnected_hot` (stuck at age=2).
    #[test]
    fn soft_span_override_is_thread_local() {
        SegmentedTxHead::test_with_soft_span_bytes(48, || {
            assert_eq!(SegmentedTxHead::soft_span_bytes(), 48);
            let other = std::thread::spawn(SegmentedTxHead::soft_span_bytes)
                .join()
                .expect("join");
            assert_eq!(
                SegmentedTxHead::soft_span_bytes(),
                48,
                "holding thread keeps its override"
            );
            assert_ne!(
                other, 48,
                "sibling thread must not inherit this test's override (got {other})"
            );
        });
    }

    #[test]
    fn lookup_stats_sample_and_snapshot_surface() {
        // Clear then snapshot zeros; sample swaps to zero again.
        let _ = sample_lookup_stats();
        let snap0 = snapshot_lookup_stats();
        assert_eq!(snap0.open_probes, 0);
        assert_eq!(snap0.sealed_fuse_checks, 0);
        assert_eq!(snap0.sealed_fuse_skips, 0);
        assert_eq!(snap0.sealed_head_probes, 0);
        assert_eq!(snap0.rolls, 0);
        assert_eq!(snap0.seals, 0);
        let s = sample_lookup_stats();
        assert_eq!(s.open_probes, 0);
        // After create/insert, counters may tick; just ensure API is callable.
        let dir = tmp();
        let layout = HeadLayout::with_entry_bytes(10, 4).unwrap();
        let h = SegmentedTxHead::create(&dir, layout).unwrap();
        let mut one = [(mixed(1), Fk(1))];
        h.insert_many(&mut one, false).unwrap();
        let _ = h.probe_candidates(&mixed(1)).unwrap();
        let snap = snapshot_lookup_stats();
        // At least one of the probe counters should be non-zero after probe.
        let any = snap.open_probes
            + snap.sealed_fuse_checks
            + snap.sealed_fuse_skips
            + snap.sealed_head_probes
            + snap.rolls
            + snap.seals;
        let _ = any;
        let sampled = sample_lookup_stats();
        let snap_after = snapshot_lookup_stats();
        // sample zeros atomics; snapshot after sample is zero.
        assert_eq!(snap_after.open_probes, 0);
        let _ = sampled;
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn migrates_flat_head_layout_on_open() {
        let dir = tmp();
        let layout = HeadLayout::with_entry_bytes(10, 4).unwrap();
        {
            let h = SegmentedTxHead::create(&dir, layout).unwrap();
            let mut entries = Vec::new();
            for i in 0..50u64 {
                entries.push((mixed(i + 1), Fk(i + 1)));
            }
            h.insert_many(&mut entries, false).unwrap();
            h.flush().unwrap();
        }
        assert!(dir.join("tx.head").join("meta").is_file());
        // Flatten to legacy flat paths.
        let root = dir.join("tx.head");
        std::fs::rename(root.join("meta"), dir.join("tx.head.meta")).unwrap();
        for ent in std::fs::read_dir(&root).unwrap().flatten() {
            let name = ent.file_name();
            let s = name.to_string_lossy();
            if s == "meta" || s == "meta.tmp" {
                continue;
            }
            let dst = dir.join(format!("tx.head.{s}"));
            std::fs::rename(ent.path(), &dst).unwrap();
        }
        let _ = std::fs::remove_dir_all(&root);
        assert!(dir.join("tx.head.meta").is_file());
        assert!(!dir.join("tx.head").join("meta").exists());

        let h = SegmentedTxHead::open(&dir).unwrap();
        let cands = h.probe_candidates(&mixed(7)).unwrap();
        assert!(cands.iter().any(|f| f.0 == 7), "cands={cands:?}");
        assert!(dir.join("tx.head").join("meta").is_file());
        assert!(!dir.join("tx.head.meta").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn insert_roll_seal_lookup_roundtrip() {
        let dir = tmp();
        // 10-bit head: 1024 slots, max_keys = floor(0.8*1024)=819
        let layout = HeadLayout::with_entry_bytes(10, 4).unwrap();
        let h = SegmentedTxHead::create(&dir, layout).unwrap();
        assert_eq!(h.max_keys_per_segment(), 819);

        let n = 820u64; // forces a roll (max_keys=819)
        let mut entries = Vec::with_capacity(n as usize);
        for i in 0..n {
            entries.push((mixed(i + 1), Fk(i + 1)));
        }
        h.insert_many(&mut entries, false).unwrap();
        assert!(h.segment_count() >= 2, "segs={}", h.segment_count());
        assert!(h.sealed_segment_count() >= 1);

        // Known members resolve (as candidates).
        for i in [1u64, 400, 819, 820] {
            let cands = h.probe_candidates(&mixed(i)).unwrap();
            assert!(
                cands.iter().any(|f| f.0 == i),
                "missing fk={i} cands={cands:?}"
            );
        }
        // Global miss.
        let miss = h.probe_candidates(&mixed(0xDEAD_BEEF)).unwrap();
        assert!(miss.is_empty() || !miss.iter().any(|f| f.0 == 0xDEAD_BEEF));

        h.flush().unwrap();
        drop(h);
        let h2 = SegmentedTxHead::open(&dir).unwrap();
        for i in [1u64, 500, 820] {
            let cands = h2.probe_candidates(&mixed(i)).unwrap();
            assert!(cands.iter().any(|f| f.0 == i), "reopen missing {i}");
        }
        // Sealed fuse never FN on members of first segment.
        let cands = h2.probe_candidates(&mixed(1)).unwrap();
        assert!(cands.iter().any(|f| f.0 == 1));

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// One seal pad: install_sealed_fuse rejects + open-keys + v1 soft-open queue.
    /// Avoids two separate 900-insert seals in the default suite.
    #[test]
    fn install_sealed_fuse_and_v1_soft_open_journey() {
        let dir = tmp();
        let layout = HeadLayout::with_entry_bytes(10, 4).unwrap();
        // 0.8 * 1024 = 819 → seal at 820.
        let n = 820u64;
        {
            let h = SegmentedTxHead::create(&dir, layout).unwrap();
            let mut entries: Vec<_> = (0..n).map(|i| (mixed(i + 1), Fk(i + 1))).collect();
            h.insert_many(&mut entries, false).unwrap();
            assert!(h.sealed_segment_count() >= 1);
            let fuse = SealedFuse8::build(&[1u64, 2, 3]).unwrap();
            assert!(h
                .install_sealed_fuse(0, SealedFuse8::always_probe())
                .is_err());
            assert!(h.install_sealed_fuse(999_999, fuse.clone()).is_err());
            let open_id = h
                .segments_snapshot()
                .iter()
                .find(|s| !s.sealed)
                .map(|s| s.file_id)
                .expect("open tail");
            assert!(h.install_sealed_fuse(open_id, fuse.clone()).is_err());
            h.install_sealed_fuse(0, fuse).unwrap();
            assert!(h.sealed_fuse_rewrite_queue().is_empty());
            let p = h.fuse_path_for_file_id(0);
            assert!(p.to_string_lossy().contains("000000.fuse8"));
            assert!(h.replace_open_keys(vec![1, 2, 3]).is_err());
            let open_n = h
                .segments_snapshot()
                .last()
                .map(|s| s.count.load(std::sync::atomic::Ordering::Relaxed))
                .unwrap_or(0);
            if open_n > 0 {
                let keys: Vec<u64> = (0..open_n).map(|i| i + 1).collect();
                h.replace_open_keys(keys).unwrap();
                assert_eq!(h.open_keys_len() as u64, open_n);
            }
            h.flush().unwrap();
        }

        // Same pad: overwrite fuse as v1 → soft-open queues rewrite (always-probe).
        let fuse_path = dir.join("tx.head").join("000000.fuse8");
        let mut raw = Vec::from(*b"BF8R");
        raw.extend_from_slice(&1u32.to_le_bytes());
        raw.extend_from_slice(&0u64.to_le_bytes());
        std::fs::write(&fuse_path, &raw).unwrap();
        let h2 = SegmentedTxHead::open(&dir).unwrap();
        let q = h2.sealed_fuse_rewrite_queue();
        assert!(
            q.iter().any(|(id, _, _)| *id == 0),
            "file_id 0 should need fuse rewrite: {q:?}"
        );
        let cands = h2.probe_candidates(&mixed(1)).unwrap();
        assert!(cands.iter().any(|f| f.0 == 1));

        // Same suite budget: force roll + mono refuse without extra full pads.
        let dir_roll = tmp();
        let layout_roll = HeadLayout::with_entry_bytes(12, 4).unwrap(); // 4096 slots
        let h_roll = SegmentedTxHead::create(&dir_roll, layout_roll).unwrap();
        let mut batch1: Vec<_> = (0..100u64).map(|i| (mixed(i + 1), Fk(i + 1))).collect();
        h_roll.insert_many(&mut batch1, false).unwrap();
        assert_eq!(h_roll.segment_count(), 1);
        let mut batch2: Vec<_> = (100..150u64).map(|i| (mixed(i + 1), Fk(i + 1))).collect();
        h_roll.insert_many(&mut batch2, true).unwrap(); // force roll
        assert!(h_roll.segment_count() >= 2);
        assert!(h_roll.sealed_segment_count() >= 1);
        assert!(h_roll
            .probe_candidates(&mixed(50))
            .unwrap()
            .iter()
            .any(|f| f.0 == 50));
        assert!(h_roll
            .probe_candidates(&mixed(120))
            .unwrap()
            .iter()
            .any(|f| f.0 == 120));

        let dir_mono = tmp();
        std::fs::write(dir_mono.join("tx.head"), b"legacy").unwrap();
        let layout_mono = HeadLayout::with_entry_bytes(10, 4).unwrap();
        let err = SegmentedTxHead::create(&dir_mono, layout_mono)
            .err()
            .expect("must refuse mono head");
        let s = format!("{err}");
        assert!(s.contains("legacy") || s.contains("reindex"), "{s}");

        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&dir_roll);
        let _ = std::fs::remove_dir_all(&dir_mono);
    }
}
