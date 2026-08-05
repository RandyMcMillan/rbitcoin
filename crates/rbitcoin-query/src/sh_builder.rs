//! Direct-IBD scripthash builder: **large append-only spills + SEAL + tip fan-in**.
//!
//! Confirm enqueues thin creates (no open-hash RMW). A background worker spills
//! sorted runs and coalesces to a target size (bounded local k-way only). There
//! is **no** mid-IBD global pair-merge.
//!
//! Durability: cataloged runs + `SEAL` (`max_create_fk`). Memtable is RAM-only;
//! resume re-collects Class A for `(SEAL, tip]`.
//!
//! Tip: claim runs → `reduce_runs_to_fanin` → stream into durable SH bulk load.

use super::run_builder_core::{
    clear_runs_dir, finalize_wait_join, memtable_cap, on_disk_run_count, runs_dir_io, spawn_worker,
    RunControl, RunMemtable, FAMILY_SH, AFTER_WORK, IDLE_POLL,
};
use rbitcoin_log::{debug, info, warn};
use rbitcoin_primitives::Fk;
use rbitcoin_store::{
    claim_run_for_materialize, commit_fanin_reduce_and_drop_inputs, for_each_merged_rec_opts,
    list_fanin_reduce_outputs, list_materialize_claims, list_runs, merge_runs, next_run_path,
    reduce_runs_to_fanin, write_sorted_run, ScriptHashEntry,
    ScriptHashRecord, Store, StoreError, SortedRunPath,
};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// Linear dedup is fine for short chains; switch to a set past this length.
const CHAIN_SET_THRESHOLD: usize = 16;

/// Fixed run record: scripthash[32] | create_tx_fk:u64 = 40 bytes (no vout).
pub const SH_RUN_REC_LEN: u32 = 40;
pub const SH_RUN_KEY_LEN: u32 = 32;

const DEFAULT_MEMTABLE_CAP: usize = 1_000_000;
const HARD_MEMTABLE_MUL: usize = 2;
/// Coalesce L0 spills until a cataloged run is about this large.
const DEFAULT_TARGET_RUN_BYTES: u64 = 256 * 1024 * 1024;
/// Max open runs in any k-way pass (tip + L0 coalesce).
const DEFAULT_MERGE_FANIN: usize = 32;

#[inline]
fn run_body_bytes(run: &SortedRunPath) -> u64 {
    run.count.saturating_mul(u64::from(run.rec_len))
}

fn target_run_bytes() -> u64 {
    std::env::var("RBITCOIN_SH_TARGET_RUN_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_TARGET_RUN_BYTES)
        .max(u64::from(SH_RUN_REC_LEN) * 1024)
}

fn merge_fanin() -> usize {
    std::env::var("RBITCOIN_SH_MERGE_FANIN")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_MERGE_FANIN)
        .clamp(8, 64)
}

fn encode_rec(sh: &[u8; 32], tx_fk: Fk) -> [u8; SH_RUN_REC_LEN as usize] {
    let mut r = [0u8; SH_RUN_REC_LEN as usize];
    r[0..32].copy_from_slice(sh);
    r[32..40].copy_from_slice(&tx_fk.0.to_le_bytes());
    r
}

#[inline(always)]
fn decode_rec_fixed(buf: &[u8]) -> ([u8; 32], Fk) {
    debug_assert!(buf.len() >= SH_RUN_REC_LEN as usize);
    let sh: [u8; 32] = buf[0..32].try_into().unwrap();
    let tx_fk = Fk(u64::from_le_bytes(buf[32..40].try_into().unwrap()));
    (sh, tx_fk)
}

#[inline]
fn chain_has_fk(chain: &[ScriptHashEntry], fk: Fk) -> bool {
    chain.iter().any(|e| e.create_tx_fk == fk)
}

// ── SEAL (max durable create_fk in cataloged runs) ───────────────────────────

fn seal_path(runs_dir: &Path) -> PathBuf {
    runs_dir.join("SEAL")
}

/// Load sealed max create_fk (0 if missing/corrupt).
pub fn load_seal(runs_dir: &Path) -> u64 {
    let path = seal_path(runs_dir);
    let Ok(buf) = std::fs::read(&path) else {
        return 0;
    };
    if buf.len() < 8 {
        return 0;
    }
    u64::from_le_bytes(buf[0..8].try_into().unwrap_or([0; 8]))
}

fn store_seal(runs_dir: &Path, max_fk: u64) -> Result<(), StoreError> {
    let path = seal_path(runs_dir);
    let tmp = runs_dir.join("SEAL.tmp");
    std::fs::create_dir_all(runs_dir).map_err(|e| StoreError::io(runs_dir, e))?;
    std::fs::write(&tmp, max_fk.to_le_bytes()).map_err(|e| StoreError::io(&tmp, e))?;
    {
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(&tmp)
            .map_err(|e| StoreError::io(&tmp, e))?;
        f.sync_all().map_err(|e| StoreError::io(&tmp, e))?;
    }
    std::fs::rename(&tmp, &path).map_err(|e| StoreError::io(&path, e))?;
    Ok(())
}

fn bump_seal(runs_dir: &Path, max_fk: u64) -> Result<(), StoreError> {
    if max_fk == 0 {
        return Ok(());
    }
    let cur = load_seal(runs_dir);
    if max_fk > cur {
        store_seal(runs_dir, max_fk)?;
    }
    Ok(())
}

fn max_fk_in_body(body: &[u8]) -> u64 {
    let rec = SH_RUN_REC_LEN as usize;
    let mut max = 0u64;
    let mut i = 0;
    while i + rec <= body.len() {
        let fk = u64::from_le_bytes(body[i + 32..i + 40].try_into().unwrap());
        if fk > max {
            max = fk;
        }
        i += rec;
    }
    max
}

// ── Memtable / builder ───────────────────────────────────────────────────────

struct Inner {
    pending: Vec<([u8; 32], Fk)>,
    ctrl: RunControl,
    /// Uncataloged L0 spill paths awaiting coalesce (under runs_dir/l0/).
    l0: Vec<SortedRunPath>,
    l0_bytes: u64,
}

impl RunMemtable for Inner {
    fn pending_len(&self) -> usize {
        self.pending.len()
    }
    fn control(&self) -> &RunControl {
        &self.ctrl
    }
    fn control_mut(&mut self) -> &mut RunControl {
        &mut self.ctrl
    }
    fn flush_pending(&mut self) -> Result<u64, StoreError> {
        if self.pending.is_empty() {
            return Ok(0);
        }
        let mut recs = std::mem::take(&mut self.pending);
        recs.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1 .0.cmp(&b.1 .0)));
        recs.dedup_by(|a, b| a.0 == b.0 && a.1 == b.1);
        let mut body = Vec::with_capacity(recs.len() * SH_RUN_REC_LEN as usize);
        for (sh, fk) in &recs {
            body.extend_from_slice(&encode_rec(sh, *fk));
        }
        let l0_dir = self.ctrl.runs_dir.join("l0");
        std::fs::create_dir_all(&l0_dir).map_err(|e| StoreError::io(&l0_dir, e))?;
        let path = next_run_path(&l0_dir, self.ctrl.next_seq);
        self.ctrl.next_seq += 1;
        let _io = self.ctrl.runs_io.lock().unwrap();
        // Write without parent MANIFEST (l0 dir has no catalog); file only.
        let run = write_sorted_run(&path, SH_RUN_KEY_LEN, SH_RUN_REC_LEN, &body)?;
        // Detach from l0 MANIFEST pollution: list_runs on l0 may build MANIFEST —
        // we track l0 in RAM. Remove MANIFEST if write_sorted_run created one.
        let _ = std::fs::remove_file(l0_dir.join("MANIFEST"));
        self.l0_bytes = self.l0_bytes.saturating_add(run_body_bytes(&run));
        self.l0.push(run);
        Ok(recs.len() as u64)
    }
}

/// Shared Direct-IBD SH builder + low-prio worker.
pub struct ShRunBuilder {
    inner: Arc<Mutex<Inner>>,
    cv: Arc<Condvar>,
    enabled: AtomicBool,
    join: Mutex<Option<JoinHandle<()>>>,
    pub enqueued: AtomicU64,
    /// Process cache of SEAL (shared with worker).
    sealed_fk: Arc<AtomicU64>,
    runs_dir: PathBuf,
}

impl ShRunBuilder {
    pub fn new(store_dir: &Path) -> Self {
        let ctrl = RunControl::open(store_dir, "scripthash.runs");
        let runs_dir = ctrl.runs_dir.clone();
        let sealed = load_seal(&runs_dir);
        Self {
            inner: Arc::new(Mutex::new(Inner {
                pending: Vec::new(),
                ctrl,
                l0: Vec::new(),
                l0_bytes: 0,
            })),
            cv: Arc::new(Condvar::new()),
            enabled: AtomicBool::new(false),
            join: Mutex::new(None),
            enqueued: AtomicU64::new(0),
            sealed_fk: Arc::new(AtomicU64::new(sealed)),
            runs_dir,
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Max create_fk present in durable cataloged runs (SEAL).
    pub fn sealed_max_create_fk(&self) -> u64 {
        self.sealed_fk.load(Ordering::Acquire)
    }

    pub fn enable(&self) {
        {
            let mut g = self.inner.lock().unwrap();
            g.ctrl.reset_for_enable();
        }
        let sealed = load_seal(&self.runs_dir);
        self.sealed_fk.store(sealed, Ordering::Release);
        let inner_w = Arc::clone(&self.inner);
        let cv_w = Arc::clone(&self.cv);
        let runs_dir = self.runs_dir.clone();
        let sealed_w = Arc::clone(&self.sealed_fk);

        spawn_worker(
            "ibd-sh-index",
            || {
                info!(
                    "ibd: scripthash catch-up mode ON (memtable→target-sized runs+SEAL; bulk materialize at tip)"
                );
            },
            &self.enabled,
            &self.join,
            move || {
                debug!("ibd: SH run worker started (idle IO prio, spill+coalesce)");
                sh_worker_loop(
                    memtable_cap("RBITCOIN_SH_MEMTABLE_CAP", DEFAULT_MEMTABLE_CAP),
                    inner_w,
                    cv_w,
                    runs_dir,
                    sealed_w,
                );
                debug!("ibd: SH run worker stopped");
            },
        );
    }

    pub fn on_disk_run_count(&self) -> usize {
        let g = self.inner.lock().unwrap();
        let (dir, io) = runs_dir_io(&g.ctrl);
        drop(g);
        on_disk_run_count(&dir, &io)
    }

    pub fn memtable_len(&self) -> usize {
        self.inner.lock().unwrap().pending.len()
    }

    pub fn enqueue(&self, creates: &[ScriptHashRecord]) {
        if !self.is_enabled() || creates.is_empty() {
            return;
        }
        let cap = memtable_cap("RBITCOIN_SH_MEMTABLE_CAP", DEFAULT_MEMTABLE_CAP);
        let hard = cap.saturating_mul(HARD_MEMTABLE_MUL);
        let sealed = self.sealed_max_create_fk();
        let mut g = self.inner.lock().unwrap();
        for rec in creates {
            if rec.create_tx_fk.is_null() {
                continue;
            }
            // Already durable in a sealed run — skip.
            if rec.create_tx_fk.0 <= sealed {
                continue;
            }
            while g.pending.len() >= hard && !g.ctrl.stop {
                self.cv.notify_all();
                g = self
                    .cv
                    .wait_timeout(g, Duration::from_millis(50))
                    .unwrap()
                    .0;
            }
            g.pending.push((rec.scripthash, rec.create_tx_fk));
            self.enqueued.fetch_add(1, Ordering::Relaxed);
        }
        if g.pending.len() >= cap {
            self.cv.notify_all();
        }
    }

    /// Reload SEAL from disk into process cache (after worker coalesce / resume).
    pub fn refresh_seal(&self) {
        let s = load_seal(&self.runs_dir);
        self.sealed_fk.store(s, Ordering::Release);
    }

    /// Force flush memtable + L0 coalesce (tests / resume).
    pub fn drain_spills(&self) -> Result<(), StoreError> {
        let mut g = self.inner.lock().unwrap();
        if !g.pending.is_empty() {
            g.flush_pending()?;
        }
        let runs_dir = g.ctrl.runs_dir.clone();
        let runs_io = Arc::clone(&g.ctrl.runs_io);
        let mut next_seq = g.ctrl.next_seq;
        let l0 = std::mem::take(&mut g.l0);
        g.l0_bytes = 0;
        drop(g);
        if !l0.is_empty() {
            let _io = runs_io.lock().unwrap();
            coalesce_l0_to_catalog(&runs_dir, l0, &mut next_seq, &self.sealed_fk)?;
        }
        {
            let mut g = self.inner.lock().unwrap();
            g.ctrl.next_seq = next_seq;
        }
        self.refresh_seal();
        Ok(())
    }

    /// Flush memtable, coalesce, claim runs, fan-in reduce, cold bulk-load durable SH.
    pub fn finalize_and_bulk_materialize(&self, store: &Store) -> Result<u64, StoreError> {
        finalize_wait_join(&self.enabled, &self.inner, &self.cv, &self.join)?;
        // Drain any leftover pending + L0 (worker may have stopped with L0 in RAM).
        self.drain_spills()?;

        let (runs_dir, runs_io) = {
            let g = self.inner.lock().unwrap();
            (g.ctrl.runs_dir.clone(), Arc::clone(&g.ctrl.runs_io))
        };

        let merge_dir = runs_dir.join("merge");

        let t_claim = Instant::now();
        let mut claimed: Vec<SortedRunPath> = Vec::new();
        let mut stream_inputs: Vec<SortedRunPath> = Vec::new();
        let mut resumed_from_reduce = false;
        {
            let _io = runs_io.lock().unwrap();
            let mut prior = list_materialize_claims(&runs_dir)?;
            let mut runs = list_runs(&runs_dir)?;
            runs.sort_by_key(|r| r.seq().unwrap_or(u64::MAX));

            // Resume: fan-in finished and claimed inputs already dropped.
            if prior.is_empty() && runs.is_empty() {
                if let Some(reduced) = list_fanin_reduce_outputs(&merge_dir)? {
                    info!(
                        "node: scripthash resuming fan-in reduce outputs ({}) under merge/",
                        reduced.len()
                    );
                    stream_inputs = reduced;
                    resumed_from_reduce = true;
                }
            }

            if !resumed_from_reduce {
                // Incomplete prior reduce (no READY) — discard partial work files.
                let _ = std::fs::remove_dir_all(&merge_dir);
                if !prior.is_empty() {
                    info!(
                        "node: scripthash resuming {} incomplete materialize claim(s)",
                        prior.len()
                    );
                }
                claimed.append(&mut prior);
                for run in runs {
                    claimed.push(claim_run_for_materialize(&run)?);
                }
            }
        }
        let claim_ns = t_claim.elapsed().as_nanos() as u64;

        if !resumed_from_reduce && claimed.is_empty() {
            info!("node: scripthash bulk materialize: no runs");
            clear_runs_dir(&runs_dir);
            // Keep SEAL if any; tip may still re-collect.
            return Ok(0);
        }

        let fanin = merge_fanin();
        let t_reduce = Instant::now();
        if !resumed_from_reduce {
            stream_inputs = {
                let _io = runs_io.lock().unwrap();
                let out = reduce_runs_to_fanin(&claimed, &merge_dir, fanin)?;
                // Free claimed `*.run.mat` / catalog inputs now fully folded into
                // merge/ (READY marker enables crash resume without re-claims).
                commit_fanin_reduce_and_drop_inputs(&merge_dir, &claimed, &out)?;
                out
            };
            info!(
                "node: scripthash tip fanin reduce claimed={} stream={} fanin={fanin} elapsed={:?}",
                claimed.len(),
                stream_inputs.len(),
                t_reduce.elapsed()
            );
        }
        let reduce_ns = t_reduce.elapsed().as_nanos() as u64;

        if stream_inputs.is_empty() {
            info!("node: scripthash bulk materialize: no stream inputs after reduce");
            clear_runs_dir(&runs_dir);
            return Ok(0);
        }

        let total_recs: u64 = stream_inputs.iter().map(|r| r.count).sum();
        let n_existing = store.scripthash.entry_count();
        let head_empty = store.scripthash.head_is_empty();
        info!(
            "node: scripthash reinit empty for cold rematerialize \
             stream_runs={} entry_count={n_existing} head_empty={head_empty}",
            stream_inputs.len()
        );
        let t_reinit = Instant::now();
        store.scripthash.reinit_empty_for_cold_materialize()?;
        let reinit_ns = t_reinit.elapsed().as_nanos() as u64;
        debug_assert_eq!(store.scripthash.entry_count(), 0);
        debug_assert!(store.scripthash.head_is_empty());
        info!(
            "node: scripthash bulk materialize start runs={} records≈{total_recs} cold=true",
            stream_inputs.len()
        );
        let t0 = Instant::now();
        let n_shards = store.scripthash.head_shard_count();
        let mut session = store.scripthash.bulk_session(total_recs.max(1))?;
        let mut cur_key: Option<[u8; 32]> = None;
        let mut chain: Vec<ScriptHashEntry> = Vec::with_capacity(8);
        let mut long_seen: Option<HashSet<u64>> = None;
        let mut unique_in = 0u64;
        let mut last_log_keys = 0u64;
        let mut last_shards = 0u32;
        let mut max_fk_seen = 0u64;

        let t_stream = Instant::now();
        for_each_merged_rec_opts(&stream_inputs, false, |rec| {
            if rec.len() < SH_RUN_REC_LEN as usize {
                return Err(StoreError::Corrupt("sh run short record in merge stream"));
            }
            let (sh, tx_fk) = decode_rec_fixed(rec);
            if tx_fk.is_null() {
                return Ok(());
            }
            if tx_fk.0 > max_fk_seen {
                max_fk_seen = tx_fk.0;
            }
            if cur_key != Some(sh) {
                if let Some(prev) = cur_key {
                    if !chain.is_empty() {
                        unique_in = unique_in.saturating_add(1);
                        session.put_chain(prev, &chain)?;
                        chain.clear();
                        long_seen = None;
                        let keys = session.keys_written();
                        let shards = session.shards_flushed();
                        if keys == 1
                            || keys.saturating_sub(last_log_keys) >= 100_000
                            || shards > last_shards
                        {
                            last_log_keys = keys;
                            last_shards = shards;
                            info!(
                                "node: scripthash materialize progress keys≈{} creates≈{} shards={shards}/{n_shards} \
                                 body_flush={:?} head_fill={:?} elapsed={:?}",
                                keys,
                                session.creates_written(),
                                Duration::from_nanos(session.body_flush_ns),
                                Duration::from_nanos(session.head_fill_ns),
                                t0.elapsed()
                            );
                        }
                    }
                }
                cur_key = Some(sh);
            }
            let is_dup = if let Some(ref set) = long_seen {
                set.contains(&tx_fk.0)
            } else {
                chain_has_fk(&chain, tx_fk)
            };
            if !is_dup {
                chain.push(ScriptHashEntry::new(tx_fk));
                if let Some(ref mut set) = long_seen {
                    set.insert(tx_fk.0);
                } else if chain.len() >= CHAIN_SET_THRESHOLD {
                    let mut set = HashSet::with_capacity(chain.len() * 2);
                    for e in &chain {
                        set.insert(e.create_tx_fk.0);
                    }
                    long_seen = Some(set);
                }
            }
            Ok(())
        })?;
        if let Some(prev) = cur_key.take() {
            if !chain.is_empty() {
                unique_in = unique_in.saturating_add(1);
                session.put_chain(prev, &chain)?;
            }
        }
        let stream_ns = t_stream.elapsed().as_nanos() as u64;

        let t_finish = Instant::now();
        let (n_total, n_keys, body_flush_ns, head_fill_ns) = session.finish()?;
        store.scripthash.flush()?;
        let finish_ns = t_finish.elapsed().as_nanos() as u64;

        // Success barrier: drop any leftover claims, stream inputs, merge work, catalog.
        for run in &claimed {
            let _ = std::fs::remove_file(&run.path);
        }
        for run in &stream_inputs {
            let _ = std::fs::remove_file(&run.path);
        }
        let _ = std::fs::remove_dir_all(&merge_dir);
        clear_runs_dir(&runs_dir);
        if max_fk_seen > 0 {
            let _ = store_seal(&runs_dir, max_fk_seen);
            self.sealed_fk.store(max_fk_seen, Ordering::Release);
        }

        info!(
            "node: scripthash bulk materialize done creates≈{n_total} keys≈{n_keys} unique_in≈{unique_in} \
             shards={n_shards} elapsed={:?} \
             stages: claim={:?} reduce={:?} reinit={:?} stream={:?} body_flush={:?} head_fill={:?} finish_flush={:?}",
            t0.elapsed(),
            Duration::from_nanos(claim_ns),
            Duration::from_nanos(reduce_ns),
            Duration::from_nanos(reinit_ns),
            Duration::from_nanos(stream_ns),
            Duration::from_nanos(body_flush_ns),
            Duration::from_nanos(head_fill_ns),
            Duration::from_nanos(finish_ns),
        );
        let _ = FAMILY_SH;
        Ok(n_total)
    }
}

/// Coalesce L0 spills into cataloged runs under `runs_dir` MANIFEST.
fn coalesce_l0_to_catalog(
    runs_dir: &Path,
    mut l0: Vec<SortedRunPath>,
    next_seq: &mut u64,
    sealed: &AtomicU64,
) -> Result<(), StoreError> {
    if l0.is_empty() {
        return Ok(());
    }
    l0.sort_by_key(|r| r.seq().unwrap_or(u64::MAX));
    let fanin = merge_fanin();
    let target = target_run_bytes();
    let mut i = 0;
    while i < l0.len() {
        let mut chunk = Vec::new();
        let mut bytes = 0u64;
        while i < l0.len() && chunk.len() < fanin {
            let b = run_body_bytes(&l0[i]);
            if !chunk.is_empty() && bytes + b > target && bytes >= target / 4 {
                break;
            }
            bytes += b;
            chunk.push(l0[i].clone());
            i += 1;
            if bytes >= target {
                break;
            }
        }
        let mut max_fk = 0u64;
        for r in &chunk {
            if let Ok(body) = rbitcoin_store::read_run_body(r) {
                max_fk = max_fk.max(max_fk_in_body(&body));
            }
        }
        let out = next_run_path(runs_dir, *next_seq);
        *next_seq += 1;
        // merge_runs works for 1..N inputs: writes out, deletes inputs, MANIFEST.
        let _merged = merge_runs(&chunk, &out)?;
        if max_fk > 0 {
            bump_seal(runs_dir, max_fk)?;
            let cur = sealed.load(Ordering::Relaxed);
            if max_fk > cur {
                sealed.store(max_fk, Ordering::Release);
            }
        }
    }
    let l0_dir = runs_dir.join("l0");
    let _ = std::fs::remove_dir_all(&l0_dir);
    Ok(())
}

fn sh_worker_loop(
    soft_cap: usize,
    inner: Arc<Mutex<Inner>>,
    cv: Arc<Condvar>,
    runs_dir: PathBuf,
    sealed: Arc<AtomicU64>,
) {
    let target = target_run_bytes();
    let fanin = merge_fanin();
    loop {
        let mut g = inner.lock().unwrap();
        if g.ctrl.stop {
            break;
        }
        let need_flush = g.pending.len() >= soft_cap || (g.ctrl.finalize && !g.pending.is_empty());
        let need_coalesce = g.l0_bytes >= target
            || g.l0.len() >= fanin
            || (g.ctrl.finalize && !g.l0.is_empty());

        if need_flush {
            if !g.pending.is_empty() {
                if let Err(e) = g.flush_pending() {
                    warn!("ibd: SH run flush failed: {e}");
                }
                cv.notify_all();
            }
            drop(g);
            std::thread::sleep(AFTER_WORK);
            continue;
        }

        if need_coalesce {
            let l0 = std::mem::take(&mut g.l0);
            g.l0_bytes = 0;
            let runs_io = Arc::clone(&g.ctrl.runs_io);
            let mut next_seq = g.ctrl.next_seq;
            drop(g);
            {
                let _io = runs_io.lock().unwrap();
                if let Err(e) = coalesce_l0_to_catalog(&runs_dir, l0, &mut next_seq, &sealed) {
                    warn!("ibd: SH L0 coalesce failed: {e}");
                }
            }
            let mut g = inner.lock().unwrap();
            g.ctrl.next_seq = next_seq;
            drop(g);
            std::thread::sleep(AFTER_WORK);
            continue;
        }

        if g.ctrl.finalize && g.pending.is_empty() && g.l0.is_empty() {
            g.ctrl.stop = true;
            cv.notify_all();
            break;
        }

        let (gg, _) = cv.wait_timeout(g, IDLE_POLL).unwrap();
        g = gg;
        if g.ctrl.stop {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rbitcoin_store::Store;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn enqueue_flush_materialize() {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-sh-builder-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store = Store::open_or_create(&dir).unwrap();
        let b = ShRunBuilder::new(&dir);
        b.enable();
        let mut creates = Vec::new();
        for i in 0..100u32 {
            let mut sh = [0u8; 32];
            sh[0] = (i % 17) as u8;
            sh[1] = (i / 17) as u8;
            creates.push(ScriptHashRecord::from_fk(sh, Fk(i as u64 + 1)));
        }
        b.enqueue(&creates);
        let n = b.finalize_and_bulk_materialize(&store).unwrap();
        assert!(n >= 100, "inserted={n}");
        assert!(store.scripthash.entry_count() >= 100);
        assert!(b.sealed_max_create_fk() >= 100);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn seal_advances_on_spill() {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-sh-seal-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let b = ShRunBuilder::new(&dir);
        b.enable();
        let mut creates = Vec::new();
        for i in 1..=50u64 {
            let mut sh = [0u8; 32];
            sh[0] = i as u8;
            creates.push(ScriptHashRecord::from_fk(sh, Fk(i)));
        }
        b.enqueue(&creates);
        b.drain_spills().unwrap();
        assert!(
            b.sealed_max_create_fk() >= 50,
            "seal={}",
            b.sealed_max_create_fk()
        );
        // Re-enqueue same fks: filtered by seal.
        let before = b.enqueued.load(Ordering::Relaxed);
        b.enqueue(&creates);
        assert_eq!(b.enqueued.load(Ordering::Relaxed), before);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn fanin_many_runs_materialize() {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-sh-fanin-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        // Smaller than historical 40×10 but still multi-pass: fanin 4 → several reduce waves.
        std::env::set_var("RBITCOIN_SH_MERGE_FANIN", "4");
        let store = Store::open_or_create(&dir).unwrap();
        let b = ShRunBuilder::new(&dir);
        let runs_dir = dir.join("scripthash.runs");
        std::fs::create_dir_all(&runs_dir).unwrap();
        const N_RUNS: u64 = 16;
        const PER_RUN: u64 = 8;
        for seq in 1..=N_RUNS {
            let mut body = Vec::new();
            for j in 0..PER_RUN {
                let mut sh = [0u8; 32];
                sh[0] = seq as u8;
                sh[1] = j as u8;
                body.extend_from_slice(&encode_rec(&sh, Fk(seq * 100 + j + 1)));
            }
            let path = next_run_path(&runs_dir, seq);
            write_sorted_run(&path, SH_RUN_KEY_LEN, SH_RUN_REC_LEN, &body).unwrap();
        }
        let n = b.finalize_and_bulk_materialize(&store).unwrap();
        assert_eq!(n, N_RUNS * PER_RUN);
        assert_eq!(store.scripthash.entry_count(), N_RUNS * PER_RUN);
        std::env::remove_var("RBITCOIN_SH_MERGE_FANIN");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn materialize_reinits_nonempty_table_for_cold_reload() {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-sh-reinit-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store = Store::open_or_create(&dir).unwrap();
        let b = ShRunBuilder::new(&dir);
        let runs_dir = dir.join("scripthash.runs");
        std::fs::create_dir_all(&runs_dir).unwrap();

        let mut body = Vec::new();
        for i in 0..30u32 {
            let mut sh = [0u8; 32];
            sh[0] = i as u8;
            body.extend_from_slice(&encode_rec(&sh, Fk(i as u64 + 1)));
        }
        let path = next_run_path(&runs_dir, 1);
        write_sorted_run(&path, SH_RUN_KEY_LEN, SH_RUN_REC_LEN, &body).unwrap();
        let n1 = b.finalize_and_bulk_materialize(&store).unwrap();
        assert!(n1 >= 30);
        assert!(store.scripthash.entry_count() >= 30);

        let mut body2 = Vec::new();
        for i in 0..40u32 {
            let mut sh = [0u8; 32];
            sh[0] = (i + 100) as u8;
            body2.extend_from_slice(&encode_rec(&sh, Fk(i as u64 + 1000)));
        }
        let path2 = next_run_path(&runs_dir, 2);
        let run2 = write_sorted_run(&path2, SH_RUN_KEY_LEN, SH_RUN_REC_LEN, &body2).unwrap();
        let _ = claim_run_for_materialize(&run2).unwrap();
        assert!(store.scripthash.entry_count() > 0);

        let n2 = b.finalize_and_bulk_materialize(&store).unwrap();
        assert!(n2 >= 40, "inserted={n2}");
        assert_eq!(store.scripthash.entry_count(), n2 as u64);
        assert!(list_materialize_claims(&runs_dir).unwrap().is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn materialize_recovers_claimed_mat_files() {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-sh-mat-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store = Store::open_or_create(&dir).unwrap();
        let b = ShRunBuilder::new(&dir);
        let runs_dir = dir.join("scripthash.runs");
        std::fs::create_dir_all(&runs_dir).unwrap();
        let mut body = Vec::new();
        for i in 0..50u32 {
            let mut sh = [0u8; 32];
            sh[0] = i as u8;
            body.extend_from_slice(&encode_rec(&sh, Fk(i as u64 + 1)));
        }
        let path = next_run_path(&runs_dir, 1);
        let run = write_sorted_run(&path, SH_RUN_KEY_LEN, SH_RUN_REC_LEN, &body).unwrap();
        let claimed = claim_run_for_materialize(&run).unwrap();
        assert!(claimed.path.to_string_lossy().ends_with(".run.mat"));
        assert!(list_runs(&runs_dir).unwrap().is_empty());
        assert_eq!(list_materialize_claims(&runs_dir).unwrap().len(), 1);

        let n = b.finalize_and_bulk_materialize(&store).unwrap();
        assert!(n >= 50, "inserted={n}");
        assert!(store.scripthash.entry_count() >= 50);
        assert!(list_materialize_claims(&runs_dir).unwrap().is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn same_key_run_finalize_materialize() {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-sh-run-same-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store = Store::open_or_create(&dir).unwrap();
        let b = ShRunBuilder::new(&dir);
        let runs_dir = dir.join("scripthash.runs");
        std::fs::create_dir_all(&runs_dir).unwrap();
        let sh = [0xabu8; 32];
        let mut body = Vec::new();
        for i in 1..=20u64 {
            body.extend_from_slice(&encode_rec(&sh, Fk(i)));
        }
        let path = next_run_path(&runs_dir, 1);
        write_sorted_run(&path, SH_RUN_KEY_LEN, SH_RUN_REC_LEN, &body).unwrap();
        let n = b.finalize_and_bulk_materialize(&store).unwrap();
        assert_eq!(n, 20);
        assert_eq!(store.scripthash.entries(&sh).unwrap().len(), 20);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn same_key_append_preserves_chain() {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-sh-append-same-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store = Store::open_or_create(&dir).unwrap();
        let sh = [0xabu8; 32];
        let mut batch = Vec::new();
        for i in 1..=20u64 {
            batch.push(ScriptHashRecord::from_fk(sh, Fk(i)));
        }
        let mut heads = std::collections::HashMap::new();
        let (n, _) = store
            .scripthash
            .put_create_batch_append(&batch, &mut heads)
            .unwrap();
        assert_eq!(n, 20);
        assert_eq!(store.scripthash.entries(&sh).unwrap().len(), 20);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
