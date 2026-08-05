//! Plan Shape A head resolve: **txids → (fk, body_range)** (denserels optional after).
//!
//! **Page-promote work-queue** (schema 13+), one TLS [`UringSession`]:
//!
//! - Software queue of HEAD / ID / IDX jobs feeds a generic free list of uring slots.
//! - Seed HEAD for **segs[0]** (open if present), keys grouped by probe page.
//! - HEAD → hop_scan → deepest-first software cand list → **one ID SQE per key**.
//! - ID miss → next cand; last miss → bag key on the page’s **promote** list.
//! - ID hit → IDX body_range → `winner[key]` done (never later segs).
//! - When the **last key leaves a page job** (hit-done or bagged), fuse-walk the
//!   promote bag: enqueue one HEAD for the first later seg with any fuse hits;
//!   fuse-misses walk further without waiting. Pages promote independently
//!   (page A can load sealed while page B is still on open).
//!
//! **Hard errors:** HEAD/ID/IDX IO failures (after ENOTSUP soft-retry for DONTCACHE)
//! and idx decode/empty-range after identity hit return `Err` and drain the ring.
//! Identity *mismatch* remains a soft cand miss.
//!
//! Backend: `RBITCOIN_HEAD_RESOLVE_IO` / global `RBITCOIN_IO` (`uring` \| `pread`).

use crate::address_head::{
    h1_in_page, h2_in_page, hop_scan_page, page_base_for_txid, page_slot_count, MAX_PROBE,
};
use crate::error::StoreError;
use crate::idx_body_pipeline::{run_idx_body_pipeline, BodyMode, IdxBodyJob};
use crate::io_backend::{self, ReadIoBackend};
use crate::segmented_head::ResolveSeg;
use crate::tx_idx::BodyRangeIdxPlan;
use crate::tx_table::{
    decode_packed_tx_outs_with_spender_rels_secret, OutputRecord, TxRecord, TxTable,
};
use crate::txid_body::{TXID_ENTRY_LEN, TxidBody};
use crate::uring_session::{self, UringSession};
use rbitcoin_primitives::Fk;
use std::collections::{HashMap, VecDeque};
use std::os::fd::RawFd;
use std::time::Instant;

const MAX_SLOTS: usize = 128;
const STAGE_HEAD: u64 = 1;
const STAGE_ID: u64 = 2;
const STAGE_IDX: u64 = 3;

// ── public API ──────────────────────────────────────────────────────────────

pub fn resolve_fk_and_range_batch(
    table: &TxTable,
    txids: &[[u8; 32]],
) -> Result<Vec<([u8; 32], Option<(Fk, (u64, u64))>)>, StoreError> {
    if txids.is_empty() {
        return Ok(Vec::new());
    }
    match io_backend::head_resolve_io_backend() {
        ReadIoBackend::Uring => match resolve_fk_and_range_uring(table, txids) {
            Ok(v) => Ok(v),
            Err(_) => resolve_fk_and_range_pread(table, txids),
        },
        ReadIoBackend::Pread => resolve_fk_and_range_pread(table, txids),
    }
}

pub fn resolve_fk_and_denserels_batch(
    table: &TxTable,
    txids: &[[u8; 32]],
) -> Result<
    (
        Vec<(
            [u8; 32],
            Option<(
                Fk,
                Option<(TxRecord, Vec<OutputRecord>, Vec<u32>)>,
            )>,
        )>,
        u64,
    ),
    StoreError,
> {
    if txids.is_empty() {
        return Ok((Vec::new(), 0));
    }
    match io_backend::head_resolve_io_backend() {
        ReadIoBackend::Uring => match resolve_denserels_uring(table, txids) {
            Ok(v) => Ok(v),
            Err(_) => resolve_denserels_pread(table, txids),
        },
        ReadIoBackend::Pread => resolve_denserels_pread(table, txids),
    }
}

// ── helpers ─────────────────────────────────────────────────────────────────

fn hop_cands_abs(seg: &ResolveSeg, mixed: &[u8; 32], page: &[u8], es: u8, nslots: u64) -> Vec<u64> {
    let bits = seg.head.bits();
    let scan = hop_scan_page(
        page,
        es,
        h1_in_page(mixed, bits),
        h2_in_page(mixed, bits),
        nslots,
        MAX_PROBE,
    );
    let mut abs = Vec::with_capacity(scan.cands.len());
    for &(_, rel) in scan.cands.iter().rev() {
        if let Some(fk) = seg.rel_to_abs(rel) {
            abs.push(fk.0);
        }
    }
    abs
}

fn group_by_page(mixed: &[[u8; 32]], key_is: &[u32], bits: u32) -> Vec<(u64, Vec<u32>)> {
    let mut order: Vec<(u64, u32)> = key_is
        .iter()
        .map(|&ki| (page_base_for_txid(&mixed[ki as usize], bits), ki))
        .collect();
    order.sort_unstable_by_key(|&(p, k)| (p, k));
    let mut out = Vec::new();
    let mut i = 0;
    while i < order.len() {
        let pb = order[i].0;
        let mut keys = Vec::new();
        while i < order.len() && order[i].0 == pb {
            keys.push(order[i].1);
            i += 1;
        }
        out.push((pb, keys));
    }
    out
}

fn pread_fill(fd: RawFd, off: u64, buf: &mut [u8], dontcache: bool, path: &std::path::Path) -> Result<(), StoreError> {
    let rc = crate::bulk_io::pread_single(fd, off, buf, dontcache);
    if rc < 0 {
        return Err(StoreError::io(path, std::io::Error::from_raw_os_error(-rc)));
    }
    if (rc as usize) < buf.len() {
        let rc2 = crate::bulk_io::pread_single(fd, off, buf, false);
        if rc2 < 0 || (rc2 as usize) < buf.len() {
            return Err(StoreError::io(
                path,
                std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "short pread"),
            ));
        }
    }
    Ok(())
}

// ── pread path (same promote semantics) ─────────────────────────────────────

fn resolve_fk_and_range_pread(
    table: &TxTable,
    txids: &[[u8; 32]],
) -> Result<Vec<([u8; 32], Option<(Fk, (u64, u64))>)>, StoreError> {
    crate::head_resolve_stats::add_keys(txids.len() as u64);
    let mixed: Vec<[u8; 32]> = txids.iter().map(|t| table.secret.mix_txid(t)).collect();
    let segs = table.head.resolve_segments();
    if segs.is_empty() {
        return Ok(txids.iter().map(|t| (*t, None)).collect());
    }

    let side = table.txid_sidefile();
    let body_count = table.body.count();
    let mut winner = vec![None; txids.len()];
    let mut done = vec![false; txids.len()];
    let mut body_lookups = 0u64;
    let mut miss_peeks = 0u64;
    let mut id_ns = 0u64;
    let mut idx_ns = 0u64;
    let mut probe_ns = 0u64;
    let mut cands_total = 0u64;

    let mut jobs: VecDeque<(usize, u64, Vec<u32>)> = VecDeque::new();
    let all: Vec<u32> = (0..txids.len() as u32).collect();
    for (pb, ks) in group_by_page(&mixed, &all, segs[0].head.bits()) {
        jobs.push_back((0, pb, ks));
    }

    while let Some((seg_ord, page_base, key_is)) = jobs.pop_front() {
        if seg_ord >= segs.len() {
            for ki in key_is {
                done[ki as usize] = true;
            }
            continue;
        }
        let seg = &segs[seg_ord];
        let bits = seg.head.bits();
        let es = seg.head.entry_bytes();
        let need = seg.head.probe_page_need(page_base, page_slot_count(bits));
        let t0 = Instant::now();
        let mut buf = vec![0u8; need.max(1)];
        if need > 0 {
            pread_fill(
                seg.head.read_fd(),
                seg.head.entry_off(page_base),
                &mut buf[..need],
                seg.dontcache(),
                seg.head.path(),
            )?;
        }
        probe_ns += t0.elapsed().as_nanos() as u64;
        let nslots = if need == 0 { 0 } else { (need / es as usize) as u64 };
        let page = &buf[..need];
        let mut promote = Vec::new();

        for ki in key_is {
            let i = ki as usize;
            if done[i] {
                continue;
            }
            let cands = if need == 0 {
                Vec::new()
            } else {
                hop_cands_abs(seg, &mixed[i], page, es, nslots)
            };
            cands_total += cands.len() as u64;
            let mut peeks = 0u32;
            let mut hit = false;
            for &fk_u in &cands {
                if fk_u == 0 || fk_u > body_count {
                    continue;
                }
                peeks += 1;
                let fk = Fk(fk_u);
                let t_id = Instant::now();
                let got = match side.get(fk) {
                    Ok(t) => t,
                    Err(StoreError::NotFound) | Err(StoreError::InvalidFk) => {
                        id_ns += t_id.elapsed().as_nanos() as u64;
                        miss_peeks += 1;
                        body_lookups += 1;
                        continue;
                    }
                    Err(e) => return Err(e),
                };
                id_ns += t_id.elapsed().as_nanos() as u64;
                body_lookups += 1;
                if got != txids[i] {
                    miss_peeks += 1;
                    continue;
                }
                crate::head_resolve_stats::add_hit_rank(peeks as u64);
                let t_idx = Instant::now();
                match table.body.record_range(fk) {
                    Ok((off, len)) if len > 0 => {
                        winner[i] = Some((fk, (off, len)));
                        done[i] = true;
                        hit = true;
                    }
                    Ok(_) => {
                        return Err(StoreError::Corrupt(
                            "head resolve: identity hit but empty body_range",
                        ));
                    }
                    Err(e) => return Err(e),
                }
                idx_ns += t_idx.elapsed().as_nanos() as u64;
                break;
            }
            if !hit && !done[i] {
                promote.push(ki);
            }
        }
        promote_keys_serial(&mut jobs, &segs, &mixed, &mut done, &mut winner, promote, page_base, seg_ord + 1);
    }

    for d in &mut done {
        *d = true;
    }

    crate::head_resolve_stats::add_probe(probe_ns);
    crate::head_resolve_stats::add_body(id_ns);
    crate::head_resolve_stats::add_idx(idx_ns);
    crate::head_resolve_stats::add_cands(cands_total);
    crate::head_resolve_stats::add_body_lookups(body_lookups);
    crate::head_resolve_stats::add_miss_peeks(miss_peeks);

    Ok(txids.iter().enumerate().map(|(i, t)| (*t, winner[i])).collect())
}

fn promote_keys_serial(
    jobs: &mut VecDeque<(usize, u64, Vec<u32>)>,
    segs: &[ResolveSeg],
    mixed: &[[u8; 32]],
    done: &mut [bool],
    winner: &mut [Option<(Fk, (u64, u64))>],
    keys: Vec<u32>,
    page_base: u64,
    start_seg: usize,
) {
    let mut remain: Vec<u32> = keys.into_iter().filter(|&k| !done[k as usize]).collect();
    let mut seg = start_seg;
    while seg < segs.len() && !remain.is_empty() {
        let mut pass = Vec::new();
        let mut fail = Vec::new();
        for ki in remain {
            if segs[seg].fuse_contains(&mixed[ki as usize]) {
                pass.push(ki);
            } else {
                fail.push(ki);
            }
        }
        if !pass.is_empty() {
            jobs.push_back((seg, page_base, pass));
        }
        remain = fail;
        seg += 1;
    }
    for ki in remain {
        done[ki as usize] = true;
        winner[ki as usize] = None;
    }
}

fn denserels_after(
    table: &TxTable,
    ranges: Vec<([u8; 32], Option<(Fk, (u64, u64))>)>,
) -> Result<
    (
        Vec<(
            [u8; 32],
            Option<(Fk, Option<(TxRecord, Vec<OutputRecord>, Vec<u32>)>)>,
        )>,
        u64,
    ),
    StoreError,
> {
    let mut dens_ns = 0u64;
    let mut dens: HashMap<usize, (TxRecord, Vec<OutputRecord>, Vec<u32>)> = HashMap::new();
    let mut need = Vec::new();
    for (i, (_t, row)) in ranges.iter().enumerate() {
        if let Some((fk, range)) = row {
            need.push((i, *fk, *range));
        }
    }
    if !need.is_empty() {
        let t0 = Instant::now();
        let mut jobs: Vec<IdxBodyJob> = need
            .iter()
            .map(|(_, fk, r)| IdxBodyJob::new(fk.0, Some(*r)))
            .collect();
        run_idx_body_pipeline(&table.body, &mut jobs, BodyMode::OutsDenserels)?;
        dens_ns = t0.elapsed().as_nanos() as u64;
        for ((ki, fk, _), job) in need.into_iter().zip(jobs) {
            if !job.ok || job.body.is_empty() {
                continue;
            }
            match decode_packed_tx_outs_with_spender_rels_secret(&job.body, Some(&table.secret)) {
                Ok(mut d) => {
                    if let Ok(tid) = table.txid_sidefile().get(fk) {
                        d.0.txid = tid;
                    }
                    dens.insert(ki, d);
                }
                Err(StoreError::NotFound) | Err(StoreError::Corrupt(_)) => {}
                Err(e) => return Err(e),
            }
        }
    }
    let mut out = Vec::with_capacity(ranges.len());
    for (i, (txid, row)) in ranges.into_iter().enumerate() {
        out.push((txid, row.map(|(fk, _)| (fk, dens.remove(&i)))));
    }
    Ok((out, dens_ns))
}

fn resolve_denserels_pread(
    table: &TxTable,
    txids: &[[u8; 32]],
) -> Result<
    (
        Vec<(
            [u8; 32],
            Option<(Fk, Option<(TxRecord, Vec<OutputRecord>, Vec<u32>)>)>,
        )>,
        u64,
    ),
    StoreError,
> {
    denserels_after(table, resolve_fk_and_range_pread(table, txids)?)
}

// ── uring work-queue ────────────────────────────────────────────────────────

#[derive(Clone, Copy)]
enum Job {
    Head(u32),
    Id(u32),
    Idx(u32),
}

struct PageCtx {
    seg_ord: u16,
    page_base: u64,
    /// Keys attached to this HEAD@seg job.
    keys: Vec<u32>,
    /// Keys that left this page (global done or bagged for promote).
    n_finished: u32,
    /// Last-cand / empty-cand misses waiting for page-finish fuse walk.
    promote: Vec<u32>,
    head_buf: Vec<u8>,
    head_need: usize,
    /// True until HEAD CQE hop finishes (or empty-need synthetic hop).
    head_pending: bool,
}

struct KeyState {
    cands: Vec<u64>,
    cand_i: usize,
    id_buf: [u8; 32],
    pending_fk: u64,
    peeks: u32,
    idx_plan: Option<BodyRangeIdxPlan>,
    idx_bufs: Vec<Vec<u8>>,
    idx_page_i: u8,
    done: bool,
    page_ctx_id: u32,
}

impl KeyState {
    fn new() -> Self {
        Self {
            cands: Vec::new(),
            cand_i: 0,
            id_buf: [0u8; 32],
            pending_fk: 0,
            peeks: 0,
            idx_plan: None,
            idx_bufs: Vec::new(),
            idx_page_i: 0,
            done: false,
            page_ctx_id: u32::MAX,
        }
    }
}

fn resolve_fk_and_range_uring(
    table: &TxTable,
    txids: &[[u8; 32]],
) -> Result<Vec<([u8; 32], Option<(Fk, (u64, u64))>)>, StoreError> {
    crate::head_resolve_stats::add_keys(txids.len() as u64);
    uring_session::with_thread_local(uring_session::DEFAULT_ENTRIES, |s| {
        resolve_uring_on(s, table, txids)
    })?
}

fn resolve_uring_on(
    session: &mut UringSession,
    table: &TxTable,
    txids: &[[u8; 32]],
) -> Result<Vec<([u8; 32], Option<(Fk, (u64, u64))>)>, StoreError> {
    let mixed: Vec<[u8; 32]> = txids.iter().map(|t| table.secret.mix_txid(t)).collect();
    let segs = table.head.resolve_segments();
    if segs.is_empty() {
        return Ok(txids.iter().map(|t| (*t, None)).collect());
    }

    let side = table.txid_sidefile();
    let side_fd = side.body_read_fd();
    let side_path = side.file_path().to_path_buf();
    let body_count = table.body.count();
    let side_n = side.count();

    let mut winner = vec![None; txids.len()];
    let mut keys: Vec<KeyState> = (0..txids.len()).map(|_| KeyState::new()).collect();
    let mut pages: Vec<Option<PageCtx>> = Vec::new();
    let mut free_pcids: Vec<u32> = Vec::new();
    let mut work: VecDeque<Job> = VecDeque::new();
    let mut coalesce: HashMap<(u16, u64), Vec<u32>> = HashMap::new();

    let mut free_slots: Vec<usize> = (0..MAX_SLOTS).collect();
    let mut inflight: Vec<Option<Job>> = vec![None; MAX_SLOTS];
    let mut n_inf = 0usize;

    let mut body_lookups = 0u64;
    let mut miss_peeks = 0u64;
    let mut id_ns = 0u64;
    let mut idx_ns = 0u64;
    let mut probe_ns = 0u64;
    let mut cands_total = 0u64;

    // Seed open segment.
    let all: Vec<u32> = (0..txids.len() as u32).collect();
    for (pb, ks) in group_by_page(&mixed, &all, segs[0].head.bits()) {
        coalesce.entry((0, pb)).or_default().extend(ks);
    }
    flush_coalesce(&mut coalesce, &mut pages, &mut free_pcids, &mut work, &segs);

    let mut ring = DrainOnDrop(session);

    loop {
        submit_work(
            &mut ring,
            &mut work,
            &mut free_slots,
            &mut inflight,
            &mut n_inf,
            &mut pages,
            &mut keys,
            &segs,
            side_fd,
            body_count,
            side_n,
            &mut id_ns,
            &mut coalesce,
            &mut free_pcids,
            &mixed,
            &mut winner,
        )?;

        if n_inf == 0 && work.is_empty() {
            flush_coalesce(&mut coalesce, &mut pages, &mut free_pcids, &mut work, &segs);
            if work.is_empty() {
                // All promote paths should have marked winners; leftover = hang/bug.
                for i in 0..txids.len() {
                    if !keys[i].done {
                        return Err(StoreError::Corrupt(
                            "head resolve: idle with unfinished keys",
                        ));
                    }
                }
                break;
            }
            continue;
        }

        if n_inf == 0 {
            return Err(StoreError::Corrupt("head resolve: idle with pending work"));
        }

        let mut cqes = ring.harvest_ready();
        if cqes.is_empty() {
            ring.submit_and_wait_one()?;
            cqes = ring.harvest_ready();
        }

        for (ud, res) in cqes {
            let (kind, slot_u) = uring_session::unpack_ud(ud);
            let slot = slot_u as usize;
            let job = inflight[slot]
                .take()
                .ok_or(StoreError::Corrupt("head resolve empty inflight"))?;
            n_inf -= 1;
            free_slots.push(slot);

            match (kind, job) {
                (STAGE_HEAD, Job::Head(pcid)) => {
                    handle_head(
                        res,
                        pcid,
                        table,
                        txids,
                        &mixed,
                        &segs,
                        &mut pages,
                        &mut keys,
                        &mut work,
                        &mut coalesce,
                        &mut free_pcids,
                        &mut winner,
                        &mut cands_total,
                        &mut probe_ns,
                    )?;
                }
                (STAGE_ID, Job::Id(ki)) => {
                    body_lookups += 1;
                    handle_id(
                        res,
                        ki,
                        table,
                        txids,
                        &mut keys,
                        &mut pages,
                        &mut work,
                        &mut coalesce,
                        &mut free_pcids,
                        &segs,
                        &mixed,
                        &mut winner,
                        &mut miss_peeks,
                        side_fd,
                        &side_path,
                    )?;
                }
                (STAGE_IDX, Job::Idx(ki)) => {
                    handle_idx(
                        res,
                        ki,
                        table,
                        &mut keys,
                        &mut pages,
                        &mut work,
                        &mut coalesce,
                        &mut free_pcids,
                        &segs,
                        &mixed,
                        &mut winner,
                        &mut idx_ns,
                    )?;
                }
                _ => return Err(StoreError::Corrupt("head resolve stage mismatch")),
            }
        }
        flush_coalesce(&mut coalesce, &mut pages, &mut free_pcids, &mut work, &segs);
    }

    drop(ring);
    crate::head_resolve_stats::add_probe(probe_ns);
    crate::head_resolve_stats::add_body(id_ns);
    crate::head_resolve_stats::add_idx(idx_ns);
    crate::head_resolve_stats::add_cands(cands_total);
    crate::head_resolve_stats::add_body_lookups(body_lookups);
    crate::head_resolve_stats::add_miss_peeks(miss_peeks);

    Ok(txids.iter().enumerate().map(|(i, t)| (*t, winner[i])).collect())
}

fn flush_coalesce(
    coalesce: &mut HashMap<(u16, u64), Vec<u32>>,
    pages: &mut Vec<Option<PageCtx>>,
    free_pcids: &mut Vec<u32>,
    work: &mut VecDeque<Job>,
    segs: &[ResolveSeg],
) {
    let items: Vec<_> = coalesce.drain().collect();
    for ((seg_ord, page_base), mut kis) in items {
        if (seg_ord as usize) >= segs.len() {
            continue;
        }
        kis.sort_unstable();
        kis.dedup();
        if kis.is_empty() {
            continue;
        }
        let seg = &segs[seg_ord as usize];
        let need = seg
            .head
            .probe_page_need(page_base, page_slot_count(seg.head.bits()));
        let ctx = PageCtx {
            seg_ord,
            page_base,
            keys: kis,
            n_finished: 0,
            promote: Vec::new(),
            head_buf: vec![0u8; need.max(1)],
            head_need: need,
            head_pending: true,
        };
        let pcid = if let Some(id) = free_pcids.pop() {
            pages[id as usize] = Some(ctx);
            id
        } else {
            let id = pages.len() as u32;
            pages.push(Some(ctx));
            id
        };
        work.push_back(Job::Head(pcid));
    }
}

fn submit_work(
    session: &mut UringSession,
    work: &mut VecDeque<Job>,
    free_slots: &mut Vec<usize>,
    inflight: &mut [Option<Job>],
    n_inf: &mut usize,
    pages: &mut [Option<PageCtx>],
    keys: &mut [KeyState],
    segs: &[ResolveSeg],
    side_fd: RawFd,
    body_count: u64,
    side_n: u64,
    id_ns: &mut u64,
    coalesce: &mut HashMap<(u16, u64), Vec<u32>>,
    free_pcids: &mut Vec<u32>,
    mixed: &[[u8; 32]],
    winner: &mut [Option<(Fk, (u64, u64))>],
) -> Result<(), StoreError> {
    while !free_slots.is_empty() && !work.is_empty() && session.free_sq() > 0 {
        let job = work.pop_front().unwrap();
        match job {
            Job::Head(pcid) => {
                let need = match pages[pcid as usize].as_ref() {
                    Some(c) => c.head_need,
                    None => continue,
                };
                if need == 0 {
                    // Empty page region: hop yields no cands — bag all keys, finish page.
                    let key_is = {
                        let ctx = pages[pcid as usize].as_mut().unwrap();
                        ctx.head_pending = false;
                        ctx.keys.clone()
                    };
                    for &ki in &key_is {
                        if keys[ki as usize].done {
                            if let Some(c) = pages[pcid as usize].as_mut() {
                                c.n_finished += 1;
                            }
                            continue;
                        }
                        keys[ki as usize].cands.clear();
                        keys[ki as usize].cand_i = 0;
                        bag_promote(ki, pcid, keys, pages);
                    }
                    maybe_finish_page(
                        pcid, pages, free_pcids, coalesce, segs, mixed, keys, winner,
                    );
                    continue;
                }
                let slot = free_slots.pop().unwrap();
                let ctx = pages[pcid as usize].as_mut().unwrap();
                let seg = &segs[ctx.seg_ord as usize];
                let rw = if seg.dontcache() && crate::bulk_io::rwf_dontcache_ok() {
                    uring_session::RWF_DONTCACHE
                } else {
                    0
                };
                let off = seg.head.entry_off(ctx.page_base);
                ctx.head_buf[..need].fill(0);
                session.push_pread_flags(
                    seg.head.read_fd(),
                    off,
                    &mut ctx.head_buf[..need],
                    uring_session::pack_ud(STAGE_HEAD, slot as u32),
                    rw,
                )?;
                inflight[slot] = Some(Job::Head(pcid));
                *n_inf += 1;
            }
            Job::Id(ki) => {
                if keys[ki as usize].done {
                    continue;
                }
                let slot = free_slots.pop().unwrap();
                if !push_next_id(
                    &mut keys[ki as usize],
                    session,
                    side_fd,
                    body_count,
                    side_n,
                    slot as u32,
                    id_ns,
                )? {
                    free_slots.push(slot);
                    // Exhausted cands (all invalid fks) → bag for page finish.
                    let pcid = keys[ki as usize].page_ctx_id;
                    bag_promote(ki, pcid, keys, pages);
                    maybe_finish_page(
                        pcid, pages, free_pcids, coalesce, segs, mixed, keys, winner,
                    );
                    continue;
                }
                inflight[slot] = Some(Job::Id(ki));
                *n_inf += 1;
            }
            Job::Idx(ki) => {
                if keys[ki as usize].done || keys[ki as usize].idx_plan.is_none() {
                    continue;
                }
                let slot = free_slots.pop().unwrap();
                push_idx_page(&mut keys[ki as usize], session, slot as u32)?;
                inflight[slot] = Some(Job::Idx(ki));
                *n_inf += 1;
            }
        }
    }
    session.sync_submission();
    let _ = session.submit();
    Ok(())
}

fn push_next_id(
    k: &mut KeyState,
    session: &mut UringSession,
    side_fd: RawFd,
    body_count: u64,
    side_n: u64,
    slot: u32,
    id_ns: &mut u64,
) -> Result<bool, StoreError> {
    while k.cand_i < k.cands.len() {
        let fk = k.cands[k.cand_i];
        k.cand_i += 1;
        if fk == 0 || fk > body_count {
            continue;
        }
        let t0 = Instant::now();
        let Ok(off) = TxidBody::entry_offset(fk) else {
            continue;
        };
        *id_ns += t0.elapsed().as_nanos() as u64;
        k.pending_fk = fk;
        k.peeks += 1;
        k.id_buf = [0u8; 32];
        let flags = crate::dontcache_policy::sidefile_sqe_rw_flags(fk, side_n);
        session.push_pread_flags(
            side_fd,
            off,
            &mut k.id_buf,
            uring_session::pack_ud(STAGE_ID, slot),
            flags,
        )?;
        return Ok(true);
    }
    Ok(false)
}

fn push_idx_page(k: &mut KeyState, session: &mut UringSession, slot: u32) -> Result<(), StoreError> {
    let pi = k.idx_page_i as usize;
    let plan = k.idx_plan.as_ref().unwrap();
    let page = &plan.pages[pi];
    k.idx_bufs[pi].fill(0);
    session.push_pread_flags(
        page.fd,
        page.page_off,
        &mut k.idx_bufs[pi],
        uring_session::pack_ud(STAGE_IDX, slot),
        page.rw_flags,
    )?;
    Ok(())
}

fn handle_head(
    res: i32,
    pcid: u32,
    _table: &TxTable,
    _txids: &[[u8; 32]],
    mixed: &[[u8; 32]],
    segs: &[ResolveSeg],
    pages: &mut Vec<Option<PageCtx>>,
    keys: &mut [KeyState],
    work: &mut VecDeque<Job>,
    coalesce: &mut HashMap<(u16, u64), Vec<u32>>,
    free_pcids: &mut Vec<u32>,
    winner: &mut [Option<(Fk, (u64, u64))>],
    cands_total: &mut u64,
    probe_ns: &mut u64,
) -> Result<(), StoreError> {
    let t0 = Instant::now();
    let hops = {
        let ctx = pages[pcid as usize]
            .as_mut()
            .ok_or(StoreError::Corrupt("HEAD missing page ctx"))?;
        let seg = &segs[ctx.seg_ord as usize];
        let need = ctx.head_need;
        if need > 0 {
            if res < 0 {
                if res == -95 {
                    crate::bulk_io::note_rwf_dontcache_unsupported();
                    pread_fill(
                        seg.head.read_fd(),
                        seg.head.entry_off(ctx.page_base),
                        &mut ctx.head_buf[..need],
                        false,
                        seg.head.path(),
                    )?;
                } else {
                    return Err(StoreError::io(
                        seg.head.path(),
                        std::io::Error::from_raw_os_error(-res),
                    ));
                }
            } else if (res as usize) < need {
                pread_fill(
                    seg.head.read_fd(),
                    seg.head.entry_off(ctx.page_base),
                    &mut ctx.head_buf[..need],
                    false,
                    seg.head.path(),
                )?;
            }
        }
        *probe_ns += t0.elapsed().as_nanos() as u64;
        ctx.head_pending = false;

        let es = seg.head.entry_bytes();
        let nslots = if need == 0 {
            0
        } else {
            (need / es as usize) as u64
        };
        let key_is = ctx.keys.clone();
        // Hop into per-key cand lists while page bytes still live, then drop the buf.
        let mut hops: Vec<(u32, Vec<u64>)> = Vec::with_capacity(key_is.len());
        {
            let page = &ctx.head_buf[..need];
            for &ki in &key_is {
                if keys[ki as usize].done {
                    hops.push((ki, Vec::new()));
                    continue;
                }
                let cands = if need == 0 {
                    Vec::new()
                } else {
                    hop_cands_abs(seg, &mixed[ki as usize], page, es, nslots)
                };
                hops.push((ki, cands));
            }
            ctx.head_buf = Vec::new();
        }
        hops
    };

    for (ki, cands) in hops {
        if keys[ki as usize].done {
            if let Some(c) = pages[pcid as usize].as_mut() {
                c.n_finished += 1;
            }
            continue;
        }
        *cands_total += cands.len() as u64;
        keys[ki as usize].page_ctx_id = pcid;
        keys[ki as usize].cands = cands;
        keys[ki as usize].cand_i = 0;
        keys[ki as usize].peeks = 0;
        if keys[ki as usize].cands.is_empty() {
            bag_promote(ki, pcid, keys, pages);
        } else {
            work.push_back(Job::Id(ki));
        }
    }

    maybe_finish_page(
        pcid, pages, free_pcids, coalesce, segs, mixed, keys, winner,
    );
    Ok(())
}

fn handle_id(
    res: i32,
    ki: u32,
    table: &TxTable,
    txids: &[[u8; 32]],
    keys: &mut [KeyState],
    pages: &mut Vec<Option<PageCtx>>,
    work: &mut VecDeque<Job>,
    coalesce: &mut HashMap<(u16, u64), Vec<u32>>,
    free_pcids: &mut Vec<u32>,
    segs: &[ResolveSeg],
    mixed: &[[u8; 32]],
    winner: &mut [Option<(Fk, (u64, u64))>],
    miss_peeks: &mut u64,
    side_fd: RawFd,
    side_path: &std::path::Path,
) -> Result<(), StoreError> {
    let i = ki as usize;
    if keys[i].done {
        return Ok(());
    }

    // Soft identity miss vs hard IO error.
    let soft_miss = if res < 0 {
        if res == -95 {
            crate::bulk_io::note_rwf_dontcache_unsupported();
            let off = TxidBody::entry_offset(keys[i].pending_fk)?;
            match pread_fill(side_fd, off, &mut keys[i].id_buf, false, side_path) {
                Ok(()) => keys[i].id_buf != txids[i],
                Err(e) => return Err(e),
            }
        } else {
            return Err(StoreError::io(
                side_path,
                std::io::Error::from_raw_os_error(-res),
            ));
        }
    } else if (res as usize) != TXID_ENTRY_LEN as usize {
        true
    } else {
        keys[i].id_buf != txids[i]
    };

    if soft_miss {
        *miss_peeks += 1;
        if keys[i].cand_i < keys[i].cands.len() {
            // Remaining cands (push_next_id filters invalid fks).
            work.push_back(Job::Id(ki));
            return Ok(());
        }
        // Last cand miss for this segment → bag; HEAD only after page finish.
        let pcid = keys[i].page_ctx_id;
        bag_promote(ki, pcid, keys, pages);
        maybe_finish_page(
            pcid, pages, free_pcids, coalesce, segs, mixed, keys, winner,
        );
        return Ok(());
    }

    // Identity hit → IDX plan (hard fail if missing — not soft cand miss).
    crate::head_resolve_stats::add_hit_rank(keys[i].peeks.max(1) as u64);
    let fk = Fk(keys[i].pending_fk);
    let plan = match table.body.plan_body_range_idx(fk) {
        Ok(p) if !p.pages.is_empty() => p,
        Ok(_) => {
            return Err(StoreError::Corrupt(
                "head resolve: identity hit but empty idx plan",
            ));
        }
        Err(e) => return Err(e),
    };
    keys[i].idx_bufs = plan.pages.iter().map(|p| vec![0u8; p.want]).collect();
    keys[i].idx_plan = Some(plan);
    keys[i].idx_page_i = 0;
    work.push_back(Job::Idx(ki));
    Ok(())
}

fn handle_idx(
    res: i32,
    ki: u32,
    _table: &TxTable,
    keys: &mut [KeyState],
    pages: &mut Vec<Option<PageCtx>>,
    work: &mut VecDeque<Job>,
    coalesce: &mut HashMap<(u16, u64), Vec<u32>>,
    free_pcids: &mut Vec<u32>,
    segs: &[ResolveSeg],
    mixed: &[[u8; 32]],
    winner: &mut [Option<(Fk, (u64, u64))>],
    idx_ns: &mut u64,
) -> Result<(), StoreError> {
    let i = ki as usize;
    if keys[i].done {
        return Ok(());
    }
    let page_i = keys[i].idx_page_i as usize;
    let plan = keys[i]
        .idx_plan
        .as_ref()
        .ok_or(StoreError::Corrupt("IDX without plan"))?;
    let page = &plan.pages[page_i];
    let want = page.want;
    let fd = page.fd;
    let off = page.page_off;
    let flags = page.rw_flags;
    let n_pages = plan.pages.len();

    if res < 0 {
        if res == -95 && flags != 0 {
            crate::bulk_io::note_rwf_dontcache_unsupported();
            pread_fill(
                fd,
                off,
                &mut keys[i].idx_bufs[page_i],
                false,
                std::path::Path::new("tx.idx"),
            )?;
        } else {
            // Critical: IDX failure aborts resolve (not cand miss).
            return Err(StoreError::io(
                "tx.idx",
                std::io::Error::from_raw_os_error(-res),
            ));
        }
    } else if (res as usize) < want {
        pread_fill(
            fd,
            off,
            &mut keys[i].idx_bufs[page_i],
            false,
            std::path::Path::new("tx.idx"),
        )?;
    }

    if page_i + 1 < n_pages {
        keys[i].idx_page_i = (page_i + 1) as u8;
        work.push_back(Job::Idx(ki));
        return Ok(());
    }

    let t0 = Instant::now();
    let refs: Vec<&[u8]> = keys[i].idx_bufs.iter().map(|b| b.as_slice()).collect();
    let range = match keys[i].idx_plan.as_ref().unwrap().decode_range(&refs) {
        Ok((o, len)) if len > 0 => (o, len),
        Ok(_) => {
            return Err(StoreError::Corrupt(
                "head resolve: identity hit but zero-length body_range",
            ));
        }
        Err(e) => return Err(e),
    };
    *idx_ns += t0.elapsed().as_nanos() as u64;

    let fk = Fk(keys[i].pending_fk);
    winner[i] = Some((fk, range));
    keys[i].done = true;
    keys[i].idx_plan = None;
    keys[i].idx_bufs.clear();

    let pcid = keys[i].page_ctx_id;
    if pcid != u32::MAX {
        if let Some(ctx) = pages[pcid as usize].as_mut() {
            ctx.n_finished += 1;
        }
        keys[i].page_ctx_id = u32::MAX;
        maybe_finish_page(
            pcid, pages, free_pcids, coalesce, segs, mixed, keys, winner,
        );
    }
    Ok(())
}

/// Bag a key for later-seg fuse walk; does **not** enqueue HEAD until page finish.
fn bag_promote(ki: u32, pcid: u32, keys: &mut [KeyState], pages: &mut [Option<PageCtx>]) {
    let i = ki as usize;
    if keys[i].done {
        return;
    }
    keys[i].page_ctx_id = u32::MAX;
    keys[i].cands.clear();
    keys[i].cand_i = 0;
    if pcid == u32::MAX {
        return;
    }
    if let Some(ctx) = pages[pcid as usize].as_mut() {
        ctx.promote.push(ki);
        ctx.n_finished += 1;
    }
}

/// When every key has left the page job, fuse-walk the promote bag once.
fn maybe_finish_page(
    pcid: u32,
    pages: &mut [Option<PageCtx>],
    free_pcids: &mut Vec<u32>,
    coalesce: &mut HashMap<(u16, u64), Vec<u32>>,
    segs: &[ResolveSeg],
    mixed: &[[u8; 32]],
    keys: &mut [KeyState],
    winner: &mut [Option<(Fk, (u64, u64))>],
) {
    if pcid == u32::MAX {
        return;
    }
    let Some(ctx) = pages[pcid as usize].as_ref() else {
        return;
    };
    if ctx.head_pending || (ctx.n_finished as usize) < ctx.keys.len() {
        return;
    }
    let page_base = ctx.page_base;
    let next_seg = ctx.seg_ord as usize + 1;
    let promote = pages[pcid as usize]
        .as_mut()
        .map(|c| std::mem::take(&mut c.promote))
        .unwrap_or_default();
    pages[pcid as usize] = None;
    free_pcids.push(pcid);
    promote_keys(
        coalesce, segs, mixed, keys, winner, promote, page_base, next_seg,
    );
}

/// Fuse-walk remaining keys from `start_seg`; coalesce HEADs per (seg, page_base).
fn promote_keys(
    coalesce: &mut HashMap<(u16, u64), Vec<u32>>,
    segs: &[ResolveSeg],
    mixed: &[[u8; 32]],
    keys: &mut [KeyState],
    winner: &mut [Option<(Fk, (u64, u64))>],
    key_is: Vec<u32>,
    page_base: u64,
    start_seg: usize,
) {
    let mut remain: Vec<u32> = key_is
        .into_iter()
        .filter(|&ki| !keys[ki as usize].done)
        .collect();
    let mut seg = start_seg;
    while seg < segs.len() && !remain.is_empty() {
        let mut pass = Vec::new();
        let mut fail = Vec::new();
        for ki in remain {
            if segs[seg].fuse_contains(&mixed[ki as usize]) {
                pass.push(ki);
            } else {
                fail.push(ki);
            }
        }
        if !pass.is_empty() {
            // Fuse hits → enqueue HEAD for this page@seg; fuse-misses walk further now.
            coalesce
                .entry((seg as u16, page_base))
                .or_default()
                .extend(pass);
        }
        remain = fail;
        seg += 1;
    }
    for ki in remain {
        let i = ki as usize;
        if !keys[i].done {
            keys[i].done = true;
            winner[i] = None;
                }
    }
}

fn resolve_denserels_uring(
    table: &TxTable,
    txids: &[[u8; 32]],
) -> Result<
    (
        Vec<(
            [u8; 32],
            Option<(Fk, Option<(TxRecord, Vec<OutputRecord>, Vec<u32>)>)>,
        )>,
        u64,
    ),
    StoreError,
> {
    denserels_after(table, resolve_fk_and_range_uring(table, txids)?)
}

struct DrainOnDrop<'a>(&'a mut UringSession);
impl std::ops::Deref for DrainOnDrop<'_> {
    type Target = UringSession;
    fn deref(&self) -> &UringSession {
        self.0
    }
}
impl std::ops::DerefMut for DrainOnDrop<'_> {
    fn deref_mut(&mut self) -> &mut UringSession {
        self.0
    }
}
impl Drop for DrainOnDrop<'_> {
    fn drop(&mut self) {
        self.0.drain_all();
    }
}

// ── tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tx_table::{InputRecord, OutputRecord, TxRecord, TxTable};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn tmp(name: &str) -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let id = N.fetch_add(1, Ordering::Relaxed);
        let p = std::env::temp_dir().join(format!("rbitcoin-hq-{name}-{id}"));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn seed_table(n: u8) -> (PathBuf, TxTable, Vec<[u8; 32]>) {
        let dir = tmp("seed");
        let t = TxTable::create(&dir).unwrap();
        let mut items = Vec::new();
        let mut txids = Vec::new();
        for i in 0..n {
            let mut tid = [0u8; 32];
            tid[0] = i;
            tid[1] = 0xa5;
            tid[2] = 0x5a;
            txids.push(tid);
            items.push((
                TxRecord {
                    txid: tid,
                    version: 1,
                    locktime: 0,
                    input_start_fk: Fk::NULL,
                    input_count: 1,
                    output_start_fk: Fk::NULL,
                    output_count: 1,
                },
                vec![InputRecord::coinbase(u32::MAX, vec![], vec![])],
                vec![OutputRecord::unspent(
                    1000 + i as i64,
                    (0..((i as usize % 17) + 1)).map(|b| b as u8).collect(),
                )],
            ));
        }
        let _ = t.put_full_batch_indexed(&items, true).unwrap();
        (dir, t, txids)
    }

    #[test]
    fn uring_fk_and_range_matches_pread() {
        let (dir, t, txids) = seed_table(40);
        let pread = resolve_fk_and_range_pread(&t, &txids).unwrap();
        let via = resolve_fk_and_range_batch(&t, &txids).unwrap();
        assert_eq!(pread.len(), via.len());
        for (a, b) in pread.iter().zip(via.iter()) {
            assert_eq!(a.0, b.0);
            assert_eq!(a.1, b.1, "txid[0]={}", a.0[0]);
        }
        for (_tid, row) in &pread {
            if let Some((fk, range)) = row {
                assert_eq!(t.body.record_range(*fk).unwrap(), *range);
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn batch_hits_all_seeded() {
        let (dir, t, txids) = seed_table(24);
        let got = resolve_fk_and_range_batch(&t, &txids).unwrap();
        for (i, (_tid, row)) in got.iter().enumerate() {
            let (fk, range) = row.expect("seeded must resolve");
            assert!(range.1 > 0, "i={i}");
            assert_eq!(t.body.record_range(fk).unwrap(), range);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn miss_returns_none() {
        let (dir, t, _) = seed_table(5);
        let miss = resolve_fk_and_range_batch(&t, &[[0xff; 32]]).unwrap();
        assert_eq!(miss[0].1, None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Two creates of the same txid: deepest (second insert) must win (BIP30).
    #[test]
    fn deepest_create_wins() {
        let dir = tmp("bip30");
        let t = TxTable::create(&dir).unwrap();
        let txid = [0xcd; 32];
        let mk = |hint: u8| {
            (
                TxRecord {
                    txid,
                    version: 1,
                    locktime: 0,
                    input_start_fk: Fk::NULL,
                    input_count: 1,
                    output_start_fk: Fk::NULL,
                    output_count: 1,
                },
                vec![InputRecord {
                    prev_txid: [0u8; 32],
                    create_fk: Fk::NULL,
                    prev_index: u32::MAX,
                    sequence: u32::MAX,
                    script_sig: vec![hint],
                    witness: vec![],
                }],
                vec![OutputRecord::unspent(1, vec![0x51])],
            )
        };
        let _fk1 = t.put_full_batch_indexed(&[mk(1)], true).unwrap()[0];
        let fk2 = t.put_full_batch_indexed(&[mk(2)], true).unwrap()[0];
        let pread = resolve_fk_and_range_pread(&t, &[txid]).unwrap();
        let via = resolve_fk_and_range_batch(&t, &[txid]).unwrap();
        assert_eq!(pread[0].1.map(|(f, _)| f), Some(fk2));
        assert_eq!(via[0].1.map(|(f, _)| f), Some(fk2));
        assert_eq!(pread[0].1, via[0].1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn mixed_hit_and_miss_batch() {
        let (dir, t, txids) = seed_table(12);
        let mut q = txids.clone();
        q.push([0xee; 32]);
        q.push([0xef; 32]);
        let got = resolve_fk_and_range_batch(&t, &q).unwrap();
        for i in 0..12 {
            assert!(got[i].1.is_some(), "seeded i={i}");
        }
        assert_eq!(got[12].1, None);
        assert_eq!(got[13].1, None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn plan_body_range_idx_matches_record_range() {
        let (dir, t, _) = seed_table(20);
        let count = t.body.count();
        for id in 1..=count {
            let fk = Fk(id);
            let expected = t.body.record_range(fk).unwrap();
            let plan = t.body.plan_body_range_idx(fk).unwrap();
            let bufs: Vec<Vec<u8>> = plan
                .pages
                .iter()
                .map(|p| {
                    let mut b = vec![0u8; p.want];
                    let rc = unsafe {
                        libc::pread(
                            p.fd,
                            b.as_mut_ptr() as *mut libc::c_void,
                            p.want,
                            p.page_off as libc::off_t,
                        )
                    };
                    assert!(rc > 0);
                    b
                })
                .collect();
            let refs: Vec<&[u8]> = bufs.iter().map(|b| b.as_slice()).collect();
            assert_eq!(plan.decode_range(&refs).unwrap(), expected);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
