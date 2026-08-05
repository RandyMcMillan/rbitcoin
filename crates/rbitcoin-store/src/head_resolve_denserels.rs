//! Plan Shape A head resolve: **txids → (fk, body_range)** (or denserels after).
//!
//! **Segment-ordered fused machine** (schema 13+), one TLS [`UringSession`]:
//!
//! 1. **Open segment** (no fuse): group active keys by head page → up to 128 page
//!    slots; each slot: HEAD pread → hop_scan → depth-first STAGE_ID (`txid.body`)
//!    → STAGE_IDX (`tx.idx`) until every key on that page finishes for the segment.
//! 2. **Sealed newest→oldest:** fuse-gate remaining active keys, same page-slot
//!    lifecycle. Keys that already hit never load older head pages.
//!
//! Within a segment all cands for a key live on **one** 4 KiB page (SCHEMA).
//! BIP30: open first, sealed newest first; hop cands deepest-first.
//!
//! Slot rule: **one uring slot = one head page end-to-end** (all CQEs for that
//! page stay on that slot until every key on the page is done for the segment).
//!
//! Backend: `RBITCOIN_HEAD_RESOLVE_IO` / global `RBITCOIN_IO` (`uring` \| `pread`).

use crate::address_head::{
    h1_in_page, h2_in_page, hop_scan_page, page_base_for_txid, page_slot_count, MAX_PROBE,
    PROBE_REGION_BYTES,
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
use std::os::fd::RawFd;
use std::time::Instant;

/// Page slots = ring depth (one page lifecycle per slot).
const MAX_PAGE_SLOTS: usize = 128;

const STAGE_HEAD: u64 = 1;
const STAGE_ID: u64 = 2;
const STAGE_IDX: u64 = 3;

/// Per input key (batch-lifetime).
struct KeyState {
    /// Absolute create_fk cands for *current* segment page (deepest-first).
    cands: Vec<u64>,
    cand_i: usize,
    id_buf: [u8; 32],
    pending_fk: u64,
    /// Identity peeks issued for this key this resolve (for hit_rank).
    peeks: u32,
    idx_plan: Option<BodyRangeIdxPlan>,
    idx_bufs: Vec<Vec<u8>>,
    idx_page_i: u8,
    /// Still needs older segments (not yet identity-matched).
    active: bool,
}

impl KeyState {
    fn fresh() -> Self {
        Self {
            cands: Vec::new(),
            cand_i: 0,
            id_buf: [0u8; 32],
            pending_fk: 0,
            peeks: 0,
            idx_plan: None,
            idx_bufs: Vec::new(),
            idx_page_i: 0,
            active: true,
        }
    }

    fn clear_segment_cands(&mut self) {
        self.cands.clear();
        self.cand_i = 0;
        self.pending_fk = 0;
        self.idx_plan = None;
        self.idx_bufs.clear();
        self.idx_page_i = 0;
    }
}

/// One uring slot = one head page for the current segment wave.
struct PageSlot {
    occupied: bool,
    page_base: u64,
    head_buf: Vec<u8>,
    /// key_i values on this page.
    keys: Vec<u32>,
    /// Index into `keys` currently driving ID/IDX (serial within slot).
    key_pos: usize,
    /// Keys finished for this segment (match, empty cands, or cands exhausted).
    keys_finished: usize,
    phase: SlotPhase,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SlotPhase {
    Empty,
    HeadInFlight,
    /// Driving identity/idx for keys[key_pos..].
    KeyIo,
}

impl PageSlot {
    fn empty() -> Self {
        Self {
            occupied: false,
            page_base: 0,
            head_buf: vec![0u8; PROBE_REGION_BYTES],
            keys: Vec::new(),
            key_pos: 0,
            keys_finished: 0,
            phase: SlotPhase::Empty,
        }
    }

    fn clear(&mut self) {
        self.occupied = false;
        self.page_base = 0;
        self.keys.clear();
        self.key_pos = 0;
        self.keys_finished = 0;
        self.phase = SlotPhase::Empty;
    }
}

/// Stamp short-circuit: **txids → (fk, body_range)**.
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

/// Resolve many parent txids to create fk + denserels (plan Shape A full).
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

// ── shared helpers ──────────────────────────────────────────────────────────

fn mix_all(table: &TxTable, txids: &[[u8; 32]]) -> Vec<[u8; 32]> {
    txids.iter().map(|t| table.secret.mix_txid(t)).collect()
}

/// Group active key indices by head page base for this segment's bits.
fn page_groups(
    mixed: &[[u8; 32]],
    active: &[bool],
    bits: u32,
    fuse_pass: impl Fn(usize) -> bool,
) -> Vec<(u64, Vec<u32>)> {
    let mut order: Vec<(u64, u32)> = Vec::new();
    for (i, &a) in active.iter().enumerate() {
        if !a {
            continue;
        }
        if !fuse_pass(i) {
            continue;
        }
        let pb = page_base_for_txid(&mixed[i], bits);
        order.push((pb, i as u32));
    }
    order.sort_unstable_by_key(|&(p, k)| (p, k));
    let mut out: Vec<(u64, Vec<u32>)> = Vec::new();
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

fn hop_cands_abs(seg: &ResolveSeg, mixed: &[u8; 32], page: &[u8], es: u8, nslots: u64) -> Vec<u64> {
    let bits = seg.head.bits();
    let h1 = h1_in_page(mixed, bits);
    let h2 = h2_in_page(mixed, bits);
    let scan = hop_scan_page(page, es, h1, h2, nslots, MAX_PROBE);
    // Deepest-first (BIP30): reverse shallow→deep hop order.
    let mut abs = Vec::with_capacity(scan.cands.len());
    for &(_, rel) in scan.cands.iter().rev() {
        if let Some(fk) = seg.rel_to_abs(rel) {
            abs.push(fk.0);
        }
    }
    abs
}

// ── pread: same segment order, serial pages ─────────────────────────────────

fn resolve_fk_and_range_pread(
    table: &TxTable,
    txids: &[[u8; 32]],
) -> Result<Vec<([u8; 32], Option<(Fk, (u64, u64))>)>, StoreError> {
    crate::head_resolve_stats::add_keys(txids.len() as u64);
    let mixed = mix_all(table, txids);
    let side = table.txid_sidefile();
    let body_count = table.body.count();
    let mut winner: Vec<Option<(Fk, (u64, u64))>> = vec![None; txids.len()];
    let mut active = vec![true; txids.len()];
    let mut body_lookups = 0u64;
    let mut miss_peeks = 0u64;
    let mut id_ns = 0u64;
    let mut idx_ns = 0u64;
    let mut probe_ns = 0u64;
    let mut cands_total = 0u64;
    let mut any_active = true;

    let segs = table.head.resolve_segments();
    for seg in &segs {
        if !any_active {
            break;
        }
        let bits = seg.head.bits();
        let es = seg.head.entry_bytes();
        let page_slots = page_slot_count(bits);
        let groups = page_groups(&mixed, &active, bits, |i| seg.fuse_contains(&mixed[i]));
        if groups.is_empty() {
            continue;
        }
        let dc = seg.dontcache();
        let t_probe = Instant::now();
        for (page_base, keys) in groups {
            let need = seg.head.probe_page_need(page_base, page_slots);
            if need == 0 {
                continue;
            }
            let mut buf = vec![0u8; need];
            let rc = crate::bulk_io::pread_single(
                seg.head.read_fd(),
                seg.head.entry_off(page_base),
                &mut buf,
                dc,
            );
            if rc < 0 {
                return Err(StoreError::io(
                    seg.head.path(),
                    std::io::Error::from_raw_os_error(-rc),
                ));
            }
            if (rc as usize) < need {
                seg.head
                    .file_read_at(seg.head.entry_off(page_base), &mut buf)?;
            }

            let nslots = (need / es as usize) as u64;
            for &ki in &keys {
                if !active[ki as usize] {
                    continue;
                }
                let cands = hop_cands_abs(seg, &mixed[ki as usize], &buf, es, nslots);
                cands_total = cands_total.saturating_add(cands.len() as u64);
                let mut peeks = 0u32;
                for &fk_u in &cands {
                    if fk_u == 0 || fk_u > body_count {
                        continue;
                    }
                    let fk = Fk(fk_u);
                    peeks = peeks.saturating_add(1);
                    let t_id = Instant::now();
                    let got = match side.get(fk) {
                        Ok(t) => t,
                        Err(StoreError::NotFound) | Err(StoreError::InvalidFk) => {
                            id_ns = id_ns.saturating_add(t_id.elapsed().as_nanos() as u64);
                            miss_peeks = miss_peeks.saturating_add(1);
                            body_lookups = body_lookups.saturating_add(1);
                            continue;
                        }
                        Err(e) => return Err(e),
                    };
                    id_ns = id_ns.saturating_add(t_id.elapsed().as_nanos() as u64);
                    body_lookups = body_lookups.saturating_add(1);
                    if got != txids[ki as usize] {
                        miss_peeks = miss_peeks.saturating_add(1);
                        continue;
                    }
                    crate::head_resolve_stats::add_hit_rank(peeks as u64);
                    let t_idx = Instant::now();
                    match table.body.record_range(fk) {
                        Ok((off, len)) if len > 0 => {
                            winner[ki as usize] = Some((fk, (off, len)));
                            active[ki as usize] = false;
                        }
                        Ok(_) | Err(StoreError::NotFound) | Err(StoreError::Corrupt(_)) => {}
                        Err(e) => return Err(e),
                    }
                    idx_ns = idx_ns.saturating_add(t_idx.elapsed().as_nanos() as u64);
                    break;
                }
            }
        }
        probe_ns = probe_ns.saturating_add(t_probe.elapsed().as_nanos() as u64);
        any_active = active.iter().any(|&a| a);
    }

    crate::head_resolve_stats::add_probe(probe_ns);
    crate::head_resolve_stats::add_body(id_ns);
    crate::head_resolve_stats::add_idx(idx_ns);
    crate::head_resolve_stats::add_cands(cands_total);
    crate::head_resolve_stats::add_body_lookups(body_lookups);
    crate::head_resolve_stats::add_miss_peeks(miss_peeks);

    Ok(txids
        .iter()
        .enumerate()
        .map(|(i, t)| (*t, winner[i]))
        .collect())
}

fn resolve_denserels_pread(
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
    denserels_after_ranges(table, resolve_fk_and_range_pread(table, txids)?)
}

fn denserels_after_ranges(
    table: &TxTable,
    ranges: Vec<([u8; 32], Option<(Fk, (u64, u64))>)>,
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
    let mut dens_ns = 0u64;
    let mut dens_decoded: std::collections::HashMap<
        usize,
        (TxRecord, Vec<OutputRecord>, Vec<u32>),
    > = std::collections::HashMap::new();

    let mut need: Vec<(usize, Fk, (u64, u64))> = Vec::new();
    for (i, (_tid, row)) in ranges.iter().enumerate() {
        if let Some((fk, range)) = row {
            need.push((i, *fk, *range));
        }
    }
    if !need.is_empty() {
        let t_dens = Instant::now();
        let mut jobs: Vec<IdxBodyJob> = need
            .iter()
            .map(|(_, fk, range)| IdxBodyJob::new(fk.0, Some(*range)))
            .collect();
        run_idx_body_pipeline(&table.body, &mut jobs, BodyMode::OutsDenserels)?;
        dens_ns = t_dens.elapsed().as_nanos() as u64;
        for ((ki, fk, _), job) in need.into_iter().zip(jobs.into_iter()) {
            if !job.ok || job.body.is_empty() {
                continue;
            }
            match decode_packed_tx_outs_with_spender_rels_secret(&job.body, Some(&table.secret)) {
                Ok(mut decoded) => {
                    if let Ok(tid) = table.txid_sidefile().get(fk) {
                        decoded.0.txid = tid;
                    }
                    dens_decoded.insert(ki, decoded);
                }
                Err(StoreError::NotFound) | Err(StoreError::Corrupt(_)) => {}
                Err(e) => return Err(e),
            }
        }
    }

    let mut out = Vec::with_capacity(ranges.len());
    for (i, (txid, row)) in ranges.into_iter().enumerate() {
        let mapped = row.map(|(fk, _range)| (fk, dens_decoded.remove(&i)));
        out.push((txid, mapped));
    }
    Ok((out, dens_ns))
}

// ── uring machine ───────────────────────────────────────────────────────────

fn resolve_fk_and_range_uring(
    table: &TxTable,
    txids: &[[u8; 32]],
) -> Result<Vec<([u8; 32], Option<(Fk, (u64, u64))>)>, StoreError> {
    crate::head_resolve_stats::add_keys(txids.len() as u64);
    uring_session::with_thread_local(uring_session::DEFAULT_ENTRIES, |session| {
        resolve_fk_and_range_uring_on(session, table, txids)
    })?
}

fn resolve_fk_and_range_uring_on(
    session: &mut UringSession,
    table: &TxTable,
    txids: &[[u8; 32]],
) -> Result<Vec<([u8; 32], Option<(Fk, (u64, u64))>)>, StoreError> {
    let mixed = mix_all(table, txids);
    let side = table.txid_sidefile();
    let side_fd = side.body_read_fd();
    let side_path = side.file_path().to_path_buf();
    let body_count = table.body.count();
    let side_n = side.count();

    let mut winner: Vec<Option<(Fk, (u64, u64))>> = vec![None; txids.len()];
    let mut keys: Vec<KeyState> = (0..txids.len()).map(|_| KeyState::fresh()).collect();
    let mut body_lookups = 0u64;
    let mut miss_peeks = 0u64;
    let mut id_ns = 0u64;
    let mut idx_ns = 0u64;
    let mut probe_ns = 0u64;
    let mut cands_total = 0u64;

    // Slot buffers live for the whole call; DrainSessionOnDrop drains before drop.
    let mut slots: Vec<PageSlot> = (0..MAX_PAGE_SLOTS).map(|_| PageSlot::empty()).collect();
    let mut ring = DrainSessionOnDrop(session);

    let segs = table.head.resolve_segments();
    for seg in &segs {
        if !keys.iter().any(|k| k.active) {
            break;
        }
        let active_flags: Vec<bool> = keys.iter().map(|k| k.active).collect();
        let mut page_q = page_groups(&mixed, &active_flags, seg.head.bits(), |i| {
            seg.fuse_contains(&mixed[i])
        });
        if page_q.is_empty() {
            continue;
        }
        // Process queue from front.
        page_q.reverse(); // pop from end as stack; reverse so first page is last
        let mut free: Vec<usize> = (0..MAX_PAGE_SLOTS).collect();
        for s in slots.iter_mut() {
            s.clear();
        }

        let t_seg = Instant::now();
        arm_pages(
            &mut ring,
            seg,
            &mut page_q,
            &mut free,
            &mut slots,
        )?;
        ring.sync_submission();
        let _ = ring.submit();

        while free.len() < MAX_PAGE_SLOTS || !page_q.is_empty() {
            let mut cqes = ring.harvest_ready();
            if cqes.is_empty() {
                if ring.in_flight() == 0 {
                    // Free any pages that finished without a trailing CQE.
                    for s in 0..MAX_PAGE_SLOTS {
                        if slots[s].occupied
                            && slots[s].keys_finished >= slots[s].keys.len()
                        {
                            slots[s].clear();
                            free.push(s);
                        }
                    }
                    if page_q.is_empty() && free.len() == MAX_PAGE_SLOTS {
                        break;
                    }
                    // Occupied unfinished page + zero in-flight: re-arm ID/IDX.
                    for s in 0..MAX_PAGE_SLOTS {
                        if slots[s].occupied
                            && slots[s].keys_finished < slots[s].keys.len()
                        {
                            arm_next_key_or_idle(
                                &mut slots[s],
                                &mut keys,
                                &mut ring,
                                s as u32,
                                side,
                                side_fd,
                                body_count,
                                side_n,
                                &mut id_ns,
                            )?;
                            // After re-arm, page may be fully finished (invalid cands).
                            if slots[s].occupied
                                && slots[s].keys_finished >= slots[s].keys.len()
                            {
                                slots[s].clear();
                                free.push(s);
                            }
                        }
                    }
                    arm_pages(&mut ring, seg, &mut page_q, &mut free, &mut slots)?;
                    ring.sync_submission();
                    let _ = ring.submit();
                    if ring.in_flight() == 0 {
                        // Still nothing to wait on: free complete pages again.
                        for s in 0..MAX_PAGE_SLOTS {
                            if slots[s].occupied
                                && slots[s].keys_finished >= slots[s].keys.len()
                            {
                                slots[s].clear();
                                free.push(s);
                            }
                        }
                        if free.len() == MAX_PAGE_SLOTS && page_q.is_empty() {
                            break;
                        }
                        if free.len() < MAX_PAGE_SLOTS {
                            return Err(StoreError::Corrupt(
                                "head resolve: page slot stuck with no in-flight SQE",
                            ));
                        }
                    }
                }
                if ring.in_flight() > 0 {
                    ring.submit_and_wait_one()?;
                    cqes = ring.harvest_ready();
                }
            }

            for (ud, res) in cqes {
                let (kind, slot_u) = uring_session::unpack_ud(ud);
                let s = slot_u as usize;
                if s >= slots.len() || !slots[s].occupied {
                    return Err(StoreError::Corrupt("head resolve bad page slot"));
                }

                match kind {
                    STAGE_HEAD => {
                        on_head_cqe(
                            table,
                            txids,
                            &mixed,
                            seg,
                            &mut slots[s],
                            &mut keys,
                            res,
                            &mut cands_total,
                            &mut ring,
                            s as u32,
                            side,
                            side_fd,
                            body_count,
                            side_n,
                            &mut id_ns,
                        )?;
                    }
                    STAGE_ID => {
                        id_ns = id_ns.saturating_add(0); // wait attributed loosely
                        body_lookups = body_lookups.saturating_add(1);
                        on_id_cqe(
                            table,
                            txids,
                            &mut slots[s],
                            &mut keys,
                            res,
                            &mut ring,
                            s as u32,
                            side,
                            side_fd,
                            &side_path,
                            body_count,
                            side_n,
                            &mut id_ns,
                            &mut idx_ns,
                            &mut winner,
                            &mut miss_peeks,
                        )?;
                    }
                    STAGE_IDX => {
                        on_idx_cqe(
                            table,
                            &mut slots[s],
                            &mut keys,
                            res,
                            &mut ring,
                            s as u32,
                            side,
                            side_fd,
                            body_count,
                            side_n,
                            &mut id_ns,
                            &mut idx_ns,
                            &mut winner,
                        )?;
                    }
                    _ => return Err(StoreError::Corrupt("head resolve bad stage")),
                }

                // Page finished?
                if slots[s].occupied
                    && slots[s].keys_finished >= slots[s].keys.len()
                    && slots[s].phase != SlotPhase::HeadInFlight
                {
                    slots[s].clear();
                    free.push(s);
                }
            }

            arm_pages(
                &mut ring,
                seg,
                &mut page_q,
                &mut free,
                &mut slots,
            )?;
            ring.sync_submission();
            let _ = ring.submit();
        }
        probe_ns = probe_ns.saturating_add(t_seg.elapsed().as_nanos() as u64);
    }

    drop(ring);

    crate::head_resolve_stats::add_probe(probe_ns);
    crate::head_resolve_stats::add_body(id_ns);
    crate::head_resolve_stats::add_idx(idx_ns);
    crate::head_resolve_stats::add_cands(cands_total);
    crate::head_resolve_stats::add_body_lookups(body_lookups);
    crate::head_resolve_stats::add_miss_peeks(miss_peeks);

    Ok(txids
        .iter()
        .enumerate()
        .map(|(i, t)| (*t, winner[i]))
        .collect())
}

fn arm_pages(
    session: &mut UringSession,
    seg: &ResolveSeg,
    page_q: &mut Vec<(u64, Vec<u32>)>,
    free: &mut Vec<usize>,
    slots: &mut [PageSlot],
) -> Result<(), StoreError> {
    let bits = seg.head.bits();
    let page_slots = page_slot_count(bits);
    let dc = seg.dontcache();
    let rw_flags = if dc && crate::bulk_io::rwf_dontcache_ok() {
        uring_session::RWF_DONTCACHE
    } else {
        0
    };
    let fd = seg.head.read_fd();

    while !free.is_empty() && !page_q.is_empty() && session.free_sq() > 0 {
        let (page_base, keys) = page_q.pop().unwrap();
        let need = seg.head.probe_page_need(page_base, page_slots);
        if need == 0 {
            // No bytes — treat all keys as no cands (done for segment).
            continue;
        }
        let s = free.pop().unwrap();
        let slot = &mut slots[s];
        slot.occupied = true;
        slot.page_base = page_base;
        slot.keys = keys;
        slot.key_pos = 0;
        slot.keys_finished = 0;
        slot.phase = SlotPhase::HeadInFlight;
        if slot.head_buf.len() < need {
            slot.head_buf.resize(need, 0);
        }
        slot.head_buf[..need].fill(0);
        let off = seg.head.entry_off(page_base);
        let ud = uring_session::pack_ud(STAGE_HEAD, s as u32);
        session.push_pread_flags(fd, off, &mut slot.head_buf[..need], ud, rw_flags)?;
    }
    let _ = bits;
    Ok(())
}

fn on_head_cqe(
    table: &TxTable,
    txids: &[[u8; 32]],
    mixed: &[[u8; 32]],
    seg: &ResolveSeg,
    slot: &mut PageSlot,
    keys: &mut [KeyState],
    res: i32,
    cands_total: &mut u64,
    session: &mut UringSession,
    slot_id: u32,
    side: &TxidBody,
    side_fd: RawFd,
    body_count: u64,
    side_n: u64,
    id_ns: &mut u64,
) -> Result<(), StoreError> {
    let bits = seg.head.bits();
    let es = seg.head.entry_bytes();
    let page_slots = page_slot_count(bits);
    let need = seg.head.probe_page_need(slot.page_base, page_slots);
    let path = seg.head.path();

    if res < 0 {
        if res == -95 && crate::bulk_io::rwf_dontcache_ok() {
            crate::bulk_io::note_rwf_dontcache_unsupported();
            let off = seg.head.entry_off(slot.page_base);
            slot.head_buf[..need].fill(0);
            let ud = uring_session::pack_ud(STAGE_HEAD, slot_id);
            session.push_pread_flags(seg.head.read_fd(), off, &mut slot.head_buf[..need], ud, 0)?;
            return Ok(());
        }
        return Err(StoreError::io(
            path,
            std::io::Error::from_raw_os_error(-res),
        ));
    }

    let mut n = res as usize;
    if n < need {
        seg.head
            .file_read_at(seg.head.entry_off(slot.page_base), &mut slot.head_buf[..need])?;
        n = need;
    }
    n = n.min(need);
    let es_u = es as usize;
    if n < es_u {
        // Empty page — all keys done for segment.
        slot.keys_finished = slot.keys.len();
        slot.phase = SlotPhase::KeyIo;
        return Ok(());
    }
    let nslots = (n / es_u) as u64;
    let page = &slot.head_buf[..n];

    for &ki in &slot.keys {
        let k = &mut keys[ki as usize];
        if !k.active {
            slot.keys_finished += 1;
            continue;
        }
        k.clear_segment_cands();
        k.cands = hop_cands_abs(seg, &mixed[ki as usize], page, es, nslots);
        *cands_total = cands_total.saturating_add(k.cands.len() as u64);
        if k.cands.is_empty() {
            slot.keys_finished += 1;
        }
    }

    slot.phase = SlotPhase::KeyIo;
    slot.key_pos = 0;
    // Arm first identity SQE; keys with empty/invalid cands are finished here.
    // Critical: if submit_next_id returns false (all cands invalid), we must
    // finish that key and try the next — otherwise the slot stays occupied
    // with zero in-flight SQEs and the segment wave hangs forever.
    arm_next_key_or_idle(
        slot,
        keys,
        session,
        slot_id,
        side,
        side_fd,
        body_count,
        side_n,
        id_ns,
    )?;
    let _ = (table, txids);
    Ok(())
}

fn on_id_cqe(
    table: &TxTable,
    txids: &[[u8; 32]],
    slot: &mut PageSlot,
    keys: &mut [KeyState],
    res: i32,
    session: &mut UringSession,
    slot_id: u32,
    side: &TxidBody,
    side_fd: RawFd,
    side_path: &std::path::Path,
    body_count: u64,
    side_n: u64,
    id_ns: &mut u64,
    idx_ns: &mut u64,
    _winner: &mut [Option<(Fk, (u64, u64))>],
    miss_peeks: &mut u64,
) -> Result<(), StoreError> {
    let ki = slot.keys[slot.key_pos] as usize;
    let need_arm: bool;

    {
        let k = &mut keys[ki];
        if res < 0 {
            if res == -95 && crate::bulk_io::rwf_dontcache_ok() {
                crate::bulk_io::note_rwf_dontcache_unsupported();
                let off = TxidBody::entry_offset(k.pending_fk)?;
                k.id_buf = [0u8; 32];
                let ud = uring_session::pack_ud(STAGE_ID, slot_id);
                session.push_pread_flags(side_fd, off, &mut k.id_buf, ud, 0)?;
                return Ok(());
            }
            return Err(StoreError::io(
                side_path,
                std::io::Error::from_raw_os_error(-res),
            ));
        }

        if (res as usize) != TXID_ENTRY_LEN as usize || k.id_buf != txids[ki] {
            if (res as usize) == TXID_ENTRY_LEN as usize {
                *miss_peeks = miss_peeks.saturating_add(1);
            }
            need_arm = if !submit_next_id(
                k, session, side, side_fd, body_count, side_n, slot_id, id_ns,
            )? {
                finish_key_on_page(slot);
                true
            } else {
                false
            };
        } else {
            // Identity hit → plan idx.
            crate::head_resolve_stats::add_hit_rank(k.peeks.max(1) as u64);
            let fk = Fk(k.pending_fk);
            let t_idx = Instant::now();
            match table.body.plan_body_range_idx(fk) {
                Ok(plan) => {
                    *idx_ns = idx_ns.saturating_add(t_idx.elapsed().as_nanos() as u64);
                    if plan.pages.is_empty() {
                        finish_key_on_page(slot);
                        need_arm = true;
                    } else {
                        k.idx_bufs = plan.pages.iter().map(|p| vec![0u8; p.want]).collect();
                        k.idx_plan = Some(plan);
                        k.idx_page_i = 0;
                        submit_idx_page(k, session, slot_id)?;
                        need_arm = false;
                    }
                }
                Err(StoreError::NotFound)
                | Err(StoreError::Corrupt(_))
                | Err(StoreError::InvalidFk) => {
                    *idx_ns = idx_ns.saturating_add(t_idx.elapsed().as_nanos() as u64);
                    need_arm = if !submit_next_id(
                        k, session, side, side_fd, body_count, side_n, slot_id, id_ns,
                    )? {
                        finish_key_on_page(slot);
                        true
                    } else {
                        false
                    };
                }
                Err(e) => return Err(e),
            }
        }
    }

    if need_arm {
        arm_next_key_or_idle(
            slot, keys, session, slot_id, side, side_fd, body_count, side_n, id_ns,
        )?;
    }
    Ok(())
}

fn on_idx_cqe(
    _table: &TxTable,
    slot: &mut PageSlot,
    keys: &mut [KeyState],
    res: i32,
    session: &mut UringSession,
    slot_id: u32,
    side: &TxidBody,
    side_fd: RawFd,
    body_count: u64,
    side_n: u64,
    id_ns: &mut u64,
    idx_ns: &mut u64,
    winner: &mut [Option<(Fk, (u64, u64))>],
) -> Result<(), StoreError> {
    let ki = slot.keys[slot.key_pos] as usize;
    let need_arm: bool;

    {
        let k = &mut keys[ki];
        let page_i = k.idx_page_i as usize;
        let plan = k.idx_plan.as_ref().expect("STAGE_IDX without plan");
        let page = &plan.pages[page_i];
        let want = page.want;
        let fd = page.fd;
        let off = page.page_off;
        let rw_flags = page.rw_flags;
        let n_pages = plan.pages.len();

        if res < 0 {
            if res == -95 && crate::bulk_io::rwf_dontcache_ok() && rw_flags != 0 {
                crate::bulk_io::note_rwf_dontcache_unsupported();
                let ud = uring_session::pack_ud(STAGE_IDX, slot_id);
                k.idx_bufs[page_i].fill(0);
                session.push_pread_flags(fd, off, &mut k.idx_bufs[page_i], ud, 0)?;
                return Ok(());
            }
            k.idx_plan = None;
            k.idx_bufs.clear();
            k.idx_page_i = 0;
            need_arm = if !submit_next_id(
                k, session, side, side_fd, body_count, side_n, slot_id, id_ns,
            )? {
                finish_key_on_page(slot);
                true
            } else {
                false
            };
        } else {
            let short_fail = if (res as usize) < want {
                let buf = &mut k.idx_bufs[page_i];
                let rc = unsafe {
                    libc::pread(
                        fd,
                        buf.as_mut_ptr() as *mut libc::c_void,
                        want,
                        off as libc::off_t,
                    )
                };
                rc < 0 || (rc as usize) < want
            } else {
                false
            };

            if short_fail {
                k.idx_plan = None;
                k.idx_bufs.clear();
                k.idx_page_i = 0;
                need_arm = if !submit_next_id(
                    k, session, side, side_fd, body_count, side_n, slot_id, id_ns,
                )? {
                    finish_key_on_page(slot);
                    true
                } else {
                    false
                };
            } else if page_i + 1 < n_pages {
                k.idx_page_i = (page_i + 1) as u8;
                submit_idx_page(k, session, slot_id)?;
                need_arm = false;
            } else {
                let t0 = Instant::now();
                let page_refs: Vec<&[u8]> = k.idx_bufs.iter().map(|b| b.as_slice()).collect();
                let plan_ref = k.idx_plan.as_ref().unwrap();
                let range = match plan_ref.decode_range(&page_refs) {
                    Ok((o, len)) if len > 0 => Some((o, len)),
                    Ok(_) | Err(StoreError::Corrupt(_)) => None,
                    Err(e) => return Err(e),
                };
                *idx_ns = idx_ns.saturating_add(t0.elapsed().as_nanos() as u64);
                let fk = Fk(k.pending_fk);

                if let Some(r) = range {
                    winner[ki] = Some((fk, r));
                    k.active = false;
                    k.idx_plan = None;
                    k.idx_bufs.clear();
                    finish_key_on_page(slot);
                    need_arm = true;
                } else {
                    k.idx_plan = None;
                    k.idx_bufs.clear();
                    k.idx_page_i = 0;
                    need_arm = if !submit_next_id(
                        k, session, side, side_fd, body_count, side_n, slot_id, id_ns,
                    )? {
                        finish_key_on_page(slot);
                        true
                    } else {
                        false
                    };
                }
            }
        }
    }

    if need_arm {
        arm_next_key_or_idle(
            slot, keys, session, slot_id, side, side_fd, body_count, side_n, id_ns,
        )?;
    }
    Ok(())
}

/// Current key finished for this segment (hit, miss, or empty cands).
fn finish_key_on_page(slot: &mut PageSlot) {
    slot.keys_finished = slot.keys_finished.saturating_add(1);
    slot.key_pos = slot.key_pos.saturating_add(1);
}

/// Arm identity for the next unfinished key on this page, or leave slot idle.
///
/// Empty-cand / inactive keys were already counted in `keys_finished` at hop.
/// Keys with only invalid fks (0 / OOB) get finished here when submit fails.
fn arm_next_key_or_idle(
    slot: &mut PageSlot,
    keys: &mut [KeyState],
    session: &mut UringSession,
    slot_id: u32,
    side: &TxidBody,
    side_fd: RawFd,
    body_count: u64,
    side_n: u64,
    id_ns: &mut u64,
) -> Result<(), StoreError> {
    while slot.key_pos < slot.keys.len() {
        let ki = slot.keys[slot.key_pos] as usize;
        if !keys[ki].active || keys[ki].cands.is_empty() {
            // Already counted finished at hop — skip past.
            slot.key_pos = slot.key_pos.saturating_add(1);
            continue;
        }
        if keys[ki].cand_i >= keys[ki].cands.len() {
            // Exhausted without a prior finish_key_on_page (defensive).
            finish_key_on_page(slot);
            continue;
        }
        if submit_next_id(
            &mut keys[ki],
            session,
            side,
            side_fd,
            body_count,
            side_n,
            slot_id,
            id_ns,
        )? {
            return Ok(());
        }
        // All remaining cands invalid (0 / OOB) — finish key, try next.
        finish_key_on_page(slot);
    }
    Ok(())
}

fn submit_next_id(
    k: &mut KeyState,
    session: &mut UringSession,
    side: &TxidBody,
    side_fd: RawFd,
    body_count: u64,
    side_n: u64,
    slot_id: u32,
    id_ns: &mut u64,
) -> Result<bool, StoreError> {
    let _ = side;
    while k.cand_i < k.cands.len() {
        let fk = k.cands[k.cand_i];
        k.cand_i += 1;
        if fk == 0 || fk > body_count {
            continue;
        }
        let t0 = Instant::now();
        let off = match TxidBody::entry_offset(fk) {
            Ok(o) => o,
            Err(_) => {
                *id_ns = id_ns.saturating_add(t0.elapsed().as_nanos() as u64);
                continue;
            }
        };
        *id_ns = id_ns.saturating_add(t0.elapsed().as_nanos() as u64);
        k.pending_fk = fk;
        k.peeks = k.peeks.saturating_add(1);
        k.id_buf = [0u8; 32];
        let ud = uring_session::pack_ud(STAGE_ID, slot_id);
        let rw_flags = crate::dontcache_policy::sidefile_sqe_rw_flags(fk, side_n);
        session.push_pread_flags(side_fd, off, &mut k.id_buf, ud, rw_flags)?;
        return Ok(true);
    }
    Ok(false)
}

fn submit_idx_page(
    k: &mut KeyState,
    session: &mut UringSession,
    slot_id: u32,
) -> Result<(), StoreError> {
    let page_i = k.idx_page_i as usize;
    let plan = k.idx_plan.as_ref().expect("idx plan");
    let page = &plan.pages[page_i];
    let ud = uring_session::pack_ud(STAGE_IDX, slot_id);
    let buf = &mut k.idx_bufs[page_i];
    buf.fill(0);
    session.push_pread_flags(page.fd, page.page_off, buf, ud, page.rw_flags)?;
    Ok(())
}

fn resolve_denserels_uring(
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
    denserels_after_ranges(table, resolve_fk_and_range_uring(table, txids)?)
}

// ── AddressHead file read helper (avoid private field) ──────────────────────

trait HeadFileRead {
    fn file_read_at(&self, off: u64, buf: &mut [u8]) -> Result<(), StoreError>;
}

impl HeadFileRead for crate::address_head::AddressHead {
    fn file_read_at(&self, off: u64, buf: &mut [u8]) -> Result<(), StoreError> {
        // path + pread via existing public API if any — use read_fd + pread_single.
        let rc = crate::bulk_io::pread_single(self.read_fd(), off, buf, false);
        if rc < 0 {
            return Err(StoreError::io(
                self.path(),
                std::io::Error::from_raw_os_error(-rc),
            ));
        }
        if (rc as usize) < buf.len() {
            // Incomplete — error
            return Err(StoreError::io(
                self.path(),
                std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "short head page"),
            ));
        }
        Ok(())
    }
}

/// Drain the session on drop while caller-held SQE buffers are still live.
struct DrainSessionOnDrop<'a>(&'a mut UringSession);

impl std::ops::Deref for DrainSessionOnDrop<'_> {
    type Target = UringSession;
    fn deref(&self) -> &UringSession {
        self.0
    }
}
impl std::ops::DerefMut for DrainSessionOnDrop<'_> {
    fn deref_mut(&mut self) -> &mut UringSession {
        self.0
    }
}
impl Drop for DrainSessionOnDrop<'_> {
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
        let p = std::env::temp_dir().join(format!("rbitcoin-head-res-{name}-{id}"));
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
            let tx = TxRecord {
                txid: tid,
                version: 1,
                locktime: 0,
                input_start_fk: Fk::NULL,
                input_count: 1,
                output_start_fk: Fk::NULL,
                output_count: 1,
            };
            let script: Vec<u8> = (0..((i as usize % 17) + 1)).map(|b| b as u8).collect();
            items.push((
                tx,
                vec![InputRecord::coinbase(u32::MAX, vec![], vec![])],
                vec![OutputRecord::unspent(1000 + i as i64, script)],
            ));
        }
        let _fks = t.put_full_batch_indexed(&items, true).unwrap();
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
    fn plan_body_range_idx_matches_record_range() {
        let (dir, t, _txids) = seed_table(20);
        let count = t.body.count();
        for id in 1..=count {
            let fk = Fk(id);
            let expected = t.body.record_range(fk).unwrap();
            let plan = t.body.plan_body_range_idx(fk).unwrap();
            assert!(!plan.pages.is_empty());
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
                    assert!(rc > 0, "pread idx page");
                    b
                })
                .collect();
            let refs: Vec<&[u8]> = bufs.iter().map(|b| b.as_slice()).collect();
            let got = plan.decode_range(&refs).unwrap();
            assert_eq!(got, expected, "fk={fk:?}");
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

    #[test]
    fn batch_hits_all_seeded_txids() {
        let (dir, t, txids) = seed_table(24);
        let got = resolve_fk_and_range_batch(&t, &txids).unwrap();
        for (i, (_tid, row)) in got.iter().enumerate() {
            let (fk, range) = row.expect("seeded txid must resolve");
            assert!(range.1 > 0, "i={i}");
            assert_eq!(t.body.record_range(fk).unwrap(), range);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
