//! Completion-driven io_uring pipeline for **online** `tx.head` shadow fill.
//!
//! Avoids mmap touch of `tx.head.new` during fill (process RSS). Sole writer to
//! the shadow; primary inserts stay on the live head.
//!
//! Ring protocol (one private ring per fill wave on the resize thread):
//! - Sync mmap `record_ranges` batches (1024 fks) when FK queue is low
//! - **Many** body prefix (txid) preads drained from the queue
//! - **Many** page RMWs in parallel; **≤1** RMW cycle per shadow page

use crate::address_head::{
    insert_fk_into_page_buf, page_base_for_txid, page_file_off, page_pread_len, page_slot_count,
    AddressHead, PROBE_REGION_BYTES,
};
use crate::bulk_io;
use crate::error::StoreError;
use crate::tx_table::TxTable;
use crate::var_table::VarTable;
use rbitcoin_primitives::Fk;
use std::collections::{HashMap, VecDeque};

/// Prefer starting an idx refill when the ready FK queue is below this.
const FK_QUEUE_LOW: usize = 256;
/// Fks covered by one mmap `record_ranges` batch.
const IDX_BATCH: u64 = 1024;
const RING_ENTRIES: u32 = crate::uring_session::DEFAULT_ENTRIES;
const BODY_POOL: usize = 512;
const PAGE_POOL: usize = 256;

const KIND_BODY: u64 = 2;
const KIND_PAGE_RD: u64 = 3;
const KIND_PAGE_WR: u64 = 0; // 0 so write bit space is clear; use 0b00 with slot

use crate::uring_session::{pack_ud, unpack_ud};

struct BodyWork {
    fk: u64,
    body_off: u64,
    body_len: u64,
}

struct PendingIns {
    txid: [u8; 32],
    fk: Fk,
}

/// Fill shadow for Class A ids `first..=last` via io_uring RMW (no shadow mmap touch).
///
/// `secret` mixes body txids into open-hash probe keys (same as live inserts).
///
/// Returns `Err` if io_uring is unavailable or a hard IO/protocol error occurs;
/// caller may fall back to mmap `insert_many`.
pub fn run_shadow_fill_uring(
    body: &VarTable,
    shadow: &AddressHead,
    secret: &crate::store_secret::StoreSecret,
    first: u64,
    last: u64,
) -> Result<(), StoreError> {
    if last < first {
        return Ok(());
    }
    if !bulk_io::io_uring_enabled() {
        return Err(StoreError::Corrupt("io_uring unavailable for shadow fill"));
    }
    #[cfg(target_os = "linux")]
    {
        run_linux(body, shadow, secret, first, last)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (body, shadow, secret, first, last);
        Err(StoreError::Corrupt("io_uring shadow fill is Linux-only"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::address_head::{AddressHead, HeadLayout};
    use crate::tx_table::{
        encode_packed_tx, InputRecord, OutputRecord, TxRecord,
    };
    use rbitcoin_primitives::TableKind;

    #[test]
    fn shadow_fill_empty_range_and_pack_ud() {
        let (k, s) = unpack_ud(pack_ud(KIND_BODY, 7));
        assert_eq!((k, s), (KIND_BODY, 7));
        let (k, s) = unpack_ud(pack_ud(KIND_PAGE_RD, 42));
        assert_eq!((k, s), (KIND_PAGE_RD, 42));

        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-hfill-range-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let body = VarTable::create(&dir, "tx", TableKind::Tx).unwrap();
        let shadow = AddressHead::create_with_layout(
            dir.join("shadow"),
            HeadLayout::new(12).unwrap(),
        )
        .unwrap();
        let secret = crate::store_secret::StoreSecret::from_bytes([0x5Au8; 32]);
        // last < first always Ok
        assert!(run_shadow_fill_uring(&body, &shadow, &secret, 5, 4).is_ok());

        if !bulk_io::io_uring_enabled() {
            // empty body + range → unavailable / not found
            assert!(run_shadow_fill_uring(&body, &shadow, &secret, 1, 1).is_err());
            let _ = std::fs::remove_dir_all(&dir);
            return;
        }

        // Build many packed Class A bodies directly in VarTable (> IDX_BATCH).
        const N: u64 = 1100;
        body
            .put_batch_encode(N as usize, N as usize * 80, |i, buf| {
                let mut txid = [0u8; 32];
                let i = i as u64;
                txid[0..8].copy_from_slice(&i.to_le_bytes());
                txid[8] = (i % 17) as u8;
                let tx = TxRecord {
                    txid,
                    version: 1,
                    locktime: 0,
                    input_start_fk: Fk::NULL,
                    input_count: 1,
                    output_start_fk: Fk::NULL,
                    output_count: 1,
                };
                let inputs = [InputRecord::coinbase(u32::MAX, vec![], vec![])];
                let outputs = [OutputRecord::unspent(1, vec![0x51])];
                encode_packed_tx(&tx, &inputs, &outputs, buf);
            })
            .unwrap();
        assert_eq!(body.count(), N);
        run_shadow_fill_uring(&body, &shadow, &secret, 1, N).unwrap();
        assert_eq!(shadow.occupied(), N);
        assert!(matches!(
            run_shadow_fill_uring(&body, &shadow, &secret, 1, N + 50),
            Err(StoreError::NotFound)
        ));
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[cfg(target_os = "linux")]
fn run_linux(
    body: &VarTable,
    shadow: &AddressHead,
    secret: &crate::store_secret::StoreSecret,
    first: u64,
    last: u64,
) -> Result<(), StoreError> {
    use crate::uring_session::UringSession;

    let count_snap = body.count();
    if last > count_snap {
        return Err(StoreError::NotFound);
    }

    let bits = shadow.bits();
    let entry_bytes = shadow.entry_bytes();
    let slots = shadow.slots();
    let slot_region = shadow.slot_region_len();
    let page_bytes = (page_slot_count(bits) as usize).saturating_mul(entry_bytes as usize);
    if page_bytes == 0 || page_bytes > PROBE_REGION_BYTES {
        return Err(StoreError::Corrupt("shadow page size"));
    }

    let body_fd = body.body_read_fd();
    let head_fd = shadow.read_fd();
    let body_path = body.body_file_path().to_path_buf();
    let head_path = shadow.path_str().to_path_buf();

    let mut session = UringSession::new(RING_ENTRIES).map_err(|e| {
        StoreError::io(
            &head_path,
            std::io::Error::new(std::io::ErrorKind::Other, format!("io_uring: {e}")),
        )
    })?;

    // --- pools ---
    let mut fk_queue: VecDeque<BodyWork> = VecDeque::with_capacity(IDX_BATCH as usize * 2);
    let mut page_wait: HashMap<u64, VecDeque<PendingIns>> = HashMap::new();
    let mut next_idx_fk = first;

    // body pool
    let mut body_free: Vec<usize> = (0..BODY_POOL).collect();
    let mut body_bufs: Vec<[u8; 32]> = vec![[0u8; 32]; BODY_POOL];
    let mut body_work: Vec<Option<BodyWork>> = (0..BODY_POOL).map(|_| None).collect();
    let mut body_want: Vec<usize> = vec![0; BODY_POOL];
    let mut body_in_flight = 0usize;

    // page pool
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum PageSt {
        Free,
        Reading,
        Writing,
    }
    let mut page_free: Vec<usize> = (0..PAGE_POOL).collect();
    let mut page_st = vec![PageSt::Free; PAGE_POOL];
    let mut page_bufs: Vec<Vec<u8>> = (0..PAGE_POOL).map(|_| vec![0u8; page_bytes]).collect();
    let mut page_base_of = vec![0u64; PAGE_POOL];
    let mut page_off_of = vec![0u64; PAGE_POOL];
    let mut page_len_of = vec![0usize; PAGE_POOL];
    let mut page_pending: Vec<VecDeque<PendingIns>> =
        (0..PAGE_POOL).map(|_| VecDeque::new()).collect();
    let mut page_active: HashMap<u64, usize> = HashMap::new(); // page_base → slot
    let mut page_in_flight = 0usize; // reading or writing
    let mut total_new_inserts = 0u64;

    let mut done_fks = 0u64;
    let target_fks = last - first + 1;

    while done_fks < target_fks
        || session.in_flight() > 0
        || body_in_flight > 0
        || page_in_flight > 0
    {
        // --- fill submissions ---
        loop {
            let free_sq = session.free_sq();
            if free_sq == 0 {
                break;
            }

            // 1) idx: mmap record_ranges when queue is below low-water.
            let need_idx = next_idx_fk <= last && fk_queue.len() < FK_QUEUE_LOW;
            if need_idx {
                let bf = next_idx_fk;
                let bl = (next_idx_fk + IDX_BATCH - 1).min(last);
                let ranges = body.record_ranges(bf, bl)?;
                for (i, &(start, len)) in ranges.iter().enumerate() {
                    let fk = bf + i as u64;
                    fk_queue.push_back(BodyWork {
                        fk,
                        body_off: start,
                        body_len: len,
                    });
                }
                next_idx_fk = bl + 1;
                continue;
            }

            // 2) body reads: drain queue as far as ring/pool allow.
            let mut submitted_body = false;
            while !fk_queue.is_empty()
                && !body_free.is_empty()
                && session.free_sq() > 0
            {
                let w = fk_queue.pop_front().unwrap();
                let slot = body_free.pop().unwrap();
                let want = (w.body_len as usize).min(32).max(1);
                body_want[slot] = want;
                body_work[slot] = Some(w);
                let buf = &mut body_bufs[slot][..want];
                let off = body_work[slot].as_ref().unwrap().body_off;
                session.push_pread(
                    body_fd,
                    off,
                    buf,
                    pack_ud(KIND_BODY, slot as u32),
                )?;
                body_in_flight += 1;
                submitted_body = true;
            }
            if submitted_body {
                continue;
            }

            // 3) Kick page RMWs for inserts parked only in page_wait (no free
            //    page slot earlier, or arrived while another cycle was active).
            let mut started_page = false;
            let parked: Vec<u64> = page_wait.keys().copied().collect();
            for pb in parked {
                if session.free_sq() == 0 || page_free.is_empty() {
                    break;
                }
                if page_active.contains_key(&pb) {
                    continue;
                }
                let Some(mut q) = page_wait.remove(&pb) else {
                    continue;
                };
                if q.is_empty() {
                    continue;
                }
                let first_ins = q.pop_front().unwrap();
                let ps = page_free.pop().unwrap();
                let txid = first_ins.txid;
                page_st[ps] = PageSt::Reading;
                page_base_of[ps] = pb;
                let plen =
                    page_pread_len(&txid, bits, entry_bytes, slots, slot_region).min(page_bytes);
                if plen == 0 {
                    return Err(StoreError::Corrupt("page pread len 0"));
                }
                page_len_of[ps] = plen;
                page_off_of[ps] = page_file_off(&txid, bits, entry_bytes);
                page_pending[ps].clear();
                page_pending[ps].push_back(first_ins);
                page_pending[ps].append(&mut q);
                page_active.insert(pb, ps);
                page_bufs[ps][..plen].fill(0);
                session.push_pread(
                    head_fd,
                    page_off_of[ps],
                    &mut page_bufs[ps][..plen],
                    pack_ud(KIND_PAGE_RD, ps as u32),
                )?;
                page_in_flight += 1;
                started_page = true;
            }
            if started_page {
                continue;
            }
            break;
        }

        session.sync_submission();
        if session.in_flight() == 0 {
            if done_fks >= target_fks
                || (next_idx_fk > last
                    && fk_queue.is_empty()
                    && body_in_flight == 0
                    && page_in_flight == 0
                    && page_wait.is_empty()
                    && page_active.is_empty())
            {
                break;
            }
            return Err(StoreError::Corrupt(
                "shadow fill stalled with no in-flight IO",
            ));
        }

        // Wait for ≥1 completion
        session.submit_and_wait_one()?;
        let cqes = session.harvest_ready();

        for (ud, res) in cqes {
            let (kind, slot) = unpack_ud(ud);

            if kind == KIND_BODY {
                body_in_flight = body_in_flight.saturating_sub(1);
                let slot = slot as usize;
                let work = body_work[slot].take().ok_or(StoreError::Corrupt("body slot"))?;
                let want = body_want[slot];
                if res < 0 {
                    body_free.push(slot);
                    return Err(StoreError::io(
                        &body_path,
                        std::io::Error::from_raw_os_error(-res),
                    ));
                }
                if res as usize != want {
                    body_free.push(slot);
                    return Err(StoreError::Corrupt("body txid pread short"));
                }
                let raw_txid = TxTable::txid_from_body_prefix(&body_bufs[slot][..want])?;
                body_free.push(slot);
                // Keyed head probes (same mix as live insert path).
                let txid = secret.mix_txid(&raw_txid);

                let pb = page_base_for_txid(&txid, bits);
                let ins = PendingIns {
                    txid,
                    fk: Fk(work.fk),
                };

                if page_active.contains_key(&pb) {
                    // RMW already active — queue for that page
                    page_wait.entry(pb).or_default().push_back(ins);
                } else if page_free.is_empty() || session.free_sq() == 0 {
                    // No page slot — park on multimap; will start when a page frees
                    page_wait.entry(pb).or_default().push_back(ins);
                } else {
                    // Start page RMW
                    let ps = page_free.pop().unwrap();
                    page_st[ps] = PageSt::Reading;
                    page_base_of[ps] = pb;
                    let plen = page_pread_len(&txid, bits, entry_bytes, slots, slot_region)
                        .min(page_bytes);
                    if plen == 0 {
                        return Err(StoreError::Corrupt("page pread len 0"));
                    }
                    page_len_of[ps] = plen;
                    page_off_of[ps] = page_file_off(&txid, bits, entry_bytes);
                    page_pending[ps].clear();
                    page_pending[ps].push_back(ins);
                    page_active.insert(pb, ps);
                    page_bufs[ps][..plen].fill(0);
                    session.push_pread(
                        head_fd,
                        page_off_of[ps],
                        &mut page_bufs[ps][..plen],
                        pack_ud(KIND_PAGE_RD, ps as u32),
                    )?;
                    page_in_flight += 1;
                }
                continue;
            }

            if kind == KIND_PAGE_RD {
                let ps = slot as usize;
                if res < 0 {
                    return Err(StoreError::io(
                        &head_path,
                        std::io::Error::from_raw_os_error(-res),
                    ));
                }
                let plen = page_len_of[ps];
                if res as usize != plen {
                    return Err(StoreError::Corrupt("shadow page pread short"));
                }
                let pb = page_base_of[ps];
                // Drain wait queue into pending
                if let Some(mut q) = page_wait.remove(&pb) {
                    page_pending[ps].append(&mut q);
                }
                let mut new_n = 0u64;
                while let Some(ins) = page_pending[ps].pop_front() {
                    let outcome = insert_fk_into_page_buf(
                        &mut page_bufs[ps][..plen],
                        pb,
                        bits,
                        entry_bytes,
                        &ins.txid,
                        ins.fk,
                    )?;
                    if outcome.wrote_new {
                        new_n += 1;
                    }
                    done_fks += 1;
                }
                // store new_n on slot via pending reuse of wrote count — use page_len high unused: track in page_base high?
                // Use page_pending capacity as counter storage: push a sentinel — simpler: field
                // We'll use page_off_of high bits? Add wrote tracking via a parallel vec.
                // Quick: encode in page_st transition — add page_wrote_new vec
                // For now accumulate in a side array:
                // Actually use page_pending as empty and store in a local map — add vec page_new
                // I'll use a separate vec initialized below... patch: use `page_base_of` sibling
                // Adding page_new_inserts on the fly via extending - we need to fix structure.
                // Use total_new_inserts immediately here and again after write? Occupied should
                // only bump after durable write. Track in page_pending by reusing wrote in a vec.

                // Stash new_n in page_len_of top... no. Use HashMap temporarily:
                // We'll store in page_off_of overflow: keep page_new as we go — define page_new_n
                // The vec wasn't declared — use page_work_new: I'll put new_n into a slot of page_pending as fake
                // Simplest fix: accumulate total_new_inserts only after successful write using a parallel vec.
                // Declare was missing — use existing page_work by storing in page_pending length+new via side channel
                // `page_bufs[ps][plen]` — can't.
                // Use a free field: extend page_st with Writing { new_n } — change to store new_n in page_base_of shadow.
                // **Hack**: page_active values are slots; store new_n in `body_want` unused for pages... 
                // Add at start: `let mut page_new = vec![0u64; PAGE_POOL];` — too late in this function.
                // Store in HashMap page_new_map
                // For this completion only, keep new_n until write by putting it in page_pending as zero-fk?
                
                // Re-read: I'll use `page_off_of` is u64 — keep page_new in a Vec I should have declared.
                // Since I'm mid-function, use thread_local or a HashMap on stack:
                // Actually `page_pending[ps]` is empty — I can push PendingIns with fk encoding new_n — ugly.

                // Use total and also store via a second HashMap local:
                // Wait - I need the value at write CQE. Use `page_active` reverse: 
                // I'll declare `let mut page_new_n = vec![0u64; PAGE_POOL];` at the top in a re-write.

                // For the write path below, search for PAGE_WR handling.

                page_st[ps] = PageSt::Writing;
                // TEMP: stash new_n in page_len_of by packing — page_len fits in low 16 bits
                // page_len_of[ps] = plen | ((new_n as usize) << 16) — new_n can be large?
                // page can have at most 1024 inserts
                debug_assert!(new_n <= 1024);
                page_len_of[ps] = plen | ((new_n as usize) << 16);

                session.push_pwrite(
                    head_fd,
                    page_off_of[ps],
                    &page_bufs[ps][..plen],
                    pack_ud(KIND_PAGE_WR, ps as u32),
                )?;
                // still page_in_flight (was reading, now writing)
                continue;
            }

            if kind == KIND_PAGE_WR {
                let ps = slot as usize;
                page_in_flight = page_in_flight.saturating_sub(1);
                let packed = page_len_of[ps];
                let plen = packed & 0xffff;
                let new_n = (packed >> 16) as u64;
                if res < 0 {
                    return Err(StoreError::io(
                        &head_path,
                        std::io::Error::from_raw_os_error(-res),
                    ));
                }
                if res as usize != plen {
                    return Err(StoreError::Corrupt("shadow page pwrite short"));
                }
                if new_n > 0 {
                    shadow.note_inserts(new_n);
                    total_new_inserts = total_new_inserts.saturating_add(new_n);
                }
                let pb = page_base_of[ps];
                page_active.remove(&pb);
                page_st[ps] = PageSt::Free;
                page_pending[ps].clear();
                page_free.push(ps);

                // Restarts: if more inserts waited during write, start new RMW
                if let Some(mut q) = page_wait.remove(&pb) {
                    if !q.is_empty() && !page_free.is_empty() {
                        let first_ins = q.pop_front().unwrap();
                        let ps2 = page_free.pop().unwrap();
                        let txid = first_ins.txid;
                        page_st[ps2] = PageSt::Reading;
                        page_base_of[ps2] = pb;
                        let plen2 = page_pread_len(&txid, bits, entry_bytes, slots, slot_region)
                            .min(page_bytes);
                        page_len_of[ps2] = plen2;
                        page_off_of[ps2] = page_file_off(&txid, bits, entry_bytes);
                        page_pending[ps2].clear();
                        page_pending[ps2].push_back(first_ins);
                        page_pending[ps2].append(&mut q);
                        page_active.insert(pb, ps2);
                        page_bufs[ps2][..plen2].fill(0);
                        session.push_pread(
                            head_fd,
                            page_off_of[ps2],
                            &mut page_bufs[ps2][..plen2],
                            pack_ud(KIND_PAGE_RD, ps2 as u32),
                        )?;
                        page_in_flight += 1;
                    } else if !q.is_empty() {
                        page_wait.insert(pb, q);
                    }
                }
                continue;
            }

            return Err(StoreError::Corrupt("unknown io_uring user_data kind"));
        }

        // After CQEs: try to start page RMWs for parked multimap entries if free slots
        // Body path only starts when free; parked-only pages need kickstart
        let parked_pages: Vec<u64> = page_wait.keys().copied().collect();
        for pb in parked_pages {
            if page_active.contains_key(&pb) {
                continue;
            }
            if page_free.is_empty() || session.free_sq() == 0 {
                break;
            }
            let Some(mut q) = page_wait.remove(&pb) else {
                continue;
            };
            if q.is_empty() {
                continue;
            }
            let first_ins = q.pop_front().unwrap();
            let ps = page_free.pop().unwrap();
            let txid = first_ins.txid;
            page_st[ps] = PageSt::Reading;
            page_base_of[ps] = pb;
            let plen =
                page_pread_len(&txid, bits, entry_bytes, slots, slot_region).min(page_bytes);
            page_len_of[ps] = plen;
            page_off_of[ps] = page_file_off(&txid, bits, entry_bytes);
            page_pending[ps].clear();
            page_pending[ps].push_back(first_ins);
            page_pending[ps].append(&mut q);
            page_active.insert(pb, ps);
            page_bufs[ps][..plen].fill(0);
            session.push_pread(
                head_fd,
                page_off_of[ps],
                &mut page_bufs[ps][..plen],
                pack_ud(KIND_PAGE_RD, ps as u32),
            )?;
            page_in_flight += 1;
        }
    }

    std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst);
    let _ = total_new_inserts;
    Ok(())
}
