//! mmap-backed durable mempool directory (slots + tx.body + meta).

use crate::error::MempoolError;
use bitcoin::consensus::encode::deserialize;
use bitcoin::hashes::Hash;
use bitcoin::{Transaction, Txid};
use memmap2::MmapMut;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// File magic: `rBMP` (rbitcoin mempool).
pub const MEM_MAGIC: [u8; 4] = *b"rBMP";
/// Schema version for the meta header.
pub const MEM_SCHEMA: u16 = 1;

const META_LEN: usize = 64;
/// Initial slot table capacity (records). Grows in later phases.
const DEFAULT_SLOT_CAP: u32 = 4096;
/// Slot record: status(1) + pad(3) + body_off(8) + body_len(4) + txid(32) = 48.
/// Body payload at `body_off`: fee_sat(8) + weight(8) + raw_tx (body_len includes prefix).
const SLOT_REC: usize = 48;
const SLOTS_HEADER: usize = 16;
const BODY_HEADER: usize = 16;
/// Prefix before each serialized tx in `tx.body`.
const BODY_TX_PREFIX: usize = 16;

const SLOT_FREE: u8 = 0;
const SLOT_LIVE: u8 = 1;
const SLOT_DEAD: u8 = 2;

/// Snapshot of durable meta fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MempoolMeta {
    /// Last committed generation (bumped on flush).
    pub generation: u64,
    /// Slot table capacity (record count).
    pub slot_cap: u32,
    /// Number of LIVE slots (0 at P1).
    pub live_count: u32,
}

/// Open durable mempool under `dir` (`{datadir}/mempool`).
pub struct Mempool {
    dir: PathBuf,
    meta_file: Mutex<File>,
    meta_map: Mutex<MmapMut>,
    slots_file: Mutex<File>,
    slots_map: Mutex<MmapMut>,
    body_file: Mutex<File>,
    body_map: Mutex<MmapMut>,
    generation: u64,
    slot_cap: u32,
    live_count: u32,
}

impl Mempool {
    /// Create `dir` if needed and open (or initialize) meta/slots/body.
    pub fn open_or_create(dir: impl Into<PathBuf>) -> Result<Self, MempoolError> {
        let dir = dir.into();
        fs::create_dir_all(&dir).map_err(|e| MempoolError::io(&dir, e))?;

        let meta_path = dir.join("meta");
        let slots_path = dir.join("slots");
        let body_path = dir.join("tx.body");

        let (meta_file, meta_map, generation, slot_cap, live_count) =
            open_or_init_meta(&meta_path)?;
        let (slots_file, slots_map) = open_or_init_slots(&slots_path, slot_cap)?;
        let (body_file, body_map) = open_or_init_body(&body_path)?;

        Ok(Self {
            dir,
            meta_file: Mutex::new(meta_file),
            meta_map: Mutex::new(meta_map),
            slots_file: Mutex::new(slots_file),
            slots_map: Mutex::new(slots_map),
            body_file: Mutex::new(body_file),
            body_map: Mutex::new(body_map),
            generation,
            slot_cap,
            live_count,
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn meta(&self) -> MempoolMeta {
        MempoolMeta {
            generation: self.generation,
            slot_cap: self.slot_cap,
            live_count: self.live_count,
        }
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn live_count(&self) -> u32 {
        self.live_count
    }

    /// Test / rebuild helper: set live_count without scanning.
    pub(crate) fn set_live_count(&mut self, n: u32) {
        self.live_count = n;
    }

    /// Persist meta (bump generation) and msync maps.
    ///
    /// Accept path does **not** fsync per tx; a crash loses at most the last
    /// uncommitted batch (slots written after the previous flush).
    pub fn flush(&mut self) -> Result<(), MempoolError> {
        self.generation = self.generation.saturating_add(1);
        self.write_meta()?;
        {
            let map = self.meta_map.lock().unwrap();
            map.flush().map_err(|e| MempoolError::io(self.dir.join("meta"), e))?;
        }
        {
            let map = self.slots_map.lock().unwrap();
            map.flush()
                .map_err(|e| MempoolError::io(self.dir.join("slots"), e))?;
        }
        {
            let map = self.body_map.lock().unwrap();
            map.flush()
                .map_err(|e| MempoolError::io(self.dir.join("tx.body"), e))?;
        }
        // fsync files so generation is crash-stable.
        self.meta_file
            .lock()
            .unwrap()
            .sync_data()
            .map_err(|e| MempoolError::io(self.dir.join("meta"), e))?;
        self.slots_file
            .lock()
            .unwrap()
            .sync_data()
            .map_err(|e| MempoolError::io(self.dir.join("slots"), e))?;
        self.body_file
            .lock()
            .unwrap()
            .sync_data()
            .map_err(|e| MempoolError::io(self.dir.join("tx.body"), e))?;
        Ok(())
    }

    /// Append raw tx bytes + fee/weight prefix; mark a FREE slot LIVE.
    ///
    /// Returns the slot index. Updates `live_count` (not generation — call flush).
    pub fn append_live_tx(
        &mut self,
        raw_tx: &[u8],
        txid: &Txid,
        fee_sat: u64,
        weight: u64,
    ) -> Result<u32, MempoolError> {
        let payload_len = BODY_TX_PREFIX + raw_tx.len();
        if payload_len > u32::MAX as usize {
            return Err(MempoolError::Corrupt("tx body too large"));
        }
        let body_off = self.reserve_body(payload_len)?;
        {
            let mut map = self.body_map.lock().unwrap();
            let off = body_off as usize;
            map[off..off + 8].copy_from_slice(&fee_sat.to_le_bytes());
            map[off + 8..off + 16].copy_from_slice(&weight.to_le_bytes());
            map[off + BODY_TX_PREFIX..off + payload_len].copy_from_slice(raw_tx);
        }
        let slot = self.alloc_slot()?;
        self.write_slot(slot, SLOT_LIVE, body_off, payload_len as u32, txid)?;
        self.live_count = self.live_count.saturating_add(1);
        // Keep meta live_count current (generation unchanged until flush).
        self.write_meta()?;
        Ok(slot)
    }

    /// Mark slot DEAD and decrement live_count (rollback path).
    pub fn mark_slot_dead(&mut self, slot: u32) -> Result<(), MempoolError> {
        if slot >= self.slot_cap {
            return Err(MempoolError::Corrupt("slot OOB"));
        }
        let mut map = self.slots_map.lock().unwrap();
        let off = SLOTS_HEADER + (slot as usize) * SLOT_REC;
        if map[off] == SLOT_LIVE {
            map[off] = SLOT_DEAD;
            self.live_count = self.live_count.saturating_sub(1);
            drop(map);
            self.write_meta()?;
        }
        Ok(())
    }

    /// Count FREE / LIVE / DEAD slots (for compaction triggers).
    pub fn slot_stats(&self) -> (u32, u32, u32) {
        let map = self.slots_map.lock().unwrap();
        let mut free = 0u32;
        let mut live = 0u32;
        let mut dead = 0u32;
        for slot in 0..self.slot_cap {
            let off = SLOTS_HEADER + (slot as usize) * SLOT_REC;
            match map[off] {
                SLOT_LIVE => live += 1,
                SLOT_DEAD => dead += 1,
                _ => free += 1,
            }
        }
        (free, live, dead)
    }

    /// Logical body length in bytes (includes header).
    pub fn body_logical_len(&self) -> Result<usize, MempoolError> {
        let map = self.body_map.lock().unwrap();
        body_logical_len(&map)
    }

    /// Rewrite body/slots to contain only LIVE payloads packed from the header.
    ///
    /// Returns `(live_after, body_bytes_after)`. Callers must rebuild RAM indexes
    /// with the new slot numbers from [`load_live_txs`].
    pub fn compact(&mut self) -> Result<(u32, usize), MempoolError> {
        let live = self.load_live_txs()?;
        let path_body = self.dir.join("tx.body");
        let path_slots = self.dir.join("slots");

        // Build compact body in a Vec then write.
        let mut new_body = vec![0u8; BODY_HEADER];
        new_body[0..4].copy_from_slice(&MEM_MAGIC);
        new_body[4..6].copy_from_slice(&MEM_SCHEMA.to_le_bytes());
        let mut new_slots = vec![0u8; SLOTS_HEADER + (self.slot_cap as usize) * SLOT_REC];
        new_slots[0..4].copy_from_slice(&MEM_MAGIC);
        new_slots[4..6].copy_from_slice(&MEM_SCHEMA.to_le_bytes());
        new_slots[8..12].copy_from_slice(&self.slot_cap.to_le_bytes());

        let mut next_slot = 0u32;
        for (_old_slot, fee_sat, weight, tx) in &live {
            let raw = bitcoin::consensus::encode::serialize(tx);
            let payload_len = BODY_TX_PREFIX + raw.len();
            let body_off = new_body.len() as u64;
            new_body.extend_from_slice(&fee_sat.to_le_bytes());
            new_body.extend_from_slice(&weight.to_le_bytes());
            new_body.extend_from_slice(&raw);
            let off = SLOTS_HEADER + (next_slot as usize) * SLOT_REC;
            new_slots[off] = SLOT_LIVE;
            new_slots[off + 4..off + 12].copy_from_slice(&body_off.to_le_bytes());
            new_slots[off + 12..off + 16]
                .copy_from_slice(&(payload_len as u32).to_le_bytes());
            new_slots[off + 16..off + 48].copy_from_slice(tx.compute_txid().as_byte_array());
            next_slot += 1;
        }
        let logical = new_body.len();
        new_body[8..16].copy_from_slice(&(logical as u64).to_le_bytes());

        // Resize files and remmap.
        {
            let mut f = self.body_file.lock().unwrap();
            f.set_len(logical.max(BODY_HEADER + 64) as u64)
                .map_err(|e| MempoolError::io(&path_body, e))?;
            f.seek(SeekFrom::Start(0))
                .map_err(|e| MempoolError::io(&path_body, e))?;
            f.write_all(&new_body)
                .map_err(|e| MempoolError::io(&path_body, e))?;
            f.flush().map_err(|e| MempoolError::io(&path_body, e))?;
        }
        {
            let mut f = self.slots_file.lock().unwrap();
            f.seek(SeekFrom::Start(0))
                .map_err(|e| MempoolError::io(&path_slots, e))?;
            f.write_all(&new_slots)
                .map_err(|e| MempoolError::io(&path_slots, e))?;
            f.flush().map_err(|e| MempoolError::io(&path_slots, e))?;
        }
        // Remap body + slots.
        {
            let file = self.body_file.lock().unwrap();
            let map =
                unsafe { MmapMut::map_mut(&*file) }.map_err(|e| MempoolError::io(&path_body, e))?;
            *self.body_map.lock().unwrap() = map;
        }
        {
            let file = self.slots_file.lock().unwrap();
            let map =
                unsafe { MmapMut::map_mut(&*file) }.map_err(|e| MempoolError::io(&path_slots, e))?;
            *self.slots_map.lock().unwrap() = map;
        }
        self.live_count = next_slot;
        self.write_meta()?;
        Ok((self.live_count, logical))
    }

    /// Load all LIVE txs from slots/body for graph rebuild.
    ///
    /// Returns `(slot, fee_sat, weight, tx)`.
    pub fn load_live_txs(&self) -> Result<Vec<(u32, u64, u64, Transaction)>, MempoolError> {
        let mut out = Vec::new();
        let slots = self.slots_map.lock().unwrap();
        let body = self.body_map.lock().unwrap();
        let logical = body_logical_len(&body)?;
        for slot in 0..self.slot_cap {
            let off = SLOTS_HEADER + (slot as usize) * SLOT_REC;
            if slots[off] != SLOT_LIVE {
                continue;
            }
            let body_off = u64::from_le_bytes(slots[off + 4..off + 12].try_into().unwrap());
            let body_len = u32::from_le_bytes(slots[off + 12..off + 16].try_into().unwrap()) as usize;
            if body_off as usize + body_len > logical || body_len < BODY_TX_PREFIX {
                return Err(MempoolError::Corrupt("live slot body range"));
            }
            let start = body_off as usize;
            let fee_sat = u64::from_le_bytes(body[start..start + 8].try_into().unwrap());
            let weight = u64::from_le_bytes(body[start + 8..start + 16].try_into().unwrap());
            let raw = &body[start + BODY_TX_PREFIX..start + body_len];
            let tx: Transaction = deserialize(raw)
                .map_err(|_| MempoolError::Corrupt("tx deserialize"))?;
            out.push((slot, fee_sat, weight, tx));
        }
        Ok(out)
    }

    fn alloc_slot(&self) -> Result<u32, MempoolError> {
        let map = self.slots_map.lock().unwrap();
        for slot in 0..self.slot_cap {
            let off = SLOTS_HEADER + (slot as usize) * SLOT_REC;
            if map[off] == SLOT_FREE || map[off] == SLOT_DEAD {
                return Ok(slot);
            }
        }
        Err(MempoolError::Corrupt("slot table full"))
    }

    fn write_slot(
        &self,
        slot: u32,
        status: u8,
        body_off: u64,
        body_len: u32,
        txid: &Txid,
    ) -> Result<(), MempoolError> {
        let mut map = self.slots_map.lock().unwrap();
        let off = SLOTS_HEADER + (slot as usize) * SLOT_REC;
        map[off] = status;
        map[off + 1..off + 4].fill(0);
        map[off + 4..off + 12].copy_from_slice(&body_off.to_le_bytes());
        map[off + 12..off + 16].copy_from_slice(&body_len.to_le_bytes());
        map[off + 16..off + 48].copy_from_slice(txid.as_byte_array());
        Ok(())
    }

    /// Ensure body has `need` free bytes; return offset of free region. Updates logical len.
    fn reserve_body(&mut self, need: usize) -> Result<u64, MempoolError> {
        let path = self.dir.join("tx.body");
        loop {
            let logical = {
                let map = self.body_map.lock().unwrap();
                body_logical_len(&map)?
            };
            let end = logical.saturating_add(need);
            let cap = {
                let map = self.body_map.lock().unwrap();
                map.len()
            };
            if end <= cap {
                let mut map = self.body_map.lock().unwrap();
                map[8..16].copy_from_slice(&(end as u64).to_le_bytes());
                return Ok(logical as u64);
            }
            // Grow file (double or fit).
            let new_len = (cap.max(4096) * 2).max(end + 4096);
            {
                let f = self.body_file.lock().unwrap();
                f.set_len(new_len as u64)
                    .map_err(|e| MempoolError::io(&path, e))?;
            }
            // Remap.
            let file = self.body_file.lock().unwrap();
            let new_map =
                unsafe { MmapMut::map_mut(&*file) }.map_err(|e| MempoolError::io(&path, e))?;
            drop(file);
            *self.body_map.lock().unwrap() = new_map;
        }
    }

    fn write_meta(&self) -> Result<(), MempoolError> {
        let mut map = self.meta_map.lock().unwrap();
        write_meta_bytes(&mut map, self.generation, self.slot_cap, self.live_count);
        // Also keep file header in sync for non-mmap readers.
        let mut f = self.meta_file.lock().unwrap();
        f.seek(SeekFrom::Start(0))
            .map_err(|e| MempoolError::io(self.dir.join("meta"), e))?;
        f.write_all(&map[..META_LEN])
            .map_err(|e| MempoolError::io(self.dir.join("meta"), e))?;
        Ok(())
    }
}

fn body_logical_len(map: &MmapMut) -> Result<usize, MempoolError> {
    if map.len() < BODY_HEADER {
        return Err(MempoolError::Corrupt("body too short"));
    }
    let n = u64::from_le_bytes(map[8..16].try_into().unwrap()) as usize;
    if n < BODY_HEADER || n > map.len() {
        return Err(MempoolError::Corrupt("body logical len"));
    }
    Ok(n)
}

fn open_or_init_meta(
    path: &Path,
) -> Result<(File, MmapMut, u64, u32, u32), MempoolError> {
    if path.exists() {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| MempoolError::io(path, e))?;
        let mut buf = [0u8; META_LEN];
        file.read_exact(&mut buf)
            .map_err(|e| MempoolError::io(path, e))?;
        if buf[0..4] != MEM_MAGIC {
            return Err(MempoolError::BadMagic);
        }
        let schema = u16::from_le_bytes([buf[4], buf[5]]);
        if schema != MEM_SCHEMA {
            return Err(MempoolError::BadSchema(schema));
        }
        let generation = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        let slot_cap = u32::from_le_bytes(buf[16..20].try_into().unwrap());
        let live_count = u32::from_le_bytes(buf[20..24].try_into().unwrap());
        if slot_cap == 0 {
            return Err(MempoolError::Corrupt("slot_cap zero"));
        }
        let map = unsafe { MmapMut::map_mut(&file) }.map_err(|e| MempoolError::io(path, e))?;
        Ok((file, map, generation, slot_cap, live_count))
    } else {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|e| MempoolError::io(path, e))?;
        file.set_len(META_LEN as u64)
            .map_err(|e| MempoolError::io(path, e))?;
        let mut map = unsafe { MmapMut::map_mut(&file) }.map_err(|e| MempoolError::io(path, e))?;
        write_meta_bytes(&mut map, 0, DEFAULT_SLOT_CAP, 0);
        file.seek(SeekFrom::Start(0))
            .map_err(|e| MempoolError::io(path, e))?;
        file.write_all(&map[..META_LEN])
            .map_err(|e| MempoolError::io(path, e))?;
        file.flush().map_err(|e| MempoolError::io(path, e))?;
        Ok((file, map, 0, DEFAULT_SLOT_CAP, 0))
    }
}

fn write_meta_bytes(map: &mut MmapMut, generation: u64, slot_cap: u32, live_count: u32) {
    map[0..4].copy_from_slice(&MEM_MAGIC);
    map[4..6].copy_from_slice(&MEM_SCHEMA.to_le_bytes());
    map[6..8].copy_from_slice(&0u16.to_le_bytes()); // reserved
    map[8..16].copy_from_slice(&generation.to_le_bytes());
    map[16..20].copy_from_slice(&slot_cap.to_le_bytes());
    map[20..24].copy_from_slice(&live_count.to_le_bytes());
    // rest zero / reserved
}

fn open_or_init_slots(path: &Path, slot_cap: u32) -> Result<(File, MmapMut), MempoolError> {
    let need = SLOTS_HEADER as u64 + (slot_cap as u64) * (SLOT_REC as u64);
    if path.exists() {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| MempoolError::io(path, e))?;
        let len = file.metadata().map_err(|e| MempoolError::io(path, e))?.len();
        if len < need {
            return Err(MempoolError::Corrupt("slots file short"));
        }
        let map = unsafe { MmapMut::map_mut(&file) }.map_err(|e| MempoolError::io(path, e))?;
        if map[0..4] != MEM_MAGIC {
            return Err(MempoolError::BadMagic);
        }
        Ok((file, map))
    } else {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|e| MempoolError::io(path, e))?;
        file.set_len(need).map_err(|e| MempoolError::io(path, e))?;
        let mut map = unsafe { MmapMut::map_mut(&file) }.map_err(|e| MempoolError::io(path, e))?;
        map[0..4].copy_from_slice(&MEM_MAGIC);
        map[4..6].copy_from_slice(&MEM_SCHEMA.to_le_bytes());
        map[8..12].copy_from_slice(&slot_cap.to_le_bytes());
        file.flush().map_err(|e| MempoolError::io(path, e))?;
        Ok((file, map))
    }
}

fn open_or_init_body(path: &Path) -> Result<(File, MmapMut), MempoolError> {
    let initial = BODY_HEADER as u64 + 64;
    if path.exists() {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| MempoolError::io(path, e))?;
        let map = unsafe { MmapMut::map_mut(&file) }.map_err(|e| MempoolError::io(path, e))?;
        if map.len() < BODY_HEADER {
            return Err(MempoolError::Corrupt("body too short"));
        }
        if map[0..4] != MEM_MAGIC {
            return Err(MempoolError::BadMagic);
        }
        Ok((file, map))
    } else {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|e| MempoolError::io(path, e))?;
        file.set_len(initial).map_err(|e| MempoolError::io(path, e))?;
        let mut map = unsafe { MmapMut::map_mut(&file) }.map_err(|e| MempoolError::io(path, e))?;
        map[0..4].copy_from_slice(&MEM_MAGIC);
        map[4..6].copy_from_slice(&MEM_SCHEMA.to_le_bytes());
        // body logical length (header only) in bytes 8..16
        map[8..16].copy_from_slice(&(BODY_HEADER as u64).to_le_bytes());
        file.flush().map_err(|e| MempoolError::io(path, e))?;
        Ok((file, map))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn tmp_dir() -> PathBuf {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        // Prefer /tmp so mmap works (workspace may be 9p without MAP_SHARED write).
        let p = PathBuf::from(format!("/tmp/rbitcoin-mempool-test-{n}"));
        let _ = fs::remove_dir_all(&p);
        p
    }

    #[test]
    fn empty_create_reopen_flush() {
        let dir = tmp_dir();
        {
            let mut mp = Mempool::open_or_create(&dir).expect("create");
            let m = mp.meta();
            assert_eq!(m.generation, 0);
            assert_eq!(m.live_count, 0);
            assert!(m.slot_cap >= 1024);
            mp.flush().expect("flush");
            assert_eq!(mp.generation(), 1);
        }
        {
            let mp = Mempool::open_or_create(&dir).expect("reopen");
            assert_eq!(mp.generation(), 1);
            assert_eq!(mp.live_count(), 0);
            assert!(mp.dir().join("meta").exists());
            assert!(mp.dir().join("slots").exists());
            assert!(mp.dir().join("tx.body").exists());
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn flush_bumps_generation_monotone() {
        let dir = tmp_dir();
        let mut mp = Mempool::open_or_create(&dir).unwrap();
        for i in 1..=5 {
            mp.flush().unwrap();
            assert_eq!(mp.generation(), i);
        }
        drop(mp);
        let mp = Mempool::open_or_create(&dir).unwrap();
        assert_eq!(mp.generation(), 5);
        let _ = fs::remove_dir_all(&dir);
    }
}
