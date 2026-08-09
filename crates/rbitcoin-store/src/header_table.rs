use crate::error::StoreError;
use crate::file::{TableFile, FILE_HEADER_LEN};
use crate::sharded_hashhead::ShardedHashHead;
use bitcoin_hashes::{sha256, Hash, HashEngine};
use rbitcoin_primitives::{Fk, TableKind};
use std::sync::Mutex;

/// Fixed-size header body record (88 bytes). See SCHEMA.md.
pub const HEADER_RECORD_LEN: usize = 88;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeaderRecord {
    pub prev_fk: Fk,
    pub version: i32,
    pub timestamp: u32,
    pub bits: u32,
    pub nonce: u32,
    pub merkle_root: [u8; 32],
    pub hash: [u8; 32],
}

impl HeaderRecord {
    pub fn encode(&self) -> [u8; HEADER_RECORD_LEN] {
        let mut out = [0u8; HEADER_RECORD_LEN];
        out[0..8].copy_from_slice(&self.prev_fk.0.to_le_bytes());
        out[8..12].copy_from_slice(&self.version.to_le_bytes());
        out[12..16].copy_from_slice(&self.timestamp.to_le_bytes());
        out[16..20].copy_from_slice(&self.bits.to_le_bytes());
        out[20..24].copy_from_slice(&self.nonce.to_le_bytes());
        out[24..56].copy_from_slice(&self.merkle_root);
        out[56..88].copy_from_slice(&self.hash);
        out
    }

    pub fn decode(buf: &[u8]) -> Result<Self, StoreError> {
        if buf.len() < HEADER_RECORD_LEN {
            return Err(StoreError::Corrupt("short header record"));
        }
        Ok(Self {
            prev_fk: Fk(u64::from_le_bytes(buf[0..8].try_into().unwrap())),
            version: i32::from_le_bytes(buf[8..12].try_into().unwrap()),
            timestamp: u32::from_le_bytes(buf[12..16].try_into().unwrap()),
            bits: u32::from_le_bytes(buf[16..20].try_into().unwrap()),
            nonce: u32::from_le_bytes(buf[20..24].try_into().unwrap()),
            merkle_root: buf[24..56].try_into().unwrap(),
            hash: buf[56..88].try_into().unwrap(),
        })
    }
}

/// Double-SHA256 of a bitcoin block header (80 bytes), internal byte order.
pub fn block_header_hash(
    version: i32,
    prev_hash: &[u8; 32],
    merkle_root: &[u8; 32],
    timestamp: u32,
    bits: u32,
    nonce: u32,
) -> [u8; 32] {
    let mut ser = [0u8; 80];
    ser[0..4].copy_from_slice(&version.to_le_bytes());
    ser[4..36].copy_from_slice(prev_hash);
    ser[36..68].copy_from_slice(merkle_root);
    ser[68..72].copy_from_slice(&timestamp.to_le_bytes());
    ser[72..76].copy_from_slice(&bits.to_le_bytes());
    ser[76..80].copy_from_slice(&nonce.to_le_bytes());
    let mut eng = sha256::HashEngine::default();
    eng.input(&ser);
    let mid = sha256::Hash::from_engine(eng);
    let mut eng2 = sha256::HashEngine::default();
    eng2.input(mid.as_byte_array());
    sha256::Hash::from_engine(eng2).to_byte_array()
}

pub struct HeaderTable {
    body: TableFile,
    head: ShardedHashHead,
    count: std::sync::atomic::AtomicU64,
    /// Serializes check-then-put so two threads cannot both miss and both append
    /// the same full hash (I1 + I4).
    put_lock: Mutex<()>,
}

impl HeaderTable {
    pub fn create(dir: &std::path::Path) -> Result<Self, StoreError> {
        let body = TableFile::create(dir.join("header.body"), TableKind::Header)?;
        let head = ShardedHashHead::create_for_role(
            dir.join("header.head"),
            crate::hashhead::HeadRole::Header,
        )?;
        Ok(Self {
            body,
            head,
            count: std::sync::atomic::AtomicU64::new(0),
            put_lock: Mutex::new(()),
        })
    }

    pub fn open(dir: &std::path::Path) -> Result<Self, StoreError> {
        let body = TableFile::open(dir.join("header.body"), TableKind::Header)?;
        let head = ShardedHashHead::open_for_role(
            dir.join("header.head"),
            crate::hashhead::HeadRole::Header,
        )?;
        let body_len = body.logical_len().saturating_sub(FILE_HEADER_LEN as u64);
        if body_len % HEADER_RECORD_LEN as u64 != 0 {
            return Err(StoreError::Corrupt("header body size"));
        }
        let count = body_len / HEADER_RECORD_LEN as u64;
        Ok(Self {
            body,
            head,
            count: std::sync::atomic::AtomicU64::new(count),
            put_lock: Mutex::new(()),
        })
    }

    /// Append-only insert **without** uniqueness. Prefer [`Self::ensure`].
    ///
    /// Used by offline rebuild tools that rewrite a clean table from scratch.
    pub fn put_raw(&self, rec: &HeaderRecord) -> Result<Fk, StoreError> {
        let _g = self.put_lock.lock().unwrap_or_else(|e| e.into_inner());
        self.put_unlocked(rec)
    }

    /// Append header and publish into the hash head. Returns new FK.
    ///
    /// **Deprecated for hot paths:** use [`Self::ensure`] so the same full hash
    /// cannot get a second body row with a divergent `prev_fk`.
    pub fn put(&self, rec: &HeaderRecord) -> Result<Fk, StoreError> {
        self.ensure(rec)
    }

    /// Write gate: at most one body row per full block hash (I1).
    ///
    /// - If `hash` already exists → return that fk (ignore caller's `prev_fk`).
    /// - Else if `prev_fk` is non-null → parent must exist and
    ///   `hash` must equal SHA256D(header fields with parent.hash as prev) (I2/I3).
    /// - Else (`prev_fk` null) → append as-is (genesis / synthetic test rows).
    ///
    /// Lookup + insert hold [`Self::put_lock`] (I4).
    pub fn ensure(&self, rec: &HeaderRecord) -> Result<Fk, StoreError> {
        let _g = self.put_lock.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((fk, _)) = self.get_by_hash_unlocked(&rec.hash)? {
            return Ok(fk);
        }
        if !rec.prev_fk.is_null() {
            let parent = self.get(rec.prev_fk)?;
            let expect = block_header_hash(
                rec.version,
                &parent.hash,
                &rec.merkle_root,
                rec.timestamp,
                rec.bits,
                rec.nonce,
            );
            if expect != rec.hash {
                return Err(StoreError::Corrupt(
                    "header prev_fk does not match block hash (false parent edge)",
                ));
            }
        }
        self.put_unlocked(rec)
    }

    fn put_unlocked(&self, rec: &HeaderRecord) -> Result<Fk, StoreError> {
        use std::sync::atomic::Ordering;
        // body → count → head (allocate-then-publish).
        let base = self.count.load(Ordering::Acquire);
        let fk = Fk(base + 1);
        let offset = FILE_HEADER_LEN as u64 + base * HEADER_RECORD_LEN as u64;
        let bytes = rec.encode();
        self.body.write_at(offset, &bytes)?;
        self.count.store(base + 1, Ordering::Release);
        self.head.insert(&rec.hash, fk)?;
        Ok(fk)
    }

    pub fn get(&self, fk: Fk) -> Result<HeaderRecord, StoreError> {
        use std::sync::atomic::Ordering;
        let id = fk.get().ok_or(StoreError::InvalidFk)?;
        let count = self.count.load(Ordering::Acquire);
        if id == 0 || id > count {
            return Err(StoreError::NotFound);
        }
        let offset = FILE_HEADER_LEN as u64 + (id - 1) * HEADER_RECORD_LEN as u64;
        let mut buf = [0u8; HEADER_RECORD_LEN];
        self.body.read_at(offset, &mut buf)?;
        HeaderRecord::decode(&buf)
    }

    pub fn get_by_hash(&self, hash: &[u8; 32]) -> Result<Option<(Fk, HeaderRecord)>, StoreError> {
        // Head reads are safe without put_lock (body append-only; head multi-list
        // is append-oriented). Callers that check-then-put must use ensure.
        self.get_by_hash_unlocked(hash)
    }

    fn get_by_hash_unlocked(
        &self,
        hash: &[u8; 32],
    ) -> Result<Option<(Fk, HeaderRecord)>, StoreError> {
        // 16-byte head prefix may collide — verify full hash on the body.
        for fk in self.head.get_all(hash)? {
            let rec = self.get(fk)?;
            if rec.hash == *hash {
                return Ok(Some((fk, rec)));
            }
        }
        Ok(None)
    }

    /// Number of header rows currently stored (highest fk = this value).
    pub fn count(&self) -> u64 {
        self.count.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Overwrite one header row in place (offline repair / rebuild tools).
    pub fn rewrite(&self, fk: Fk, rec: &HeaderRecord) -> Result<(), StoreError> {
        let _g = self.put_lock.lock().unwrap_or_else(|e| e.into_inner());
        let id = fk.get().ok_or(StoreError::InvalidFk)?;
        let count = self.count.load(std::sync::atomic::Ordering::Acquire);
        if id == 0 || id > count {
            return Err(StoreError::NotFound);
        }
        let offset = FILE_HEADER_LEN as u64 + (id - 1) * HEADER_RECORD_LEN as u64;
        self.body.write_at(offset, &rec.encode())?;
        Ok(())
    }

    /// Occupied open-address slots in the header hash head.
    pub fn head_occupied(&self) -> u64 {
        self.head.occupied()
    }

    pub fn flush(&self) -> Result<(), StoreError> {
        self.body.flush()?;
        self.head.flush()?;
        Ok(())
    }

    pub fn flush_async(&self) -> Result<(), StoreError> {
        self.body.flush_async()?;
        self.head.flush_async()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp() -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!(
            "rbitcoin-header-{}-{}",
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

    fn sample(hash: [u8; 32]) -> HeaderRecord {
        HeaderRecord {
            prev_fk: Fk::NULL,
            version: 1,
            timestamp: 100,
            bits: 0x1d00ffff,
            nonce: 7,
            merkle_root: [2u8; 32],
            hash,
        }
    }

    #[test]
    fn header_put_get_by_hash_open_flush() {
        let dir = tmp();
        let t = HeaderTable::create(&dir).unwrap();
        let h1 = [1u8; 32];
        let h2 = [2u8; 32];
        let fk1 = t.put(&sample(h1)).unwrap();
        let fk2 = t.put(&sample(h2)).unwrap();
        assert_eq!(t.count(), 2);
        assert_eq!(t.get(fk1).unwrap().hash, h1);
        assert_eq!(t.get(fk2).unwrap().hash, h2);
        assert_eq!(t.get_by_hash(&h1).unwrap().unwrap().0, fk1);
        assert!(t.get_by_hash(&[9u8; 32]).unwrap().is_none());
        assert!(matches!(t.get(Fk::NULL), Err(StoreError::InvalidFk)));
        assert!(matches!(t.get(Fk(99)), Err(StoreError::NotFound)));
        // short decode
        assert!(matches!(
            HeaderRecord::decode(&[0u8; 10]),
            Err(StoreError::Corrupt(_))
        ));
        t.flush().unwrap();
        t.flush_async().unwrap();
        drop(t);
        let t = HeaderTable::open(&dir).unwrap();
        assert_eq!(t.count(), 2);
        assert_eq!(t.get_by_hash(&h2).unwrap().unwrap().1.nonce, 7);
        // Shrink OS file below HWM so open clamps logical to a non-record size.
        {
            use crate::file::FILE_HEADER_LEN;
            let body = dir.join("header.body");
            std::fs::OpenOptions::new()
                .write(true)
                .open(&body)
                .unwrap()
                .set_len((FILE_HEADER_LEN + 3) as u64)
                .unwrap();
        }
        assert!(matches!(
            HeaderTable::open(&dir),
            Err(StoreError::Corrupt(_))
        ));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Real block-header fields with PoW-style hash committed to a real parent.
    fn linked_child(parent: &HeaderRecord, parent_fk: Fk, salt: u32) -> HeaderRecord {
        let version = 1;
        let timestamp = 1_700_000_000 + salt;
        let bits = 0x207fffff;
        let nonce = salt;
        let mut merkle = [0u8; 32];
        merkle[0..4].copy_from_slice(&salt.to_le_bytes());
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

    /// Production failure shape: same full hash as an older block, but `prev_fk`
    /// points at a tip-extension header. Without the write gate this plants a
    /// false child edge that resume walks as "headers past tip".
    #[test]
    fn ensure_rejects_duplicate_hash_with_divergent_prev_and_false_parent_edge() {
        let dir = tmp();
        let t = HeaderTable::create(&dir).unwrap();

        // G (null prev, synthetic hash) → A → B → C (real linked hashes).
        let g = sample([0x11; 32]);
        let g_fk = t.ensure(&g).unwrap();
        let a = linked_child(&g, g_fk, 1);
        let a_fk = t.ensure(&a).unwrap();
        let b = linked_child(&a, a_fk, 2);
        let b_fk = t.ensure(&b).unwrap();
        let c = linked_child(&b, b_fk, 3);
        let c_fk = t.ensure(&c).unwrap();
        assert_eq!(t.count(), 4);

        // Poison: re-insert G's identity with prev_fk = C (false parent).
        let mut poison = g.clone();
        poison.prev_fk = c_fk;
        // Same hash as G; gate must return G's fk and not append.
        let again = t.ensure(&poison).unwrap();
        assert_eq!(again, g_fk, "same hash must not create a second row");
        assert_eq!(t.count(), 4, "duplicate hash must not grow the table");
        assert_eq!(t.get(g_fk).unwrap().prev_fk, Fk::NULL);

        // First-time insert of a header whose hash commits to A as parent, but
        // caller lies with prev_fk = C → corrupt (false parent edge).
        let honest = linked_child(&a, a_fk, 99);
        let mut lying = honest.clone();
        lying.prev_fk = c_fk;
        let err = t.ensure(&lying).unwrap_err();
        assert!(
            matches!(err, StoreError::Corrupt(_)),
            "false parent edge must be rejected at write gate, got {err}"
        );
        assert_eq!(t.count(), 4);

        // Honest insert still works.
        let ok = t.ensure(&honest).unwrap();
        assert_eq!(t.count(), 5);
        assert_eq!(t.get(ok).unwrap().prev_fk, a_fk);

        // Children of C: only what truly points at C (none of the poisons).
        let mut kids_of_c = 0u32;
        for id in 1..=t.count() {
            let rec = t.get(Fk(id)).unwrap();
            if rec.prev_fk == c_fk {
                kids_of_c += 1;
            }
        }
        assert_eq!(
            kids_of_c, 0,
            "C must not gain false children from poison puts"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn ensure_same_hash_twice_is_idempotent() {
        let dir = tmp();
        let t = HeaderTable::create(&dir).unwrap();
        let h = sample([7u8; 32]);
        let fk1 = t.ensure(&h).unwrap();
        let mut h2 = h.clone();
        h2.prev_fk = Fk(999); // divergent prev ignored on hit
        let fk2 = t.ensure(&h2).unwrap();
        assert_eq!(fk1, fk2);
        assert_eq!(t.count(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
