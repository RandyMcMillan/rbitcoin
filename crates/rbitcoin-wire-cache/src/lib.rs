//! Tip wire-format block cache for the soft zone (not full historical blocks).
//!
//! Indexed by **block hash** so competing tips and side branches at the same height
//! are retained until they age out of the recent window (max tip height − depth).

use std::sync::RwLock;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

/// Crate identity for diagnostics.
pub fn crate_name() -> &'static str {
    "rbitcoin-wire-cache"
}

/// One wire block in the soft-zone cache.
#[derive(Clone, Debug)]
pub struct WireEntry {
    pub height: u32,
    pub hash: [u8; 32],
    pub prev_hash: [u8; 32],
    pub wire: Vec<u8>,
}

/// Wire-format cache for recent blocks across **all** candidate tips / reorg branches.
///
/// - Storage key is **hash** (not height), so two blocks at the same height both stay.
/// - Eviction drops only entries with `height < window_floor`, where
///   `window_floor = max_height.saturating_sub(depth.saturating_sub(1))` over all
///   cached blocks (and any reported tip heights).
/// - Optional on-disk files: `{dir}/{hash_hex}.bin` for crash recovery of the soft zone.
#[derive(Debug)]
pub struct WireRing {
    depth: u32,
    dir: Option<PathBuf>,
    inner: RwLock<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    by_hash: HashMap<[u8; 32], WireEntry>,
    /// Known tip hashes (best chain and candidates) for window calculation.
    tips: HashSet<[u8; 32]>,
    /// Highest height ever seen (any branch).
    max_height: u32,
}

impl WireRing {
    pub fn new(depth: u32) -> Self {
        Self {
            depth,
            dir: None,
            inner: RwLock::new(Inner::default()),
        }
    }

    pub fn with_dir(depth: u32, dir: impl Into<PathBuf>) -> std::io::Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        let mut ring = Self {
            depth,
            dir: Some(dir),
            inner: RwLock::new(Inner::default()),
        };
        ring.load_from_disk()?;
        Ok(ring)
    }

    pub fn depth(&self) -> u32 {
        self.depth
    }

    pub fn len(&self) -> usize {
        self.inner.read().unwrap().by_hash.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Insert wire bytes for a block. Does not overwrite a different block at the same height.
    ///
    /// `is_tip`: mark this hash as a candidate tip (best or competing).
    pub fn push(
        &self,
        height: u32,
        hash: [u8; 32],
        prev_hash: [u8; 32],
        wire: Vec<u8>,
        is_tip: bool,
    ) -> std::io::Result<()> {
        {
            let mut g = self.inner.write().unwrap();
            g.by_hash.insert(
                hash,
                WireEntry {
                    height,
                    hash,
                    prev_hash,
                    wire: wire.clone(),
                },
            );
            if height > g.max_height {
                g.max_height = height;
            }
            if is_tip {
                // New tip at this height supersedes prior tips only when it extends; keep
                // other tip hashes as candidates until they age out of the window.
                g.tips.insert(hash);
            }
        }
        if let Some(dir) = &self.dir {
            let path = wire_path(dir, &hash);
            let mut f = File::create(&path)?;
            f.write_all(&hash)?;
            f.write_all(&prev_hash)?;
            f.write_all(&height.to_le_bytes())?;
            f.write_all(&(wire.len() as u32).to_le_bytes())?;
            f.write_all(&wire)?;
            f.sync_all()?;
        }
        self.evict_old()?;
        Ok(())
    }

    /// Convenience: push and mark as tip; also notes best-chain tip height for window.
    pub fn push_tip(
        &self,
        height: u32,
        hash: [u8; 32],
        prev_hash: [u8; 32],
        wire: Vec<u8>,
    ) -> std::io::Result<()> {
        self.push(height, hash, prev_hash, wire, true)
    }

    /// Register a candidate tip hash already present (or about to be pushed).
    pub fn mark_tip(&self, hash: [u8; 32]) {
        let mut g = self.inner.write().unwrap();
        g.tips.insert(hash);
    }

    /// Clear tip marker (e.g. after reorg abandoned a branch tip). Entry remains until aged out.
    pub fn unmark_tip(&self, hash: &[u8; 32]) {
        self.inner.write().unwrap().tips.remove(hash);
    }

    /// Drop all entries with height ≤ `height` (archive finalize). Affects every branch.
    pub fn drop_through(&self, height: u32) -> std::io::Result<()> {
        let mut g = self.inner.write().unwrap();
        let remove: Vec<[u8; 32]> = g
            .by_hash
            .iter()
            .filter(|(_, e)| e.height <= height)
            .map(|(h, _)| *h)
            .collect();
        for h in remove {
            g.by_hash.remove(&h);
            g.tips.remove(&h);
            if let Some(dir) = &self.dir {
                let _ = fs::remove_file(wire_path(dir, &h));
            }
        }
        // Recompute max_height
        g.max_height = g.by_hash.values().map(|e| e.height).max().unwrap_or(0);
        Ok(())
    }

    pub fn get_by_hash(&self, hash: &[u8; 32]) -> Option<Vec<u8>> {
        self.inner
            .read().unwrap()
            .by_hash
            .get(hash)
            .map(|e| e.wire.clone())
    }

    pub fn entry(&self, hash: &[u8; 32]) -> Option<WireEntry> {
        self.inner.read().unwrap().by_hash.get(hash).cloned()
    }

    /// All wire blocks at a given height (forks / competing tips).
    pub fn get_all_at_height(&self, height: u32) -> Vec<WireEntry> {
        self.inner
            .read().unwrap()
            .by_hash
            .values()
            .filter(|e| e.height == height)
            .cloned()
            .collect()
    }

    /// Best-effort single block at height (prefer marked tip if unique at that height).
    pub fn get_by_height(&self, height: u32) -> Option<Vec<u8>> {
        let g = self.inner.read().unwrap();
        let at: Vec<_> = g.by_hash.values().filter(|e| e.height == height).collect();
        if at.is_empty() {
            return None;
        }
        if at.len() == 1 {
            return Some(at[0].wire.clone());
        }
        // Prefer a marked tip at this height.
        for e in &at {
            if g.tips.contains(&e.hash) {
                return Some(e.wire.clone());
            }
        }
        Some(at[0].wire.clone())
    }

    pub fn contains_hash(&self, hash: &[u8; 32]) -> bool {
        self.inner.read().unwrap().by_hash.contains_key(hash)
    }

    pub fn contains_height(&self, height: u32) -> bool {
        self.inner
            .read().unwrap()
            .by_hash
            .values()
            .any(|e| e.height == height)
    }

    /// Hashes currently marked as tips (candidates).
    pub fn tip_hashes(&self) -> Vec<[u8; 32]> {
        self.inner.read().unwrap().tips.iter().copied().collect()
    }

    pub fn max_height(&self) -> u32 {
        self.inner.read().unwrap().max_height
    }

    /// Lowest height retained given current max height and depth.
    pub fn window_floor(&self) -> u32 {
        let g = self.inner.read().unwrap();
        window_floor(g.max_height, self.depth)
    }

    fn evict_old(&self) -> std::io::Result<()> {
        if self.depth == 0 {
            let max = self.inner.read().unwrap().max_height;
            return self.drop_through(max);
        }
        let floor = {
            let g = self.inner.read().unwrap();
            window_floor(g.max_height, self.depth)
        };
        if floor == 0 {
            return Ok(());
        }
        // Keep height >= floor; drop height < floor (all branches).
        if floor > 0 {
            self.drop_through(floor.saturating_sub(1))?;
        }
        Ok(())
    }

    fn load_from_disk(&mut self) -> std::io::Result<()> {
        let Some(dir) = self.dir.clone() else {
            return Ok(());
        };
        let mut g = self.inner.write().unwrap();
        for ent in fs::read_dir(&dir)? {
            let ent = ent?;
            let name = ent.file_name();
            let name = name.to_string_lossy();
            if !name.ends_with(".bin") {
                continue;
            }
            let mut f = File::open(ent.path())?;
            let mut hash = [0u8; 32];
            f.read_exact(&mut hash)?;
            // New format: hash || prev || height || len || wire
            // Legacy format was: hash || len || wire (height in filename)
            let meta_rest = {
                let mut buf = Vec::new();
                f.read_to_end(&mut buf)?;
                buf
            };
            let (prev_hash, height, wire) = if meta_rest.len() >= 4 + 4 + 32
                && name.len() >= 64
            {
                // Prefer new format: 32 prev + 4 height + 4 len + wire
                if meta_rest.len() >= 40 {
                    let prev: [u8; 32] = meta_rest[0..32].try_into().unwrap();
                    let height = u32::from_le_bytes(meta_rest[32..36].try_into().unwrap());
                    let len = u32::from_le_bytes(meta_rest[36..40].try_into().unwrap()) as usize;
                    if meta_rest.len() >= 40 + len {
                        let wire = meta_rest[40..40 + len].to_vec();
                        (prev, height, wire)
                    } else {
                        continue;
                    }
                } else {
                    continue;
                }
            } else {
                continue;
            };
            if height > g.max_height {
                g.max_height = height;
            }
            g.by_hash.insert(
                hash,
                WireEntry {
                    height,
                    hash,
                    prev_hash,
                    wire,
                },
            );
        }
        Ok(())
    }
}

fn window_floor(max_height: u32, depth: u32) -> u32 {
    if depth == 0 {
        return max_height.saturating_add(1); // keep nothing
    }
    max_height.saturating_sub(depth.saturating_sub(1))
}

fn wire_path(dir: &Path, hash: &[u8; 32]) -> PathBuf {
    dir.join(format!("{}.bin", rbitcoin_primitives::hex_encode(hash)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn tmp() -> PathBuf {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let p = std::env::temp_dir().join(format!("rbitcoin-wire-{n}"));
        let _ = std::fs::remove_dir_all(&p);
        p
    }

    fn h(b: u8) -> [u8; 32] {
        [b; 32]
    }

    #[test]
    fn crate_name_and_memory_ring_basics() {
        assert_eq!(crate_name(), "rbitcoin-wire-cache");
        let ring = WireRing::new(3);
        assert!(ring.is_empty());
        assert_eq!(ring.depth(), 3);
        assert_eq!(ring.len(), 0);
        assert_eq!(ring.max_height(), 0);
        assert_eq!(ring.window_floor(), 0);

        ring.push(1, h(1), h(0), vec![0xaa], true).unwrap();
        ring.push_tip(2, h(2), h(1), vec![0xbb]).unwrap();
        ring.mark_tip(h(1));
        assert_eq!(ring.len(), 2);
        assert!(!ring.is_empty());
        assert!(ring.contains_hash(&h(1)));
        assert!(ring.contains_height(2));
        assert!(!ring.contains_height(99));
        assert_eq!(ring.get_by_hash(&h(2)).unwrap(), vec![0xbb]);
        assert_eq!(ring.entry(&h(1)).unwrap().height, 1);
        assert_eq!(ring.get_by_height(2).unwrap(), vec![0xbb]);
        assert!(ring.get_by_height(99).is_none());
        assert_eq!(ring.get_all_at_height(1).len(), 1);
        assert!(ring.tip_hashes().contains(&h(1)));
        assert_eq!(ring.max_height(), 2);
        assert_eq!(ring.window_floor(), 0); // max 2 depth 3 → floor 0

        ring.unmark_tip(&h(1));
        assert!(!ring.tip_hashes().contains(&h(1)));

        // Competing tips at same height: prefer marked tip.
        ring.push(2, h(3), h(1), vec![0xcc], true).unwrap();
        assert_eq!(ring.get_all_at_height(2).len(), 2);
        let preferred = ring.get_by_height(2).unwrap();
        assert!(preferred == vec![0xbb] || preferred == vec![0xcc]);

        ring.drop_through(1).unwrap();
        assert!(!ring.contains_height(1));
        assert!(ring.contains_height(2));
    }

    #[test]
    fn depth_zero_evicts_all_and_disk_roundtrip() {
        let dir = tmp();
        {
            let ring = WireRing::with_dir(2, &dir).unwrap();
            ring.push_tip(10, h(10), h(9), vec![1, 2, 3]).unwrap();
            ring.push_tip(11, h(11), h(10), vec![4, 5]).unwrap();
            ring.push_tip(12, h(12), h(11), vec![6]).unwrap();
            // Window floor for max=12 depth=2 is 11; height 10 dropped.
            assert!(!ring.contains_height(10));
            assert!(ring.contains_height(11));
            assert!(ring.contains_height(12));
            assert_eq!(ring.get_by_hash(&h(12)).unwrap(), vec![6]);
        }
        // Reload from disk.
        {
            let ring = WireRing::with_dir(2, &dir).unwrap();
            assert!(ring.contains_hash(&h(11)) || ring.contains_hash(&h(12)));
            assert!(ring.len() >= 1);
            assert_eq!(ring.max_height() >= 11, true);
        }
        // depth 0: keep nothing after push
        {
            let ring = WireRing::new(0);
            ring.push_tip(5, h(5), h(4), vec![9]).unwrap();
            assert!(ring.is_empty());
            assert_eq!(window_floor(5, 0), 6);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn window_floor_math() {
        assert_eq!(window_floor(0, 1), 0);
        assert_eq!(window_floor(10, 1), 10);
        assert_eq!(window_floor(10, 5), 6);
        assert_eq!(window_floor(0, 0), 1);
    }

    #[test]
    fn multi_at_height_prefers_tip_and_loads_junk_files() {
        let dir = tmp();
        // Non-.bin junk ignored on load.
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("readme.txt"), b"hi").unwrap();
        {
            let ring = WireRing::with_dir(5, &dir).unwrap();
            ring.push(1, h(1), h(0), vec![1], false).unwrap();
            ring.push(1, h(2), h(0), vec![2], true).unwrap();
            // Prefer marked tip among two at same height.
            assert_eq!(ring.get_by_height(1).unwrap(), vec![2]);
            ring.unmark_tip(&h(2));
            // No tip → first available.
            let any = ring.get_by_height(1).unwrap();
            assert!(any == vec![1] || any == vec![2]);
        }
        // Short rest after 32-byte hash is skipped (continue), not fatal.
        let bad = dir.join(format!("{}.bin", rbitcoin_primitives::hex_encode(h(9))));
        std::fs::write(&bad, [0u8; 32]).unwrap(); // hash only → no prev/height/len
        let ring = WireRing::with_dir(5, &dir).unwrap();
        assert!(ring.contains_hash(&h(1)) || ring.contains_hash(&h(2)));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
