//! Tip wire-format block cache for the soft zone (not full historical blocks).
//!
//! Indexed by **block hash** so competing tips and side branches at the same height
//! are retained until they age out of the recent window (max tip height − depth).

use parking_lot::RwLock;
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
        self.inner.read().by_hash.len()
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
            let mut g = self.inner.write();
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
        let mut g = self.inner.write();
        g.tips.insert(hash);
    }

    /// Clear tip marker (e.g. after reorg abandoned a branch tip). Entry remains until aged out.
    pub fn unmark_tip(&self, hash: &[u8; 32]) {
        self.inner.write().tips.remove(hash);
    }

    /// Drop all entries with height ≤ `height` (archive finalize). Affects every branch.
    pub fn drop_through(&self, height: u32) -> std::io::Result<()> {
        let mut g = self.inner.write();
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
            .read()
            .by_hash
            .get(hash)
            .map(|e| e.wire.clone())
    }

    pub fn entry(&self, hash: &[u8; 32]) -> Option<WireEntry> {
        self.inner.read().by_hash.get(hash).cloned()
    }

    /// All wire blocks at a given height (forks / competing tips).
    pub fn get_all_at_height(&self, height: u32) -> Vec<WireEntry> {
        self.inner
            .read()
            .by_hash
            .values()
            .filter(|e| e.height == height)
            .cloned()
            .collect()
    }

    /// Best-effort single block at height (prefer marked tip if unique at that height).
    pub fn get_by_height(&self, height: u32) -> Option<Vec<u8>> {
        let g = self.inner.read();
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
        self.inner.read().by_hash.contains_key(hash)
    }

    pub fn contains_height(&self, height: u32) -> bool {
        self.inner
            .read()
            .by_hash
            .values()
            .any(|e| e.height == height)
    }

    /// Hashes currently marked as tips (candidates).
    pub fn tip_hashes(&self) -> Vec<[u8; 32]> {
        self.inner.read().tips.iter().copied().collect()
    }

    pub fn max_height(&self) -> u32 {
        self.inner.read().max_height
    }

    /// Lowest height retained given current max height and depth.
    pub fn window_floor(&self) -> u32 {
        let g = self.inner.read();
        window_floor(g.max_height, self.depth)
    }

    fn evict_old(&self) -> std::io::Result<()> {
        if self.depth == 0 {
            let max = self.inner.read().max_height;
            return self.drop_through(max);
        }
        let floor = {
            let g = self.inner.read();
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
        let mut g = self.inner.write();
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
    dir.join(format!("{}.bin", hex::encode(hash)))
}
