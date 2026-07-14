//! Tip wire-format block ring (soft zone only; not full historical blocks).

use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

/// Crate identity for diagnostics.
pub fn crate_name() -> &'static str {
    "rbitcoin-wire-cache"
}

/// Wire-format block ring for the non-finalized soft zone.
///
/// When `dir` is set, blocks are also written under `{dir}/{height:08}.bin` for
/// crash recovery of the tip window. Historical serve still uses reconstruct.
#[derive(Debug)]
pub struct WireRing {
    depth: u32,
    dir: Option<PathBuf>,
    inner: RwLock<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    /// height → wire bytes
    by_height: BTreeMap<u32, Vec<u8>>,
    /// block hash → height
    by_hash: HashMap<[u8; 32], u32>,
}

impl WireRing {
    pub fn new(depth: u32) -> Self {
        Self {
            depth,
            dir: None,
            inner: RwLock::new(Inner::default()),
        }
    }

    /// Create ring with optional on-disk directory (created if missing).
    pub fn with_dir(depth: u32, dir: impl Into<PathBuf>) -> std::io::Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        let mut ring = Self {
            depth,
            dir: Some(dir.clone()),
            inner: RwLock::new(Inner::default()),
        };
        ring.load_from_disk()?;
        Ok(ring)
    }

    pub fn depth(&self) -> u32 {
        self.depth
    }

    pub fn len(&self) -> usize {
        self.inner.read().by_height.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Insert wire bytes for a block at `height`. Evicts heights that fall outside the window
    /// when `tip_height` is known (keep `(tip_height.saturating_sub(depth), tip_height]`).
    pub fn push(
        &self,
        height: u32,
        hash: [u8; 32],
        wire: Vec<u8>,
        tip_height: u32,
    ) -> std::io::Result<()> {
        {
            let mut g = self.inner.write();
            g.by_height.insert(height, wire.clone());
            g.by_hash.insert(hash, height);
        }
        if let Some(dir) = &self.dir {
            let path = wire_path(dir, height);
            let mut f = File::create(&path)?;
            f.write_all(&hash)?;
            f.write_all(&(wire.len() as u32).to_le_bytes())?;
            f.write_all(&wire)?;
            f.sync_all()?;
        }
        self.evict_below_window(tip_height)?;
        Ok(())
    }

    /// Drop all wire entries with height ≤ `height` (after archive finalize).
    pub fn drop_through(&self, height: u32) -> std::io::Result<()> {
        let mut g = self.inner.write();
        let remove: Vec<u32> = g.by_height.range(..=height).map(|(h, _)| *h).collect();
        for h in remove {
            if let Some(bytes) = g.by_height.remove(&h) {
                let _ = bytes;
            }
            g.by_hash.retain(|_, hh| *hh != h);
            if let Some(dir) = &self.dir {
                let _ = fs::remove_file(wire_path(dir, h));
            }
        }
        Ok(())
    }

    pub fn get_by_height(&self, height: u32) -> Option<Vec<u8>> {
        self.inner.read().by_height.get(&height).cloned()
    }

    pub fn get_by_hash(&self, hash: &[u8; 32]) -> Option<Vec<u8>> {
        let g = self.inner.read();
        let h = g.by_hash.get(hash)?;
        g.by_height.get(h).cloned()
    }

    pub fn contains_height(&self, height: u32) -> bool {
        self.inner.read().by_height.contains_key(&height)
    }

    fn evict_below_window(&self, tip_height: u32) -> std::io::Result<()> {
        if self.depth == 0 {
            return self.drop_through(tip_height);
        }
        let min_keep = tip_height.saturating_sub(self.depth.saturating_sub(1));
        if min_keep == 0 {
            return Ok(());
        }
        // drop heights < min_keep
        if min_keep > 0 {
            self.drop_through(min_keep.saturating_sub(1))?;
        }
        Ok(())
    }

    fn load_from_disk(&mut self) -> std::io::Result<()> {
        let Some(dir) = &self.dir else {
            return Ok(());
        };
        let mut g = self.inner.write();
        for ent in fs::read_dir(dir)? {
            let ent = ent?;
            let name = ent.file_name();
            let name = name.to_string_lossy();
            if !name.ends_with(".bin") {
                continue;
            }
            let stem = name.trim_end_matches(".bin");
            let Ok(height) = stem.parse::<u32>() else {
                continue;
            };
            let mut f = File::open(ent.path())?;
            let mut hash = [0u8; 32];
            f.read_exact(&mut hash)?;
            let mut len_buf = [0u8; 4];
            f.read_exact(&mut len_buf)?;
            let len = u32::from_le_bytes(len_buf) as usize;
            let mut wire = vec![0u8; len];
            f.read_exact(&mut wire)?;
            g.by_height.insert(height, wire);
            g.by_hash.insert(hash, height);
        }
        Ok(())
    }
}

fn wire_path(dir: &Path, height: u32) -> PathBuf {
    dir.join(format!("{height:08}.bin"))
}
